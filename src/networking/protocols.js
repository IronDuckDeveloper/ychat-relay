import * as lp from 'it-length-prefixed';
import { pipe } from 'it-pipe';
import { checkAndLogRegistration, checkIfProfileExists } from '../database/db.js';
import { generateAuthToken } from '../utils/crypto.js';
import { CONFIG } from '../config.js';
import { multiaddr } from '@multiformats/multiaddr';
import { issueSessionToken } from '../utils/sessionToken.js';

export function setupAntiFloodProtocol(libp2p, pubsub, archivist, globalRegistryDb = null) {
  console.log(`📡 [libp2p] Регистрация кастомного протокола: ${CONFIG.TOPICS.RPC_PROTOCOL}`);

  libp2p.handle(CONFIG.TOPICS.RPC_PROTOCOL, async ({ stream, connection }) => {
    try {
      await pipe(
        stream.source,
        lp.decode,
        async function (source) {
          for await (const chunk of source) {
            const data = JSON.parse(new TextDecoder().decode(chunk.subarray()));
            const clientFingerprint = data.fingerprint;
            const clientIp = data.ipAddress;
            const targetPeerId = data.peerId || connection?.remotePeer?.toString(); // 👈 поднято наверх — общее для REGISTER и LOGIN

            console.log(`📥 [RPC] Запрос верификации. Действие: ${data.action}, Сетевой IP: ${clientIp}, FP: ${clientFingerprint?.slice(-12)}...`);

            if (!clientIp || !clientFingerprint) {
              console.warn('⚠️ [Protocol] Клиент прислал пустой IP или Fingerprint. Отклоняем.');
              const errorPayload = JSON.stringify({
                status: CONFIG.MSG.FORBIDDEN,
                message: CONFIG.MSG.EMPTY_FINGERPRINT
              });
              await pipe([new TextEncoder().encode(errorPayload)], lp.encode, stream.sink);
              break;
            }

            let responseStatus = CONFIG.MSG.FORBIDDEN;
            let responseMessage = CONFIG.MSG.LIMIT_EXCEEDED;
            let sessionToken = null; // 👈 НОВОЕ

            if (data.action === 'REGISTER') {
              const isAllowed = checkAndLogRegistration(clientIp, clientFingerprint, data.profileDbAddress);

              if (isAllowed && data.profileDbAddress) {
                if (data.clientMultiaddr) {
                  console.log(`🔗 [Архивариус] Пытаюсь соединиться с клиентом: ${data.clientMultiaddr}`);
                  await libp2p.dial(multiaddr(data.clientMultiaddr)).catch(e => console.error('Не смог подконнектиться к клиенту:', e));
                }

                archivist.pinRoom(data.profileDbAddress).catch(e => console.error('Ошибка пина профиля:', e));

                if (targetPeerId && globalRegistryDb) {
                  try {
                    await globalRegistryDb.put(targetPeerId, data.profileDbAddress);
                    console.log(`📇 [RPC-Register] Мгновенно привязали PeerID к БД: ${targetPeerId.slice(-12)} -> ${data.profileDbAddress}`);
                  } catch (regErr) {
                    console.error('❌ [RPC-Register] Ошибка записи в globalRegistryDb:', regErr.message);
                  }
                }

                if (targetPeerId) {
                  try {
                    const profileUpdatePayload = JSON.stringify({
                      type: CONFIG.MSG.PROFILE_UPDATED,
                      senderId: targetPeerId,
                      profileDbAddress: data.profileDbAddress,
                      timestamp: Date.now()
                    });
                    await pubsub.publish(CONFIG.TOPICS.PROFILE_UPDATES_TOPIC, new TextEncoder().encode(profileUpdatePayload));
                    console.log(`📢 [RPC-Register] Анонс обновления профиля отправлен в ${CONFIG.TOPICS.PROFILE_UPDATES_TOPIC}`);
                  } catch (pubErr) {
                    console.error('⚠️ [RPC-Register] Не удалось отправить бродкаст профиля:', pubErr.message);
                  }
                }
              }

              responseStatus = isAllowed ? CONFIG.MSG.SUCCESS : CONFIG.MSG.FORBIDDEN;
              responseMessage = isAllowed ? CONFIG.MSG.REG_IS_OVER : CONFIG.MSG.LIMIT_EXCEEDED;

              if (isAllowed && targetPeerId) {
                sessionToken = issueSessionToken(targetPeerId); // 👈 НОВОЕ
              }

              try {
                const timestamp = Date.now();
                const auth = generateAuthToken(timestamp, CONFIG.SECURITY.clusterSecret);
                const liveRecord = {
                  ip: clientIp,
                  fingerprint: clientFingerprint,
                  profile_address: data.profileDbAddress,
                  created_at: timestamp
                };
                const payload = JSON.stringify({ timestamp, auth, record: liveRecord });
                pubsub.publish(CONFIG.TOPICS.DB_LIVE_SYNC, new TextEncoder().encode(payload));
                console.log(`📢 [LIVE-SYNC] Бродкаст новой регистрации отправлен в кластер.`);
              } catch (err) {
                console.error('❌ [LIVE-SYNC] Ошибка отправки бродкаста:', err.message);
              }

            } else if (data.action === 'LOGIN') {
              console.log(`🔑 [Protocol] Проверяем вход для БД: ${data.profileDbAddress?.slice(-12)}...`);
              const userExists = checkIfProfileExists(data.profileDbAddress);

              if (userExists) {
                if (data.clientMultiaddr) {
                  console.log(`🔗 [Архивариус] Пытаюсь соединиться с клиентом: ${data.clientMultiaddr}`);
                  await libp2p.dial(multiaddr(data.clientMultiaddr)).catch(e => console.error('Не смог подконнектиться к клиенту:', e));
                }
                archivist.pinRoom(data.profileDbAddress).catch(e => console.error('Ошибка пина профиля:', e));
                console.log(`✅ [Protocol] Пользователь найден в БД, вход разрешен.`);
                responseStatus = CONFIG.MSG.SUCCESS;
                responseMessage = CONFIG.MSG.LOGIN_SUCCESS;
                sessionToken = issueSessionToken(targetPeerId); // 👈 НОВОЕ
              } else {
                console.warn(`⚠️ [Protocol] Отказ во входе: профиль не зарегистрирован.`);
                responseStatus = CONFIG.MSG.NOT_FOUND;
                responseMessage = CONFIG.MSG.PROFILE_NOT_FOUND;
              }

            } else {
              console.warn(`⚠️ [Protocol] Неизвестное действие: ${data.action}`);
              responseStatus = CONFIG.MSG.INCORRECT_ACTION;
              responseMessage = CONFIG.MSG.INCORRECT_ACTION_MSG;
            }

            const responsePayload = JSON.stringify({
              status: responseStatus,
              message: responseMessage,
              sessionToken // 👈 НОВОЕ
            });

            try {
              await pipe([new TextEncoder().encode(responsePayload)], lp.encode, stream.sink);
            } catch (err) {
              if (!err.message.includes('ended pushable') && !err.message.includes('stream reset')) {
                console.error('❌ [Protocol] Ошибка ответа в стрим:', err);
              }
            }

            break;
          }
        }
      );
    } catch (error) {
      console.error('❌ [RPC Error] Ошибка обработки запроса:', error);
      stream.close();
    }
  }, {
    runOnTransientConnection: true
  });
}

export async function registerAnnounceProtocol(node, archivist, pendingRequests) {
  await node.handle(CONFIG.TOPICS.ANNOUNCE, async ({ stream }) => {
    const remotePeerId = stream.remotePeer;
    try {
      const { pipe } = await import('it-pipe');
      await pipe(
        stream,
        async function (source) {
          for await (const buf of source) {
            const decoded = new TextDecoder().decode(buf.subarray()).trim();
            try {
              const parsed = JSON.parse(decoded);
              const targetAddress = (typeof parsed === 'string') ? parsed : parsed.address;
              
              if (targetAddress) {
                const now = Date.now();
                if (pendingRequests.has(targetAddress)) {
                  const lastSeen = pendingRequests.get(targetAddress);
                  if (now - lastSeen < CONFIG.DELAY_START_MS) return;  
                }

                pendingRequests.set(targetAddress, now);
                console.log(`🏠 [Protocol] Запрос на архивацию БД: ${targetAddress}`);
                
                archivist.pinRoom(targetAddress, remotePeerId).catch(err => {
                  console.error('❌ Ошибка фонового открытия БД:', err);
                });
              }
            } catch (e) {
              if (decoded) {
                console.log(`🏠 [Protocol] Запрос на архивацию БД (raw): ${decoded}`);
                archivist.pinRoom(decoded).catch(err => console.error(err));
              }
            }
          }
        }
      );
    } catch (err) {
      console.error(`❌ [Protocol] Ошибка стрима: ${err.message}`);
    }
  }, {
    runOnTransientConnection: true
  });
}