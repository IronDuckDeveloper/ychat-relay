import * as lp from 'it-length-prefixed';
import { pipe } from 'it-pipe';
import { checkAndLogRegistration, checkIfProfileExists } from '../database/db.js';
import { generateAuthToken } from '../utils/crypto.js';
import { CONFIG } from '../config.js';

export function setupAntiFloodProtocol(libp2p, pubsub) {
  console.log(`📡 [libp2p] Регистрация кастомного протокола: ${CONFIG.TOPICS.RPC_PROTOCOL}`);

  libp2p.handle(CONFIG.TOPICS.RPC_PROTOCOL, async ({ stream, connection }) => {
    try {
      // Читаем входящий поток данных от клиента
      await pipe(
        stream.source,
        lp.decode,
        async function (source) {
          for await (const chunk of source) {
            // 1. Декодируем входящий JSON от клиента
            const data = JSON.parse(new TextDecoder().decode(chunk.subarray()));
            const clientFingerprint = data.fingerprint;
            const clientIp = data.ipAddress;

            console.log(`📥 [RPC] Запрос верификации. Действие: ${data.action}, Сетевой IP: ${clientIp}, FP: ${clientFingerprint?.slice(0, 10)}...`);

            // Защита от пустых данных
            if (!clientIp || !clientFingerprint) {
              console.warn('⚠️ [Protocol] Клиент прислал пустой IP или Fingerprint. Отклоняем.');
              
              const errorPayload = JSON.stringify({
                status: CONFIG.MSG.FORBIDDEN,
                message: CONFIG.MSG.EMPTY_FINGERPRINT
              });

              await pipe([new TextEncoder().encode(errorPayload)], lp.encode, stream.sink);
              break; 
            }

            // Переменные для формирования ответа (теперь они видны везде)
            let responseStatus = CONFIG.MSG.FORBIDDEN;
            let responseMessage = CONFIG.MSG.LIMIT_EXCEEDED;

            // ==========================================
            // Обработка РЕГИСТРАЦИИ
            // ==========================================
            if (data.action === 'REGISTER') {
              // 🌟 Передаем три параметра, как требует наша новая db.js
              const isAllowed = checkAndLogRegistration(clientIp, clientFingerprint, data.profileDbAddress);
              
              responseStatus = isAllowed ? CONFIG.MSG.SUCCESS : CONFIG.MSG.FORBIDDEN;
              responseMessage = isAllowed ? CONFIG.MSG.REG_IS_OVER : CONFIG.MSG.LIMIT_EXCEEDED;

              try {
                const timestamp = Date.now();
                const auth = generateAuthToken(timestamp, CONFIG.SECURITY.clusterSecret);
                
                // Формируем запись так, как ожидает функция mergeRegistrations
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
            // ==========================================
            // Обработка ВХОДА
            // ==========================================
            } else if (data.action === 'LOGIN') {
              console.log(`🔑 [Protocol] Проверяем вход для БД: ${data.profileDbAddress?.slice(0, 15)}...`);
              
              // Проверяем БД на наличие профиля
              const userExists = checkIfProfileExists(data.profileDbAddress);

              if (userExists) {
                console.log(`✅ [Protocol] Пользователь найден в БД, вход разрешен.`);
                responseStatus = CONFIG.MSG.SUCCESS;
                responseMessage = CONFIG.MSG.LOGIN_SUCCESS;
              } else {
                console.warn(`⚠️ [Protocol] Отказ во входе: профиль не зарегистрирован.`);
                responseStatus = CONFIG.MSG.NOT_FOUND;
                responseMessage = CONFIG.MSG.PROFILE_NOT_FOUND;
              }

            // ==========================================
            // Неизвестное действие
            // ==========================================
            } else {
              console.warn(`⚠️ [Protocol] Неизвестное действие: ${data.action}`);
              responseStatus = CONFIG.MSG.INCORRECT_ACTION;
              responseMessage = CONFIG.MSG.INCORRECT_ACTION_MSG;
            }

            // ==========================================
            // Единая отправка ответа обратно в стрим
            // ==========================================
            const responsePayload = JSON.stringify({
              status: responseStatus,
              message: responseMessage
            });

            try {
              await pipe(
                [new TextEncoder().encode(responsePayload)],
                lp.encode,
                stream.sink
              );
            } catch (err) {
              if (!err.message.includes('ended pushable') && !err.message.includes('stream reset')) {
                console.error('❌ [Protocol] Ошибка ответа в стрим:', err);
              }
            }
            
            break; // Обработали один пакет и закрываем цикл пакетной обработки
          }
        }
      );
    } catch (error) {
      console.error('❌ [RPC Error] Ошибка обработки запроса:', error);
      stream.close();
    }
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
    });
}