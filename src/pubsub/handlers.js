
import crypto from 'crypto'; // Встроенный модуль Node.js, ничего доустанавливать не надо
import { multiaddr } from '@multiformats/multiaddr';
import { peerIdFromString } from '@libp2p/peer-id'; // Убедись, что импорт совпадает с твоим проектом
import { CONFIG } from '../config.js';
import { loadKnownPeersConfig, saveKnownPeersConfig } from '../storage/peers-config.js';
import { generateAuthToken } from '../utils/crypto.js';
import { mergeRegistrations, mergeBanRecords } from '../database/db.js'; // <-- Добавь эту строку

export function setupPubSubHandlers(node, pubsub, archivistService = null, globalRegistryDb = null) {
  pubsub.addEventListener('message', async (evt) => {
    const msg = evt.detail || evt;
    const { topic, data, from } = msg;
    let text = '';
    try { text = new TextDecoder().decode(data); } catch (e) { return; }

    // ==========================================
    // ОБРАБОТКА ЗАПРОСА СИНХРОНИЗАЦИИ (Incoming Request)
    // ==========================================
    if (topic === CONFIG.TOPICS.PEER_SYNC_REQUEST) {
      try {
        const payload = JSON.parse(text);
        const target = payload?.from;
        if (!target || target === node.peerId.toString()) return;

        // 1. БЛОК ЗАПИСИ (Доступен только для других релеев)
      if (payload.relay) {
        const { timestamp, auth } = payload;
        
        if (!timestamp || !auth) {
          console.warn(`🔒 [PEER-SYNC] Отклонено от ${target.slice(-12)}: попытка изменить список релеев без авторизации.`);
          // ВАЖНО: здесь мы не делаем глобальный return, 
          // а просто пропускаем блок добавления в базу!
        } else {
          // Проверяем токен
          const timeDifference = Math.abs(Date.now() - timestamp);
          if (timeDifference > CONFIG.LIVE_STAMP_TOKEN) {
            console.warn(`🔒 [PEER-SYNC] Токен устарел от ${target.slice(-12)}`);
          } else {
            const expectedAuth = generateAuthToken(timestamp, CONFIG.SECURITY.clusterSecret);
            if (auth !== expectedAuth) {
              console.warn(`🔒 [PEER-SYNC] КРИТИЧЕСКАЯ ОШИБКА: неверный пароль от ${target.slice(-12)}!`);
            } else {
              // ✅ Пароль совпал, обновляем базу релеев
              const current = loadKnownPeersConfig();
              const existingIndex = current.relays.findIndex(r => 
                r.peerId === payload.relay.peerId || r.address === payload.relay.address
              );

              if (existingIndex !== -1) {
                const existing = current.relays[existingIndex];
                if (existing.peerId !== payload.relay.peerId || 
                    existing.address !== payload.relay.address || 
                    existing.name !== payload.relay.name) {
                  
                  // Перезаписываем устаревшую запись актуальными данными
                  current.relays[existingIndex] = payload.relay;
                  saveKnownPeersConfig(current);
                  console.log(`🔄 [PEER-SYNC] Обновлены данные узла: ${payload.relay.name}`);
                }
              } else {
                // Узла нет в списке, добавляем как новый
                current.relays.push(payload.relay);
                saveKnownPeersConfig(current);
                console.log(`🆕 [PEER-SYNC] Добавлен новый узел: ${payload.relay.name}`);
              }
              saveKnownPeersConfig(current);

              // ==========================================
              // 🔥 ВЫДАЕМ ПРАВО ЗАПИСИ ДОВЕРЕННОМУ РЕЛЕЮ В РЕЕСТР:
              // ==========================================
              // 🔥 Изменение: проверяем и выдаем права именно на orbitDbIdentity (zdpu...)
              if (globalRegistryDb && payload.relay.orbitDbIdentity) {
                try {
                  await globalRegistryDb.access.grant('write', payload.relay.orbitDbIdentity);
                  console.log(`🔐 [Registry] Выдано право записи релею ${payload.relay.name}: ${payload.relay.orbitDbIdentity}`);
                } catch (err) {
                  console.error('❌ [Registry] Ошибка выдачи прав:', err.message);
                }
              }
            }
          }
        }
      }

      // 2. БЛОК ЧТЕНИЯ (Выполняется ВСЕГДА, отдаем список всем: и релеям, и клиентам-браузерам)
      const config = loadKnownPeersConfig();
      const responseTopic = `${CONFIG.TOPICS.PEER_SYNC_RESPONSE_BASE}`;
      
      const resTimestamp = Date.now();
      // Сервер подписывает свой ответ, чтобы другие релеи знали, что список легитимный
      const resAuth = generateAuthToken(resTimestamp, CONFIG.SECURITY.clusterSecret);

      const responsePayload = JSON.stringify({ 
        to: target,
        relays: config.relays,
        // Отдаем реальный сгенерированный адрес глобального реестра
        globalRegistryAddress: CONFIG.GLOBAL_REGISTRY_ADDRESS,
        timestamp: resTimestamp,
        auth: resAuth
      });

      await pubsub.publish(responseTopic, new TextEncoder().encode(responsePayload));
      console.log(`📤 [PEER-SYNC] Отправлен список пиров для ${target.slice(-12)} (DB: ${CONFIG.GLOBAL_REGISTRY_ADDRESS})`);
      
    } catch (e) {
      console.error('❌ Ошибка в PEER_SYNC_REQUEST:', e.message);
    }
  }

    // ==========================================
    // ОБРАБОТКА ЖИВОЙ СИНХРОНИЗАЦИИ БД (Incoming Live Sync)
    // ==========================================
    if (topic === CONFIG.TOPICS.DB_LIVE_SYNC) {
      try {
        const payload = JSON.parse(text);
        const { timestamp, auth, record } = payload;

        // 1. Базовые проверки на целостность пакета
        if (!timestamp || !auth || !record) return;
        
        // 2. Проверка на устаревший пакет (TTL 60 секунд), чтобы избежать replay-атак
        if (Math.abs(Date.now() - timestamp) > 60000) {
          console.warn('🔒 [LIVE-SYNC] Отклонен устаревший бродкаст');
          return;
        }

        // 3. Валидация криптографической подписи HMAC
        const expectedAuth = generateAuthToken(timestamp, CONFIG.SECURITY.clusterSecret);
        if (auth !== expectedAuth) {
          console.error(`🔒 [LIVE-SYNC] КРИТИЧЕСКАЯ ОШИБКА: Неверная подпись кластера от пира!`);
          return;
        }

        // 4. Если всё чисто — сливаем с нашей БД
        // mergeRegistrations ожидает массив, поэтому оборачиваем record в [ ]
        mergeRegistrations([record]);
        console.log(`⚡ [LIVE-SYNC] Новая регистрация синхронизирована (БД: ${record.profile_address?.slice(-12)}...)`);
      } catch (err) {
        console.error('❌ [LIVE-SYNC] Ошибка обработки бродкаста:', err.message);
      }
      return; // Важно выйти, чтобы сообщение не обрабатывалось дальше
    }

    // ==========================================
    // 🚫 ОБРАБОТКА ЖИВОЙ СИНХРОНИЗАЦИИ БАНОВ (Incoming Live Sync)
    // ==========================================
    if (topic === CONFIG.TOPICS.BAN_LIVE_SYNC) {
      try {
        const payload = JSON.parse(text);
        const { timestamp, auth, record } = payload;

        if (!timestamp || !auth || !record) return;

        if (Math.abs(Date.now() - timestamp) > 60000) {
          console.warn('🔒 [Ban-Live-Sync] Отклонен устаревший бродкаст');
          return;
        }

        const expectedAuth = generateAuthToken(timestamp, CONFIG.SECURITY.clusterSecret);
        if (auth !== expectedAuth) {
          console.error('🔒 [Ban-Live-Sync] КРИТИЧЕСКАЯ ОШИБКА: Неверная подпись кластера!');
          return;
        }

        mergeBanRecords([record]);
        console.log(`⚡ [Ban-Live-Sync] Синхронизировано изменение бана: ${record.user_id?.slice(-12)} -> ${record.status}`);
      } catch (err) {
        console.error('❌ [Ban-Live-Sync] Ошибка обработки бродкаста:', err.message);
      }
      return;
    }

    // ==========================================
    // 🔥 ОБРАБОТКА ОБНОВЛЕНИЙ ПРОФИЛЕЙ (Аватарки)
    // ==========================================
    if (topic === CONFIG.TOPICS.PROFILE_UPDATES_TOPIC) {
      try {
        const payload = JSON.parse(text);
        // Иначе игнорируются клиенты, которые обновили только никнейм.
        if (payload.type === CONFIG.MSG.PROFILE_UPDATED) {

          // 1. ЗАПИСЫВАЕМ СВЯЗКУ PEER_ID -> ADDRESS В ГЛОБАЛЬНЫЙ РЕЕСТР
          if (globalRegistryDb && payload.senderId && payload.profileDbAddress) {
            await globalRegistryDb.put(payload.senderId, payload.profileDbAddress);
            console.log(`📇 [Registry] Записан профиль: ${payload.senderId.slice(-12)} -> ${payload.profileDbAddress}`);
          }

          // 2. ПИННИНГ БАЗЫ ДАННЫХ ПРОФИЛЯ (Используем archivistService вместо прямого orbitdb.open)
          if (payload.profileDbAddress && archivistService) {
            console.log(`🗄️ [PubSub] Обнаружена БД профиля: ${payload.profileDbAddress}. Начинаем синхронизацию...`);
            
            archivistService.pinRoom(payload.profileDbAddress).then(() => {
              console.log(`✅ [Кэш] База профиля ${payload.profileDbAddress} успешно открыта (запинена) через Архивариус!`);
            }).catch(err => {
              console.error(`❌ Ошибка открытия БД профиля через Архивариус:`, err.message);
            });
          } else if (payload.profileDbAddress && !archivistService) {
            console.warn(`⚠️ [PubSub] archivistService не передан в обработчики, пиннинг БД ${payload.profileDbAddress} невозможен!`);
          }
        }
      } catch (err) {
        console.error('❌ Ошибка обработки профиля:', err);
      }
      return; // Важно выйти, чтобы не дублировать логи ниже
    }

        // Обычные сообщения (чат)
    if (topic.includes('/orbitdb/')) {
      console.log(`📩 [${topic}] Скрытое сообщение от ${from.toString().slice(-12)}: ${text}`);
      return;
    }
    // Обычные сообщения (чат)
    if (!topic.includes('sync')) {
      console.log(`📩 [${topic}] Сообщение от ${from.toString().slice(-12)}: ${text}`);
      return;
    }

  });
}

// ==========================================
// ИНИЦИАЦИЯ ЗАПРОСА СИНХРОНИЗАЦИИ (Outgoing Request)
// ==========================================
export async function requestPeerSync(node, pubsub, orbitdb = null) {
  const myPeerId = node.peerId.toString();
  const responseTopic = `${CONFIG.TOPICS.PEER_SYNC_RESPONSE_BASE}`;
  let received = false;

  const myOrbitDbIdentity = orbitdb ? orbitdb.identity.id : null;

  const onResponse = async (evt) => {
    const msg = evt.detail || evt;
    if (msg.topic !== responseTopic) return;
    try {
      const payload = JSON.parse(new TextDecoder().decode(msg.data));
      
      const { timestamp, auth, relays, globalRegistryAddress } = payload;
      if (!timestamp || !auth || !relays) return;

      if (Math.abs(Date.now() - timestamp) > CONFIG.LIVE_STAMP_TOKEN) {
        console.warn(`🔒 [PEER-SYNC] Ответ от сервера проигнорирован: устаревший таймстамп.`);
        return;
      }

      const expectedAuth = generateAuthToken(timestamp, CONFIG.SECURITY.clusterSecret);
      if (auth !== expectedAuth) {
        console.error(`🔒 [PEER-SYNC] КРИТИЧЕСКОЕ ПРЕДУПРЕЖДЕНИЕ: Ответ на синхронизацию пришел от сервера с неверным CLUSTER_SECRET!`);
        return;
      }

      // Если мы еще не знаем адрес глобальной БД, сохраняем тот, что прислал соседний релей
      if (globalRegistryAddress && !CONFIG.GLOBAL_REGISTRY_ADDRESS) {
        CONFIG.GLOBAL_REGISTRY_ADDRESS = globalRegistryAddress;
        console.log(`📥 [PEER-SYNC] Получен адрес глобального реестра от соседа: ${globalRegistryAddress}`);
      }

      // Если сервер подтвердил, что знает секрет — берем его пиры
      if (payload.relays) {
        const localConfig = loadKnownPeersConfig();
        payload.relays.forEach(remoteRelay => {
          if (!localConfig.relays.find(r => r.peerId === remoteRelay.peerId)) {
            localConfig.relays.push(remoteRelay);
            const fullAddr = `${remoteRelay.address}/p2p/${remoteRelay.peerId}`;

            if (r.peerId === pubsub.libp2p.peerId.toString()) {
              return; // Пропускаем самого себя
            }

            node.dial(multiaddr(fullAddr)).catch(err => {
              console.log(`📡 Не удалось достучаться до ${remoteRelay.name}: ${err.message}`);
            });
          }
        });
        saveKnownPeersConfig(localConfig);
        console.log(`📥 [PEER-SYNC] Список узлов успешно обновлен и верифицирован. Всего: ${localConfig.relays.length}`);
        received = true;
      }
    } catch (e) {
      console.error('❌ Ошибка парсинга ответа синхронизации:', e.message);
    }
  };

  await pubsub.subscribe(responseTopic);
  pubsub.addEventListener('message', onResponse);

  const reqTimestamp = Date.now();
  const reqAuth = generateAuthToken(reqTimestamp, CONFIG.SECURITY.clusterSecret);

  const reqPayload = JSON.stringify({ 
    from: myPeerId,
    timestamp: reqTimestamp,
    auth: reqAuth,
    relay: {
      name: CONFIG.NODE_NAME,
      peerId: myPeerId,
      address: `/ip4/${CONFIG.NETWORK.IP}/tcp/${CONFIG.NETWORK.PORT}/ws`,
      orbitDbIdentity: myOrbitDbIdentity,
      // Сообщаем другим релеям, какой адрес глобальной БД используем мы
      globalRegistryAddress: CONFIG.GLOBAL_REGISTRY_ADDRESS
    }
  });

  try {
    await pubsub.publish(CONFIG.TOPICS.PEER_SYNC_REQUEST, new TextEncoder().encode(reqPayload));
    console.log(`🚀 [PEER-SYNC] Отправлен подписанный запрос на синхронизацию...`);
  } catch (err) {
    if (err.message !== 'PublishError.InsufficientPeers') throw err;
  }

  await new Promise(r => setTimeout(r, 5000));
  pubsub.removeEventListener('message', onResponse);
  return received;
}