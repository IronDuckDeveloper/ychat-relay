
import crypto from 'crypto'; // Встроенный модуль Node.js, ничего доустанавливать не надо
import { multiaddr } from '@multiformats/multiaddr';
import { peerIdFromString } from '@libp2p/peer-id'; // Убедись, что импорт совпадает с твоим проектом
import { CONFIG } from '../config.js';
import { loadKnownPeersConfig, saveKnownPeersConfig } from '../storage/peers-config.js';
import { generateAuthToken } from '../utils/crypto.js';
import { mergeRegistrations } from '../database/db.js'; // <-- Добавь эту строку

export function setupPubSubHandlers(node, pubsub) {
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

        // 🛡️ ПРОВЕРКА БЕЗОПАСНОСТИ КЛУБА
        const { timestamp, auth } = payload;
        if (!timestamp || !auth) {
          console.warn(`🔒 [PEER-SYNC] Отклонено от ${target.slice(-6)}: отсутствует авторизация.`);
          return;
        }

        // Проверяем «свежесть» токена (защита от повтора старых пакетов)
        const timeDifference = Math.abs(Date.now() - timestamp);
        if (timeDifference > CONFIG.LIVE_STAMP_TOKEN) { // Токен живет ровно 1 минуту
          console.warn(`🔒 [PEER-SYNC] Отклонено от ${target.slice(-6)}: токен устарел (${timeDifference}ms)`);
          return;
        }

        // Пересчитываем хэш с локальным секретом
        const expectedAuth = generateAuthToken(timestamp, CONFIG.SECURITY.clusterSecret);
        if (auth !== expectedAuth) {
          console.warn(`🔒 [PEER-SYNC] КРИТИЧЕСКАЯ ОШИБКА АВТОРИЗАЦИИ: Узел ${target.slice(-6)} прислал неверный токен!`);
          return; // Пароли не совпали, игнорируем узел полностью
        }

        // Если проверка пройдена — узел легитимен, работаем с базой
        if (payload.relay) {
          const current = loadKnownPeersConfig();

          // Ищем дубликат: совпадает либо PeerID, либо IP-адрес
          const existingIndex = current.relays.findIndex(r => 
            r.peerId === payload.relay.peerId || r.address === payload.relay.address
          );

          // Узел найден. Проверяем, изменились ли данные
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
        }

        // Отправляем ответ, также защищая его подписью, чтобы запрашивающий знал, что мы тоже "свои"
        const config = loadKnownPeersConfig();
        const responseTopic = `${CONFIG.TOPICS.PEER_SYNC_RESPONSE_BASE}${target}`;
        
        const resTimestamp = Date.now();
        const resAuth = generateAuthToken(resTimestamp, CONFIG.SECURITY.clusterSecret);

        const responsePayload = JSON.stringify({ 
          relays: config.relays,
          timestamp: resTimestamp,
          auth: resAuth
        });

        await pubsub.publish(responseTopic, new TextEncoder().encode(responsePayload));
        console.log(`📤 [PEER-SYNC] Отправлен подписанный список пиров для ${target.slice(-6)}`);
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
        console.log(`⚡ [LIVE-SYNC] Новая регистрация синхронизирована (БД: ${record.profile_address?.slice(0, 10)}...)`);
      } catch (err) {
        console.error('❌ [LIVE-SYNC] Ошибка обработки бродкаста:', err.message);
      }
      return; // Важно выйти, чтобы сообщение не обрабатывалось дальше
    }

        // Обычные сообщения (чат)
    if (topic.includes('/orbitdb/')) {
      console.log(`📩 [${topic}] Скрытое сообщение от ${from.toString().slice(-6)}: ${text}`);
      return;
    }
    // Обычные сообщения (чат)
    if (!topic.includes('sync')) {
      console.log(`📩 [${topic}] Сообщение от ${from.toString().slice(-6)}: ${text}`);
      return;
    }

  });
}

// ==========================================
// ИНИЦИАЦИЯ ЗАПРОСА СИНХРОНИЗАЦИИ (Outgoing Request)
// ==========================================
export async function requestPeerSync(node, pubsub) {
  const myPeerId = node.peerId.toString();
  const responseTopic = `${CONFIG.TOPICS.PEER_SYNC_RESPONSE_BASE}${myPeerId}`;
  let received = false;

  const onResponse = async (evt) => {
    const msg = evt.detail || evt;
    if (msg.topic !== responseTopic) return;
    try {
      const payload = JSON.parse(new TextDecoder().decode(msg.data));
      
      // 🛡️ ПРОВЕРКА БЕЗОПАСНОСТИ ОТВЕТА СЕРВЕРА
      const { timestamp, auth, relays } = payload;
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

      // Если сервер подтвердил, что знает секрет — берем его пиры
      if (payload.relays) {
        const localConfig = loadKnownPeersConfig();
        payload.relays.forEach(remoteRelay => {
          if (!localConfig.relays.find(r => r.peerId === remoteRelay.peerId)) {
            localConfig.relays.push(remoteRelay);
            const fullAddr = `${remoteRelay.address}/p2p/${remoteRelay.peerId}`;
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

  // Генерируем подпись нашего запроса
  const reqTimestamp = Date.now();
  const reqAuth = generateAuthToken(reqTimestamp, CONFIG.SECURITY.clusterSecret);

  const reqPayload = JSON.stringify({ 
    from: myPeerId,
    timestamp: reqTimestamp,
    auth: reqAuth, // Отправляем хэш вместо чистого пароля
    relay: {
      name: CONFIG.NODE_NAME,
      peerId: myPeerId,
      address: `/ip4/${CONFIG.NETWORK.IP}/tcp/${CONFIG.NETWORK.PORT}/ws`
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