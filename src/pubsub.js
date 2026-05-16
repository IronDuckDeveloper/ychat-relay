import { multiaddr } from '@multiformats/multiaddr';
import { CONFIG } from './config.js';
import { loadKnownPeersConfig, saveKnownPeersConfig } from './storage.js';

export const subscribedTopics = new Set();

export async function safeSubscribe(pubsub, room) {
  if (!room || subscribedTopics.has(room)) return;
  try {
    await pubsub.subscribe(room);
    subscribedTopics.add(room);
    console.log(`🎯 [TOPIC] Подписан на: ${room}`);
  } catch (e) {
    console.error(`❌ Ошибка подписки на ${room}:`, e.message);
  }
}

export function setupPubSubHandlers(node, pubsub) {
  pubsub.addEventListener('message', async (evt) => {
    const msg = evt.detail || evt;
    const { topic, data, from } = msg;
    let text = '';
    try { text = new TextDecoder().decode(data); } catch (e) { return; }

    // 1. Обычные сообщения (чат)
    if (topic !== CONFIG.TOPICS.ANNOUNCE && !topic.includes('sync')) {
      console.log(`📩 [${topic}] Сообщение от ${from.toString().slice(-6)}: ${text}`);
      return;
    }

    // 2. Анонсы комнат от браузеров
    if (topic === CONFIG.TOPICS.ANNOUNCE) {
      try {
        const { room } = JSON.parse(text);
        if (room) await safeSubscribe(pubsub, room);
      } catch (e) {}
    }

    // 3. Запрос списка комнат (Синхронизация)
    if (topic === CONFIG.TOPICS.SYNC_REQUEST) {
      try {
        const payload = JSON.parse(text);
        const target = payload?.from;
        if (!target || target === node.peerId.toString()) return;

        const rooms = Array.from(subscribedTopics).filter(t => 
          t !== CONFIG.TOPICS.ANNOUNCE && !t.includes('sync')
        );
        const responseTopic = `${CONFIG.TOPICS.SYNC_RESPONSE_BASE}${target}`;
        
        await pubsub.publish(responseTopic, new TextEncoder().encode(JSON.stringify({ rooms })));
        console.log(`📤 [SYNC] Отправлен список (${rooms.length} комнат) пиру ${target.slice(-6)}`);
      } catch (e) {}
    }

    // 4. Запрос обмена пирами (Known Peers)
    if (topic === CONFIG.TOPICS.PEER_SYNC_REQUEST) {
      try {
        const payload = JSON.parse(text);
        const target = payload?.from;
        if (!target || target === node.peerId.toString()) return;

        if (payload.relay) {
          const current = loadKnownPeersConfig();
          const exists = current.relays.find(r => r.peerId === payload.relay.peerId);
          if (!exists) {
            current.relays.push(payload.relay);
            saveKnownPeersConfig(current);
            console.log(`🆕 [PEER-SYNC] Добавлен новый узел: ${payload.relay.name}`);
          }
        }

        const config = loadKnownPeersConfig();
        const responseTopic = `${CONFIG.TOPICS.PEER_SYNC_RESPONSE_BASE}${target}`;
        await pubsub.publish(responseTopic, new TextEncoder().encode(JSON.stringify({ relays: config.relays })));
        console.log(`📤 [PEER-SYNC] Отправлен список пиров для ${target.slice(-6)}`);
      } catch (e) {
        console.error('❌ Ошибка в PEER_SYNC_REQUEST:', e.message);
      }
    }
  });
}

export async function requestSyncViaPubSub(node, pubsub) {
  const myPeerId = node.peerId.toString();
  const responseTopic = `${CONFIG.TOPICS.SYNC_RESPONSE_BASE}${myPeerId}`;
  let received = false;

  const onResponse = async (evt) => {
    const msg = evt.detail || evt;
    if (msg.topic !== responseTopic) return;
    try {
      const payload = JSON.parse(new TextDecoder().decode(msg.data));
      if (payload?.rooms) {
        console.log(`📥 [SYNC] Получено комнат: ${payload.rooms.length}`);
        for (const room of payload.rooms) {
          await safeSubscribe(pubsub, room);
        }
        received = true;
      }
    } catch (e) {}
  };

  await pubsub.subscribe(responseTopic);
  pubsub.addEventListener('message', onResponse);

  console.log('📢 [SYNC] Запрашиваю список комнат у соседей...');
  await pubsub.publish(CONFIG.TOPICS.SYNC_REQUEST, new TextEncoder().encode(JSON.stringify({ from: myPeerId })));

  await new Promise(r => setTimeout(r, 7000));
  pubsub.removeEventListener('message', onResponse);
  return received;
}

export async function requestPeerSync(node, pubsub) {
  const myPeerId = node.peerId.toString();
  const responseTopic = `${CONFIG.TOPICS.PEER_SYNC_RESPONSE_BASE}${myPeerId}`;
  let received = false;

  const onResponse = async (evt) => {
    const msg = evt.detail || evt;
    if (msg.topic !== responseTopic) return;
    try {
      const payload = JSON.parse(new TextDecoder().decode(msg.data));
      if (payload?.relays) {
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
        console.log(`📥 [PEER-SYNC] Список узлов обновлен. Всего: ${localConfig.relays.length}`);
        received = true;
      }
    } catch (e) {}
  };

  await pubsub.subscribe(responseTopic);
  pubsub.addEventListener('message', onResponse);

  const reqPayload = JSON.stringify({ 
    from: myPeerId,
    relay: {
      name: CONFIG.NODE_NAME,
      peerId: myPeerId,
      address: `/ip4/${CONFIG.MY_PUBLIC_IP}/tcp/15002/ws`
    }
  });
  
  await pubsub.publish(CONFIG.TOPICS.PEER_SYNC_REQUEST, new TextEncoder().encode(reqPayload));

  await new Promise(r => setTimeout(r, 5000));
  pubsub.removeEventListener('message', onResponse);
  return received;
}