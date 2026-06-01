import { multiaddr } from '@multiformats/multiaddr';
import { CONFIG } from '../config.js';
import { loadKnownPeersConfig, saveKnownPeersConfig } from '../storage/peers-config.js';

export function setupPubSubHandlers(node, pubsub) {
  pubsub.addEventListener('message', async (evt) => {
    const msg = evt.detail || evt;
    const { topic, data, from } = msg;
    let text = '';
    try { text = new TextDecoder().decode(data); } catch (e) { return; }

    if (topic.includes('/orbitdb/')) {
      console.log(`📩 [${topic}] Скрытое сообщение от ${from.toString().slice(-6)}: ${text}`);
        return;
    }
    // Обычные сообщения (чат)
    if (!topic.includes('sync')) {
      console.log(`📩 [${topic}] Сообщение от ${from.toString().slice(-6)}: ${text}`);
      return;
    }

    // Запрос обмена пирами (Known Peers)
if (topic === CONFIG.TOPICS.PEER_SYNC_REQUEST) {
      try {
        const payload = JSON.parse(text);
        const target = payload?.from;
        if (!target || target === node.peerId.toString()) return;

        if (payload.relay) {
          const current = loadKnownPeersConfig();
          
          // Ищем дубликат: совпадает либо PeerID, либо IP-адрес
          const existingIndex = current.relays.findIndex(r => 
            r.peerId === payload.relay.peerId || r.address === payload.relay.address
          );

          if (existingIndex !== -1) {
            // Узел найден. Проверяем, изменились ли данные
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

  try {
  await pubsub.publish(CONFIG.TOPICS.PEER_SYNC_REQUEST, new TextEncoder().encode(reqPayload));
} catch (err) {
  if (err.message !== 'PublishError.InsufficientPeers') throw err;
}
  

  await new Promise(r => setTimeout(r, 5000));
  pubsub.removeEventListener('message', onResponse);
  return received;
}