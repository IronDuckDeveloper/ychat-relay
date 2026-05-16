import { multiaddr } from '@multiformats/multiaddr';
import { CONFIG } from './config.js';
import { loadStoredRooms, saveRooms } from './storage.js';
import { createRelayNode } from './network.js';
import { 
  subscribedTopics, 
  safeSubscribe, 
  setupPubSubHandlers, 
  requestSyncViaPubSub, 
  requestPeerSync 
} from './pubsub.js';

async function main() {
  // 1. Инициализация ноды
  const { heliaInstance, bootstrapList } = await createRelayNode();
  const node = heliaInstance.libp2p;
  const pubsub = node.services.pubsub;

  let syncCompleted = false;

  // 2. Настройка обработчиков PubSub
  setupPubSubHandlers(node, pubsub);

  // 3. Прямой анонс (/p2p-relay/v1/announce)
  await node.handle('/p2p-relay/v1/announce', async ({ stream }) => {
    try {
      const { pipe } = await import('it-pipe');
      await pipe(
        stream,
        async function (source) {
          for await (const buf of source) {
            const decoded = new TextDecoder().decode(buf.subarray());
            try {
              const roomName = JSON.parse(decoded);
              if (roomName && typeof roomName === 'string') {
                console.log(`🏠 [Protocol] Прямой запрос подписки на: ${roomName}`);
                await safeSubscribe(pubsub, roomName);
              }
            } catch (e) {
              await safeSubscribe(pubsub, decoded);
            }
          }
        }
      );
    } catch (err) {
      console.error(`❌ [Protocol] Ошибка стрима: ${err.message}`);
    }
  });

  // 4. Подписка на системные топики и восстановление сохраненного
  await safeSubscribe(pubsub, CONFIG.TOPICS.ANNOUNCE);
  await safeSubscribe(pubsub, CONFIG.TOPICS.SYNC_REQUEST);
  await safeSubscribe(pubsub, CONFIG.TOPICS.PEER_SYNC_REQUEST);

  const savedRooms = loadStoredRooms();
  for (const r of savedRooms) {
    await safeSubscribe(pubsub, r);
  }

  console.log('🔗 PeerID:', node.peerId.toString());
  console.log('🚀 SERVER READY');

  // 5. Обработчик подключения новых пиров (Синхронизация)
  node.addEventListener('peer:connect', async (evt) => {
    const peerId = evt.detail.toString();
    console.log(`🤝 Подключен пир: ${peerId.slice(-6)}`);
    
    if (!syncCompleted) {
      await new Promise(r => setTimeout(r, 2000)); // Ждем обмен IDENTIFY

      console.log('🔄 [SYNC] Начинаю синхронизацию пиров...');
      await requestPeerSync(node, pubsub);

      const ok = await requestSyncViaPubSub(node, pubsub);
      if (ok) {
        syncCompleted = true;
        const toSave = Array.from(subscribedTopics).filter(t => 
          t !== CONFIG.TOPICS.ANNOUNCE && !t.includes('sync')
        );
        saveRooms(toSave);
      }
    }
  });

  // 6. Поддержание сети (Форсированный коннект)
  setInterval(async () => {
    if (node.getPeers().length === 0 && bootstrapList.length > 0) {
      try {
        await node.dial(multiaddr(bootstrapList));
        console.log('🛠 [MAINTENANCE] Форсированный dial к соседу...');
      } catch (e) {
        // Ошибки здесь обычно спамят в консоль, лучше оставить их тихими
      }
    }
  }, 15000);

  // 7. Мониторинг
  setInterval(() => {
    console.log(`\n📊 Статус: Пиров: ${node.getPeers().length} | Топиков: ${pubsub.getTopics().length} | Синх: ${syncCompleted ? '✅' : '⏳'}`);
  }, 10000);
}

// Запуск приложения
main().catch(console.error);