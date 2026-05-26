import { multiaddr } from '@multiformats/multiaddr';
import { createOrbitDB, Identities } from '@orbitdb/core';
import { CONFIG } from './config.js';
import { createRelayNode } from './network.js';
import { ArchivistService } from '../services/ArchivistService.js';
import { 
  safeSubscribe, 
  setupPubSubHandlers, 
  requestPeerSync 
} from './pubsub.js';

// 🛡️ ГЛОБАЛЬНЫЙ ФИЛЬТР СПАМА ОТ ORBITDB
process.on('unhandledRejection', (reason, promise) => {
  if (reason) {
    const errName = reason.name || '';
    const errCode = reason.code || '';
    
    // Игнорируем штатные ошибки "не найдено", выпадающие из фоновых задач OrbitDB
    if (errCode === 'ERR_NOT_FOUND' || errName === 'NotFoundError' || errName === 'AbortError') {
      return; // Молча гасим
    }
  }
  
  // Все остальные реальные ошибки выводим как обычно
  console.error('❌ Неперехваченная ошибка промиса:', reason);
});

process.on('uncaughtException', (err) => {
  const msg = err.message || '';
  if (err.name === 'StreamResetError' || err.code === 'ERR_STREAM_RESET' 

  ) {
    console.log('⚠️ [Network] Игнорируем обрыв P2P-стрима (клиент отключился)');
    return;
  }
  console.error('🔥 КРИТИЧЕСКАЯ ОШИБКА:', err);
});

process.on('unhandledRejection', (reason) => {
  const msg = reason?.message || '';
  if (msg.includes('stream reset') || msg.includes('The operation was aborted') || msg.includes('unexpected end of input')) {
    // Тихо игнорируем типичный мусор от отключающихся пиров
    return;
  }
  console.error('🔥 Необработанный отказ (Promise):', reason);
});

let intervalTopology;
let intervalMonitor;

async function main() {
  // 1. Инициализация сетевой ноды (Helia + libp2p)
  const { heliaInstance, bootstrapList } = await createRelayNode();
  const node = heliaInstance.libp2p;
  const pubsub = node.services.pubsub;

const pendingRequests = new Map();


  // Инициализация OrbitDB и сервиса Архивариуса поверх Helia
  const orbitdb = await createOrbitDB({ 
    ipfs: heliaInstance,
    directory: CONFIG.ORBITDB_DIR,
    id: heliaInstance.libp2p.peerId.toString(),
    storage: {
    // Настройки хранилища без внешних роутеров
    }
  });
  const timeoutMs = CONFIG.ARCHIVIST_TIMEOUT || 90 * 24 * 60 * 60 * 1000;
  const archivist = new ArchivistService(heliaInstance,orbitdb, timeoutMs);

  // ==========================================
  // НАСТРОЙКА GRACEFUL SHUTDOWN (БЕЗОПАСНОЕ ВЫКЛЮЧЕНИЕ)
  // ==========================================
  async function gracefulShutdown() { 
    console.log('\n🛑 [Релей] Получен сигнал выключения. Закрываем базы данных и останавливаем сетевые узлы...');
    clearInterval(intervalTopology);
    clearInterval(intervalMonitor);
    try { 
      if (orbitdb) { 
        await orbitdb.stop(); 
        console.log('✅ [Релей] OrbitDB успешно остановлен.'); 
      } 
      if (heliaInstance) { 
        await heliaInstance.stop(); 
        console.log('✅ [Релей] Helia успешно остановлена.'); 
      } 
    } catch (error) {
      console.error('❌ [Релей] Ошибка при корректном закрытии:', error); 
    } finally { 
      process.exit(0); 
    } 
  }

  // Перехватываем Ctrl+C и сигналы остановки Docker/системы
  process.on('SIGINT', gracefulShutdown); 
  process.on('SIGTERM', gracefulShutdown);
  // ==========================================

  let syncCompleted = false;

  // 3. Настройка системных обработчиков PubSub
  setupPubSubHandlers(node, pubsub);

  // 4. Прямой анонс через кастомный протокол (/p2p-relay/v1/announce)
  await node.handle('/p2p-relay/v1/announce', async ({ stream }) => {
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
                // Добавьте этот блок защиты (использует ваш Set 'pending')
                const now = Date.now();
                if (pendingRequests.has(targetAddress)) {
                  const lastSeen = pendingRequests.get(targetAddress);
                  if (now - lastSeen < 60000) return; // Игнорируем запрос, если был за последние 60 сек
                }

                pendingRequests.set(targetAddress, now);

                console.log(`🏠 [Protocol] Запрос на архивацию БД: ${targetAddress}`);
                archivist.pinRoom(targetAddress, remotePeerId);
              }
            } catch (e) {
              if (decoded) {
                console.log(`🏠 [Protocol] Запрос на архивацию БД (raw): ${decoded}`);
                archivist.pinRoom(decoded);
              }
            }
          }
        }
      );
    } catch (err) {
      console.error(`❌ [Protocol] Ошибка стрима: ${err.message}`);
    }
  });

  // 5. Подписка на системные топики управления
  await safeSubscribe(pubsub, CONFIG.TOPICS.ANNOUNCE);
  await safeSubscribe(pubsub, CONFIG.TOPICS.PEER_SYNC_REQUEST);
  
  if (CONFIG.TOPICS.ARCHIVIST) {
    await safeSubscribe(pubsub, CONFIG.TOPICS.ARCHIVIST);
    
    pubsub.addEventListener('message', async (evt) => {
      if (evt.detail.topic === CONFIG.TOPICS.ARCHIVIST) {
        const address = new TextDecoder().decode(evt.detail.data).trim();
        console.log(`📡 [PubSub System] Получен системный запрос на пиннинг: ${address}`);
        archivist.pinRoom(address);
      }
    });
  }

  console.log('🔗 PeerID:', node.peerId.toString());
  console.log('🚀 SERVER READY (ARCHIVIST MODE)');

  // 7. Обработчик подключения новых пиров (Синхронизация метаданных сети)
  node.addEventListener('peer:connect', async (evt) => {
    const peerId = evt.detail.toString();
    console.log(`🤝 Подключен пир: ${peerId.slice(-6)}`);
    
    if (!syncCompleted) {
      await new Promise(r => setTimeout(r, 2000)); // Ждем обмен IDENTIFY

      console.log('🔄 [SYNC] Начинаю синхронизацию пиров...');
      await requestPeerSync(node, pubsub);

      syncCompleted = true;
    }
  });

  // 8. Поддержание топологии сети (Форсированный коннект к бутстрапам)
  intervalTopology = setInterval(async () => {
    if (node.getPeers().length === 0 && bootstrapList.length > 0) {
      try {
        await node.dial(multiaddr(bootstrapList));
        console.log('🛠 [MAINTENANCE] Форсированный dial к соседу...');
      } catch (e) {
        // Тихо гасим ошибки подключения при отсутствии сети
      }
    }
  }, 15000);

  // 9. Мониторинг ресурсов и аптайма баз
  intervalMonitor = setInterval(() => {
    const activeCount = archivist.getPinnedRooms().length;
    console.log(`\n📊 Статус: Пиров: ${node.getPeers().length} | Активных БД на пине: ${activeCount} | Синх: ${syncCompleted ? '✅' : '⏳'}`);
  }, 10000);
}

// Запуск приложения
main().catch(console.error);