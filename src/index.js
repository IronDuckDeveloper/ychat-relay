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
import { initDatabase } from './networking/db.js';
import { setupAntiFloodProtocol } from './networking/rpcHandler.js';

// 🛡️ ГЛОБАЛЬНЫЙ ФИЛЬТР МУСОРА ОТ ORBITDB И P2P
process.on('unhandledRejection', (reason) => {
  const errName = reason?.name || '';
  const errCode = reason?.code || '';
  const msg = reason?.message || '';

  // 1. Игнорируем ошибки отсутствия данных
  if (errCode === 'ERR_NOT_FOUND' || errName === 'NotFoundError' || errName === 'AbortError') return;
  
  // 2. Игнорируем рассинхрон протоколов (наш ERR_UNSUPPORTED_PROTOCOL)
  if (errCode === 'ERR_UNSUPPORTED_PROTOCOL' || msg.includes('protocol selection failed')) return;

  // 3. Игнорируем обрывы связи при отключении клиентов
  if (msg.includes('stream reset') || msg.includes('The operation was aborted') || msg.includes('unexpected end of input')) return;

  // Все остальные реальные ошибки выводим
  console.error('❌ Неперехваченная ошибка промиса:', reason);
});

process.on('uncaughtException', (err) => {
  if (err.name === 'StreamResetError' || err.code === 'ERR_STREAM_RESET') {
    console.log('⚠️ [Network] Игнорируем обрыв P2P-стрима (клиент отключился)');
    return;
  }
  console.error('🔥 КРИТИЧЕСКАЯ ОШИБКА:', err);
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

let intervalTopology;
let intervalMonitor;

async function main() {
  // Сначала поднимаем базу данных
  initDatabase();
  // 1. Инициализация сетевой ноды (Helia + libp2p)
  const { heliaInstance, bootstrapList } = await createRelayNode();
  const node = heliaInstance.libp2p;
  const pubsub = node.services.pubsub;
  // Включаем защиту
  setupAntiFloodProtocol(node);

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
  const timeoutMs = CONFIG.INACTIVITY_TIMEOUT_MS || 20 * 60 * 1000;
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
                  if (now - lastSeen < 3000) return; 
                }

                pendingRequests.set(targetAddress, now);

                console.log(`🏠 [Protocol] Запрос на архивацию БД: ${targetAddress}`);
                
                // ВАЖНО: Никаких await. База SQLite открывается в фоне,
                // стрим мгновенно освобождается, клиент не блокируется.
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

  // 5. Подписка на системные топики управления
  await safeSubscribe(pubsub, CONFIG.TOPICS.PEER_SYNC_REQUEST);

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
// Исправленный безопасный вариант:
intervalTopology = setInterval(async () => {
  if (node.getPeers().length === 0 && bootstrapList.length > 0) {
    try {
      // Берем первый адрес из списка соседей
      const targetAddress = bootstrapList[0]; 
      await node.dial(multiaddr(targetAddress));
      console.log('🛠 [MAINTENANCE] Сеть упала до 0 пиров. Форсированный dial к соседу...');
    } catch (e) {
      // Тихо гасим ошибки, чтобы не спамить в консоль, если сосед тоже лежит
    }
  }
}, 15000);

 // 9. Мониторинг ресурсов и аптайма баз
  intervalMonitor = setInterval(() => {
    if (!archivist || !archivist.activeRooms) return; 

    // Получаем список PeerID всех бутстрап-релеев, чтобы отсечь их
    const bootstrapPeerIds = bootstrapList.map(addr => {
      try { return multiaddr(addr).getPeerId(); } catch { return null; }
    }).filter(Boolean);

    for (const roomAddress of archivist.activeRooms.keys()) {
      const allSubscribers = node.services.pubsub.getSubscribers(roomAddress); 

      // Фильтруем: оставляем только тех, чьих PeerID НЕТ в списке бутстрап-соседей
      const clientSubscribers = allSubscribers.filter(peer => 
        !bootstrapPeerIds.includes(peer.toString())
      );

      // Теперь проверяем именно ЖИВЫХ КЛИЕНТОВ (браузеры)
      if (clientSubscribers.length === 0) {
        if (!archivist.roomTimers.has(roomAddress)) {
          archivist.startDestructionTimer(roomAddress);
        }
      } else {
        if (archivist.roomTimers.has(roomAddress)) {
          clearTimeout(archivist.roomTimers.get(roomAddress));
          archivist.roomTimers.delete(roomAddress);
          console.log(`\n📈 [Архивариус] Клиенты вернулись в ${roomAddress.slice(-12)}. Таймер отменен.`);
        }
      }
    }
    
    // Сводный лог статуса
    const totalPeers = node.getPeers().length;
    const activeCount = archivist.activeRooms.size;
    const timersCount = archivist.roomTimers.size;
    console.log(`📊 Сеть: Пиров=${totalPeers} | Комнат=${activeCount} | На удаление⏳=${timersCount} | Синх=${syncCompleted ? '✅' : '⏳'}`);

  }, 15000); // 15 секунд — оптимальный интервал для теста
}

// Запуск приложения
main().catch(console.error);