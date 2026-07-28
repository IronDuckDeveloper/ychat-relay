import './errors.js';
import { createOrbitDB, Identities } from '@orbitdb/core';
import { CONFIG } from './config.js';
import { createRelayNode } from './networking/node.js';
import { ArchivistService } from '../services/ArchivistService.js';
import { setupPubSubHandlers, requestPeerSync } from './pubsub/handlers.js';
import { setupDatabaseSyncProtocol, requestDatabaseSync } from './networking/dbSync.js';
import { safeSubscribe } from './pubsub/subscription.js';
import { initDatabase } from './database/db.js';
import { setupAntiFloodProtocol, registerAnnounceProtocol } from './networking/protocols.js';

async function main() {
  // Сначала поднимаем базу данных
  initDatabase();
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

  const timeoutMs = CONFIG.INACTIVITY_TIMEOUT_MS || 20 * 60 * 1000;
  // Передаем node и bootstrapList последними аргументами
  const archivist = new ArchivistService(heliaInstance, orbitdb, timeoutMs, node, bootstrapList);
  // Запускаем внутренний мониторинг комнат
  archivist.startMonitoring();

  // Включаем защиту
  setupAntiFloodProtocol(node, pubsub, archivist);
  // Подключаем слушатель запросов на БД
  setupDatabaseSyncProtocol(node);

  // ==========================================
  // НАСТРОЙКА GRACEFUL SHUTDOWN (БЕЗОПАСНОЕ ВЫКЛЮЧЕНИЕ)
  // ==========================================
  async function gracefulShutdown() { 
    console.log('\n🛑 [Релей] Получен сигнал выключения. Закрываем базы данных и останавливаем сетевые узлы...');
    try { 
      if (archivist) {
        archivist.stop();
      }
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
  setupPubSubHandlers(node, pubsub, archivist, orbitdb);

  // 4. Прямой анонс через кастомный протокол (/p2p-relay/v1/announce)
  registerAnnounceProtocol(node, archivist, pendingRequests);

  // 5. Подписка на системные топики управления
  await safeSubscribe(pubsub, CONFIG.TOPICS.PEER_SYNC_REQUEST);
  // 6. Подписка на топик живой синхронизации БД (для получения новых регистраций от других релеев)
  await safeSubscribe(pubsub, CONFIG.TOPICS.DB_LIVE_SYNC);
  // 7. Подписка на топик обновлений профилей (для обмена данными клиентами)
  await safeSubscribe(pubsub, CONFIG.TOPICS.PROFILE_UPDATES_TOPIC);

  console.log('🔗 PeerID:', node.peerId.toString());
  console.log('🚀 SERVER READY (ARCHIVIST MODE)');

  // 7. Обработчик подключения новых пиров (Синхронизация метаданных сети)
  node.addEventListener('peer:connect', async (evt) => {
    // Используем одно имя переменной для удобства
    const remotePeerId = evt.detail.toString(); 
    
    // Проверяем, есть ли этот пир в списке доверенных серверов-релеев
    const isRelay = bootstrapList.some(addr => addr.includes(remotePeerId));

    if (isRelay) {
      console.log(`🤝 Подключен другой Релей: ${remotePeerId.slice(-6)}`);
      
      // Запрашиваем базу только 1 раз при старте
      if (!syncCompleted) {
        syncCompleted = true;
        await new Promise(r => setTimeout(r, 2000)); // Ждем обмен IDENTIFY

        // Ищем конкретное соединение с этим релеем, чтобы достать его Multiaddr
        const connections = node.getConnections();
        const targetConnection = connections.find(conn => conn.remotePeer.toString() === remotePeerId);

        if (targetConnection) {
          const targetAddr = targetConnection.remoteAddr.toString(); // Вот она, нужная строка!
          console.log(`🔄 Запрашиваю синхронизацию базы данных у релея: ${remotePeerId.slice(-6)}...`);
          
          await requestDatabaseSync(node, targetAddr);
        } else {
          console.log(`⚠️ [DB-SYNC] Соединение с релеем ${remotePeerId.slice(-6)} потеряно до начала синхронизации.`);
        }
      }
    } else {
      // Это обычный клиент (браузер). Никаких запросов БД!
      console.log(`🤝 Подключен клиент: ${remotePeerId.slice(-6)}.`);
    }
  });
}

// Запуск приложения
main().catch(console.error);