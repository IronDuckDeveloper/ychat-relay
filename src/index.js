import './errors.js';
import { createOrbitDB, Identities } from '@orbitdb/core';
import { CONFIG } from './config.js';
import { createRelayNode } from './networking/node.js';
import { ArchivistService } from '../services/ArchivistService.js';
import { setupPubSubHandlers, requestPeerSync } from './pubsub/handlers.js';
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
  // Передаем node и bootstrapList последними аргументами
  const archivist = new ArchivistService(heliaInstance, orbitdb, timeoutMs, node, bootstrapList);
  // Запускаем внутренний мониторинг комнат
  archivist.startMonitoring();

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
  setupPubSubHandlers(node, pubsub);

  // 4. Прямой анонс через кастомный протокол (/p2p-relay/v1/announce)
  registerAnnounceProtocol(node, archivist, pendingRequests);

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
}

// Запуск приложения
main().catch(console.error);