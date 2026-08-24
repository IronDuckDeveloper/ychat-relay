import './errors.js';
import { createOrbitDB, Identities, OrbitDBAccessController } from '@orbitdb/core';
import { CONFIG } from './config.js';
import { createRelayNode } from './networking/node.js';
import { ArchivistService } from '../services/ArchivistService.js';
import { setupPubSubHandlers, requestPeerSync } from './pubsub/handlers.js';
import { setupDatabaseSyncProtocol, requestDatabaseSync } from './networking/dbSync.js';
import { safeSubscribe } from './pubsub/subscription.js';
import { initDatabase } from './database/db.js';
import { setupAntiFloodProtocol, registerAnnounceProtocol } from './networking/protocols.js';
import express from 'express';
import { createCheckUploadHandler } from './routes/checkUpload.js';

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

  // =========================================================================
  // 🔥 ГЛОБАЛЬНЫЙ ПУЛ И ДЕДУПЛИКАЦИЯ ORBITDB.OPEN (In-Flight Deduplication)
  // Предотвращает LEVEL_LOCKED при параллельном обращении Registry, PubSub и Архивариуса
  // =========================================================================
  const openDBInstances = new Map();
  const inFlightOpenPromises = new Map();
  const originalOrbitDbOpen = orbitdb.open.bind(orbitdb);

  orbitdb.open = async function(address, options = {}) {
    if (!address) throw new Error('Address is required for orbitdb.open');

    // 1. Если база уже открыта, возвращаем готовый экземпляр
    if (openDBInstances.has(address)) {
      console.log(`♻️ [DB-Pool] База уже открыта, возвращаем из пула: ${address.slice?.(-16) || address}`);
      return openDBInstances.get(address);
    }

    // 2. Если база прямо сейчас открывается другим процессом, ждем этот же промис (Race Condition Fix)
    if (inFlightOpenPromises.has(address)) {
      console.log(`⏳ [DB-Pool] База в процессе открытия, ожидаем In-Flight промис: ${address.slice?.(-16) || address}`);
      return inFlightOpenPromises.get(address);
    }

    // 3. Запускаем открытие и регистрируем In-Flight промис
    const openPromise = (async () => {
      try {
        const db = await originalOrbitDbOpen(address, options);
        openDBInstances.set(address, db);

        // Перехватываем close(), чтобы корректно удалять базу из кэша при закрытии
        const originalClose = db.close.bind(db);
        db.close = async function() {
          openDBInstances.delete(address);
          return originalClose();
        };

        return db;
      } finally {
        // Очищаем In-Flight промис после завершения (успешного или с ошибкой)
        inFlightOpenPromises.delete(address);
      }
    })();

    inFlightOpenPromises.set(address, openPromise);
    return openPromise;
  };

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
      if (globalRegistryDb) {
        await globalRegistryDb.close();
        console.log('✅ [Релей] Глобальный реестр закрыт.');
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

  // Подписка на системные топики управления (ДО подключения пиров)
  await safeSubscribe(pubsub, CONFIG.TOPICS.PEER_SYNC_REQUEST);
  await safeSubscribe(pubsub, CONFIG.TOPICS.DB_LIVE_SYNC);
  await safeSubscribe(pubsub, CONFIG.TOPICS.PROFILE_UPDATES_TOPIC);

  let syncCompleted = false;

  // 1. Регистрируем обработчик подключений ДО вызова dial()
  node.addEventListener('peer:connect', async (evt) => {
    const remotePeerId = evt.detail.toString(); 
    
    // Проверяем, есть ли этот пир в списке доверенных серверов-релеев
    const isRelay = bootstrapList.some(addr => addr.includes(remotePeerId));

    if (isRelay) {
      console.log(`🤝 Подключен другой Релей: ${remotePeerId.slice(-12)}`);
      
      // Запрашиваем базу только 1 раз при старте
      if (!syncCompleted) {
        syncCompleted = true;
        await new Promise(r => setTimeout(r, 1000)); // Ждем обмен IDENTIFY

        // Синхронизируем пиры и ОБЯЗАТЕЛЬНО передаем orbitdb, 
        // чтобы отправить наш OrbitID другим релеям для получения прав на запись в Registry!
        console.log('🔄 [SYNC] Начинаю синхронизацию пиров и обмен OrbitID...');
        await requestPeerSync(node, pubsub, orbitdb);

        // Ищем конкретное соединение с этим релеем, чтобы достать его Multiaddr
        const connections = node.getConnections();
        const targetConnection = connections.find(conn => conn.remotePeer.toString() === remotePeerId);

        if (targetConnection) {
          const targetAddr = targetConnection.remoteAddr.toString();
          console.log(`🔄 Запрашиваю синхронизацию базы данных у релея: ${remotePeerId.slice(-12)}...`);
          
          await requestDatabaseSync(node, targetAddr);
        } else {
          console.log(`⚠️ [DB-SYNC] Соединение с релеем ${remotePeerId.slice(-12)} потеряно до начала синхронизации.`);
        }
      }
    } else {
      // Это обычный клиент (браузер). Никаких запросов БД!
      console.log(`🤝 Подключен клиент: ${remotePeerId.slice(-12)}.`);
    }
  });

  // 2. Если есть соседи в bootstrapList, подключаемся к ним и ждем, пока peer:connect получит адрес
  if (bootstrapList && bootstrapList.length > 0) {
    console.log('📡 [Startup] Пробуем связаться с соседями для получения адреса БД...');
    for (const addr of bootstrapList) {
      try {
        await node.dial(addr);
      } catch (e) {
        // Игнорируем ошибки недоступности отдельных пиров
      }
    }
    // Ожидаем 2.5 сек, чтобы сработал peer:connect и заполнился CONFIG.GLOBAL_REGISTRY_ADDRESS
    await new Promise(r => setTimeout(r, 2500));
  }

  // =========================================================================
  // 🔥 ГЛОБАЛЬНЫЙ РЕЕСТР ПРОФИЛЕЙ (ТЕЛЕФОННАЯ КНИГА)
  // Если CONFIG.GLOBAL_REGISTRY_ADDRESS был получен от соседа — открываем его.
  // Если остался пустым — мы первые в сети, создаем новый!
  // =========================================================================
  const registryIdentifier = CONFIG.GLOBAL_REGISTRY_ADDRESS && CONFIG.GLOBAL_REGISTRY_ADDRESS.trim() !== ''
    ? CONFIG.GLOBAL_REGISTRY_ADDRESS
    : CONFIG.TOPICS.DB_GLOBAL_SYNC;

  console.log(`📇 [Registry] Открываем/создаем глобальный реестр: ${registryIdentifier}...`);
  
  let globalRegistryDb;

  if (registryIdentifier === CONFIG.TOPICS.DB_GLOBAL_SYNC) {
    console.log('👑 [Registry] Мы первые в сети! Создаем глобальную базу...');
    globalRegistryDb = await orbitdb.open(registryIdentifier, {
      type: 'keyvalue',
      AccessController: OrbitDBAccessController({
        type: 'orbitdb', // Обязательно для поддержки .grant() / .revoke()
        write: [orbitdb.identity.id] // Разрешаем запись текущему релею
      })
    });
    // Запоминаем созданный адрес
    CONFIG.GLOBAL_REGISTRY_ADDRESS = globalRegistryDb.address;
  } else {
    console.log(`🔗 [Registry] Подключаемся к существующей базе от соседа...`);
    globalRegistryDb = await orbitdb.open(registryIdentifier);
  }

  console.log(`✅ [Registry] Глобальная книга профилей готова! Реальный адрес: ${CONFIG.GLOBAL_REGISTRY_ADDRESS}`);
  // ==========================================

  // ==========================================
  // 🆕 HTTP-СЕРВЕР ДЛЯ NGINX AUTH_REQUEST
  // ==========================================
  const app = express();
  app.get('/api/check-upload', createCheckUploadHandler());

  app.listen(CONFIG.NETWORK.HTTP_PORT, () => {
    console.log(`🌐 [HTTP] Внутренний API-сервер слушает порт ${CONFIG.NETWORK.HTTP_PORT}`);
  });
  // ==========================================

  // 3. Настройка системных обработчиков PubSub и протоколов с уже открытой базой
  setupPubSubHandlers(node, pubsub, archivist, globalRegistryDb);

  // 4. Прямой анонс через кастомный протокол (/p2p-relay/v1/announce)
  registerAnnounceProtocol(node, archivist, pendingRequests);

  console.log('🔗 PeerID:', node.peerId.toString());
  console.log('🚀 SERVER READY (ARCHIVIST MODE)');
}

// Запуск приложения
main().catch(console.error);