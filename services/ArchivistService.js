import { bootstrap } from '@libp2p/bootstrap';
import { createOrbitDB, OrbitDBAccessController } from '@orbitdb/core';
import { CONFIG } from '../src/config.js';
import { multiaddr } from '@multiformats/multiaddr';

export class ArchivistService {
  /**
   * @param {Object} orbitdbInstance - Экземпляр OrbitDB поверх Helia
   * @param {number} timeoutMs - Время неактивности в миллисекундах
   * @param {helia} helia - Экземпляр Helia
   */
  constructor(helia, orbitdbInstance, timeoutMs, node, bootstrapList) {
    this.helia = helia;
    this.orbitdb = orbitdbInstance;

    this.activeRooms = new Map();
    this.roomTimers = new Map();
    this.openingPromises = new Map();

    this.timeoutMs = timeoutMs;
    this.node = node;
    this.bootstrapList = bootstrapList || [];

    this.monitorInterval = null;

    console.log('🛡️ ArchivistService инициализирован');
  }

  /**
   * Запуск внутреннего цикла мониторинга ресурсов и аптайма баз
   */
  startMonitoring() {
    if (this.monitorInterval) return;

    this.monitorInterval = setInterval(() => {
      if (!this.activeRooms) return;

      // Получаем список PeerID всех бутстрап-релеев, чтобы отсечь их
      const bootstrapPeerIds = this.bootstrapList.map(addr => {
        try { 
          return multiaddr(addr).getPeerId(); 
        } catch { 
          return null; 
        }
      }).filter(Boolean);

      for (const roomAddress of this.activeRooms.keys()) {
        // Запрашиваем подписчиков напрямую через переданный node
        const allSubscribers = this.node.services.pubsub.getSubscribers(roomAddress);

        // Фильтруем: оставляем только тех, чьих PeerID НЕТ в списке бутстрап-соседей
        const clientSubscribers = allSubscribers.filter(peer => 
          !bootstrapPeerIds.includes(peer.toString())
        );

        // Проверяем именно ЖИВЫХ КЛИЕНТОВ (браузеры)
        if (clientSubscribers.length === 0) {
          if (!this.roomTimers.has(roomAddress)) {
            this.startDestructionTimer(roomAddress);
          }
        } else {
          if (this.roomTimers.has(roomAddress)) {
            clearTimeout(this.roomTimers.get(roomAddress));
            this.roomTimers.delete(roomAddress);
            console.log(`\n📈 [Архивариус] Клиенты вернулись в ${roomAddress.slice(-12)}. Таймер отменен.`);
          }
        }
      }

      // Сводный лог статуса
      const totalPeers = this.node.getPeers().length;
      const activeCount = this.activeRooms.size;
      const timersCount = this.roomTimers.size;
      
      console.log(`📊 Сеть: Пиров=${totalPeers} | Комнат=${activeCount} | На удаление⏳=${timersCount}`);
    }, 15000); // 15 секунд
  }

  /**
   * Остановка мониторинга и очистка всех таймеров (для Graceful Shutdown)
   */
  stop() {
    if (this.monitorInterval) {
      clearInterval(this.monitorInterval);
      this.monitorInterval = null;
    }
    
    // Чистим все запущенные таймеры удаления комнат
    for (const timer of this.roomTimers.values()) {
      clearTimeout(timer);
    }
    this.roomTimers.clear();
    console.log('📉 [Архивариус] Интервал мониторинга ресурсов остановлен, таймеры очищены.');
  }

  // Метод для начала отслеживания комнаты
async pinRoom(roomAddress, requesterPeerId = null) {
  if (!roomAddress) return null;

  // 📦 2. ПРОВЕРКА КЭША
  if (this.activeRooms.has(roomAddress)) {
    return this.activeRooms.get(roomAddress);
  }

  if (this.openingPromises.has(roomAddress)) {
    return this.openingPromises.get(roomAddress);
  }

  // 🚀 3. СОЗДАЕМ ЕДИНЫЙ ПРОМИС (Mutex + Защита от Race Condition)
  const openingPromise = (async () => {
    try {
      console.log(`[Архивариус] Пиннинг: ${roomAddress}`);
      
      let db = null;
      let retries = 8;
      
      while (retries > 0) {
        try {

          db = await this.orbitdb.open(roomAddress, {
            type: 'events',
            sync: true,
            replicate: true,
            timeout: 90000,
            AccessController: OrbitDBAccessController ({ 
              // type: 'ipfs',
              type: 'orbitdb',
              write: ['*']
            })
          });
          
          console.log(`✅ Успешно открыта: ${roomAddress}`);
          // Принудительная репликация при каждом обновлении
      db.events.on('update', async (entry) => {
        console.log(`[UPDATE] ${roomAddress.slice(-12)}`);        
        // Форсируем распространение
        try {
          await db.replicate();
        } catch (e) {}
      });
          break;
        } catch (err) {
          retries--;
          if (err.code === 'ERR_NOT_FOUND' || 
              err.name === 'NotFoundError' || 
              err.message?.includes('not found') ||
              err.message?.includes('unexpected end of input')) {
            
            console.log(`⏳ Манифест/блоки ещё не пришли (${retries} попыток)...`);
            await new Promise(r => setTimeout(r, 5000));
            continue;
          }
          
          // Критическая ошибка
          console.error(`❌ Критическая ошибка при открытии ${roomAddress}:`, err.message);
          throw err;
        }
      }

      if (!db) {
        console.error(`❌ Не удалось открыть базу после попыток: ${roomAddress}`);
        return null;
      }

      db.events.on('join', async (peerId) => {
  console.log('JOIN', peerId);

  try {
    await db.sync?.();
  } catch {}
});

      this.activeRooms.set(roomAddress, db);

      db.events.on('error', (err) => {
        if (err.message?.includes('unexpected end of input') 
          // || err.message?.includes('stream reset')
        ) {
          console.warn(`⚠️ Ожидаемая сетевая ошибка в ${roomAddress.slice(-12)}`);
          return;
        }
        console.error(`[OrbitDB Error] ${roomAddress.slice(-12)}:`, err.message);
      });
      
      return db;
    } catch (error) {
      console.error(`[Архивариус] Ошибка pinRoom ${roomAddress}:`, error);
      return null;
    } finally {
      this.openingPromises.delete(roomAddress);
    }
  })();

  this.openingPromises.set(roomAddress, openingPromise);
  return openingPromise;
}

  // Логика обновления/сброса таймера
  startDestructionTimer(roomAddress) {
  if (this.roomTimers.has(roomAddress)) return; // Таймер уже тикает, не трогаем

  console.log(`⏳ [Архивариус] Комната ${roomAddress.slice(-12)} пуста. Запуск таймера очистки (20 мин)...`);
  
  const timer = setTimeout(async () => {
    console.log(`🛑 [Архивариус] Время вышло. Закрываем БД ${roomAddress.slice(-12)}`);
    
    const db = this.activeRooms.get(roomAddress);
    if (db) {
      await db.close();
      this.activeRooms.delete(roomAddress);
    }
    this.roomTimers.delete(roomAddress);
  }, CONFIG.ARCHIVIST.INACTIVITY_TIMEOUT_MS);

  this.roomTimers.set(roomAddress, timer);
}

  // Какие комнаты сейчас на пиннинге
  getPinnedRooms() {
    return Array.from(this.activeRooms.keys());
  }
}