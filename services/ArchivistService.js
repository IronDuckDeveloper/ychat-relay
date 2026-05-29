import { bootstrap } from '@libp2p/bootstrap';
import { createOrbitDB, OrbitDBAccessController } from '@orbitdb/core';
import { CONFIG } from '../src/config.js';

export class ArchivistService {
  /**
   * @param {Object} orbitdbInstance - Экземпляр OrbitDB поверх Helia
   * @param {number} timeoutMs - Время неактивности в миллисекундах
   * @param {helia} helia - Экземпляр Helia
   */
  constructor(helia, orbitdbInstance, timeoutMs) {
    this.helia = helia;
    this.orbitdb = orbitdbInstance;
    this.activeRooms = new Map();
    this.roomTimers = new Map();
    this.timeoutMs = timeoutMs;
    this.openingPromises = new Map();

    console.log('🛡️ ArchivistService инициализирован');
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