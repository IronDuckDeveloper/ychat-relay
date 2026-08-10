import * as lp from 'it-length-prefixed';
import { pipe } from 'it-pipe';
import { CONFIG } from '../config.js';
import { generateAuthToken } from '../utils/crypto.js';
import { getAllRegistrations, mergeRegistrations } from '../database/db.js';
import { multiaddr } from '@multiformats/multiaddr';

// ==========================================
// 1. ПРИЕМ: Нода отдает свою БД по запросу
// ==========================================
export function setupDatabaseSyncProtocol(node) {
  console.log(`📡 [libp2p] Регистрация протокола синхронизации БД: ${CONFIG.TOPICS.DB_SYNC}`);

  node.handle(CONFIG.TOPICS.DB_SYNC, async ({ stream, connection }) => {
    try {
      await pipe(
        stream.source,
        lp.decode,
        async function (source) {
          for await (const chunk of source) {
            const request = JSON.parse(new TextDecoder().decode(chunk.subarray()));
            const { timestamp, auth } = request;

            // 🛡️ Валидация безопасности
            if (!timestamp || !auth || Math.abs(Date.now() - timestamp) > 60000) {
              console.warn('🔒 [DB-SYNC] Отказ: устаревший или неверный запрос.');
              return;
            }
            
            const expectedAuth = generateAuthToken(timestamp, CONFIG.SECURITY.clusterSecret);
            if (auth !== expectedAuth) {
              console.error('🔒 [DB-SYNC] КРИТИЧЕСКАЯ ОШИБКА: Неверный пароль кластера!');
              return;
            }

            // Выгружаем записи
            const records = getAllRegistrations();
            const CHUNK_SIZE = 500; // Отправляем по 500 записей за раз
            const totalChunks = Math.ceil(records.length / CHUNK_SIZE);

            if (records.length === 0) {
              // База пуста, отправляем пустой ответ
              const emptyPayload = JSON.stringify({ records: [], isLast: true });
              await pipe([new TextEncoder().encode(emptyPayload)], lp.encode, stream.sink);
              return;
            }

            console.log(`⏳ [DB-SYNC] Начинаем отправку ${records.length} записей (чанков: ${totalChunks})...`);

            // 📦 Функция-генератор чанков
            async function* generateChunks() {
              for (let i = 0; i < totalChunks; i++) {
                const slice = records.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
                const isLast = i === totalChunks - 1;
                
                const payload = JSON.stringify({ 
                  records: slice, 
                  isLast: isLast 
                });
                
                yield new TextEncoder().encode(payload);
              }
            }

            // Отправляем чанки в стрим один за другим
            await pipe(
              generateChunks(),
              lp.encode,
              stream.sink
            );
            
            console.log(`📤 [DB-SYNC] Успешно завершена отправка для ${connection.remotePeer.toString().slice(-12)}`);
            break; // Обработали запрос, выходим из цикла чтения
          }
        }
      );
    } catch (err) {
      console.error('❌ [DB-SYNC] Ошибка обработчика:', err.message);
    }
  });
}

// ==========================================
// 2. ОТПРАВКА: Запрос БД у конкретного пира
// ==========================================
export async function requestDatabaseSync(node, targetMultiaddrStr) {
  try {
    const ma = multiaddr(targetMultiaddrStr);
    const stream = await node.dialProtocol(ma, CONFIG.TOPICS.DB_SYNC);

    const timestamp = Date.now();
    const auth = generateAuthToken(timestamp, CONFIG.SECURITY.clusterSecret);

    const requestPayload = JSON.stringify({ timestamp, auth });

    // Отправляем запрос
    await pipe(
      [new TextEncoder().encode(requestPayload)],
      lp.encode,
      stream.sink
    );

    let totalReceived = 0;

    // Читаем ответ частями
    await pipe(
      stream.source,
      lp.decode,
      async function (source) {
        for await (const chunk of source) {
          const response = JSON.parse(new TextDecoder().decode(chunk.subarray()));
          
          if (response.records && Array.isArray(response.records)) {
            totalReceived += response.records.length;
            console.log(`📥 [DB-SYNC] Получен чанк: ${response.records.length} записей (Всего скачано: ${totalReceived})`);
            
            // Сразу мержим чанк в базу, не ждем конца скачивания
            mergeRegistrations(response.records);
          }

          // Если это последний кусок — прерываем чтение
          if (response.isLast) {
            console.log(`✅ [DB-SYNC] Синхронизация полностью завершена. Всего принято: ${totalReceived}`);
            break;
          }
        }
      }
    );
  } catch (err) {
    console.error(`❌ [DB-SYNC] Ошибка запроса БД у пира ${targetMultiaddrStr}:`, err.message);
  }
}