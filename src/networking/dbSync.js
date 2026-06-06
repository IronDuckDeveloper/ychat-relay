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

            // Выгружаем записи и отправляем
            const records = getAllRegistrations();
            const responsePayload = JSON.stringify({ records });

            await pipe(
              [new TextEncoder().encode(responsePayload)],
              lp.encode,
              stream.sink
            );
            
            console.log(`📤 [DB-SYNC] Успешно отправлено ${records.length} записей для ${connection.remotePeer.toString().slice(-6)}`);
            break;
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

    await pipe(
      [new TextEncoder().encode(requestPayload)],
      lp.encode,
      stream.sink
    );

    // Читаем ответ
    await pipe(
      stream.source,
      lp.decode,
      async function (source) {
        for await (const chunk of source) {
          const response = JSON.parse(new TextDecoder().decode(chunk.subarray()));
          
          if (response.records && Array.isArray(response.records)) {
            console.log(`📥 [DB-SYNC] Получено ${response.records.length} записей. Запускаем слияние...`);
            mergeRegistrations(response.records);
          }
          break;
        }
      }
    );
  } catch (err) {
    console.error(`❌ [DB-SYNC] Ошибка запроса БД у пира ${targetMultiaddrStr}:`, err.message);
  }
}