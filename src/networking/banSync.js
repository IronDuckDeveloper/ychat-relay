import * as lp from 'it-length-prefixed';
import { pipe } from 'it-pipe';
import { CONFIG } from '../config.js';
import { generateAuthToken } from '../utils/crypto.js';
import { getAllBanRecords, mergeBanRecords } from '../database/db.js';
import { multiaddr } from '@multiformats/multiaddr';

// ==========================================
// 1. ПРИЕМ: Нода отдает свой список банов по запросу
// ==========================================
export function setupBanSyncProtocol(node) {
  console.log(`📡 [libp2p] Регистрация протокола синхронизации банов: ${CONFIG.TOPICS.BAN_SYNC}`);

  node.handle(CONFIG.TOPICS.BAN_SYNC, async ({ stream, connection }) => {
    try {
      await pipe(
        stream.source,
        lp.decode,
        async function (source) {
          for await (const chunk of source) {
            const request = JSON.parse(new TextDecoder().decode(chunk.subarray()));
            const { timestamp, auth } = request;

            if (!timestamp || !auth || Math.abs(Date.now() - timestamp) > 60000) {
              console.warn('🔒 [Ban-Sync] Отказ: устаревший или неверный запрос.');
              return;
            }

            const expectedAuth = generateAuthToken(timestamp, CONFIG.SECURITY.clusterSecret);
            if (auth !== expectedAuth) {
              console.error('🔒 [Ban-Sync] КРИТИЧЕСКАЯ ОШИБКА: Неверный пароль кластера!');
              return;
            }

            const records = getAllBanRecords();
            const CHUNK_SIZE = 500;
            const totalChunks = Math.max(1, Math.ceil(records.length / CHUNK_SIZE));

            if (records.length === 0) {
              const emptyPayload = JSON.stringify({ records: [], isLast: true });
              await pipe([new TextEncoder().encode(emptyPayload)], lp.encode, stream.sink);
              return;
            }

            console.log(`⏳ [Ban-Sync] Отправляем ${records.length} записей о банах (чанков: ${totalChunks})...`);

            async function* generateChunks() {
              for (let i = 0; i < totalChunks; i++) {
                const slice = records.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
                const isLast = i === totalChunks - 1;
                yield new TextEncoder().encode(JSON.stringify({ records: slice, isLast }));
              }
            }

            await pipe(generateChunks(), lp.encode, stream.sink);
            console.log(`📤 [Ban-Sync] Завершена отправка для ${connection.remotePeer.toString().slice(-12)}`);
            break;
          }
        }
      );
    } catch (err) {
      console.error('❌ [Ban-Sync] Ошибка обработчика:', err.message);
    }
  });
}

// ==========================================
// 2. ОТПРАВКА: Запрос списка банов у конкретного пира
// ==========================================
export async function requestBanSync(node, targetMultiaddrStr) {
  try {
    const ma = multiaddr(targetMultiaddrStr);
    const stream = await node.dialProtocol(ma, CONFIG.TOPICS.BAN_SYNC);

    const timestamp = Date.now();
    const auth = generateAuthToken(timestamp, CONFIG.SECURITY.clusterSecret);
    const requestPayload = JSON.stringify({ timestamp, auth });

    await pipe([new TextEncoder().encode(requestPayload)], lp.encode, stream.sink);

    let totalReceived = 0;

    await pipe(
      stream.source,
      lp.decode,
      async function (source) {
        for await (const chunk of source) {
          const response = JSON.parse(new TextDecoder().decode(chunk.subarray()));

          if (response.records && Array.isArray(response.records)) {
            totalReceived += response.records.length;
            mergeBanRecords(response.records);
          }

          if (response.isLast) {
            console.log(`✅ [Ban-Sync] Синхронизация банов завершена. Обработано: ${totalReceived}`);
            break;
          }
        }
      }
    );
  } catch (err) {
    console.error(`❌ [Ban-Sync] Ошибка запроса банов у пира ${targetMultiaddrStr}:`, err.message);
  }
}