// src/networking/rpcHandler.ts
import * as lp from 'it-length-prefixed';
import { pipe } from 'it-pipe';
import { checkAndLogRegistration } from '../database/db.js';
import { CONFIG } from '../config.js';


export function setupAntiFloodProtocol(libp2p) {

  console.log(`📡 [libp2p] Регистрация кастомного протокола: ${CONFIG.TOPICS.PROTOCOL}`);

  libp2p.handle(CONFIG.TOPICS.PROTOCOL, async ({ stream, connection }) => {
    try {
      // Читаем входящий поток данных от клиента
      await pipe(
        stream.source,
        lp.decode(),
        async function (source) {
          for await (const chunk of source) {
            // 1. Декодируем входящий JSON от клиента
            const data = JSON.parse(new TextDecoder().decode(chunk.subarray()));
            const clientFingerprint = data.fingerprint;
            const clientIp = data.ipAddress;

            console.log(`📥 [RPC] Запрос верификации. Сетевой IP: ${clientIp}, FP: ${clientFingerprint?.slice(0, 10)}...`);

            // 2. Проверяем по базе данных SQLite
            const isAllowed = checkAndLogRegistration(clientIp, clientFingerprint);

            // 3. Формируем ответ для клиента
            const responsePayload = JSON.stringify({
              status: isAllowed ? CONFIG.MSG.SUCCESS : CONFIG.MSG.FORBIDDEN,
              message: isAllowed ? CONFIG.MSG.REG_IS_OVER : CONFIG.MSG.LIMIT_EXCEEDED
            });

            // Отправляем ответ обратно в стрим
            try {
              await pipe(
                [new TextEncoder().encode(responsePayload)],
                lp.encode(),
                stream.sink
              );
            } catch (err) {
              // Проверяем, не вызвана ли ошибка тем, что клиент уже отключился
              if (err.message.includes('ended pushable') || err.message.includes('stream reset')) {
                // Тихо игнорируем — база уже открыта, просто клиент не дождался ответа
                // console.log('⚠️ [Protocol] Клиент закрыл стрим до получения ответа (нормально)');
              } else {
                // Реальные проблемы логируем
                console.error('❌ [Protocol] Ошибка ответа в стрим:', err);
              }
            }
            
            break; // Обработали один пакет и закрываем цикл
          }
        }
      );
    } catch (error) {
      console.error('❌ [RPC Error] Ошибка обработки запроса регистрации:', error);
      stream.close();
    }
  });
}

export async function registerAnnounceProtocol(node, archivist, pendingRequests) {
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
                    if (now - lastSeen < CONFIG.DELAY_START_MS) return;  // Защита от спама
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
}