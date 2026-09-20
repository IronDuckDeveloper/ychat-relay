import * as lp from 'it-length-prefixed';
import { pipe } from 'it-pipe';
import { CONFIG } from '../config.js';
import { generateAuthToken } from '../utils/crypto.js';
import { isUserBanned, saveContactRequest, getContactRequestsFor } from '../database/db.js';

async function reply(stream, status) {
  try {
    await pipe([new TextEncoder().encode(JSON.stringify({ status }))], lp.encode, stream.sink);
  } catch { /* стрим уже закрыт */ }
}

// ПРИЁМ: клиент A кладёт запрос для B.
// Отправитель — connection.remotePeer (подтверждён noise), а не поле из payload: подделать нельзя.
export function registerContactRequestProtocol(node, pubsub) {
  node.handle(CONFIG.TOPICS.CONTACT_REQUEST_DEPOSIT, async ({ stream, connection }) => {
    const senderId = connection.remotePeer.toString();
    try {
      await pipe(stream.source, lp.decode, async function (source) {
        for await (const chunk of source) {
          if (chunk.length > CONFIG.CONTACT_REQUESTS.MAX_PAYLOAD_BYTES) {
            return await reply(stream, CONFIG.MSG.FORBIDDEN);
          }

          const { targetId, payload } = JSON.parse(new TextDecoder().decode(chunk.subarray()));

          const valid =
            typeof targetId === 'string' && targetId.length > 0 && targetId.length <= 100 &&
            targetId !== senderId &&
            payload?.type === CONFIG.MSG.PROFILE_UPDATED;

          if (!valid || isUserBanned(senderId)) {
            return await reply(stream, CONFIG.MSG.FORBIDDEN);
          }

          const json = JSON.stringify({ ...payload, senderId }); // senderId принудительно = remotePeer
          const record = Buffer.byteLength(json) <= CONFIG.CONTACT_REQUESTS.MAX_PAYLOAD_BYTES
            ? saveContactRequest(targetId, senderId, json)
            : null;

          if (!record) return await reply(stream, CONFIG.MSG.FORBIDDEN);

          // Живая рассылка остальным релеям (отставшие догонят через startup-sync)
          try {
            const timestamp = Date.now();
            const auth = generateAuthToken(timestamp, CONFIG.SECURITY.clusterSecret);
            await pubsub.publish(
              CONFIG.TOPICS.CONTACT_REQUEST_LIVE_SYNC,
              new TextEncoder().encode(JSON.stringify({ timestamp, auth, record }))
            );
          } catch { /* других релеев онлайн нет */ }

          console.log(`📥 [ContactRequest] ${senderId.slice(-12)} -> ${targetId.slice(-12)}`);
          await reply(stream, CONFIG.MSG.SUCCESS);
          break;
        }
      });
    } catch (err) {
      console.error('❌ [ContactRequest] Ошибка приёма:', err.message);
    }
  }, { runOnTransientConnection: true });
}

// ВЫДАЧА: клиент сам забирает свои запросы. Кто спрашивает — по connection.remotePeer.
export function registerContactRequestFetchProtocol(node) {
  node.handle(CONFIG.TOPICS.CONTACT_REQUEST_FETCH, async ({ stream, connection }) => {
    const peerId = connection.remotePeer.toString();
    try {
      await pipe(stream.source, lp.decode, async function (source) {
        for await (const _ of source) {
          const requests = isUserBanned(peerId)
            ? []
            : getContactRequestsFor(peerId).map(r => r.payload);

          await pipe(
            [new TextEncoder().encode(JSON.stringify({ status: CONFIG.MSG.SUCCESS, requests }))],
            lp.encode,
            stream.sink
          );
          console.log(`📬 [ContactRequest] Выдано ${requests.length} -> ${peerId.slice(-12)}`);
          break;
        }
      });
    } catch (err) {
      console.error('❌ [ContactRequest] Ошибка выдачи:', err.message);
    }
  }, { runOnTransientConnection: true });
}