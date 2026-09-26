import * as lp from 'it-length-prefixed';
import { pipe } from 'it-pipe';
import { CONFIG } from '../config.js';
import { generateAuthToken } from '../utils/crypto.js';
import { savePushSubscription, removePushSubscription } from '../database/db.js';
import { sendPushToTarget } from '../../services/pushSendService.js';

async function reply(stream, status) {
  try {
    await pipe([new TextEncoder().encode(JSON.stringify({ status }))], lp.encode, stream.sink);
  } catch { /* стрим уже закрыт */ }
}

// ПРИЁМ: клиент присылает свою Web Push подписку. peerId — connection.remotePeer, не из тела запроса.
export function registerPushSubscribeProtocol(node, pubsub) {
  node.handle(CONFIG.TOPICS.PUSH_SUBSCRIBE, async ({ stream, connection }) => {
    const peerId = connection.remotePeer.toString();
    try {
      await pipe(stream.source, lp.decode, async function (source) {
        for await (const chunk of source) {
          if (chunk.length > CONFIG.PUSH.MAX_PAYLOAD_BYTES) {
            return await reply(stream, CONFIG.MSG.FORBIDDEN);
          }

          const sub = JSON.parse(new TextDecoder().decode(chunk.subarray()));
          const valid =
            typeof sub?.endpoint === 'string' && sub.endpoint.startsWith('https://') &&
            typeof sub?.keys?.p256dh === 'string' && typeof sub?.keys?.auth === 'string';

          if (!valid) return await reply(stream, CONFIG.MSG.FORBIDDEN);

          const record = savePushSubscription(peerId, sub.endpoint, JSON.stringify(sub));
          if (!record) return await reply(stream, CONFIG.MSG.FORBIDDEN); // лимит устройств исчерпан

          try {
            const timestamp = Date.now();
            const auth = generateAuthToken(timestamp, CONFIG.SECURITY.clusterSecret);
            await pubsub.publish(
              CONFIG.TOPICS.PUSH_LIVE_SYNC,
              new TextEncoder().encode(JSON.stringify({ timestamp, auth, record }))
            );
          } catch { /* других релеев онлайн нет */ }

          console.log(`🔔 [Push] Подписка сохранена: ${peerId.slice(-12)}`);
          await reply(stream, CONFIG.MSG.SUCCESS);
          break;
        }
      });
    } catch (err) {
      console.error('❌ [Push] Ошибка приёма подписки:', err.message);
    }
  }, { runOnTransientConnection: true });
}

// ОТПИСКА: клиент присылает { endpoint }
export function registerPushUnsubscribeProtocol(node, pubsub) {
  node.handle(CONFIG.TOPICS.PUSH_UNSUBSCRIBE, async ({ stream, connection }) => {
    const peerId = connection.remotePeer.toString();
    try {
      await pipe(stream.source, lp.decode, async function (source) {
        for await (const chunk of source) {
          const { endpoint } = JSON.parse(new TextDecoder().decode(chunk.subarray()));
          if (typeof endpoint !== 'string') return await reply(stream, CONFIG.MSG.FORBIDDEN);

          const record = removePushSubscription(peerId, endpoint);

          try {
            const timestamp = Date.now();
            const auth = generateAuthToken(timestamp, CONFIG.SECURITY.clusterSecret);
            await pubsub.publish(
              CONFIG.TOPICS.PUSH_LIVE_SYNC,
              new TextEncoder().encode(JSON.stringify({ timestamp, auth, record }))
            );
          } catch { /* других релеев онлайн нет */ }

          console.log(`🔕 [Push] Подписка удалена: ${peerId.slice(-12)}`);
          await reply(stream, CONFIG.MSG.SUCCESS);
          break;
        }
      });
    } catch (err) {
      console.error('❌ [Push] Ошибка отписки:', err.message);
    }
  }, { runOnTransientConnection: true });
}

const lastNotify = new Map(); // `${senderId}:${targetId}` -> timestamp
const NOTIFY_COOLDOWN_MS = 3000;

// УВЕДОМЛЕНИЕ: отправитель просит запушить получателя после отправки сообщения
export function registerPushNotifyProtocol(node) {
  node.handle(CONFIG.TOPICS.PUSH_NOTIFY, async ({ stream, connection }) => {
    const senderId = connection.remotePeer.toString();
    try {
      await pipe(stream.source, lp.decode, async function (source) {
        for await (const chunk of source) {
          const { targetId } = JSON.parse(new TextDecoder().decode(chunk.subarray()));
          if (typeof targetId !== 'string' || targetId === senderId) {
            return await reply(stream, CONFIG.MSG.FORBIDDEN);
          }

          const key = `${senderId}:${targetId}`;
          if (Date.now() - (lastNotify.get(key) || 0) < NOTIFY_COOLDOWN_MS) {
            return await reply(stream, CONFIG.MSG.SUCCESS); // тихо игнорим, не ошибка для клиента
          }
          lastNotify.set(key, Date.now());

          sendPushToTarget(targetId, senderId).catch(err =>
            console.error('❌ [Push] Ошибка sendPushToTarget:', err.message)
          );

          await reply(stream, CONFIG.MSG.SUCCESS);
          break;
        }
      });
    } catch (err) {
      console.error('❌ [Push] Ошибка notify:', err.message);
    }
  }, { runOnTransientConnection: true });
}