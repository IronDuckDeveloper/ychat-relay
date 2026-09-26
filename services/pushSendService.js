import webpush from 'web-push';
import { CONFIG } from '../src/config.js';
import { getPushSubscriptionsFor, removePushSubscription } from '../src/database/db.js';

webpush.setVapidDetails(
  CONFIG.SECURITY.vapidSubject,
  CONFIG.SECURITY.vapidPublicKey,
  CONFIG.SECURITY.vapidPrivateKey
);

// Payload — только { from: senderId }, без текста: релей не должен знать контент.
export async function sendPushToTarget(targetId, senderId) {
  const subs = getPushSubscriptionsFor(targetId);
  if (subs.length === 0) return;

  const payload = JSON.stringify({ from: senderId });

  await Promise.all(subs.map(async ({ endpoint, subscription_json }) => {
    try {
      await webpush.sendNotification(JSON.parse(subscription_json), payload, { TTL: 86400, urgency: 'high' });
      console.log(`📨 [Push] Отправлено на ${endpoint.slice(-12)} (получатель ${targetId.slice(-12)}, от ${senderId.slice(-12)})`);
    } catch (err) {
      if (err.statusCode === 404 || err.statusCode === 410) {
        removePushSubscription(targetId, endpoint);
        console.log(`🔕 [Push] Подписка ${endpoint.slice(-12)} мертва (${err.statusCode}), удалена`);
      } else {
        console.error(`❌ [Push] Ошибка отправки на ${endpoint.slice(-12)}:`, err.message);
      }
    }
  }));
}