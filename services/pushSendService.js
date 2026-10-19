import webpush from 'web-push';
import { CONFIG } from '../src/config.js';
import { getPushSubscriptionsFor, removePushSubscription } from '../src/database/db.js';

webpush.setVapidDetails(
  CONFIG.SECURITY.vapidSubject,
  CONFIG.SECURITY.vapidPublicKey,
  CONFIG.SECURITY.vapidPrivateKey
);

// Payload — только { from: senderId }, без текста: релей не должен знать контент.
const mutedSet = (json) => { try { return new Set(JSON.parse(json || '[]')); } catch { return new Set(); } };

// groupId = null для 1:1. Ключ мьюта: id собеседника (1:1) или id группы.
export async function sendPushToTarget(targetId, senderId, groupId = null) {
  const subs = getPushSubscriptionsFor(targetId);
  if (subs.length === 0) return;

  const key = groupId ?? senderId;
  const payload = JSON.stringify(groupId ? { from: senderId, g: groupId } : { from: senderId });

  await Promise.all(subs.map(async ({ endpoint, subscription_json, muted_json }) => {
    if (mutedSet(muted_json).has(key)) return;
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

const lastBySender = new Map(); // `${senderId}:${groupId}` -> ts
const lastByTarget = new Map(); // `${targetId}:${groupId}` -> ts

function sweep(map, ttl) {
  if (map.size < 5000) return;
  const now = Date.now();
  for (const [k, t] of map) if (now - t > ttl) map.delete(k);
}

// memberIds: активные участники, уже проверенные по состоянию группы
export function claimGroupSender(senderId, groupId) {
  const now = Date.now();
  const sk = `${senderId}:${groupId}`;
  if (now - (lastBySender.get(sk) ?? 0) < CONFIG.PUSH.GROUP_SENDER_COOLDOWN_MS) return false;
  lastBySender.set(sk, now);
  return true;
}

export async function sendPushToGroup(groupId, senderId, memberIds) {
  const now = Date.now();

  const targets = memberIds.filter((id) => {
    if (id === senderId) return false;
    const k = `${id}:${groupId}`;
    if (now - (lastByTarget.get(k) ?? 0) < CONFIG.PUSH.GROUP_TARGET_COOLDOWN_MS) return false;
    lastByTarget.set(k, now);
    return true;
  });

  let i = 0;
  const worker = async () => {
    while (i < targets.length) {
      await sendPushToTarget(targets[i++], senderId, groupId).catch(() => {});
    }
  };
  await Promise.all(Array.from({ length: Math.min(CONFIG.PUSH.GROUP_CONCURRENCY, targets.length) }, worker));

  sweep(lastBySender, CONFIG.PUSH.GROUP_SENDER_COOLDOWN_MS);
  sweep(lastByTarget, CONFIG.PUSH.GROUP_TARGET_COOLDOWN_MS);
}