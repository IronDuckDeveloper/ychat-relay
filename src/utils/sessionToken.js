import crypto from 'crypto';
import { CONFIG } from '../config.js';

const SESSION_TTL_MS = 24 * 60 * 60 * 1000; // 24 часа

/**
 * Выдаёт подписанный токен сессии для конкретного peerId.
 * userId зашит внутрь и подписан — клиент не может подменить чужой ID.
 */
export function issueSessionToken(userId) {
  const expiry = Date.now() + SESSION_TTL_MS;
  const payload = JSON.stringify({ userId, expiry });
  const signature = crypto
    .createHmac('sha256', CONFIG.SECURITY.clientSessionSecret)
    .update(payload)
    .digest('hex');

  return Buffer.from(JSON.stringify({ userId, expiry, signature })).toString('base64');
}

/**
 * Проверяет токен. Возвращает userId, если подпись верна и токен не истёк, иначе null.
 */
export function verifySessionToken(token) {
  try {
    const decoded = Buffer.from(token, 'base64').toString('utf8');
    const { userId, expiry, signature } = JSON.parse(decoded);
    if (!userId || !expiry || !signature) return null;
    if (Date.now() > expiry) return null;

    const payload = JSON.stringify({ userId, expiry });
    const expectedSignature = crypto
      .createHmac('sha256', CONFIG.SECURITY.clientSessionSecret)
      .update(payload)
      .digest('hex');

    const sigBuf = Buffer.from(signature, 'hex');
    const expBuf = Buffer.from(expectedSignature, 'hex');
    if (sigBuf.length !== expBuf.length) return null;
    if (!crypto.timingSafeEqual(sigBuf, expBuf)) return null;

    return userId;
  } catch (e) {
    return null;
  }
}