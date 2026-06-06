import crypto from 'crypto';

/**
 * Генерирует HMAC-SHA256 токен на основе времени и секретного ключа
 * @param {number} timestamp - время в миллисекундах
 * @param {string} secret - секретная строка (CLUSTER_SECRET)
 * @returns {string} hex-строка токена
 */
export function generateAuthToken(timestamp, secret) {
  return crypto
    .createHmac('sha256', secret)
    .update(timestamp.toString())
    .digest('hex');
}