import { verifySessionToken } from '../utils/sessionToken.js';

/**
 * Шаг 2: проверяем подпись x-session-token вместо простого поиска
 * userId в реестре. Валидная подпись уже доказывает, что токен выдан
 * релеем при успешном REGISTER/LOGIN именно этому peerId — отдельный
 * запрос к globalRegistryDb больше не нужен.
 */
export function createCheckUploadHandler() {
  return async function checkUploadHandler(req, res) {
    const token = req.headers['x-session-token'];
    if (!token) {
      return res.status(401).send('Missing session token');
    }

    const userId = verifySessionToken(token);
    if (!userId) {
      console.warn('⚠️ [Upload-Check] Невалидный или просроченный токен.');
      return res.status(401).send('Invalid or expired token');
    }

    console.log(`✅ [Upload-Check] Разрешена загрузка для ${userId.slice(-12)}`);
    return res.status(200).send('OK');
  };
}