import { verifySessionToken } from '../utils/sessionToken.js';
import { isUserBanned } from '../database/db.js';

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

    if (isUserBanned(userId)) {
      console.warn(`🚫 [Upload-Check] Забаненный пользователь ${userId.slice(-12)} пытался загрузить файл.`);
      return res.status(403).send('User banned');
    }

    console.log(`✅ [Upload-Check] Разрешена загрузка для ${userId.slice(-12)}`);
    return res.status(200).send('OK');
  };
}