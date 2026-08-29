import { verifySessionToken } from '../utils/sessionToken.js';
import { isUserBanned, registerFileOwner } from '../database/db.js';

export function createRegisterFileHandler() {
  return async function registerFileHandler(req, res) {
    const token = req.headers['x-session-token'];
    if (!token) {
      return res.status(401).send('Missing session token');
    }

    const userId = verifySessionToken(token);
    if (!userId) {
      return res.status(401).send('Invalid or expired token');
    }

    if (isUserBanned(userId)) {
      return res.status(403).send('User banned');
    }

    const { cid } = req.body || {};
    if (!cid || typeof cid !== 'string') {
      return res.status(400).send('Missing cid');
    }

    const registered = registerFileOwner(cid, userId);
    if (registered) {
      console.log(`📌 [Register-File] ${cid} закреплён за ${userId.slice(-12)}`);
    } else {
      console.log(`ℹ️ [Register-File] ${cid} уже был зарегистрирован ранее.`);
    }

    return res.status(200).json({ ok: true });
  };
}