import { verifySessionToken } from '../utils/sessionToken.js';
import { isUserBanned, getFileOwner, deleteFileOwnerRecord } from '../database/db.js';

// Троттлинг repo/gc: не даём каждому удалению запускать полный GC у Kubo
let lastGcAt = 0;
const GC_MIN_INTERVAL_MS = 60 * 1000;

async function unpinAndMaybeGc(cid) {
  const unpinRes = await fetch(`http://ychat-kubo:5001/api/v0/pin/rm?arg=${encodeURIComponent(cid)}`, {
    method: 'POST',
  });

  if (!unpinRes.ok) {
    const text = await unpinRes.text();
    if (!text.includes('not pinned')) {
      console.warn(`⚠️ [Delete-File] Kubo unpin вернул ошибку для ${cid}:`, text);
    }
  }

  const now = Date.now();
  if (now - lastGcAt > GC_MIN_INTERVAL_MS) {
    lastGcAt = now;
    fetch('http://ychat-kubo:5001/api/v0/repo/gc', { method: 'POST' }).catch((err) => {
      console.warn('⚠️ [Delete-File] Ошибка запуска repo/gc:', err.message);
    });
  }
}

export function createDeleteFileHandler() {
  return async function deleteFileHandler(req, res) {
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

    const ownerId = getFileOwner(cid);

    if (!ownerId) {
      // Нет записи о владельце — файл не зарегистрирован через /api/register-file.
      // По умолчанию отклоняем: без записи проверить право на удаление нельзя.
      console.warn(`⚠️ [Delete-File] Нет владельца для ${cid}, удаление отклонено.`);
      return res.status(403).send('Unknown file owner');
    }

    if (ownerId !== userId) {
      console.warn(`🚫 [Delete-File] ${userId.slice(-12)} попытался удалить чужой файл ${cid} (владелец: ${ownerId.slice(-12)}).`);
      return res.status(403).send('Only the sender can delete this file');
    }

    try {
      await unpinAndMaybeGc(cid);
      deleteFileOwnerRecord(cid);
      console.log(`🗑️ [Delete-File] Файл ${cid} удалён пользователем ${userId.slice(-12)}.`);
      return res.status(200).json({ ok: true });
    } catch (err) {
      console.error(`❌ [Delete-File] Ошибка при удалении ${cid}:`, err);
      return res.status(502).send('Failed to delete file from storage');
    }
  };
}