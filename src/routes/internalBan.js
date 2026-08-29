import { CONFIG } from '../config.js';
import { generateAuthToken } from '../utils/crypto.js';
import { banUser, unbanUser } from '../database/db.js';

/**
 * Внутренние эндпоинты для scripts/ban.js. Мутации (ban/unban) идут ТОЛЬКО
 * через этот роут, а не напрямую в SQLite из скрипта — так бродкаст в кластер
 * никогда не забывается (пишем в БД и рассылаем изменение одной операцией).
 *
 * Защищено тем же CLUSTER_SECRET, что и остальной межрелейный трафик:
 * скрипт запускается на том же хосте/контейнере, что и сам релей
 * (см. docker exec ниже), отдельный секрет здесь не даёт дополнительной изоляции.
 */
export function createInternalBanRoutes(pubsub) {
  async function broadcastBanChange(userId, status, reason, updatedAt) {
    try {
      const timestamp = Date.now();
      const auth = generateAuthToken(timestamp, CONFIG.SECURITY.clusterSecret);
      const record = { user_id: userId, status, reason: reason ?? null, updated_at: updatedAt };
      const payload = JSON.stringify({ timestamp, auth, record });
      await pubsub.publish(CONFIG.TOPICS.BAN_LIVE_SYNC, new TextEncoder().encode(payload));
      console.log(`📢 [Ban-Live-Sync] Бродкаст изменения бана отправлен в кластер: ${userId.slice(-12)} -> ${status}`);
    } catch (err) {
      console.error('❌ [Ban-Live-Sync] Ошибка отправки бродкаста:', err.message);
    }
  }

  function checkInternalAuth(req, res) {
    const secret = req.headers['x-internal-secret'];
    if (secret !== CONFIG.SECURITY.clusterSecret) {
      console.warn('🚫 [Internal] Попытка вызова внутреннего роута без корректного секрета.');
      res.status(403).send('Forbidden');
      return false;
    }
    return true;
  }

  return {
    banHandler: async (req, res) => {
      if (!checkInternalAuth(req, res)) return;
      const { userId, reason } = req.body || {};
      if (!userId) return res.status(400).send('Требуется userId');

      const updatedAt = banUser(userId, reason || 'Не указана');
      await broadcastBanChange(userId, 'banned', reason || 'Не указана', updatedAt);
      return res.status(200).send('OK');
    },

    unbanHandler: async (req, res) => {
      if (!checkInternalAuth(req, res)) return;
      const { userId } = req.body || {};
      if (!userId) return res.status(400).send('Требуется userId');

      const updatedAt = unbanUser(userId);
      await broadcastBanChange(userId, 'unbanned', null, updatedAt);
      return res.status(200).send('OK');
    },
  };
}