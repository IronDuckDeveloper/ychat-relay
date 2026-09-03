import { OrbitDBAccessController } from '@orbitdb/core';

// Тип должен совпадать с тем, что зарегистрировано у клиента
// (ychat-client/src/lib/p2p/orbit/rateLimitedAccessController.ts).
// Иначе релей не сможет резолвить AccessController комнат, созданных
// клиентом, при открытии по уже существующему адресу.
const type = 'ychat-rate-limited';

// Лимиты синхронизированы со значениями в клиенте. Это не обязательное
// требование (каждый пир применяет проверку локально, консенсуса тут нет),
// но одинаковые значения дают предсказуемое поведение по всей сети.
const MAX_MESSAGES = 15;         // сообщений
const WINDOW_MS = 10_000;        // за 10 секунд
const MAX_TEXT_LENGTH = 10_000;  // символов
const BACKLOG_THRESHOLD_MS = 30_000; // старше 30с от текущего момента — считаем "историей", не живым потоком

/**
 * Обёртка над штатным OrbitDBAccessController с антиспам-проверкой.
 * На релее выполняется при каждой репликации записи от клиента
 * (Log.joinEntry -> access.canAppend), поэтому релей тоже не будет дальше
 * реплицировать/хранить записи, нарушающие лимит.
 */
export const RateLimitedAccessController = (options = {}) => {
  const baseFactory = OrbitDBAccessController({ write: options.write ?? ['*'] });

  return async (params) => {
    const base = await baseFactory(params);
    const { identities } = params;

    const history = new Map();
    
    const verifiedHashes = new Set();

    const canAppend = async (entry) => {
      if (verifiedHashes.has(entry.hash)) return true; // уже проверяли эту же запись — не считаем повторно
      // 1. Базовая проверка прав на запись + подписи identity
      const baseAllowed = await base.canAppend(entry);
      if (!baseAllowed) return false;

      const writerIdentity = await identities.getIdentity(entry.identity);
      if (!writerIdentity) return false;
      const id = writerIdentity.id;

      const key = entry.payload?.key;

      // 2.1. Проверяет, что запись принадлежит этому пользователю (ключ начинается с msg_<peerId>_).
      if (key && !key.startsWith(`msg_${id}_`)) {
        console.warn(`🚫 [Ownership] ${id.slice(-12)} пытается изменить чужой ключ ${key}`);
        return false;
      }

      // 2.2. Защита от гигантских сообщений — тоже форма спама/DoS на реплику
      const value = entry.payload?.value;
      if (value && typeof value.text === 'string' && value.text.length > MAX_TEXT_LENGTH) {
        console.warn(`🚫 [AntiSpam] Отклонено — слишком длинный текст от ${id.slice(-12)}`);
        return false;
      }

      // 3. Скользящее окно по времени получения записи (Date.now() на этой
      // машине), а не по полю value.ts, которое легко подделать отправителю
      const now = Date.now();
      const claimedTs = typeof value?.ts === 'number' ? value.ts : now;
      const isBacklog = now - claimedTs > BACKLOG_THRESHOLD_MS;

      if (!isBacklog) { // 👈 частоту считаем только для "живых" записей
        const recent = (history.get(id) || []).filter((t) => now - t < WINDOW_MS);
        if (recent.length >= MAX_MESSAGES) {
          console.warn(`🚫 [AntiSpam] Rate limit: ${id.slice(-12)} — больше ${MAX_MESSAGES} записей за ${WINDOW_MS}мс`);
          return false;
        }
        recent.push(now);
        history.set(id, recent);
      }

      verifiedHashes.add(entry.hash);
      return true;
    };

    return {
      ...base,
      type,
      canAppend,
    };
  };
};

RateLimitedAccessController.type = type;
