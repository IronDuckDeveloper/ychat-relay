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

    const canAppend = async (entry) => {
      // 1. Базовая проверка прав на запись + подписи identity
      const baseAllowed = await base.canAppend(entry);
      if (!baseAllowed) return false;

      const writerIdentity = await identities.getIdentity(entry.identity);
      if (!writerIdentity) return false;
      const id = writerIdentity.id;

      // 2. Защита от гигантских сообщений
      const value = entry.payload?.value;
      if (value && typeof value.text === 'string' && value.text.length > MAX_TEXT_LENGTH) {
        console.warn(`🚫 [AntiSpam] Отклонено — слишком длинный текст от ${id.slice(-12)}`);
        return false;
      }

      // 3. Скользящее окно по времени получения записи (Date.now() на этой
      // машине), а не по полю value.ts, которое легко подделать отправителю
      const now = Date.now();
      const recent = (history.get(id) || []).filter((t) => now - t < WINDOW_MS);

      if (recent.length >= MAX_MESSAGES) {
        console.warn(`🚫 [AntiSpam] Rate limit: ${id.slice(-12)} — больше ${MAX_MESSAGES} записей за ${WINDOW_MS}мс`);
        return false;
      }

      recent.push(now);
      history.set(id, recent);

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
