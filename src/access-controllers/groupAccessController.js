import { IPFSAccessController } from '@orbitdb/core';

const type = 'ychat-group';
const IPFS_PREFIX = '/ipfs/';
const OWN_PREFIX = `/${type}/`;

// Те же лимиты, что в rateLimitedAccessController.ts — держать синхронно
const MAX_MESSAGES = 15;
const WINDOW_MS = 10_000;
const MAX_TEXT_LENGTH = 10_000;
const BACKLOG_THRESHOLD_MS = 30_000;
const MAX_ACT_BYTES = 2_048;

// Должен совпадать с типами событий в groupState.ts (следующий шаг)
const ACT_TYPES = new Set([
  'add', 'kick', 'ban', 'unban', 'role', 'write',
  'leave', 'profile', 'group', 'del_msg', 'transfer', 'close',
]);

export const GroupAccessController = () => async (params) => {
  const { orbitdb, identities, address } = params;

  // IPFSAccessController хранит манифест { write: [owner] }: при создании owner = identity создателя,
  // при открытии по адресу читается из манифеста. Это и есть владелец группы (db.access.write[0]).
  const base = await IPFSAccessController({ write: [orbitdb.identity.id] })({
    ...params,
    address: address ? address.replace(OWN_PREFIX, IPFS_PREFIX) : undefined,
  });

  const history = new Map();
  const verifiedHashes = new Set();

  const canAppend = async (entry) => {
    if (verifiedHashes.has(entry.hash)) return true;

    const writerIdentity = await identities.getIdentity(entry.identity);
    if (!writerIdentity) return false;

    try {
      if (!(await identities.verifyIdentity(writerIdentity))) return false;
    } catch (err) {
      console.warn('🚫 [Group] verifyIdentity упал:', err);
      return false;
    }
    const id = writerIdentity.id;

    const payload = entry.payload;
    const key = payload?.key;
    const value = payload?.value;
    if (payload?.op !== 'PUT' || typeof key !== 'string' || !value || value._id !== key) return false;

    // Писать можно только под своим префиксом и от своего имени
    const isMsg = key.startsWith(`msg_${id}_`);
    const isAct = key.startsWith(`act_${id}_`);
    if (!isMsg && !isAct) {
      console.warn(`🚫 [Group] ${id.slice(-12)} пишет в чужой/неизвестный ключ ${key}`);
      return false;
    }
    if (value.whoSent !== id) return false;

    if (isMsg && typeof value.text === 'string' && value.text.length > MAX_TEXT_LENGTH) return false;
    if (isAct) {
      if (!ACT_TYPES.has(value.type)) return false;
      if (JSON.stringify(value).length > MAX_ACT_BYTES) return false;
    }

    const now = Date.now();
    const claimedTs = typeof value.ts === 'number' ? value.ts : now;
    if (now - claimedTs <= BACKLOG_THRESHOLD_MS) {
      const recent = (history.get(id) || []).filter((t) => now - t < WINDOW_MS);
      if (recent.length >= MAX_MESSAGES) {
        console.warn(`🚫 [Group] Rate limit: ${id.slice(-12)}`);
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
    address: base.address.replace(IPFS_PREFIX, OWN_PREFIX),
    canAppend,
  };
};

(GroupAccessController).type = type;