export const MAX_WRITERS = 50;

const isWriterSlot = (s, m) =>
  m.status === 'active' && (m.role !== 'member' || s.writeMode === 'all' || m.canWrite === true);

const writerSlots = (s) => {
  let n = 0;
  for (const m of s.members.values()) if (isWriterSlot(s, m)) n++;
  return n;
};

const roleOf = (s, id) => {
  if (id === s.owner) return 'owner';
  const m = s.members.get(id);
  return m?.status === 'active' ? m.role : null;
};

const canModerate = (s, actor, target) => {
  const a = roleOf(s, actor);
  if (target === s.owner || actor === target) return false;
  if (a === 'owner') return true;
  if (a === 'admin') return roleOf(s, target) !== 'admin';
  return false;
};

export const canWrite = (s, id) => {
  if (s.closed) return false;
  const r = roleOf(s, id);
  if (r === 'owner' || r === 'admin') return true;
  const m = s.members.get(id);
  if (!m || m.status !== 'active' || m.mutedAt !== undefined) return false;
  return s.writeMode === 'all' || m.canWrite === true;
};

export const activeMemberIds = (s) =>
  [...s.members.values()].filter((m) => m.status === 'active').map((m) => m.id);

export function computeGroupState(docs, owner) {
  const s = {
    owner, writeMode: 'all', joinMode: 'invite', writeModeSince: null,
    members: new Map([[owner, { id: owner, role: 'owner', status: 'active' }]]),
    closed: false, closedAt: null,
  };
  const remove = (id) => { s.members.delete(id); };

  const acts = docs
    .filter((d) => typeof d?._id === 'string' && d._id.startsWith('act_')
      && typeof d.ts === 'number' && typeof d.whoSent === 'string')
    .sort((a, b) => a.ts - b.ts || (a._id < b._id ? -1 : 1));

  for (const e of acts) {
    if (s.closedAt !== null && e.type !== 'leave' && e.type !== 'profile') continue;
    const actor = e.whoSent;
    const target = typeof e.target === 'string' ? e.target : undefined;
    const ar = roleOf(s, actor);
    const isStaff = ar === 'owner' || ar === 'admin';

    switch (e.type) {
      case 'group':
        if (!isStaff) break;
        if (e.joinMode === 'invite' || e.joinMode === 'link') s.joinMode = e.joinMode;
        if ((e.writeMode === 'all' || e.writeMode === 'allowed') && e.writeMode !== s.writeMode) {
          const activeCount = [...s.members.values()].filter((m) => m.status === 'active').length;
          if (e.writeMode === 'allowed' || activeCount <= MAX_WRITERS) {
            s.writeMode = e.writeMode;
            s.writeModeSince = e.writeMode === 'allowed' ? e.ts : null;
          }
        }
        break;

      case 'add':
        if (!isStaff || !target || s.members.has(target)) break;
        if (s.writeMode === 'all' && writerSlots(s) >= MAX_WRITERS) break;
        s.members.set(target, { id: target, role: 'member', status: 'active' });
        break;

      case 'join':
        if (s.joinMode !== 'link' || s.members.has(actor)) break;
        if (s.writeMode === 'all' && writerSlots(s) >= MAX_WRITERS) break;
        s.members.set(actor, { id: actor, role: 'member', status: 'active' });
        break;

      case 'leave':
        if (actor !== s.owner && s.members.get(actor)?.status === 'active') remove(actor);
        break;

      case 'kick':
        if (target && canModerate(s, actor, target) && s.members.get(target)?.status === 'active') remove(target);
        break;

      case 'ban':
        if (target && canModerate(s, actor, target) && s.members.get(target)?.status !== 'banned') {
          s.members.set(target, { id: target, role: 'member', status: 'banned' });
        }
        break;

      case 'unban':
        if (target && canModerate(s, actor, target) && s.members.get(target)?.status === 'banned') {
          if (s.writeMode === 'all' && writerSlots(s) >= MAX_WRITERS) break;
          s.members.set(target, { id: target, role: 'member', status: 'active' });
        }
        break;

      case 'role': {
        if (actor !== s.owner || !target || target === s.owner) break;
        const m = s.members.get(target);
        if (m?.status === 'active' && (e.role === 'admin' || e.role === 'member')) {
          if (e.role === 'admin' && !isWriterSlot(s, m) && writerSlots(s) >= MAX_WRITERS) break;
          m.role = e.role;
        }
        break;
      }

      case 'write': {
        if (!target || !canModerate(s, actor, target) || typeof e.canWrite !== 'boolean') break;
        const m = s.members.get(target);
        if (m?.status === 'active' && m.role === 'member') {
          if (e.canWrite && !isWriterSlot(s, m) && writerSlots(s) >= MAX_WRITERS) break;
          m.canWrite = e.canWrite;
          m.mutedAt = e.canWrite ? undefined : e.ts;
        }
        break;
      }

      case 'transfer': {
        if (actor !== s.owner || !target || target === actor) break;
        const m = s.members.get(target);
        if (m?.status !== 'active') break;
        const prev = s.members.get(actor);
        if (prev) prev.role = 'admin';
        m.role = 'owner';
        m.canWrite = undefined;
        m.mutedAt = undefined;
        s.owner = target;
        break;
      }

      case 'close':
        if (actor === s.owner) { s.closed = true; s.closedAt = e.ts; }
        break;
    }
  }
  return s;
}

// Кэш состояния группы: сбрасывается событием 'update' управляющей БД, поэтому кик/бан учитываются сразу.
export function createGroupLoader(archivist) {
  const states = new Map();      // address -> Promise<GroupState>
  const watched = new WeakSet(); // инстансы БД с уже повешенным слушателем

  return async function loadGroup(groupId) {
    const address = `/orbitdb/${groupId.slice(4)}`;
    const db = await archivist.getPinnedRoom(address);
    if (!db) return null; // группа не запинена: клиент повторит со следующим сообщением

    if (db.access?.type !== 'ychat-group' || !db.access.write?.[0]) {
      console.warn(`⚠️ [Push] ${address.slice(-12)} открыта не как ychat-group (access: ${db.access?.type})`);
      return null;
    }

    if (!watched.has(db)) {
      watched.add(db);
      db.events.on('update', () => states.delete(address));
    }

    let p = states.get(address);
    if (!p) {
      p = (async () => {
        const rows = await db.all();
        return computeGroupState(rows.map((r) => r?.value ?? r), db.access.write[0]);
      })();
      states.set(address, p);
      p.catch(() => states.delete(address));
    }
    return p;
  };
}