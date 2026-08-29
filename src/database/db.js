import Database from 'better-sqlite3';
import path from 'path';
import { CONFIG } from '../config.js';

// Создаем или открываем файл базы данных в корне сервера
const dbPath = path.resolve(process.cwd(), CONFIG.SQL.DB_PATH);
const db = new Database(dbPath);

// Включаем режим WAL для высокой производительности при параллельных запросах
db.pragma('journal_mode = WAL');

/**
 * Инициализация таблиц антифрода
 */
export function initDatabase() {
  db.prepare(`
    CREATE TABLE IF NOT EXISTS ${CONFIG.SQL.DB_NAME} (
      ${CONFIG.SQL.ID_COLUMN} INTEGER PRIMARY KEY AUTOINCREMENT,
      ${CONFIG.SQL.IP} TEXT NOT NULL,
      ${CONFIG.SQL.DEVICE_HASH} TEXT NOT NULL,
      ${CONFIG.SQL.PROFILE_ADDRESS} TEXT,
      ${CONFIG.SQL.TIMESTAMP} INTEGER NOT NULL,
      UNIQUE(${CONFIG.SQL.IP}, ${CONFIG.SQL.DEVICE_HASH}, ${CONFIG.SQL.PROFILE_ADDRESS}, ${CONFIG.SQL.TIMESTAMP})
    )
  `).run();

  // Создаем индексы для быстрого поиска
  db.prepare(`CREATE INDEX IF NOT EXISTS idx_ip ON ${CONFIG.SQL.DB_NAME}(${CONFIG.SQL.PROFILE_ADDRESS})`).run();
  db.prepare(`CREATE INDEX IF NOT EXISTS idx_device ON ${CONFIG.SQL.DB_NAME}(${CONFIG.SQL.DEVICE_HASH})`).run();
  db.prepare(`CREATE INDEX IF NOT EXISTS idx_profile ON ${CONFIG.SQL.DB_NAME}(${CONFIG.SQL.PROFILE_ADDRESS})`).run();

  db.prepare(`
    CREATE TABLE IF NOT EXISTS banned_users (
      user_id TEXT PRIMARY KEY,
      status TEXT NOT NULL DEFAULT 'banned',   -- 'banned' | 'unbanned'
      reason TEXT,
      updated_at INTEGER NOT NULL
    )
  `).run();

  db.prepare(`CREATE INDEX IF NOT EXISTS idx_ban_updated ON banned_users(updated_at)`).run();

  db.prepare(`
    CREATE TABLE IF NOT EXISTS file_owners (
      cid TEXT PRIMARY KEY,
      owner_id TEXT NOT NULL,
      created_at INTEGER NOT NULL
    )
  `).run();

  db.prepare(`CREATE INDEX IF NOT EXISTS idx_file_owner ON file_owners(owner_id)`).run();

  console.log('🗄️ [SQLite] База данных и индексы успешно инициализированы.');
}

/**
 * Проверяет лимиты и записывает лог новой регистрации вместе с адресом профиля
 * @returns boolean — разрешить регистрацию или нет
 */
export function checkAndLogRegistration(ip, deviceHash, profileAddress) {
  const now = Date.now();
  const timeLimit = now - CONFIG.SQL.ONE_YEAR_MS;

  // 1. Считаем количество регистраций за последний год по IP
  const ipCountRow = db.prepare(
    `SELECT COUNT(*) as count FROM ${CONFIG.SQL.DB_NAME} WHERE ${CONFIG.SQL.IP} = ? AND ${CONFIG.SQL.TIMESTAMP} > ?`
  ).get(ip, timeLimit);

  // 2. Считаем количество регистраций за последний год по Fingerprint устройства
  const deviceCountRow = db.prepare(
    `SELECT COUNT(*) as count FROM ${CONFIG.SQL.DB_NAME} WHERE ${CONFIG.SQL.DEVICE_HASH} = ? AND ${CONFIG.SQL.TIMESTAMP} > ?`
  ).get(deviceHash, timeLimit);

  // 3. Проверяем жесткий лимит (не более 3 регистраций в год)
  if (ipCountRow.count >= CONFIG.SQL.MAX_REGISTRATIONS || deviceCountRow.count >= CONFIG.SQL.MAX_REGISTRATIONS) {
    console.warn(`🚨 [Anti-Fraud] Отклонено! Превышен лимит. IP: ${ip} (Рег: ${ipCountRow.count}), FP: ${deviceHash.slice(-12)}... (Рег: ${deviceCountRow.count})`);
    return false;
  }

  // 4. Логируем успешную операцию вместе с profileAddress
  const insert = db.prepare(
    `INSERT INTO ${CONFIG.SQL.DB_NAME} (${CONFIG.SQL.IP}, ${CONFIG.SQL.DEVICE_HASH}, ${CONFIG.SQL.PROFILE_ADDRESS}, ${CONFIG.SQL.TIMESTAMP}) VALUES (?, ?, ?, ?)`
  );
  insert.run(ip, deviceHash, profileAddress, now);

  console.log(`✅ [Anti-Fraud] Регистрация одобрена и залогирована. IP: ${ip}, Profile: ${profileAddress?.slice(-12)}...`);
  return true;
}

/**
 * 🌟 НОВАЯ ФУНКЦИЯ: Проверяет, существует ли уже такой профиль в базе (для LOGIN)
 * @param {string} profileAddress - Адрес OrbitDB базы данных профиля клиента
 * @returns {boolean} - true, если профиль найден
 */
export function checkIfProfileExists(profileAddress) {
  if (!profileAddress) return false;

  try {
    const stmt = db.prepare(`SELECT COUNT(*) as count FROM ${CONFIG.SQL.DB_NAME} WHERE ${CONFIG.SQL.PROFILE_ADDRESS} = ?`);
    const result = stmt.get(profileAddress);

    return result.count > 0;
  } catch (error) {
    console.error('❌ [DB Error] Ошибка при проверке существования профиля:', error);
    return false;
  }
}

// 1. Выгрузка всех регистраций для отправки соседу
export function getAllRegistrations() {
  try {
    const stmt = db.prepare(`SELECT * FROM ${CONFIG.SQL.DB_NAME}`); 
    return stmt.all();
  } catch (err) {
    console.error('❌ [DB] Ошибка чтения базы для синхронизации:', err);
    return [];
  }
}

// 2. Безопасное слияние чужих записей с нашими
export function mergeRegistrations(records) {
  if (!records || records.length === 0) return;

  try {
    // INSERT OR IGNORE гарантирует, что если запись с таким fingerprint/ip уже есть,
    // БД просто пропустит её без ошибки (нужно, чтобы в таблице стоял UNIQUE констрейнт)
    const insert = db.prepare(`
      INSERT OR IGNORE INTO ${CONFIG.SQL.DB_NAME} (${CONFIG.SQL.IP}, ${CONFIG.SQL.DEVICE_HASH}, ${CONFIG.SQL.PROFILE_ADDRESS}, ${CONFIG.SQL.TIMESTAMP})
      VALUES (?, ?, ?, ?)
    `);

    // Транзакция ускоряет массовую вставку в сотни раз
    const transaction = db.transaction((rows) => {
      let insertedCount = 0;
      for (const row of rows) {
        const result = insert.run(row.ip_address, row.device_hash, row.profile_address, row.timestamp);
        if (result.changes > 0) insertedCount++;
      }
      return insertedCount;
    });

    const newRecordsCount = transaction(records);
    if (newRecordsCount > 0) {
      console.log(`🗄️ [DB-SYNC] База обновлена. Добавлено новых записей: ${newRecordsCount}`);
    } else {
      console.log(`🗄️ [DB-SYNC] Синхронизация завершена. Новых записей нет.`);
    }
  } catch (err) {
    console.error('❌ [DB] Ошибка при слиянии записей БД:', err);
  }
}

//////////////////////////////////////////////////
// Бан, Разбан пользователей                    //
//////////////////////////////////////////////////
export function isUserBanned(userId) {
  if (!userId) return true;
  const row = db.prepare(`SELECT status FROM banned_users WHERE user_id = ?`).get(userId);
  return !!row && row.status === 'banned';
}

export function banUser(userId, reason = 'Не указана') {
  const updatedAt = Date.now();
  db.prepare(`
    INSERT INTO banned_users (user_id, status, reason, updated_at)
    VALUES (?, 'banned', ?, ?)
    ON CONFLICT(user_id) DO UPDATE SET
      status = 'banned',
      reason = excluded.reason,
      updated_at = excluded.updated_at
    WHERE excluded.updated_at > banned_users.updated_at
  `).run(userId, reason, updatedAt);
  console.log(`🚫 [Ban] Пользователь ${userId.slice(-12)} забанен. Причина: ${reason}`);
  return updatedAt;
}

export function unbanUser(userId) {
  const updatedAt = Date.now();
  db.prepare(`
    INSERT INTO banned_users (user_id, status, reason, updated_at)
    VALUES (?, 'unbanned', NULL, ?)
    ON CONFLICT(user_id) DO UPDATE SET
      status = 'unbanned',
      reason = NULL,
      updated_at = excluded.updated_at
    WHERE excluded.updated_at > banned_users.updated_at
  `).run(userId, updatedAt);
  console.log(`✅ [Ban] Пользователь ${userId.slice(-12)} разбанен.`);
  return updatedAt;
}

export function getBannedUsers() {
  return db.prepare(`SELECT * FROM banned_users WHERE status = 'banned' ORDER BY updated_at DESC`).all();
}

export function getAllBanRecords() {
  // Для bulk-синхронизации между релеями — отдаём ВСЕ строки (и banned, и unbanned),
  // чтобы новый релей узнал не только о банах, но и о разбанах, которые уже произошли
  return db.prepare(`SELECT * FROM banned_users`).all();
}

export function mergeBanRecords(records) {
  if (!records || records.length === 0) return 0;

  const upsert = db.prepare(`
    INSERT INTO banned_users (user_id, status, reason, updated_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(user_id) DO UPDATE SET
      status = excluded.status,
      reason = excluded.reason,
      updated_at = excluded.updated_at
    WHERE excluded.updated_at > banned_users.updated_at
  `);

  const transaction = db.transaction((rows) => {
    let applied = 0;
    for (const row of rows) {
      if (!row.user_id || !row.status || !row.updated_at) continue;
      const result = upsert.run(row.user_id, row.status, row.reason ?? null, row.updated_at);
      if (result.changes > 0) applied++;
    }
    return applied;
  });

  const appliedCount = transaction(records);
  if (appliedCount > 0) {
    console.log(`🗄️ [Ban-Sync] Применено обновлений бана: ${appliedCount}`);
  }
  return appliedCount;
}

/**
 * Закрепляет CID за пользователем при первой регистрации.
 * INSERT OR IGNORE — если запись уже есть, повторный вызов ничего не меняет
 * (первый зарегистрировавший CID считается владельцем).
 */
export function registerFileOwner(cid, ownerId) {
  if (!cid || !ownerId) return false;
  try {
    const result = db.prepare(`
      INSERT OR IGNORE INTO file_owners (cid, owner_id, created_at)
      VALUES (?, ?, ?)
    `).run(cid, ownerId, Date.now());
    return result.changes > 0;
  } catch (err) {
    console.error('❌ [DB] Ошибка регистрации владельца файла:', err);
    return false;
  }
}

export function getFileOwner(cid) {
  if (!cid) return null;
  const row = db.prepare(`SELECT owner_id FROM file_owners WHERE cid = ?`).get(cid);
  return row ? row.owner_id : null;
}

export function deleteFileOwnerRecord(cid) {
  if (!cid) return;
  db.prepare(`DELETE FROM file_owners WHERE cid = ?`).run(cid);
}