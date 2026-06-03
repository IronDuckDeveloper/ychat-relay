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
    CREATE TABLE IF NOT EXISTS registration_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ip_address TEXT NOT NULL,
      device_hash TEXT NOT NULL,
      profile_address TEXT,
      timestamp INTEGER NOT NULL
    )
  `).run();

  // Создаем индексы для быстрого поиска
  db.prepare('CREATE INDEX IF NOT EXISTS idx_ip ON registration_logs(ip_address)').run();
  db.prepare('CREATE INDEX IF NOT EXISTS idx_device ON registration_logs(device_hash)').run();
  db.prepare('CREATE INDEX IF NOT EXISTS idx_profile ON registration_logs(profile_address)').run();

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
    'SELECT COUNT(*) as count FROM registration_logs WHERE ip_address = ? AND timestamp > ?'
  ).get(ip, timeLimit);

  // 2. Считаем количество регистраций за последний год по Fingerprint устройства
  const deviceCountRow = db.prepare(
    'SELECT COUNT(*) as count FROM registration_logs WHERE device_hash = ? AND timestamp > ?'
  ).get(deviceHash, timeLimit);

  // 3. Проверяем жесткий лимит (не более 3 регистраций в год)
  if (ipCountRow.count >= CONFIG.SQL.MAX_REGISTRATIONS || deviceCountRow.count >= CONFIG.SQL.MAX_REGISTRATIONS) {
    console.warn(`🚨 [Anti-Fraud] Отклонено! Превышен лимит. IP: ${ip} (Рег: ${ipCountRow.count}), FP: ${deviceHash.slice(0, 10)}... (Рег: ${deviceCountRow.count})`);
    return false;
  }

  // 4. Логируем успешную операцию вместе с profileAddress
  const insert = db.prepare(
    'INSERT INTO registration_logs (ip_address, device_hash, profile_address, timestamp) VALUES (?, ?, ?, ?)'
  );
  insert.run(ip, deviceHash, profileAddress, now);

  console.log(`✅ [Anti-Fraud] Регистрация одобрена и залогирована. IP: ${ip}, Profile: ${profileAddress?.slice(0, 15)}...`);
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
    const stmt = db.prepare('SELECT COUNT(*) as count FROM registration_logs WHERE profile_address = ?');
    const result = stmt.get(profileAddress);

    return result.count > 0;
  } catch (error) {
    console.error('❌ [DB Error] Ошибка при проверке существования профиля:', error);
    return false;
  }
}