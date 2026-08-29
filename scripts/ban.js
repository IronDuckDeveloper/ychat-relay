import { initDatabase, getBannedUsers, isUserBanned } from '../src/database/db.js';

/**
    docker exec -it ychat-relay node scripts/ban.js ban 12D3KooW... "спам"
    docker exec -it ychat-relay node scripts/ban.js unban 12D3KooW...
    docker exec -it ychat-relay node scripts/ban.js list
    docker exec -it ychat-relay node scripts/ban.js check 12D3KooW...
 */

const [, , command, ...args] = process.argv;

const RELAY_INTERNAL_URL = process.env.RELAY_INTERNAL_URL || 'http://localhost:15004'; // 👈 HTTP_PORT, не 15003
const CLUSTER_SECRET = process.env.CLUSTER_SECRET;

function printUsage() {
  console.log(`
Использование:
  node scripts/ban.js ban <peerId> "<причина>"
  node scripts/ban.js unban <peerId>
  node scripts/ban.js list
  node scripts/ban.js check <peerId>
`);
}

async function callInternalRoute(path, body) {
  if (!CLUSTER_SECRET) {
    console.error('❌ Переменная окружения CLUSTER_SECRET не задана.');
    process.exit(1);
  }

  const response = await fetch(`${RELAY_INTERNAL_URL}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-internal-secret': CLUSTER_SECRET },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    console.error(`❌ Ошибка: ${response.status} ${await response.text()}`);
    process.exit(1);
  }
  return response;
}

initDatabase();

switch (command) {
  case 'ban': {
    const [peerId, ...reasonParts] = args;
    if (!peerId) { printUsage(); process.exit(1); }
    await callInternalRoute('/api/internal/ban', { userId: peerId, reason: reasonParts.join(' ') || 'Не указана' });
    console.log(`🚫 Забанен и разослан по кластеру: ${peerId}`);
    break;
  }
  case 'unban': {
    const [peerId] = args;
    if (!peerId) { printUsage(); process.exit(1); }
    await callInternalRoute('/api/internal/unban', { userId: peerId });
    console.log(`✅ Разбанен и разослан по кластеру: ${peerId}`);
    break;
  }
  case 'list': {
    const banned = getBannedUsers();
    if (banned.length === 0) {
      console.log('Список бана пуст.');
    } else {
      console.log(`\nЗабаненные пользователи (${banned.length}):\n`);
      console.table(banned.map(b => ({
        PeerID: b.user_id,
        Причина: b.reason,
        Обновлено: new Date(b.updated_at).toLocaleString('ru-RU'),
      })));
    }
    break;
  }
  case 'check': {
    const [peerId] = args;
    if (!peerId) { printUsage(); process.exit(1); }
    console.log(isUserBanned(peerId) ? `🚫 ${peerId} забанен.` : `✅ ${peerId} не забанен.`);
    break;
  }
  default:
    printUsage();
}

process.exit(0);