import dotenv from 'dotenv';

// Подключаем dotenv
dotenv.config();

// Fail-fast проверка секретов
if (!process.env.CLUSTER_SECRET) {
  console.error('❌ КРИТИЧЕСКАЯ ОШИБКА: Переменная CLUSTER_SECRET не задана в .env!');
  process.exit(1);
}

// 1. Сначала извлекаем IP, чтобы использовать его для NODE_NAME
const publicIp = process.env.MY_PUBLIC_IP || '127.0.0.1';

// Парсим список пиров из строки в массив (если переменной нет, будет пустой массив)
const rawBootstrapPeers = process.env.BOOTSTRAP_LIST || '';
const bootstrapPeersArray = rawBootstrapPeers
  .split(',')
  .map(peer => peer.trim())
  .filter(peer => peer.length > 0); // Убираем пустые строки

export const CONFIG = {
  NODE_NAME: process.env.NODE_NAME || `Node-${publicIp.split('.').pop()}`,
  ORBITDB_DIR: './data/orbitdb', // Директория для хранения данных OrbitDB
  ORBITDB_BLOCKS_DIR: './data/blocks.level', // Директория для хранения блоков LevelDB
  DATA_DIR: './data', // Общая директория для всех данных
  KNOWN_PEERS_FILE: './data/known-peers.json', // Файл для хранения известных пиров
  GLOBAL_REGISTRY_ADDRESS: '', // Адрес глобальной базы профилей (будет установлен при старте)

  DELAY_START_MS: 3, // АнтиСпам задержка между запросами на одного и того же пира (3 секунды)
  LIVE_STAMP_TOKEN: 60000, // Время жизни токена для синхронизации пиров (1 минута)

  NETWORK: {
    IP: publicIp, // Защита от undefined
    PORT: parseInt(process.env.PORT || '15003', 10),
    BOOTSTRAP_LIST: bootstrapPeersArray
  },
  SECURITY: {
    clusterSecret: process.env.CLUSTER_SECRET
  },

  MSG: {
    SUCCESS : 'SUCCESS',
    FORBIDDEN : 'FORBIDDEN',
    NOT_FOUND : 'NOT_FOUND',
    INCORRECT_ACTION : 'INCORRECT_ACTION',
    INCORRECT_ACTION_MSG : 'Incorrect action provided',
    LOGIN_SUCCESS : 'Login successful',
    REGISTRATION_IS_OVER : 'Registration allowed',
    LIMIT_EXCEEDED : 'Registration limit per device/IP exceeded',
    EMPTY_FINGERPRINT : 'Empty fingerprint provided',
    PROFILE_NOT_FOUND : 'Profile not found. Please check that the seed phrase is correct.',
    PROFILE_UPDATED: 'PROFILE_UPDATED', // Сообщение об обновлении профиля
  },

  SQL: {
    DB_PATH: './data/ychat-server.db', // Путь к базе данных
    DB_NAME: 'registration_logs', // Имя базы данных
    ONE_YEAR_MS: 365 * 24 * 60 * 60 * 1000, // Количество миллисекунд в одном году
    MAX_REGISTRATIONS: 300, // Максимальное количество регистраций на IP/устройство в год
    ID_COLUMN: 'id',
    IP: 'ip_address',
    DEVICE_HASH: 'device_hash',
    PROFILE_ADDRESS: 'profile_address',
    TIMESTAMP: 'timestamp'
  },

  TOPICS: {
    ANNOUNCE: '/p2p-relay/v1/announce', // Топик для объявления о себе
    PEER_SYNC_REQUEST: 'peers:sync:request', // Топик для запроса синхронизации пиров
    PEER_SYNC_RESPONSE_BASE: 'peers:sync:response:', // Базовый топик для ответа на синхронизацию пиров (добавляем ID запроса)
    PROFILE_UPDATES_TOPIC: 'ychat/profiles/updates', // Топик для обновления профилей
    RPC_PROTOCOL: '/ychat/anti-flood/1.0.0', // Протокол для RPC-метода проверки регистрации (антифрод)
    DB_SYNC: '/ychat/db-sync/1.0.0', // Протокол для синхронизации БД между релеями при старте
    DB_LIVE_SYNC: '/ychat/db-live-sync/1.0.0', // Протокол для живой синхронизации БД при добавлении новых записей
    DB_GLOBAL_SYNC: '/ychat/db-global-sync/1.0.0', // Протокол для глобальной синхронизации БД
  },

  ARCHIVIST: {
    INACTIVITY_TIMEOUT_MS: 20 * 60 * 1000 // Время в миллисекундах до закрытия неактивной комнаты (20 минут)
  }
};