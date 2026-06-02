// Теперь MY_PUBLIC_IP доступен в этом файле
const MY_PUBLIC_IP = process.env.MY_PUBLIC_IP; 

// Проверяем, что MY_PUBLIC_IP задана при запуске
if (!MY_PUBLIC_IP) {
  console.error('❌ ОШИБКА: Не задана переменная MY_PUBLIC_IP!');
  console.error('👉 Запуск: MY_PUBLIC_IP="1.2.3.4" node src/index.js');
  process.exit(1);
}

export const CONFIG = {
  MY_PUBLIC_IP, // MY_PUBLIC_IP теперь доступен в CONFIG
  NODE_NAME: process.env.NODE_NAME || `Node-${MY_PUBLIC_IP.split('.').pop()}`, // NODE_NAME теперь доступен в CONFIG
  ORBITDB_DIR: './data/orbitdb', // Директория для хранения данных OrbitDB
  ORBITDB_BLOCKS_DIR: './data/blocks.level', // Директория для хранения блоков LevelDB
  DATA_DIR: './data', // Общая директория для всех данных
  KNOWN_PEERS_FILE: './data/known-peers.json', // Файл для хранения известных пиров

  DELAY_START_MS: 3000, // АнтиСпам задержка между запросами на одного и того же пира (3 секунды)

  MSG: {
    SUCCESS : 'SUCCESS',
    FORBIDDEN : 'FORBIDDEN',
    REGISTRATION_IS_OVER : 'Registration allowed',
    LIMIT_EXCEEDED : 'Registration limit per device/IP exceeded'
  },

  SQL: {
    DB_PATH: './data/ychat-server.db', // Путь к базе данных
    ONE_YEAR_MS: 365 * 24 * 60 * 60 * 1000, // Количество миллисекунд в одном году
    MAX_REGISTRATIONS: 3, // Максимальное количество регистраций на IP/устройство в год
  },

  TOPICS: {
    ANNOUNCE: '/p2p-relay/v1/announce', // Топик для объявления о себе
    PEER_SYNC_REQUEST: 'peers:sync:request', // Топик для запроса синхронизации пиров
    PEER_SYNC_RESPONSE_BASE: 'peers:sync:response:', // Базовый топик для ответа на синхронизацию пиров (добавляем ID запроса)
    RPC_PROTOCOL: '/ychat/anti-flood/1.0.0' // Протокол для RPC-метода проверки регистрации (антифрод)
  },

  ARCHIVIST: {
    INACTIVITY_TIMEOUT_MS: 20 * 60 * 1000 // Время в миллисекундах до закрытия неактивной комнаты (20 минут)
  }
};