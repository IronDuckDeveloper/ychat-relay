const MY_PUBLIC_IP = process.env.MY_PUBLIC_IP;

if (!MY_PUBLIC_IP) {
  console.error('❌ ОШИБКА: Не задана переменная MY_PUBLIC_IP!');
  console.error('👉 Запуск: MY_PUBLIC_IP="1.2.3.4" node src/index.js');
  process.exit(1);
}

export const CONFIG = {
  MY_PUBLIC_IP,
  NODE_NAME: process.env.NODE_NAME || `Node-${MY_PUBLIC_IP.split('.').pop()}`,
  ORBITDB_DIR: './data/orbitdb',
  ORBITDB_BLOCKS_DIR: './data/blocks.level',
  DATA_DIR: './data',
  KNOWN_PEERS_FILE: './data/known-peers.json',
  
  
  TOPICS: {
    ANNOUNCE: '/p2p-relay/v1/announce',
    PEER_SYNC_REQUEST: 'peers:sync:request',
    PEER_SYNC_RESPONSE_BASE: 'peers:sync:response:',
  },
  ARCHIVIST: {
    INACTIVITY_TIMEOUT_MS: 90 * 24 * 60 * 60 * 1000 // 3 месяца (90 дней) //ПОКА НЕ ИСПОЛЬЗУЕТСЯ
  }
};