const MY_PUBLIC_IP = process.env.MY_PUBLIC_IP;

if (!MY_PUBLIC_IP) {
  console.error('❌ ОШИБКА: Не задана переменная MY_PUBLIC_IP!');
  console.error('👉 Запуск: MY_PUBLIC_IP="1.2.3.4" node src/index.js');
  process.exit(1);
}

export const CONFIG = {
  MY_PUBLIC_IP,
  NODE_NAME: process.env.NODE_NAME || `Node-${MY_PUBLIC_IP.split('.').pop()}`,
  DATA_DIR: './data',
  ROOMS_FILE: './subscribed-rooms.json',
  KNOWN_PEERS_FILE: './data/known-peers.json',
  
  TOPICS: {
    SYNC_REQUEST: 'rooms:sync:request',
    SYNC_RESPONSE_BASE: 'rooms:sync:response:',
    ANNOUNCE: 'rooms:announce',
    PEER_SYNC_REQUEST: 'peers:sync:request',
    PEER_SYNC_RESPONSE_BASE: 'peers:sync:response:'
  }
};