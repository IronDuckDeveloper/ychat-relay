// src/utils/peerLoader.js
import { multiaddr } from '@multiformats/multiaddr';
import { peerIdFromString } from '@libp2p/peer-id'; // Укажи свой правильный пакет для peerId
import { CONFIG } from '../config.js';

/**
 * Загружает и парсит списки пиров для libp2p
 * @param {Function} loadKnownPeersConfig - функция чтения JSON-файла (передаем как аргумент)
 * @returns {{ bootstrapList: string[], directPeersList: Array }}
 */
export function loadNetworkPeers(loadKnownPeersConfig) {
  let bootstrapList = [];
  let directPeersList = [];

  // Ветка 1: Если задан список в ENV
  if (process.env.BOOTSTRAP_LIST) {
    bootstrapList = process.env.BOOTSTRAP_LIST.split(',').map(s => s.trim()).filter(Boolean);
    
    bootstrapList.forEach(peerStr => {
      try {
        const parts = peerStr.split('/p2p/');
        if (parts.length === 2) {
          const relayAddress = parts[0];
          const relayPeerId = parts[1];

          if (CONFIG.NETWORK.IP && relayAddress.includes(CONFIG.NETWORK.IP)) return;

          directPeersList.push({
            id: peerIdFromString(relayPeerId),
            addrs: [multiaddr(relayAddress)]
          });
        }
      } catch (err) {
        console.warn(`⚠️ Не удалось распарсить пир для directPeersList из ENV: ${peerStr}`, err.message);
      }
    });
    console.log(`🚀 Загружено соседей из ENV (bootstrap + directPeers): ${bootstrapList.length}`);
  } 
  // Ветка 2: Если ENV пуст, читаем локальный JSON файл
  else {
    const config = loadKnownPeersConfig();
    if (config && config.relays && Array.isArray(config.relays)) {
      config.relays.forEach(relay => {
        if (CONFIG.NETWORK.IP && relay.address.includes(CONFIG.NETWORK.IP)) return;

        bootstrapList.push(`${relay.address}/p2p/${relay.peerId}`);
        directPeersList.push({
          id: peerIdFromString(relay.peerId),
          addrs: [multiaddr(relay.address)]
        });
      });
      console.log(`📂 Загружено соседей из файла: ${bootstrapList.length}`);
    }
  }

  // Возвращаем оба сформированных списка одним объектом
  return { bootstrapList, directPeersList };
}