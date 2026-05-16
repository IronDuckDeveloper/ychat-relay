import { createHelia } from 'helia';
import { webSockets } from '@libp2p/websockets';
import { identify } from '@libp2p/identify';
import { circuitRelayServer } from '@libp2p/circuit-relay-v2';
import { noise } from '@chainsafe/libp2p-noise';
import { yamux } from '@chainsafe/libp2p-yamux';
import { FsDatastore } from 'datastore-fs';
import { bootstrap } from '@libp2p/bootstrap';
import { multiaddr } from '@multiformats/multiaddr';
import { gossipsub } from '@chainsafe/libp2p-gossipsub';
import { peerIdFromString } from '@libp2p/peer-id';

import { CONFIG } from './config.js';
import { loadKnownPeersConfig } from './storage.js';

export async function createRelayNode() {
  let bootstrapList = [];
  let directPeersList = [];

  // Загрузка пиров из ENV или файла
  if (process.env.BOOTSTRAP_LIST) {
    // Проверяем переменную окружения 
    bootstrapList = process.env.BOOTSTRAP_LIST.split(',').map(s => s.trim()).filter(Boolean);
  } else {
    // Если пуста — парсим файл known-peers.json
    const config = loadKnownPeersConfig();
    if (config.relays && Array.isArray(config.relays)) {
      config.relays.forEach(relay => {
        // Пропускаем самого себя IP
        if (CONFIG.MY_PUBLIC_IP && relay.address.includes(CONFIG.MY_PUBLIC_IP)) return;

        bootstrapList.push(`${relay.address}/p2p/${relay.peerId}`);
        directPeersList.push({
          id: peerIdFromString(relay.peerId),
          addrs: [multiaddr(relay.address)]
        });
      });
      console.log(`📂 Загружено соседей из файла: ${bootstrapList.length}`);
    }
  }

  const datastore = new FsDatastore(CONFIG.DATA_DIR);

  const heliaInstance = await createHelia({
    datastore,
    libp2p: {
      addresses: {
        listen: ['/ip4/0.0.0.0/tcp/15002/ws'],
        announce: [`/ip4/${CONFIG.MY_PUBLIC_IP}/tcp/15002/ws`]
      },
      transports: [webSockets()],
      connectionEncrypters: [noise()],
      connectionGater: {
        denyInboundConnection: () => false,
        denyOutboundConnection: () => false,
      },
      streamMuxers: [yamux()],
      peerDiscovery: bootstrapList.length > 0 ? [bootstrap({ list: bootstrapList })] : [],
      services: {
        identify: identify(),
        pubsub: gossipsub({
          canRelayMessage: true,
          doPX: true,          
          D: 3, Dlo: 2, Dhi: 5, Dscore: 1,
          heartbeatInterval: 1000,
          directPeers: directPeersList,
          scoreThresholds: {
            gossipThreshold: -Infinity,
            publishThreshold: -Infinity,
            graylistThreshold: -Infinity,
            acceptPXThreshold: -Infinity,
            opportunisticGraftThreshold: -Infinity
          },
          scoreParams: {
            IPColocationFactorWeight: 0,
            behaviourPenaltyWeight: 0
          },
          fallbackToFloodsub: true,
          allowPublishToZeroTopicPeers: true
        }),
        relay: circuitRelayServer({
          reservations: { maxReservations: Infinity },
          advertise: { enabled: true },
          hop: { enabled: true }
        })
      }
    }
  });

  return { heliaInstance, bootstrapList };
}