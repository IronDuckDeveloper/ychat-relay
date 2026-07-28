import { createHelia } from 'helia';
import { webSockets } from '@libp2p/websockets';
import { identify } from '@libp2p/identify';
import { circuitRelayServer } from '@libp2p/circuit-relay-v2';
import { noise } from '@chainsafe/libp2p-noise';
import { yamux } from '@chainsafe/libp2p-yamux';
import { LevelBlockstore } from 'blockstore-level';
import { FsDatastore } from 'datastore-fs';
import { bootstrap } from '@libp2p/bootstrap';
import { multiaddr } from '@multiformats/multiaddr';
import { gossipsub } from '@chainsafe/libp2p-gossipsub';
import { peerIdFromString } from '@libp2p/peer-id';
import { ping } from '@libp2p/ping';
import { kadDHT } from '@libp2p/kad-dht';
import { all } from '@libp2p/websockets/filters';
import { bitswap } from '@helia/block-brokers';
import { CONFIG } from '../config.js';
import { loadKnownPeersConfig } from '../storage/peers-config.js';
import { loadNetworkPeers } from '../utils/peerLoader.js';

export async function createRelayNode() {

  const { bootstrapList, directPeersList } = loadNetworkPeers(loadKnownPeersConfig);
  const datastore = new FsDatastore(CONFIG.DATA_DIR);
  const blockstore = new LevelBlockstore(CONFIG.ORBITDB_BLOCKS_DIR);

  await datastore.open();
  await blockstore.open();

  // =======================================================================
  // INIT HELIA
  // =======================================================================
  const heliaInstance = await createHelia({
    blockstore: blockstore,
    datastore: datastore,
    blockBrokers: [bitswap()],
    libp2p: {
      addresses: {
        listen: [`/ip4/0.0.0.0/tcp/${CONFIG.NETWORK.PORT}/ws`],
        announce: [`/ip4/${CONFIG.NETWORK.IP}/tcp/${CONFIG.NETWORK.PORT}/ws`]
      },
      transports: [webSockets({ filter: all })],
      connectionManager: {
        autoDial: true,
        dialTimeout: 30000
      },
      connectionEncrypters: [noise()],
      connectionGater: {
        denyDialMultiaddr: () => false
      },
      streamMuxers: [yamux()],
      peerDiscovery: bootstrapList.length > 0 ? [bootstrap({ 
        list: bootstrapList,
        timeout: 1000,
        tagName: 'bootstrap',
        tagValue: 50,
        tagTTL: 120000
      })] : [],
      services: {
        identify: identify(),
        pubsub: gossipsub({
          emitSelf: true,
          canRelayMessage: true,
          doPX: true,          
          D: 8,
          Dlo: 6,
          Dhi: 12,
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
          reservations: { 
            maxReservations: Infinity,
            reservationTtl: 5 * 60 * 1000 // Время жизни резервации (5 минут)
          },
          advertise: { enabled: true },
          // В v2 лимиты сессии (data и duration) передаются через объект внутри конфигурации сервера
          limit: {
            duration: 300000, // Увеличиваем лимит жизни сквозного туннеля до 5 минут (300000 ms)
            data: 1024 * 1024 * 1024 // 1 GB трафика на сессию
          }
        }),
        dht: kadDHT({
          protocol: '/ychat/kad/1.0.0',
          clientMode: false,
          kBucketSize: 20,
          validators: {},
          selectors: {},
          allowPublishToZeroPeers: true
        }),
        ping: ping()
      }
    }
  });

  // ========================================================
  // ПОДДЕРЖАНИЕ ТОПОЛОГИИ СЕТИ
  // ========================================================
  const node = heliaInstance.libp2p;

  const intervalTopology = setInterval(async () => {
    if (node.getPeers().length === 0 && bootstrapList.length > 0) {
      // Умный перебор: пробуем подключиться к любому случайному из списка бутстрапов
      const randomAddress = bootstrapList[Math.floor(Math.random() * bootstrapList.length)];
      try {
        await node.dial(multiaddr(randomAddress));
        console.log(`🛠️  [MAINTENANCE] Сеть упала до 0 пиров. Успешный форсированный dial к ${randomAddress.slice(0, 30)}...`);
      } catch (e) {
        // Тихо гасим, если этот конкретный сосед лежит
      }
    }
  }, 15000);

  node.addEventListener('stop', () => {
    clearInterval(intervalTopology);
    console.log('📉 [Network] Интервал поддержки拓扑 остановлен.');
  });

  return { heliaInstance, bootstrapList };
}