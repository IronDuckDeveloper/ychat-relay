import { createHelia } from 'helia';
import { webSockets } from '@libp2p/websockets';
import { identify } from '@libp2p/identify';
import { circuitRelayServer } from '@libp2p/circuit-relay-v2';
import { noise } from '@chainsafe/libp2p-noise';
import { yamux } from '@chainsafe/libp2p-yamux';
import { LevelBlockstore } from 'blockstore-level';
import { FsDatastore } from 'datastore-fs'
import { MemoryDatastore } from 'datastore-core';
import { bootstrap } from '@libp2p/bootstrap';
import { multiaddr } from '@multiformats/multiaddr';
import { gossipsub } from '@chainsafe/libp2p-gossipsub';
import { peerIdFromString } from '@libp2p/peer-id';
import { CONFIG } from './config.js';
import { loadKnownPeersConfig } from './storage.js';
import { ping } from '@libp2p/ping'
import { kadDHT } from '@libp2p/kad-dht';
// import { delegatedHTTPRouting } from '@helia/routers';
import { all } from '@libp2p/websockets/filters';
import { bitswap } from '@helia/block-brokers';

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

// Использование:
  // const rawBlockstore = new LevelBlockstore(CONFIG.ORBITDB_BLOCKS_DIR);
  // const blockstore = new CompatibleBlockstore(rawBlockstore);
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
    blockBrokers: [
      bitswap() // <-- СЕРВЕРУ ТОЖЕ НУЖЕН BITSWAP, здесь ему самое место!
    ],
  //     routers: [
  //   delegatedHTTPRouting('https://delegated-ipfs.dev'),
  //   delegatedHTTPRouting('https://dht.ipfs.io')
  // ],
    libp2p: {
      addresses: {
        listen: ['/ip4/0.0.0.0/tcp/15002/ws'],
        announce: [`/ip4/${CONFIG.MY_PUBLIC_IP}/tcp/15002/ws`]
      },
      transports: [webSockets({
    filter: all // Разрешаем стучаться на любые IP-адреса (важно для тестов и локалки)
  })],
      connectionManager: {
      autoDial: true,
      dialTimeout: 30000 //  30 секунд для relay
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
      // Снабжаем Helia инструментами для поиска блоков в P2P-сети
      // contentRouters: [
      //   delegatedHTTPRouting('https://delegated-ipfs.dev'),
      //   delegatedHTTPRouting('https://dht.ipfs.io')
      // ],
      // routers: ['https://delegated-ipfs.dev'],
      // routing: [
      //   delegatedHTTPRouting('https://delegated-ipfs.dev')
      // ],
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
          reservations: { maxReservations: Infinity },
          advertise: { enabled: true },
          hop: { enabled: true, timeout: 30000 }
        }),
        // Включаем полноценный серверный DHT-режим для релея
        dht: kadDHT({
          clientMode: false,
          kBucketSize: 20, // Сервер участвует в маршрутизации и хранит чужие записи
          validators: {},
          selectors: {},
          allowPublishToZeroPeers: true
        }),
        ping: ping()
      }
    }
  });

  return { heliaInstance, bootstrapList };
}