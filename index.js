import { createHelia } from 'helia'
import { webSockets } from '@libp2p/websockets'
import { identify } from '@libp2p/identify'
import { circuitRelayServer } from '@libp2p/circuit-relay-v2'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { FsDatastore } from 'datastore-fs'
import { bootstrap } from '@libp2p/bootstrap'
import { multiaddr } from '@multiformats/multiaddr'
import { writeFileSync, readFileSync, existsSync } from 'fs'
import { gossipsub } from '@chainsafe/libp2p-gossipsub'
import { peerIdFromString } from '@libp2p/peer-id'

// === [ СЕКЦИЯ КОНФИГУРАЦИИ ] ===

const MY_PUBLIC_IP = process.env.MY_PUBLIC_IP

if (!MY_PUBLIC_IP) {
  console.error('❌ ОШИБКА: Не задана переменная MY_PUBLIC_IP!');
  console.error('👉 Запуск: MY_PUBLIC_IP="1.2.3.4" node index.js');
  process.exit(1);
}

const DATA_DIR = './data'
const ROOMS_FILE = './subscribed-rooms.json'

const SYNC_REQUEST_TOPIC = 'rooms:sync:request'
const SYNC_RESPONSE_TOPIC_BASE = 'rooms:sync:response:'
const ANNOUNCE_TOPIC = 'rooms:announce'

const KNOWN_PEERS_FILE = './data/known-peers.json'

let bootstrapList = []
let directPeersList = []

// 1. Сначала проверяем переменную окружения (высокий приоритет)
if (process.env.BOOTSTRAP_LIST) {
  bootstrapList = process.env.BOOTSTRAP_LIST.split(',').map(s => s.trim()).filter(Boolean)
}
// 2. Если пуста — парсим твой файл known-peers.json
else if (existsSync(KNOWN_PEERS_FILE)) {
  try {
    const config = JSON.parse(readFileSync(KNOWN_PEERS_FILE, 'utf-8'))
    
    if (config.relays && Array.isArray(config.relays)) {
      config.relays.forEach(relay => {
        // Пропускаем самого себя, чтобы не пытаться соединиться с собой по внешнему IP
        if (MY_PUBLIC_IP && relay.address.includes(MY_PUBLIC_IP)) return

        const fullAddr = `${relay.address}/p2p/${relay.peerId}`
        bootstrapList.push(fullAddr)

        directPeersList.push({
          id: peerIdFromString(relay.peerId),
          addrs: [multiaddr(relay.address)]
        })
      })
      console.log(`📂 Загружено соседей из файла: ${bootstrapList.length}`)
    }
  } catch (e) {
    console.error('❌ Ошибка парсинга known-peers.json:', e.message)
  }
}

// ===============================

const datastore = new FsDatastore(DATA_DIR)

const heliaInstance = await createHelia({
  datastore,
  libp2p: {
    addresses: {
      listen: ['/ip4/0.0.0.0/tcp/15002/ws'],
      announce: [`/ip4/${MY_PUBLIC_IP}/tcp/15002/ws`]
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
           // ✅ Включить PX
        doPX: true,          
           // ✅ Параметры Mesh
            D: 3,
            Dlo: 2,
            Dhi: 5,
            Dscore: 1,
            heartbeatInterval: 1000,
            directPeers: directPeersList,
           // ✅ Отключение скоринга (браузеры не должны пессимизироваться)
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
        })
,
      relay: circuitRelayServer({
        reservations: { maxReservations: Infinity },
        advertise: { enabled: true },
        hop: { enabled: true }
      })
    }
  }
})

const node = heliaInstance.libp2p
const pubsub = node.services.pubsub
const subscribedTopics = new Set()
let syncCompleted = false

// --- [ ЛОГИКА ПОДПИСОК И СООБЩЕНИЙ ] ---

async function safeSubscribe(room) {
  if (!room || subscribedTopics.has(room)) return
  try {
    await pubsub.subscribe(room)
    subscribedTopics.add(room)
    console.log(`🎯 [TOPIC] Подписан на: ${room}`)
  } catch (e) {
    console.error(`❌ Ошибка подписки на ${room}:`, e.message)
  }
}

const messageHandler = (evt) => {
  const msg = evt.detail || evt
  const topic = msg.topic
  
  // 1. Фильтруем служебные топики для чистоты логов
  if (topic === ANNOUNCE_TOPIC || topic.includes('sync')) return

  try {
    const text = new TextDecoder().decode(msg.data)
    console.log(`📩 [${topic}] Сообщение от ${msg.from.toString().slice(-6)}: ${text}`)
  } catch (e) {
    console.log(`📩 [${topic}] Получены бинарные данные от ${msg.from.toString().slice(-6)}`)
  }
}
pubsub.addEventListener('message', messageHandler)

// --- [ СИНХРОНИЗАЦИЯ ] ---

const generalMessageHandler = async (evt) => {
  const message = evt.detail || evt
  const { topic, data, from } = message
  let text = ''
  try { text = new TextDecoder().decode(data) } catch (e) { return }

  // Обработка объявлений от браузеров
  if (topic === ANNOUNCE_TOPIC) {
    try {
      const { room } = JSON.parse(text)
      if (room) await safeSubscribe(room)
    } catch (e) {}
  }

  // Ответ на запрос синхронизации от другого сервера
  if (topic === SYNC_REQUEST_TOPIC) {
    try {
      const payload = JSON.parse(text)
      const target = payload?.from
      if (!target || target === node.peerId.toString()) return

      const rooms = Array.from(subscribedTopics).filter(t => 
        t !== ANNOUNCE_TOPIC && !t.includes('sync')
      )
      const responseTopic = `${SYNC_RESPONSE_TOPIC_BASE}${target}`
      const responsePayload = JSON.stringify({ rooms })

      await pubsub.publish(responseTopic, new TextEncoder().encode(responsePayload))
      console.log(`📤 [SYNC] Отправлен список (${rooms.length} комнат) пиру ${target.slice(-6)}`)
    } catch (e) {}
  }
}
pubsub.addEventListener('message', generalMessageHandler)

async function requestSyncViaPubSub(timeoutMs = 7000) {
  const myPeerId = node.peerId.toString()
  const responseTopic = `${SYNC_RESPONSE_TOPIC_BASE}${myPeerId}`
  let received = false

  const onResponse = async (evt) => {
    const msg = evt.detail || evt
    if (msg.topic !== responseTopic) return
    try {
      const payload = JSON.parse(new TextDecoder().decode(msg.data))
      if (payload?.rooms) {
        console.log(`📥 [SYNC] Получено комнат: ${payload.rooms.length}`)
        for (const room of payload.rooms) {
          await safeSubscribe(room)
        }
        received = true
      }
    } catch (e) {}
  }

  await pubsub.subscribe(responseTopic)
  pubsub.addEventListener('message', onResponse)

  console.log('📢 [SYNC] Запрашиваю список комнат у соседей...')
  const reqPayload = JSON.stringify({ from: myPeerId })
  await pubsub.publish(SYNC_REQUEST_TOPIC, new TextEncoder().encode(reqPayload))

  await new Promise(r => setTimeout(r, timeoutMs))
  pubsub.removeEventListener('message', onResponse)
  return received
}

// --- [ СТАРТ И ВОССТАНОВЛЕНИЕ ] ---

function loadStoredRooms() {
  if (existsSync(ROOMS_FILE)) {
    try {
      const rooms = JSON.parse(readFileSync(ROOMS_FILE, 'utf-8'))
      return rooms
    } catch (e) { return [] }
  }
  return []
}

// 1. Подписываемся на базу
await pubsub.subscribe(ANNOUNCE_TOPIC)
await pubsub.subscribe(SYNC_REQUEST_TOPIC)
subscribedTopics.add(ANNOUNCE_TOPIC)
subscribedTopics.add(SYNC_REQUEST_TOPIC)

// 2. Восстанавливаем старое
const saved = loadStoredRooms()
for (const r of saved) await safeSubscribe(r)

console.log('🔗 PeerID:', node.peerId.toString())
console.log('🚀 SERVER READY')

// 3. Пытаемся синхронизироваться при подключении нового пира
node.addEventListener('peer:connect', async (evt) => {
  const peerId = evt.detail.toString()
  console.log(`🤝 Подключен пир: ${peerId.slice(-6)}`)
  
  if (!syncCompleted) {
    // Небольшая задержка, чтобы протоколы успели обменяться IDENTIFY
    await new Promise(r => setTimeout(r, 2000))
    const ok = await requestSyncViaPubSub()
    if (ok) {
      syncCompleted = true
      // Сохраняем результат
      const toSave = Array.from(subscribedTopics).filter(t => t !== ANNOUNCE_TOPIC && !t.includes('sync'))
      writeFileSync(ROOMS_FILE, JSON.stringify(toSave, null, 2))
    }
  }
})

// 4. План Б: Форсированный коннект
setInterval(async () => {
  if (node.getPeers().length === 0 && bootstrapList.length > 0) {
    try {
      await node.dial(multiaddr(bootstrapList))
      console.log('🛠 [MAINTENANCE] Форсированный dial к соседу...')
    } catch (e) {
      console.log('🛠 [MAINTENANCE] Не удалось подключиться к соседу:', e.message)
    }
  }
}, 15000)

// 5. Мониторинг
setInterval(() => {
  console.log(`\n📊 Статус: Пиров: ${node.getPeers().length} | Топиков: ${pubsub.getTopics().length} | Синх: ${syncCompleted ? '✅' : '⏳'}`)
}, 10000)

