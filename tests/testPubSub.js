import test from 'node:test';
import assert from 'node:assert';
import { writeFileSync, readFileSync, existsSync, unlinkSync } from 'fs';

// Имитируем CONFIG для теста, чтобы не завязываться на реальные файлы
const TEST_KNOWN_PEERS_FILE = './data/known-peers.test.json';

// Заглушки для функций из storage.js, адаптированные под тестовый файл
function loadTestConfig() {
  if (existsSync(TEST_KNOWN_PEERS_FILE)) {
    return JSON.parse(readFileSync(TEST_KNOWN_PEERS_FILE, 'utf-8'));
  }
  return { relays: [] };
}

function saveTestConfig(config) {
  writeFileSync(TEST_KNOWN_PEERS_FILE, JSON.stringify(config, null, 2));
}

// Наша тестируемая логика обработки (выжимка из pubsub.js)
function processIncomingRelay(payloadRelay) {
  const current = loadTestConfig();
  
  const existingIndex = current.relays.findIndex(r => 
    r.peerId === payloadRelay.peerId || r.address === payloadRelay.address
  );

  if (existingIndex !== -1) {
    const existing = current.relays[existingIndex];
    if (existing.peerId !== payloadRelay.peerId || 
        existing.address !== payloadRelay.address || 
        existing.name !== payloadRelay.name) {
      current.relays[existingIndex] = payloadRelay;
      saveTestConfig(current);
      return 'UPDATED';
    }
    return 'NO_CHANGE';
  } else {
    current.relays.push(payloadRelay);
    saveTestConfig(current);
    return 'ADDED';
  }
}

// === САМИ ТЕСТЫ ===

test('📋 Тестирование адресной книги (known-peers.json)', async (t) => {
  
  // Подготовка: очищаем тестовый файл перед стартом
  if (existsSync(TEST_KNOWN_PEERS_FILE)) unlinkSync(TEST_KNOWN_PEERS_FILE);
  saveTestConfig({ relays: [] });

  await t.test('1. Должен успешно добавить новое реле, если его нет в списке', () => {
    const newRelay = {
      name: "Relay-Main",
      peerId: "ID-AAA",
      address: "/ip4/1.1.1.1/tcp/15003/ws"
    };

    const result = processIncomingRelay(newRelay);
    const config = loadTestConfig();

    assert.strictEqual(result, 'ADDED');
    assert.strictEqual(config.relays.length, 1);
    assert.strictEqual(config.relays[0].peerId, 'ID-AAA');
  });

  await t.test('2. Не должен добавлять дубликат, если данные полностью совпадают', () => {
    const identicalRelay = {
      name: "Relay-Main",
      peerId: "ID-AAA",
      address: "/ip4/1.1.1.1/tcp/15003/ws"
    };

    const result = processIncomingRelay(identicalRelay);
    const config = loadTestConfig();

    assert.strictEqual(result, 'NO_CHANGE');
    assert.strictEqual(config.relays.length, 1); // Размер не вырос!
  });

  await t.test('3. Должен ОБНОВИТЬ запись, если IP совпадает, но изменился PeerID (упала нода)', () => {
    const rebootedRelay = {
      name: "Relay-Main",
      peerId: "ID-NEW-NEW-NEW", // Новый ID
      address: "/ip4/1.1.1.1/tcp/15003/ws"   // Тот же IP
    };

    const result = processIncomingRelay(rebootedRelay);
    const config = loadTestConfig();

    assert.strictEqual(result, 'UPDATED');
    assert.strictEqual(config.relays.length, 1); // Все еще одна запись!
    assert.strictEqual(config.relays[0].peerId, 'ID-NEW-NEW-NEW'); // ID обновился
  });

  await t.test('4. Должен ОБНОВИТЬ запись, если PeerID совпадает, но изменился IP (смена сети)', () => {
    const movedRelay = {
      name: "Relay-Main",
      peerId: "ID-NEW-NEW-NEW", // Тот же ID
      address: "/ip4/2.2.2.2/tcp/15003/ws"   // Новый IP
    };

    const result = processIncomingRelay(movedRelay);
    const config = loadTestConfig();

    assert.strictEqual(result, 'UPDATED');
    assert.strictEqual(config.relays.length, 1); 
    assert.strictEqual(config.relays[0].address, '/ip4/2.2.2.2/tcp/15003/ws'); // IP обновился
  });

  // Чистим за собой
  if (existsSync(TEST_KNOWN_PEERS_FILE)) unlinkSync(TEST_KNOWN_PEERS_FILE);

  //node --test tests/testPubSub.js
});