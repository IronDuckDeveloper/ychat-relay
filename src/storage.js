import { writeFileSync, readFileSync, existsSync } from 'fs';
import { CONFIG } from './config.js';

export function loadStoredRooms() {
  if (existsSync(CONFIG.ROOMS_FILE)) {
    try {
      return JSON.parse(readFileSync(CONFIG.ROOMS_FILE, 'utf-8'));
    } catch (e) { return []; }
  }
  return [];
}

export function saveRooms(roomsArray) {
  writeFileSync(CONFIG.ROOMS_FILE, JSON.stringify(roomsArray, null, 2));
}

export function loadKnownPeersConfig() {
  if (existsSync(CONFIG.KNOWN_PEERS_FILE)) {
    try {
      return JSON.parse(readFileSync(CONFIG.KNOWN_PEERS_FILE, 'utf-8'));
    } catch (e) {
      console.error('❌ Ошибка парсинга known-peers.json:', e.message);
    }
  }
  return { relays: [] };
}

export function saveKnownPeersConfig(config) {
  writeFileSync(CONFIG.KNOWN_PEERS_FILE, JSON.stringify(config, null, 2));
}