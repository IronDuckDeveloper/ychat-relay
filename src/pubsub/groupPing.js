// Мост для ping-топиков групп. Браузеры подключены только к релею, поэтому релей должен быть
// подписан на топик, пока на него подписан хотя бы один клиент, иначе публикации не дойдут.
const TOPIC_RE = /^ychat\/groups\/grp_(zdpu[1-9A-HJ-NP-Za-km-z]{40,60})\/ping$/;
const MAX_TOPICS = 2000; // защита от клиента, который подписывается на тысячи выдуманных топиков
const SWEEP_MS = 60_000;

export function setupGroupPingBridge(pubsub, isRelayPeer, archivist) {
  const managed = new Set();

  // Клиент подписался на ping группы: группа существует, создатель онлайн. Пиним сами, не дожидаясь анонса
  const pinGroup = (match) => {
    const address = `/orbitdb/${match[1]}`;
    if (archivist.activeRooms.has(address) || archivist.openingPromises.has(address)) return;
    console.log(`📌 [GroupPing] Пиним группу по подписке: ${address.slice(-12)}`);
    archivist.pinRoom(address).catch((err) => console.error('❌ [GroupPing] Ошибка пиннинга:', err.message));
  };

  // Другие релеи не считаем: иначе два релея будут удерживать подписку друг друга вечно
  const clientSubscribers = (topic) =>
    pubsub.getSubscribers(topic).filter((p) => !isRelayPeer(p.toString()));

  pubsub.addEventListener('subscription-change', (evt) => {
    const { peerId, subscriptions } = evt.detail;
    if (isRelayPeer(peerId.toString())) return;

    for (const { topic, subscribe } of subscriptions) {
      const match = subscribe ? TOPIC_RE.exec(topic) : null;
      if (!match) continue;
      pinGroup(match); // при каждой подписке: если прошлая попытка не удалась, следующая запустит новую
      if (managed.has(topic)) continue;
      if (managed.size >= MAX_TOPICS) {
        console.warn('⚠️ [GroupPing] Достигнут лимит ping-топиков, подписка пропущена.');
        continue;
      }
      try {
        pubsub.subscribe(topic);
        managed.add(topic);
        console.log(`🔔 [GroupPing] Подписан на ${topic.slice(-24)} (топиков: ${managed.size})`);
      } catch (err) {
        console.warn('⚠️ [GroupPing] Не удалось подписаться:', err.message);
      }
    }
  });

  const timer = setInterval(() => {
    for (const topic of [...managed]) {
      if (clientSubscribers(topic).length > 0) continue;
      try { pubsub.unsubscribe(topic); } catch { /* уже отписан */ }
      managed.delete(topic);
      console.log(`🔕 [GroupPing] Отписан от ${topic.slice(-24)}: клиентов нет`);
    }
  }, SWEEP_MS);
  timer.unref?.();

  return () => clearInterval(timer);
}