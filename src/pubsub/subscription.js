export const subscribedTopics = new Set();


export async function safeSubscribe(pubsub, room) {
  if (!room || subscribedTopics.has(room)) return;
  try {
    await pubsub.subscribe(room);
    subscribedTopics.add(room);
    console.log(`🎯 [TOPIC] Подписан на: ${room}`);
  } catch (e) {
    console.error(`❌ Ошибка подписки на ${room}:`, e.message);
  }
}

export async function safeUnsubscribe(pubsub, room) {
  if (!room || !subscribedTopics.has(room)) return;
  try {
    await pubsub.unsubscribe(room);
    subscribedTopics.delete(room);
    console.log(`🚫 [TOPIC] Отписан от: ${room}`);
  } catch (e) {
    console.error(`❌ Ошибка отписки от ${room}:`, e.message);
  }
}