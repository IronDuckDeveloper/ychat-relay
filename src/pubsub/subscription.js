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