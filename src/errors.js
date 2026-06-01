// src/errors.js

/**
 * 🛡️ ГЛОБАЛЬНЫЙ ФИЛЬТР МУСОРА ОТ ORBITDB И P2P
 * Глушит ожидаемые сетевые ворнинги и ошибки отсутствия блоков,
 * чтобы не забивать логи релея фоновым шумом.
 */

process.on('unhandledRejection', (reason) => {
  const errName = reason?.name || '';
  const errCode = reason?.code || '';
  const msg = reason?.message || '';

  // 1. Игнорируем ошибки отсутствия данных в IPFS/Blockstore
  if (errCode === 'ERR_NOT_FOUND' || errName === 'NotFoundError' || errName === 'AbortError') {
    return;
  }
  
  // 2. Игнорируем рассинхрон протоколов (например, при согласовании версий с другими нодами)
  if (errCode === 'ERR_UNSUPPORTED_PROTOCOL' || msg.includes('protocol selection failed')) {
    return;
  }

  // 3. Игнорируем естественные обрывы связи при отключении клиентов
  if (
    msg.includes('stream reset') || 
    msg.includes('The operation was aborted') || 
    msg.includes('unexpected end of input')
  ) {
    return;
  }

  // Все остальные реальные ошибки выводим в консоль
  console.error('❌ Неперехваченная ошибка промиса (Unhandled Rejection):', reason);
});

process.on('uncaughtException', (err) => {
  const msg = err?.message || '';
  const code = err?.code || '';
  const name = err?.name || '';

  // Игнорируем внезапные обрывы P2P-стримов, когда клиент просто закрыл вкладку браузера
  if (name === 'StreamResetError' || code === 'ERR_STREAM_RESET' || msg.includes('stream reset')) {
    console.log('⚠️ [Network] Игнорируем обрыв P2P-стрима (клиент отключился)');
    return;
  }

  // Критические ошибки ломают процесс, их обязательно логируем
  console.error('🔥 КРИТИЧЕСКАЯ ОШИБКА (Uncaught Exception):', err);
  // В продакшене здесь обычно вызывают process.exit(1), 
  // но для релея в процессе разработки оставляем мягкое логирование
});

console.log('🛡️  Глобальный фильтр сетевых ошибок успешно подключен');