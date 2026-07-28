#!/bin/sh
echo "🚀 [Kubo Init] Применяем серверный профиль..."
ipfs config profile apply server

echo "🔌 [Kubo Init] Открываем API и Gateway..."
ipfs config Addresses.API /ip4/0.0.0.0/tcp/5001
ipfs config Addresses.Gateway /ip4/0.0.0.0/tcp/8080

echo "🛡️ [Kubo Init] Разрешаем CORS запросы для API (RPC uploads)..."
ipfs config --json API.HTTPHeaders.Access-Control-Allow-Origin '["*"]'
ipfs config --json API.HTTPHeaders.Access-Control-Allow-Methods '["PUT", "POST", "GET", "OPTIONS"]'
ipfs config --json API.HTTPHeaders.Access-Control-Allow-Headers '["Content-Type", "Authorization"]'

echo "🛡️ [Kubo Init] Разрешаем CORS запросы для Gateway (HTTP downloads)..."
ipfs config --json Gateway.HTTPHeaders.Access-Control-Allow-Origin '["*"]'
ipfs config --json Gateway.HTTPHeaders.Access-Control-Allow-Methods '["GET", "OPTIONS"]'

echo "🧹 [Kubo Init] Изолируем сеть: удаляем публичные бутстрап-узлы..."
ipfs bootstrap rm --all

echo "🔓 [Kubo Init] Разрешаем внутреннюю сеть..."
ipfs config --json Swarm.AddrFilters '[]'

echo "🛑 [Kubo Init] Отключаем DHT и Delegated Routers (работаем в режиме автономного хранилища)..."
ipfs config --json Routing.DelegatedRouters '[]'
ipfs config Routing.Type "none"

echo "✅ [Kubo Init] Конфигурация успешно применена!"