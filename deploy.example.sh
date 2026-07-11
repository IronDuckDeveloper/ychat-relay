#!/bin/bash

# Папки и файлы, которые МЫ ИГНОРИРУЕМ (аналог секции ignore в SFTP)
EXCLUDES="--exclude=node_modules --exclude=data --exclude=.git --exclude=.vscode --exclude=deploy.sh --exclude=deploy.example.sh --exclude=tests --exclude=.env --exclude=.env.example.sh"

echo "🚀 Начинаем деплой на кластер ychat..."

# 1. Загрузка и перезапуск на Сервер А
echo "📦 Отправка на Сервер А (X.X.X.X)..."
rsync -avz $EXCLUDES ./ root@X.X.X.X:/root/ychat-relay/
echo "🔄 Перезапуск Docker на Сервере А..."
ssh root@X.X.X.X "cd /root/ychat-relay && docker-compose up -d --build"

# 2. Загрузка и перезапуск на Сервер Б
echo "📦 Отправка на Сервер Б (X.X.X.X)..."
rsync -avz $EXCLUDES ./ root@X.X.X.X:/root/ychat-relay/
echo "🔄 Перезапуск Docker на Сервере Б..."
ssh root@X.X.X.X "cd /root/ychat-relay && docker-compose up -d --build"

echo "✅ Все файлы успешно обновлены, а контейнеры перезапущены на обоих серверах!"