#!/bin/bash

# Папки, которые МЫ ИГНОРИРУЕМ (аналог секции ignore в SFTP)
EXCLUDES="--exclude=node_modules --exclude=data --exclude=.git --exclude=.vscode --exclude=deploy.sh --exclude=deploy.example.sh"

echo "🚀 Начинаем деплой на кластер ychat..."

# 1. Загрузка на Сервер А
echo "📦 Отправка на Сервер А (X.X.X.X)..."
rsync -avz $EXCLUDES ./ root@X.X.X.X:/root/ychat-relay/

# 2. Загрузка на Сервер Б
echo "📦 Отправка на Сервер Б (X.X.X.X)..."
rsync -avz $EXCLUDES ./ root@X.X.X.X:/root/ychat-relay/

echo "✅ Все файлы успешно обновлены на обоих серверах!"
