#!/bin/sh

# Устанавливаем часовой пояс, если переменная TZ задана
if [ -n "$TZ" ]; then
  echo "Установка часового пояса на $TZ"
  ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime
  echo "$TZ" > /etc/timezone
fi

# set -e: Exit immediately if a command exits with a non-zero status.
# This is a good practice for scripts to avoid unexpected behavior.
set -e

# Загружаем переменные из .env.server, чтобы получить WEBHOOK_HOST
if [ -f .env.server ]; then
  # Используем 'grep' и 'cut' вместо 'source', чтобы избежать выполнения команд из .env файла
  WEBHOOK_HOST_URL=$(grep '^WEBHOOK_HOST=' .env.server | cut -d'=' -f2-)
fi

# Собираем список хостов для проверки
HOSTS_TO_CHECK="api.telegram.org"

if [ -n "$WEBHOOK_HOST_URL" ]; then
  # Извлекаем только имя хоста из URL (убираем протокол и путь)
  WEBHOOK_HOSTNAME=$(echo "$WEBHOOK_HOST_URL" | sed -e 's|^https\?://||' -e 's|/.*$||')
  if [ -n "$WEBHOOK_HOSTNAME" ]; then
    HOSTS_TO_CHECK="$HOSTS_TO_CHECK $WEBHOOK_HOSTNAME"
  fi
fi

# Запускаем Python-скрипт для ожидания доступности DNS
echo "Entrypoint: Starting DNS check for hosts: $HOSTS_TO_CHECK"
python wait-for-dns.py $HOSTS_TO_CHECK

echo "Entrypoint: All checks passed. Starting application..."

# Заменяем текущий процесс (скрипт) на команду, переданную в CMD Dockerfile (python i_m.py).
# Это КЛЮЧЕВОЙ момент. Без 'exec' скрипт завершится, и контейнер остановится вместе с ним.
exec "$@"