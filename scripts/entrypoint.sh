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

# Логируем команду и защищаемся от пустого вызова
echo "Entrypoint will exec the command: $@"
if [ $# -eq 0 ]; then
  echo "ERROR: no command provided to entrypoint (CMD empty). Exiting."
  exit 1
fi

# Если нужно отладить поведение — можно установить KEEP_ALIVE_ON_EXIT=1.
# В этом режиме команда будет выполнена, но при её завершении контейнер не будет падать,
# а будет держаться (tail -f /dev/null) — это помогает изучить логи и состояние контейнера.
if [ "${KEEP_ALIVE_ON_EXIT:-0}" = "1" ]; then
  echo "DEBUG: KEEP_ALIVE_ON_EXIT=1 — running command and keeping container alive on exit for debugging"
  "$@"
  code=$?
  echo "Command exited with code ${code}. Keeping container alive for inspection."
  # Держим контейнер запущенным для возможности зайти внутрь и посмотреть логи
  tail -f /dev/null
else
  # Заменяем текущий процесс на команду (стандартное поведение)
  exec "$@"
fi