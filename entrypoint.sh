#!/bin/sh

# set -e: Exit immediately if a command exits with a non-zero status.
# This is a good practice for scripts to avoid unexpected behavior.
set -e

# Функция для проверки доступности хоста через DNS
wait_for_host() {
  host_to_check=$1
  echo "Entrypoint: Waiting for DNS resolution for '$host_to_check'..."
  # Пытаемся разрешить хост в цикле с задержкой. Утилита 'host' установлена в Dockerfile.
  until host "$host_to_check" > /dev/null 2>&1; do
    echo "Entrypoint: Host '$host_to_check' is not yet resolvable, retrying in 2 seconds..."
    sleep 2
  done
  echo "Entrypoint: Host '$host_to_check' is resolvable."
}

# Загружаем переменные из .env.server, чтобы получить WEBHOOK_HOST
if [ -f .env.server ]; then
  # Используем 'grep' и 'cut' вместо 'source', чтобы избежать выполнения команд из .env файла
  WEBHOOK_HOST_URL=$(grep '^WEBHOOK_HOST=' .env.server | cut -d'=' -f2-)
fi

# Проверяем ключевые хосты, которые необходимы для работы бота
wait_for_host "api.telegram.org"
if [ -n "$WEBHOOK_HOST_URL" ]; then
  # Извлекаем только имя хоста из URL (убираем протокол и путь)
  WEBHOOK_HOSTNAME=$(echo "$WEBHOOK_HOST_URL" | sed -e 's|^https\?://||' -e 's|/.*$||')
  wait_for_host "$WEBHOOK_HOSTNAME"
fi

echo "Entrypoint: All checks passed. Starting application..."

# Заменяем текущий процесс (скрипт) на команду, переданную в CMD Dockerfile (python i_m.py).
# Это КЛЮЧЕВОЙ момент. Без 'exec' скрипт завершится, и контейнер остановится вместе с ним.
exec "$@"