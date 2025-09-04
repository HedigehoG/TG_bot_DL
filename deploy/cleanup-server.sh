#!/bin/bash
#
# Скрипт для ПОЛНОГО удаления бота, его данных и пользователя с сервера.
# ВНИМАНИЕ: Это действие необратимо.
#
set -euo pipefail

# Проверяем, что скрипт запущен с правами root, так как он будет удалять пользователя.
if [ "$(id -u)" -ne 0 ]; then
  echo "Ошибка: Этот скрипт должен выполняться с правами root (через sudo)." >&2
  exit 1
fi

if [ -z "$1" ]; then
  echo "Ошибка: Имя пользователя для удаления не указано в качестве аргумента." >&2
  echo "Пример использования: sudo $0 <имя_пользователя>" >&2
  exit 1
fi

DEPLOY_USER="$1"
WORK_DIR="/home/${DEPLOY_USER}"

echo "--- Начало полного удаления для пользователя: ${DEPLOY_USER} ---"

# 1. Останавливаем и удаляем Docker-сервисы, связанные с ботом.
if [ -f "${WORK_DIR}/docker-compose.yml" ]; then
  echo "Найден docker-compose.yml. Останавливаем и удаляем сервисы..."
  # Переходим в рабочую директорию, чтобы docker-compose подхватил .env файлы
  cd "${WORK_DIR}"

  # Загружаем переменные из .env.server, чтобы docker-compose мог сделать подстановки (например, ${BOT_NAME})
  if [ -f .env.server ]; then
    set -o allexport
    source .env.server
    set +o allexport
  fi

  # ИСПОЛЬЗУЕМ 'docker compose' (v2) вместо устаревшего 'docker-compose' (v1)
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    docker compose down --volumes --remove-orphans
    echo "Docker-сервисы остановлены и удалены."
  else
    echo "Предупреждение: Команда 'docker compose' не найдена. Пропускаем остановку сервисов."
  fi
  cd /
else
  echo "Файл docker-compose.yml не найден в ${WORK_DIR}. Пропускаем шаг с Docker."
fi

# 2. Завершаем все оставшиеся процессы пользователя.
echo "Завершение всех сессий и процессов пользователя ${DEPLOY_USER}..."
pkill -9 -u "${DEPLOY_USER}" || echo "Не удалось завершить процессы (возможно, их уже нет)."
sleep 2

# 3. Удаляем пользователя и его домашнюю директорию.
echo "Удаление пользователя ${DEPLOY_USER} и его домашней директории ${WORK_DIR}..."
deluser --remove-home "${DEPLOY_USER}" || echo "Предупреждение: Не удалось удалить пользователя (возможно, он уже удален)."

# 4. Удаляем файл sudoers, который разрешал этому пользователю запускать данный скрипт.
rm -f "/etc/sudoers.d/99-${DEPLOY_USER}-cleanup"

echo "--- Очистка для пользователя ${DEPLOY_USER} завершена. ---"