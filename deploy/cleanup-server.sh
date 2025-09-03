#!/bin/bash
#
# Скрипт для ПОЛНОГО УДАЛЕНИЯ бота и всех его данных с сервера.
# ВНИМАНИЕ: Это действие необратимо!
#
# Что делает скрипт:
# 1. Проверяет права суперпользователя (root).
# 2. Принимает имя пользователя для удаления в качестве аргумента.
# 3. Останавливает и удаляет Docker-контейнер, сеть и тома.
# 4. Удаляет пользователя и его домашнюю директорию.
# 5. Удаляет связанные конфигурационные файлы (SSH, sudoers).
# 6. Удаляет сам себя.
#

set -euo pipefail

# --- Функции ---

check_root() {
  if [ "$(id -u)" -ne 0 ]; then
    echo "Ошибка: Этот скрипт должен выполняться с правами root." >&2
    exit 1
  fi
}

# --- Основная логика ---

main() {
  check_root

  if [ "$#" -ne 1 ]; then
    echo "Ошибка: Не указано имя пользователя для удаления." >&2
    echo "Использование: $0 <имя_пользователя>" >&2
    exit 1
  fi

  local user_to_delete="$1"
  local work_dir="/home/${user_to_delete}"
  local compose_file="${work_dir}/docker-compose.yml"

  echo "--- Начало полного удаления для пользователя: ${user_to_delete} ---"

  # 1. Остановить и удалить Docker-ресурсы
  if [ -f "${compose_file}" ]; then
    echo "Найден docker-compose.yml. Останавливаем и удаляем сервисы..."
    # Запускаем от имени пользователя, чтобы docker-compose нашел .env файл
    sudo -u "${user_to_delete}" docker-compose --project-directory "${work_dir}" down --rmi all --volumes --remove-orphans || echo "Не удалось остановить docker-compose, возможно, сервисы уже остановлены. Продолжаем."
  else
    echo "Файл docker-compose.yml не найден, пропускаем остановку сервисов."
  fi

  # 2. Удалить пользователя и его домашнюю директорию
  if id -u "${user_to_delete}" >/dev/null 2>&1; then
    echo "Завершение всех сессий и процессов пользователя ${user_to_delete}..."
    # Это необходимо, чтобы userdel не выдавал ошибку "user is currently used by process".
    # Сначала пытаемся использовать loginctl, так как это более "чистый" способ.
    if command -v loginctl >/dev/null 2>&1; then
      loginctl terminate-user "${user_to_delete}" || true
    else
      # Если loginctl недоступен, используем pkill как запасной вариант.
      pkill -u "${user_to_delete}" || true
    fi
    sleep 2 # Небольшая пауза, чтобы система успела обработать завершение процессов.
    echo "Удаление пользователя ${user_to_delete} и его домашней директории ${work_dir}..."
    deluser --remove-home "${user_to_delete}" # Теперь удаление должно пройти без ошибок.
    echo "Пользователь ${user_to_delete} удален."
  else
    echo "Пользователь ${user_to_delete} не найден, пропускаем удаление."
  fi

  # 3. Удалить конфигурацию SSH и sudoers
  echo "Очистка системных конфигураций..."
  rm -f "/etc/ssh/sshd_config.d/99-disable-password-auth.conf"
  rm -f "/etc/sudoers.d/99-${user_to_delete}-cleanup"
  rm -f -- "$0" # Удаляем сам скрипт очистки

  echo "Перезапуск SSH сервиса..."
  systemctl restart sshd

  echo "--- Очистка завершена ---"
  echo "ВАЖНО: Не забудьте вручную:"
  echo "  1. Удалить конфигурацию для вашего домена из файла Caddyfile."
  echo "  2. Удалить секреты из настроек репозитория GitHub."
}

main "$@"