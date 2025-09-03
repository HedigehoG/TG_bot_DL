# Используем официальный образ Python.
# Если что-то не работает, зафиксируйте версию python:3.12-slim или ещё больше python:3.12-slim-bookworm с версией системы образа
# Текущая просто последняя 3я версия
FROM python:3-slim

# Устанавливаем системные зависимости, которые отсутствуют в 'slim' образе,
# но необходимы для надежной работы entrypoint и healthcheck.
# - dnsutils: предоставляет утилиту 'host' для проверки DNS в entrypoint.sh.
# - netbase: предоставляет базовые сетевые файлы (например, /etc/services), необходимые для работы 'host'.
# - curl: используется в healthcheck для проверки состояния приложения.
# - ca-certificates: необходимы для SSL/TLS верификации.
RUN apt-get update && apt-get install -y --no-install-recommends \
    dnsutils \
    netbase \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем рабочую директорию в контейнере
WORKDIR /app

# Копируем файл с зависимостями
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем остальной код приложения
COPY . .

# Делаем скрипт-обертку исполняемым. Он уже скопирован в /app вместе со всем кодом.
RUN chmod +x ./entrypoint.sh

# Теперь entrypoint.sh будет запускаться первым, а затем он запустит команду из CMD.
ENTRYPOINT ["./entrypoint.sh"]
CMD ["python", "i_m.py"]