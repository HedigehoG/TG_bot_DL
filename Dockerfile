# --- Этап 1: Сборщик (Builder) ---
# Используем полный образ Python, который содержит все необходимые инструменты для сборки.
FROM python:3.12 as builder

# Устанавливаем рабочую директорию
WORKDIR /app

# Устанавливаем системные зависимости, необходимые для компиляции
# build-essential - для C/C++ расширений (aiohttp, pydantic-core)
# libffi-dev - часто требуется для криптографических библиотек
# rustc и cargo - для сборки Rust-компонентов (pydantic-core)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libffi-dev \
    curl \
    && curl https://sh.rustup.rs -sSf | sh -s -- -y \
    && . "$HOME/.cargo/env"

# Добавляем cargo в PATH для этой и последующих команд
ENV PATH="/root/.cargo/bin:${PATH}"

# Создаем виртуальное окружение
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Копируем только файл с зависимостями
COPY requirements.txt .

# Устанавливаем зависимости в виртуальное окружение.
# Это кэширует слои и ускоряет последующие сборки, если requirements.txt не менялся.
RUN pip install --no-cache-dir -r requirements.txt


# --- Этап 2: Финальный образ (Final Image) ---
# Используем легковесный образ для уменьшения размера и повышения безопасности.
FROM python:3.12-slim

WORKDIR /app

# Копируем виртуальное окружение со всеми зависимостями из сборщика
COPY --from=builder /opt/venv /opt/venv

# Копируем остальной код приложения
COPY . .

# Активируем виртуальное окружение для всех последующих команд
ENV PATH="/opt/venv/bin:$PATH"

# Указываем команду для запуска приложения
ENTRYPOINT ["./entrypoint.sh"]
CMD ["python", "i_m.py"]