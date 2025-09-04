import requests
import debugpy # https://github.com/microsoft/debugpy

import asyncio
import aiohttp
import sys
import re
import os
import time
import random
import threading
import logging
from dotenv import load_dotenv
from urllib.parse import urlparse, quote

from instagrapi import Client # Возвращаемся к синхронному instagrapi
from instagrapi.exceptions import ( # Исключения из instagrapi
	LoginRequired,
	ChallengeRequired,
	BadCredentials,
	BadPassword,
	TwoFactorRequired,
	PrivateError,
	ClientError # ClientError также есть в instagrapi
)
from pydantic import ValidationError

# pip install google-genai
from google import genai
from google.genai.types import Tool, GoogleSearch, GenerateContentConfig

from aiogram import Bot, Dispatcher, types, F 
from aiogram.filters import CommandStart, Command, CommandObject
from aiogram.types import Message, URLInputFile, InputMediaVideo, BufferedInputFile
from aiogram.enums import ParseMode, ChatAction
from aiogram.exceptions import TelegramAPIError
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application


import redis.asyncio as redis

import json
import uuid
from bs4 import BeautifulSoup

from typing import Optional

from aiohttp_socks import ProxyConnector

# Импортируем stem для взаимодействия с Tor
from stem.control import Controller
from stem.connection import AuthenticationFailure

# --- Настройка логирования ---
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

# --- Загрузка переменных окружения ---
load_dotenv()
TG_IDS_RAW = os.getenv("TG_IDS")
if not TG_IDS_RAW: exit("TG_IDS is not set")
TG_IDS = TG_IDS_RAW.split(",")  # Список ID администраторов
# --- Настройка Telegram Bot ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN: exit("BOT_TOKEN is not set")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY: exit("GOOGLE_API_KEY is not set")
client = genai.Client() # the API is automatically loaded from the environement variable
MODEL_20 = "gemini-2.0-flash"
MODEL_25 = "gemini-2.5-flash"

# --- Webhook settings ---
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
# Путь для вебхука. Использование токена в пути — простая мера безопасности.
WEBHOOK_PATH = f"/bot{BOT_TOKEN}"
# Убираем возможный слэш в конце WEBHOOK_HOST, чтобы избежать двойных слэшей // в итоговом URL.
BASE_WEBHOOK_URL = f"{WEBHOOK_HOST.rstrip('/')}{WEBHOOK_PATH}"

# --- Web server settings ---
# Адрес и порт, который будет слушать веб-сервер внутри контейнера.
WEB_SERVER_HOST = "0.0.0.0"
WEB_SERVER_PORT = int(os.getenv("LISTEN_PORT", 8080))

# --- Redis ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


# --- insta dev ---
IG_DEVICE_CONFIG = {
	"my_config": {
		# Новые параметры от Samsung Galaxy S23 Ultra для имитации другого устройства
		"user_agent": "Instagram 360.0.0.30.109 Android (33/13; 640dpi; 1440x3088; samsung; SM-S918B; d2s; qcom; en_US; 674228472)",
		"device": {
			"app_version": "360.0.0.30.109",
			"android_version": 33,
			"android_release": "13",
			"dpi": "640dpi",
			"resolution": "1440x3088",
			"manufacturer": "samsung",
			"model": "SM-S918B",
			"device": "d2s",
			"cpu": "qcom",
			"language": "en_US" # Сменим язык на en_US для большей "незаметности"
		}
	}
}

# --- Код для активации отладки через debugpy ---
# Чтобы включить отладку:
# 1. Установите переменную окружения DEBUG_MODE=1 в вашем .env файле.
# 2. Добавьте проброс порта 5678 в docker-compose.yml.
# 3. Пересоздайте контейнер: docker-compose up -d --force-recreate
if os.getenv("DEBUG_MODE") == "1":
	try:
		logging.info("🚀 РЕЖИМ ОТЛАДКИ АКТИВИРОВАН. Ожидание подключения отладчика на порту 5678...")
		debugpy.listen(("0.0.0.0", 5678))
		debugpy.wait_for_client()
	except Exception as e:
		logging.error(f"Не удалось запустить debugpy: {e}")

# --- Bot и Dispatcher ---
bot = Bot(token=BOT_TOKEN) #,session=my_custom_session
dp = Dispatcher()

# --- Глобальный кэш для клиентов Instagrapi ---
# Используем потокобезопасную блокировку, так как доступ к кэшу будет из разных потоков
INSTA_CLIENTS_CACHE = {}
INSTA_CLIENTS_LOCK = threading.Lock()

# --- Функция для классификации сообщений с помощью AI ---
async def classify_message_with_ai(text: str) -> dict:
	# Промпт был переписан для большей ясности и надежности.
	# Он теперь представляет собой чистую "системную инструкцию" для модели Gemini.
	# f-строка используется для удобного встраивания примеров JSON.
	# Внешние {} - для f-строки, сдвоенные внутренние {{}} - для литеральных скобок в итоговой строке.
	prompt = f'''You are a message classifier for a music bot. Your task is to analyze the user's message and return a single, valid JSON object with two keys: "type" and "content". Do not add any other text, just the JSON.

The possible values for "type" are: "song", "instagram_link", "yandex_music_link", "sberzvuk_link", "mts_music_link", or "chat".

Follow these rules for classification:

1.  **Type: "song"**
	*   If the message appears to be a song title and/or artist name (even with typos or incomplete).
	*   The "content" should be a JSON object: {{"song": "Corrected Artist - Corrected Title", "duration": DURATION_IN_SECONDS}}.
	*   Use your knowledge and the provided search tool to find the correct artist, title, and duration in seconds.
	*   If duration is unknown, use 0.
	*   Example: for "Заточка - мкжик", you should return a "song" type with content like {{"song": "Заточка - Последний нормальный мужик", "duration": 266}}.

2.  **Type: "instagram_link"**
	*   If the message is a valid Instagram post link (e.g., `https://www.instagram.com/p/ABC123/`).
	*   The "content" should be a JSON object: {{"shortcode": "SHORTCODE"}}.
	*   Example: for `https://www.instagram.com/p/Cxyz123/`, the shortcode is `Cxyz123`.

3.  **Type: "yandex_music_link"**
	*   If the message is a Yandex Music track link (e.g., `https://music.yandex.com/album/123/track/456`).
	*   The "content" should be a JSON object: {{"track_id": "TRACK_ID"}}.
	*   Example: for the link above, the track_id is `456`.

4.  **Type: "sberzvuk_link"**
	*   If the message is a SberZvuk (zvuk.com) track link (e.g., `https://zvuk.com/track/123`).
	*   The "content" should be a JSON object: {{"track_id": "TRACK_ID"}}.
	*   Example: for the link above, the track_id is `123`.

5.  **Type: "mts_music_link"**
	*   If the message is an MTS Music track link (e.g., `https://music.mts.ru/track/789`).
	*   The "content" should be a JSON object: {{"track_id": "TRACK_ID"}}.
	*   Example: for the link above, the track_id is `789`.

6.  **Type: "chat"**
	*   If the message does not match any of the types above, classify it as "chat".
	*   The "content" should be the original user message as a string.

The user's message will be provided as the main content to process. Analyze it and return only the JSON object.
'''
	try:
		response = await client.aio.models.generate_content(
			model=MODEL_20,			
			contents=text,
			config=GenerateContentConfig(
				tools=[Tool(google_search=GoogleSearch())],
				response_mime_type="application/json",
				system_instruction=prompt,
			),
		)
		return json.loads(response.text)
	except Exception as e:
		logging.error(f"Ошибка классификации AI Gemini: {e}")
		return {"type": "chat", "content": text}


import pyshorteners
def shorten_url(url):
	"""Сокращает URL с помощью TinyURL."""
	s = pyshorteners.Shortener()
	try:
		# Преобразуем URL в строку, если это объект HttpUrl
		url_str = str(url)
		short_url = s.tinyurl.short(url_str)
		return short_url
	except Exception as e:
		logging.error(f"Ошибка при сокращении URL {url}: {e}")
		return None


# --- Функция для смены IP-адреса Tor ---
def check_tor_connection(control_port=9051, cookie_path="/run/tor/control.authcookie", renew=False):
	"""
	Проверяет статус подключения к Tor через контроллер.
	Использует CookieAuthentication.
	"""
	try:
		with Controller.from_port(port=control_port) as controller:
			controller.authenticate()  # cookie auth by default
			logging.info("✅ Успешно подключено к Tor.")

			if controller.is_alive():
				logging.info("🟢 Tor работает. Версия: %s", controller.get_version())
				if renew:
					logging.info("🔄 Новая цепочка Tor (перезапрос IP)...")
					controller.signal("NEWNYM")
					# Даем Tor время на построение новой цепочки
					time.sleep(2)
					# Для проверки IP нужно, чтобы сам запрос шел через Tor
					tor_proxy = "socks5h://127.0.0.1:9050" # Используем socks5h для DNS через прокси
					proxies = { "http": tor_proxy, "https": tor_proxy }
					try:
						# Используем requests с указанием прокси
						response = requests.get('https://check.torproject.org/api/ip', proxies=proxies, timeout=10)
						logging.info(f"Tor IP check: {response.json()}")
					except requests.exceptions.RequestException as e:
						logging.error(f"Ошибка при проверке IP через Tor: {e}")
				return True
			else:
				logging.warning("❌ Контроллер неактивен.")
				return False

	except AuthenticationFailure as e:
		logging.error("❌ Ошибка аутентификации: %s", e)
		return False
	except Exception as e:
		logging.error("❌ Ошибка подключения к Tor: %s", e)
		return False

# --- Настройка прокси для Instagram ---
# Пример: "http://user:password@host:port" или "socks5://host:port"
INSTAGRAM_PROXY = os.getenv("INSTAGRAM_PROXY")

def get_proxy(args=None):
	proxies = {
		"instagram": lambda: INSTAGRAM_PROXY,
		"tor": lambda: "socks5://127.0.0.1:9050" if check_tor_connection() else None,
		"freeproxy": lambda: None
	}
	proxy = proxies.get(args, lambda: None)()
	logging.info(f"Используется прокси: {proxy}")
	return proxy


# --- Командные обработчики ---
@dp.message(CommandStart())
async def command_start_handler(message: Message):
	await message.answer(f"Привет, {message.from_user.full_name}! Я бот с интеллектом от Google Gemini.")


@dp.message(F.text, ~F.text.startswith('/'))
async def ai_router_handler(message: Message):
	processing_msg = await message.reply("🤔 Думаю...")
	classification = await classify_message_with_ai(message.text)
	await processing_msg.delete()
	intent_type, content = classification.get("type"), classification.get("content")
	if intent_type == "instagram_link": await handle_instagram_link(message, content)
	elif intent_type == "yandex_music_link": await handle_yandex_music(message, content)
	elif intent_type == "song": await handle_song_search(message, content)
	elif intent_type == "sberzvuk_link": await handle_sberzvuk_music(message, content)
	elif intent_type == "mts_music_link": await handle_mts_music(message, content)
	else: await handle_chat_request(message, message.text)


# --- Настройки Instagrapi ---
INSTA_REDIS_KEY = 'insta'
# Максимальный размер видео для прямой отправки через Telegram Bot API (в байтах)
MAX_VIDEO_SIZE_BYTES = 50 * 1024 * 1024 # 50 MB

# --- Функция для получения клиента Instagram ---
def get_instagram_client(user_id: str, session_data: dict | None = None, username: str | None = None, password: str | None = None) -> Client | None:
	# --- Попытка 0: Получить клиент из кэша в памяти (потокобезопасно) ---
	cached_client = None
	with INSTA_CLIENTS_LOCK:
		cached_client = INSTA_CLIENTS_CACHE.get(user_id)

	if cached_client:
		try:
			# Быстрая проверка, жива ли сессия (ВНЕ блокировки, чтобы не тормозить другие потоки)
			cached_client.get_timeline_feed()
			logging.info(f"✅ Используется кэшированный клиент instagrapi для user {user_id}")
			return cached_client
		except (LoginRequired, ChallengeRequired, ClientError) as e:
			logging.warning(f"⚠️ Кэшированный клиент для user {user_id} недействителен: {e}. Удаляем из кэша.")
			# Удаляем недействительный клиент под блокировкой во избежание гонки состояний
			with INSTA_CLIENTS_LOCK:
				# Проверяем, что клиент в кэше все еще тот самый, который мы проверяли
				if INSTA_CLIENTS_CACHE.get(user_id) == cached_client:
					del INSTA_CLIENTS_CACHE[user_id]

	# --- Если в кэше нет или он недействителен, создаем новый ---
	new_client = None

	# Попытка 1: Восстановить сессию из Redis
	if session_data:
		cl = Client()
		cl.delay_range = [1, 4]
		proxy = get_proxy("instagram")
		if proxy: cl.set_proxy(proxy)
		try:
			cl.set_settings(session_data)
			cl.get_timeline_feed()
			logging.info(f"✅ Вход по сессии для user {user_id} прошёл успешно")
			new_client = cl
		except Exception as e:
			logging.warning(f"⚠️ Не удалось восстановить сессию для user {user_id}: {e}. Пробуем войти по паролю.")

	# Попытка 2: Войти по логину и паролю (если восстановление по сессии не удалось)
	if not new_client and username and password:
		cl = Client()
		cl.delay_range = [1, 6]
		proxy = get_proxy("instagram")
		if proxy: cl.set_proxy(proxy)
		try:
			cl.set_user_agent(IG_DEVICE_CONFIG["my_config"]["user_agent"])
			cl.set_device(IG_DEVICE_CONFIG["my_config"]["device"])
			cl.login(username, password)
			logging.info(f"✅ Успешный вход по логину/паролю для user {user_id}")
			new_client = cl
		except (ChallengeRequired, BadPassword) as e:
			logging.warning(f"❗ Ошибка входа для user {user_id}: {e}")
		except Exception as e:
			logging.error(f"❌ Неизвестная ошибка логина для user {user_id}: {e}")

	# Сохраняем новый успешный клиент в кэш
	if new_client:
		with INSTA_CLIENTS_LOCK:
			INSTA_CLIENTS_CACHE[user_id] = new_client

	return new_client

# Вспомогательная функция для получения информации о медиа
def get_media_info_private(client: Client, code: str) -> dict:
	"""
	Вспомогательная синхронная функция для выполнения запроса в потоке.
	Использует низкоуровневый private_request для получения сырого JSON,
	чтобы иметь доступ ко всем версиям видео ('video_versions').
	"""
	pk = client.media_pk_from_code(code)
	try:
		logging.info(f"Получаем информацию о медиа {code} через private_request...")
		# Используем v1, так как он более стабилен для получения базовой информации
		media_data = client.private_request(f'media/{pk}/info/')
		media = media_data.get('items', [{}])[0]

		result = {
			"video_url": None,
			"video_versions": [],
			"owner_username": media.get('user', {}).get('username', 'unknown_user'),
			"video_duration": media.get('video_duration', 0), # Извлекаем длительность
			"is_video": False,
			"is_carousel": False,
			"shortcode": code
		}

		versions_to_sort = []
		# Case 1: Single Video Post
		if media.get('media_type') == 2 and media.get('video_versions'):
			result["is_video"] = True
			versions_to_sort = media.get('video_versions')

		# Case 2: Carousel Post
		elif media.get('media_type') == 8 and media.get('carousel_media'):
			result["is_carousel"] = True
			for r in media['carousel_media']:
				# Find the first video in the carousel
				if r.get('media_type') == 2 and r.get('video_versions'):
					result["is_video"] = True
					versions_to_sort = r.get('video_versions')
					break # Found a video, stop searching
		
		if versions_to_sort:
			# 1. Удаляем дубликаты версий по URL, оставляя только уникальные.
			# API часто возвращает несколько записей для одного и того же файла с разными 'type'.
			unique_versions = []
			seen_urls = set()
			for v in versions_to_sort:
				url = v.get('url')
				if url and url not in seen_urls:
					unique_versions.append(v)
					seen_urls.add(url)
			
			# 2. Сортируем уникальные версии по битрейту (bandwidth) от худшего к лучшему.
			# Это более точный показатель качества, чем просто разрешение.
			unique_versions.sort(key=lambda v: v.get('bandwidth', 0))
			
			result["video_versions"] = unique_versions
			result["video_url"] = unique_versions[-1]['url'] if unique_versions else None # URL лучшего качества

		return result

	except Exception as e:
		logging.error(f"private_request для pk {pk} не удался: {e}")
		return {}

# --- Функции управления сессиями для instagrapi ---
async def load_session_from_redis(user_id):
	"""Загружает данные сессии из Redis."""
	session_data_str = await r.hget(f"{INSTA_REDIS_KEY}:user0", user_id)
	if session_data_str:
		try:
			return json.loads(session_data_str)
		except json.JSONDecodeError:
			logging.error(f"Не удалось декодировать сессию для пользователя {user_id}")
			return None
	return None

# Функция для сохранения сессии в Redis
async def save_session_to_redis(user_id, session_data_dict):
	"""Сериализует словарь сессии в JSON и сохраняет в Redis."""
	session_data_str = json.dumps(session_data_dict)
	await r.hset(f"{INSTA_REDIS_KEY}:user0", user_id, session_data_str)
	logging.info(f"Сессия для пользователя {user_id} сохранена в Redis")


# --- Командные обработчики ---
@dp.message(Command("igpass"))
async def cmd_igpass(message: Message):
	user_id = str(message.from_user.id)
	params = message.text.split()[1:]
	if len(params) != 2:
		await message.answer("❌ **Неверный формат.**\nИспользуйте: `/igpass <логин> <пароль>`", parse_mode=ParseMode.MARKDOWN)
		return

	username, password = params
	await message.answer("⏳ Авторизуюсь...")

	session_data = await load_session_from_redis(user_id)
	cl = await asyncio.to_thread(get_instagram_client, user_id, session_data, username, password)

	if cl:
		new_settings = cl.get_settings()
		await save_session_to_redis(user_id, new_settings)
		await message.answer("✅ Авторизация успешна!")
	else:
		await message.answer("❌ Не удалось авторизоваться. Проверь логин/пароль или пройди challenge через Instagram.")

# Команда для выхода из аккаунта Instagram
@dp.message(Command("iglogout"))
async def cmd_iglogout(message: Message):
	user_id = str(message.from_user.id)
	# Атомарно и потокобезопасно удаляем клиент из кэша в памяти, если он там есть.
	# .pop() является атомарной операцией в CPython, что защищает от гонки состояний.
	if INSTA_CLIENTS_CACHE.pop(user_id, None):
		logging.info(f"Клиент для user {user_id} удален из кэша памяти.")
	deleted_count = await r.hdel(f"{INSTA_REDIS_KEY}:user0", user_id)
	await message.reply("✅ Вы вышли из системы." if deleted_count > 0 else "🤔 Вы и так не были авторизованы.")

# --- ОБРАБОТЧИКИ --------------------------------------------------------------------------
		
# --- Обработчик Instagram-ссылок ---
async def handle_instagram_link(message: Message, content: dict): # url теперь передается из ai_router_handler
	p_msg = await message.reply("🔗 Обнаружена ссылка Instagram") # Изменено начальное сообщение

	shortcode = content.get('shortcode')
	url = message.text
	if not shortcode:
		# Теперь парсим shortcode внутри хендлера
		regexp_shortcode = re.search(r'(?:instagram\.com|instagr\.am)/(?:p|reel|tv)/([\w-]+)', url)
		shortcode = regexp_shortcode.group(1) if regexp_shortcode else None

		if not shortcode:
			await p_msg.edit_text("❌ **Неверная ссылка Instagram.**\nПожалуйста, отправьте ссылку на пост в формате: `https://www.instagram.com/p/shortcode/`")
			return
	
	user_id = str(message.from_user.id) 
	history_key = f"{INSTA_REDIS_KEY}:download_history"

	# Проверка истории загрузок в Redis (асинхронная)
	try:
		download_info_json = await r.hget(history_key, shortcode)
		if download_info_json:
			logging.info(f"Пост {shortcode} уже был обработан ранее. Используем данные из Redis.")
			try:
				download_info = json.loads(download_info_json)
				content_type = download_info.get('type')

				if content_type == 'video':
					file_id = download_info.get('file_id')
					cached_original_post_url = download_info.get('original_post_url')
					cached_owner_username = download_info.get('owner_username', 'Неизвестно')

					if file_id and cached_original_post_url: 
						try:
							await bot.send_chat_action(chat_id=message.chat.id, action=ChatAction.UPLOAD_VIDEO)
							
							# Восстанавливаем подпись из кэшированных данных
							caption = (
								f"📹 <a href='{cached_original_post_url}'>➡️💯🅶</a>\n"
								f"©: <code>{cached_owner_username}</code>"
							)
							
							if p_msg:
								await p_msg.edit_media(media=InputMediaVideo(media=file_id, caption=caption, parse_mode=ParseMode.HTML))
							else:
								await bot.send_video(message.chat.id, video=file_id, caption=caption, parse_mode=ParseMode.HTML)
							logging.info(f"Видео для {shortcode} успешно отправлено по file_id из Redis с подписью.")
							await message.delete()
							return
						except TelegramAPIError as e:
							logging.error(f"Ошибка Telegram API при отправке по file_id для {shortcode}: {e}")
						except Exception as e:
							logging.error(f"Неожиданная ошибка при отправке по file_id для {shortcode}: {e}")
				elif content_type == 'link':
					# Используем сохраненную оригинальную ссылку на пост для SaveFrom.net
					cached_original_post_url = download_info.get('original_post_url') 
					owner_username = download_info.get('owner_username', 'Неизвестно')
					if cached_original_post_url:
						savefrom_url = f"https://en.savefrom.net/#url={cached_original_post_url}"
						shortened_savefrom_url = shorten_url(savefrom_url)
						final_link = shortened_savefrom_url if shortened_savefrom_url else savefrom_url

						reason = download_info.get('reason')
						size_mb = download_info.get('size_mb')
						reason_text = ""
						if reason == 'too_large' and size_mb:
							reason_text = f"⚠️ Видео слишком большое ({size_mb} МБ), поэтому Telegram не может отправить его напрямую.\n\n"
						elif reason == 'carousel':
							reason_text = "ℹ️ Это видео из карусели. Для скачивания используйте ссылку ниже.\n\n"

						caption = (
							f"📹 <a href='{cached_original_post_url}'>➡️💯🅶</a>\n" # Используем оригинальную ссылку здесь
							f"©: <code>{owner_username}</code>\n\n"
							f"{reason_text}"
							f"Качай отсюда: {final_link}"
						)
						await p_msg.edit_text(caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
						logging.info(f"Ссылка на SaveFrom.net для {shortcode} отправлена из Redis (причина: {reason}).")
						await message.delete()
						return
			except json.JSONDecodeError:
				logging.error(f"Ошибка декодирования JSON истории загрузок для shortcode {shortcode}")
			except Exception as e:
				logging.error(f"Ошибка при обработке данных истории загрузок из Redis для shortcode {shortcode}: {e}")
	except Exception as e:
		logging.error(f"Ошибка при проверке истории загрузок в Redis для shortcode {shortcode}: {e}")

	# --- Новая логика авторизации и получения данных ---
	await p_msg.edit_text("🔑 Проверяю сессию Instagram...")

	# 1. Загружаем сессию из Redis
	session_data = await load_session_from_redis(user_id)
	if not session_data:
		logging.warning(f"Нет сессии Instagram для user {user_id}. Требуется авторизация.")
		await p_msg.edit_text("❌ **Требуется авторизация.**\nВойдите через `/igpass <логин> <пароль>`.")
		return

	# 2. Получаем и валидируем клиент instagrapi
	# Запускаем синхронный код в отдельном потоке, чтобы не блокировать asyncio
	cl = await asyncio.to_thread(get_instagram_client, user_id, session_data)

	if not cl:
		logging.warning(f"Сессия для user {user_id} истекла или недействительна.")
		await p_msg.edit_text("❌ **Сессия недействительна или истекла!**\nАвторизуйтесь заново через `/igpass`.")
		return

	# 3. Используем полученный клиент для запроса информации о медиа
	try:
		await p_msg.edit_text("ℹ️ Получаю информацию о посте...")
		
		# Имитация задержки пользователя перед действием (как будто он смотрит на пост)
		user_like_delay = random.uniform(1.5, 3.5)
		logging.info(f"Имитируем задержку пользователя: {user_like_delay:.2f} сек.")
		await asyncio.sleep(user_like_delay)

		logging.info(f"Доступ к {shortcode} для user {user_id} с активной сессией.")
		await bot.send_chat_action(chat_id=message.chat.id, action=ChatAction.TYPING)

		video_info = await asyncio.to_thread(get_media_info_private, cl, shortcode)

		if not video_info or not video_info.get("is_video"):
			await p_msg.edit_text("❌ В этом посте нет видео или не удалось получить информацию.")
			return

		video_url = video_info.get("video_url")
		if not video_url:
			await p_msg.edit_text("❌ Не удалось найти URL видео.")
			return

		if video_info.get("is_carousel"):
			savefrom_url = f"https://en.savefrom.net/#url={url}"
			shortened_savefrom_url = shorten_url(savefrom_url)
			final_link = shortened_savefrom_url if shortened_savefrom_url else savefrom_url
			caption = (
				f"📹 <a href='{url}'>➡️💯🅶</a>\n"
				f"©: <code>{video_info.get('owner_username')}</code>\n\n"
				f"Качай отсюда: {final_link}"
			)
			await p_msg.edit_text(caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
			logging.info(f"Ссылка на SaveFrom.net для карусели {shortcode} успешно отправлена.")
			await message.delete()
			try:
				download_info_to_save = json.dumps({
					'type': 'link', 'reason': 'carousel',
					'original_post_url': url,
					'owner_username': video_info.get('owner_username'),
					'timestamp': time.time()
				})
				await r.hset(history_key, shortcode, download_info_to_save)
				logging.info(f"Информация об обработке поста {shortcode} (карусель) сохранена в Redis.")
			except Exception as e:
				logging.error(f"Ошибка при сохранении истории обработки в Redis для shortcode {shortcode}: {e}")
		else:
			try:
				await p_msg.edit_text("📥 Скачиваю и отправляю видео...")
				download_delay = random.uniform(2.0, 4.0)
				logging.info(f"Имитируем задержку перед скачиванием: {download_delay:.2f} сек.")
				await asyncio.sleep(download_delay)

				# --- Логика выбора качества видео на основе расчета по битрейту ---
				video_versions = video_info.get("video_versions", [])
				duration = video_info.get("video_duration", 0)

				if not video_versions:
					await p_msg.edit_text("❌ Не удалось найти версии видео в информации о посте.")
					return
				if duration <= 0:
					await p_msg.edit_text("❌ Не удалось определить длительность видео для расчета размера.")
					logging.warning(f"Длительность видео для {shortcode} равна нулю, расчет невозможен.")
					return

				url_to_send = None
				caption_note = ""
				best_vsize = 0
				
				# Перебираем все версии от лучшей к худшей
				await p_msg.edit_text(f"ℹ️ Найдено {len(video_versions)} версий видео. Подбираю подходящую по размеру...")
				
				for version in reversed(video_versions):
					if not version.get('url'):
						continue # Пропускаем, если нет URL
					
					# Расчитываем размер
					vsize = (version.get('bandwidth', 0) * duration) / 8
					if best_vsize == 0: # Сохраняем размер самой лучшей версии для сообщения об ошибке
						best_vsize = vsize
					
					logging.info(f"Проверяю версию ({version.get('width')}x{version.get('height')}), расчетный размер: {vsize:.0f} байт")
					
					if 0 < vsize <= MAX_VIDEO_SIZE_BYTES:
						logging.info(f"Найдена подходящая версия! ({version.get('width')}x{version.get('height')}).")
						url_to_send = version.get('url')
						if version != video_versions[-1]: # Если это не самая лучшая версия
							caption_note = f" (версия с разрешением {version.get('width')}x{version.get('height')})"
						break # Нашли подходящую, выходим из цикла

				if url_to_send:
					await p_msg.edit_text(f"✅ Начинаю загрузку...")
					await bot.send_chat_action(chat_id=message.chat.id, action=ChatAction.UPLOAD_VIDEO)
					video = URLInputFile(str(url_to_send), filename=f"{shortcode}.mp4")
					caption = f"📹 <a href='{url}'>➡️💯🅶</a>{caption_note}\n©: <code>{video_info.get('owner_username')}</code>"
					upd_mes = await p_msg.edit_media(media=InputMediaVideo(media=video, caption=caption, parse_mode=ParseMode.HTML))
					file_id = upd_mes.video.file_id
					logging.info(f"Видео для {shortcode} успешно загружено в Telegram с file_id: {file_id}")
					if file_id:
						try:
							download_info_to_save = json.dumps({
								'type': 'video', 'file_id': file_id, 'msg_id': message.message_id,
								'chat_id': message.chat.id, 'original_post_url': url,
								'owner_username': video_info.get('owner_username'), 'timestamp': time.time()
							})
							await r.hset(history_key, shortcode, download_info_to_save)
							logging.info(f"Информация о загрузке поста {shortcode} сохранена в Redis.")
							await message.delete()
						except Exception as e:
							logging.error(f"Ошибка при сохранении истории загрузок в Redis для {shortcode}: {e}")
				else: # Если подходящей версии не найдено
					logging.info(f"Подходящих версий видео для {shortcode} не найдено. Отправляю ссылку.")
					savefrom_url = f"https://en.savefrom.net/#url={url}"
					shortened_savefrom_url = shorten_url(savefrom_url)
					final_link = shortened_savefrom_url if shortened_savefrom_url else savefrom_url
					vsize_mb_str = f"{best_vsize / (1024*1024):.2f}"
					caption = (
						f"⚠️ Видео слишком большое (примерно {vsize_mb_str} МБ), а версии поменьше не найдены. "
						f"Telegram не может отправить его напрямую.\n\n"
						f"Качай отсюда: {final_link}"
					)
					await p_msg.edit_text(caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
					try:
						download_info_to_save = json.dumps({
							'type': 'link', 'reason': 'too_large', 'size_mb': vsize_mb_str,
							'original_post_url': url, 'owner_username': video_info.get('owner_username'),
							'timestamp': time.time()
						})
						await r.hset(history_key, shortcode, download_info_to_save)
						logging.info(f"Информация об обработке большого видео {shortcode} сохранена в Redis.")
					except Exception as e:
						logging.error(f"Ошибка при сохранении истории обработки в Redis для {shortcode}: {e}")
					await message.delete()
					return
			except Exception as e:
				logging.error(f"Ошибка при скачивании или отправке видео {shortcode}: {e}")
				shortened_video_url = shorten_url(video_url)
				if shortened_video_url:
					await p_msg.edit_text(f"Скачать видео {shortcode} не получилось напрямую. Вот ссылка, попробуйте скачать сами:\n{shortened_video_url}", parse_mode=ParseMode.HTML)
				else:
					await p_msg.edit_text(f"Скачать видео {shortcode} не получилось напрямую. Вот ссылка, попробуйте скачать сами:\n{video_url}", parse_mode=ParseMode.HTML)
	except (BadCredentials, LoginRequired, ChallengeRequired):
		logging.warning(f"Сессия для user {user_id} истекла или недействительна.")
		await p_msg.edit_text("❌ **Сессия недействительна или истекла!**\nАвторизуйтесь заново через `/igpass`.")
	except ValidationError as e:
		logging.error(f"Ошибка валидации данных от Instagram (instagrapi): {e}")
		await p_msg.edit_text("❌ **Ошибка обработки ответа от Instagram.**\n"
							  "Похоже, структура данных поста изменилась. "
							  "Попробуйте позже или используйте другой пост.")
	except ClientError as e:
		error_message = str(e)
		if "checkpoint_required" in error_message:
			error_message = "Требуется подтверждение аккаунта (чекпойнт). Попробуйте войти через официальное приложение Instagram."
		elif "challenge_required" in error_message:
			error_message = "Требуется пройти проверку безопасности (challenge). Попробуйте войти через официальное приложение Instagram."
		elif "proxy" in error_message.lower():
			error_message = "Проблема с прокси или IP-адресом. Попробуйте другой VPN/IP."
		else:
			error_message = f"Общая ошибка Instagram API: `{error_message}`"
		logging.error(f"Ошибка instagrapi при получении информации о посте для user {user_id}: {e}")
		await p_msg.edit_text(f"❌ <b>Ошибка от Instagram API!</b>\n{error_message}", parse_mode=ParseMode.HTML)
	except PrivateError:
		await p_msg.edit_text("❌ **Приватный профиль!**\nВаш аккаунт не подписан на пользователя, или профиль приватный.")
	except Exception as e:
		logging.error(f"Неизвестная ошибка скачивания: {e}")
		await p_msg.edit_text(f"❌ **Произошла неизвестная ошибка:**\n`{e}`")

# Муз сервисы
async def handle_yandex_music(message: Message, content: dict):
	p_msg = await message.reply("🎶 Ищем трек на Яндекс.Музыке...")

	track_id = content.get('track_id')
	if not track_id:
		# Fallback regex if AI fails to extract ID
		track_pattern = re.compile(r'music\.yandex\.ru/album/\d+/track/(\d+)')
		match = track_pattern.search(message.text)
		if not match:
			await p_msg.edit_text("❌ Не удалось извлечь ID трека из ссылки.")
			return
		track_id = match.group(1)

	# 1. Get Tor proxy
	proxy_url = get_proxy('tor')
	if not proxy_url:
		await p_msg.edit_text("⚠️ Tor-прокси не настроен или недоступен.")
		return

	# 2. Setup aiohttp session with proxy
	connector = ProxyConnector.from_url(proxy_url)
	headers = {
		'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
		'Accept': 'application/json',
		'Accept-Language': 'en-US,en;q=0.5',
	}
	
	api_url = f'https://api.music.yandex.net/tracks/{track_id}'
	music_info = None
	MAX_ATTEMPTS = 3

	async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
		for attempt in range(1, MAX_ATTEMPTS + 1):
			await p_msg.edit_text(f"🎶 Ищем трек... (попытка {attempt}/{MAX_ATTEMPTS})")
			try:
				logging.info(f"Запрос к {api_url} через Tor...")
				async with session.get(api_url, timeout=15) as response:
					if response.status == 200:
						data = await response.json()
						track_data = data.get('result', [])[0] if data.get('result') else None
						if track_data:
							title = track_data.get('title')
							artists = ', '.join([a.get('name') for a in track_data.get('artists', [])])
							duration_ms = track_data.get('durationMs', 0)
							
							# --- Извлекаем информацию об альбоме ---
							cover_uri = None
							album_title = "Неизвестен"
							album_year = ""
							if track_data.get('albums'):
								album_data = track_data['albums'][0]
								cover_uri = album_data.get('coverUri')
								album_title = album_data.get('title', "Неизвестен")
								album_year = f"({album_data.get('year')})" if album_data.get('year') else ""
							
							cover_url = f"https://{cover_uri.replace('%%', '400x400')}" if cover_uri else None
							
							music_info = {
								'artist': artists,
								'title': title,
								'duration_sec': duration_ms // 1000,
								'cover_url': cover_url,
								'album_title': album_title,
								'album_year': album_year,
								'source_url': message.text # Сохраняем оригинальную ссылку
							}
							logging.info(f"Найден трек: {artists} - {title}")
							break # Success, exit loop
					else:
						logging.warning(f"Попытка {attempt}: Яндекс.Музыка вернула статус {response.status}. Текст: {await response.text(encoding='utf-8', errors='ignore')}")
						if attempt < MAX_ATTEMPTS:
							await p_msg.edit_text(f"⚠️ Ответ {response.status}. Меняю IP Tor...")
							check_tor_connection(renew=True)
							await asyncio.sleep(3)
						
			except Exception as e:
				logging.error(f"Попытка {attempt}: Ошибка при запросе к Яндекс.Музыке: {e}")
				if attempt < MAX_ATTEMPTS:
					await p_msg.edit_text(f"⚠️ Ошибка соединения. Меняю IP Tor...")
					check_tor_connection(renew=True)
					await asyncio.sleep(3)

	if music_info:
		# --- Сначала выводим информацию о треке ---
		duration_sec = music_info.get('duration_sec', 0)
		minutes, seconds = divmod(duration_sec, 60)
		info_caption = (
			f"<b>Исполнитель:</b> {music_info['artist']}\n"
			f"<b>Трек:</b> {music_info['title']}\n"
			f"<b>Альбом:</b> {music_info['album_title']} {music_info['album_year']}\n"
			f"<b>Длительность:</b> {minutes}:{seconds:02d}\n"
			f"<b>Источник:</b> <a href='{music_info['source_url']}'>Яндекс.Музыка</a>"
		)

		# Удаляем сообщение "Ищем..." и отправляем новое с картинкой (если есть)
		await p_msg.delete()
		if music_info['cover_url']:
			p_msg = await message.answer_photo(
				photo=music_info['cover_url'],
				caption=info_caption,
				parse_mode=ParseMode.HTML
			)
		else:
			p_msg = await message.answer(info_caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
		await message.delete() # Удаляем сообщение пользователя

		# --- Теперь ищем трек на сторонних сайтах ---
		song_obj = {
			'song': f"{music_info['artist']} - {music_info['title']}",
			'duration': music_info['duration_sec']
		}
		# Вызываем поиск без p_msg, чтобы он создал новое сообщение и не трогал это
		await handle_song_search(message, song_obj)
	else:
		await p_msg.edit_text("❌ Не удалось получить информацию о треке после нескольких попыток. Сервис может быть недоступен через Tor.")

async def handle_sberzvuk_music(message: Message, content: dict):
	"""Обрабатывает ссылки на треки из Звук (zvuk.com), включая короткие share.zvuk.com."""
	p_msg = await message.reply("🎶 Ищем трек в Звук...")

	async def resolve_zvuk_url(url):
		"""Если ссылка короткая, раскрывает её, иначе возвращает как есть."""
		if "share.zvuk.com" in url:
			headers = {
				'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
				'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
				'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
			}
			async with aiohttp.ClientSession(headers=headers) as session:
				async with session.get(url, allow_redirects=True, timeout=10) as response:
					if response.status == 200:
						return str(response.url)
					else:
						return None
		return url

	# 1. Получаем финальную ссылку (раскрываем короткую при необходимости)
	original_url = message.text
	final_url = await resolve_zvuk_url(original_url)
	if not final_url:
		await p_msg.edit_text("❌ Не удалось раскрыть короткую ссылку Звук.")
		return

	# 2. Извлекаем track_id из финальной ссылки
	track_id = None
	match = re.search(r'zvuk\.com/track/(\d+)', final_url)
	if match:
		track_id = match.group(1)

	if not track_id:
		# Если не удалось распарсить, пробуем взять ID от AI как запасной вариант
		track_id = content.get('track_id')
		if not track_id:
			await p_msg.edit_text("❌ Не удалось извлечь ID трека из ссылки Звук.")
			return

	# 3. Получаем временный токен и инфу о треке
	headers = {
		'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
		'Accept': 'application/json, text/plain, */*',
		'Origin': 'https://zvuk.com',
	}
	music_info = None
	try:
		async with aiohttp.ClientSession() as session:
			async with session.get("https://zvuk.com/api/tiny/profile", headers=headers, timeout=10) as resp:
				if resp.status != 200:
					await p_msg.edit_text("❌ Не удалось получить временный токен от Звук.")
					return
				data = await resp.json(content_type=None)
				token = data.get("result", {}).get("token")
			if not token:
				await p_msg.edit_text("❌ Временный токен от Звук пуст.")
				return

			payload = {
				"operationName": "getFullTrack",
				"variables": {"id": track_id},
				"query": """
					query getFullTrack($id: ID!) {
					  getTracks(ids: [$id]) {
						title
						duration
						artists { title }
						release {
						  title
						  date
						  image { src }
						}
					  }
					}
				"""
			}
			graphql_headers = {
				"User-Agent": headers["User-Agent"],
				"Accept": headers["Accept"],
				"Origin": headers["Origin"],
				"x-auth-token": token
			}
			async with session.post("https://zvuk.com/api/v1/graphql", json=payload, headers=graphql_headers, timeout=10) as resp:
				if resp.status == 200:
					data = await resp.json(content_type=None)
					tracks_list = data.get("data", {}).get("getTracks", [])
					if tracks_list:
						track_data = tracks_list[0]
						if track_data: # Проверяем, что трек действительно найден, а не null
							title = track_data.get('title')
							artists = ', '.join([a.get('title') for a in track_data.get('artists', [])])
							duration_sec = track_data.get('duration', 0)
							release_info = track_data.get('release', {})
							album_title = release_info.get('title', "Неизвестен")
							album_date = release_info.get('date')
							album_year_val = album_date.split('-')[0] if album_date else None
							album_year = f"({album_year_val})" if album_year_val else ""
							cover_url = release_info.get("image", {}).get("src")
							music_info = {
								'artist': artists, 'title': title, 'duration_sec': duration_sec,
								'cover_url': cover_url, 'album_title': album_title, 'album_year': album_year,
								'source_url': message.text
							}
							logging.info(f"Найден трек в Звук: {artists} - {title}")
				else:
					logging.warning(f"Zvuk (graphql) вернул статус {resp.status}. Ответ: {await resp.text()}")
	except Exception as e:
		logging.error(f"Ошибка при обработке ссылки Звук: {e}", exc_info=True)
		await p_msg.edit_text(f"❌ Произошла ошибка при запросе к Звук: `{e}`")
		return

	if music_info:
		duration_sec = music_info.get('duration_sec', 0)
		minutes, seconds = divmod(duration_sec, 60)
		info_caption = (
			f"<b>Исполнитель:</b> {music_info.get('artist')}\n"
			f"<b>Трек:</b> {music_info.get('title')}\n"
			f"<b>Альбом:</b> {music_info.get('album_title')} {music_info.get('album_year')}\n"
			f"<b>Длительность:</b> {minutes}:{seconds:02d}\n"
			f"<b>Источник:</b> <a href='{music_info.get('source_url')}'>Звук</a>"
		)
		await p_msg.delete()
		if music_info.get('cover_url'):
			await message.answer_photo(photo=music_info['cover_url'], caption=info_caption, parse_mode=ParseMode.HTML)
		else:
			await message.answer(info_caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
		await message.delete()
		song_obj = {'song': f"{music_info['artist']} - {music_info['title']}", 'duration': music_info['duration_sec']}
		await handle_song_search(message, song_obj)
	else:
		await p_msg.edit_text("❌ Не удалось получить информацию о треке из Звук.")
		
async def handle_mts_music(message: Message, content: dict):
	"""Обрабатывает ссылки на треки из МТС Музыка (music.mts.ru)."""
	p_msg = await message.reply("🎶 Ищем трек в МТС Музыка...")

	track_id = content.get('track_id')
	if not track_id:
		track_pattern = re.compile(r'music\.mts\.ru/track/(\d+)')
		match = track_pattern.search(message.text)
		if not match:
			await p_msg.edit_text("❌ Не удалось извлечь ID трека из ссылки МТС Музыка.")
			return
		track_id = match.group(1)

	page_url = f"https://music.mts.ru/track/{track_id}"
	music_info = None

	try:
		async with aiohttp.ClientSession() as session:
			async with session.get(page_url, timeout=10) as response:
				if response.status == 200:
					soup = BeautifulSoup(await response.text(), 'html.parser')
					ld_json_script = soup.find('script', type='application/ld+json')
					if ld_json_script:
						data = json.loads(ld_json_script.string)
						title = data.get('name')
						artists = ', '.join([a.get('name') for a in data.get('byArtist', [])])
						album_title = data.get('inAlbum', {}).get('name', "Неизвестен")
						album_year = f"({data.get('inAlbum', {}).get('datePublished')})" if data.get('inAlbum', {}).get('datePublished') else ""
						cover_url = data.get('image')
						
						duration_sec = 0
						duration_iso = data.get('duration') # PT3M25S
						if duration_iso:
							match = re.search(r'PT(?:(\d+)M)?(?:(\d+)S)?', duration_iso)
							if match:
								minutes = int(match.group(1) or 0)
								seconds = int(match.group(2) or 0)
								duration_sec = minutes * 60 + seconds

						music_info = {
							'artist': artists, 'title': title, 'duration_sec': duration_sec,
							'cover_url': cover_url, 'album_title': album_title, 'album_year': album_year,
							'source_url': message.text
						}
						logging.info(f"Найден трек в МТС Музыка: {artists} - {title}")
				else:
					logging.warning(f"МТС Музыка вернула статус {response.status}")
	except Exception as e:
		logging.error(f"Ошибка при парсинге МТС Музыка: {e}")

	if music_info:
		duration_sec = music_info.get('duration_sec', 0)
		minutes, seconds = divmod(duration_sec, 60)
		info_caption = (
			f"<b>Исполнитель:</b> {music_info['artist']}\n"
			f"<b>Трек:</b> {music_info['title']}\n"
			f"<b>Альбом:</b> {music_info['album_title']} {music_info['album_year']}\n"
			f"<b>Длительность:</b> {minutes}:{seconds:02d}\n"
			f"<b>Источник:</b> <a href='{music_info['source_url']}'>МТС Музыка</a>"
		)
		await p_msg.delete()
		if music_info['cover_url']:
			await message.answer_photo(photo=music_info['cover_url'], caption=info_caption, parse_mode=ParseMode.HTML)
		else:
			await message.answer(info_caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
		await message.delete()

		song_obj = {'song': f"{music_info['artist']} - {music_info['title']}", 'duration': music_info['duration_sec']}
		await handle_song_search(message, song_obj)
	else:
		await p_msg.edit_text("❌ Не удалось получить информацию о треке из МТС Музыка.")

# --- ЕДИНЫЙ ПАРСЕР МУЗЫКАЛЬНЫХ САЙТОВ ---

def _parse_duration_mm_ss(duration_str: str) -> int:
	"""Вспомогательная функция для парсинга длительности из формата 'MM:SS'."""
	if not isinstance(duration_str, str): return 0
	try:
		time_parts = duration_str.strip().split(':')
		return int(time_parts[0]) * 60 + int(time_parts[1])
	except (ValueError, IndexError):
		return 0

def _extractor_muzika_fun(item: BeautifulSoup, base_url: str) -> Optional[dict]:
	"""Извлекает данные для сайта muzika.fun."""
	link_element = item.select_one('[data-url]')
	if not link_element: return None
	return {
		"link": link_element.get('data-url'),
		"artist": item.get('data-artist'),
		"title": item.get('data-title'),
		"duration": int(item.get('data-duration', 0))
	}

def _extractor_mp3iq(item: BeautifulSoup, base_url: str) -> Optional[dict]:
	"""Извлекает данные для сайта mp3iq.net (новая структура)."""
	# Извлекаем данные из атрибутов тега <li>
	link = item.get('data-mp3')
	duration_ms = item.get('data-duration')
	# Извлекаем исполнителя и название из дочерних элементов
	artist_a = item.select_one('h2.playlist-name b a')
	title_a = item.select_one('h2.playlist-name em a')
	
	if not all([link, duration_ms, artist_a, title_a]): return None
	
	try:
		duration_sec = int(duration_ms) // 1000
	except (ValueError, TypeError):
		duration_sec = 0

	return {
		"link": link,
		"artist": artist_a.text.strip(),
		"title": title_a.text.strip(),
		"duration": duration_sec
	}

def _extractor_mp3party(item: BeautifulSoup, base_url: str) -> Optional[dict]:
	"""Извлекает данные для сайта mp3party.net."""
	user_panel = item.find('div', class_='track__user-panel')
	duration_div = item.find('div', class_='track__info-item')
	link_btn = item.find('div', class_='play-btn')

	if not all([user_panel, duration_div, link_btn]): return None
	
	return {
		"link": link_btn.get('href'),
		"artist": user_panel.get('data-js-artist-name'),
		"title": user_panel.get('data-js-song-title'),
		"duration": _parse_duration_mm_ss(duration_div.text)
	}

async def _parse_music_site(config: dict, song_name: str) -> Optional[list]:
	"""Универсальный парсер музыкальных сайтов, управляемый конфигурацией."""
	search_url = config["base_url"] + config["search_path"].format(query=quote(song_name))
	
	session_args = {"headers": config.get("headers", {})}
	
	# Проверяем, нужен ли прокси для этого сайта
	proxy_type = config.get("proxy")
	if proxy_type:
		proxy_url = get_proxy(proxy_type)
		if proxy_url:
			connector = ProxyConnector.from_url(proxy_url)
			session_args["connector"] = connector
		else:
			logging.warning(f"Прокси '{proxy_type}' для сайта {config['name']} недоступен. Запрос будет выполнен без прокси.")

	try:
		async with aiohttp.ClientSession(**session_args) as session:
			async with session.get(search_url, timeout=15) as response:
				if response.status != 200:
					logging.error(f"Ошибка HTTP {response.status} при запросе {search_url}")
					return None
				soup = BeautifulSoup(await response.text(), 'html.parser')
	except Exception as e:
		logging.error(f"Ошибка при запросе {search_url}: {e}", exc_info=True)
		return None

	parsed_songs = []
	song_list = soup.select(config["item_selector"])
	if not song_list:
		logging.warning(f"Не найдены элементы песен на {search_url} по селектору '{config['item_selector']}'")
		return None

	for item in song_list:
		try:
			song_data = config["extractor_func"](item, config["base_url"])
			if song_data and all(song_data.values()):
				parsed_songs.append(song_data)
		except Exception as e:
			logging.warning(f"Не удалось распарсить элемент на {config['name']}: {e}")
			continue
	
	return parsed_songs if parsed_songs else None

BASE_HEADERS = {
	"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
	"Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7"
}

SEARCH_PROVIDER_CONFIGS = [
	{
		"name": "muzika.fun",
		"base_url": "https://w1.muzika.fun",
		"search_path": "/search/{query}",
		"item_selector": "ul.mainSongs li",
		"extractor_func": _extractor_muzika_fun,
		"headers": BASE_HEADERS,
	},
	{
		"name": "mp3iq.net",
		"base_url": "https://mp3iq.net",
		"search_path": "/search/f/{query}/",
		"item_selector": "li.track",
		"extractor_func": _extractor_mp3iq,
		"headers": {**BASE_HEADERS, "Referer": "https://mp3iq.net/"},
		"proxy": "tor", # Для этого сайта требуется Tor
	},
	{
		"name": "mp3party.net",
		"base_url": "https://mp3party.net",
		"search_path": "/search?q={query}",
		"item_selector": "div.track.song-item",
		"extractor_func": _extractor_mp3party,
		"headers": {**BASE_HEADERS, "Referer": "https://mp3party.net/"},
	},
]

def normalize_for_match(s: str) -> str:
	"""Удаляет все, кроме букв и цифр, и приводит к нижнему регистру для сравнения."""
	if not s: return ""
	return re.sub(r'[^a-zа-я0-9]', '', s.lower())

async def handle_song_search(message: Message, song_obj: dict):
	"""
	Обрабатывает запрос на поиск песни, используя несколько источников параллельно.
	Фильтрует и сортирует результаты для максимальной релевантности.
	"""
	song_name = song_obj.get('song')
	duration = song_obj.get('duration') or 0

	status_msg = await message.answer(f"🎤 Ищу «{song_name}» на всех доступных источниках...")
	
	# --- 1. Параллельный поиск ---
	tasks = [_parse_music_site(provider, song_name) for provider in SEARCH_PROVIDER_CONFIGS]
	results = await asyncio.gather(*tasks, return_exceptions=True)

	all_songs = []
	for i, result in enumerate(results):
		if isinstance(result, list):
			all_songs.extend(result)

	if not all_songs:
		await status_msg.edit_text("❌ Ничего не найдено по вашему запросу.")
		return

	# --- 2. Фильтрация и сортировка ---
	normalized_query = normalize_for_match(song_name)
	
	# Ищем совпадения, сначала точные, потом частичные
	filtered_songs = [s for s in all_songs if normalize_for_match(f"{s.get('artist')} {s.get('title')}") == normalized_query]
	if not filtered_songs:
		filtered_songs = [s for s in all_songs if normalized_query in normalize_for_match(f"{s.get('artist')} {s.get('title')}")]

	songs_to_process = filtered_songs if filtered_songs else all_songs
	
	if duration > 0:
		songs_to_process.sort(key=lambda s: abs(s.get('duration', 0) - duration))

	# Удаляем дубликаты
	unique_songs = []
	seen = set()
	for song in songs_to_process:
		identifier = (normalize_for_match(song.get('artist')), normalize_for_match(song.get('title')))
		if identifier not in seen:
			unique_songs.append(song)
			seen.add(identifier)
	
	logging.info(f"После фильтрации и дедупликации осталось {len(unique_songs)} треков.")

	if unique_songs:
		# --- Если найден всего 1 трек, сразу его загружаем ---
		if len(unique_songs) == 1:
			song = unique_songs[0]
			await status_msg.edit_text(f"✅ Найден один подходящий трек, скачиваю...")
			audio_data = await download_audio(song.get('link'))
			if audio_data:
				await message.answer_audio(audio=BufferedInputFile(audio_data, filename=f"{song.get('artist')}-{song.get('title')}.mp3"), performer=song.get('artist'), title=song.get('title'), duration=song.get('duration'))
				await status_msg.delete()
			else:
				await status_msg.edit_text("❌ Ошибка скачивания трека.")
			return

		await status_msg.delete() # Удаляем сообщение "Ищу..."
		await display_music_list(message, unique_songs)
	else:
		await status_msg.edit_text("❌ Подходящих треков не найдено после фильтрации.")

async def download_audio(url):
	try:
		async with aiohttp.ClientSession() as session:
			async with session.get(url, timeout=10) as response:
				if response.status != 200:
					logging.error(f"Ошибка HTTP {response.status} при скачивании {url}")
					return None
				data = await response.read()
				logging.info(f"Успешно скачан аудиофайл с {url}")
				return data
	except Exception as e:
		logging.error(f"Ошибка скачивания аудио с {url}: {e}")
		return None

async def display_music_list(message: Message, list_music: list, items_per_page: int = 5):
	user_id = str(message.from_user.id)
	uid = uuid.uuid4().hex
	state = {"list": list_music, "current_page": 0, "items_per_page": items_per_page}
	await r.set(f"music_session:{user_id}:{uid}", json.dumps(state), ex=600)
	num_pages = -(-len(list_music) // items_per_page)
	keyboard = create_keyboard(list_music, 0, items_per_page, uid, num_pages)
	text = f"Результаты поиска ({len(list_music)}). Выберите подходящий вариант:"
	await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

def create_keyboard(list_music: list, page: int, items_per_page: int, uid: str, num_pages: int):
	builder = InlineKeyboardBuilder()
	start_idx, end_idx = page * items_per_page, min((page + 1) * items_per_page, len(list_music))
	for idx in range(start_idx, end_idx):
		song = list_music[idx]
		minutes, seconds = divmod(song.get("duration", 0), 60)
		builder.button(
			text=f"🎧 {song.get('artist')} - {song.get('title')} ({minutes}:{seconds:02d})",
			callback_data=f"select_song:{idx}:{uid}"
		)
	builder.adjust(1)
	nav_buttons = []
	if page > 0:
		nav_buttons.append(types.InlineKeyboardButton(text="◀️ Назад", callback_data=f"prev_page:{uid}"))
	nav_buttons.append(types.InlineKeyboardButton(text="❌ Отмена", callback_data=f"cancel:{uid}"))
	if page < num_pages - 1:
		nav_buttons.append(types.InlineKeyboardButton(text="Вперед ▶️", callback_data=f"next_page:{uid}"))
	if nav_buttons:
		builder.row(*nav_buttons)
	return builder.as_markup()

@dp.callback_query(F.data.startswith(("select_song", "prev_page", "next_page", "cancel")))
async def handle_callback_query(callback: types.CallbackQuery):
	action, *params = callback.data.split(":")
	uid = params[-1]
	user_id = str(callback.from_user.id)
	state_data = await r.get(f"music_session:{user_id}:{uid}")
	if not state_data:
		await callback.answer("Сессия истекла.", show_alert=True)
		await callback.message.delete()
		return
	state = json.loads(state_data)
	
	if action == "select_song":
		idx = int(params[0])
		song = state["list"][idx]
		await callback.answer(f"Загружаю: {song.get('artist')}...")
		audio_data = await download_audio(song.get('link'))
		if audio_data:
			await callback.message.answer_audio(
				audio=BufferedInputFile(audio_data, filename=f"{song.get('artist')}-{song.get('title')}.mp3"),
				performer=song.get('artist'),
				title=song.get('title'),
				duration=song.get('duration')
			)
			# await callback.message.delete()
		else:
			await callback.answer("❌ Ошибка скачивания.", show_alert=True)
	elif action in ["prev_page", "next_page"]:
		state["current_page"] += 1 if action == "next_page" else -1
		await r.set(f"music_session:{user_id}:{uid}", json.dumps(state), ex=600)
		num_pages = -(-len(state["list"]) // state["items_per_page"])
		keyboard = create_keyboard(state["list"], state["current_page"], state["items_per_page"], uid, num_pages)
		await callback.message.edit_reply_markup(reply_markup=keyboard)
	elif action == "cancel":
		await callback.message.delete()
		await r.delete(f"music_session:{user_id}:{uid}")
		await callback.answer("Поиск отменён.")
	await callback.answer()


async def handle_chat_request(message: Message, text: str):
	p_msg = await message.reply("🤖...")
	# Проверяем, что текст не пустой
	try:
		response = await client.aio.models.generate_content(
			model=MODEL_25,
			contents=text,
			config=genai.types.GenerateContentConfig(
				tools=[{"google_search": {}}],
				system_instruction="You are a helpful assistant with access to real-time Google Search. Use search when needed to answer accurately. Answer in a user question language"
			)
		)
		await p_msg.edit_text(response.text)
	except Exception as e:
		logging.error(f"Ошибка чата Gemini: {e}")
		await p_msg.edit_text("😕 Мой AI-мозг временно перегружен.")


async def on_startup(bot: Bot) -> None:
	"""Действия при запуске бота: проверка и установка вебхука."""
	# Проверяем, что все необходимые переменные для вебхука установлены
	if not all([WEBHOOK_HOST, WEBHOOK_SECRET]):
		logging.critical("WEBHOOK_HOST или WEBHOOK_SECRET не установлены! Бот не может запуститься в режиме вебхука.")
		sys.exit(1)

	try:
		# Получаем текущую информацию о вебхуке
		current_webhook = await bot.get_webhook_info()

		# Если URL не совпадает с целевым, выполняем полную и чистую переустановку.
		# Это решает проблему, когда вебхук был установлен некорректно (например, с пустым URL).
		if current_webhook.url != BASE_WEBHOOK_URL:
			logging.info(f"Текущий URL вебхука ('{current_webhook.url or 'не установлен'}') отличается от целевого. Выполняем обновление...")
			
			# Сначала удаляем старый вебхук, чтобы обеспечить чистое состояние.
			await bot.delete_webhook(drop_pending_updates=True)
			logging.info("Старый вебхук удален (или не был установлен).")
			
			# Затем устанавливаем новый.
			await bot.set_webhook(url=BASE_WEBHOOK_URL, secret_token=WEBHOOK_SECRET)
			logging.info(f"Вебхук успешно установлен на {BASE_WEBHOOK_URL}")
		else:
			logging.info(f"Вебхук уже установлен на {BASE_WEBHOOK_URL}. Пропускаем установку.")

	except TelegramBadRequest as e:
		# Обрабатываем конкретные ошибки, которые могут возникнуть при установке вебхука
		if "Failed to resolve host" in e.message:
			logging.critical(
				f"Критическая ошибка: Telegram не может разрешить хост '{WEBHOOK_HOST}'. "
				"Возможные причины:\n"
				"1. Ошибка в доменном имени в переменной WEBHOOK_HOST.\n"
				"2. DNS-запись еще не обновилась (требуется время на распространение).\n"
				"3. Проблемы с DNS-провайдером или сетевые ограничения.\n"
			)
		else:
			logging.critical(f"Ошибка при установке вебхука: {e}")
		sys.exit(1)
	except Exception as e:
		logging.critical(f"Непредвиденная ошибка при установке вебхука: {e}")
		sys.exit(1)

async def on_shutdown(bot: Bot) -> None:
	"""Действия при остановке бота: удаление вебхука и закрытие соединений."""
	logging.info("Остановка бота...")
	await bot.delete_webhook()
	logging.info("Вебхук удален.")
	await r.close()
	logging.info("Соединение с Redis закрыто.")

async def main():
	# Регистрируем обработчики жизненного цикла
	dp.startup.register(on_startup)
	dp.shutdown.register(on_shutdown)

	# Создаем приложение aiohttp
	app = web.Application()

	# Создаем эндпоинт для healthcheck, который требует docker-compose.yml
	async def health_check(request: web.Request) -> web.Response:
		return web.Response(text="OK")
	app.router.add_get("/health", health_check)

	# Создаем обработчик вебхуков
	webhook_requests_handler = SimpleRequestHandler(
		dispatcher=dp, bot=bot, secret_token=WEBHOOK_SECRET,
	)
	# Регистрируем его в приложении
	webhook_requests_handler.register(app, path=WEBHOOK_PATH)

	# "Монтируем" диспетчер и бота в приложение aiohttp
	setup_application(app, dp, bot=bot)

	# Запускаем веб-сервер
	runner = web.AppRunner(app)
	await runner.setup()
	site = web.TCPSite(runner, WEB_SERVER_HOST, WEB_SERVER_PORT)
	logging.info(f"✅ Бот запущен в режиме webhook на http://{WEB_SERVER_HOST}:{WEB_SERVER_PORT}")
	await site.start()

	# Бесконечно ждем, пока приложение не будет остановлено
	await asyncio.Event().wait()

if __name__ == "__main__":
	logging.info("Запуск бота...")
	try:
		asyncio.run(main())
	except KeyboardInterrupt:
		# Обработка Ctrl+C
		logging.info("Бот остановлен вручную (KeyboardInterrupt).")
	except SystemExit as e:
		# Обработка вызовов sys.exit()
		if e.code == 0 or e.code is None:
			logging.info("Бот штатно завершил работу.")
		else:
			logging.critical(f"Бот остановлен из-за критической ошибки (exit code: {e.code}).")
