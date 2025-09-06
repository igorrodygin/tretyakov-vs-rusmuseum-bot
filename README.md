# ТГ-бот «Третьяковка vs Русский музей»

## Запуск локально
1. Python 3.11+
2. `pip install -r requirements.txt`
3. Установить переменную окружения `BOT_TOKEN` (токен @BotFather).
4. `python main.py` и в Telegram ввести /start.

## Деплой на Railway
- Создать сервис из репозитория / архива.
- Variables: `BOT_TOKEN`, (опц.) `DB_PATH`, `DATA_PATH`.
- Процесс запуска берётся из Procfile.

## Формат данных
`data/paintings.json` — массив объектов: `title, artist, year, museum, image_url, note, source`.
