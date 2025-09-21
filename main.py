import os
import json
import random
import time
import sqlite3
import asyncio
from datetime import datetime, timezone, timedelta

from telegram import InlineKeyboardMarkup, InlineKeyboardButton, Update, InputMediaPhoto
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import Application, ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes

BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATA_PATH = os.environ.get("DATA_PATH", "data/paintings.json")
DB_PATH = os.environ.get("DB_PATH", "bot.sqlite3")

VALID_MUSEUMS = {"Русский музей", "Третьяковская галерея"}
WEEK_WINDOW_DAYS = 7
DAILY_LIMIT = 16  # дневной лимит показов карточек на пользователя

PAINTINGS = None

# -------------------- Utilities: dates & quota --------------------

def _today_key() -> str:
    return datetime.now(timezone.utc).strftime('%Y%m%d')

def _today_date_str_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()

def _tomorrow_9utc_epoch() -> int:
    # Отправляем на следующее утро в 09:00 UTC (можно поменять или сделать per-user TZ)
    now = datetime.now(timezone.utc)
    tomorrow = (now + timedelta(days=1)).date()
    dt = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 9, 0, 0, tzinfo=timezone.utc)
    return int(dt.timestamp())

def get_used_today(con: sqlite3.Connection, user_id: int) -> int:
    day = _today_key()
    row = con.execute("SELECT used FROM daily_quota WHERE user_id=? AND day=?", (user_id, day)).fetchone()
    return row[0] if row else 0

def inc_used_today(con: sqlite3.Connection, user_id: int, delta: int = 1) -> None:
    day = _today_key()
    con.execute(
        """
        INSERT INTO daily_quota(user_id, day, used)
        VALUES(?,?,?)
        ON CONFLICT(user_id, day) DO UPDATE SET used = daily_quota.used + excluded.used
        """,
        (user_id, day, delta),
    )
    con.commit()

# -------------------- DB init & data loading --------------------

def db_init():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            created_at INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stats(
            user_id INTEGER PRIMARY KEY,
            correct INTEGER DEFAULT 0,
            total INTEGER DEFAULT 0,
            updated_at INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS leaderboard(
            user_id INTEGER PRIMARY KEY,
            correct INTEGER DEFAULT 0,
            total INTEGER DEFAULT 0,
            ts INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions(
            user_id INTEGER PRIMARY KEY,
            q_title TEXT,
            q_artist TEXT,
            q_year TEXT,
            q_museum TEXT,
            q_image_url TEXT,
            q_note TEXT,
            ts INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS decks(
            user_id INTEGER PRIMARY KEY,
            deck_json TEXT NOT NULL,
            shown_json TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_quota(
            user_id INTEGER NOT NULL,
            day TEXT NOT NULL,
            used INTEGER DEFAULT 0,
            PRIMARY KEY(user_id, day)
        )
    """)
    # Очередь отложенных сообщений со статистикой
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stats_queue(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            stats_date TEXT NOT NULL,   -- дата (UTC) за которую сформирована статистика
            payload TEXT NOT NULL,      -- зафиксированный текст статистики на конец дня
            send_at INTEGER NOT NULL,   -- unix epoch (UTC) когда отправить
            sent_at INTEGER,            -- unix epoch когда фактически отправили
            UNIQUE(user_id, stats_date) -- не дублировать одно и то же за день
        )
    """)
    con.commit()
    con.close()

def load_paintings():
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    cleaned = []
    for item in data:
        museum = item.get("museum", "").strip()
        if museum in VALID_MUSEUMS and item.get("image_url"):
            cleaned.append({
                "title": item.get("title", "").strip(),
                "artist": item.get("artist", "").strip(),
                "year": item.get("year", "").strip(),
                "museum": museum,
                "image_url": item["image_url"].strip(),
                "note": item.get("note", "").strip()
            })

    if not cleaned:
        raise RuntimeError("В paintings.json нет валидных записей для игры.")

    return cleaned

# -------------------- User & stats helpers --------------------

def ensure_user(update: Update):
    user = update.effective_user
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute(
            """
            INSERT INTO users(user_id, username, first_name, last_name, created_at)
            VALUES(?,?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_name=excluded.last_name
            """,
            (user.id, user.username, user.first_name, user.last_name, int(time.time())),
        )
        con.commit()
    finally:
        con.close()

def update_stats(con: sqlite3.Connection, user_id: int, correct: bool):
    cur = con.cursor()
    row = cur.execute("SELECT correct, total FROM stats WHERE user_id=?", (user_id,)).fetchone()
    if not row:
        correct_cnt = 1 if correct else 0
        total_cnt = 1
        cur.execute(
            "INSERT INTO stats(user_id, correct, total, updated_at) VALUES(?,?,?,?)",
            (user_id, correct_cnt, total_cnt, int(time.time()))
        )
    else:
        correct_cnt, total_cnt = row
        correct_cnt += 1 if correct else 0
        total_cnt += 1
        cur.execute(
            "UPDATE stats SET correct=?, total=?, updated_at=? WHERE user_id=?",
            (correct_cnt, total_cnt, int(time.time()), user_id)
        )
    cur.execute("""
        INSERT INTO leaderboard(user_id, correct, total, ts)
        VALUES(?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
            correct=leaderboard.correct + excluded.correct,
            total=leaderboard.total + excluded.total,
            ts=excluded.ts
    """, (user_id, 1 if correct else 0, 1, int(time.time())))
    con.commit()

def leaderboard_top(limit: int = 10):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    now = int(time.time())
    week_ago = now - WEEK_WINDOW_DAYS * 86400
    cur.execute("""
        SELECT l.user_id, l.correct, l.total, u.username, u.first_name, u.last_name
        FROM leaderboard l
        JOIN users u ON u.user_id = l.user_id
        WHERE l.ts >= ?
        ORDER BY (CAST(l.correct AS REAL)/NULLIF(l.total,0)) DESC, l.correct DESC, l.total ASC
        LIMIT ?
    """, (week_ago, limit))
    rows = cur.fetchall()
    con.close()
    return rows

def _format_stats_payload(con: sqlite3.Connection, user_id: int) -> str:
    row = con.execute("SELECT correct, total FROM stats WHERE user_id=?", (user_id,)).fetchone()
    if not row:
        return "Статистика пока пустая. Нажми /play, чтобы начать."
    correct, total = row
    acc = (correct / total * 100) if total else 0.0
    return (
        "Твоя статистика за вчерашний день:\n"
        f"Правильных ответов: {correct}/{total} ({acc:.1f}%)"
    )

def _enqueue_tomorrow_stats(user_id: int) -> None:
    """Фиксируем сегодняшнюю статистику и планируем отправку на завтра утром."""
    con = sqlite3.connect(DB_PATH)
    try:
        payload = _format_stats_payload(con, user_id)
        stats_date = _today_date_str_utc()     # дата, за которую статистика
        send_at = _tomorrow_9utc_epoch()       # когда отправить
        con.execute(
            "INSERT OR IGNORE INTO stats_queue(user_id, stats_date, payload, send_at) VALUES(?,?,?,?)",
            (user_id, stats_date, payload, send_at)
        )
        con.commit()
    finally:
        con.close()

# -------------------- Deck (no-repeat, fixed order for all users) --------------------

_rng = random.SystemRandom()

def _new_deck(n: int):
    deck = list(range(n))
    # IMPORTANT: keep the SAME global order for every user — do NOT shuffle
    # _rng.shuffle(deck)
    return deck

def _load_deck(con: sqlite3.Connection, user_id: int):
    cur = con.cursor()
    row = cur.execute("SELECT deck_json, shown_json FROM decks WHERE user_id=?", (user_id,)).fetchone()
    if row is None:
        deck, shown = _new_deck(len(PAINTINGS)), []
        cur.execute("INSERT INTO decks(user_id, deck_json, shown_json) VALUES(?,?,?)",
                    (user_id, json.dumps(deck), json.dumps(shown)))
        con.commit()
        return deck, shown
    return json.loads(row[0]), json.loads(row[1])

def _save_deck(con: sqlite3.Connection, user_id: int, deck, shown):
    con.execute("UPDATE decks SET deck_json=?, shown_json=? WHERE user_id=?",
                (json.dumps(deck), json.dumps(shown), user_id))
    con.commit()

def draw_next_painting(con: sqlite3.Connection, user_id: int) -> dict:
    deck, shown = _load_deck(con, user_id)
    if not deck:
        deck = _new_deck(len(PAINTINGS))
        shown = []
    idx = deck.pop(0)
    shown.append(idx)
    _save_deck(con, user_id, deck, shown)
    return PAINTINGS[idx]

# -------------------- Sessions --------------------

def save_session(user_id: int, q: dict):
    """Сохраняем текущий вопрос для пользователя (для последующей проверки ответа)."""
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("""
            INSERT INTO sessions(user_id, q_title, q_artist, q_year, q_museum, q_image_url, q_note, ts)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                q_title=excluded.q_title,
                q_artist=excluded.q_artist,
                q_year=excluded.q_year,
                q_museum=excluded.q_museum,
                q_image_url=excluded.q_image_url,
                q_note=excluded.q_note,
                ts=excluded.ts
        """, (
            user_id,
            q.get("title"),
            q.get("artist"),
            q.get("year"),
            q.get("museum"),
            q.get("image_url"),
            q.get("note"),
            int(time.time())
        ))
        con.commit()
    finally:
        con.close()

# -------------------- UI helpers --------------------

def answer_keyboard():
    # Only museum choices, no "next" button – we auto-advance after answer
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("1) Русский музей", callback_data="ans:Русский музей"),
            InlineKeyboardButton("2) Третьяковская галерея", callback_data="ans:Третьяковская галерея")
        ]
    ])

# -------------------- Handlers --------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user(update)
    text = (
        "Привет! Это викторина «Третьяковка vs Русский музей».\n\n"
        "Нажми /play чтобы начать: я покажу картину, а ты угадай, из какого музея она.\n"
        "Команды: /play, /stats, /top"
    )
    await update.effective_message.reply_text(text)

async def play(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user(update)
    user_id = update.effective_user.id
    con = sqlite3.connect(DB_PATH)
    try:
        used = get_used_today(con, user_id)
        if used >= DAILY_LIMIT:
            _enqueue_tomorrow_stats(user_id)
            await update.effective_message.reply_text("На сегодня всё. Приходите завтра!")
            return

        q = draw_next_painting(con, user_id)
        inc_used_today(con, user_id, 1)
        save_session(user_id, q)

        caption = (
            f"🖼 <b>{q['title']}</b>\n{q['artist']}, {q['year']}\n\n"
            "<i>Из какого музея эта работа?</i>"
        )

        try:
            await update.effective_message.reply_photo(
                photo=q["image_url"],
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=answer_keyboard()
            )
            # Успешно: удаляем предыдущее сообщение об ошибке, если было
            err_id = context.user_data.pop("last_error_msg_id", None)
            if err_id:
                try:
                    await context.bot.delete_message(
                        chat_id=update.effective_chat.id,
                        message_id=err_id
                    )
                except Exception:
                    pass
        except BadRequest:
            msg = await update.effective_message.reply_text("Не удалось показать картину, попробуйте ещё раз")
            context.user_data["last_error_msg_id"] = msg.message_id
            return await play(update, context)
        except Exception:
            msg = await update.effective_message.reply_text("Не удалось показать картину, попробуйте ещё раз")
            context.user_data["last_error_msg_id"] = msg.message_id
            return await play(update, context)
    finally:
        con.close()

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    data = (query.data or "")

    if not data.startswith("ans:"):
        return

    chosen = data.split(":", 1)[1]

    con = sqlite3.connect(DB_PATH)
    try:
        row = con.execute(
            "SELECT q_title, q_artist, q_year, q_museum, q_image_url, q_note FROM sessions WHERE user_id=?",
            (user_id,)
        ).fetchone()
        if not row:
            await query.edit_message_caption(caption="Сессия не найдена. Нажми /play.")
            # авто-переход к следующему на всякий случай
            return await play(update, context)

        q_title, q_artist, q_year, q_museum, q_image_url, q_note = row
        is_correct = (chosen == q_museum)
        update_stats(con, user_id, is_correct)

        result = "✅ Верно!" if is_correct else f"❌ Неверно. Правильно: {q_museum}"
        extra = f"\n\n<b>{q_title}</b>\n<i>{q_artist}</i>, {q_year}\n\n{q_note}" if q_note else ""
        try:
            # Меняем подпись у текущего сообщения...
            await query.edit_message_caption(
                caption=result + extra,
                parse_mode=ParseMode.HTML,
            )
        except BadRequest:
            # Если не получилось — отправим отдельным сообщением
            await query.message.reply_text(result + extra, parse_mode=ParseMode.HTML)
    finally:
        con.close()

    # ...и сразу показываем следующее изображение — без кнопки "Следующая"
    return await play(update, context)

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user(update)
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT correct, total FROM stats WHERE user_id=?", (update.effective_user.id,))
    row = cur.fetchone()
    con.close()
    if not row:
        await update.effective_message.reply_text("Статистика пока пустая. Нажми /play, чтобы начать.")
        return
    correct, total = row
    acc = (correct / total * 100) if total else 0.0
    await update.effective_message.reply_text(
        f"Твоя статистика:\nПравильных ответов: {correct}/{total} ({acc:.1f}%)"
    )

async def _send_due_stats_job(context: ContextTypes.DEFAULT_TYPE):
    """Фоновая задача: отправить все отложенные статистики, которые уже пора отправить."""
    now_ts = int(time.time())  # UTC
    con = sqlite3.connect(DB_PATH)
    try:
        rows = con.execute(
            """
            SELECT id, user_id, payload FROM stats_queue
            WHERE sent_at IS NULL AND send_at <= ?
            ORDER BY send_at ASC
            LIMIT 50
            """,
            (now_ts,)
        ).fetchall()
        for q_id, user_id, payload in rows:
            try:
                await context.bot.send_message(chat_id=user_id, text=payload)
                con.execute("UPDATE stats_queue SET sent_at=? WHERE id=?", (now_ts, q_id))
                con.commit()
            except Exception:
                # Оставляем неотправленным — повторим на следующем тике
                pass
    finally:
        con.close()

async def top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = leaderboard_top()
    if not rows:
        await update.effective_message.reply_text("Пока нет результатов за последние 7 дней.")
        return
    lines = ["🏆 Топ за 7 дней:"]
    for idx, (user_id, correct, total, username, first_name, last_name) in enumerate(rows, 1):
        if username:
            who = f"@{username}"
        else:
            parts = [x for x in [first_name, last_name] if x]
            who = " ".join(parts) if parts else f"id:{user_id}"
        acc = (correct / total * 100) if total else 0.0
        lines.append(f"{idx}. {who}: {correct}/{total} ({acc:.1f}%)")
    await update.effective_message.reply_text("\n".join(lines))

# -------------------- App bootstrap --------------------

def main():
    global PAINTINGS
    db_init()
    PAINTINGS = load_paintings()
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN в переменных окружения.")

    app: Application = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("play", play))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("top", top))
    app.add_handler(CallbackQueryHandler(on_callback))

    # Каждую минуту проверяем, нет ли отложенных статистик для отправки
    app.job_queue.run_repeating(_send_due_stats_job, interval=60, first=10)

    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
