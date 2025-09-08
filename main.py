import os
import json
import random
import time
import sqlite3

from datetime import datetime, timezone

DAILY_LIMIT = 15  # дневной лимит показов карточек на пользователя

def _today_key() -> str:
    return datetime.now(timezone.utc).strftime('%Y%m%d')

def get_used_today(con: sqlite3.Connection, user_id: int) -> int:
    day = _today_key()
    row = con.execute(
        'SELECT used FROM daily_quota WHERE user_id=? AND day=?',
        (user_id, day)
    ).fetchone()
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

from telegram import InlineKeyboardMarkup, InlineKeyboardButton, Update, InputMediaPhoto
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import Application, ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes

BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATA_PATH = os.environ.get("DATA_PATH", "data/paintings.json")
DB_PATH = os.environ.get("DB_PATH", "bot.sqlite3")
VALID_MUSEUMS = {"Русский музей", "Третьяковская галерея"}
WEEK_WINDOW_DAYS = 7

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
            streak INTEGER DEFAULT 0,
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
        CREATE TABLE IF NOT EXISTS decks (
          user_id    INTEGER PRIMARY KEY,
          deck_json  TEXT    NOT NULL,
          shown_json TEXT    NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_quota(
            user_id INTEGER NOT NULL,
            day TEXT NOT NULL,
            used INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, day)
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

PAINTINGS = None

# --- Deck-based no-repeat randomizer ---
import json as _json
_rng = random.SystemRandom()

def _new_deck(n: int):
    deck = list(range(n))
    ### _rng.shuffle(deck) ## Do not shuffle
    return deck

def _load_deck(con, user_id: int):
    cur = con.cursor()
    row = cur.execute("SELECT deck_json, shown_json FROM decks WHERE user_id=?", (user_id,)).fetchone()
    if row is None:
        deck, shown = _new_deck(len(PAINTINGS)), []
        cur.execute("INSERT INTO decks(user_id, deck_json, shown_json) VALUES(?,?,?)",
                    (user_id, _json.dumps(deck), _json.dumps(shown)))
        con.commit()
        return deck, shown
    return _json.loads(row[0]), _json.loads(row[1])

def _save_deck(con, user_id: int, deck, shown):
    con.execute("UPDATE decks SET deck_json=?, shown_json=? WHERE user_id=?",
                (_json.dumps(deck), _json.dumps(shown), user_id))
    con.commit()

def draw_next_painting(con, user_id: int) -> dict:
    deck, shown = _load_deck(con, user_id)
    if not deck:
        deck, shown = _new_deck(len(PAINTINGS)), []
    idx = deck.pop()
    shown.append(idx)
    _save_deck(con, user_id, deck, shown)
    return PAINTINGS[idx]


def ensure_user(update: Update):
    u = update.effective_user
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("INSERT OR IGNORE INTO users(user_id, username, first_name, last_name, created_at) VALUES(?,?,?,?,?)",
                (u.id, u.username, u.first_name, u.last_name, int(time.time())))
    cur.execute("INSERT OR IGNORE INTO stats(user_id, correct, total, streak, updated_at) VALUES(?,?,?,?,?)",
                (u.id, 0, 0, 0, int(time.time())))
    con.commit()
    con.close()

def save_session(user_id: int, q: dict):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO sessions(user_id, q_title, q_artist, q_year, q_museum, q_image_url, q_note, ts)
        VALUES(?,?,?,?,?,?,?,?)
    """, (user_id, q["title"], q["artist"], q["year"], q["museum"], q["image_url"], q.get("note",""), int(time.time())))
    con.commit()
    con.close()

def get_session(user_id: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT q_title, q_artist, q_year, q_museum, q_image_url, q_note, ts
        FROM sessions WHERE user_id=?
    """, (user_id,))
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    keys = ["title", "artist", "year", "museum", "image_url", "note", "ts"]
    return dict(zip(keys, row))

def update_stats(user_id: int, correct: bool):
    now = int(time.time())
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT correct, total, streak FROM stats WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    c, t, s = row if row else (0, 0, 0)
    t += 1
    if correct:
        c += 1
        s += 1
    else:
        s = 0
    cur.execute("INSERT OR REPLACE INTO stats(user_id, correct, total, streak, updated_at) VALUES(?,?,?,?,?)",
                (user_id, c, t, s, now))

    cur.execute("SELECT correct, total, ts FROM leaderboard WHERE user_id=?", (user_id,))
    row2 = cur.fetchone()
    if not row2:
        lc, lt = (1 if correct else 0), 1
    else:
        lc, lt, _ = row2
        lc += (1 if correct else 0)
        lt += 1
    cur.execute("INSERT OR REPLACE INTO leaderboard(user_id, correct, total, ts) VALUES(?,?,?,?)",
                (user_id, lc, lt, now))
    con.commit()
    con.close()

def leaderboard_top(limit=10):
    now = int(time.time())
    week_ago = now - 7 * 86400
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
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

def answer_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("1) Русский музей", callback_data="ans:Русский музей"),
         InlineKeyboardButton("2) Третьяковская галерея", callback_data="ans:Третьяковская галерея")]
    ])

def next_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("Ещё картину ▶️", callback_data="next")]])

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
            await stats(update, context)
            await update.effective_message.reply_text("На сегодня всё. Приходите завтра!")
            return
        q = draw_next_painting(con, user_id)
        inc_used_today(con, user_id, 1)
    finally:
        con.close()
    save_session(user_id, q)
    caption = f"🖼 <b>{q['title']}</b>\n{q['artist']}, {q['year']}\n\n<i>Из какого музея эта работа?</i>"
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
                await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=err_id)
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

async def on_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    # If "next" pressed (legacy), just load the next artwork
    if data == "next":
        return await play(update, context)

    # Only handle answers like "ans:<museum>"
    if not isinstance(data, str) or not data.startswith("ans:"):
        return

    chosen = data.split(":", 1)[1]

    session = get_session(user_id)
    if not session:
        # No session -> prompt to start and immediately continue
        try:
            await query.message.edit_caption(caption="Сессия не найдена. Нажмите /play", parse_mode=ParseMode.HTML, reply_markup=None)
        except Exception:
            pass
        return await play(update, context)

    is_correct = (chosen == session["museum"])
    update_stats(user_id, is_correct)

    verdict = "✅ Правильно!" if is_correct else f"❌ Неверно. Правильный ответ: <b>{session['museum']}</b>."
    note = (" " + session["note"]) if session.get("note") else ""
    caption = f"🖼 <b>{session['title']}</b>\n{session['artist']}, {session['year']}\n\n{verdict}{note}"

    # Show verdict on the same message (remove the old answer buttons)
    try:
        await query.message.edit_caption(caption=caption, parse_mode=ParseMode.HTML, reply_markup=None)
    except Exception:
        pass

    # Immediately load the next artwork (no 'Еще картину' button)
    return await play(update, context)


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user(update)
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT correct, total, streak FROM stats WHERE user_id=?", (update.effective_user.id,))
    row = cur.fetchone()
    con.close()
    if not row:
        await update.effective_message.reply_text("Статистика пока пустая. Нажми /play, чтобы начать.")
        return
    correct, total, streak = row
    acc = (correct / total * 100) if total else 0.0
    await update.effective_message.reply_text(
        f"Твоя статистика:\nПравильных ответов: {correct}/{total} ({acc:.1f}%)\nСерия подряд: {streak}"
    )
    
async def top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = leaderboard_top()
    if not rows:
        await update.effective_message.reply_text("Пока нет результатов за последние 7 дней.")
        return
    lines = ["🏆 Топ за 7 дней:"]
    for idx, (user_id, correct, total, username, first_name, last_name) in enumerate(rows, 1):
        if username:
            name = f"@{username}"
        else:
            full = " ".join([n for n in [first_name, last_name] if n])
            name = full if full else f"ID {user_id}"
        rate = (correct / total * 100) if total else 0.0
        lines.append(f"{idx}. {name}: {correct}/{total} ({rate:.1f}%)")
    await update.effective_message.reply_text("\n".join(lines))

# async def start(update: Update, context):
#     keyboard = [
#         [InlineKeyboardButton("Запустить игру 🎨", web_app={"url": "https://igorrodygin.github.io/what-museum-miniapp/"})]
#     ]
#     reply_markup = InlineKeyboardMarkup(keyboard)
#     await update.message.reply_text("Добро пожаловать! Жми кнопку ниже, чтобы сыграть 👇", reply_markup=reply_markup)


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
    app.add_handler(CallbackQueryHandler(on_answer))
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()