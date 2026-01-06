import os
import json
import random
import time
import sqlite3
import hashlib
import hmac
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

from telegram import InlineKeyboardMarkup, InlineKeyboardButton, Update, InputMediaPhoto
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATA_PATH = os.environ.get("DATA_PATH", "data/paintings.json")
DB_PATH = os.environ.get("DB_PATH", "bot.sqlite3")

# Defines "day" for daily plan + quota
BOT_TZ = os.environ.get("BOT_TZ", "Europe/Amsterdam")
_TZ = ZoneInfo(BOT_TZ)

VALID_MUSEUMS = {"Русский музей", "Третьяковская галерея"}

WEEK_WINDOW_DAYS = 7
DAILY_LIMIT = int(os.environ.get("DAILY_LIMIT", "10"))
DIFFICULT_WINDOW_DAYS = int(os.environ.get("DIFFICULT_WINDOW_DAYS", "1"))

# ---- Option A config (global daily plan) ----
PLAN_VERSION = 1
REVIEW_EVERY = int(os.environ.get("REVIEW_EVERY", "4"))  # insert REVIEW after each (REVIEW_EVERY-1) NEW slots
REVIEW_TAIL_SLOTS = int(os.environ.get("REVIEW_TAIL_SLOTS", str(DAILY_LIMIT * 50)))
REVIEW_PREFIX_SLOTS = int(os.environ.get("REVIEW_PREFIX_SLOTS", "0"))

GLOBAL_READY_MIN_TOTAL_ATTEMPTS = int(os.environ.get("GLOBAL_READY_MIN_TOTAL_ATTEMPTS", "200"))
GLOBAL_READY_MIN_PICTURES = int(os.environ.get("GLOBAL_READY_MIN_PICTURES", "15"))
GLOBAL_READY_MIN_ATTEMPTS_PER_PICTURE = int(os.environ.get("GLOBAL_READY_MIN_ATTEMPTS_PER_PICTURE", "5"))
GLOBAL_TOP_MISTAKES_LIMIT = int(os.environ.get("GLOBAL_TOP_MISTAKES_LIMIT", "30"))

USER_TOP_MISTAKES_LIMIT = int(os.environ.get("USER_TOP_MISTAKES_LIMIT", "30"))
USER_MIN_ATTEMPTS_PER_PICTURE = int(os.environ.get("USER_MIN_ATTEMPTS_PER_PICTURE", "2"))

CYCLE_COOLDOWN_SECONDS = int(os.environ.get("CYCLE_COOLDOWN_SECONDS", str(7 * 86400)))

MAX_SCAN_SLOTS_PER_PLAY = int(os.environ.get("MAX_SCAN_SLOTS_PER_PLAY", "2000"))

GLOBAL_PLAN_SECRET = os.environ.get("GLOBAL_PLAN_SECRET") or BOT_TOKEN or "tretyakov-vs-rusmuseum"

# Catalog globals
PAINTINGS: List[Dict[str, Any]] = []
PAINTINGS_BY_ID: Dict[str, Dict[str, Any]] = {}
ALL_PICTURE_IDS: List[str] = []

# Catalog reverse-lookup indexes
CATALOG_BY_KEY4: Dict[Tuple[str, str, str, str], List[str]] = {}
CATALOG_BY_KEY5: Dict[Tuple[str, str, str, str, str], str] = {}


# -------------------- DB connection --------------------

def connect_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30)
    # Better concurrency characteristics for a bot workload
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    return con


# -------------------- Utilities: dates & quota --------------------

def _today_key() -> str:
    return datetime.now(_TZ).strftime("%Y%m%d")


def _today_date_str_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _tomorrow_9utc_epoch() -> int:
    now = datetime.now(timezone.utc)
    tomorrow = (now + timedelta(days=1)).date()
    dt = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 9, 0, 0, tzinfo=timezone.utc)
    return int(dt.timestamp())


def get_used_today(con: sqlite3.Connection, user_id: int) -> int:
    day = _today_key()
    row = con.execute("SELECT used FROM daily_quota WHERE user_id=? AND day=?", (user_id, day)).fetchone()
    return int(row[0]) if row else 0


def inc_used_today_tx(con: sqlite3.Connection, user_id: int, delta: int = 1) -> None:
    day = _today_key()
    con.execute(
        """
        INSERT INTO daily_quota(user_id, day, used) VALUES(?,?,?)
        ON CONFLICT(user_id, day) DO UPDATE SET used = daily_quota.used + excluded.used
        """,
        (user_id, day, delta),
    )


# -------------------- DB init & migrations --------------------

def _table_has_column(con: sqlite3.Connection, table: str, column: str) -> bool:
    cur = con.execute(f"PRAGMA table_info({table})")
    return any(r[1] == column for r in cur.fetchall())


def _ensure_column(con: sqlite3.Connection, table: str, column: str, coltype: str) -> None:
    if not _table_has_column(con, table, column):
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")


def db_init() -> None:
    con = connect_db()
    cur = con.cursor()

    # Core tables
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
            q_picture_id TEXT,
            q_title TEXT,
            q_artist TEXT,
            q_year TEXT,
            q_museum TEXT,
            q_image_url TEXT,
            q_note TEXT,
            ts INTEGER
        )
    """)

    # Old table kept (no longer used)
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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS stats_queue(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            stats_date TEXT NOT NULL,
            payload TEXT NOT NULL,
            send_at INTEGER NOT NULL,
            sent_at INTEGER,
            UNIQUE(user_id, stats_date)
        )
    """)

    # Answer log: keep old columns for compatibility; extend with picture_id
    cur.execute("""
        CREATE TABLE IF NOT EXISTS painting_results(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            picture_id TEXT,
            title TEXT NOT NULL,
            artist TEXT NOT NULL,
            year TEXT NOT NULL,
            museum TEXT NOT NULL,
            image_url TEXT NOT NULL,
            is_correct INTEGER NOT NULL,
            ts INTEGER NOT NULL
        )
    """)

    # ---- Option A tables ----
    cur.execute("""
        CREATE TABLE IF NOT EXISTS global_daily_plan(
            day_key TEXT PRIMARY KEY,
            items_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            plan_version INTEGER NOT NULL,
            seed INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_day_progress(
            user_id INTEGER NOT NULL,
            day_key TEXT NOT NULL,
            cursor INTEGER DEFAULT 0,
            PRIMARY KEY(user_id, day_key)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_cycle_state(
            user_id INTEGER PRIMARY KEY,
            cycle_id INTEGER NOT NULL,
            started_at INTEGER NOT NULL,
            completed_at INTEGER,
            total_pictures_snapshot INTEGER NOT NULL,
            seen_count INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_picture_state(
            user_id INTEGER NOT NULL,
            picture_id TEXT NOT NULL,
            last_seen_cycle_id INTEGER,
            last_seen_at INTEGER,
            last_wrong_at INTEGER,
            attempts INTEGER DEFAULT 0,
            wrong INTEGER DEFAULT 0,
            correct INTEGER DEFAULT 0,
            PRIMARY KEY(user_id, picture_id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS global_picture_state(
            picture_id TEXT PRIMARY KEY,
            attempts INTEGER DEFAULT 0,
            wrong INTEGER DEFAULT 0,
            correct INTEGER DEFAULT 0,
            updated_at INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta(
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    # Pending fields (two-phase commit for /play)
    _ensure_column(con, "sessions", "pending_day_key", "TEXT")
    _ensure_column(con, "sessions", "pending_slot_index", "INTEGER")
    _ensure_column(con, "sessions", "pending_next_cursor", "INTEGER")
    _ensure_column(con, "sessions", "pending_cycle_id", "INTEGER")
    _ensure_column(con, "sessions", "pending_kind", "TEXT")

    # Migrations
    _ensure_column(con, "painting_results", "picture_id", "TEXT")
    _ensure_column(con, "sessions", "q_picture_id", "TEXT")

    # Indexes (performance + fewer locks)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_painting_results_ts ON painting_results(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_painting_results_pid ON painting_results(picture_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_user_picture_state_user ON user_picture_state(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_global_picture_state_attempts ON global_picture_state(attempts)")

    con.commit()
    con.close()


# -------------------- Paintings loading & catalog indexes --------------------

def _canon(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def load_paintings() -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]], List[str]]:
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    cleaned: List[Dict[str, Any]] = []
    by_id: Dict[str, Dict[str, Any]] = {}

    for item in data:
        museum = (item.get("museum") or "").strip()
        image_url = (item.get("image_url") or "").strip()
        if museum not in VALID_MUSEUMS or not image_url:
            continue

        pid = (item.get("id") or "").strip()
        if not pid:
            raise RuntimeError("paintings.json must contain stable 'id' for each record now")

        rec = {
            "id": pid,
            "title": (item.get("title") or "").strip(),
            "artist": (item.get("artist") or "").strip(),
            "year": (item.get("year") or "").strip(),
            "museum": museum,
            "image_url": image_url,
            "note": (item.get("note") or "").strip(),
        }

        if pid in by_id:
            raise RuntimeError(f"Duplicate picture id in paintings.json: {pid}")

        by_id[pid] = rec
        cleaned.append(rec)

    if not cleaned:
        raise RuntimeError("В paintings.json нет валидных записей для игры.")

    all_ids = [p["id"] for p in cleaned]
    return cleaned, by_id, all_ids


def _key4_from_fields(title: str, artist: str, year: str, museum: str) -> Tuple[str, str, str, str]:
    return (_canon(title), _canon(artist), _canon(year), _canon(museum))


def _key5_from_fields(title: str, artist: str, year: str, museum: str, image_url: str) -> Tuple[str, str, str, str, str]:
    return (_canon(title), _canon(artist), _canon(year), _canon(museum), _canon(image_url))


def build_catalog_indexes() -> None:
    global CATALOG_BY_KEY4, CATALOG_BY_KEY5
    CATALOG_BY_KEY4 = {}
    CATALOG_BY_KEY5 = {}
    for p in PAINTINGS:
        k4 = _key4_from_fields(p["title"], p["artist"], p["year"], p["museum"])
        CATALOG_BY_KEY4.setdefault(k4, []).append(p["id"])
        k5 = _key5_from_fields(p["title"], p["artist"], p["year"], p["museum"], p["image_url"])
        CATALOG_BY_KEY5[k5] = p["id"]


def resolve_picture_id(
    picture_id: Optional[str],
    title: str,
    artist: str,
    year: str,
    museum: str,
    image_url: str,
) -> Optional[str]:
    if picture_id and picture_id in PAINTINGS_BY_ID:
        return picture_id

    k5 = _key5_from_fields(title, artist, year, museum, image_url)
    if k5 in CATALOG_BY_KEY5:
        return CATALOG_BY_KEY5[k5]

    k4 = _key4_from_fields(title, artist, year, museum)
    ids = CATALOG_BY_KEY4.get(k4, [])
    if len(ids) == 1:
        return ids[0]

    return None


# -------------------- Users & leaderboard --------------------

def ensure_user(update: Update) -> None:
    user = update.effective_user
    con = connect_db()
    try:
        with con:
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
    finally:
        con.close()


def update_stats(con: sqlite3.Connection, user_id: int, correct: bool) -> None:
    row = con.execute("SELECT correct, total FROM stats WHERE user_id=?", (user_id,)).fetchone()
    if not row:
        con.execute(
            "INSERT INTO stats(user_id, correct, total, updated_at) VALUES(?,?,?,?)",
            (user_id, 1 if correct else 0, 1, int(time.time())),
        )
    else:
        correct_cnt, total_cnt = row
        con.execute(
            "UPDATE stats SET correct=?, total=?, updated_at=? WHERE user_id=?",
            (int(correct_cnt) + (1 if correct else 0), int(total_cnt) + 1, int(time.time()), user_id),
        )

    # Rolling leaderboard accumulator (simple and cheap)
    con.execute(
        """
        INSERT INTO leaderboard(user_id, correct, total, ts)
        VALUES(?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
            correct = leaderboard.correct + excluded.correct,
            total = leaderboard.total + excluded.total,
            ts = excluded.ts
        """,
        (user_id, 1 if correct else 0, 1, int(time.time())),
    )


def leaderboard_top(limit: int = 10):
    con = connect_db()
    try:
        now = int(time.time())
        week_ago = now - WEEK_WINDOW_DAYS * 86400
        rows = con.execute(
            """
            SELECT l.user_id, l.correct, l.total, u.username, u.first_name, u.last_name
            FROM leaderboard l
            JOIN users u ON u.user_id = l.user_id
            WHERE l.ts >= ?
            ORDER BY (CAST(l.correct AS REAL)/NULLIF(l.total,0)) DESC, l.correct DESC, l.total ASC
            LIMIT ?
            """,
            (week_ago, limit),
        ).fetchall()
        return rows
    finally:
        con.close()


# -------------------- Existing hardest pictures window (kept) --------------------

def hardest_paintings_window(days: int = DIFFICULT_WINDOW_DAYS, limit: int = 1, min_attempts: int = 2):
    cutoff = int(time.time()) - days * 86400
    con = connect_db()
    try:
        rows = con.execute(
            """
            SELECT
              title,
              artist,
              year,
              museum,
              image_url,
              SUM(CASE WHEN is_correct=0 THEN 1 ELSE 0 END) AS wrong,
              COUNT(*) AS total
            FROM painting_results
            WHERE ts >= ?
            GROUP BY title, artist, year, museum, image_url
            HAVING total >= ?
            ORDER BY (wrong * 1.0 / total) DESC, total DESC
            LIMIT ?
            """,
            (cutoff, min_attempts, limit),
        ).fetchall()

        out = []
        for r in rows:
            title, artist, year, museum, image_url, wrong, total = r
            pct = (wrong / total * 100.0) if total else 0.0
            out.append((title, artist, year, museum, image_url, wrong, total, pct))
        return out
    finally:
        con.close()


# -------------------- Stats payload + queue (kept) --------------------

def _format_stats_payload(con: sqlite3.Connection, user_id: int) -> str:
    row = con.execute("SELECT correct, total FROM stats WHERE user_id=?", (user_id,)).fetchone()
    if not row:
        return "Статистика пока пустая. Нажми /play, чтобы начать."

    correct, total = row
    acc = (correct / total * 100) if total else 0.0
    return (
        "Твоя статистика за вчерашний день:

"
        f"Правильных ответов: {correct}/{total} ({acc:.1f}%)

"
        "Нажми /play, чтобы продолжить играть."
    )


def _enqueue_tomorrow_stats(user_id: int) -> None:
    con = connect_db()
    try:
        payload = _format_stats_payload(con, user_id)
        stats_date = _today_date_str_utc()
        send_at = _tomorrow_9utc_epoch()
        with con:
            con.execute(
                """
                INSERT INTO stats_queue(user_id, stats_date, payload, send_at)
                VALUES(?,?,?,?)
                ON CONFLICT(user_id, stats_date) DO NOTHING
                """,
                (user_id, stats_date, payload, send_at),
            )
    finally:
        con.close()


# -------------------- Option A: global daily plan + cycles --------------------

def _daily_seed(day_key: str) -> int:
    d = hmac.new(GLOBAL_PLAN_SECRET.encode("utf-8"), day_key.encode("utf-8"), hashlib.sha256).digest()
    return int.from_bytes(d[:8], "big", signed=False)


def _is_global_ready(con: sqlite3.Connection) -> bool:
    total_attempts = con.execute("SELECT COALESCE(SUM(attempts),0) FROM global_picture_state").fetchone()[0]
    if int(total_attempts) < GLOBAL_READY_MIN_TOTAL_ATTEMPTS:
        return False
    pics = con.execute(
        "SELECT COUNT(*) FROM global_picture_state WHERE attempts >= ?",
        (GLOBAL_READY_MIN_ATTEMPTS_PER_PICTURE,),
    ).fetchone()[0]
    return int(pics) >= GLOBAL_READY_MIN_PICTURES


def _get_global_top_mistakes(con: sqlite3.Connection, limit: int) -> List[str]:
    rows = con.execute(
        """
        SELECT picture_id
        FROM global_picture_state
        WHERE attempts >= ? AND wrong > 0
        ORDER BY (wrong * 1.0 / attempts) DESC, attempts DESC
        LIMIT ?
        """,
        (GLOBAL_READY_MIN_ATTEMPTS_PER_PICTURE, limit),
    ).fetchall()
    return [r[0] for r in rows if r[0] in PAINTINGS_BY_ID]


def _get_user_top_mistakes(con: sqlite3.Connection, user_id: int, limit: int) -> List[str]:
    rows = con.execute(
        """
        SELECT picture_id
        FROM user_picture_state
        WHERE user_id = ?
          AND attempts >= ?
          AND wrong > 0
        ORDER BY (wrong * 1.0 / attempts) DESC, attempts DESC
        LIMIT ?
        """,
        (user_id, USER_MIN_ATTEMPTS_PER_PICTURE, limit),
    ).fetchall()
    return [r[0] for r in rows if r[0] in PAINTINGS_BY_ID]


def ensure_global_daily_plan(con: sqlite3.Connection, day_key: str) -> List[Dict[str, Any]]:
    row = con.execute("SELECT items_json FROM global_daily_plan WHERE day_key=?", (day_key,)).fetchone()
    if row:
        return json.loads(row[0])

    seed = _daily_seed(day_key)
    rng = random.Random(seed)

    new_order = list(ALL_PICTURE_IDS)
    rng.shuffle(new_order)

    global_ready = _is_global_ready(con)
    review_ids: List[str] = []
    if global_ready:
        review_ids = _get_global_top_mistakes(con, GLOBAL_TOP_MISTAKES_LIMIT)
        rng.shuffle(review_ids)

    items: List[Dict[str, Any]] = []

    for i in range(max(0, REVIEW_PREFIX_SLOTS)):
        pid = review_ids[i % len(review_ids)] if (global_ready and review_ids) else None
        items.append({"kind": "REVIEW", "picture_id": pid})

    new_since_review = 0
    review_cursor = 0
    for pid in new_order:
        items.append({"kind": "NEW", "picture_id": pid})
        new_since_review += 1
        if REVIEW_EVERY > 0 and new_since_review >= max(1, REVIEW_EVERY - 1):
            new_since_review = 0
            review_pid = None
            if global_ready and review_ids:
                review_pid = review_ids[review_cursor % len(review_ids)]
                review_cursor += 1
            items.append({"kind": "REVIEW", "picture_id": review_pid})

    for i in range(max(0, REVIEW_TAIL_SLOTS)):
        review_pid = None
        if global_ready and review_ids:
            review_pid = review_ids[(review_cursor + i) % len(review_ids)]
        items.append({"kind": "REVIEW", "picture_id": review_pid})

    # Race-safe create
    with con:
        con.execute(
            """
            INSERT OR IGNORE INTO global_daily_plan(day_key, items_json, created_at, plan_version, seed)
            VALUES(?,?,?,?,?)
            """,
            (day_key, json.dumps(items), int(time.time()), PLAN_VERSION, seed),
        )

    row = con.execute("SELECT items_json FROM global_daily_plan WHERE day_key=?", (day_key,)).fetchone()
    return json.loads(row[0])


def _ensure_user_day_progress(con: sqlite3.Connection, user_id: int, day_key: str) -> int:
    with con:
        con.execute(
            "INSERT OR IGNORE INTO user_day_progress(user_id, day_key, cursor) VALUES(?,?,0)",
            (user_id, day_key),
        )
    row = con.execute(
        "SELECT cursor FROM user_day_progress WHERE user_id=? AND day_key=?",
        (user_id, day_key),
    ).fetchone()
    return int(row[0] or 0)


def _set_user_day_cursor(con: sqlite3.Connection, user_id: int, day_key: str, cursor: int) -> None:
    con.execute(
        """
        INSERT INTO user_day_progress(user_id, day_key, cursor)
        VALUES(?,?,?)
        ON CONFLICT(user_id, day_key) DO UPDATE SET cursor=excluded.cursor
        """,
        (user_id, day_key, cursor),
    )


def _get_or_advance_cycle(con: sqlite3.Connection, user_id: int, now_ts: int) -> Tuple[int, int, int, Optional[int]]:
    current_total = len(ALL_PICTURE_IDS)
    row = con.execute(
        "SELECT cycle_id, started_at, completed_at, total_pictures_snapshot, seen_count FROM user_cycle_state WHERE user_id=?",
        (user_id,),
    ).fetchone()

    if row is None:
        with con:
            con.execute(
                """
                INSERT INTO user_cycle_state(user_id, cycle_id, started_at, completed_at, total_pictures_snapshot, seen_count)
                VALUES(?,?,?,?,?,?)
                """,
                (user_id, 1, now_ts, None, current_total, 0),
            )
        return 1, current_total, 0, None

    cycle_id, started_at, completed_at, total_snapshot, seen_count = row
    cycle_id = int(cycle_id)
    total_snapshot = int(total_snapshot)
    seen_count = int(seen_count)
    completed_at = int(completed_at) if completed_at is not None else None

    # If catalog grew, update snapshot; if completed, re-open cycle so user can see new pictures
    if current_total > total_snapshot:
        total_snapshot = current_total
        if completed_at is not None:
            completed_at = None
        with con:
            con.execute(
                "UPDATE user_cycle_state SET total_pictures_snapshot=?, completed_at=? WHERE user_id=?",
                (total_snapshot, completed_at, user_id),
            )

    # If completed and cooldown passed -> new cycle
    if completed_at is not None and (now_ts - completed_at) >= CYCLE_COOLDOWN_SECONDS:
        cycle_id += 1
        started_at = now_ts
        completed_at = None
        total_snapshot = current_total
        seen_count = 0
        with con:
            con.execute(
                """
                UPDATE user_cycle_state
                SET cycle_id=?, started_at=?, completed_at=?, total_pictures_snapshot=?, seen_count=?
                WHERE user_id=?
                """,
                (cycle_id, started_at, completed_at, total_snapshot, seen_count, user_id),
            )

    return cycle_id, total_snapshot, seen_count, completed_at


def _bump_cycle_seen_if_first_time_this_cycle(
    con: sqlite3.Connection,
    user_id: int,
    picture_id: str,
    cycle_id: int,
    now_ts: int,
) -> None:
    row = con.execute(
        "SELECT last_seen_cycle_id FROM user_picture_state WHERE user_id=? AND picture_id=?",
        (user_id, picture_id),
    ).fetchone()
    last_seen_cycle_id = row[0] if row else None
    first_time_this_cycle = (last_seen_cycle_id != cycle_id)

    if row is None:
        con.execute(
            """
            INSERT INTO user_picture_state(user_id, picture_id, last_seen_cycle_id, last_seen_at, last_wrong_at, attempts, wrong, correct)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (user_id, picture_id, cycle_id, now_ts, None, 0, 0, 0),
        )
    else:
        con.execute(
            """
            UPDATE user_picture_state
            SET last_seen_cycle_id=?, last_seen_at=?
            WHERE user_id=? AND picture_id=?
            """,
            (cycle_id, now_ts, user_id, picture_id),
        )

    if first_time_this_cycle:
        con.execute("UPDATE user_cycle_state SET seen_count = seen_count + 1 WHERE user_id=?", (user_id,))
        seen_count, total_snapshot, completed_at = con.execute(
            "SELECT seen_count, total_pictures_snapshot, completed_at FROM user_cycle_state WHERE user_id=?",
            (user_id,),
        ).fetchone()
        if completed_at is None and int(seen_count) >= int(total_snapshot):
            con.execute("UPDATE user_cycle_state SET completed_at=? WHERE user_id=?", (now_ts, user_id))


def _pick_user_review_fallback(con: sqlite3.Connection, user_id: int, day_key: str, cursor: int) -> Optional[str]:
    lst = _get_user_top_mistakes(con, user_id, USER_TOP_MISTAKES_LIMIT)
    if not lst:
        return None
    seed = _daily_seed(f"{day_key}:{user_id}")
    idx = (seed + cursor) % len(lst)
    return lst[idx]


def _review_is_eligible(con: sqlite3.Connection, user_id: int, pid: str) -> bool:
    # REVIEW is "repeat only": require user attempted before
    row = con.execute(
        "SELECT attempts FROM user_picture_state WHERE user_id=? AND picture_id=?",
        (user_id, pid),
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def peek_next_candidate(con: sqlite3.Connection, user_id: int) -> Optional[Dict[str, Any]]:
    day_key = _today_key()
    now_ts = int(time.time())

    items = ensure_global_daily_plan(con, day_key)
    cursor = _ensure_user_day_progress(con, user_id, day_key)

    cycle_id, _, _, _ = _get_or_advance_cycle(con, user_id, now_ts)

    scan = 0
    i = cursor
    while scan < MAX_SCAN_SLOTS_PER_PLAY and i < len(items):
        slot = items[i]
        kind = slot.get("kind")
        pid = slot.get("picture_id")

        if kind == "NEW":
            if pid and pid in PAINTINGS_BY_ID:
                row = con.execute(
                    "SELECT last_seen_cycle_id FROM user_picture_state WHERE user_id=? AND picture_id=?",
                    (user_id, pid),
                ).fetchone()
                last_seen_cycle_id = row[0] if row else None
                if last_seen_cycle_id != cycle_id:
                    return {
                        "day_key": day_key,
                        "slot_index": i,
                        "next_cursor": i + 1,
                        "cycle_id": cycle_id,
                        "kind": "NEW",
                        "picture_id": pid,
                        "painting": PAINTINGS_BY_ID[pid],
                    }

        elif kind == "REVIEW":
            chosen_pid = pid or _pick_user_review_fallback(con, user_id, day_key, i)
            if chosen_pid and chosen_pid in PAINTINGS_BY_ID and _review_is_eligible(con, user_id, chosen_pid):
                return {
                    "day_key": day_key,
                    "slot_index": i,
                    "next_cursor": i + 1,
                    "cycle_id": cycle_id,
                    "kind": "REVIEW",
                    "picture_id": chosen_pid,
                    "painting": PAINTINGS_BY_ID[chosen_pid],
                }

        i += 1
        scan += 1

    # Persist cursor forward to avoid rescanning dead zones
    with con:
        _set_user_day_cursor(con, user_id, day_key, i)
    return None


def commit_candidate(con: sqlite3.Connection, user_id: int, cand: Dict[str, Any]) -> None:
    now_ts = int(time.time())
    with con:
        _set_user_day_cursor(con, user_id, cand["day_key"], cand["next_cursor"])
        _bump_cycle_seen_if_first_time_this_cycle(con, user_id, cand["picture_id"], cand["cycle_id"], now_ts)
        inc_used_today_tx(con, user_id, 1)
        con.execute(
            """
            UPDATE sessions SET
                pending_day_key=NULL,
                pending_slot_index=NULL,
                pending_next_cursor=NULL,
                pending_cycle_id=NULL,
                pending_kind=NULL
            WHERE user_id=?
            """,
            (user_id,),
        )


def skip_candidate_slot(con: sqlite3.Connection, user_id: int, cand: Dict[str, Any]) -> None:
    # On send failure: advance cursor past the slot so we don't retry a broken image endlessly.
    with con:
        _set_user_day_cursor(con, user_id, cand["day_key"], cand["next_cursor"])


def save_session_pending(con: sqlite3.Connection, user_id: int, q: Dict[str, Any], cand: Dict[str, Any]) -> None:
    con.execute(
        """
        INSERT INTO sessions(
            user_id, q_picture_id, q_title, q_artist, q_year, q_museum, q_image_url, q_note, ts,
            pending_day_key, pending_slot_index, pending_next_cursor, pending_cycle_id, pending_kind
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
            q_picture_id=excluded.q_picture_id,
            q_title=excluded.q_title,
            q_artist=excluded.q_artist,
            q_year=excluded.q_year,
            q_museum=excluded.q_museum,
            q_image_url=excluded.q_image_url,
            q_note=excluded.q_note,
            ts=excluded.ts,
            pending_day_key=excluded.pending_day_key,
            pending_slot_index=excluded.pending_slot_index,
            pending_next_cursor=excluded.pending_next_cursor,
            pending_cycle_id=excluded.pending_cycle_id,
            pending_kind=excluded.pending_kind
        """,
        (
            user_id,
            q.get("id"),
            q.get("title"),
            q.get("artist"),
            q.get("year"),
            q.get("museum"),
            q.get("image_url"),
            q.get("note"),
            int(time.time()),
            cand["day_key"],
            cand["slot_index"],
            cand["next_cursor"],
            cand["cycle_id"],
            cand["kind"],
        ),
    )


def _update_picture_answer_aggregates(con: sqlite3.Connection, user_id: int, picture_id: str, is_correct: bool, now_ts: int) -> None:
    with con:
        con.execute(
            """
            INSERT INTO global_picture_state(picture_id, attempts, wrong, correct, updated_at)
            VALUES(?,?,?,?,?)
            ON CONFLICT(picture_id) DO UPDATE SET
                attempts = global_picture_state.attempts + 1,
                wrong = global_picture_state.wrong + ?,
                correct = global_picture_state.correct + ?,
                updated_at = excluded.updated_at
            """,
            (
                picture_id,
                1,
                0 if is_correct else 1,
                1 if is_correct else 0,
                now_ts,
                0 if is_correct else 1,
                1 if is_correct else 0,
            ),
        )

        row = con.execute(
            "SELECT attempts FROM user_picture_state WHERE user_id=? AND picture_id=?",
            (user_id, picture_id),
        ).fetchone()
        if row is None:
            con.execute(
                """
                INSERT INTO user_picture_state(user_id, picture_id, last_seen_cycle_id, last_seen_at, last_wrong_at, attempts, wrong, correct)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    user_id,
                    picture_id,
                    None,
                    None,
                    now_ts if not is_correct else None,
                    1,
                    0 if is_correct else 1,
                    1 if is_correct else 0,
                ),
            )
        else:
            con.execute(
                """
                UPDATE user_picture_state
                SET attempts = attempts + 1,
                    wrong = wrong + ?,
                    correct = correct + ?,
                    last_wrong_at = CASE WHEN ?=1 THEN last_wrong_at ELSE ? END
                WHERE user_id=? AND picture_id=?
                """,
                (
                    0 if is_correct else 1,
                    1 if is_correct else 0,
                    1 if is_correct else 0,
                    now_ts,
                    user_id,
                    picture_id,
                ),
            )


# -------------------- Backfill from painting_results (one-time) --------------------

def backfill_picture_states_if_needed() -> None:
    con = connect_db()
    try:
        done = con.execute("SELECT value FROM meta WHERE key='backfill_v2'").fetchone()
        if done and done[0] == "1":
            return

        total_rows = con.execute("SELECT COUNT(*) FROM painting_results").fetchone()[0]
        if int(total_rows) == 0:
            with con:
                con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('backfill_v2','1')")
            return

        now_ts = int(time.time())

        global_agg: Dict[str, Dict[str, int]] = {}
        user_agg: Dict[Tuple[int, str], Dict[str, int]] = {}
        user_last_seen: Dict[Tuple[int, str], int] = {}
        user_last_wrong: Dict[Tuple[int, str], int] = {}
        user_seen_set: Dict[int, set] = {}

        updates: List[Tuple[str, int]] = []

        rows = con.execute(
            "SELECT id, user_id, title, artist, year, museum, image_url, is_correct, ts, picture_id FROM painting_results"
        ).fetchall()

        for rid, user_id, title, artist, year, museum, image_url, is_correct, ts, pid in rows:
            pid2 = resolve_picture_id(pid, title, artist, year, museum, image_url)
            if not pid2:
                continue

            if not pid:
                updates.append((pid2, rid))

            # global
            g = global_agg.setdefault(pid2, {"attempts": 0, "wrong": 0, "correct": 0})
            g["attempts"] += 1
            if int(is_correct) == 1:
                g["correct"] += 1
            else:
                g["wrong"] += 1

            # user
            uid = int(user_id)
            key = (uid, pid2)
            u = user_agg.setdefault(key, {"attempts": 0, "wrong": 0, "correct": 0})
            u["attempts"] += 1
            if int(is_correct) == 1:
                u["correct"] += 1
            else:
                u["wrong"] += 1
                user_last_wrong[key] = max(user_last_wrong.get(key, 0), int(ts))

            user_last_seen[key] = max(user_last_seen.get(key, 0), int(ts))
            user_seen_set.setdefault(uid, set()).add(pid2)

        with con:
            if updates:
                con.executemany("UPDATE painting_results SET picture_id=? WHERE id=?", updates)

            for pid2, g in global_agg.items():
                con.execute(
                    """
                    INSERT INTO global_picture_state(picture_id, attempts, wrong, correct, updated_at)
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(picture_id) DO UPDATE SET
                        attempts=excluded.attempts,
                        wrong=excluded.wrong,
                        correct=excluded.correct,
                        updated_at=excluded.updated_at
                    """,
                    (pid2, g["attempts"], g["wrong"], g["correct"], now_ts),
                )

            for (uid, pid2), u in user_agg.items():
                con.execute(
                    """
                    INSERT INTO user_picture_state(user_id, picture_id, last_seen_cycle_id, last_seen_at, last_wrong_at, attempts, wrong, correct)
                    VALUES(?,?,?,?,?,?,?,?)
                    ON CONFLICT(user_id, picture_id) DO UPDATE SET
                        last_seen_cycle_id=excluded.last_seen_cycle_id,
                        last_seen_at=excluded.last_seen_at,
                        last_wrong_at=excluded.last_wrong_at,
                        attempts=excluded.attempts,
                        wrong=excluded.wrong,
                        correct=excluded.correct
                    """,
                    (
                        uid,
                        pid2,
                        1,  # mark as already seen in cycle 1
                        user_last_seen.get((uid, pid2)),
                        user_last_wrong.get((uid, pid2)),
                        u["attempts"],
                        u["wrong"],
                        u["correct"],
                    ),
                )

            current_total = len(ALL_PICTURE_IDS)
            for uid, seen_set in user_seen_set.items():
                seen_count = len(seen_set)
                completed_at = now_ts if seen_count >= current_total else None
                con.execute(
                    """
                    INSERT INTO user_cycle_state(user_id, cycle_id, started_at, completed_at, total_pictures_snapshot, seen_count)
                    VALUES(?,?,?,?,?,?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        cycle_id=1,
                        started_at=excluded.started_at,
                        completed_at=excluded.completed_at,
                        total_pictures_snapshot=excluded.total_pictures_snapshot,
                        seen_count=excluded.seen_count
                    """,
                    (uid, 1, now_ts, completed_at, current_total, seen_count),
                )

            con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('backfill_v2','1')")
    finally:
        con.close()


# -------------------- Telegram UI --------------------

def answer_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("1) Русский музей", callback_data="ans:Русский музей"),
            InlineKeyboardButton("2) Третьяковская галерея", callback_data="ans:Третьяковская галерея"),
        ]
    ])


# -------------------- Handlers --------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user(update)
    text = (
        "Привет! Это викторина «Третьяковка vs Русский музей».

"
        "Нажми /play чтобы начать: я покажу картину, а ты угадай, из какого музея она.
"
        "Команды: /play, /stats, /top"
    )
    await update.effective_message.reply_text(text)


async def play(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user(update)
    user_id = update.effective_user.id

    con = connect_db()
    try:
        used = get_used_today(con, user_id)
        if used >= DAILY_LIMIT:
            _enqueue_tomorrow_stats(user_id)
            await update.effective_message.reply_text("На сегодня всё. Приходите завтра!")
            return

        # Try a few times in case some image URLs are bad.
        for _attempt in range(3):
            cand = peek_next_candidate(con, user_id)
            if not cand:
                _enqueue_tomorrow_stats(user_id)
                await update.effective_message.reply_text("На сегодня всё. Приходите завтра!")
                return

            q = cand["painting"]
            caption = (
                f"🖼 <b>{q['title']}</b>
{q['artist']}, {q['year']}

"
                "<i>Из какого музея эта работа?</i>"
            )

            # Save session (with pending metadata) before sending
            with con:
                save_session_pending(con, user_id, q, cand)

            try:
                await update.effective_message.reply_photo(
                    photo=q["image_url"],
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=answer_keyboard(),
                )
                # Commit only after successful send
                commit_candidate(con, user_id, cand)
                return
            except BadRequest:
                skip_candidate_slot(con, user_id, cand)
            except Exception:
                skip_candidate_slot(con, user_id, cand)

        await update.effective_message.reply_text("Не удалось показать картину. Попробуйте позже.")
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

    con = connect_db()
    try:
        row = con.execute(
            """
            SELECT q_picture_id, q_title, q_artist, q_year, q_museum, q_image_url, q_note
            FROM sessions WHERE user_id=?
            """,
            (user_id,),
        ).fetchone()

        if not row:
            await query.message.reply_text("Не нашёл активный вопрос. Нажми /play чтобы продолжить.")
            return

        q_picture_id, q_title, q_artist, q_year, q_museum, q_image_url, q_note = row
        resolved_pid = resolve_picture_id(q_picture_id, q_title, q_artist, q_year, q_museum, q_image_url)

        is_correct = (chosen == q_museum)
        now_ts = int(time.time())

        with con:
            update_stats(con, user_id, is_correct)

            con.execute(
                """
                INSERT INTO painting_results(user_id, picture_id, title, artist, year, museum, image_url, is_correct, ts)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (user_id, resolved_pid, q_title, q_artist, q_year, q_museum, q_image_url, 1 if is_correct else 0, now_ts),
            )

        if resolved_pid:
            _update_picture_answer_aggregates(con, user_id, resolved_pid, is_correct, now_ts)

        result = "✅ Верно!" if is_correct else f"❌ Неверно. Правильно: {q_museum}"
        extra = f"

<b>{q_title}</b>
<i>{q_artist}</i>, {q_year}

{q_note}"

        try:
            await query.edit_message_caption(
                caption=result + extra,
                parse_mode=ParseMode.HTML,
                reply_markup=None,
            )
        except Exception:
            # Message might be uneditable; ignore.
            pass

        # Auto-advance to next question
        try:
            await play(update, context)
        except Exception:
            pass
    finally:
        con.close()


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user(update)
    con = connect_db()
    try:
        row = con.execute("SELECT correct, total FROM stats WHERE user_id=?", (update.effective_user.id,)).fetchone()
        if not row:
            await update.effective_message.reply_text("Статистика пока пустая. Нажми /play, чтобы начать.")
            return
        correct, total = row
        acc = (correct / total * 100) if total else 0.0
        await update.effective_message.reply_text(f"Твоя статистика:
Правильных ответов: {correct}/{total} ({acc:.1f}%)")
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
    await update.effective_message.reply_text("
".join(lines))


# -------------------- Scheduled stats sending --------------------

async def _prepare_hardest_picture_stat(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    hardest = hardest_paintings_window(days=DIFFICULT_WINDOW_DAYS, limit=3, min_attempts=2)
    if not hardest:
        return

    media = []
    for idx, (title, artist, year, museum, image_url, wrong, total, pct) in enumerate(hardest, 1):
        cap = (
            f"🔥 Сложная картина #{idx} за последние {DIFFICULT_WINDOW_DAYS} дн.
"
            f"<b>{title}</b>
<i>{artist}</i>, {year}
"
            f"Ошибок: {wrong}/{total} ({pct:.1f}%)"
        )
        media.append(InputMediaPhoto(media=image_url, caption=cap, parse_mode=ParseMode.HTML))

    try:
        await context.bot.send_media_group(chat_id=user_id, media=media)
    except Exception:
        for (title, artist, year, museum, image_url, wrong, total, pct) in hardest:
            cap = (
                f"🔥 Сложная картина за последние {DIFFICULT_WINDOW_DAYS} дн.
"
                f"<b>{title}</b>
<i>{artist}</i>, {year}
"
                f"Ошибок: {wrong}/{total} ({pct:.1f}%)"
            )
            try:
                await context.bot.send_photo(chat_id=user_id, photo=image_url, caption=cap, parse_mode=ParseMode.HTML)
            except Exception:
                pass


async def _send_due_stats_job(context: ContextTypes.DEFAULT_TYPE):
    now_ts = int(time.time())
    con = connect_db()
    try:
        rows = con.execute(
            """
            SELECT id, user_id, payload FROM stats_queue
            WHERE sent_at IS NULL AND send_at <= ?
            ORDER BY send_at ASC
            LIMIT 50
            """,
            (now_ts,),
        ).fetchall()

        for q_id, user_id, payload in rows:
            try:
                try:
                    await _prepare_hardest_picture_stat(context, user_id)
                except Exception:
                    pass
                await context.bot.send_message(chat_id=user_id, text=payload)
                with con:
                    con.execute("UPDATE stats_queue SET sent_at=? WHERE id=?", (now_ts, q_id))
            except Exception:
                # keep unsent, retry later
                pass
    finally:
        con.close()


# -------------------- App bootstrap --------------------

def main():
    global PAINTINGS, PAINTINGS_BY_ID, ALL_PICTURE_IDS

    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN в переменных окружения.")

    PAINTINGS, PAINTINGS_BY_ID, ALL_PICTURE_IDS = load_paintings()
    build_catalog_indexes()

    db_init()
    backfill_picture_states_if_needed()

    app: Application = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("play", play))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("top", top))
    app.add_handler(CallbackQueryHandler(on_callback))

    app.job_queue.run_repeating(_send_due_stats_job, interval=60, first=10)

    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
