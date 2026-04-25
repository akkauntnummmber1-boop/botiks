import random
import sqlite3
import logging
import time
import uuid
import html
import os
from pathlib import Path
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.error import BadRequest
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, Defaults, ConversationHandler, MessageHandler, filters
BOT_TOKEN = '8659612914:AAEVU_gNd4ZCjeVdLlRXjGYuZrrPRLTopz8'
ADMIN_IDS = {5037478748, 6991875}
DB_DIR = 'data'
DB_PATH = os.path.join(DB_DIR, 'bot.db')
os.makedirs(DB_DIR, exist_ok=True)
TRIGGERS = {'кто я', 'кто', 'я'}
ROLE_COOLDOWN_SECONDS = 10 * 60

CASINO_COOLDOWN_SECONDS = 30
MIN_SLOT_BET_MILLI = 100       # 0.1 USDT
MAX_SLOT_BET_MILLI = 10000     # 10 USDT
SLOT_WIN_CHANCE_PERCENT = 12  # шанс выигрыша в слотах: 10–15%

MIN_COIN_BET_MILLI = 100       # 0.1 USDT
MAX_COIN_BET_MILLI = 10000     # 10 USDT

SLOT_SYMBOLS = ['🍒', '🍋', '💎', '⭐️', '7️⃣']
SLOT_PAY_TABLE = {
    ('7️⃣', '7️⃣', '7️⃣'): 20,
    ('💎', '💎', '💎'): 10,
    ('⭐️', '⭐️', '⭐️'): 5,
    ('🍒', '🍒', '🍒'): 3,
}

BONUS_AMOUNT_MILLI = 100
MIN_WITHDRAW_MILLI = 100000
DAY_SECONDS = 24 * 60 * 60
DAILY_ROLE_BONUS_LIMIT = 5
RARITY_CHANCES = [
    ('common', 69),
    ('rare', 20),
    ('epic', 8),
    ('legendary', 2),
    ('secret', 1),
]

RARITY_LABELS = {
    'common': 'Обычная',
    'rare': 'Редкая',
    'epic': 'Эпическая',
    'legendary': 'Легендарная',
    'secret': 'Секретная',
}

ROLE_REWARDS_MILLI = {
    'common': 100,       # 0.1 USDT
    'rare': 200,         # 0.2 USDT
    'epic': 300,         # 0.3 USDT
    'legendary': 500,    # 0.5 USDT
    'secret': 40000,     # 40 USDT
}

RARITY_ALIASES = {
    'обычная': 'common',
    'обычный': 'common',
    'common': 'common',
    'редкая': 'rare',
    'редкий': 'rare',
    'rare': 'rare',
    'эпическая': 'epic',
    'эпический': 'epic',
    'epic': 'epic',
    'легендарная': 'legendary',
    'легендарный': 'legendary',
    'legendary': 'legendary',
    'секретная': 'secret',
    'секретный': 'secret',
    'секрет': 'secret',
    'secret': 'secret',
}
DAILY_BONUS_CHANCES = [
    (5000, 1),   # 5 USDT — 1%
    (4000, 5),   # 4 USDT — 5%
    (3000, 6),   # 3 USDT — 6%
    (2000, 7),   # 2 USDT — 7%
    (1000, 81),  # 1 USDT — 70% + оставшиеся 11%, чтобы бонус всегда выпадал
]
WAIT_PHRASE = 1
WAIT_WALLET = 2
WAIT_AMOUNT = 3
WAIT_GIVE_USER = 4
WAIT_GIVE_AMOUNT = 5
WAIT_TAKE_USER = 6
WAIT_TAKE_AMOUNT = 7
WAIT_UID_USER = 8
WAIT_UID_VALUE = 9
WAIT_HIDE_USER = 10
WAIT_SEARCH_USER = 11
WAIT_UNHIDE_USER = 12
WAIT_DELETE_PHRASE = 13
WAIT_BROADCAST_TEXT = 14
WAIT_PROMO_CODE = 15
WAIT_PROMO_AMOUNT = 16
WAIT_PROMO_LIMIT = 17
WAIT_PROMO_ACTIVATE = 18
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
PE_USER = '<tg-emoji emoji-id="5258011929993026890">👤</tg-emoji>'
PE_OK = '<tg-emoji emoji-id="5260726538302660868">✅</tg-emoji>'
PE_USERS = '<tg-emoji emoji-id="5258513401784573443">👥</tg-emoji>'
PE_ANNOUNCE = '<tg-emoji emoji-id="5260268501515377807">📣</tg-emoji>'
PE_INFO = '<tg-emoji emoji-id="5258503720928288433">ℹ️</tg-emoji>'
PE_STOP = '<tg-emoji emoji-id="5258362429389152256">✋</tg-emoji>'
PE_WALLET = '<tg-emoji emoji-id="5258204546391351475">💰</tg-emoji>'
PE_PLUS = '<tg-emoji emoji-id="5274008024585871702">➕</tg-emoji>'
PE_CHART = '<tg-emoji emoji-id="5258391025281408576">📈</tg-emoji>'
PE_CHAT = '<tg-emoji emoji-id="5260348422266822411">💬</tg-emoji>'
PE_WARN = '<tg-emoji emoji-id="5258474669769497337">❗️</tg-emoji>'
PE_HOME = '<tg-emoji emoji-id="5257963315258204021">🏘</tg-emoji>'
PE_STAR = '<tg-emoji emoji-id="5258185631355378853">⭐️</tg-emoji>'
PE_EYE = '<tg-emoji emoji-id="5253959125838090076">👁</tg-emoji>'
PE_UID = '<tg-emoji emoji-id="5359629206948976159">🔖</tg-emoji>'
PE_TROPHY = '<tg-emoji emoji-id="5409008750893734809">🏆</tg-emoji>'
PE_TOP1 = '<tg-emoji emoji-id="5280735858926822987">🥇</tg-emoji>'
PE_TOP2 = '<tg-emoji emoji-id="5283195573812340110">🥈</tg-emoji>'
PE_TOP3 = '<tg-emoji emoji-id="5282750778409233531">🥉</tg-emoji>'
PE_SEARCH = '<tg-emoji emoji-id="5429571366384842791">🔎</tg-emoji>'
PE_CROSS = '<tg-emoji emoji-id="5260342697075416641">❌</tg-emoji>'
PE_TIMER = '<tg-emoji emoji-id="5258258882022612173">⏲</tg-emoji>'
PE_MASKS = '<tg-emoji emoji-id="5258430848218176413">🎭</tg-emoji>'
PE_CASINO = '<tg-emoji emoji-id="5453884647966524953">🎰</tg-emoji>'
PE_DICE = '<tg-emoji emoji-id="5260547274957672345">🎲</tg-emoji>'
PE_COIN = '<tg-emoji emoji-id="5379600444098093058">🪙</tg-emoji>'
PE_DOLLAR = '<tg-emoji emoji-id="5945214041747100767">💲</tg-emoji>'
PE_X2 = '<tg-emoji emoji-id="5785038454828043276">✖️</tg-emoji>'
PE_PLUS_ONE = '<tg-emoji emoji-id="5784967785436154901">➕</tg-emoji>'
PE_LOADING = '<tg-emoji emoji-id="5787344001862471785">✍️</tg-emoji>'
PE_FLYING_MONEY = '<tg-emoji emoji-id="5472030678633684592">💸</tg-emoji>'
PE_SLOT_CHERRY = '<tg-emoji emoji-id="5406759193052995173">🍒</tg-emoji>'
PE_SLOT_STAR = '<tg-emoji emoji-id="5435957248314579621">⭐️</tg-emoji>'
PE_SLOT_DIAMOND = '<tg-emoji emoji-id="5471952986970267163">💎</tg-emoji>'
PE_SLOT_SEVEN = '<tg-emoji emoji-id="5382132232829804982">7️⃣</tg-emoji>'
PE_RARITY_COMMON = '<tg-emoji emoji-id="5433713454319938373">🩶</tg-emoji>'
PE_RARITY_RARE = '<tg-emoji emoji-id="5449380056201697322">💚</tg-emoji>'
PE_RARITY_EPIC = '<tg-emoji emoji-id="5434031913260035048">🩷</tg-emoji>'
PE_RARITY_LEGENDARY = '<tg-emoji emoji-id="5449366943666543715">💛</tg-emoji>'
PE_RARITY_SECRET = '<tg-emoji emoji-id="5449692618151695997">🖤</tg-emoji>'
PE_NUM_1 = '<tg-emoji emoji-id="5382322671679708881">1️⃣</tg-emoji>'
PE_NUM_2 = '<tg-emoji emoji-id="5381990043642502553">2️⃣</tg-emoji>'
PE_NUM_3 = '<tg-emoji emoji-id="5381879959335738545">3️⃣</tg-emoji>'
PE_NUM_4 = '<tg-emoji emoji-id="5382054253403577563">4️⃣</tg-emoji>'
PE_NUM_5 = '<tg-emoji emoji-id="5391197405553107640">5️⃣</tg-emoji>'
PE_NUM_6 = '<tg-emoji emoji-id="5390966190283694453">6️⃣</tg-emoji>'
PE_NUM_7 = '<tg-emoji emoji-id="5382132232829804982">7️⃣</tg-emoji>'
PE_NUM_8 = '<tg-emoji emoji-id="5391038994274329680">8️⃣</tg-emoji>'
PE_NUM_9 = '<tg-emoji emoji-id="5391234698754138414">9️⃣</tg-emoji>'
PE_NUM_0 = '<tg-emoji emoji-id="5393480373944459905">0️⃣</tg-emoji>'

def pe(text: str) -> str:
    """Заменяет обычные emoji на premium emoji в HTML-тексте сообщения."""
    if text is None:
        return text
    text = str(text)
    replacements = [('ℹ️', PE_INFO), ('❗️', PE_WARN), ('⚠️', PE_WARN), ('⭐️', PE_STAR), ('👤', PE_USER), ('✅', PE_OK), ('👥', PE_USERS), ('📣', PE_ANNOUNCE), ('✋', PE_STOP), ('⛔', PE_STOP), ('🚫', PE_STOP), ('💰', PE_WALLET), ('💸', PE_FLYING_MONEY), ('➕', PE_PLUS), ('📈', PE_CHART), ('📊', PE_CHART), ('💬', PE_CHAT), ('❗', PE_WARN), ('❌', PE_CROSS), ('🏘', PE_HOME), ('🏠', PE_HOME), ('⭐', PE_STAR), ('👁', PE_EYE), ('🔖', PE_UID), ('🆔', PE_UID), ('🏆', PE_TROPHY), ('🥇', PE_TOP1), ('🥈', PE_TOP2), ('🥉', PE_TOP3), ('🔎', PE_SEARCH), ('⏲', PE_TIMER), ('⏳', PE_TIMER), ('1️⃣', PE_NUM_1), ('2️⃣', PE_NUM_2), ('3️⃣', PE_NUM_3), ('4️⃣', PE_NUM_4), ('5️⃣', PE_NUM_5), ('6️⃣', PE_NUM_6), ('7️⃣', PE_NUM_7), ('8️⃣', PE_NUM_8), ('9️⃣', PE_NUM_9), ('0️⃣', PE_NUM_0), ('🩶', PE_RARITY_COMMON), ('💚', PE_RARITY_RARE), ('🩷', PE_RARITY_EPIC), ('💛', PE_RARITY_LEGENDARY), ('🖤', PE_RARITY_SECRET), ('⭐️', PE_SLOT_STAR), ('🍒', PE_SLOT_CHERRY), ('💎', PE_SLOT_DIAMOND), ('🎭', PE_MASKS), ('🎰', PE_CASINO), ('🎲', PE_DICE), ('🪙', PE_COIN), ('💲', PE_DOLLAR), ('✖️', PE_X2), ('✖', PE_X2), ('✍️', PE_LOADING), ('✍', PE_LOADING), ('⚙', PE_INFO), ('🔢', PE_INFO), ('📋', PE_CHAT), ('📄', PE_CHAT), ('📛', PE_USER), ('🗄', PE_INFO), ('🗑', PE_CROSS), ('🙈', PE_EYE), ('➖', PE_CROSS), ('⬅', PE_HOME), ('🎁', PE_STAR)]
    placeholders = []
    for index, (old, new) in enumerate(replacements):
        placeholder = f'__PE_{index}__'
        placeholders.append((placeholder, new))
        text = text.replace(old, placeholder)
    for placeholder, new in placeholders:
        text = text.replace(placeholder, new)
    return text

def ts() -> int:
    return int(time.time())

def day_start() -> int:
    now = ts()
    return now - now % DAY_SECONDS

def seconds_until_next_day() -> int:
    return day_start() + DAY_SECONDS - ts()

def format_time_left(seconds: int) -> str:
    hours = seconds // 3600
    minutes = seconds % 3600 // 60
    return f'{hours} ч. {minutes} мин.'

def is_admin(user_id: int | None) -> bool:
    return user_id in ADMIN_IDS

def is_group(chat) -> bool:
    return chat and chat.type in ('group', 'supergroup')

def money(milli: int) -> str:
    whole = milli // 1000
    frac = milli % 1000
    if frac == 0:
        return f'{whole} USDT'
    return f'{whole}.{frac:03d}'.rstrip('0') + ' USDT'

def parse_money(text: str) -> int | None:
    try:
        value = float(text.strip().replace(',', '.'))
    except ValueError:
        return None
    return int(round(value * 1000))

def normalize_rarity(value: str) -> str | None:
    value = (value or '').strip().lower()
    return RARITY_ALIASES.get(value)

def parse_phrase_input(text: str) -> tuple[str, str]:
    text = (text or '').strip()
    if '|' in text:
        left, right = text.split('|', 1)
        rarity = normalize_rarity(left)
        phrase = right.strip()
        if rarity and phrase:
            return phrase, rarity
    return text, 'common'

def roll_weighted(items):
    total = sum(weight for _, weight in items)
    number = random.randint(1, total)
    current = 0
    for value, weight in items:
        current += weight
        if number <= current:
            return value
    return items[-1][0]

def roll_role_rarity() -> str:
    return roll_weighted(RARITY_CHANCES)

def roll_daily_bonus_amount() -> int:
    return roll_weighted(DAILY_BONUS_CHANCES)

def mention(user) -> str:
    name = user.full_name or user.username or str(user.id)
    return f'<a href="tg://user?id={user.id}">{html.escape(name)}</a>'

def db():
    os.makedirs(DB_DIR, exist_ok=True)
    return sqlite3.connect(DB_PATH)

def columns(conn, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f'PRAGMA table_info({table})').fetchall()}

def init_db():
    os.makedirs(DB_DIR, exist_ok=True)
    conn = db()
    cur = conn.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
    cur.execute('\n        CREATE TABLE IF NOT EXISTS phrases (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            text TEXT NOT NULL UNIQUE,\n            created_at INTEGER NOT NULL\n        )\n        ')
    cur.execute('\n        CREATE TABLE IF NOT EXISTS users (\n            user_id INTEGER PRIMARY KEY,\n            username TEXT,\n            first_name TEXT,\n            uid TEXT UNIQUE,\n            balance_milli INTEGER NOT NULL DEFAULT 0,\n            openings INTEGER NOT NULL DEFAULT 0,\n            last_role_at INTEGER NOT NULL DEFAULT 0,\n            hidden INTEGER NOT NULL DEFAULT 0,\n            casino_last_spin_at INTEGER NOT NULL DEFAULT 0,\n            created_at INTEGER NOT NULL\n        )\n        ')
    cur.execute('\n        CREATE TABLE IF NOT EXISTS bonus_claims (\n            bonus_id TEXT PRIMARY KEY,\n            user_id INTEGER NOT NULL,\n            amount_milli INTEGER NOT NULL,\n            claimed INTEGER NOT NULL DEFAULT 0,\n            created_at INTEGER NOT NULL,\n            claimed_at INTEGER\n        )\n        ')
    cur.execute('\n        CREATE TABLE IF NOT EXISTS daily_bonuses (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            user_id INTEGER NOT NULL,\n            amount_milli INTEGER NOT NULL,\n            claimed_at INTEGER NOT NULL\n        )\n        ')
    cur.execute('\n        CREATE TABLE IF NOT EXISTS groups (\n            chat_id INTEGER PRIMARY KEY,\n            title TEXT,\n            username TEXT,\n            type TEXT,\n            added_at INTEGER NOT NULL,\n            last_seen_at INTEGER NOT NULL\n        )\n        ')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_roles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            phrase TEXT NOT NULL,
            rarity TEXT NOT NULL,
            received_at INTEGER NOT NULL
        )
        """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            amount_milli INTEGER NOT NULL,
            max_uses INTEGER NOT NULL,
            used_count INTEGER NOT NULL DEFAULT 0,
            created_by INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        )
        """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS promo_activations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            activated_at INTEGER NOT NULL,
            UNIQUE(code, user_id)
        )
        """)

    cur.execute("\n        CREATE TABLE IF NOT EXISTS withdrawals (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            user_id INTEGER NOT NULL,\n            wallet TEXT NOT NULL,\n            amount_milli INTEGER NOT NULL,\n            status TEXT NOT NULL DEFAULT 'pending',\n            created_at INTEGER NOT NULL,\n            reviewed_by INTEGER,\n            reviewed_at INTEGER\n        )\n        ")
    phrase_cols = columns(conn, 'phrases')
    if 'rarity' not in phrase_cols:
        cur.execute("ALTER TABLE phrases ADD COLUMN rarity TEXT NOT NULL DEFAULT 'common'")
    user_cols = columns(conn, 'users')
    if 'hidden' not in user_cols:
        cur.execute('ALTER TABLE users ADD COLUMN hidden INTEGER NOT NULL DEFAULT 0')

    if 'casino_last_spin_at' not in user_cols:
        cur.execute('ALTER TABLE users ADD COLUMN casino_last_spin_at INTEGER NOT NULL DEFAULT 0')

    cur.execute("INSERT OR IGNORE INTO meta (key, value) VALUES ('next_uid', '1')")
    conn.commit()
    conn.close()
    logger.info('База данных создана/открыта: %s', os.path.abspath(DB_PATH))

def next_uid(conn) -> str:
    row = conn.execute("SELECT value FROM meta WHERE key='next_uid'").fetchone()
    current = int(row[0]) if row else 1
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('next_uid', ?)", (str(current + 1),))
    return str(current)

def register_user(user):
    if not user:
        return
    with db() as conn:
        row = conn.execute('SELECT user_id FROM users WHERE user_id=?', (user.id,)).fetchone()
        if row:
            conn.execute('UPDATE users SET username=?, first_name=? WHERE user_id=?', (user.username, user.first_name, user.id))
        else:
            conn.execute('\n                INSERT INTO users\n                (user_id, username, first_name, uid, balance_milli, openings, last_role_at, hidden, created_at)\n                VALUES (?, ?, ?, ?, 0, 0, 0, 0, ?)\n                ', (user.id, user.username, user.first_name, next_uid(conn), ts()))
        conn.commit()

def remember_group(chat):
    if not is_group(chat):
        return
    with db() as conn:
        row = conn.execute('SELECT chat_id FROM groups WHERE chat_id=?', (chat.id,)).fetchone()
        if row:
            conn.execute('UPDATE groups SET title=?, username=?, type=?, last_seen_at=? WHERE chat_id=?', (chat.title, chat.username, chat.type, ts(), chat.id))
        else:
            conn.execute('INSERT INTO groups (chat_id, title, username, type, added_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?)', (chat.id, chat.title, chat.username, chat.type, ts(), ts()))
        conn.commit()

def get_user(user_id: int):
    with db() as conn:
        return conn.execute('\n            SELECT user_id, username, first_name, uid, balance_milli, openings, last_role_at, hidden, casino_last_spin_at\n            FROM users WHERE user_id=?\n            ', (user_id,)).fetchone()

def add_phrase_db(text: str) -> bool:
    phrase, rarity = parse_phrase_input(text)
    if not phrase:
        return False
    with db() as conn:
        try:
            conn.execute('INSERT INTO phrases (text, rarity, created_at) VALUES (?, ?, ?)', (phrase, rarity, ts()))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

def random_phrase() -> tuple[str, str] | None:
    rarity = roll_role_rarity()
    with db() as conn:
        rows = conn.execute('SELECT text, rarity FROM phrases WHERE rarity=?', (rarity,)).fetchall()
        if not rows:
            rows = conn.execute('SELECT text, rarity FROM phrases').fetchall()
    if not rows:
        return None
    phrase_text, rarity = random.choice(rows)
    return phrase_text, rarity or 'common'

def last_phrases(limit=10):
    with db() as conn:
        return conn.execute('SELECT id, text, rarity FROM phrases ORDER BY id DESC LIMIT ?', (limit,)).fetchall()

def phrase_count() -> int:
    with db() as conn:
        return int(conn.execute('SELECT COUNT(*) FROM phrases').fetchone()[0])

def delete_phrase_db(pid: int) -> bool:
    with db() as conn:
        cur = conn.execute('DELETE FROM phrases WHERE id=?', (pid,))
        conn.commit()
        return cur.rowcount > 0

def get_all_users(include_hidden: bool=True):
    with db() as conn:
        if include_hidden:
            return conn.execute('\n                SELECT user_id, username, first_name, uid, balance_milli, openings, hidden\n                FROM users\n                ORDER BY user_id ASC\n                ').fetchall()
        return conn.execute('\n            SELECT user_id, username, first_name, uid, balance_milli, openings, hidden\n            FROM users\n            WHERE hidden=0\n            ORDER BY user_id ASC\n            ').fetchall()

def registered_users_count() -> int:
    with db() as conn:
        return int(conn.execute('SELECT COUNT(*) FROM users').fetchone()[0])

def admin_stats_text() -> str:
    rows = get_all_users(include_hidden=True)
    if not rows:
        return '📊 <b>Статистика</b>\n\nЗарегистрировано: <b>0</b>'
    lines = ['📊 <b>Статистика</b>', '', f'👥 Зарегистрировано: <b>{len(rows)}</b>', '', '<b>Пользователи:</b>']
    for user_id, username, first_name, uid, balance, openings, hidden in rows:
        username_text = f'@{username}' if username else 'нет username'
        name_text = first_name or 'без имени'
        hidden_text = ' | скрыт' if hidden else ''
        lines.append(f'• {html.escape(username_text)} | {html.escape(name_text)}\n  ID: <code>{user_id}</code> | UID: <code>{html.escape(str(uid))}</code>{hidden_text}')
    return '\n'.join(lines)

def add_balance(user_id: int, amount: int):
    with db() as conn:
        conn.execute('UPDATE users SET balance_milli=balance_milli+? WHERE user_id=?', (amount, user_id))
        conn.commit()

def take_balance(user_id: int, amount: int) -> tuple[bool, str]:
    with db() as conn:
        row = conn.execute('SELECT balance_milli FROM users WHERE user_id=?', (user_id,)).fetchone()
        if not row:
            return (False, 'Пользователь не найден.')
        bal = int(row[0])
        if amount > bal:
            return (False, f'У пользователя только {money(bal)}.')
        conn.execute('UPDATE users SET balance_milli=balance_milli-? WHERE user_id=?', (amount, user_id))
        conn.commit()
    return (True, 'Готово.')

def set_uid(user_id: int, uid: str) -> tuple[bool, str]:
    uid = uid.strip()
    if not uid:
        return (False, 'UID пустой.')
    with db() as conn:
        if not conn.execute('SELECT user_id FROM users WHERE user_id=?', (user_id,)).fetchone():
            return (False, 'Пользователь не найден. Он должен сначала вызвать бота или написать /start.')
        try:
            conn.execute('UPDATE users SET uid=? WHERE user_id=?', (uid, user_id))
            conn.commit()
            return (True, 'UID изменен.')
        except sqlite3.IntegrityError:
            return (False, 'Такой UID уже занят.')



def parse_duration_to_until(value: str) -> tuple[bool, int, str]:
    """
    Возвращает: ok, banned_until, readable.
    banned_until = 0 значит навсегда.
    Форматы:
    30m, 1h, 7d, 1w, perm, forever, навсегда
    """
    raw = (value or "").strip().lower()

    if raw in ("perm", "permanent", "forever", "навсегда", "0"):
        return True, 0, "навсегда"

    match = re.fullmatch(r"(\d+)(m|h|d|w)", raw)

    if not match:
        return False, 0, ""

    number = int(match.group(1))
    unit = match.group(2)

    if number <= 0:
        return False, 0, ""

    seconds_by_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60,
    }

    labels = {
        "m": "мин.",
        "h": "ч.",
        "d": "дн.",
        "w": "нед.",
    }

    until = ts() + number * seconds_by_unit[unit]
    return True, until, f"{number} {labels[unit]}"


def ban_time_text(banned_until: int) -> str:
    if not banned_until:
        return "навсегда"

    left = banned_until - ts()

    if left <= 0:
        return "истек"

    days = left // 86400
    hours = (left % 86400) // 3600
    minutes = (left % 3600) // 60

    if days > 0:
        return f"{days} дн. {hours} ч."
    if hours > 0:
        return f"{hours} ч. {minutes} мин."

    return f"{minutes} мин."


def clear_expired_ban(user_id: int) -> bool:
    row = get_user(user_id)

    # В старой структуре бана еще нет.
    if not row or len(row) < 14:
        return False

    banned = int(row[8] or 0)
    banned_until = int(row[10] or 0)

    if banned and banned_until and banned_until <= ts():
        with db() as conn:
            conn.execute(
                """
                UPDATE users
                SET banned=0, ban_reason=NULL, banned_until=0, banned_by=NULL, banned_at=0
                WHERE user_id=?
                """,
                (user_id,),
            )
            conn.commit()
        return True

    return False



def get_user_ban_status_direct(user_id: int) -> tuple[bool, str, int]:
    """
    Надежная проверка бана напрямую из SQLite.
    Не использует get_user(), потому что у старых версий tuple может иметь другую структуру.
    Возвращает: banned, reason, banned_until.
    """
    try:
        with db() as conn:
            user_cols = columns(conn, "users")

            if "banned" not in user_cols:
                return False, "", 0

            select_fields = ["banned"]

            if "ban_reason" in user_cols:
                select_fields.append("ban_reason")
            else:
                select_fields.append("NULL")

            if "banned_until" in user_cols:
                select_fields.append("banned_until")
            else:
                select_fields.append("0")

            row = conn.execute(
                f"SELECT {', '.join(select_fields)} FROM users WHERE user_id=?",
                (user_id,),
            ).fetchone()

            if not row:
                return False, "", 0

            banned = bool(int(row[0] or 0))
            reason = row[1] or "не указана"
            banned_until = int(row[2] or 0)

            # Если бан временный и истек — автоматически снимаем.
            if banned and banned_until and banned_until <= ts():
                reset_fields = ["banned=0"]

                if "ban_reason" in user_cols:
                    reset_fields.append("ban_reason=NULL")
                if "banned_until" in user_cols:
                    reset_fields.append("banned_until=0")
                if "banned_by" in user_cols:
                    reset_fields.append("banned_by=NULL")
                if "banned_at" in user_cols:
                    reset_fields.append("banned_at=0")

                conn.execute(
                    f"UPDATE users SET {', '.join(reset_fields)} WHERE user_id=?",
                    (user_id,),
                )
                conn.commit()

                return False, "", 0

            return banned, reason, banned_until

    except Exception:
        # При любой проблеме с миграцией/колонками не баним пользователя ложно.
        return False, "", 0


def is_banned_user(user_id: int) -> bool:
    banned, reason, banned_until = get_user_ban_status_direct(user_id)
    return banned


def get_ban_info(user_id: int) -> tuple[bool, str, int]:
    return get_user_ban_status_direct(user_id)


def set_ban_user(
    user_id: int,
    banned: int,
    reason: str | None = None,
    banned_until: int = 0,
    admin_id: int | None = None
) -> tuple[bool, str]:
    with db() as conn:
        row = conn.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,)).fetchone()

        if not row:
            return False, "Пользователь не найден."

        if banned:
            conn.execute(
                """
                UPDATE users
                SET banned=1, ban_reason=?, banned_until=?, banned_by=?, banned_at=?
                WHERE user_id=?
                """,
                (reason or "не указана", banned_until, admin_id, ts(), user_id),
            )
            conn.commit()

            return True, (
                "Пользователь забанен.\n"
                f"Время: <b>{html.escape(ban_time_text(banned_until))}</b>\n"
                f"Причина: <b>{html.escape(reason or 'не указана')}</b>"
            )

        conn.execute(
            """
            UPDATE users
            SET banned=0, ban_reason=NULL, banned_until=0, banned_by=NULL, banned_at=0
            WHERE user_id=?
            """,
            (user_id,),
        )
        conn.commit()

    return True, "Пользователь разбанен."


def hide_user(user_id: int) -> tuple[bool, str]:
    with db() as conn:
        if not conn.execute('SELECT user_id FROM users WHERE user_id=?', (user_id,)).fetchone():
            return (False, 'Пользователь не найден.')
        conn.execute('UPDATE users SET hidden=1 WHERE user_id=?', (user_id,))
        conn.commit()
    return (True, 'Пользователь скрыт.')

def unhide_user(user_id: int) -> tuple[bool, str]:
    with db() as conn:
        if not conn.execute('SELECT user_id FROM users WHERE user_id=?', (user_id,)).fetchone():
            return (False, 'Пользователь не найден.')
        conn.execute('UPDATE users SET hidden=0 WHERE user_id=?', (user_id,))
        conn.commit()
    return (True, 'Пользователь раскрыт.')

def search_user_text(user_id: int) -> str | None:
    row = get_user(user_id)

    if not row:
        return None

    user_id = row[0]
    username = row[1]
    first_name = row[2]
    uid = row[3]
    balance = row[4]
    openings = row[5]
    hidden = row[7] if len(row) > 7 else 0

    # Если человек скрыт, поиск делает вид, что его нет в боте.
    if hidden:
        return None

    banned = int(row[8] or 0) if len(row) >= 14 else 0
    ban_reason = row[9] if len(row) >= 14 else None
    banned_until = int(row[10] or 0) if len(row) >= 14 else 0

    username_text = f"@{username}" if username else "нет"
    first_name_text = first_name or "нет"
    ban_status = "забанен" if banned else "не забанен"

    text = (
        "🔎 <b>Пользователь найден</b>\n\n"
        f"🆔 Telegram ID: <code>{user_id}</code>\n"
        f"🔖 UID: <code>{html.escape(str(uid))}</code>\n"
        f"💰 Баланс: <b>{money(balance)}</b>\n"
        f"👁 Открытия: <b>{openings}</b>\n"
        f"📛 Username: {html.escape(username_text)}\n"
        f"👤 Имя: {html.escape(first_name_text)}\n"
        f"🚫 Статус бана: <b>{ban_status}</b>"
    )

    if banned:
        text += (
            f"\nПричина бана: <b>{html.escape(ban_reason or 'не указана')}</b>"
            f"\nОсталось: <b>{html.escape(ban_time_text(banned_until))}</b>"
        )

    return text



def inc_opening(user_id: int):
    with db() as conn:
        conn.execute('UPDATE users SET openings=openings+1, last_role_at=? WHERE user_id=?', (ts(), user_id))
        conn.commit()

def create_bonus(user_id: int) -> str:
    bonus_id = uuid.uuid4().hex[:16]
    with db() as conn:
        conn.execute('INSERT INTO bonus_claims (bonus_id, user_id, amount_milli, claimed, created_at) VALUES (?, ?, ?, 0, ?)', (bonus_id, user_id, BONUS_AMOUNT_MILLI, ts()))
        conn.commit()
    return bonus_id

def claimed_role_bonuses_today(conn, user_id: int) -> int:
    row = conn.execute(
        'SELECT COUNT(*) FROM bonus_claims WHERE user_id=? AND claimed=1 AND claimed_at>=?',
        (user_id, day_start()),
    ).fetchone()
    return int(row[0]) if row else 0

def claim_bonus(bonus_id: str, user_id: int) -> str:
    with db() as conn:
        row = conn.execute('SELECT user_id, amount_milli, claimed FROM bonus_claims WHERE bonus_id=?', (bonus_id,)).fetchone()
        if not row:
            return 'Бонус не найден.'
        owner, amount, claimed = row
        if int(owner) != int(user_id):
            return 'Этот бонус не для вас.'
        if claimed:
            return 'Вы уже получили этот бонус.'
        conn.execute('UPDATE bonus_claims SET claimed=1, claimed_at=? WHERE bonus_id=?', (ts(), bonus_id))
        conn.execute('UPDATE users SET balance_milli=balance_milli+? WHERE user_id=?', (amount, user_id))
        conn.commit()
    return f'Вы получили {money(amount)}'

def claim_daily_bonus(user_id: int) -> str:
    amount = roll_daily_bonus_amount()
    with db() as conn:
        row = conn.execute(
            'SELECT amount_milli, claimed_at FROM daily_bonuses WHERE user_id=? AND claimed_at>=? ORDER BY claimed_at DESC LIMIT 1',
            (user_id, day_start()),
        ).fetchone()
        if row:
            return f'Ежедневный бонус уже получен. Следующий через {format_time_left(seconds_until_next_day())}.'
        conn.execute('INSERT INTO daily_bonuses (user_id, amount_milli, claimed_at) VALUES (?, ?, ?)', (user_id, amount, ts()))
        conn.execute('UPDATE users SET balance_milli=balance_milli+? WHERE user_id=?', (amount, user_id))
        conn.commit()
    return f'Ежедневный бонус: {money(amount)}'

def top_text() -> str:
    with db() as conn:
        rows = conn.execute('\n            SELECT user_id, username, first_name, uid, balance_milli\n            FROM users\n            WHERE hidden=0\n            ORDER BY balance_milli DESC\n            LIMIT 3\n            ').fetchall()
    if not rows:
        return 'Топ пока пуст.'
    medals = ['🥇', '🥈', '🥉']
    lines = ['🏆 <b>Топ 3 по USDT</b>\n']
    for i, (user_id, username, first_name, uid, balance) in enumerate(rows):
        name = f'@{username}' if username else first_name or f'ID {user_id}'
        lines.append(f'{medals[i]} {html.escape(name)} | UID: <code>{html.escape(str(uid))}</code> | <b>{money(balance)}</b>')
    return '\n'.join(lines)

def profile_text(user_id: int) -> str:
    row = get_user(user_id)

    if not row:
        return "Профиль не найден. Напиши /start."

    user_id = row[0]
    username = row[1]
    first_name = row[2]
    uid = row[3]
    balance = row[4]
    openings = row[5]
    hidden = row[7] if len(row) > 7 else 0

    # Новые ban-поля есть только если len(row) >= 14.
    banned = int(row[8] or 0) if len(row) >= 14 else 0
    ban_reason = row[9] if len(row) >= 14 else None
    banned_until = int(row[10] or 0) if len(row) >= 14 else 0

    uname = f"@{username}" if username else "нет"
    hidden_line = "\n🙈 Статус: <b>скрыт</b>" if hidden else ""

    ban_line = ""
    if banned:
        ban_line = (
            f"\n🚫 Бан: <b>да</b>"
            f"\nПричина: <b>{html.escape(ban_reason or 'не указана')}</b>"
            f"\nОсталось: <b>{html.escape(ban_time_text(banned_until))}</b>"
        )

    return (
        "👤 <b>Профиль</b>\n\n"
        f"🆔 Telegram ID: <code>{user_id}</code>\n"
        f"🔖 UID: <code>{html.escape(str(uid))}</code>\n"
        f"👁 Открытия: <b>{openings}</b>\n"
        f"💰 Баланс: <b>{money(balance)}</b>\n"
        f"📛 Username: {html.escape(uname)}"
        f"{hidden_line}"
        f"{ban_line}"
    )



def groups_text() -> str:
    with db() as conn:
        rows = conn.execute('SELECT chat_id, title, username, type FROM groups ORDER BY last_seen_at DESC').fetchall()
    if not rows:
        return 'Бот пока не найден ни в одной группе.'
    lines = ['👥 <b>Группы с ботом</b>\n']
    for chat_id, title, username, typ in rows[:50]:
        title = title or 'Без названия'
        uname = f'@{username}' if username else 'нет username'
        lines.append(f'• <b>{html.escape(title)}</b>\n  ID: <code>{chat_id}</code>\n  Username: {html.escape(uname)}')
    return '\n\n'.join(lines)


def rarity_icon(rarity: str) -> str:
    return {
        "common": "🩶",
        "rare": "💚",
        "epic": "🩷",
        "legendary": "💛",
        "secret": "🖤",
    }.get(rarity, "🩶")


def create_promo_code(code: str, amount_milli: int, max_uses: int, admin_id: int) -> tuple[bool, str]:
    code = (code or "").strip().upper()

    if not code:
        return False, "Промокод пустой."
    if amount_milli <= 0:
        return False, "Сумма должна быть больше 0."
    if max_uses <= 0:
        return False, "Лимит активаций должен быть больше 0."

    with db() as conn:
        try:
            conn.execute(
                """
                INSERT INTO promo_codes
                (code, amount_milli, max_uses, used_count, created_by, created_at, active)
                VALUES (?, ?, ?, 0, ?, ?, 1)
                """,
                (code, amount_milli, max_uses, admin_id, ts()),
            )
            conn.commit()
            return True, f"Промокод <code>{html.escape(code)}</code> создан: <b>{money(amount_milli)}</b>, активаций: <b>{max_uses}</b>."
        except sqlite3.IntegrityError:
            return False, "Такой промокод уже существует."


def activate_promo_code(user_id: int, code: str) -> tuple[bool, str]:
    if is_banned_user(user_id):
        return False, 'Вы забанены у бота.'

    code = (code or "").strip().upper()

    if not code:
        return False, "Введите промокод."

    with db() as conn:
        promo = conn.execute(
            """
            SELECT code, amount_milli, max_uses, used_count, active
            FROM promo_codes
            WHERE code=?
            """,
            (code,),
        ).fetchone()

        if not promo:
            return False, "Промокод не найден."

        code, amount_milli, max_uses, used_count, active = promo

        if not active:
            return False, "Промокод отключен."
        if used_count >= max_uses:
            return False, "Лимит активаций промокода исчерпан."

        already = conn.execute(
            "SELECT id FROM promo_activations WHERE code=? AND user_id=?",
            (code, user_id),
        ).fetchone()

        if already:
            return False, "Вы уже активировали этот промокод."

        conn.execute(
            "INSERT INTO promo_activations (code, user_id, activated_at) VALUES (?, ?, ?)",
            (code, user_id, ts()),
        )
        conn.execute("UPDATE promo_codes SET used_count=used_count+1 WHERE code=?", (code,))
        conn.execute("UPDATE users SET balance_milli=balance_milli+? WHERE user_id=?", (amount_milli, user_id))
        conn.commit()

    return True, f"Промокод активирован. Начислено: <b>+{money(amount_milli)}</b>."


def promo_codes_text() -> str:
    with db() as conn:
        rows = conn.execute(
            """
            SELECT code, amount_milli, max_uses, used_count, active
            FROM promo_codes
            ORDER BY created_at DESC
            LIMIT 20
            """
        ).fetchall()

    if not rows:
        return "Промокодов пока нет."

    lines = ["🎁 <b>Последние промокоды</b>", ""]

    for code, amount_milli, max_uses, used_count, active in rows:
        status = "активен" if active else "отключен"
        lines.append(f"<code>{html.escape(code)}</code> — <b>{money(amount_milli)}</b> | {used_count}/{max_uses} | {status}")

    return "\n".join(lines)


def create_withdrawal(user_id: int, wallet: str, amount: int) -> int:
    with db() as conn:
        cur = conn.execute("INSERT INTO withdrawals (user_id, wallet, amount_milli, status, created_at) VALUES (?, ?, ?, 'pending', ?)", (user_id, wallet, amount, ts()))
        conn.commit()
        return cur.lastrowid

def get_withdrawal(wid: int):
    with db() as conn:
        return conn.execute('SELECT id, user_id, wallet, amount_milli, status FROM withdrawals WHERE id=?', (wid,)).fetchone()

def set_withdrawal(wid: int, status: str, admin_id: int) -> bool:
    with db() as conn:
        row = conn.execute('SELECT status FROM withdrawals WHERE id=?', (wid,)).fetchone()
        if not row or row[0] != 'pending':
            return False
        conn.execute('UPDATE withdrawals SET status=?, reviewed_by=?, reviewed_at=? WHERE id=?', (status, admin_id, ts(), wid))
        conn.commit()
    return True

def main_menu(admin=False, group=False):
    buttons = [[InlineKeyboardButton('🎭 Кто я', callback_data='whoami')]]

    if not group:
        buttons.append([
            InlineKeyboardButton('👤 Профиль', callback_data='profile'),
            InlineKeyboardButton('💸 Вывод USDT', callback_data='withdraw')
        ])
        buttons.append([InlineKeyboardButton('🔎 Поиск по ID', callback_data='search_user')])
        buttons.append([InlineKeyboardButton('🎁 Промокод', callback_data='promo_activate')])

    buttons.append([InlineKeyboardButton('🎰 Казино', callback_data='casino')])
    buttons.append([InlineKeyboardButton('🏆 Топ 3', callback_data='top3')])

    return InlineKeyboardMarkup(buttons)


def reply_main_menu(admin=False, group=False):
    if group:
        rows = []
    else:
        rows = [
            ['🎭 Кто я'],
            ['👤 Профиль', '💸 Вывод USDT'],
            ['🔎 Поиск по ID', '🎰 Казино'],
            ['🎁 Промокод'],
            ['🏆 Топ 3'],
        ]

    return ReplyKeyboardMarkup(
        rows,
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder='Выберите действие...'
    )


def role_menu(group=False):
    buttons = []

    if not group:
        buttons.append([
            InlineKeyboardButton('👤 Профиль', callback_data='profile'),
            InlineKeyboardButton('💸 Вывод USDT', callback_data='withdraw')
        ])
        buttons.append([InlineKeyboardButton('🔎 Поиск по ID', callback_data='search_user')])
        buttons.append([InlineKeyboardButton('🎁 Промокод', callback_data='promo_activate')])
        buttons.append([InlineKeyboardButton('🎰 Казино', callback_data='casino')])

    return InlineKeyboardMarkup(buttons) if buttons else None


def admin_panel_text() -> str:
    return (
        "<b>Админ-панель</b>\n\n"
        "1️⃣ <code>/add текст</code> — добавить фразу\n"
        "2️⃣ <code>/list</code> — последние фразы\n"
        "3️⃣ <code>/delete ID</code> — удалить фразу\n"
        "4️⃣ <code>/give USER_ID SUM причина</code> — выдать USDT\n"
        "5️⃣ <code>/take USER_ID SUM</code> — забрать USDT\n"
        "6️⃣ <code>/setuid USER_ID UID</code> — выдать кастом UID\n"
        "7️⃣ <code>/hide USER_ID</code> — скрыть пользователя\n"
        "8️⃣ <code>/unhide USER_ID</code> — раскрыть пользователя\n"
        "9️⃣ <code>/ban USER_ID TIME причина</code> — забанить пользователя\n"
        "1️⃣0️⃣ <code>/unban USER_ID</code> — разбанить пользователя\n\n"
        "<b>Дополнительно:</b>\n"
        "<code>/promo_create CODE SUM LIMIT</code> — создать промокод\n"
        "<code>/promos</code> — список промокодов\n"
        "<code>/adminstats</code> — статистика\n"
        "<code>/groups</code> — группы с ботом\n"
        "<code>/broadcast текст</code> — уведомление всем\n"
    )


def admin_menu():
    return InlineKeyboardMarkup([[InlineKeyboardButton('➕ Добавить фразу', callback_data='add_phrase')], [InlineKeyboardButton('🗑 Удалить фразу', callback_data='delete_phrase_btn')], [InlineKeyboardButton('📋 Последние фразы', callback_data='last_phrases')], [InlineKeyboardButton('🔢 Количество фраз', callback_data='phrase_count')], [InlineKeyboardButton('📣 Уведомление в бот', callback_data='broadcast')], [InlineKeyboardButton('📊 Статистика', callback_data='admin_stats')], [InlineKeyboardButton('🎁 Создать промокод', callback_data='promo_create')], [InlineKeyboardButton('📋 Промокоды', callback_data='promo_list')], [InlineKeyboardButton('💰 Выдать USDT', callback_data='give_usdt')], [InlineKeyboardButton('➖ Забрать USDT', callback_data='take_usdt')], [InlineKeyboardButton('🆔 Выдать кастом UID', callback_data='custom_uid')], [InlineKeyboardButton('🙈 Скрыть пользователя', callback_data='hide_user')], [InlineKeyboardButton('👁 Раскрыть пользователя', callback_data='unhide_user')], [InlineKeyboardButton('👥 Группы с ботом', callback_data='groups')], [InlineKeyboardButton('⬅️ Назад', callback_data='back')]])

def withdraw_admin_menu(wid: int):
    return InlineKeyboardMarkup([[InlineKeyboardButton('✅ Одобрить', callback_data=f'wd_ok:{wid}'), InlineKeyboardButton('❌ Отклонить', callback_data=f'wd_no:{wid}')]])

async def delete_last_private(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    mid = context.user_data.get('last_private_result')
    if not mid:
        return
    try:
        await context.bot.delete_message(chat_id, mid)
    except BadRequest:
        pass
    except Exception:
        pass
    context.user_data['last_private_result'] = None

async def delete_last_group_clean_result(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    last_id = context.chat_data.get("last_clean_result_message_id")

    if not last_id:
        return

    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=last_id)
    except BadRequest:
        pass
    except Exception:
        pass

    context.chat_data["last_clean_result_message_id"] = None


async def send_clean_group_result(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    """
    Сообщения больше не удаляются ни в ЛС, ни в группах.
    Функция оставлена для совместимости с кодом казино и топа.
    """
    return await send_result(update, context, text, reply_markup=reply_markup)



async def send_result(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    chat = update.effective_chat

    # В ЛС сообщения больше не удаляются.
    msg = await context.bot.send_message(
        chat.id,
        pe(text),
        parse_mode='HTML',
        reply_markup=reply_markup
    )

    return msg

async def send_long_message(bot, chat_id: int, text: str, reply_markup=None):
    max_len = 3900
    if len(text) <= max_len:
        await bot.send_message(chat_id, pe(text), parse_mode='HTML', reply_markup=reply_markup)
        return
    parts = []
    current = ''
    for line in text.split('\n'):
        if len(current) + len(line) + 1 > max_len:
            parts.append(current)
            current = line
        else:
            current = line if not current else current + '\n' + line
    if current:
        parts.append(current)
    for index, part in enumerate(parts):
        await bot.send_message(chat_id, pe(part), parse_mode='HTML', reply_markup=reply_markup if index == len(parts) - 1 else None)


def casino_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🎰 Слоты 0.1 USDT', callback_data='slots_bet:0.1')],
        [InlineKeyboardButton('🎰 Слоты 0.5 USDT', callback_data='slots_bet:0.5')],
        [InlineKeyboardButton('🎰 Слоты 1 USDT', callback_data='slots_bet:1')],
        [InlineKeyboardButton('🎰 Слоты 5 USDT', callback_data='slots_bet:5')],
        [InlineKeyboardButton('🪙 Орел 1 USDT', callback_data='coin_bet:orel:1')],
        [InlineKeyboardButton('🪙 Решка 1 USDT', callback_data='coin_bet:reshka:1')],
    ])


def slots_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔄 Еще раз 0.1', callback_data='slots_bet:0.1')],
        [InlineKeyboardButton('🔄 Еще раз 0.5', callback_data='slots_bet:0.5')],
        [InlineKeyboardButton('🔄 Еще раз 1', callback_data='slots_bet:1')],
        [InlineKeyboardButton('🔄 Еще раз 5', callback_data='slots_bet:5')],
        [InlineKeyboardButton('🎰 Казино', callback_data='casino')],
    ])


def coin_menu():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton('🪙 Орел 0.5', callback_data='coin_bet:orel:0.5'),
            InlineKeyboardButton('🪙 Решка 0.5', callback_data='coin_bet:reshka:0.5'),
        ],
        [
            InlineKeyboardButton('🪙 Орел 1', callback_data='coin_bet:orel:1'),
            InlineKeyboardButton('🪙 Решка 1', callback_data='coin_bet:reshka:1'),
        ],
        [
            InlineKeyboardButton('🪙 Орел 5', callback_data='coin_bet:orel:5'),
            InlineKeyboardButton('🪙 Решка 5', callback_data='coin_bet:reshka:5'),
        ],
        [InlineKeyboardButton('🎰 Казино', callback_data='casino')],
    ])


def get_casino_last_spin(user_id: int) -> int:
    row = get_user(user_id)

    if not row:
        return 0

    # Новая структура: casino_last_spin_at — индекс 13.
    if len(row) >= 14:
        return int(row[13] or 0)

    # Старая структура: casino_last_spin_at — последний индекс, обычно 8.
    if len(row) >= 9:
        return int(row[8] or 0)

    return 0



def set_casino_last_spin(user_id: int) -> None:
    with db() as conn:
        conn.execute("UPDATE users SET casino_last_spin_at=? WHERE user_id=?", (ts(), user_id))
        conn.commit()


def roll_slots() -> list[str]:
    """
    Шанс выигрышной комбинации примерно 10–15%.
    Проигрышная комбинация специально делается без совпадений,
    чтобы не было случайного x0.5.
    """
    win_roll = random.randint(1, 100) <= SLOT_WIN_CHANCE_PERCENT

    if win_roll:
        number = random.randint(1, 100)

        if number <= 2:
            return ['7️⃣', '7️⃣', '7️⃣']
        if number <= 6:
            return ['💎', '💎', '💎']
        if number <= 14:
            return ['⭐️', '⭐️', '⭐️']
        if number <= 30:
            return ['🍒', '🍒', '🍒']

        # Любые 3 одинаковых = x2.
        symbol = random.choice(['🍋'])
        return [symbol, symbol, symbol]

    # Проигрыш: 3 разных символа.
    return random.sample(SLOT_SYMBOLS, 3)



def get_slot_multiplier(symbols: list[str]) -> float:
    combo = tuple(symbols)

    if combo in SLOT_PAY_TABLE:
        return SLOT_PAY_TABLE[combo]

    if symbols[0] == symbols[1] == symbols[2]:
        return 2

    # Две одинаковые картинки больше не дают x0.5.
    return 0



def slot_result_text(user, bet_milli: int, symbols: list[str], multiplier: float, win_milli: int, balance_after: int) -> str:
    symbols_line = " | ".join(symbols)

    if multiplier > 0:
        result_line = f"✅ Выигрыш: +{money(win_milli)}"

        if multiplier == 20:
            result_line = f"🎰 ДЖЕКПОТ: +{money(win_milli)}"
    else:
        result_line = f"❌ Проигрыш: -{money(bet_milli)}"

    return (
        f"🎰 <b>Слоты</b>\n\n"
        f"👤 Игрок: {mention(user)}\n"
        f"💲 Ставка: <b>{money(bet_milli)}</b>\n\n"
        f"{symbols_line}\n\n"
        f"✖️ Множитель: <b>x{multiplier}</b>\n"
        f"{result_line}\n"
        f"💰 Баланс: <b>{money(balance_after)}</b>"
    )


async def show_casino(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    text = (
        "🎰 <b>Казино</b>\n\n"
        "Выбери ставку для слотов или монетки.\n"
        "Команды:\n"
        "<code>/slots 1</code>\n"
        "<code>/coin орел 1</code>\n"
        "<code>/coin решка 1</code>\n\n"
        f"⏲ Кулдаун между играми: <b>{CASINO_COOLDOWN_SECONDS} сек.</b>\n"
        f"💲 Минимальная ставка: <b>{money(MIN_SLOT_BET_MILLI)}</b>\n"
        f"💲 Максимальная ставка: <b>{money(MAX_SLOT_BET_MILLI)}</b>\n"
        f"📈 Шанс выигрыша: <b>{SLOT_WIN_CHANCE_PERCENT}%</b>\n\n"
        "Выплаты слотов:\n"
        "7️⃣ 7️⃣ 7️⃣ = x20\n"
        "💎 💎 💎 = x10\n"
        "⭐️ ⭐️ ⭐️ = x5\n"
        "🍒 🍒 🍒 = x3"
    )

    await send_clean_group_result(update, context, text, reply_markup=casino_menu())


async def play_slots(update: Update, context: ContextTypes.DEFAULT_TYPE, bet_milli: int):
    user = update.effective_user
    register_user(user)
    remember_group(update.effective_chat)

    banned, reason, banned_until = get_user_ban_status_direct(user.id)
    if banned:
        await send_clean_group_result(
            update,
            context,
            '⛔ Вы забанены у бота.\n'
            f'Причина: <b>{html.escape(reason or "не указана")}</b>\n'
            f'Осталось: <b>{html.escape(ban_time_text(banned_until))}</b>'
        )
        return

    row = get_user(user.id)

    if not row:
        await send_clean_group_result(update, context, "❌ Профиль не найден. Напиши /start.")
        return

    balance_milli = int(row[4])

    if bet_milli < MIN_SLOT_BET_MILLI:
        await send_clean_group_result(update, context, f"❗️ Минимальная ставка: <b>{money(MIN_SLOT_BET_MILLI)}</b>")
        return

    if bet_milli > MAX_SLOT_BET_MILLI:
        await send_clean_group_result(update, context, f"❗️ Максимальная ставка: <b>{money(MAX_SLOT_BET_MILLI)}</b>")
        return

    if balance_milli < bet_milli:
        await send_clean_group_result(update, context, f"❌ Недостаточно средств.\nВаш баланс: <b>{money(balance_milli)}</b>")
        return

    last_spin = get_casino_last_spin(user.id)
    left = CASINO_COOLDOWN_SECONDS - (ts() - last_spin)

    if left > 0:
        await send_clean_group_result(update, context, f"⏲ Подождите еще <b>{left} сек.</b> перед следующим спином.")
        return

    ok, msg = take_balance(user.id, bet_milli)

    if not ok:
        await send_clean_group_result(update, context, f"❌ {html.escape(msg)}")
        return

    symbols = roll_slots()
    multiplier = get_slot_multiplier(symbols)
    win_milli = int(round(bet_milli * multiplier))

    if win_milli > 0:
        add_balance(user.id, win_milli)

    set_casino_last_spin(user.id)

    updated = get_user(user.id)
    balance_after = int(updated[4]) if updated else 0

    await send_clean_group_result(
        update,
        context,
        slot_result_text(user, bet_milli, symbols, multiplier, win_milli, balance_after),
        reply_markup=slots_menu()
    )




def normalize_coin_side(text: str) -> str | None:
    value = (text or "").strip().lower()

    if value in ("орел", "орёл", "orel", "heads", "o"):
        return "orel"

    if value in ("решка", "reshka", "tails", "r"):
        return "reshka"

    return None


def coin_side_label(side: str) -> str:
    return "Орел" if side == "orel" else "Решка"


def roll_coin() -> str:
    return random.choice(["orel", "reshka"])


def coin_result_text(user, bet_milli: int, choice: str, result: str, win_milli: int, balance_after: int) -> str:
    win = choice == result

    if win:
        result_line = f"✅ Выигрыш: +{money(win_milli)}"
    else:
        result_line = f"❌ Проигрыш: -{money(bet_milli)}"

    return (
        f"🪙 <b>Орел и решка</b>\n\n"
        f"👤 Игрок: {mention(user)}\n"
        f"💲 Ставка: <b>{money(bet_milli)}</b>\n"
        f"✍️ Выбор: <b>{coin_side_label(choice)}</b>\n"
        f"🪙 Выпало: <b>{coin_side_label(result)}</b>\n\n"
        f"{result_line}\n"
        f"💰 Баланс: <b>{money(balance_after)}</b>"
    )


async def play_coin(update: Update, context: ContextTypes.DEFAULT_TYPE, side: str, bet_milli: int):
    user = update.effective_user
    register_user(user)
    remember_group(update.effective_chat)

    banned, reason, banned_until = get_user_ban_status_direct(user.id)
    if banned:
        await send_clean_group_result(
            update,
            context,
            '⛔ Вы забанены у бота.\n'
            f'Причина: <b>{html.escape(reason or "не указана")}</b>\n'
            f'Осталось: <b>{html.escape(ban_time_text(banned_until))}</b>'
        )
        return

    row = get_user(user.id)

    if not row:
        await send_clean_group_result(update, context, "❌ Профиль не найден. Напиши /start.")
        return

    balance_milli = int(row[4])

    if bet_milli < MIN_COIN_BET_MILLI:
        await send_clean_group_result(update, context, f"❗️ Минимальная ставка: <b>{money(MIN_COIN_BET_MILLI)}</b>")
        return

    if bet_milli > MAX_COIN_BET_MILLI:
        await send_clean_group_result(update, context, f"❗️ Максимальная ставка: <b>{money(MAX_COIN_BET_MILLI)}</b>")
        return

    if balance_milli < bet_milli:
        await send_clean_group_result(update, context, f"❌ Недостаточно средств.\nВаш баланс: <b>{money(balance_milli)}</b>")
        return

    last_spin = get_casino_last_spin(user.id)
    left = CASINO_COOLDOWN_SECONDS - (ts() - last_spin)

    if left > 0:
        await send_clean_group_result(update, context, f"⏲ Подождите еще <b>{left} сек.</b> перед следующей игрой.")
        return

    ok, msg = take_balance(user.id, bet_milli)

    if not ok:
        await send_clean_group_result(update, context, f"❌ {html.escape(msg)}")
        return

    result = roll_coin()
    win_milli = bet_milli * 2 if side == result else 0

    if win_milli > 0:
        add_balance(user.id, win_milli)

    set_casino_last_spin(user.id)

    updated = get_user(user.id)
    balance_after = int(updated[4]) if updated else 0

    await send_clean_group_result(
        update,
        context,
        coin_result_text(user, bet_milli, side, result, win_milli, balance_after),
        reply_markup=coin_menu()
    )



async def send_role(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    register_user(user)
    remember_group(chat)

    banned, reason, banned_until = get_user_ban_status_direct(user.id)
    if banned:
        await send_result(
            update,
            context,
            '⛔ Вы забанены у бота.\n'
            f'Причина: <b>{html.escape(reason or "не указана")}</b>\n'
            f'Осталось: <b>{html.escape(ban_time_text(banned_until))}</b>'
        )
        return

    row = get_user(user.id)
    if not row:
        await send_result(update, context, 'Ошибка профиля. Напиши /start.')
        return
    last_role = row[6]
    left = ROLE_COOLDOWN_SECONDS - (ts() - last_role)
    if left > 0:
        await send_result(update, context, f'⏳ {mention(user)}, подожди еще {left // 60} мин. {left % 60} сек.')
        return
    phrase_data = random_phrase()
    if not phrase_data:
        await send_result(update, context, 'В базе пока нет фраз.')
        return
    phrase, rarity = phrase_data
    rarity_label = RARITY_LABELS.get(rarity, rarity)
    reward_milli = ROLE_REWARDS_MILLI.get(rarity, 100)

    inc_opening(user.id)
    add_balance(user.id, reward_milli)

    await send_result(
        update,
        context,
        f'{mention(user)} — <b>{html.escape(phrase)}</b>\n'
        f'⭐ Редкость: <b>{html.escape(rarity_label)}</b>\n'
        f'💰 Добавлено: <b>+{money(reward_milli)}</b>',
        reply_markup=role_menu(group=is_group(chat))
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if is_group(update.effective_chat):
        await update.message.reply_text(
            pe('🎭 Бот для игры «Кто я?»'),
            parse_mode='HTML',
            reply_markup=ReplyKeyboardRemove()
        )

        await update.message.reply_text(
            pe('Главное меню:'),
            parse_mode='HTML',
            reply_markup=main_menu(is_admin(update.effective_user.id), group=True)
        )
        return

    await update.message.reply_text(
        pe('🎭 Бот для игры «Кто я?»\n\nГлавное меню открыто снизу.'),
        parse_mode='HTML',
        reply_markup=reply_main_menu(is_admin(update.effective_user.id), group=False)
    )

async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if is_group(update.effective_chat):
        await update.message.reply_text(
            pe('Главное меню:'),
            parse_mode='HTML',
            reply_markup=main_menu(is_admin(update.effective_user.id), group=True)
        )
        return

    await update.message.reply_text(
        pe('Главное меню открыто снизу.'),
        parse_mode='HTML',
        reply_markup=reply_main_menu(is_admin(update.effective_user.id), group=False)
    )


async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_role(update, context)

async def trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    register_user(update.effective_user)
    remember_group(update.effective_chat)

    raw_text = update.message.text.strip()
    lower_text = raw_text.lower()

    if context.user_data.get('waiting_promo_activate'):
        context.user_data['waiting_promo_activate'] = False
        ok, msg = activate_promo_code(update.effective_user.id, raw_text)
        await update.message.reply_text(
            pe(('✅ ' if ok else '❌ ') + msg),
            parse_mode='HTML'
        )
        return

    if lower_text in TRIGGERS or lower_text in ('кто я?', '🎭 кто я'):
        await send_role(update, context)
        return

    if lower_text in ('профиль', '👤 профиль'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('Профиль доступен только в личке с ботом.'), parse_mode='HTML')
            return

        await send_result(update, context, profile_text(update.effective_user.id))
        return

    if lower_text in ('топ 3', '🏆 топ 3'):
        await send_clean_group_result(update, context, top_text())
        return

    if lower_text in ('промокод', '🎁 промокод'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('Промокоды доступны только в личке с ботом.'), parse_mode='HTML')
            return

        await update.message.reply_text(pe('🎁 Введите промокод одним сообщением:'), parse_mode='HTML')
        context.user_data['waiting_promo_activate'] = True
        return

    if lower_text in ('казино', '🎰 казино'):
        await show_casino(update, context)
        return

    if lower_text in ('меню', '🏠 меню'):
        if is_group(update.effective_chat):
            await update.message.reply_text(
                pe('Главное меню:'),
                parse_mode='HTML',
                reply_markup=main_menu(is_admin(update.effective_user.id), group=True)
            )
        else:
            await update.message.reply_text(
                pe('Главное меню открыто снизу.'),
                parse_mode='HTML',
                reply_markup=reply_main_menu(is_admin(update.effective_user.id), group=False)
            )
        return

async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    await update.message.reply_text(pe(admin_panel_text()), parse_mode='HTML')



async def add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return
    text = ' '.join(context.args).strip()
    if not text:
        await update.message.reply_text(pe('Напиши так:\n/add Шрек\n\nМожно указать редкость:\n/add rare | Шрек'), parse_mode='HTML')
        return
    if add_phrase_db(text):
        await update.message.reply_text(pe(f'✅ Фраза добавлена: <b>{html.escape(text)}</b>'), parse_mode='HTML')
    else:
        await update.message.reply_text(pe('⚠️ Такая фраза уже есть или текст пустой.'), parse_mode='HTML')


async def ban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    if len(context.args) < 2 or not context.args[0].isdigit():
        await update.message.reply_text(
            pe(
                'Напиши так:\n'
                '<code>/ban 123456789 1d спам</code>\n\n'
                'Время: <code>30m</code>, <code>1h</code>, <code>7d</code>, <code>1w</code>, <code>perm</code>'
            ),
            parse_mode='HTML'
        )
        return

    target = int(context.args[0])
    ok_time, banned_until, readable = parse_duration_to_until(context.args[1])

    if not ok_time:
        await update.message.reply_text(
            pe('Неверное время. Пример: <code>30m</code>, <code>1h</code>, <code>7d</code>, <code>1w</code>, <code>perm</code>'),
            parse_mode='HTML'
        )
        return

    reason = " ".join(context.args[2:]).strip() or "не указана"

    ok, msg = set_ban_user(target, 1, reason=reason, banned_until=banned_until, admin_id=update.effective_user.id)
    await update.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')

    if ok:
        try:
            await context.bot.send_message(
                chat_id=target,
                text=pe(
                    '⛔ Вы были забанены у бота.\n'
                    f'Время: <b>{html.escape(ban_time_text(banned_until))}</b>\n'
                    f'Причина: <b>{html.escape(reason)}</b>'
                ),
                parse_mode='HTML'
            )
        except Exception:
            pass



async def unban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text(pe('Напиши так:\n<code>/unban 123456789</code>'), parse_mode='HTML')
        return

    target = int(context.args[0])
    ok, msg = set_ban_user(target, 0)
    await update.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')

    if ok:
        try:
            await context.bot.send_message(
                chat_id=target,
                text=pe('✅ Вы были разбанены у бота.'),
                parse_mode='HTML'
            )
        except Exception:
            pass



async def give_direct_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    if len(context.args) < 2 or not context.args[0].isdigit():
        await update.message.reply_text(pe('Напиши так:\n<code>/give 123456789 10 причина</code>'), parse_mode='HTML')
        return

    target = int(context.args[0])
    amount = parse_money(context.args[1])
    reason = " ".join(context.args[2:]).strip() or "не указана"

    if amount is None or amount <= 0:
        await update.message.reply_text(pe('Сумма должна быть больше 0.'), parse_mode='HTML')
        return

    if not get_user(target):
        await update.message.reply_text(pe('❌ Пользователь не найден.'), parse_mode='HTML')
        return

    add_balance(target, amount)
    await update.message.reply_text(
        pe(
            f'✅ Выдано <b>{money(amount)}</b> пользователю <code>{target}</code>.\n'
            f'Причина: <b>{html.escape(reason)}</b>'
        ),
        parse_mode='HTML'
    )

    try:
        await context.bot.send_message(
            chat_id=target,
            text=pe(
                f'💰 Вам начислено <b>{money(amount)}</b>.\n'
                f'Причина: <b>{html.escape(reason)}</b>'
            ),
            parse_mode='HTML'
        )
    except Exception:
        pass



async def take_direct_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    if len(context.args) < 2 or not context.args[0].isdigit():
        await update.message.reply_text(pe('Напиши так:\n<code>/take 123456789 10</code>'), parse_mode='HTML')
        return

    target = int(context.args[0])
    amount = parse_money(context.args[1])

    if amount is None or amount <= 0:
        await update.message.reply_text(pe('Сумма должна быть больше 0.'), parse_mode='HTML')
        return

    ok, msg = take_balance(target, amount)
    await update.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')


async def setuid_direct_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    if len(context.args) < 2 or not context.args[0].isdigit():
        await update.message.reply_text(pe('Напиши так:\n<code>/setuid 123456789 VIP1</code>'), parse_mode='HTML')
        return

    target = int(context.args[0])
    uid = context.args[1]
    ok, msg = set_uid(target, uid)
    await update.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')


async def hide_direct_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text(pe('Напиши так:\n<code>/hide 123456789</code>'), parse_mode='HTML')
        return

    ok, msg = hide_user(int(context.args[0]))
    await update.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')


async def unhide_direct_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text(pe('Напиши так:\n<code>/unhide 123456789</code>'), parse_mode='HTML')
        return

    ok, msg = unhide_user(int(context.args[0]))
    await update.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')


async def promo_create_direct_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    if len(context.args) < 3:
        await update.message.reply_text(pe('Напиши так:\n<code>/promo_create CODE 5 10</code>'), parse_mode='HTML')
        return

    code = context.args[0]
    amount = parse_money(context.args[1])

    if amount is None or amount <= 0:
        await update.message.reply_text(pe('Сумма должна быть больше 0.'), parse_mode='HTML')
        return

    if not context.args[2].isdigit() or int(context.args[2]) <= 0:
        await update.message.reply_text(pe('Лимит должен быть числом больше 0.'), parse_mode='HTML')
        return

    ok, msg = create_promo_code(code, amount, int(context.args[2]), update.effective_user.id)
    await update.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')


async def promos_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    await update.message.reply_text(pe(promo_codes_text()), parse_mode='HTML')


async def groups_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    await update.message.reply_text(pe(groups_text()), parse_mode='HTML')


async def broadcast_direct_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    message = " ".join(context.args).strip()

    if not message:
        await update.message.reply_text(pe('Напиши так:\n<code>/broadcast текст уведомления</code>'), parse_mode='HTML')
        return

    with db() as conn:
        users = conn.execute("SELECT user_id FROM users WHERE hidden=0 AND banned=0").fetchall()

    sent = 0
    failed = 0

    for (user_id,) in users:
        try:
            await context.bot.send_message(chat_id=user_id, text=pe(message), parse_mode='HTML')
            sent += 1
        except Exception:
            failed += 1

    await update.message.reply_text(pe(f'📣 Рассылка завершена.\nОтправлено: <b>{sent}</b>\nОшибок: <b>{failed}</b>'), parse_mode='HTML')


async def list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return
    rows = last_phrases(20)
    if not rows:
        await update.message.reply_text(pe('Фраз пока нет.'), parse_mode='HTML')
        return
    text = '📋 Последние фразы:\n\n' + '\n'.join((f'{pid}. [{RARITY_LABELS.get(rarity, rarity)}] {html.escape(txt)}' for pid, txt, rarity in rows))
    await update.message.reply_text(pe(text), parse_mode='HTML')

async def delete_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text(pe('Напиши так:\n/delete 12'), parse_mode='HTML')
        return
    ok = delete_phrase_db(int(context.args[0]))
    await update.message.reply_text(pe('🗑 Удалено.' if ok else '⚠️ ID не найден.'), parse_mode='HTML')

async def profile_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    await send_result(update, context, profile_text(update.effective_user.id))

async def top_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    await send_clean_group_result(update, context, top_text())

async def search_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text(pe('Напиши так:\n/search 123456789'), parse_mode='HTML')
        return
    target = int(context.args[0])
    result = search_user_text(target)
    if result is None:
        await update.message.reply_text(pe('❌ Такого человека нет в боте.'), parse_mode='HTML')
    else:
        await update.message.reply_text(pe(result), parse_mode='HTML')

async def dbpath_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return
    os.makedirs(DB_DIR, exist_ok=True)
    conn = db()
    cur = conn.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS db_check (id INTEGER PRIMARY KEY AUTOINCREMENT, created_at INTEGER NOT NULL)')
    cur.execute('INSERT INTO db_check (created_at) VALUES (?)', (ts(),))
    cur.execute('SELECT COUNT(*) FROM db_check')
    count = cur.fetchone()[0]
    conn.commit()
    conn.close()
    await update.message.reply_text(pe(f'🗄 База данных:\n<code>{html.escape(os.path.abspath(DB_PATH))}</code>\n\nФайл существует: <b>{os.path.exists(DB_PATH)}</b>\nПроверочных записей: <b>{count}</b>'), parse_mode='HTML')

async def admin_stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return
    await send_long_message(context.bot, update.effective_chat.id, admin_stats_text(), reply_markup=admin_menu())



async def promo_activate_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.message.chat.type != 'private':
        await q.message.reply_text(pe('Промокоды доступны только в личке с ботом.'), parse_mode='HTML')
        return ConversationHandler.END

    register_user(q.from_user)

    await q.message.reply_text(
        pe('🎁 Введите промокод одним сообщением:'),
        parse_mode='HTML'
    )

    return WAIT_PROMO_ACTIVATE


async def promo_activate_text_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)

    if update.effective_chat.type != 'private':
        await update.message.reply_text(pe('Промокоды доступны только в личке с ботом.'), parse_mode='HTML')
        return ConversationHandler.END

    await update.message.reply_text(
        pe('🎁 Введите промокод одним сообщением:'),
        parse_mode='HTML'
    )

    return WAIT_PROMO_ACTIVATE


async def promo_activate_finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)

    code = update.message.text.strip()

    ok, msg = activate_promo_code(update.effective_user.id, code)
    await update.message.reply_text(
        pe(('✅ ' if ok else '❌ ') + msg),
        parse_mode='HTML'
    )

    return ConversationHandler.END


async def promo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)

    if not context.args:
        await send_result(update, context, "Напиши промокод:\n<code>/promo CODE</code>")
        return

    ok, msg = activate_promo_code(update.effective_user.id, context.args[0])
    await send_result(update, context, ("✅ " if ok else "❌ ") + msg)


async def casino_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_casino(update, context)


async def slots_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if not context.args:
        await send_clean_group_result(update, context, "Напиши ставку:\n<code>/slots 1</code>")
        return

    amount = parse_money(context.args[0])

    if amount is None:
        await send_clean_group_result(update, context, "Введите ставку числом. Например:\n<code>/slots 1</code>")
        return

    await play_slots(update, context, amount)



async def coin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if len(context.args) < 2:
        await send_clean_group_result(
            update,
            context,
            "Напиши выбор и ставку:\n"
            "<code>/coin орел 1</code>\n"
            "<code>/coin решка 1</code>"
        )
        return

    side = normalize_coin_side(context.args[0])

    if side is None:
        await send_clean_group_result(update, context, "Выбери: <b>орел</b> или <b>решка</b>.")
        return

    amount = parse_money(context.args[1])

    if amount is None:
        await send_clean_group_result(update, context, "Введите ставку числом. Например:\n<code>/coin орел 1</code>")
        return

    await play_coin(update, context, side, amount)


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(pe('Отменено.'), parse_mode='HTML')
    return ConversationHandler.END

async def add_phrase_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(q.from_user.id):
        await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END
    await q.message.reply_text(pe('➕ Отправь новую фразу одним сообщением.\n\nМожно указать редкость так:\nrare | Шрек\nepic | Супергерой\nlegendary | Легенда\n\nМожно также отправить .txt файл: каждая непустая строка добавится как отдельная фраза. В .txt тоже можно использовать формат rare | фраза.\n\nДля отмены напиши /cancel'), parse_mode='HTML')
    return WAIT_PHRASE

async def receive_phrase(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END
    text = update.message.text.strip()
    if add_phrase_db(text):
        await update.message.reply_text(pe(f'✅ Фраза добавлена: <b>{html.escape(text)}</b>'), parse_mode='HTML', reply_markup=admin_menu())
    else:
        await update.message.reply_text(pe('⚠️ Такая фраза уже есть или текст пустой.'), reply_markup=admin_menu(), parse_mode='HTML')
    return ConversationHandler.END

async def delete_phrase_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(q.from_user.id):
        await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END
    await q.message.reply_text(pe('🗑 Введите ID фразы, которую нужно удалить.\n\nID можно посмотреть через кнопку «Последние фразы» или команду /list.\nДля отмены напиши /cancel'), parse_mode='HTML')
    return WAIT_DELETE_PHRASE

async def delete_phrase_finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END
    raw = update.message.text.strip()
    if not raw.isdigit():
        await update.message.reply_text(pe('Введите ID фразы числом.'), parse_mode='HTML')
        return WAIT_DELETE_PHRASE
    ok = delete_phrase_db(int(raw))
    if ok:
        await update.message.reply_text(pe('✅ Фраза удалена.'), reply_markup=admin_menu(), parse_mode='HTML')
    else:
        await update.message.reply_text(pe('⚠️ Фраза с таким ID не найдена.'), reply_markup=admin_menu(), parse_mode='HTML')
    return ConversationHandler.END

async def broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(q.from_user.id):
        await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END
    await q.message.reply_text(pe('📣 Отправь текст уведомления, который нужно разослать всем пользователям бота.\n\nДля отмены напиши /cancel'), parse_mode='HTML')
    return WAIT_BROADCAST_TEXT

async def broadcast_finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END
    text = update.message.text.strip()
    if not text:
        await update.message.reply_text(pe('Текст пустой. Отправь уведомление еще раз.'), parse_mode='HTML')
        return WAIT_BROADCAST_TEXT
    rows = get_all_users(include_hidden=True)
    sent = 0
    failed = 0
    await update.message.reply_text(pe(f'📣 Начинаю рассылку для {len(rows)} пользователей...'), parse_mode='HTML')
    for user_id, username, first_name, uid, balance, openings, hidden in rows:
        try:
            await context.bot.send_message(chat_id=user_id, text=pe(f'📣 <b>Уведомление</b>\n\n{html.escape(text)}'), parse_mode='HTML')
            sent += 1
        except Exception:
            failed += 1
    await update.message.reply_text(pe(f'✅ Рассылка завершена.\n\nОтправлено: <b>{sent}</b>\nНе отправлено: <b>{failed}</b>'), parse_mode='HTML', reply_markup=admin_menu())
    return ConversationHandler.END

async def txt_phrases_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not is_admin(user.id):
        return
    document = update.message.document
    if not document:
        return
    filename = document.file_name or ''
    if not filename.lower().endswith('.txt'):
        await update.message.reply_text(pe('⚠️ Отправь файл именно в формате .txt'), parse_mode='HTML')
        return
    try:
        file = await context.bot.get_file(document.file_id)
        data = await file.download_as_bytearray()
        content = bytes(data).decode('utf-8-sig', errors='ignore')
    except Exception as e:
        await update.message.reply_text(pe(f'⚠️ Не удалось прочитать файл: {html.escape(str(e))}'), parse_mode='HTML')
        return
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if not lines:
        await update.message.reply_text(pe('⚠️ В файле нет фраз. Добавь каждую фразу с новой строки.'), parse_mode='HTML')
        return
    added = 0
    skipped = 0
    for phrase in lines:
        if add_phrase_db(phrase):
            added += 1
        else:
            skipped += 1
    await update.message.reply_text(pe(f'📄 <b>Импорт .txt завершен</b>\n\nДобавлено: <b>{added}</b>\nПропущено: <b>{skipped}</b>\nВсего строк: <b>{len(lines)}</b>'), parse_mode='HTML', reply_markup=admin_menu())

async def admin_stats_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(q.from_user.id):
        await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return
    await send_long_message(context.bot, q.message.chat.id, admin_stats_text(), reply_markup=admin_menu())

async def withdraw_start_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)

    if update.effective_chat.type != 'private':
        await update.message.reply_text(pe('Вывод доступен только в личке с ботом.'), parse_mode='HTML')
        return ConversationHandler.END

    row = get_user(update.effective_user.id)

    if not row:
        await update.message.reply_text(pe('Профиль не найден. Напиши /start.'), parse_mode='HTML')
        return ConversationHandler.END

    balance_milli = row[4]

    if balance_milli < MIN_WITHDRAW_MILLI:
        await send_result(
            update,
            context,
            '❌ Недостаточно средств для вывода.\n'
            f'Минимальная сумма вывода: <b>{money(MIN_WITHDRAW_MILLI)}</b>\n'
            f'Ваш баланс: <b>{money(balance_milli)}</b>'
        )
        return ConversationHandler.END

    await send_result(update, context, '💸 Введите адрес кошелька USDT в сети TON:')
    return WAIT_WALLET


async def withdraw_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.message.chat.type != 'private':
        await q.answer('Вывод доступен только в личке с ботом.', show_alert=True)
        return ConversationHandler.END
    await q.answer()
    register_user(q.from_user)
    row = get_user(q.from_user.id)
    bal = row[4]
    if bal < MIN_WITHDRAW_MILLI:
        await send_result(update, context, f'❌ Недостаточно средств для вывода.\nМинимальная сумма вывода: <b>{money(MIN_WITHDRAW_MILLI)}</b>\nВаш баланс: <b>{money(bal)}</b>')
        return ConversationHandler.END
    await send_result(update, context, '💸 Введите адрес кошелька USDT в сети TON:')
    return WAIT_WALLET

async def withdraw_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wallet = update.message.text.strip()
    if len(wallet) < 10:
        await update.message.reply_text(pe('Адрес слишком короткий. Отправь корректный адрес.'), parse_mode='HTML')
        return WAIT_WALLET
    context.user_data['wallet'] = wallet
    await update.message.reply_text(pe('Теперь введи сумму вывода в USDT.\nНапример: 100 или 150.5'), parse_mode='HTML')
    return WAIT_AMOUNT

async def withdraw_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    amount = parse_money(update.message.text)
    if amount is None:
        await update.message.reply_text(pe('Введите сумму числом.'), parse_mode='HTML')
        return WAIT_AMOUNT
    if amount < MIN_WITHDRAW_MILLI:
        await update.message.reply_text(pe(f'Минимальная сумма вывода: {money(MIN_WITHDRAW_MILLI)}'), parse_mode='HTML')
        return WAIT_AMOUNT
    row = get_user(update.effective_user.id)
    if amount > row[4]:
        await update.message.reply_text(pe(f'Недостаточно средств.\nВаш баланс: {money(row[4])}'), parse_mode='HTML')
        return ConversationHandler.END
    ok, msg = take_balance(update.effective_user.id, amount)
    if not ok:
        await update.message.reply_text(pe(msg), parse_mode='HTML')
        return ConversationHandler.END
    wallet = context.user_data['wallet']
    wid = create_withdrawal(update.effective_user.id, wallet, amount)
    await update.message.reply_text(pe('✅ Заявка на вывод создана и отправлена админам на проверку.'), parse_mode='HTML')
    text = f'💸 <b>Новая заявка на вывод</b>\n\nID заявки: <code>{wid}</code>\nПользователь: {mention(update.effective_user)}\nTelegram ID: <code>{update.effective_user.id}</code>\nСумма: <b>{money(amount)}</b>\nКошелек TON USDT:\n<code>{html.escape(wallet)}</code>'
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(admin_id, pe(text), parse_mode='HTML', reply_markup=withdraw_admin_menu(wid))
        except Exception as e:
            logger.warning('Не удалось отправить заявку админу %s: %s', admin_id, e)
    return ConversationHandler.END


async def promo_create_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if not is_admin(q.from_user.id):
        await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END

    await q.message.reply_text(pe('🎁 Введите название промокода:'), parse_mode='HTML')
    return WAIT_PROMO_CODE


async def promo_create_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END

    code = update.message.text.strip().upper()

    if len(code) < 3:
        await update.message.reply_text(pe('Промокод слишком короткий. Минимум 3 символа.'), parse_mode='HTML')
        return WAIT_PROMO_CODE

    context.user_data['promo_code'] = code
    await update.message.reply_text(pe('Введите сумму USDT для промокода:'), parse_mode='HTML')
    return WAIT_PROMO_AMOUNT


async def promo_create_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END

    amount = parse_money(update.message.text)

    if amount is None or amount <= 0:
        await update.message.reply_text(pe('Введите сумму больше 0.'), parse_mode='HTML')
        return WAIT_PROMO_AMOUNT

    context.user_data['promo_amount'] = amount
    await update.message.reply_text(pe('Введите лимит активаций промокода:'), parse_mode='HTML')
    return WAIT_PROMO_LIMIT


async def promo_create_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END

    raw = update.message.text.strip()

    if not raw.isdigit() or int(raw) <= 0:
        await update.message.reply_text(pe('Введите лимит числом больше 0.'), parse_mode='HTML')
        return WAIT_PROMO_LIMIT

    code = context.user_data.get('promo_code')
    amount = context.user_data.get('promo_amount')
    limit = int(raw)

    ok, msg = create_promo_code(code, amount, limit, update.effective_user.id)
    await update.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')
    return ConversationHandler.END


async def give_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(q.from_user.id):
        await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END
    await q.message.reply_text(pe('💰 Введите Telegram ID пользователя, которому нужно выдать USDT:'), parse_mode='HTML')
    return WAIT_GIVE_USER

async def give_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text.strip().isdigit():
        await update.message.reply_text(pe('Введите Telegram ID числом.'), parse_mode='HTML')
        return WAIT_GIVE_USER
    uid = int(update.message.text.strip())
    if not get_user(uid):
        await update.message.reply_text(pe('Пользователь не найден. Он должен сначала вызвать бота или написать /start.'), parse_mode='HTML')
        return ConversationHandler.END
    context.user_data['give_user'] = uid
    await update.message.reply_text(pe('Введите сумму USDT для выдачи:'), parse_mode='HTML')
    return WAIT_GIVE_AMOUNT

async def give_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    amount = parse_money(update.message.text)
    if amount is None or amount <= 0:
        await update.message.reply_text(pe('Введите сумму больше 0.'), parse_mode='HTML')
        return WAIT_GIVE_AMOUNT
    user_id = context.user_data['give_user']
    add_balance(user_id, amount)
    await update.message.reply_text(pe(f'✅ Пользователю <code>{user_id}</code> выдано <b>{money(amount)}</b>.'), parse_mode='HTML')
    try:
        await context.bot.send_message(user_id, pe(f'💰 Вам начислено <b>{money(amount)}</b>.'), parse_mode='HTML')
    except Exception:
        pass
    return ConversationHandler.END

async def take_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(q.from_user.id):
        await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END
    await q.message.reply_text(pe('➖ Введите Telegram ID пользователя, у которого нужно забрать USDT:'), parse_mode='HTML')
    return WAIT_TAKE_USER

async def take_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text.strip().isdigit():
        await update.message.reply_text(pe('Введите Telegram ID числом.'), parse_mode='HTML')
        return WAIT_TAKE_USER
    uid = int(update.message.text.strip())
    if not get_user(uid):
        await update.message.reply_text(pe('Пользователь не найден.'), parse_mode='HTML')
        return ConversationHandler.END
    context.user_data['take_user'] = uid
    await update.message.reply_text(pe('Введите сумму USDT, которую нужно забрать:'), parse_mode='HTML')
    return WAIT_TAKE_AMOUNT

async def take_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    amount = parse_money(update.message.text)
    if amount is None or amount <= 0:
        await update.message.reply_text(pe('Введите сумму больше 0.'), parse_mode='HTML')
        return WAIT_TAKE_AMOUNT
    user_id = context.user_data['take_user']
    ok, msg = take_balance(user_id, amount)
    if ok:
        await update.message.reply_text(pe(f'✅ У пользователя <code>{user_id}</code> забрано <b>{money(amount)}</b>.'), parse_mode='HTML')
    else:
        await update.message.reply_text(pe(f'⚠️ {msg}'), parse_mode='HTML')
    return ConversationHandler.END

async def uid_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(q.from_user.id):
        await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END
    await q.message.reply_text(pe('🆔 Введите Telegram ID пользователя:'), parse_mode='HTML')
    return WAIT_UID_USER

async def uid_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text.strip().isdigit():
        await update.message.reply_text(pe('Введите Telegram ID числом.'), parse_mode='HTML')
        return WAIT_UID_USER
    target = int(update.message.text.strip())
    if not get_user(target):
        await update.message.reply_text(pe('Пользователь не найден.'), parse_mode='HTML')
        return ConversationHandler.END
    context.user_data['uid_user'] = target
    await update.message.reply_text(pe('Введите новый кастом UID:'), parse_mode='HTML')
    return WAIT_UID_VALUE

async def uid_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ok, msg = set_uid(context.user_data['uid_user'], update.message.text)
    await update.message.reply_text(pe(('✅ ' if ok else '⚠️ ') + msg), parse_mode='HTML')
    return ConversationHandler.END

async def hide_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(q.from_user.id):
        await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END
    await q.message.reply_text(pe('🙈 Введите Telegram ID пользователя, которого нужно скрыть:'), parse_mode='HTML')
    return WAIT_HIDE_USER

async def hide_finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text.strip().isdigit():
        await update.message.reply_text(pe('Введите Telegram ID числом.'), parse_mode='HTML')
        return WAIT_HIDE_USER
    target = int(update.message.text.strip())
    ok, msg = hide_user(target)
    await update.message.reply_text(pe(f"{('✅' if ok else '⚠️')} {msg}"), parse_mode='HTML')
    return ConversationHandler.END

async def unhide_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(q.from_user.id):
        await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END
    await q.message.reply_text(pe('👁 Введите Telegram ID пользователя, которого нужно раскрыть:'), parse_mode='HTML')
    return WAIT_UNHIDE_USER

async def unhide_finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END
    if not update.message.text.strip().isdigit():
        await update.message.reply_text(pe('Введите Telegram ID числом.'), parse_mode='HTML')
        return WAIT_UNHIDE_USER
    target = int(update.message.text.strip())
    ok, msg = unhide_user(target)
    await update.message.reply_text(pe(f"{('✅' if ok else '⚠️')} {msg}"), parse_mode='HTML')
    return ConversationHandler.END

async def search_user_start_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)

    if update.effective_chat.type != 'private':
        await update.message.reply_text(pe('🔎 Поиск по ID доступен в личке с ботом.'), parse_mode='HTML')
        return ConversationHandler.END

    await update.message.reply_text(pe('🔎 Введите Telegram ID пользователя для поиска:'), parse_mode='HTML')
    return WAIT_SEARCH_USER


async def search_user_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.message.chat.type != 'private':
        await q.message.reply_text(pe('🔎 Поиск по ID доступен в личке с ботом.'), parse_mode='HTML')
        return ConversationHandler.END
    register_user(q.from_user)
    await q.message.reply_text(pe('🔎 Введите Telegram ID пользователя для поиска:'), parse_mode='HTML')
    return WAIT_SEARCH_USER

async def search_user_finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    if not update.message.text.strip().isdigit():
        await update.message.reply_text(pe('Введите Telegram ID числом.'), parse_mode='HTML')
        return WAIT_SEARCH_USER
    target = int(update.message.text.strip())
    result = search_user_text(target)
    if result is None:
        await update.message.reply_text(pe('❌ Такого человека нет в боте.'), parse_mode='HTML')
    else:
        await update.message.reply_text(pe(result), parse_mode='HTML')
    return ConversationHandler.END

async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data
    register_user(q.from_user)
    if q.message:
        remember_group(q.message.chat)
        await send_result(update, context, profile_text(q.from_user.id))
        return

        await send_result(update, context, profile_text(q.from_user.id))
        return

    # PROFILE_FIX_FINAL_OK
        await send_result(update, context, inventory_text(q.from_user.id))
        return

        await send_result(update, context, profile_text(q.from_user.id))
        return

    if data == 'promo_list':
        await q.answer()
        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return
        await q.message.reply_text(pe(promo_codes_text()), parse_mode='HTML')
        return

        await send_result(update, context, inventory_text(q.from_user.id))
        return

    if data == 'profile':
        await q.answer()
        register_user(q.from_user)

        if q.message.chat.type != 'private':
            await q.message.reply_text(pe('Профиль доступен только в личке с ботом.'), parse_mode='HTML')
            return

        await send_result(update, context, profile_text(q.from_user.id))
        return

    if data == 'casino':
        await q.answer()
        await show_casino(update, context)
        return

    if data.startswith('slots_bet:'):
        await q.answer()
        amount = parse_money(data.split(':', 1)[1])

        if amount is None:
            await send_result(update, context, '❌ Ошибка ставки.')
            return

        await play_slots(update, context, amount)
        return

    if data.startswith('coin_bet:'):
        await q.answer()
        parts = data.split(':')

        if len(parts) != 3:
            await send_clean_group_result(update, context, '❌ Ошибка ставки.')
            return

        side = normalize_coin_side(parts[1])
        amount = parse_money(parts[2])

        if side is None or amount is None:
            await send_clean_group_result(update, context, '❌ Ошибка ставки.')
            return

        await play_coin(update, context, side, amount)
        return

    if data.startswith('bonus:'):
        msg = claim_bonus(data.split(':', 1)[1], q.from_user.id)
        await q.answer(msg, show_alert=True)
        return
    if data.startswith('wd_ok:') or data.startswith('wd_no:'):
        await q.answer()
        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return
        wid = int(data.split(':', 1)[1])
        row = get_withdrawal(wid)
        if not row:
            await q.edit_message_text(pe('Заявка не найдена.'), parse_mode='HTML')
            return
        _, target, wallet, amount, status = row
        if status != 'pending':
            await q.edit_message_text(pe('Эта заявка уже обработана.'), parse_mode='HTML')
            return
        if data.startswith('wd_ok:'):
            if set_withdrawal(wid, 'approved', q.from_user.id):
                await q.edit_message_text(pe(f'✅ Заявка #{wid} одобрена.\nСумма: {money(amount)}'), parse_mode='HTML')
                try:
                    await context.bot.send_message(target, pe(f'✅ Ваша заявка на вывод {money(amount)} одобрена.'), parse_mode='HTML')
                except Exception:
                    pass
        elif set_withdrawal(wid, 'declined', q.from_user.id):
            add_balance(target, amount)
            await q.edit_message_text(pe(f'❌ Заявка #{wid} отклонена.\nСумма возвращена пользователю: {money(amount)}'), parse_mode='HTML')
            try:
                await context.bot.send_message(target, pe(f'❌ Ваша заявка на вывод {money(amount)} отклонена. Средства возвращены на баланс.'), parse_mode='HTML')
            except Exception:
                pass
        return
    await q.answer()
    if data == 'whoami':
        await send_role(update, context)
    elif data == 'profile':
        if q.message.chat.type != 'private':
            await q.answer('Профиль доступен только в личке.', show_alert=True)
        else:
            await send_result(update, context, profile_text(q.from_user.id))
    elif data == 'top3':
        await send_clean_group_result(update, context, top_text())
    elif data == 'daily_bonus':
        await q.answer('Ежедневный бонус отключен.', show_alert=True)
    elif data == 'admin_menu':
        if is_admin(q.from_user.id):
            await q.edit_message_text(pe(admin_panel_text()), parse_mode='HTML')
        else:
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
    elif data == 'back':
        if is_group(q.message.chat):
            await q.message.reply_text(
                pe('Главное меню:'),
                parse_mode='HTML',
                reply_markup=main_menu(is_admin(q.from_user.id), group=True)
            )
        else:
            await q.message.reply_text(
                pe('Главное меню открыто снизу.'),
                parse_mode='HTML',
                reply_markup=reply_main_menu(is_admin(q.from_user.id), group=False)
            )
    elif data == 'last_phrases':
        rows = last_phrases(10)
        text = 'Фраз пока нет.' if not rows else '📋 Последние фразы:\n\n' + '\n'.join((f'{pid}. [{RARITY_LABELS.get(rarity, rarity)}] {html.escape(txt)}' for pid, txt, rarity in rows))
        await q.edit_message_text(pe(text), parse_mode='HTML', reply_markup=admin_menu())
    elif data == 'phrase_count':
        await q.edit_message_text(pe(f'🔢 В базе фраз: {phrase_count()}'), reply_markup=admin_menu(), parse_mode='HTML')
    elif data == 'admin_stats':
        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return
        await send_long_message(context.bot, q.message.chat.id, admin_stats_text(), reply_markup=admin_menu())
    elif data == 'groups':
        await q.message.reply_text(pe(groups_text()), parse_mode='HTML')

def main():
    print('VERSION_ADMIN_FIX')
    init_db()
    app = Application.builder().token(BOT_TOKEN).defaults(Defaults(parse_mode="HTML")).build()
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('admin', admin_cmd))
    app.add_handler(CommandHandler('menu', menu_cmd))
    app.add_handler(CommandHandler('whoami', whoami))
    app.add_handler(CommandHandler('ban', ban_cmd))
    app.add_handler(CommandHandler('unban', unban_cmd))
    app.add_handler(CommandHandler('give', give_direct_cmd))
    app.add_handler(CommandHandler('take', take_direct_cmd))
    app.add_handler(CommandHandler('setuid', setuid_direct_cmd))
    app.add_handler(CommandHandler('hide', hide_direct_cmd))
    app.add_handler(CommandHandler('unhide', unhide_direct_cmd))
    app.add_handler(CommandHandler('promo_create', promo_create_direct_cmd))
    app.add_handler(CommandHandler('promos', promos_cmd))
    app.add_handler(CommandHandler('groups', groups_cmd))
    app.add_handler(CommandHandler('broadcast', broadcast_direct_cmd))
    app.add_handler(CommandHandler('add', add_cmd))
    app.add_handler(CommandHandler('list', list_cmd))
    app.add_handler(CommandHandler('delete', delete_cmd))
    app.add_handler(CommandHandler('profile', profile_cmd))
    app.add_handler(CommandHandler('top', top_cmd))
    app.add_handler(CommandHandler('promo', promo_cmd))
    app.add_handler(CommandHandler('casino', casino_cmd))
    app.add_handler(CommandHandler('slots', slots_cmd))
    app.add_handler(CommandHandler('coin', coin_cmd))
    app.add_handler(CommandHandler('search', search_cmd))
    app.add_handler(CommandHandler('dbpath', dbpath_cmd))
    app.add_handler(CommandHandler('adminstats', admin_stats_cmd))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(add_phrase_start, pattern='^add_phrase$')], states={WAIT_PHRASE: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_phrase)]}, fallbacks=[CommandHandler('cancel', cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(withdraw_start, pattern='^withdraw$'), MessageHandler(filters.Regex('^(💸 )?Вывод USDT$'), withdraw_start_text)], states={WAIT_WALLET: [MessageHandler(filters.TEXT & ~filters.COMMAND, withdraw_wallet)], WAIT_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, withdraw_amount)]}, fallbacks=[CommandHandler('cancel', cancel)]))
    app.add_handler(ConversationHandler(
        entry_points=[
            CallbackQueryHandler(promo_activate_start, pattern='^promo_activate$'),
            MessageHandler(filters.Regex('^(🎁 )?Промокод$'), promo_activate_text_start),
        ],
        states={
            WAIT_PROMO_ACTIVATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_activate_finish)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    ))

    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(promo_create_start, pattern='^promo_create$')],
        states={
            WAIT_PROMO_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_create_code)],
            WAIT_PROMO_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_create_amount)],
            WAIT_PROMO_LIMIT: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_create_limit)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    ))

    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(give_start, pattern='^give_usdt$')], states={WAIT_GIVE_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, give_user)], WAIT_GIVE_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, give_amount)]}, fallbacks=[CommandHandler('cancel', cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(take_start, pattern='^take_usdt$')], states={WAIT_TAKE_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, take_user)], WAIT_TAKE_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, take_amount)]}, fallbacks=[CommandHandler('cancel', cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(uid_start, pattern='^custom_uid$')], states={WAIT_UID_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, uid_user)], WAIT_UID_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, uid_value)]}, fallbacks=[CommandHandler('cancel', cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(hide_start, pattern='^hide_user$')], states={WAIT_HIDE_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, hide_finish)]}, fallbacks=[CommandHandler('cancel', cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(unhide_start, pattern='^unhide_user$')], states={WAIT_UNHIDE_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, unhide_finish)]}, fallbacks=[CommandHandler('cancel', cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(search_user_start, pattern='^search_user$'), MessageHandler(filters.Regex('^(🔎 )?Поиск по ID$'), search_user_start_text)], states={WAIT_SEARCH_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_user_finish)]}, fallbacks=[CommandHandler('cancel', cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(delete_phrase_start, pattern='^delete_phrase_btn$')], states={WAIT_DELETE_PHRASE: [MessageHandler(filters.TEXT & ~filters.COMMAND, delete_phrase_finish)]}, fallbacks=[CommandHandler('cancel', cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(broadcast_start, pattern='^broadcast$')], states={WAIT_BROADCAST_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, broadcast_finish)]}, fallbacks=[CommandHandler('cancel', cancel)]))
    app.add_handler(CallbackQueryHandler(admin_stats_button, pattern='^admin_stats$'))
    app.add_handler(MessageHandler(filters.Document.ALL, txt_phrases_handler))
    app.add_handler(CallbackQueryHandler(buttons))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, trigger))
    logger.info('Бот запущен')
    app.run_polling()
if __name__ == '__main__':
    main()
