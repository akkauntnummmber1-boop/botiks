import random
import sqlite3
import logging
import time
import uuid
import html
import os
from pathlib import Path
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, Defaults, ConversationHandler, MessageHandler, filters
BOT_TOKEN = '8442673427:AAEj15lEhVaxBFHUBw_EUYdJEV_-99_e6p4'
ADMIN_IDS = {5037478748, 6991875}
DB_DIR = 'data'
DB_PATH = os.path.join(DB_DIR, 'bot.db')
os.makedirs(DB_DIR, exist_ok=True)
TRIGGERS = {'кто я', 'кто', 'я'}
ROLE_COOLDOWN_SECONDS = 10 * 60
BONUS_AMOUNT_MILLI = 100
MIN_WITHDRAW_MILLI = 100000
DAY_SECONDS = 24 * 60 * 60
DAILY_ROLE_BONUS_LIMIT = 5
RARITY_CHANCES = [('common', 70), ('rare', 20), ('epic', 8), ('legendary', 2)]
RARITY_LABELS = {
    'common': 'Обычная',
    'rare': 'Редкая',
    'epic': 'Эпическая',
    'legendary': 'Легендарная',
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

def pe(text: str) -> str:
    """Заменяет обычные emoji на premium emoji в HTML-тексте сообщения."""
    if text is None:
        return text
    text = str(text)
    replacements = [('ℹ️', PE_INFO), ('❗️', PE_WARN), ('⚠️', PE_WARN), ('⭐️', PE_STAR), ('👤', PE_USER), ('✅', PE_OK), ('👥', PE_USERS), ('📣', PE_ANNOUNCE), ('✋', PE_STOP), ('⛔', PE_STOP), ('🚫', PE_STOP), ('💰', PE_WALLET), ('💸', PE_WALLET), ('➕', PE_PLUS), ('📈', PE_CHART), ('📊', PE_CHART), ('💬', PE_CHAT), ('❗', PE_WARN), ('❌', PE_CROSS), ('🏘', PE_HOME), ('🏠', PE_HOME), ('⭐', PE_STAR), ('👁', PE_EYE), ('🔖', PE_UID), ('🆔', PE_UID), ('🏆', PE_TROPHY), ('🥇', PE_TOP1), ('🥈', PE_TOP2), ('🥉', PE_TOP3), ('🔎', PE_SEARCH), ('⏲', PE_TIMER), ('⏳', PE_TIMER), ('🎭', PE_MASKS), ('⚙', PE_INFO), ('🔢', PE_INFO), ('📋', PE_CHAT), ('📄', PE_CHAT), ('📛', PE_USER), ('🗄', PE_INFO), ('🗑', PE_CROSS), ('🙈', PE_EYE), ('➖', PE_CROSS), ('⬅', PE_HOME), ('🎁', PE_STAR)]
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
    cur.execute('\n        CREATE TABLE IF NOT EXISTS users (\n            user_id INTEGER PRIMARY KEY,\n            username TEXT,\n            first_name TEXT,\n            uid TEXT UNIQUE,\n            balance_milli INTEGER NOT NULL DEFAULT 0,\n            openings INTEGER NOT NULL DEFAULT 0,\n            last_role_at INTEGER NOT NULL DEFAULT 0,\n            hidden INTEGER NOT NULL DEFAULT 0,\n            created_at INTEGER NOT NULL\n        )\n        ')
    cur.execute('\n        CREATE TABLE IF NOT EXISTS bonus_claims (\n            bonus_id TEXT PRIMARY KEY,\n            user_id INTEGER NOT NULL,\n            amount_milli INTEGER NOT NULL,\n            claimed INTEGER NOT NULL DEFAULT 0,\n            created_at INTEGER NOT NULL,\n            claimed_at INTEGER\n        )\n        ')
    cur.execute('\n        CREATE TABLE IF NOT EXISTS daily_bonuses (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            user_id INTEGER NOT NULL,\n            amount_milli INTEGER NOT NULL,\n            claimed_at INTEGER NOT NULL\n        )\n        ')
    cur.execute('\n        CREATE TABLE IF NOT EXISTS groups (\n            chat_id INTEGER PRIMARY KEY,\n            title TEXT,\n            username TEXT,\n            type TEXT,\n            added_at INTEGER NOT NULL,\n            last_seen_at INTEGER NOT NULL\n        )\n        ')
    cur.execute("\n        CREATE TABLE IF NOT EXISTS withdrawals (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            user_id INTEGER NOT NULL,\n            wallet TEXT NOT NULL,\n            amount_milli INTEGER NOT NULL,\n            status TEXT NOT NULL DEFAULT 'pending',\n            created_at INTEGER NOT NULL,\n            reviewed_by INTEGER,\n            reviewed_at INTEGER\n        )\n        ")
    phrase_cols = columns(conn, 'phrases')
    if 'rarity' not in phrase_cols:
        cur.execute("ALTER TABLE phrases ADD COLUMN rarity TEXT NOT NULL DEFAULT 'common'")
    user_cols = columns(conn, 'users')
    if 'hidden' not in user_cols:
        cur.execute('ALTER TABLE users ADD COLUMN hidden INTEGER NOT NULL DEFAULT 0')
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
        return conn.execute('\n            SELECT user_id, username, first_name, uid, balance_milli, openings, last_role_at, hidden\n            FROM users WHERE user_id=?\n            ', (user_id,)).fetchone()

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
    user_id, username, first_name, uid, balance, openings, last_role, hidden = row
    if hidden:
        return None
    username_text = f'@{username}' if username else 'нет'
    first_name_text = first_name or 'нет'
    return f'🔎 <b>Пользователь найден</b>\n\n🆔 Telegram ID: <code>{user_id}</code>\n🔖 UID: <code>{html.escape(str(uid))}</code>\n💰 Баланс: <b>{money(balance)}</b>\n👁 Открытия: <b>{openings}</b>\n📛 Username: {html.escape(username_text)}\n👤 Имя: {html.escape(first_name_text)}\n🚫 Статус бана: <b>не забанен</b>'

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
        claimed_today = claimed_role_bonuses_today(conn, user_id)
        if claimed_today >= DAILY_ROLE_BONUS_LIMIT:
            return f'Дневной лимит бонусов исчерпан: {DAILY_ROLE_BONUS_LIMIT}/{DAILY_ROLE_BONUS_LIMIT}. Попробуйте завтра.'
        conn.execute('UPDATE bonus_claims SET claimed=1, claimed_at=? WHERE bonus_id=?', (ts(), bonus_id))
        conn.execute('UPDATE users SET balance_milli=balance_milli+? WHERE user_id=?', (amount, user_id))
        conn.commit()
    return f'Вы получили {money(amount)} ({claimed_today + 1}/{DAILY_ROLE_BONUS_LIMIT} сегодня)'

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
        return 'Профиль не найден. Напиши /start.'
    user_id, username, first_name, uid, balance, openings, last_role, hidden = row
    uname = f'@{username}' if username else 'нет'
    hidden_line = '\n🙈 Статус: <b>скрыт</b>' if hidden else ''
    return f'👤 <b>Профиль</b>\n\n🆔 Telegram ID: <code>{user_id}</code>\n🔖 UID: <code>{html.escape(str(uid))}</code>\n👁 Открытия: <b>{openings}</b>\n💰 Баланс: <b>{money(balance)}</b>\n📛 Username: {html.escape(uname)}{hidden_line}'

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
    buttons = [[InlineKeyboardButton('Кто я?', callback_data='whoami')]]
    if not group:
        buttons.append([InlineKeyboardButton('Профиль', callback_data='profile'), InlineKeyboardButton('Вывод USDT', callback_data='withdraw')])
        buttons.append([InlineKeyboardButton('Ежедневный бонус', callback_data='daily_bonus')])
        buttons.append([InlineKeyboardButton('Поиск по ID', callback_data='search_user')])
    buttons.append([InlineKeyboardButton('Топ 3', callback_data='top3')])
    if admin:
        buttons.append([InlineKeyboardButton('Админ-меню', callback_data='admin_menu')])
    return InlineKeyboardMarkup(buttons)

def role_menu(bonus_id: str, group=False):
    buttons = [[InlineKeyboardButton('Бонус', callback_data=f'bonus:{bonus_id}')]]
    if not group:
        buttons.append([InlineKeyboardButton('Профиль', callback_data='profile'), InlineKeyboardButton('Вывод USDT', callback_data='withdraw')])
        buttons.append([InlineKeyboardButton('Ежедневный бонус', callback_data='daily_bonus')])
        buttons.append([InlineKeyboardButton('Поиск по ID', callback_data='search_user')])
    return InlineKeyboardMarkup(buttons)

def admin_menu():
    return InlineKeyboardMarkup([[InlineKeyboardButton('Добавить фразу', callback_data='add_phrase')], [InlineKeyboardButton('Удалить фразу', callback_data='delete_phrase_btn')], [InlineKeyboardButton('Последние фразы', callback_data='last_phrases')], [InlineKeyboardButton('Количество фраз', callback_data='phrase_count')], [InlineKeyboardButton('Уведомление в бот', callback_data='broadcast')], [InlineKeyboardButton('Статистика', callback_data='admin_stats')], [InlineKeyboardButton('Выдать USDT', callback_data='give_usdt')], [InlineKeyboardButton('Забрать USDT', callback_data='take_usdt')], [InlineKeyboardButton('Выдать кастом UID', callback_data='custom_uid')], [InlineKeyboardButton('Скрыть пользователя', callback_data='hide_user')], [InlineKeyboardButton('Раскрыть пользователя', callback_data='unhide_user')], [InlineKeyboardButton('Группы с ботом', callback_data='groups')], [InlineKeyboardButton('Назад', callback_data='back')]])

def withdraw_admin_menu(wid: int):
    return InlineKeyboardMarkup([[InlineKeyboardButton('Одобрить', callback_data=f'wd_ok:{wid}'), InlineKeyboardButton('Отклонить', callback_data=f'wd_no:{wid}')]])

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

async def send_result(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    chat = update.effective_chat
    if chat.type == 'private':
        await delete_last_private(context, chat.id)
    msg = await context.bot.send_message(chat.id, pe(text), parse_mode='HTML', reply_markup=reply_markup)
    if chat.type == 'private':
        context.user_data['last_private_result'] = msg.message_id
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

async def send_role(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    register_user(user)
    remember_group(chat)
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
    inc_opening(user.id)
    bonus_id = create_bonus(user.id)
    await send_result(update, context, f'🎭 {mention(user)}, {html.escape(phrase)}\n⭐ Редкость: {html.escape(rarity_label)}', reply_markup=role_menu(bonus_id, group=is_group(chat)))

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)
    await update.message.reply_text(pe('🎭 Бот для игры «Кто я?»\n\nВ группе напиши: <b>кто я</b>, <b>кто</b> или <b>я</b>.'), parse_mode='HTML', reply_markup=main_menu(is_admin(update.effective_user.id), is_group(update.effective_chat)))

async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_role(update, context)

async def trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    register_user(update.effective_user)
    remember_group(update.effective_chat)
    if update.message.text.strip().lower() in TRIGGERS:
        await send_role(update, context)

async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return
    await update.message.reply_text(pe('⚙️ Админ-меню:'), reply_markup=admin_menu(), parse_mode='HTML')

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
    await send_result(update, context, top_text())

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
        await send_result(update, context, top_text())
    elif data == 'daily_bonus':
        if q.message.chat.type != 'private':
            await q.answer('Ежедневный бонус доступен только в личке.', show_alert=True)
        else:
            register_user(q.from_user)
            await send_result(update, context, claim_daily_bonus(q.from_user.id))
    elif data == 'admin_menu':
        if is_admin(q.from_user.id):
            await q.edit_message_text(pe('⚙️ Админ-меню:'), reply_markup=admin_menu(), parse_mode='HTML')
        else:
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
    elif data == 'back':
        await q.edit_message_text(pe('Главное меню:'), reply_markup=main_menu(is_admin(q.from_user.id), is_group(q.message.chat)), parse_mode='HTML')
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
    init_db()
    app = Application.builder().token(BOT_TOKEN).defaults(Defaults(parse_mode="HTML")).build()
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('whoami', whoami))
    app.add_handler(CommandHandler('admin', admin_cmd))
    app.add_handler(CommandHandler('add', add_cmd))
    app.add_handler(CommandHandler('list', list_cmd))
    app.add_handler(CommandHandler('delete', delete_cmd))
    app.add_handler(CommandHandler('profile', profile_cmd))
    app.add_handler(CommandHandler('top', top_cmd))
    app.add_handler(CommandHandler('search', search_cmd))
    app.add_handler(CommandHandler('dbpath', dbpath_cmd))
    app.add_handler(CommandHandler('adminstats', admin_stats_cmd))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(add_phrase_start, pattern='^add_phrase$')], states={WAIT_PHRASE: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_phrase)]}, fallbacks=[CommandHandler('cancel', cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(withdraw_start, pattern='^withdraw$')], states={WAIT_WALLET: [MessageHandler(filters.TEXT & ~filters.COMMAND, withdraw_wallet)], WAIT_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, withdraw_amount)]}, fallbacks=[CommandHandler('cancel', cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(give_start, pattern='^give_usdt$')], states={WAIT_GIVE_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, give_user)], WAIT_GIVE_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, give_amount)]}, fallbacks=[CommandHandler('cancel', cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(take_start, pattern='^take_usdt$')], states={WAIT_TAKE_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, take_user)], WAIT_TAKE_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, take_amount)]}, fallbacks=[CommandHandler('cancel', cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(uid_start, pattern='^custom_uid$')], states={WAIT_UID_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, uid_user)], WAIT_UID_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, uid_value)]}, fallbacks=[CommandHandler('cancel', cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(hide_start, pattern='^hide_user$')], states={WAIT_HIDE_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, hide_finish)]}, fallbacks=[CommandHandler('cancel', cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(unhide_start, pattern='^unhide_user$')], states={WAIT_UNHIDE_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, unhide_finish)]}, fallbacks=[CommandHandler('cancel', cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(search_user_start, pattern='^search_user$')], states={WAIT_SEARCH_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_user_finish)]}, fallbacks=[CommandHandler('cancel', cancel)]))
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
