import random
import sqlite3
import logging
import re
import asyncio
import time
import uuid
import html
import os
from pathlib import Path
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.error import BadRequest
from telegram.ext import ApplicationHandlerStop, Application, CallbackQueryHandler, CommandHandler, ContextTypes, Defaults, ConversationHandler, MessageHandler, filters
BOT_TOKEN = '8659612914:AAEVU_gNd4ZCjeVdLlRXjGYuZrrPRLTopz8'
ADMIN_IDS = {5037478748, 6991875, 5975861407}

PE_CRYPTO_CHART = '<tg-emoji emoji-id="5431577498364158238">📊</tg-emoji>'
PE_CRYPTO_PORTFOLIO = '<tg-emoji emoji-id="5359785904535774578">💼</tg-emoji>'
PE_CRYPTO_HISTORY = '<tg-emoji emoji-id="5434144690511290129">📰</tg-emoji>'
PE_CRYPTO_GREEN = '<tg-emoji emoji-id="5305313173623041702">🟢</tg-emoji>'
PE_CRYPTO_RED = '<tg-emoji emoji-id="5411225014148014586">🔴</tg-emoji>'
PE_CRYPTO_OK = '<tg-emoji emoji-id="5273806972871787310">✅</tg-emoji>'
PE_CRYPTO_NO = '<tg-emoji emoji-id="5210952531676504517">❌</tg-emoji>'

PE_DAILY_TOP_TROPHY = '<tg-emoji emoji-id="5409008750893734809">🏆</tg-emoji>'
PE_DAILY_TOP_GOLD = '<tg-emoji emoji-id="5280735858926822987">🥇</tg-emoji>'
PE_DAILY_TOP_SILVER = '<tg-emoji emoji-id="5283195573812340110">🥈</tg-emoji>'
PE_DAILY_TOP_BRONZE = '<tg-emoji emoji-id="5282750778409233531">🥉</tg-emoji>'
PE_DAILY_TOP_USER = '<tg-emoji emoji-id="5258011929993026890">👤</tg-emoji>'
PE_DAILY_TOP_ROLES = '<tg-emoji emoji-id="5433982607035474385">📰</tg-emoji>'
PE_DAILY_TOP_PROFIT = '<tg-emoji emoji-id="5409048419211682843">💵</tg-emoji>'
PE_DAILY_TOP_CLOCK = '<tg-emoji emoji-id="5382194935057372936">⏱</tg-emoji>'

PE_TOP_TROPHY = '<tg-emoji emoji-id="5409008750893734809">🏆</tg-emoji>'
PE_TOP_GOLD = '<tg-emoji emoji-id="5280735858926822987">🥇</tg-emoji>'
PE_TOP_SILVER = '<tg-emoji emoji-id="5283195573812340110">🥈</tg-emoji>'
PE_TOP_BRONZE = '<tg-emoji emoji-id="5282750778409233531">🥉</tg-emoji>'
PE_TOP_USER = '<tg-emoji emoji-id="5258011929993026890">👤</tg-emoji>'
PE_TOP_ROLES = '<tg-emoji emoji-id="5433982607035474385">📰</tg-emoji>'
PE_TOP_PROFIT = '<tg-emoji emoji-id="5409048419211682843">💵</tg-emoji>'
PE_TOP_CLOCK = '<tg-emoji emoji-id="5382194935057372936">⏱</tg-emoji>'
ROLE_LOG_CHAT_ID = -1003782092245
DB_DIR = 'data'
DB_PATH = os.path.join(DB_DIR, 'bot.db')
os.makedirs(DB_DIR, exist_ok=True)
TRIGGERS = {'кто я', 'кто', 'я'}
ROLE_COOLDOWN_SECONDS = 5 * 60

CASINO_COOLDOWN_SECONDS = 5     # кд казино 5 секунд
FOOTBALL_WIN_MULTIPLIER = 1.5      # выигрыш футбол
BASKETBALL_WIN_MULTIPLIER = 2.0    # выигрыш баскетбол
CASE_PRICE_MILLI = 5000  # 5 💵
CASE_COOLDOWN_SECONDS = 30
LUCK_BOOSTER_SECONDS = 30 * 60
CASE_SECRET_REWARD_CHANCE = 1  # 1 из 1000
CASE_DISCOUNT_MILLI = 2000  # скидка 2 💵 на следующий кейс
CASE_PREFIXES = ["Любитель казика", "Подружка админа", "T1 WORKER"]
MIN_SLOT_BET_MILLI = 1000       # 1 💵
MAX_SLOT_BET_MILLI = 100000      # 100 💵
SLOT_WIN_CHANCE_PERCENT = 12  # шанс выигрыша в слотах: 10–15%

MIN_COIN_BET_MILLI = 1000       # 1 💵
MAX_COIN_BET_MILLI = 100000      # 100 💵
MIN_BALL_BET_MILLI = 1000       # 1 💵
MAX_BALL_BET_MILLI = 100000      # 100 💵
BASKETBALL_ANIMATION_DELAY = 4
MIN_FOOTBALL_BET_MILLI = 2000       # 2 💵
MAX_FOOTBALL_BET_MILLI = 100000      # 100 💵
FOOTBALL_ANIMATION_DELAY = 4

SLOT_SYMBOLS = ['🍒', '🍋', '💎', '⭐️', '7️⃣']
SLOT_PAY_TABLE = {
    ('7️⃣', '7️⃣', '7️⃣'): 20,
    ('💎', '💎', '💎'): 10,
    ('⭐️', '⭐️', '⭐️'): 5,
    ('🍒', '🍒', '🍒'): 3,
}

BONUS_AMOUNT_MILLI = 100
MIN_WITHDRAW_MILLI = 200000
DAY_SECONDS = 24 * 60 * 60
DAILY_ROLE_BONUS_LIMIT = 5
RARITY_CHANCES = [
    ('common', 6900),
    ('rare', 2000),
    ('epic', 800),
    ('legendary', 200),
    ('secret', 1),  # секретная роль стала примерно в 100 раз реже
]

RARITY_LABELS = {
    'common': 'Обычная',
    'rare': 'Редкая',
    'epic': 'Эпическая',
    'legendary': 'Легендарная',
    'secret': 'Секретная',
}

ROLE_REWARDS_MILLI = {
    'common': 300,
    'rare': 500,
    'epic': 700,
    'legendary': 1500,
    'secret': 30000,
}

ROLE_EXP_REWARDS = {
    'common': 1,
    'rare': 3,
    'epic': 7,
    'legendary': 15,
    'secret': 100,
}

GROUP_EVENT_DURATION_SECONDS = 60 * 60
GROUP_EVENT_EXP_MULTIPLIER = 2
RANDOM_GROUP_EVENT_CHANCE = 3  # 3% шанс при получении роли в группе

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
    (5000, 1),   # 5 💵 — 1%
    (4000, 5),   # 4 💵 — 5%
    (3000, 6),   # 3 💵 — 6%
    (2000, 7),   # 2 💵 — 7%
    (1000, 81),  # 1 💵 — 70% + оставшиеся 11%, чтобы бонус всегда выпадал
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
PE_USDT_SYMBOL = '<tg-emoji emoji-id="5409048419211682843">💵</tg-emoji>'
PE_WAVE_HELLO = '<tg-emoji emoji-id="5472055112702629499">👋</tg-emoji>'
PE_ROLES_NEWS = '<tg-emoji emoji-id="5433982607035474385">📰</tg-emoji>'
PE_GAMEPAD = '<tg-emoji emoji-id="5467583879948803288">🎮</tg-emoji>'
PE_CLOCK_NEW = '<tg-emoji emoji-id="5382194935057372936">⏱</tg-emoji>'
PE_LVL_0 = '<tg-emoji emoji-id="5206712720350545928">0⃣</tg-emoji>'
PE_LVL_1 = '<tg-emoji emoji-id="5206421246689969742">1⃣</tg-emoji>'
PE_LVL_2 = '<tg-emoji emoji-id="5206429518796981791">2⃣</tg-emoji>'
PE_LVL_3 = '<tg-emoji emoji-id="5208507874946353980">3⃣</tg-emoji>'
PE_LVL_RUBY = '<tg-emoji emoji-id="5321011803075923151">⭐</tg-emoji>'
PE_LOCK_EVENT = '<tg-emoji emoji-id="5296369303661067030">🔒</tg-emoji>'
PE_EXP_CLAP = '<tg-emoji emoji-id="5258501105293205250">👏</tg-emoji>'
PE_DAILY_EXP = '<tg-emoji emoji-id="5258501105293205250">👏</tg-emoji>'

PE_PREFIX_TAG = '<tg-emoji emoji-id="5296348778012361146">🏷</tg-emoji>'
PE_EVENT_ALERT = '<tg-emoji emoji-id="5467928559664242360">❗️</tg-emoji>'
PE_CLAN_CASTLE = '<tg-emoji emoji-id="5467928559664242360">🏰</tg-emoji>'
PE_CLAN_OWNER = '<tg-emoji emoji-id="5321011803075923151">👑</tg-emoji>'
PE_CLAN_LOCK = '<tg-emoji emoji-id="5296369303661067030">🔒</tg-emoji>'
PE_CLAN_OPEN = '<tg-emoji emoji-id="5193191330079062069">🔓</tg-emoji>'
PE_CLAN_DOOR = '<tg-emoji emoji-id="5258084656674250503">🚪</tg-emoji>'
PE_CLAN_TROPHY = '<tg-emoji emoji-id="5409008750893734809">🏆</tg-emoji>'
PE_CLAN_GOLD = '<tg-emoji emoji-id="5280735858926822987">🥇</tg-emoji>'
PE_CLAN_SILVER = '<tg-emoji emoji-id="5283195573812340110">🥈</tg-emoji>'
PE_CLAN_BRONZE = '<tg-emoji emoji-id="5282750778409233531">🥉</tg-emoji>'
PE_CLAN_USER = '<tg-emoji emoji-id="5258011929993026890">👤</tg-emoji>'
PE_CLAN_OK = '<tg-emoji emoji-id="5260726538302660868">✅</tg-emoji>'
PE_CLAN_NO = '<tg-emoji emoji-id="5260342697075416641">❌</tg-emoji>'
PE_ROLE_DESC = '<tg-emoji emoji-id="5258328383183396223">📖</tg-emoji>'




PE_ARROW_RIGHT = '<tg-emoji emoji-id="5193191330079062069">➡️</tg-emoji>'

PE_TRANSFER_USDT = '<tg-emoji emoji-id="5201692367437974073">💵</tg-emoji>'
PE_TRANSFER_GIFT = '<tg-emoji emoji-id="5199749070830197566">🎁</tg-emoji>'
PE_TRANSFER_CHAT = '<tg-emoji emoji-id="5895457880710058528">💬</tg-emoji>'
PE_TRANSFER_USER = '<tg-emoji emoji-id="5373012449597335010">👤</tg-emoji>'
PE_BASKETBALL = '<tg-emoji emoji-id="5384088040677319401">🏀</tg-emoji>'
PE_FOOTBALL = '<tg-emoji emoji-id="5258169263235013408">⚽️</tg-emoji>'
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
    replacements = [('ℹ️', PE_INFO), ('❗️', PE_WARN), ('⚠️', PE_WARN), ('⭐️', PE_STAR), ('👤', PE_USER), ('✅', PE_OK), ('👥', PE_USERS), ('📣', PE_ANNOUNCE), ('✋', PE_STOP), ('⛔', PE_STOP), ('🚫', PE_STOP), ('💰', PE_WALLET), ('💸', PE_USDT_SYMBOL), ('📰', PE_ROLES_NEWS), ('👋', PE_WAVE_HELLO), ('⏱', PE_TOP_CLOCK), ('💵', PE_TOP_PROFIT), ('📰', PE_TOP_ROLES), ('👤', PE_TOP_USER), ('🥉', PE_TOP_BRONZE), ('🥈', PE_TOP_SILVER), ('🥇', PE_TOP_GOLD), ('🏆', PE_TOP_TROPHY), ('⏱', PE_DAILY_TOP_CLOCK), ('💵', PE_DAILY_TOP_PROFIT), ('📰', PE_DAILY_TOP_ROLES), ('👤', PE_DAILY_TOP_USER), ('🥉', PE_DAILY_TOP_BRONZE), ('🥈', PE_DAILY_TOP_SILVER), ('🥇', PE_DAILY_TOP_GOLD), ('🏆', PE_DAILY_TOP_TROPHY), ('❌', PE_CRYPTO_NO), ('✅', PE_CRYPTO_OK), ('🔴', PE_CRYPTO_RED), ('🟢', PE_CRYPTO_GREEN), ('📰', PE_CRYPTO_HISTORY), ('📜', PE_CRYPTO_HISTORY), ('💼', PE_CRYPTO_PORTFOLIO), ('📊', PE_CRYPTO_CHART), ('💵', PE_USDT_SYMBOL), ('💵', PE_TRANSFER_USDT), ('🎁', PE_TRANSFER_GIFT), ('💬', PE_TRANSFER_CHAT), ('👤', PE_TRANSFER_USER), ('➕', PE_PLUS), ('📈', PE_CHART), ('📊', PE_CHART), ('💬', PE_CHAT), ('❗', PE_WARN), ('❌', PE_CROSS), ('🏘', PE_HOME), ('🏠', PE_HOME), ('⭐', PE_STAR), ('👁', PE_EYE), ('🔖', PE_UID), ('🆔', PE_UID), ('🏆', PE_TROPHY), ('🥇', PE_TOP1), ('🥈', PE_TOP2), ('🥉', PE_TOP3), ('🔎', PE_SEARCH), ('0⃣', PE_LVL_0), ('1⃣', PE_LVL_1), ('2⃣', PE_LVL_2), ('3⃣', PE_LVL_3), ('🔒', PE_LOCK_EVENT), ('👏', PE_EXP_CLAP), ('🏷', PE_PREFIX_TAG), ('📖', PE_ROLE_DESC), ('❌', PE_CLAN_NO), ('✅', PE_CLAN_OK), ('👤', PE_CLAN_USER), ('🥉', PE_CLAN_BRONZE), ('🥈', PE_CLAN_SILVER), ('🥇', PE_CLAN_GOLD), ('🏆', PE_CLAN_TROPHY), ('🚪', PE_CLAN_DOOR), ('🏰', PE_CLAN_CASTLE), ('👑', PE_CLAN_OWNER), ('🔒', PE_CLAN_LOCK), ('🔓', PE_CLAN_OPEN), ('❗️', PE_EVENT_ALERT), ('⭐', PE_LVL_RUBY), ('➡️', PE_ARROW_RIGHT), ('⏱', PE_CLOCK_NEW), ('⏲', PE_TIMER), ('⏳', PE_TIMER), ('1️⃣', PE_NUM_1), ('2️⃣', PE_NUM_2), ('3️⃣', PE_NUM_3), ('4️⃣', PE_NUM_4), ('5️⃣', PE_NUM_5), ('6️⃣', PE_NUM_6), ('7️⃣', PE_NUM_7), ('8️⃣', PE_NUM_8), ('9️⃣', PE_NUM_9), ('0️⃣', PE_NUM_0), ('🩶', PE_RARITY_COMMON), ('💚', PE_RARITY_RARE), ('🩷', PE_RARITY_EPIC), ('💛', PE_RARITY_LEGENDARY), ('🖤', PE_RARITY_SECRET), ('⭐️', PE_SLOT_STAR), ('🍒', PE_SLOT_CHERRY), ('💎', PE_SLOT_DIAMOND), ('🎭', PE_MASKS), ('⚽️', PE_FOOTBALL), ('🎮', PE_GAMEPAD), ('🏀', PE_BASKETBALL), ('🎰', PE_CASINO), ('🎲', PE_DICE), ('🪙', PE_COIN), ('💲', PE_DOLLAR), ('✖️', PE_X2), ('✖', PE_X2), ('✍️', PE_LOADING), ('✍', PE_LOADING), ('⚙', PE_INFO), ('🔢', PE_INFO), ('📋', PE_CHAT), ('📄', PE_CHAT), ('📛', PE_USER), ('🗄', PE_INFO), ('🗑', PE_CROSS), ('🙈', PE_EYE), ('➖', PE_CROSS), ('⬅', PE_HOME), ('🎁', PE_STAR)]
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
        return f'{whole} 💵'
    return f'{whole}.{frac:03d}'.rstrip('0') + ' 💵'



def money_balance(milli: int) -> str:
    try:
        whole = int(milli or 0) // 1000
        return f"{whole} 💵"
    except Exception:
        return "0 💵"



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
    total = sum(weight for _, weight in RARITY_CHANCES)
    pick = random.randint(1, total)
    current = 0

    for rarity, weight in RARITY_CHANCES:
        current += weight
        if pick <= current:
            return rarity

    return 'common'



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

    if 'banned' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN banned INTEGER NOT NULL DEFAULT 0")

    if 'ban_reason' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN ban_reason TEXT")

    if 'banned_until' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN banned_until INTEGER NOT NULL DEFAULT 0")

    if 'banned_by' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN banned_by INTEGER")

    if 'banned_at' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN banned_at INTEGER NOT NULL DEFAULT 0")

    if 'casino_last_spin_at' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN casino_last_spin_at INTEGER NOT NULL DEFAULT 0")

    if 'coin_last_result' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN coin_last_result TEXT")

    if 'coin_streak' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN coin_streak INTEGER NOT NULL DEFAULT 0")

    if 'luck_booster_until' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN luck_booster_until INTEGER NOT NULL DEFAULT 0")

    if 'secret_case_rewards' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN secret_case_rewards INTEGER NOT NULL DEFAULT 0")

    if 'last_case_open_at' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN last_case_open_at INTEGER NOT NULL DEFAULT 0")

    if 'case_discount_milli' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN case_discount_milli INTEGER NOT NULL DEFAULT 0")

    if 'prefix' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN prefix TEXT")

    if 'exp' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN exp INTEGER NOT NULL DEFAULT 0")

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

def random_admin_role_rarity() -> str:
    chances = [
        ("common", 70),
        ("rare", 20),
        ("epic", 8),
        ("legendary", 2),
    ]
    total = sum(weight for _, weight in chances)
    pick = random.randint(1, total)
    current = 0
    for rarity, weight in chances:
        current += weight
        if pick <= current:
            return rarity
    return "common"


def add_phrase_db(text: str) -> bool:
    phrase, rarity = parse_phrase_input(text)

    # Если админ просто написал текст или строка из .txt без "редкость | текст",
    # редкость выбирается рандомно, но secret никогда не выбирается автоматически.
    if "|" not in (text or ""):
        rarity = random_admin_role_rarity()

    if not phrase:
        return False

    with db() as conn:
        try:
            conn.execute('INSERT INTO phrases (text, rarity, created_at) VALUES (?, ?, ?)', (phrase, rarity, ts()))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False



def has_luck_booster(user_id: int) -> bool:
    try:
        with db() as conn:
            ensure_ban_columns(conn)
            conn.commit()
            user_cols = columns(conn, "users")
            if "luck_booster_until" not in user_cols:
                return False
            row = conn.execute("SELECT luck_booster_until FROM users WHERE user_id=?", (user_id,)).fetchone()
            return bool(row and int(row[0] or 0) > ts())
    except Exception:
        return False


def luck_booster_left(user_id: int) -> int:
    try:
        with db() as conn:
            user_cols = columns(conn, "users")
            if "luck_booster_until" not in user_cols:
                return 0
            row = conn.execute("SELECT luck_booster_until FROM users WHERE user_id=?", (user_id,)).fetchone()
            if not row:
                return 0
            return max(0, int(row[0] or 0) - ts())
    except Exception:
        return 0


def activate_luck_booster(user_id: int) -> None:
    until = ts() + LUCK_BOOSTER_SECONDS
    try:
        with db() as conn:
            user_cols = columns(conn, "users")
            if "luck_booster_until" not in user_cols:
                return
            conn.execute("UPDATE users SET luck_booster_until=? WHERE user_id=?", (until, user_id))
            conn.commit()
    except Exception:
        pass


def add_secret_case_reward(user_id: int) -> None:
    try:
        with db() as conn:
            user_cols = columns(conn, "users")
            if "secret_case_rewards" not in user_cols:
                return
            conn.execute("UPDATE users SET secret_case_rewards=secret_case_rewards+1 WHERE user_id=?", (user_id,))
            conn.commit()
    except Exception:
        pass


def booster_time_text(seconds: int) -> str:
    minutes = max(0, seconds) // 60
    secs = max(0, seconds) % 60
    return f"{minutes} мин. {secs} сек."


def random_phrase(user_id: int | None = None) -> tuple[str, str] | None:
    # Бустер удачи: шанс редкой и выше x2 на 30 минут.
    if user_id and has_luck_booster(user_id):
        chances = {
            "common": 6900,
            "rare": 4000,
            "epic": 1600,
            "legendary": 400,
            "secret": 2,  # бустер x2, но секретная все равно очень редкая
        }
        with db() as conn:
            available = conn.execute("SELECT rarity, COUNT(*) FROM phrases GROUP BY rarity").fetchall()
            available_map = {rarity: count for rarity, count in available if count}

            weighted = [(rarity, weight) for rarity, weight in chances.items() if available_map.get(rarity, 0) > 0]

            if weighted:
                total = sum(weight for _, weight in weighted)
                pick = random.randint(1, total)
                current = 0
                selected = weighted[-1][0]

                for rarity, weight in weighted:
                    current += weight
                    if pick <= current:
                        selected = rarity
                        break

                rows = conn.execute("SELECT text, rarity FROM phrases WHERE rarity=?", (selected,)).fetchall()
            else:
                rows = conn.execute("SELECT text, rarity FROM phrases").fetchall()

        if not rows:
            return None

        phrase_text, rarity = random.choice(rows)
        return phrase_text, rarity or "common"

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

def set_uid(user_id: int, new_uid: str) -> tuple[bool, str]:
    new_uid = str(new_uid).strip()

    if not new_uid:
        return False, "UID не может быть пустым."

    if not new_uid.isdigit():
        return False, "UID может состоять только из цифр."

    with db() as conn:
        if not conn.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,)).fetchone():
            return False, "Пользователь не найден."

        try:
            conn.execute("UPDATE users SET uid=? WHERE user_id=?", (new_uid, user_id))
            conn.commit()
        except sqlite3.IntegrityError:
            return False, "Такой UID уже занят."

    return True, f"UID изменен на <code>{html.escape(new_uid)}</code>."


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



def ensure_ban_columns(conn) -> None:
    user_cols = columns(conn, "users")

    if "banned" not in user_cols:
        conn.execute("ALTER TABLE users ADD COLUMN banned INTEGER NOT NULL DEFAULT 0")
    if "ban_reason" not in user_cols:
        conn.execute("ALTER TABLE users ADD COLUMN ban_reason TEXT")
    if "banned_until" not in user_cols:
        conn.execute("ALTER TABLE users ADD COLUMN banned_until INTEGER NOT NULL DEFAULT 0")
    if "banned_by" not in user_cols:
        conn.execute("ALTER TABLE users ADD COLUMN banned_by INTEGER")
    if "banned_at" not in user_cols:
        conn.execute("ALTER TABLE users ADD COLUMN banned_at INTEGER NOT NULL DEFAULT 0")


def get_user_ban_status_direct(user_id: int) -> tuple[bool, str, int]:
    """
    Надежная проверка бана напрямую из SQLite.
    Не использует get_user(), потому что у старых версий tuple может иметь другую структуру.
    Возвращает: banned, reason, banned_until.
    """
    try:
        with db() as conn:
            ensure_ban_columns(conn)
            conn.commit()
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
        ensure_ban_columns(conn)

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
                (reason or "не указана", int(banned_until or 0), admin_id, ts(), user_id),
            )
            conn.commit()

            return True, (
                "Пользователь забанен.\n"
                f"Время: <b>{html.escape(ban_time_text(int(banned_until or 0)))}</b>\n"
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

def find_user_for_transfer(target: str):
    target = (target or "").strip()

    if not target:
        return None

    with db() as conn:
        if target.isdigit():
            return conn.execute(
                "SELECT user_id, username, first_name, uid, balance_milli, hidden FROM users WHERE user_id=? OR uid=?",
                (int(target), target),
            ).fetchone()

        username = target[1:] if target.startswith("@") else target

        return conn.execute(
            "SELECT user_id, username, first_name, uid, balance_milli, hidden FROM users WHERE lower(username)=lower(?)",
            (username,),
        ).fetchone()


def transfer_money(sender_id: int, target: str, amount_milli: int, comment: str = "") -> tuple[bool, str, int | None]:
    if amount_milli <= 0:
        return False, "Сумма должна быть больше 0.", None

    if sender_id and is_banned_user(sender_id):
        return False, "Вы забанены у бота.", None

    recipient = find_user_for_transfer(target)

    if not recipient:
        return False, "Получатель не найден. Он должен быть зарегистрирован в боте.", None

    recipient_id, recipient_username, recipient_first_name, recipient_uid, recipient_balance, hidden = recipient

    if hidden:
        return False, "Получатель не найден.", None

    if int(recipient_id) == int(sender_id):
        return False, "Нельзя переводить самому себе.", None

    sender = get_user(sender_id)

    if not sender:
        return False, "Профиль отправителя не найден. Напиши /start.", None

    sender_balance = int(sender[4])

    if sender_balance < amount_milli:
        return False, f"Недостаточно средств. Ваш баланс: <b>{money(sender_balance)}</b>", None

    ok, msg = take_balance(sender_id, amount_milli)

    if not ok:
        return False, msg, None

    add_balance(recipient_id, amount_milli)

    return True, "Перевод выполнен.", int(recipient_id)


def transfer_usage_text() -> str:
    return (
        '💵 <b>Передача денег</b>\n\n'
        '👤 Получатель: ID или @username\n'
        '💬 Комментарий можно оставить после суммы.\n\n'
        '<code>/pay USER_ID сумма комментарий</code>\n'
        '<code>/pay @username сумма комментарий</code>\n\n'
        'Пример: <code>/pay 123456789 1 подарок</code>'
    )



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
        f"💰 Баланс: <b>{money_balance(balance)}</b>\n"
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
    lines = ['🏆 <b>Топ 3 по 💵</b>\n']
    for i, (user_id, username, first_name, uid, balance) in enumerate(rows):
        name = f'@{username}' if username else first_name or f'ID {user_id}'
        lines.append(f'{medals[i]} {html.escape(name)} | UID: <code>{html.escape(str(uid))}</code> | <b>{money_balance(balance)}</b>')
    return '\n'.join(lines)

def profile_text(user_id: int) -> str:
    row = get_user_full(user_id)

    if not row:
        return 'Профиль не найден. Напиши /start.'

    _, username, first_name, uid, balance, openings, _, hidden, _, created_at, games_played, turnover_milli, prefix = row
    level = level_info_by_openings(openings)
    display_name = f'@{username}' if username else (first_name or f'Игрок #{uid}')

    status = 'Активен'
    banned, ban_reason, banned_until = get_user_ban_status_direct(user_id)
    if hidden:
        status = 'Скрыт'
    if banned:
        status = f'Бан: {ban_time_text(int(banned_until or 0))}'

    extra = ''
    if prefix:
        extra += f'\n🏷 Префикс — <b>{html.escape(prefix)}</b>'

    try:
        discount = get_case_discount(user_id)
        if discount > 0:
            extra += f'\n💸 Скидка на кейс — <b>{money(discount)}</b>'
    except Exception:
        pass

    try:
        booster = luck_booster_left(user_id)
        if booster > 0:
            extra += f'\n⏱ Бустер удачи — <b>{booster_time_text(booster)}</b>'
    except Exception:
        pass

    return (
        f'<b>#{html.escape(str(uid))} {html.escape(display_name)}</b>\n\n'
        f'💵 Баланс — <b>{money_balance(balance)}</b>\n\n'
        f'Ваш прогресс — <b>{level["percent"]}%</b>\n'
        f'{level["current"]["emoji"]} {level["current"]["name"]} ➡️ {level["next"]["emoji"]} {level["next"]["name"]}\n\n'
        f'📰 Открыто ролей — <b>{int(openings or 0)}</b>\n'
        f'🎮 Сыграно — <b>{int(games_played or 0)} ставок</b>\n'
        f'⏱ Аккаунту — <b>{account_age_text(int(created_at or 0))}</b>\n'
        f'Статус — <b>{html.escape(status)}</b>'
        f'{extra}'
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
            InlineKeyboardButton('💸 Вывод 💵', callback_data='withdraw')
        ])
        buttons.append([InlineKeyboardButton('💵 Передача денег', callback_data='transfer_money')])
        buttons.append([InlineKeyboardButton('🎁 Промокод', callback_data='promo_activate')])

    buttons.append([InlineKeyboardButton('🎰 Казино', callback_data='casino')])
    buttons.append([InlineKeyboardButton('🏆 Топ 3', callback_data='top3')])

    return InlineKeyboardMarkup(buttons)


def reply_main_menu(admin=False, group=False):
    if group:
        rows = []
    else:
        rows = [
            ['🎭 Кто я', '🎮 Играть'],
            ['👤 Профиль', '🏆 Топ 3'],
            ['💵 Передача денег', '🎁 Промокод'],
            ['🏠 Главное меню'],
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
            InlineKeyboardButton('💸 Вывод 💵', callback_data='withdraw')
        ])
        buttons.append([InlineKeyboardButton('💵 Передача денег', callback_data='transfer_money')])
        buttons.append([InlineKeyboardButton('🎁 Промокод', callback_data='promo_activate')])
        buttons.append([InlineKeyboardButton('🎰 Казино', callback_data='casino')])

    return InlineKeyboardMarkup(buttons) if buttons else None


def admin_panel_text() -> str:
    return (
        "<b>Админ-панель</b>\n\n"
        "1️⃣ <code>/add текст</code> — добавить фразу\n"
        "2️⃣ <code>/list</code> — последние фразы\n"
        "3️⃣ <code>/delete ID</code> — удалить фразу\n"
        "4️⃣ <code>/give USER_ID SUM причина</code> — выдать 💵\n"
        "5️⃣ <code>/take USER_ID SUM</code> — забрать 💵\n"
        "6️⃣ <code>/setuid USER_ID UID</code> — выдать кастом UID, только цифры\n"
        "7️⃣ <code>/hide USER_ID</code> — скрыть пользователя\n"
        "8️⃣ <code>/unhide USER_ID</code> — раскрыть пользователя\n"
        "9️⃣ <code>/ban USER_ID TIME причина</code> — забанить пользователя\n"
        "1️⃣0️⃣ <code>/unban USER_ID</code> — разбанить пользователя\n<code>/banlist</code> — список забаненных\n<code>/search USER_ID</code> — поиск пользователя по ID\n\n"
        "<b>Дополнительно:</b>\n"
        "<code>/promo_create CODE SUM LIMIT</code> — создать промокод\n"
        "<code>/promos</code> — список промокодов\n"
        "<code>/adminstats</code> — статистика\n<code>/startchat</code> — запустить групповое событие на 1 час\n<code>/clearmoney</code> — очистить деньги у всех игроков\n<code>/expgive USER_ID EXP</code> — выдать EXP игроку\n<code>/exptake USER_ID EXP</code> — забрать EXP у игрока\n<code>/allclans</code> — список всех кланов\n<code>/clandelete ID/TAG</code> — удалить клан\n<code>/resetfraz</code> — удалить все роли/фразы\n<code>/add</code> — добавить роль с фото\n<code>/clearmoney</code> — очистить деньги у всех игроков\n"
        "<code>/groups</code> — группы с ботом\n"
        "<code>/broadcast текст</code> — уведомление всем\n"
    )


def admin_menu():
    return InlineKeyboardMarkup([[InlineKeyboardButton('➕ Добавить фразу', callback_data='add_phrase')], [InlineKeyboardButton('🗑 Удалить фразу', callback_data='delete_phrase_btn')], [InlineKeyboardButton('📋 Последние фразы', callback_data='last_phrases')], [InlineKeyboardButton('🔢 Количество фраз', callback_data='phrase_count')], [InlineKeyboardButton('📣 Уведомление в бот', callback_data='broadcast')], [InlineKeyboardButton('📊 Статистика', callback_data='admin_stats')], [InlineKeyboardButton('🎁 Создать промокод', callback_data='promo_create')], [InlineKeyboardButton('📋 Промокоды', callback_data='promo_list')], [InlineKeyboardButton('💰 Выдать 💵', callback_data='give_usdt')], [InlineKeyboardButton('➖ Забрать 💵', callback_data='take_usdt')], [InlineKeyboardButton('🆔 Выдать кастом UID', callback_data='custom_uid')], [InlineKeyboardButton('🙈 Скрыть пользователя', callback_data='hide_user')], [InlineKeyboardButton('👁 Раскрыть пользователя', callback_data='unhide_user')], [InlineKeyboardButton('👥 Группы с ботом', callback_data='groups')], [InlineKeyboardButton('⬅️ Назад', callback_data='back')]])

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
        [InlineKeyboardButton('🎰 Слоты 0.1 💵', callback_data='slots_bet:0.1')],
        [InlineKeyboardButton('🎰 Слоты 0.5 💵', callback_data='slots_bet:0.5')],
        [InlineKeyboardButton('🎰 Слоты 1 💵', callback_data='slots_bet:1')],
        [InlineKeyboardButton('🎰 Слоты 5 💵', callback_data='slots_bet:5')],
        [InlineKeyboardButton('🪙 Орел 1 💵', callback_data='coin_bet:orel:1')],
        [InlineKeyboardButton('🪙 Решка 1 💵', callback_data='coin_bet:reshka:1')],
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
    combo = ' '.join(symbols)

    if win_milli > 0:
        headline = f'Выигрыш <b>{money(win_milli)}</b> в игре 🎰'
    else:
        headline = f'Проигрыш <b>{money(bet_milli)}</b> в игре 🎰'

    return (
        f'{mention(user)}\n'
        f'{headline}\n'
        f'Комбинация: <b>{combo}</b>\n\n'
        f'💵 Баланс <b>{money_balance(balance_after)}</b>'
    )



async def show_casino(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    text = (
        "🎰 <b>Казино</b>\n\n"
        "<b>Команды</b>\n"
        "🎰 <code>/slots 1</code> — слоты\n"
        "🪙 <code>/coin орел 1</code> — орел / решка\n"
        "🏀 <code>/ball 1</code> — баскетбол\n"
        "⚽️ <code>/football 1</code> — футбол\n"
        "🎁 <code>/case open</code> — кейс\n\n"
        f"⏲ Кулдаун: <b>{CASINO_COOLDOWN_SECONDS} сек.</b>"
    )

    await send_clean_group_result(update, context, text)



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
        slot_result_text(user, bet_milli, symbols, multiplier, win_milli, balance_after)
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


def get_coin_streak(user_id: int) -> tuple[str | None, int]:
    try:
        with db() as conn:
            user_cols = columns(conn, "users")

            if "coin_last_result" not in user_cols or "coin_streak" not in user_cols:
                return None, 0

            row = conn.execute(
                "SELECT coin_last_result, coin_streak FROM users WHERE user_id=?",
                (user_id,),
            ).fetchone()

            if not row:
                return None, 0

            return row[0], int(row[1] or 0)
    except Exception:
        return None, 0


def set_coin_streak(user_id: int, result: str) -> None:
    try:
        last_result, streak = get_coin_streak(user_id)

        if last_result == result:
            streak += 1
        else:
            streak = 1

        with db() as conn:
            user_cols = columns(conn, "users")

            if "coin_last_result" not in user_cols or "coin_streak" not in user_cols:
                return

            conn.execute(
                "UPDATE users SET coin_last_result=?, coin_streak=? WHERE user_id=?",
                (result, streak, user_id),
            )
            conn.commit()
    except Exception:
        pass


def roll_coin(user_id: int | None = None) -> str:
    """
    Честный шанс 50/50 через SystemRandom.
    Если одна сторона выпала 5 раз подряд, следующая будет противоположной.
    """
    rng = random.SystemRandom()

    if user_id is not None:
        last_result, streak = get_coin_streak(user_id)

        if last_result in ("orel", "reshka") and streak >= 5:
            result = "reshka" if last_result == "orel" else "orel"
            set_coin_streak(user_id, result)
            return result

    result = rng.choice(["orel", "reshka"])

    if user_id is not None:
        set_coin_streak(user_id, result)

    return result



def coin_result_text(user, bet_milli: int, choice: str, result: str, win_milli: int, balance_after: int) -> str:
    won = choice == result

    if won:
        headline = f'Выигрыш <b>{money(win_milli)}</b> в игре 🪙'
    else:
        headline = f'Проигрыш <b>{money(bet_milli)}</b> в игре 🪙'

    return (
        f'{mention(user)}\n'
        f'{headline}\n'
        f'Ваш выбор: <b>{coin_side_label(choice)}</b>\n'
        f'Выпало: <b>{coin_side_label(result)}</b>\n\n'
        f'💵 Баланс <b>{money_balance(balance_after)}</b>'
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

    result = roll_coin(user.id)
    win_milli = bet_milli * 2 if side == result else 0

    if win_milli > 0:
        add_balance(user.id, win_milli)

    set_casino_last_spin(user.id)

    updated = get_user(user.id)
    balance_after = int(updated[4]) if updated else 0

    await send_clean_group_result(
        update,
        context,
        coin_result_text(user, bet_milli, side, result, win_milli, balance_after)
    )



def log_time_text() -> str:
    return time.strftime("%d.%m.%Y %H:%M:%S", time.localtime(ts()))


async def send_role_log(context: ContextTypes.DEFAULT_TYPE, user, phrase: str, rarity_label: str, reward_milli: int):
    username = f"@{user.username}" if getattr(user, "username", None) else "нет"

    text = (
        "🎭 <b>Получение карточки</b>\n"
        f"⏲ Время: <b>{html.escape(log_time_text())}</b>\n"
        f"👤 Username: <b>{html.escape(username)}</b>\n"
        f"🆔 ID: <code>{user.id}</code>\n"
        f"🎴 Карточка: <b>{html.escape(phrase)}</b>\n"
        f"⭐ Редкость: <b>{html.escape(rarity_label)}</b>\n"
        f"💰 Начислено: <b>+{money(reward_milli)}</b>"
    )

    try:
        await context.bot.send_message(
            chat_id=ROLE_LOG_CHAT_ID,
            text=pe(text),
            parse_mode="HTML"
        )
    except Exception:
        pass


async def notify_ban_private(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    banned, reason, banned_until = get_user_ban_status_direct(user_id)

    if not banned:
        return

    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=pe(
                "⛔ <b>Вы забанены у бота.</b>\n"
                f"Причина: <b>{html.escape(reason or 'не указана')}</b>\n"
                f"Осталось: <b>{html.escape(ban_time_text(int(banned_until or 0)))}</b>"
            ),
            parse_mode="HTML"
        )
    except Exception:
        pass


async def handle_banned_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    chat = update.effective_chat

    if not user or not is_banned_user(user.id):
        return False

    banned, reason, banned_until = get_user_ban_status_direct(user.id)

    if chat and is_group(chat):
        await notify_ban_private(context, user.id)
        return True

    try:
        await context.bot.send_message(
            chat_id=chat.id if chat else user.id,
            text=pe(
                "⛔ <b>Вы забанены у бота.</b>\n"
                f"Причина: <b>{html.escape(reason or 'не указана')}</b>\n"
                f"Осталось: <b>{html.escape(ban_time_text(int(banned_until or 0)))}</b>"
            ),
            parse_mode="HTML"
        )
    except Exception:
        pass

    return True


def banlist_text() -> str:
    with db() as conn:
        ensure_ban_columns(conn)
        rows = conn.execute(
            """
            SELECT user_id, username, first_name, ban_reason, banned_until
            FROM users
            WHERE banned=1
            ORDER BY banned_at DESC
            """
        ).fetchall()

    if not rows:
        return "🚫 <b>Бан-лист пуст.</b>"

    lines = [f"🚫 <b>В бане: {len(rows)}</b>\n"]

    for user_id, username, first_name, reason, banned_until in rows[:50]:
        name = f"@{username}" if username else (first_name or "нет username")
        lines.append(
            f"👤 {html.escape(str(name))} | <code>{user_id}</code>\n"
            f"Причина: <b>{html.escape(reason or 'не указана')}</b>\n"
            f"Осталось: <b>{html.escape(ban_time_text(int(banned_until or 0)))}</b>"
        )

    if len(rows) > 50:
        lines.append(f"\nПоказано 50 из {len(rows)}.")

    return "\n\n".join(lines)


async def banlist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    await update.message.reply_text(pe(banlist_text()), parse_mode='HTML')


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
    phrase_data = random_phrase(user.id)
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
        "🎭 <b>Карточка получена</b>\n\n"
        f"{mention(user)} — <b>{html.escape(phrase)}</b>\n"
        f"Редкость: <b>{html.escape(rarity_label)}</b>\n"
        f"Добавлено: <b>+{money(reward_milli)}</b>",
        reply_markup=role_menu(group=is_group(chat))
    )

    await send_role_log(context, user, phrase, rarity_label, reward_milli)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await open_main_screen(update, context)



async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await open_main_screen(update, context)



async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_role(update, context)

async def trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    raw_text = update.message.text.strip()
    txt = raw_text.lower()
    lower_text = txt

    if context.user_data.get('waiting_promo_activate'):
        context.user_data['waiting_promo_activate'] = False
        ok, msg = activate_promo_code(update.effective_user.id, raw_text)
        await update.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')
        return

    if txt in ('🏠 главное меню', 'главное меню'):
        await open_main_screen(update, context)
        return

    if txt in TRIGGERS or txt in ('кто я?', '🎭 кто я', 'я', 'кто', 'кто я'):
        await send_role(update, context)
        return

    if txt in ('играть', '🎮 играть', 'казино', '🎰 казино'):
        await show_casino(update, context)
        return

    if txt in ('профиль', '👤 профиль'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('Профиль доступен только в личке с ботом.'), parse_mode='HTML')
            return
        await send_result(update, context, profile_text(update.effective_user.id), reply_markup=profile_actions_menu())
        return

    if txt in ('топ 3', '🏆 топ 3', 'топ'):
        await send_clean_group_result(update, context, top_text())
        return

    if txt in ('передача денег', '💵 передача денег'):
        await send_result(update, context, transfer_usage_text())
        return

    if txt in ('промокод', '🎁 промокод'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('Промокоды доступны только в личке с ботом.'), parse_mode='HTML')
            return
        await update.message.reply_text(pe('🎁 Введите промокод одним сообщением:'), parse_mode='HTML')
        context.user_data['waiting_promo_activate'] = True
        return



async def clear_money_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    with db() as conn:
        cur = conn.execute("UPDATE users SET balance_milli=0")
        conn.commit()
        count = cur.rowcount if cur.rowcount is not None else 0

    await update.message.reply_text(
        pe(f'💵 <b>Баланс очищен у всех игроков.</b>\nИгроков обновлено: <b>{count}</b>'),
        parse_mode='HTML'
    )


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
        await update.message.reply_text(pe('Напиши так:\n/add Шрек\n\nРедкость выберется рандомно. Секретная сама не выпадет.\nМожно указать вручную:\n/add secret | Секретная роль'), parse_mode='HTML')
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
    uid = context.args[1].strip()

    if not uid.isdigit():
        await update.message.reply_text(pe('UID должен состоять только из цифр.'), parse_mode='HTML')
        return

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

    if await handle_banned_action(update, context):
        return
    await send_result(update, context, profile_text(update.effective_user.id))

async def top_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)

    if await handle_banned_action(update, context):
        return
    await send_clean_group_result(update, context, top_text())

async def search_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)

    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return
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

    if await handle_banned_action(update, context):
        return

    code = update.message.text.strip()

    ok, msg = activate_promo_code(update.effective_user.id, code)
    await update.message.reply_text(
        pe(('✅ ' if ok else '❌ ') + msg),
        parse_mode='HTML'
    )

    return ConversationHandler.END


async def promo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)

    if await handle_banned_action(update, context):
        return

    if not context.args:
        await send_result(update, context, "Напиши промокод:\n<code>/promo CODE</code>")
        return

    ok, msg = activate_promo_code(update.effective_user.id, context.args[0])
    await send_result(update, context, ("✅ " if ok else "❌ ") + msg)


def get_case_discount(user_id: int) -> int:
    try:
        with db() as conn:
            user_cols = columns(conn, "users")

            if "case_discount_milli" not in user_cols:
                return 0

            row = conn.execute(
                "SELECT case_discount_milli FROM users WHERE user_id=?",
                (user_id,),
            ).fetchone()

            if not row:
                return 0

            return max(0, int(row[0] or 0))
    except Exception:
        return 0


def set_case_discount(user_id: int, amount_milli: int) -> None:
    try:
        with db() as conn:
            user_cols = columns(conn, "users")

            if "case_discount_milli" not in user_cols:
                return

            conn.execute(
                "UPDATE users SET case_discount_milli=? WHERE user_id=?",
                (max(0, int(amount_milli)), user_id),
            )
            conn.commit()
    except Exception:
        pass


def set_user_prefix(user_id: int, prefix: str) -> None:
    try:
        with db() as conn:
            user_cols = columns(conn, "users")

            if "prefix" not in user_cols:
                return

            conn.execute(
                "UPDATE users SET prefix=? WHERE user_id=?",
                (prefix, user_id),
            )
            conn.commit()
    except Exception:
        pass


def get_user_prefix(user_id: int) -> str | None:
    try:
        with db() as conn:
            user_cols = columns(conn, "users")

            if "prefix" not in user_cols:
                return None

            row = conn.execute(
                "SELECT prefix FROM users WHERE user_id=?",
                (user_id,),
            ).fetchone()

            if not row:
                return None

            return row[0]
    except Exception:
        return None


def get_last_case_open_at(user_id: int) -> int:
    try:
        with db() as conn:
            user_cols = columns(conn, "users")

            if "last_case_open_at" not in user_cols:
                return 0

            row = conn.execute(
                "SELECT last_case_open_at FROM users WHERE user_id=?",
                (user_id,),
            ).fetchone()

            if not row:
                return 0

            return int(row[0] or 0)
    except Exception:
        return 0


def set_last_case_open_at(user_id: int) -> None:
    try:
        with db() as conn:
            user_cols = columns(conn, "users")

            if "last_case_open_at" not in user_cols:
                return

            conn.execute(
                "UPDATE users SET last_case_open_at=? WHERE user_id=?",
                (ts(), user_id),
            )
            conn.commit()
    except Exception:
        pass


def open_case(user_id: int) -> tuple[bool, str]:
    row = get_user(user_id)

    if not row:
        return False, "Профиль не найден. Напиши /start."

    last_case = get_last_case_open_at(user_id)
    cooldown_left = CASE_COOLDOWN_SECONDS - (ts() - last_case)

    if cooldown_left > 0:
        return False, f"Кейс можно открыть через <b>{cooldown_left} сек.</b>"

    balance = int(row[4])
    discount = min(get_case_discount(user_id), CASE_PRICE_MILLI)
    price = max(0, CASE_PRICE_MILLI - discount)

    if balance < price:
        return False, (
            "Недостаточно средств для открытия кейса.\n"
            f"Цена кейса: <b>{money(price)}</b>\n"
            f"Ваша скидка: <b>{money(discount)}</b>\n"
            f"Ваш баланс: <b>{money(balance)}</b>"
        )

    if price > 0:
        ok, msg = take_balance(user_id, price)

        if not ok:
            return False, msg

    set_last_case_open_at(user_id)

    # Скидка применяется только на один следующий кейс.
    if discount > 0:
        set_case_discount(user_id, 0)

    # Секретная награда: очень маленький шанс 1 из 1000.
    if random.randint(1, 1000) <= CASE_SECRET_REWARD_CHANCE:
        secret_amount = 100000
        add_balance(user_id, secret_amount)
        add_secret_case_reward(user_id)
        return True, (
            "🎁 <b>Кейс открыт!</b>\n\n"
            f"💸 Списано: <b>{money(price)}</b>\n"
            "🖤 <b>СЕКРЕТНАЯ НАГРАДА!</b>\n"
            f"💰 Получено: <b>+{money(secret_amount)}</b>"
        )

    roll = random.randint(1, 100)

    # Самый высокий шанс — ничего.
    if roll <= 55:
        return True, (
            "🎁 <b>Кейс открыт!</b>\n\n"
            f"💸 Списано: <b>{money(price)}</b>\n"
            "❌ Выпало: <b>Ничего</b>"
        )

    # Рандомный префикс вместо бонуса 💵.
    if roll <= 80:
        prefix = random.choice(CASE_PREFIXES)
        set_user_prefix(user_id, prefix)
        return True, (
            "🎁 <b>Кейс открыт!</b>\n\n"
            f"💸 Списано: <b>{money(price)}</b>\n"
            "🔖 Награда: <b>Рандомный префикс</b>\n"
            f"Префикс: <b>{html.escape(prefix)}</b>"
        )

    # Скидка на следующий кейс.
    if roll <= 92:
        set_case_discount(user_id, CASE_DISCOUNT_MILLI)
        next_price = max(0, CASE_PRICE_MILLI - CASE_DISCOUNT_MILLI)
        return True, (
            "🎁 <b>Кейс открыт!</b>\n\n"
            f"💸 Списано: <b>{money(price)}</b>\n"
            "🏷 Награда: <b>Скидка на следующий кейс</b>\n"
            f"Скидка: <b>{money(CASE_DISCOUNT_MILLI)}</b>\n"
            f"Следующий кейс будет стоить: <b>{money(next_price)}</b>"
        )

    activate_luck_booster(user_id)
    return True, (
        "🎁 <b>Кейс открыт!</b>\n\n"
        f"💸 Списано: <b>{money(price)}</b>\n"
        "📈 Награда: <b>Бустер удачи</b>\n"
        "⏲ Длительность: <b>30 минут</b>\n"
        "Шанс редкой, эпической, легендарной и секретной роли повышен x2."
    )


async def pay_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    if len(context.args) < 2:
        await send_result(update, context, transfer_usage_text())
        return

    target = context.args[0]
    amount = parse_money(context.args[1])

    if amount is None or amount <= 0:
        await send_result(update, context, "Введите сумму числом. Например: <code>/pay 123456789 1 подарок</code>")
        return

    comment = " ".join(context.args[2:]).strip() or "без комментария"

    ok, msg, recipient_id = transfer_money(update.effective_user.id, target, amount, comment)

    if not ok:
        await send_result(update, context, "❌ " + msg)
        return

    sender_name = mention(update.effective_user)
    await send_result(
        update,
        context,
        "💵 <b>Перевод выполнен</b>\n\n"
        f"Сумма: <b>{money(amount)}</b>\n"
        f"Получатель: <code>{html.escape(str(target))}</code>\n"
        f"Комментарий: <b>{html.escape(comment)}</b>"
    )

    if recipient_id:
        try:
            await context.bot.send_message(
                chat_id=recipient_id,
                text=pe(
                    "🎁 <b>Новый перевод</b>\n\n"
                    f"👤 От кого: {sender_name}\n"
                    f"💵 Сумма: <b>{money(amount)}</b>\n"
                    f"💬 Сообщение: <b>{html.escape(comment)}</b>"
                ),
                parse_mode="HTML"
            )
        except Exception:
            pass


async def send_ball_result_later(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, user, bet_milli: int, dice_value: int, win_milli: int, balance_after: int):
    await asyncio.sleep(BASKETBALL_ANIMATION_DELAY)
    await context.bot.send_message(
        chat_id=chat_id,
        text=pe(ball_result_text(user, bet_milli, dice_value, win_milli, balance_after)),
        parse_mode="HTML",
        reply_to_message_id=message_id,
    )


def ball_result_text(user, bet_milli: int, dice_value: int, win_milli: int, balance_after: int) -> str:
    is_hit = dice_value >= 4

    if is_hit:
        headline = f'Выигрыш <b>{money(win_milli)}</b> в игре 🏀'
        detail = 'Мяч попал в корзину!'
    else:
        headline = f'Проигрыш <b>{money(bet_milli)}</b> в игре 🏀'
        detail = 'Мяч не попал в корзину :('

    return (
        f'{mention(user)}\n'
        f'{headline}\n\n'
        f'{detail}\n\n'
        f'💵 Баланс <b>{money_balance(balance_after)}</b>'
    )



async def ball_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    register_user(user)
    remember_group(chat)

    if await handle_banned_action(update, context):
        return

    if not context.args:
        await send_result(update, context, "🏀 <b>Баскетбол</b>\nКоманда: <code>/ball сумма</code>\nПример: <code>/ball 1</code>\n"
                          f"Минимальная ставка: <b>{money(MIN_BALL_BET_MILLI)}</b>\n")
        return

    bet_milli = parse_money(context.args[0])
    if bet_milli is None or bet_milli <= 0:
        await send_result(update, context, "Введите ставку числом. Например: <code>/ball 1</code>")
        return
    if bet_milli < MIN_BALL_BET_MILLI:
        await send_result(update, context, f"❗️ Минимальная ставка: <b>{money(MIN_BALL_BET_MILLI)}</b>")
        return

    row = get_user(user.id)
    if not row:
        await send_result(update, context, "Профиль не найден. Напиши /start.")
        return

    balance_milli = int(row[4])
    if balance_milli < bet_milli:
        await send_result(update, context, f"❌ Недостаточно средств.\nВаш баланс: <b>{money(balance_milli)}</b>")
        return

    last_spin = get_casino_last_spin(user.id)
    left = CASINO_COOLDOWN_SECONDS - (ts() - last_spin)
    if left > 0:
        await send_result(update, context, f"⏲ Подождите еще <b>{left} сек.</b> перед следующей игрой.")
        return

    ok, msg = take_balance(user.id, bet_milli)
    if not ok:
        await send_result(update, context, f"❌ {html.escape(msg)}")
        return

    dice_msg = await context.bot.send_dice(
        chat_id=chat.id,
        emoji="🏀",
        reply_to_message_id=update.message.message_id if update.message else None,
    )

    dice_value = dice_msg.dice.value if dice_msg.dice else 1
    is_hit = dice_value >= 4
    win_milli = bet_milli * 2 if is_hit else 0
    if win_milli > 0:
        add_balance(user.id, win_milli)

    set_casino_last_spin(user.id)
    updated = get_user(user.id)
    balance_after = int(updated[4]) if updated else 0

    context.application.create_task(
        send_ball_result_later(context, chat.id, dice_msg.message_id, user, bet_milli, dice_value, win_milli, balance_after)
    )



def football_result_text(user, bet_milli: int, dice_value: int, win_milli: int, balance_after: int) -> str:
    is_goal = dice_value >= 3

    if is_goal:
        headline = f'Выигрыш <b>{money(win_milli)}</b> в игре ⚽️'
        detail = 'ГОООЛ! Мяч в воротах!'
    else:
        headline = f'Проигрыш <b>{money(bet_milli)}</b> в игре ⚽️'
        detail = 'Мяч попал в штангу :(' if int(dice_value or 0) == 2 else 'Мяч не попал в ворота :('

    return (
        f'{mention(user)}\n'
        f'{headline}\n\n'
        f'{detail}\n\n'
        f'💵 Баланс <b>{money_balance(balance_after)}</b>'
    )



async def send_football_result_later(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, user, bet_milli: int, dice_value: int, win_milli: int, balance_after: int):
    await asyncio.sleep(FOOTBALL_ANIMATION_DELAY)
    await context.bot.send_message(
        chat_id=chat_id,
        text=pe(football_result_text(user, bet_milli, dice_value, win_milli, balance_after)),
        parse_mode="HTML",
        reply_to_message_id=message_id,
    )


async def football_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    register_user(user)
    remember_group(chat)

    if await handle_banned_action(update, context):
        return

    if not context.args:
        await send_result(update, context, "⚽️ <b>Футбол</b>\nКоманда: <code>/football сумма</code>\nПример: <code>/football 1</code>\n"
                          f"Минимальная ставка: <b>{money(MIN_FOOTBALL_BET_MILLI)}</b>\n")
        return

    bet_milli = parse_money(context.args[0])
    if bet_milli is None or bet_milli <= 0:
        await send_result(update, context, "Введите ставку числом. Например: <code>/football 1</code>")
        return
    if bet_milli < MIN_FOOTBALL_BET_MILLI:
        await send_result(update, context, f"❗️ Минимальная ставка: <b>{money(MIN_FOOTBALL_BET_MILLI)}</b>")
        return

    row = get_user(user.id)
    if not row:
        await send_result(update, context, "Профиль не найден. Напиши /start.")
        return

    balance_milli = int(row[4])
    if balance_milli < bet_milli:
        await send_result(update, context, f"❌ Недостаточно средств.\nВаш баланс: <b>{money(balance_milli)}</b>")
        return

    last_spin = get_casino_last_spin(user.id)
    left = CASINO_COOLDOWN_SECONDS - (ts() - last_spin)
    if left > 0:
        await send_result(update, context, f"⏲ Подождите еще <b>{left} сек.</b> перед следующей игрой.")
        return

    ok, msg = take_balance(user.id, bet_milli)
    if not ok:
        await send_result(update, context, f"❌ {html.escape(msg)}")
        return

    dice_msg = await context.bot.send_dice(
        chat_id=chat.id,
        emoji="⚽",
        reply_to_message_id=update.message.message_id if update.message else None,
    )

    dice_value = dice_msg.dice.value if dice_msg.dice else 1
    is_goal = dice_value >= 3
    win_milli = int(round(bet_milli * 1.5)) if is_goal else 0
    if win_milli > 0:
        add_balance(user.id, win_milli)

    set_casino_last_spin(user.id)
    updated = get_user(user.id)
    balance_after = int(updated[4]) if updated else 0

    context.application.create_task(
        send_football_result_later(context, chat.id, dice_msg.message_id, user, bet_milli, dice_value, win_milli, balance_after)
    )


async def case_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    if is_banned_user(update.effective_user.id):
        await send_result(update, context, "⛔ Вы забанены у бота.")
        return

    if not context.args or context.args[0].lower() != "open":
        await send_result(
            update,
            context,
            "🎁 <b>Кейс</b>\n\n"
            f"Цена: <b>{money(CASE_PRICE_MILLI)}</b>\n"
            f"Кулдаун: <b>{CASE_COOLDOWN_SECONDS} сек.</b>\n"
            "Открыть: <code>/case open</code>"
        )
        return

    ok, msg = open_case(update.effective_user.id)
    prefix = "✅ " if ok else "❌ "
    await send_result(
        update,
        context,
        prefix + f"{mention(update.effective_user)}\n" + msg
    )


async def casino_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if await handle_banned_action(update, context):
        return
    await show_casino(update, context)


async def slots_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

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

    if await handle_banned_action(update, context):
        return

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
    await update.message.reply_text(pe('Действие закрыто.'), parse_mode='HTML')
    return ConversationHandler.END

async def add_phrase_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(q.from_user.id):
        await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END
    await q.message.reply_text(pe('➕ Отправь новую фразу одним сообщением. Редкость выберется рандомно.\n\nМожно указать редкость вручную так:\nrare | Шрек\nepic | Супергерой\nlegendary | Легенда\n\nМожно также отправить .txt файл: каждая непустая строка добавится как отдельная фраза. В .txt тоже можно использовать формат rare | фраза. Секретная редкость автоматически не выбирается — ее нужно указывать вручную: secret | фраза.\n\nДля отмены напиши /cancel'), parse_mode='HTML')
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

    await send_result(
        update,
        context,
        "❗️ <b>Вывод 💵</b>\nФункция временно недоступна.\n<b>Пожалуйста, вернитесь через 48 часов.</b>"
    )

    return ConversationHandler.END



async def withdraw_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    await send_result(
        update,
        context,
        "❗️ <b>Вывод 💵</b>\nФункция временно недоступна.\n<b>Пожалуйста, вернитесь через 48 часов.</b>"
    )

    return ConversationHandler.END



async def withdraw_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wallet = update.message.text.strip()
    if len(wallet) < 10:
        await update.message.reply_text(pe('Адрес слишком короткий. Отправь корректный адрес.'), parse_mode='HTML')
        return WAIT_WALLET
    context.user_data['wallet'] = wallet
    await update.message.reply_text(pe('Теперь введи сумму вывода в 💵.\nНапример: 100 или 150.5'), parse_mode='HTML')
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
    text = f'💸 <b>Новая заявка на вывод</b>\n\nID заявки: <code>{wid}</code>\nПользователь: {mention(update.effective_user)}\nTelegram ID: <code>{update.effective_user.id}</code>\nСумма: <b>{money(amount)}</b>\nКошелек TON 💵:\n<code>{html.escape(wallet)}</code>'
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
    await update.message.reply_text(pe('Введите сумму 💵 для промокода:'), parse_mode='HTML')
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
    await q.message.reply_text(pe('💰 Введите Telegram ID пользователя, которому нужно выдать 💵:'), parse_mode='HTML')
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
    await update.message.reply_text(pe('Введите сумму 💵 для выдачи:'), parse_mode='HTML')
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
    await q.message.reply_text(pe('➖ Введите Telegram ID пользователя, у которого нужно забрать 💵:'), parse_mode='HTML')
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
    await update.message.reply_text(pe('Введите сумму 💵, которую нужно забрать:'), parse_mode='HTML')
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
    data = q.data or ''

    register_user(q.from_user)

    if q.message:
        remember_group(q.message.chat)

    if data.startswith('wd_ok:') or data.startswith('wd_no:'):
        await q.answer()

        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return

        try:
            wid = int(data.split(':', 1)[1])
        except Exception:
            await q.message.reply_text(pe('❌ Ошибка заявки.'), parse_mode='HTML')
            return

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
            return

        if set_withdrawal(wid, 'declined', q.from_user.id):
            add_balance(target, amount)
            await q.edit_message_text(pe(f'❌ Заявка #{wid} отклонена.\nСумма возвращена пользователю: {money(amount)}'), parse_mode='HTML')
            try:
                await context.bot.send_message(target, pe(f'❌ Ваша заявка на вывод {money(amount)} отклонена. Средства возвращены на баланс.'), parse_mode='HTML')
            except Exception:
                pass
        return

    if data == 'profile':
        await q.answer()
        if q.message.chat.type != 'private':
            await q.message.reply_text(pe('Профиль доступен только в личке с ботом.'), parse_mode='HTML')
            return
        await q.edit_message_text(pe(profile_text(q.from_user.id)), parse_mode='HTML', reply_markup=profile_actions_menu())
        return

    if data == 'profile_stats':
        await q.answer()
        if q.message.chat.type != 'private':
            await q.message.reply_text(pe('Статистика доступна только в личке с ботом.'), parse_mode='HTML')
            return
        await q.edit_message_text(pe(profile_stats_text(q.from_user.id)), parse_mode='HTML', reply_markup=stats_actions_menu())
        return

    if data == 'back':
        await q.answer()
        if is_group(q.message.chat):
            await q.edit_message_text(pe('Главное меню:'), parse_mode='HTML', reply_markup=main_menu(is_admin(q.from_user.id), group=True))
        else:
            await q.edit_message_text(pe(main_dashboard_text()), parse_mode='HTML', reply_markup=dashboard_message_menu())
        return

    if data == 'whoami':
        await q.answer()
        await send_role(update, context)
        return

    if data == 'top3':
        await q.answer()
        await send_clean_group_result(update, context, top_text())
        return

    if data == 'casino':
        await q.answer()
        await show_casino(update, context)
        return

    if data == 'transfer_money':
        await q.answer()
        await send_result(update, context, transfer_usage_text())
        return

    if data == 'promo_list':
        await q.answer()
        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return
        await q.message.reply_text(pe(promo_codes_text()), parse_mode='HTML')
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
            await send_result(update, context, '❌ Ошибка ставки.')
            return
        side = normalize_coin_side(parts[1])
        amount = parse_money(parts[2])
        if side is None or amount is None:
            await send_result(update, context, '❌ Ошибка ставки.')
            return
        await play_coin(update, context, side, amount)
        return

    if data.startswith('repeat:'):
        await q.answer()
        parts = data.split(':')
        game = parts[1] if len(parts) > 1 else ''
        try:
            if game == 'slots' and len(parts) == 3:
                return await play_slots(update, context, int(parts[2]))
            if game == 'coin' and len(parts) == 4:
                return await play_coin(update, context, parts[2], int(parts[3]))
            if game == 'ball' and len(parts) == 3:
                context.args = [str(int(parts[2]) / 1000)]
                return await ball_cmd(update, context)
            if game == 'football' and len(parts) == 3:
                context.args = [str(int(parts[2]) / 1000)]
                return await football_cmd(update, context)
        except Exception:
            await q.message.reply_text(pe('❌ Не удалось повторить игру.'), parse_mode='HTML')
            return

    if data.startswith('bonus:'):
        msg = claim_bonus(data.split(':', 1)[1], q.from_user.id)
        await q.answer(msg, show_alert=True)
        return

    if data == 'admin_menu':
        await q.answer()
        if is_admin(q.from_user.id):
            await q.edit_message_text(pe(admin_panel_text()), parse_mode='HTML')
        else:
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    if data == 'last_phrases':
        await q.answer()
        rows = last_phrases(10)
        msg = 'Фраз пока нет.' if not rows else '📋 Последние фразы:\n\n' + '\n'.join(
            f'{pid}. [{RARITY_LABELS.get(rarity, rarity)}] {html.escape(txt)}'
            for pid, txt, rarity in rows
        )
        await q.edit_message_text(pe(msg), parse_mode='HTML', reply_markup=admin_menu())
        return

    if data == 'phrase_count':
        await q.answer()
        await q.edit_message_text(pe(f'🔢 В базе фраз: {phrase_count()}'), reply_markup=admin_menu(), parse_mode='HTML')
        return

    if data == 'admin_stats':
        await q.answer()
        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return
        await send_long_message(context.bot, q.message.chat.id, admin_stats_text(), reply_markup=admin_menu())
        return

    if data == 'groups':
        await q.answer()
        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return
        await q.message.reply_text(pe(groups_text()), parse_mode='HTML')
        return

    await q.answer()



def main():
    print('VERSION_BTC_BUTTON_ICON_ORANGE')
    print('VERSION_CRYPTO_BUTTON_EMOJI_FIX')
    print('VERSION_CRYPTO_PREMIUM_EMOJI_PACK')
    print('VERSION_CRYPTO_NO_LIMIT_FEE_02')
    print('VERSION_CRYPTO_BINANCE_MINIMAL_UI')
    print('VERSION_CRYPTO_MINIMAL_UI_FIX')
    print('VERSION_VIRTUAL_CRYPTO_MARKET')
    print('VERSION_COMMA_DECIMAL_AMOUNTS')
    print('VERSION_NO_AMOUNT_ROUNDING')
    print('VERSION_FOOTBALL_1_5_BASKETBALL_2X')
    print('VERSION_CASINO_WIN_MULTIPLIER_1_5')
    print('VERSION_CASINO_MAX_BET_100')
    print('VERSION_CASINO_PHOTOS_DATA_PATH')
    print('VERSION_TOP3_CMD_NAME_FIX')
    print('VERSION_CASINO_RESULT_PHOTOS_ASSETS')
    print('VERSION_UNSUPPORTED_CHARS_TOPDAY_REMOVED')
    print('VERSION_SEPARATE_TOP3_AND_TOPDAY')
    print('VERSION_FORCE_TOP3_DAILY_ROLES_PROFIT')
    print('VERSION_DAILY_TOP3_PROFIT_VISUAL')
    print('VERSION_INDENT_RUNNER_FIX')
    print('VERSION_ADD_NAME_SQL_SAFE_FIX')
    print('VERSION_ADD_CREATED_AT_FIX')
    print('VERSION_ADD_CONVERSATION_HANDLER_FIX')
    print('VERSION_ADD_DESCRIPTION_STEP_FIX')
    print('VERSION_ADD_CANCEL_TEXT_ONLY_LOGS')
    print('VERSION_ADD_NO_CONFIRM')
    print('VERSION_TOKEN_UPDATED_8210062279')
    print('VERSION_ADD_TEXT_HANDLER_PRIORITY_FIX')
    print('VERSION_ADD_CONFIRM_YES_FIX')
    print('VERSION_PHOTO_ADD_HANDLER_FIX')
    print('VERSION_PHOTO_ROLES_ADD_RESETFRAZ')
    print('VERSION_CLAN_PREMIUM_EMOJI_PACK')
    print('VERSION_CLAN_MENU_INLINE_FIX')
    print('VERSION_CLAN_ADMIN_COMMANDS')
    print('VERSION_CLANS_SYSTEM')
    print('VERSION_CASINO_5_SEC_COOLDOWN')
    print('VERSION_RETURN_FOOTBALL_RU_COMMAND')
    print('VERSION_HARD_REMOVE_OLD_CASINO_GAMES')
    print('VERSION_DISABLE_FOOTBALL_COIN_SLOTS')
    print('VERSION_CASINO_BASKET_CUBE_RU')
    print('VERSION_PAY_REPLY_AND_ID_GROUPS')
    print('VERSION_REPLY_PAY_IN_GROUPS')
    print('VERSION_GOLD_CASINO_6_PAYLIMIT_EXPTAKE')
    print('VERSION_DAILY_EXP_EXPGIVE_STATS_STARTCHAT_DM')
    print('VERSION_ROLL_PHRASE_COMPAT_FIX')
    print('VERSION_ROLE_TRIGGER_BUTTON_FIX')
    print('VERSION_EXP_GROUP_EVENTS_REPLY')
    print('VERSION_RUBY_CASINO_LIMIT_CLEARMONEY')
    print('VERSION_CASINO_NO_MAX_BET')
    print('VERSION_TOKEN_USDT_VISUAL_GAME_TITLES_FIX')
    print('VERSION_BALANCE_FLOOR_FIX')
    print('VERSION_TOP_TRIGGER_LEVEL_EMOJI_FIX')
    print('VERSION_CASE_MENTION_COIN_VISUAL_FIX')
    print('VERSION_START_HELLO_THEN_INLINE_MENU')
    print('VERSION_NO_TECH_KEYBOARD_MESSAGE')
    print('VERSION_HIDE_KEYBOARD_REFRESH_MESSAGE')
    print('VERSION_RESTORE_BOTTOM_KEYBOARD')
    print('VERSION_MAIN_MENU_KEYBOARD_FINAL_FIX')
    print('VERSION_CANCEL_TO_MAIN_AND_FOOTBALL_TITLE_FIX')
    print('VERSION_GAME_HEADERS_FINAL_FIX')
    print('VERSION_MAIN_MENU_BUTTON_OPENS_SCREEN')
    print('VERSION_USDT_EMOJI_MENU_VISUAL_V2')
    print('VERSION_USDT_EMOJI_MENU_VISUAL')
    print('VERSION_STYLE_LEVELS_REPEAT')
    print('VERSION_PREMIUM_VISUAL_REWORK')
    print('VERSION_WITHDRAW_TEXT_UPDATED')
    print('VERSION_BALANCE_ROUND_WITHDRAW_SUPPORT')
    print('VERSION_FOOTBALL_GOAL_FIX')
    print('VERSION_FOOTBALL_15S_BANLIST_RANDOM_ROLES')
    print('VERSION_RE_IMPORT_FIX')
    print('VERSION_LOGS_WITHDRAW_BAN_FIX')
    print('VERSION_BASKETBALL_DELAY_CASINO_TEXT')
    print('VERSION_BASKETBALL_GAME')
    print('VERSION_CASINO_NO_BUTTONS')
    print('VERSION_GAME_RESULT_NO_BUTTONS')
    print('VERSION_PAY_TEXT_FIX')
    print('VERSION_PAY_TRANSFER_REWARDS')
    print('VERSION_CASE_COOLDOWN_30')
    print('VERSION_SECRET_100X_RARER')
    print('VERSION_CASE_HIDE_CONTENT')
    print('VERSION_CASE_PREFIX_DISCOUNT')
    print('VERSION_UID_DIGITS_ONLY')
    print('VERSION_CASES_5USDT')
    print('VERSION_COIN_FIX_SAFE')
    print('VERSION_ADMIN_FIX')
    init_db()
    app = Application.builder().token(BOT_TOKEN).defaults(Defaults(parse_mode="HTML")).build()
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler('add', add_photo_role_start)],
        states={
            ADD_PHOTO: [
                MessageHandler(filters.PHOTO, add_photo_step),
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_photo_step),
                MessageHandler(filters.Document.IMAGE, add_photo_step),
            ],
            ADD_RARITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_rarity_step)],
            ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_name_step)],
        },
        fallbacks=[
            CommandHandler('cancel', add_cancel_conv),
            MessageHandler(filters.Regex('^(Отменить|отменить|отмена|cancel)$'), add_cancel_conv),
        ],
        name='photo_role_add_conversation',
        persistent=False,
    ))

    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('admin', admin_cmd))
    app.add_handler(CommandHandler('top', top_cmd))
    app.add_handler(CommandHandler('top3', top3_cmd))
    app.add_handler(CommandHandler('top3', top3_cmd))
    app.add_handler(CommandHandler('resetfraz', resetfraz_cmd))
    app.add_handler(CommandHandler('allclans', allclans_cmd))
    app.add_handler(CommandHandler('clandelete', clandelete_cmd))
    app.add_handler(CommandHandler('startchat', startchat_cmd))
    app.add_handler(CommandHandler('clearmoney', clear_money_cmd))
    app.add_handler(CommandHandler('menu', menu_cmd))
    app.add_handler(CommandHandler('dailyexp', daily_exp_cmd))
    app.add_handler(CommandHandler('whoami', whoami))
    app.add_handler(CommandHandler('ban', ban_cmd))
    app.add_handler(CommandHandler('banlist', banlist_cmd))
    app.add_handler(CommandHandler('unban', unban_cmd))
    app.add_handler(CommandHandler('give', give_direct_cmd))
    app.add_handler(CommandHandler('expgive', expgive_cmd))
    app.add_handler(CommandHandler('exptake', exptake_cmd))
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
    app.add_handler(CommandHandler('case', case_cmd))
    app.add_handler(CommandHandler('pay', pay_cmd))
    app.add_handler(CommandHandler('ball', ball_cmd))
    app.add_handler(CommandHandler('search', search_cmd))
    app.add_handler(CommandHandler('dbpath', dbpath_cmd))
    app.add_handler(CommandHandler('adminstats', admin_stats_cmd))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(add_phrase_start, pattern='^add_phrase$')], states={WAIT_PHRASE: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_phrase)]}, fallbacks=[CommandHandler('cancel', cancel), MessageHandler(filters.Regex('^(❌ )?Главное меню$'), cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(withdraw_start, pattern='^withdraw$'), MessageHandler(filters.Regex('^(💸 )?Вывод 💵$'), withdraw_start_text)], states={WAIT_WALLET: [MessageHandler(filters.TEXT & ~filters.COMMAND, withdraw_wallet)], WAIT_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, withdraw_amount)]}, fallbacks=[CommandHandler('cancel', cancel), MessageHandler(filters.Regex('^(❌ )?Главное меню$'), cancel)]))
    app.add_handler(ConversationHandler(
        entry_points=[
            CallbackQueryHandler(promo_activate_start, pattern='^promo_activate$'),
            MessageHandler(filters.Regex('^(🎁 )?Промокод$'), promo_activate_text_start),
        ],
        states={
            WAIT_PROMO_ACTIVATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_activate_finish)],
        },
        fallbacks=[CommandHandler('cancel', cancel), MessageHandler(filters.Regex('^(❌ )?Главное меню$'), cancel)],
    ))

    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(promo_create_start, pattern='^promo_create$')],
        states={
            WAIT_PROMO_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_create_code)],
            WAIT_PROMO_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_create_amount)],
            WAIT_PROMO_LIMIT: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_create_limit)],
        },
        fallbacks=[CommandHandler('cancel', cancel), MessageHandler(filters.Regex('^(❌ )?Главное меню$'), cancel)],
    ))

    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(give_start, pattern='^give_usdt$')], states={WAIT_GIVE_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, give_user)], WAIT_GIVE_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, give_amount)]}, fallbacks=[CommandHandler('cancel', cancel), MessageHandler(filters.Regex('^(❌ )?Главное меню$'), cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(take_start, pattern='^take_usdt$')], states={WAIT_TAKE_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, take_user)], WAIT_TAKE_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, take_amount)]}, fallbacks=[CommandHandler('cancel', cancel), MessageHandler(filters.Regex('^(❌ )?Главное меню$'), cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(uid_start, pattern='^custom_uid$')], states={WAIT_UID_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, uid_user)], WAIT_UID_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, uid_value)]}, fallbacks=[CommandHandler('cancel', cancel), MessageHandler(filters.Regex('^(❌ )?Главное меню$'), cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(hide_start, pattern='^hide_user$')], states={WAIT_HIDE_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, hide_finish)]}, fallbacks=[CommandHandler('cancel', cancel), MessageHandler(filters.Regex('^(❌ )?Главное меню$'), cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(unhide_start, pattern='^unhide_user$')], states={WAIT_UNHIDE_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, unhide_finish)]}, fallbacks=[CommandHandler('cancel', cancel), MessageHandler(filters.Regex('^(❌ )?Главное меню$'), cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(search_user_start, pattern='^search_user$'), MessageHandler(filters.Regex('^(🔎 )?Поиск по ID$'), search_user_start_text)], states={WAIT_SEARCH_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_user_finish)]}, fallbacks=[CommandHandler('cancel', cancel), MessageHandler(filters.Regex('^(❌ )?Главное меню$'), cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(delete_phrase_start, pattern='^delete_phrase_btn$')], states={WAIT_DELETE_PHRASE: [MessageHandler(filters.TEXT & ~filters.COMMAND, delete_phrase_finish)]}, fallbacks=[CommandHandler('cancel', cancel), MessageHandler(filters.Regex('^(❌ )?Главное меню$'), cancel)]))
    app.add_handler(ConversationHandler(entry_points=[CallbackQueryHandler(broadcast_start, pattern='^broadcast$')], states={WAIT_BROADCAST_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, broadcast_finish)]}, fallbacks=[CommandHandler('cancel', cancel), MessageHandler(filters.Regex('^(❌ )?Главное меню$'), cancel)]))
    app.add_handler(CallbackQueryHandler(admin_stats_button, pattern='^admin_stats$'))
    app.add_handler(MessageHandler(filters.Document.ALL, txt_phrases_handler))
    app.add_handler(CallbackQueryHandler(buttons))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, trigger))
    logger.info('Бот запущен')
    app.run_polling()


# ===== STYLE UPDATE: dashboard, levels, repeat game =====
BOT_PUBLIC_USERNAME = '@Ktoyaro_bot'
BOT_GROUP_LINK = '@bezdnao'

old_init_db_visual = init_db

def init_db():
    old_init_db_visual()
    with db() as conn:
        user_cols = columns(conn, 'users')
        if 'games_played' not in user_cols:
            conn.execute("ALTER TABLE users ADD COLUMN games_played INTEGER NOT NULL DEFAULT 0")
        if 'turnover_milli' not in user_cols:
            conn.execute("ALTER TABLE users ADD COLUMN turnover_milli INTEGER NOT NULL DEFAULT 0")
        conn.commit()


def get_user_full(user_id: int):
    with db() as conn:
        return conn.execute(
            """
            SELECT user_id, username, first_name, uid, balance_milli, openings,
                   last_role_at, hidden, casino_last_spin_at, created_at,
                   COALESCE(games_played, 0), COALESCE(turnover_milli, 0),
                   prefix
            FROM users WHERE user_id=?
            """,
            (user_id,),
        ).fetchone()


def add_game_stats(user_id: int, bet_milli: int):
    with db() as conn:
        conn.execute(
            "UPDATE users SET games_played=COALESCE(games_played,0)+1, turnover_milli=COALESCE(turnover_milli,0)+? WHERE user_id=?",
            (int(bet_milli or 0), user_id),
        )
        conn.commit()


def account_age_text(created_at: int) -> str:
    if not created_at:
        return 'меньше дня'
    diff = max(0, ts() - int(created_at))
    days = diff // 86400
    if days <= 0:
        return 'меньше дня'
    if days == 1:
        return '1 день'
    if 2 <= days <= 4:
        return f'{days} дня'
    return f'{days} дней'


def level_info_by_openings(openings: int) -> dict:
    openings = int(openings or 0)
    levels = [
        {'level': 0, 'emoji': '0⃣', 'name': 'None', 'start': 0, 'end': 10},
        {'level': 1, 'emoji': '1⃣', 'name': 'Bronze', 'start': 10, 'end': 35},
        {'level': 2, 'emoji': '2⃣', 'name': 'Silver', 'start': 35, 'end': 80},
        {'level': 3, 'emoji': '3⃣', 'name': 'Gold', 'start': 80, 'end': 150},
        {'level': 4, 'emoji': '⭐', 'name': 'Ruby', 'start': 150, 'end': None},
    ]

    current = levels[0]
    for item in levels:
        if item['end'] is None:
            if openings >= item['start']:
                current = item
        elif item['start'] <= openings < item['end']:
            current = item
            break

    next_level = levels[min(current['level'] + 1, len(levels) - 1)]

    if current['end'] is None:
        percent = 100
    else:
        span = max(1, current['end'] - current['start'])
        percent = int(max(0, min(100, ((openings - current['start']) / span) * 100)))

    return {'current': current, 'next': next_level, 'percent': percent}


def user_has_ruby_level(user_id: int) -> bool:
    row = get_user_full(user_id)

    if not row:
        return False

    openings = int(row[5] or 0)
    return level_info_by_openings(openings)['current']['name'] == 'Ruby'


async def require_ruby_casino(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if user_has_ruby_level(update.effective_user.id):
        return True

    row = get_user_full(update.effective_user.id)
    openings = int(row[5] or 0) if row else 0
    level = level_info_by_openings(openings)

    await send_result(
        update,
        context,
        "🔒 <b>Казино доступно только с уровня Ruby.</b>\n\n"
        f"Ваш уровень: <b>{level['current']['emoji']} {level['current']['name']}</b>\n"
        f"Прогресс: <b>{level['percent']}%</b>\n"
        f"Нужно открыть ролей до Ruby: <b>{max(0, 150 - openings)}</b>"
    )

    return False



def get_withdrawn_total(user_id: int) -> int:
    with db() as conn:
        row = conn.execute("SELECT COALESCE(SUM(amount_milli),0) FROM withdrawals WHERE user_id=? AND status='approved'", (user_id,)).fetchone()
        return int(row[0] or 0) if row else 0


def hello_keyboard_text() -> str:
    return "👤 <b>Привет!</b>"


def main_dashboard_text() -> str:
    return (
        f"👋 <b>Добро пожаловать в {BOT_PUBLIC_USERNAME}</b>\n\n"
        "🎮 Получай роли, играй и копи баланс.\n"
        f"💬 Новости и конкурсы — {BOT_GROUP_LINK}"
    )



def dashboard_message_menu():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton('👤 Профиль', callback_data='profile'),
            InlineKeyboardButton('📊 Статистика', callback_data='profile_stats'),
        ],
        [
            InlineKeyboardButton('💸 Вывести', callback_data='withdraw'),
            InlineKeyboardButton('👥 Группа', url='https://t.me/bezdnao'),
        ],
    ])


def main_menu(admin=False, group=False):
    if group:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton('🎭 Кто я', callback_data='whoami'), InlineKeyboardButton('🎮 Играть', callback_data='casino')],
            [InlineKeyboardButton('🏆 Топ 3', callback_data='top3')],
        ])
    return dashboard_message_menu()


def reply_main_menu(admin=False, group=False):
    if group:
        rows = []
    else:
        rows = [
            ['🎭 Кто я', '🎮 Играть'],
            ['👤 Профиль', '🏆 Топ 3'],
            ['💵 Передача денег', '🎁 Промокод'],
            ['🏠 Главное меню'],
        ]

    return ReplyKeyboardMarkup(
        rows,
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder='Выберите действие...'
    )



def profile_actions_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📊 Статистика', callback_data='profile_stats'), InlineKeyboardButton('💸 Вывести', callback_data='withdraw')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='back')],
    ])


def stats_actions_menu():
    return InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ Назад', callback_data='profile')]])


def withdraw_back_menu():
    return InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ Назад', callback_data='profile')]])


def repeat_game_menu(game: str, bet_milli: int, side: str | None = None):
    if game == 'slots':
        data = f'repeat:slots:{bet_milli}'
    elif game == 'coin':
        data = f'repeat:coin:{side}:{bet_milli}'
    elif game == 'ball':
        data = f'repeat:ball:{bet_milli}'
    elif game == 'football':
        data = f'repeat:football:{bet_milli}'
    else:
        return None
    return InlineKeyboardMarkup([[InlineKeyboardButton('🔁 Повторить игру', callback_data=data)]])


def profile_text(user_id: int) -> str:
    row = get_user_full(user_id)

    if not row:
        return 'Профиль не найден. Напиши /start.'

    _, username, first_name, uid, balance, openings, _, hidden, _, created_at, games_played, turnover_milli, prefix = row
    level = level_info_by_openings(openings)
    display_name = f'@{username}' if username else (first_name or f'Игрок #{uid}')

    status = 'Активен'
    banned, ban_reason, banned_until = get_user_ban_status_direct(user_id)
    if hidden:
        status = 'Скрыт'
    if banned:
        status = f'Бан: {ban_time_text(int(banned_until or 0))}'

    extra = ''
    if prefix:
        extra += f'\n🏷 Префикс — <b>{html.escape(prefix)}</b>'

    try:
        discount = get_case_discount(user_id)
        if discount > 0:
            extra += f'\n💸 Скидка на кейс — <b>{money(discount)}</b>'
    except Exception:
        pass

    try:
        booster = luck_booster_left(user_id)
        if booster > 0:
            extra += f'\n⏱ Бустер удачи — <b>{booster_time_text(booster)}</b>'
    except Exception:
        pass

    return (
        f'<b>#{html.escape(str(uid))} {html.escape(display_name)}</b>\n\n'
        f'💵 Баланс — <b>{money_balance(balance)}</b>\n\n'
        f'Ваш прогресс — <b>{level["percent"]}%</b>\n'
        f'{level["current"]["emoji"]} {level["current"]["name"]} ➡️ {level["next"]["emoji"]} {level["next"]["name"]}\n\n'
        f'📰 Открыто ролей — <b>{int(openings or 0)}</b>\n'
        f'🎮 Сыграно — <b>{int(games_played or 0)} ставок</b>\n'
        f'⏱ Аккаунту — <b>{account_age_text(int(created_at or 0))}</b>\n'
        f'Статус — <b>{html.escape(status)}</b>'
        f'{extra}'
    )



def profile_stats_text(user_id: int) -> str:
    row = get_user_full(user_id)
    if not row:
        return 'Статистика недоступна.'
    _, username, first_name, uid, _, _, _, _, _, created_at, games_played, turnover_milli, _ = row
    name = f'@{username}' if username else (first_name or f'Игрок #{uid}')
    withdrawn = get_withdrawn_total(user_id)
    return (
        f'📊 <b>Статистика</b> {html.escape(name)}\n\n'
        f'🎮 Сыграно — <b>{int(games_played or 0)} ставок</b>\n'
        f'💵 Оборот — <b>{money(int(turnover_milli or 0))}</b>\n'
        f'⏱ Аккаунту — <b>{account_age_text(int(created_at or 0))}</b>\n\n'
        f'⬇️ Пополнений — <b>0 💵</b>\n'
        f'⬆️ Выводов — <b>{money(int(withdrawn or 0))}</b>'
    )


def transfer_usage_text() -> str:
    return (
        '💵 <b>Передача денег</b>\n\n'
        'Команды:\n'
        '<code>/pay USER_ID сумма комментарий</code>\n'
        '<code>/pay @username сумма комментарий</code>\n\n'
        'Пример:\n'
        '<code>/pay 123456789 1 подарок</code>'
    )


async def show_casino(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)
    if await handle_banned_action(update, context):
        return

    if not await require_ruby_casino(update, context):
        return
    text = (
        '🎮 <b>Играть</b>\n\n'
        '🎰 <code>/slots 1</code> — слоты\n'
        '🪙 <code>/coin орел 1</code> — орел / решка\n'
        '🏀 <code>/ball 1</code> — баскетбол\n'
        '⚽️ <code>/football 1</code> — футбол\n'
        '🎁 <code>/case open</code> — кейс\n\nМинимальная ставка: <b>1 💵</b>\nМаксимальная ставка: <b>10 💵</b>\nДоступ: <b>только Ruby</b>'
    )
    await send_clean_group_result(update, context, text)


def slot_result_text(user, bet_milli: int, symbols: list[str], multiplier: float, win_milli: int, balance_after: int) -> str:
    combo = ' '.join(symbols)

    if win_milli > 0:
        headline = f'Выигрыш <b>{money(win_milli)}</b> в игре 🎰'
    else:
        headline = f'Проигрыш <b>{money(bet_milli)}</b> в игре 🎰'

    return (
        f'{mention(user)}\n'
        f'{headline}\n'
        f'Комбинация: <b>{combo}</b>\n\n'
        f'💵 Баланс <b>{money_balance(balance_after)}</b>'
    )



def coin_result_text(user, bet_milli: int, choice: str, result: str, win_milli: int, balance_after: int) -> str:
    won = choice == result

    if won:
        headline = f'Выигрыш <b>{money(win_milli)}</b> в игре 🪙'
    else:
        headline = f'Проигрыш <b>{money(bet_milli)}</b> в игре 🪙'

    return (
        f'{mention(user)}\n'
        f'{headline}\n'
        f'Ваш выбор: <b>{coin_side_label(choice)}</b>\n'
        f'Выпало: <b>{coin_side_label(result)}</b>\n\n'
        f'💵 Баланс <b>{money_balance(balance_after)}</b>'
    )



def ball_result_text(user, bet_milli: int, dice_value: int, win_milli: int, balance_after: int) -> str:
    is_hit = dice_value >= 4

    if is_hit:
        headline = f'Выигрыш <b>{money(win_milli)}</b> в игре 🏀'
        detail = 'Мяч попал в корзину!'
    else:
        headline = f'Проигрыш <b>{money(bet_milli)}</b> в игре 🏀'
        detail = 'Мяч не попал в корзину :('

    return (
        f'{mention(user)}\n'
        f'{headline}\n\n'
        f'{detail}\n\n'
        f'💵 Баланс <b>{money_balance(balance_after)}</b>'
    )



async def send_ball_result_later(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, user, bet_milli: int, dice_value: int, win_milli: int, balance_after: int):
    await asyncio.sleep(BASKETBALL_ANIMATION_DELAY)
    await context.bot.send_message(
        chat_id=chat_id,
        text=pe(ball_result_text(user, bet_milli, dice_value, win_milli, balance_after)),
        parse_mode='HTML',
        reply_to_message_id=message_id,
        reply_markup=repeat_game_menu('ball', bet_milli),
    )


def football_result_text(user, bet_milli: int, dice_value: int, win_milli: int, balance_after: int) -> str:
    is_goal = dice_value >= 3

    if is_goal:
        headline = f'Выигрыш <b>{money(win_milli)}</b> в игре ⚽️'
        detail = 'ГОООЛ! Мяч в воротах!'
    else:
        headline = f'Проигрыш <b>{money(bet_milli)}</b> в игре ⚽️'
        detail = 'Мяч попал в штангу :(' if int(dice_value or 0) == 2 else 'Мяч не попал в ворота :('

    return (
        f'{mention(user)}\n'
        f'{headline}\n\n'
        f'{detail}\n\n'
        f'💵 Баланс <b>{money_balance(balance_after)}</b>'
    )



async def send_football_result_later(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, user, bet_milli: int, dice_value: int, win_milli: int, balance_after: int):
    await asyncio.sleep(FOOTBALL_ANIMATION_DELAY)
    await context.bot.send_message(
        chat_id=chat_id,
        text=pe(football_result_text(user, bet_milli, dice_value, win_milli, balance_after)),
        parse_mode='HTML',
        reply_to_message_id=message_id,
        reply_markup=repeat_game_menu('football', bet_milli),
    )


old_start_visual = start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await open_main_screen(update, context)



async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await open_main_screen(update, context)



async def refresh_private_keyboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return



async def open_main_screen(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        pe(hello_keyboard_text()),
        parse_mode='HTML',
        reply_markup=reply_main_menu(is_admin(update.effective_user.id), group=False)
    )

    await update.message.reply_text(
        pe(main_dashboard_text()),
        parse_mode='HTML',
        reply_markup=dashboard_message_menu()
    )



async def trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    raw_text = update.message.text.strip()
    txt = raw_text.lower()
    lower_text = txt

    if context.user_data.get('waiting_promo_activate'):
        context.user_data['waiting_promo_activate'] = False
        ok, msg = activate_promo_code(update.effective_user.id, raw_text)
        await update.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')
        return

    if txt in ('🏠 главное меню', 'главное меню'):
        await open_main_screen(update, context)
        return

    if txt in TRIGGERS or txt in ('кто я?', '🎭 кто я', 'я', 'кто', 'кто я'):
        await send_role(update, context)
        return

    if txt in ('играть', '🎮 играть', 'казино', '🎰 казино'):
        await show_casino(update, context)
        return

    if txt in ('профиль', '👤 профиль'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('Профиль доступен только в личке с ботом.'), parse_mode='HTML')
            return
        await send_result(update, context, profile_text(update.effective_user.id), reply_markup=profile_actions_menu())
        return

    if txt in ('топ 3', '🏆 топ 3', 'топ'):
        await send_clean_group_result(update, context, top_text())
        return

    if txt in ('передача денег', '💵 передача денег'):
        await send_result(update, context, transfer_usage_text())
        return

    if txt in ('промокод', '🎁 промокод'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('Промокоды доступны только в личке с ботом.'), parse_mode='HTML')
            return
        await update.message.reply_text(pe('🎁 Введите промокод одним сообщением:'), parse_mode='HTML')
        context.user_data['waiting_promo_activate'] = True
        return



async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ''

    register_user(q.from_user)

    if q.message:
        remember_group(q.message.chat)

    if data.startswith('wd_ok:') or data.startswith('wd_no:'):
        await q.answer()

        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return

        try:
            wid = int(data.split(':', 1)[1])
        except Exception:
            await q.message.reply_text(pe('❌ Ошибка заявки.'), parse_mode='HTML')
            return

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
            return

        if set_withdrawal(wid, 'declined', q.from_user.id):
            add_balance(target, amount)
            await q.edit_message_text(pe(f'❌ Заявка #{wid} отклонена.\nСумма возвращена пользователю: {money(amount)}'), parse_mode='HTML')
            try:
                await context.bot.send_message(target, pe(f'❌ Ваша заявка на вывод {money(amount)} отклонена. Средства возвращены на баланс.'), parse_mode='HTML')
            except Exception:
                pass
        return

    if data == 'profile':
        await q.answer()
        if q.message.chat.type != 'private':
            await q.message.reply_text(pe('Профиль доступен только в личке с ботом.'), parse_mode='HTML')
            return
        await q.edit_message_text(pe(profile_text(q.from_user.id)), parse_mode='HTML', reply_markup=profile_actions_menu())
        return

    if data == 'profile_stats':
        await q.answer()
        if q.message.chat.type != 'private':
            await q.message.reply_text(pe('Статистика доступна только в личке с ботом.'), parse_mode='HTML')
            return
        await q.edit_message_text(pe(profile_stats_text(q.from_user.id)), parse_mode='HTML', reply_markup=stats_actions_menu())
        return

    if data == 'back':
        await q.answer()
        if is_group(q.message.chat):
            await q.edit_message_text(pe('Главное меню:'), parse_mode='HTML', reply_markup=main_menu(is_admin(q.from_user.id), group=True))
        else:
            await q.edit_message_text(pe(main_dashboard_text()), parse_mode='HTML', reply_markup=dashboard_message_menu())
        return

    if data == 'whoami':
        await q.answer()
        await send_role(update, context)
        return

    if data == 'top3':
        await q.answer()
        await send_clean_group_result(update, context, top_text())
        return

    if data == 'casino':
        await q.answer()
        await show_casino(update, context)
        return

    if data == 'transfer_money':
        await q.answer()
        await send_result(update, context, transfer_usage_text())
        return

    if data == 'promo_list':
        await q.answer()
        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return
        await q.message.reply_text(pe(promo_codes_text()), parse_mode='HTML')
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
            await send_result(update, context, '❌ Ошибка ставки.')
            return
        side = normalize_coin_side(parts[1])
        amount = parse_money(parts[2])
        if side is None or amount is None:
            await send_result(update, context, '❌ Ошибка ставки.')
            return
        await play_coin(update, context, side, amount)
        return

    if data.startswith('repeat:'):
        await q.answer()
        parts = data.split(':')
        game = parts[1] if len(parts) > 1 else ''
        try:
            if game == 'slots' and len(parts) == 3:
                return await play_slots(update, context, int(parts[2]))
            if game == 'coin' and len(parts) == 4:
                return await play_coin(update, context, parts[2], int(parts[3]))
            if game == 'ball' and len(parts) == 3:
                context.args = [str(int(parts[2]) / 1000)]
                return await ball_cmd(update, context)
            if game == 'football' and len(parts) == 3:
                context.args = [str(int(parts[2]) / 1000)]
                return await football_cmd(update, context)
        except Exception:
            await q.message.reply_text(pe('❌ Не удалось повторить игру.'), parse_mode='HTML')
            return

    if data.startswith('bonus:'):
        msg = claim_bonus(data.split(':', 1)[1], q.from_user.id)
        await q.answer(msg, show_alert=True)
        return

    if data == 'admin_menu':
        await q.answer()
        if is_admin(q.from_user.id):
            await q.edit_message_text(pe(admin_panel_text()), parse_mode='HTML')
        else:
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    if data == 'last_phrases':
        await q.answer()
        rows = last_phrases(10)
        msg = 'Фраз пока нет.' if not rows else '📋 Последние фразы:\n\n' + '\n'.join(
            f'{pid}. [{RARITY_LABELS.get(rarity, rarity)}] {html.escape(txt)}'
            for pid, txt, rarity in rows
        )
        await q.edit_message_text(pe(msg), parse_mode='HTML', reply_markup=admin_menu())
        return

    if data == 'phrase_count':
        await q.answer()
        await q.edit_message_text(pe(f'🔢 В базе фраз: {phrase_count()}'), reply_markup=admin_menu(), parse_mode='HTML')
        return

    if data == 'admin_stats':
        await q.answer()
        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return
        await send_long_message(context.bot, q.message.chat.id, admin_stats_text(), reply_markup=admin_menu())
        return

    if data == 'groups':
        await q.answer()
        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return
        await q.message.reply_text(pe(groups_text()), parse_mode='HTML')
        return

    await q.answer()



async def play_slots(update: Update, context: ContextTypes.DEFAULT_TYPE, bet_milli: int):
    user = update.effective_user
    register_user(user)
    remember_group(update.effective_chat)

    if not await require_ruby_casino(update, context):
        return
    banned, reason, banned_until = get_user_ban_status_direct(user.id)
    if banned:
        await send_clean_group_result(update, context, '⛔ Вы забанены у бота.\n' f'Причина: <b>{html.escape(reason or "не указана")}</b>\n' f'Осталось: <b>{html.escape(ban_time_text(banned_until))}</b>')
        return
    row = get_user(user.id)
    if not row:
        await send_clean_group_result(update, context, '❌ Профиль не найден. Напиши /start.')
        return
    balance_milli = int(row[4])
    if bet_milli < MIN_SLOT_BET_MILLI:
        await send_clean_group_result(update, context, f'❗️ Минимальная ставка: <b>{money(MIN_SLOT_BET_MILLI)}</b>')
        return

    if bet_milli > MAX_SLOT_BET_MILLI:
        await send_clean_group_result(update, context, f"❗️ Максимальная ставка: <b>{money(MAX_SLOT_BET_MILLI)}</b>")
        return
    if balance_milli < bet_milli:
        await send_clean_group_result(update, context, f'❌ Недостаточно средств.\nВаш баланс: <b>{money(balance_milli)}</b>')
        return
    left = CASINO_COOLDOWN_SECONDS - (ts() - get_casino_last_spin(user.id))
    if left > 0:
        await send_clean_group_result(update, context, f'⏲ Подождите еще <b>{left} сек.</b> перед следующим спином.')
        return
    ok, msg = take_balance(user.id, bet_milli)
    if not ok:
        await send_clean_group_result(update, context, f'❌ {html.escape(msg)}')
        return
    add_game_stats(user.id, bet_milli)
    symbols = roll_slots()
    multiplier = get_slot_multiplier(symbols)
    win_milli = int(round(bet_milli * multiplier))
    if win_milli > 0:
        add_balance(user.id, win_milli)
    set_casino_last_spin(user.id)
    updated = get_user(user.id)
    balance_after = int(updated[4]) if updated else 0
    await send_clean_group_result(update, context, slot_result_text(user, bet_milli, symbols, multiplier, win_milli, balance_after), reply_markup=repeat_game_menu('slots', bet_milli))


async def play_coin(update: Update, context: ContextTypes.DEFAULT_TYPE, side: str, bet_milli: int):
    user = update.effective_user
    register_user(user)
    remember_group(update.effective_chat)

    if not await require_ruby_casino(update, context):
        return
    banned, reason, banned_until = get_user_ban_status_direct(user.id)
    if banned:
        await send_clean_group_result(update, context, '⛔ Вы забанены у бота.\n' f'Причина: <b>{html.escape(reason or "не указана")}</b>\n' f'Осталось: <b>{html.escape(ban_time_text(banned_until))}</b>')
        return
    row = get_user(user.id)
    if not row:
        await send_clean_group_result(update, context, '❌ Профиль не найден. Напиши /start.')
        return
    balance_milli = int(row[4])
    if bet_milli < MIN_COIN_BET_MILLI:
        await send_clean_group_result(update, context, f'❗️ Минимальная ставка: <b>{money(MIN_COIN_BET_MILLI)}</b>')
        return

    if bet_milli > MAX_COIN_BET_MILLI:
        await send_clean_group_result(update, context, f"❗️ Максимальная ставка: <b>{money(MAX_COIN_BET_MILLI)}</b>")
        return
    if balance_milli < bet_milli:
        await send_clean_group_result(update, context, f'❌ Недостаточно средств.\nВаш баланс: <b>{money(balance_milli)}</b>')
        return
    left = CASINO_COOLDOWN_SECONDS - (ts() - get_casino_last_spin(user.id))
    if left > 0:
        await send_clean_group_result(update, context, f'⏲ Подождите еще <b>{left} сек.</b> перед следующей игрой.')
        return
    ok, msg = take_balance(user.id, bet_milli)
    if not ok:
        await send_clean_group_result(update, context, f'❌ {html.escape(msg)}')
        return
    add_game_stats(user.id, bet_milli)
    result = roll_coin(user.id)
    win_milli = bet_milli * 2 if side == result else 0
    if win_milli > 0:
        add_balance(user.id, win_milli)
    set_casino_last_spin(user.id)
    updated = get_user(user.id)
    balance_after = int(updated[4]) if updated else 0
    await send_clean_group_result(update, context, coin_result_text(user, bet_milli, side, result, win_milli, balance_after), reply_markup=repeat_game_menu('coin', bet_milli, side))


async def ball_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    register_user(user)
    remember_group(chat)
    if await handle_banned_action(update, context):
        return

    if not await require_ruby_casino(update, context):
        return
    if not context.args:
        await send_result(update, context, '🏀 <b>Баскетбол</b>\n\nКоманда: <code>/ball сумма</code>\nПример: <code>/ball 1</code>')
        return
    bet_milli = parse_money(context.args[0])
    if bet_milli is None or bet_milli <= 0:
        await send_result(update, context, 'Введите ставку числом. Например: <code>/ball 1</code>')
        return
    if bet_milli < MIN_BALL_BET_MILLI:
        await send_result(update, context, f'❗️ Минимальная ставка: <b>{money(MIN_BALL_BET_MILLI)}</b>')
        return

    if bet_milli > MAX_BALL_BET_MILLI:
        await send_result(update, context, f"❗️ Максимальная ставка: <b>{money(MAX_BALL_BET_MILLI)}</b>")
        return
    row = get_user(user.id)
    if not row:
        await send_result(update, context, 'Профиль не найден. Напиши /start.')
        return
    balance_milli = int(row[4])
    if balance_milli < bet_milli:
        await send_result(update, context, f'❌ Недостаточно средств.\nВаш баланс: <b>{money(balance_milli)}</b>')
        return
    left = CASINO_COOLDOWN_SECONDS - (ts() - get_casino_last_spin(user.id))
    if left > 0:
        await send_result(update, context, f'⏲ Подождите еще <b>{left} сек.</b> перед следующей игрой.')
        return
    ok, msg = take_balance(user.id, bet_milli)
    if not ok:
        await send_result(update, context, f'❌ {html.escape(msg)}')
        return
    add_game_stats(user.id, bet_milli)
    dice_msg = await context.bot.send_dice(chat_id=chat.id, emoji='🏀', reply_to_message_id=update.message.message_id if update.message else None)
    dice_value = dice_msg.dice.value if dice_msg.dice else 1
    is_hit = dice_value >= 4
    win_milli = bet_milli * 2 if is_hit else 0
    if win_milli > 0:
        add_balance(user.id, win_milli)
    set_casino_last_spin(user.id)
    updated = get_user(user.id)
    balance_after = int(updated[4]) if updated else 0
    context.application.create_task(send_ball_result_later(context, chat.id, dice_msg.message_id, user, bet_milli, dice_value, win_milli, balance_after))


async def football_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    register_user(user)
    remember_group(chat)
    if await handle_banned_action(update, context):
        return

    if not await require_ruby_casino(update, context):
        return
    if not context.args:
        await send_result(update, context, '⚽️ <b>Футбол</b>\n\nКоманда: <code>/football сумма</code>\nПример: <code>/football 1</code>')
        return
    bet_milli = parse_money(context.args[0])
    if bet_milli is None or bet_milli <= 0:
        await send_result(update, context, 'Введите ставку числом. Например: <code>/football 1</code>')
        return
    if bet_milli < MIN_FOOTBALL_BET_MILLI:
        await send_result(update, context, f'❗️ Минимальная ставка: <b>{money(MIN_FOOTBALL_BET_MILLI)}</b>')
        return

    if bet_milli > MAX_FOOTBALL_BET_MILLI:
        await send_result(update, context, f"❗️ Максимальная ставка: <b>{money(MAX_FOOTBALL_BET_MILLI)}</b>")
        return
    row = get_user(user.id)
    if not row:
        await send_result(update, context, 'Профиль не найден. Напиши /start.')
        return
    balance_milli = int(row[4])
    if balance_milli < bet_milli:
        await send_result(update, context, f'❌ Недостаточно средств.\nВаш баланс: <b>{money(balance_milli)}</b>')
        return
    left = CASINO_COOLDOWN_SECONDS - (ts() - get_casino_last_spin(user.id))
    if left > 0:
        await send_result(update, context, f'⏲ Подождите еще <b>{left} сек.</b> перед следующей игрой.')
        return
    ok, msg = take_balance(user.id, bet_milli)
    if not ok:
        await send_result(update, context, f'❌ {html.escape(msg)}')
        return
    add_game_stats(user.id, bet_milli)
    dice_msg = await context.bot.send_dice(chat_id=chat.id, emoji='⚽', reply_to_message_id=update.message.message_id if update.message else None)
    dice_value = dice_msg.dice.value if dice_msg.dice else 1
    is_goal = dice_value >= 3
    win_milli = int(round(bet_milli * 1.5)) if is_goal else 0
    if win_milli > 0:
        add_balance(user.id, win_milli)
    set_casino_last_spin(user.id)
    updated = get_user(user.id)
    balance_after = int(updated[4]) if updated else 0
    context.application.create_task(send_football_result_later(context, chat.id, dice_msg.message_id, user, bet_milli, dice_value, win_milli, balance_after))


old_cancel_visual = cancel
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(pe('Действие закрыто.'), parse_mode='HTML')
    if update.effective_chat.type == 'private':
        return ConversationHandler.END




def roll_phrase() -> tuple[str, str]:
    """
    Совместимость для новых override-блоков.
    Берет случайную фразу из таблицы phrases.
    Сначала выбирает редкость по шансам, потом ищет фразу этой редкости.
    Если такой редкости нет — берет любую случайную фразу.
    """
    rarity = roll_role_rarity()

    with db() as conn:
        phrase_cols = columns(conn, 'phrases')

        if 'rarity' in phrase_cols:
            row = conn.execute(
                "SELECT text, rarity FROM phrases WHERE rarity=? ORDER BY RANDOM() LIMIT 1",
                (rarity,),
            ).fetchone()

            if not row:
                row = conn.execute(
                    "SELECT text, rarity FROM phrases ORDER BY RANDOM() LIMIT 1"
                ).fetchone()

            if row:
                return row[0], row[1] or 'common'

        row = conn.execute(
            "SELECT text FROM phrases ORDER BY RANDOM() LIMIT 1"
        ).fetchone()

        if row:
            return row[0], 'common'

    return "Неизвестная роль", "common"


# ===== FINAL EXP EVENTS OVERRIDE =====

def get_user_full(user_id: int):
    with db() as conn:
        user_cols = columns(conn, 'users')
        if 'exp' not in user_cols:
            conn.execute("ALTER TABLE users ADD COLUMN exp INTEGER NOT NULL DEFAULT 0")
            conn.commit()
        if 'games_played' not in user_cols:
            conn.execute("ALTER TABLE users ADD COLUMN games_played INTEGER NOT NULL DEFAULT 0")
            conn.commit()
        if 'turnover_milli' not in user_cols:
            conn.execute("ALTER TABLE users ADD COLUMN turnover_milli INTEGER NOT NULL DEFAULT 0")
            conn.commit()

        return conn.execute(
            """
            SELECT user_id, username, first_name, uid, balance_milli, openings,
                   last_role_at, hidden, casino_last_spin_at, created_at,
                   COALESCE(games_played, 0), COALESCE(turnover_milli, 0),
                   prefix, COALESCE(exp, 0)
            FROM users WHERE user_id=?
            """,
            (user_id,),
        ).fetchone()


def get_user_exp(user_id: int) -> int:
    row = get_user_full(user_id)
    return int(row[13] or 0) if row and len(row) > 13 else 0


def add_user_exp(user_id: int, amount: int) -> int:
    amount = int(amount or 0)

    with db() as conn:
        user_cols = columns(conn, 'users')
        if 'exp' not in user_cols:
            conn.execute("ALTER TABLE users ADD COLUMN exp INTEGER NOT NULL DEFAULT 0")

        conn.execute("UPDATE users SET exp=COALESCE(exp,0)+? WHERE user_id=?", (amount, user_id))
        conn.commit()

        row = conn.execute("SELECT COALESCE(exp,0) FROM users WHERE user_id=?", (user_id,)).fetchone()
        return int(row[0] or 0) if row else 0


def level_info_by_exp(exp: int) -> dict:
    exp = int(exp or 0)
    levels = [
        {'level': 0, 'emoji': '0⃣', 'name': 'None', 'start': 0, 'end': 50},
        {'level': 1, 'emoji': '1⃣', 'name': 'Bronze', 'start': 50, 'end': 150},
        {'level': 2, 'emoji': '2⃣', 'name': 'Silver', 'start': 150, 'end': 350},
        {'level': 3, 'emoji': '3⃣', 'name': 'Gold', 'start': 350, 'end': 700},
        {'level': 4, 'emoji': '⭐', 'name': 'Ruby', 'start': 700, 'end': None},
    ]

    current = levels[0]
    for item in levels:
        if item['end'] is None:
            if exp >= item['start']:
                current = item
        elif item['start'] <= exp < item['end']:
            current = item
            break

    next_level = levels[min(current['level'] + 1, len(levels) - 1)]

    if current['end'] is None:
        percent = 100
        need = 0
    else:
        span = max(1, current['end'] - current['start'])
        percent = int(max(0, min(100, ((exp - current['start']) / span) * 100)))
        need = max(0, current['end'] - exp)

    return {'current': current, 'next': next_level, 'percent': percent, 'need': need, 'exp': exp}


def level_info_by_openings(openings: int) -> dict:
    # Совместимость со старым кодом: теперь уровни считаются по EXP.
    return level_info_by_exp(int(openings or 0))


def user_has_ruby_level(user_id: int) -> bool:
    return level_info_by_exp(get_user_exp(user_id))['current']['name'] == 'Ruby'


def group_event_key(chat_id: int) -> str:
    return f"group_event_until:{chat_id}"


def get_group_event_until(chat_id: int) -> int:
    with db() as conn:
        row = conn.execute("SELECT value FROM meta WHERE key=?", (group_event_key(chat_id),)).fetchone()
        return int(row[0]) if row and str(row[0]).isdigit() else 0


def is_group_event_active(chat_id: int) -> bool:
    return get_group_event_until(chat_id) > ts()


def start_group_event(chat_id: int) -> int:
    until = ts() + GROUP_EVENT_DURATION_SECONDS
    with db() as conn:
        conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (group_event_key(chat_id), str(until)))
        conn.commit()
    return until


def event_time_left_text(chat_id: int) -> str:
    left = max(0, get_group_event_until(chat_id) - ts())
    mins = left // 60
    secs = left % 60
    return f"{mins} мин. {secs} сек."


async def maybe_start_random_group_event(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not is_group(chat):
        return

    if is_group_event_active(chat.id):
        return

    if random.randint(1, 100) <= RANDOM_GROUP_EVENT_CHANCE:
        start_group_event(chat.id)
        await context.bot.send_message(
            chat_id=chat.id,
            text=pe(
                "❗️ <b>Групповое событие началось!</b>\n\n"
                "В течение <b>1 часа</b> опыт за роли умножается на <b>x2</b>."
            ),
            parse_mode='HTML',
            reply_to_message_id=update.message.message_id if update.message else None
        )


async def startchat_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    if not is_group(update.effective_chat):
        await update.message.reply_text(pe('❗️ Команду /startchat нужно использовать в группе.'), parse_mode='HTML')
        return

    start_group_event(update.effective_chat.id)
    await update.message.reply_text(
        pe(
            "❗️ <b>Групповое событие запущено!</b>\n\n"
            "Длительность: <b>1 час</b>\n"
            "Бонус: <b>EXP x2 за получение ролей</b>"
        ),
        parse_mode='HTML'
    )


async def require_ruby_casino(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if user_has_ruby_level(update.effective_user.id):
        return True

    level = level_info_by_exp(get_user_exp(update.effective_user.id))

    await send_result(
        update,
        context,
        "🔒 <b>Казино доступно только с уровня Ruby.</b>\n\n"
        f"Ваш уровень: <b>{level['current']['emoji']} {level['current']['name']}</b>\n"
        f"Ваш опыт: <b>{level['exp']} EXP</b>\n"
        f"До Ruby: <b>{max(0, 700 - level['exp'])} EXP</b>"
    )
    return False


def profile_text(user_id: int) -> str:
    row = get_user_full(user_id)

    if not row:
        return 'Профиль не найден. Напиши /start.'

    _, username, first_name, uid, balance, openings, _, hidden, _, created_at, games_played, turnover_milli, prefix, exp = row
    level = level_info_by_exp(exp)
    display_name = f'@{username}' if username else (first_name or f'Игрок #{uid}')

    status = 'Активен'
    banned, ban_reason, banned_until = get_user_ban_status_direct(user_id)
    if hidden:
        status = 'Скрыт'
    if banned:
        status = f'Бан: {ban_time_text(int(banned_until or 0))}'

    extra = ''
    if prefix:
        extra += f'\n🏷 Префикс — <b>{html.escape(prefix)}</b>'

    try:
        discount = get_case_discount(user_id)
        if discount > 0:
            extra += f'\n💵 Скидка на кейс — <b>{money(discount)}</b>'
    except Exception:
        pass

    try:
        booster = luck_booster_left(user_id)
        if booster > 0:
            extra += f'\n⏱ Бустер удачи — <b>{booster_time_text(booster)}</b>'
    except Exception:
        pass

    return (
        f'<b>#{html.escape(str(uid))} {html.escape(display_name)}</b>\n\n'
        f'💵 Баланс — <b>{money_balance(balance)}</b>\n\n'
        f'Ваш опыт: <b>{level["exp"]} EXP</b>\n'
        f'До следующего LvL: <b>{level["need"]} EXP</b>\n'
        f'{level["current"]["emoji"]} {level["current"]["name"]} ➡️ {level["next"]["emoji"]} {level["next"]["name"]}\n\n'
        f'📰 Открыто ролей — <b>{int(openings or 0)}</b>\n'
        f'🎮 Сыграно — <b>{int(games_played or 0)} ставок</b>\n'
        f'⏱ Аккаунту — <b>{account_age_text(int(created_at or 0))}</b>\n'
        f'Статус — <b>{html.escape(status)}</b>'
        f'{extra}'
    )


async def send_result(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    chat = update.effective_chat
    reply_to = update.message.message_id if getattr(update, 'message', None) else None

    msg = await context.bot.send_message(
        chat.id,
        pe(text),
        parse_mode='HTML',
        reply_markup=reply_markup,
        reply_to_message_id=reply_to
    )

    return msg


async def show_casino(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    if not await require_ruby_casino(update, context):
        return

    text = (
        '🎮 <b>Играть</b>\n\n'
        '🎰 <code>/slots 1</code> — слоты\n'
        '🪙 <code>/coin орел 1</code> — орел / решка\n'
        '🏀 <code>/ball 1</code> — баскетбол\n'
        '⚽️ <code>/football 1</code> — футбол\n'
        '🎁 <code>/case open</code> — кейс\n\n'
        'Минимальная ставка: <b>1 💵</b>\n'
        'Максимальная ставка: <b>10 💵</b>\n'
        'Доступ: <b>только Ruby</b>'
    )

    await send_clean_group_result(update, context, text)


async def send_role(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    register_user(user)
    remember_group(chat)

    if await handle_banned_action(update, context):
        return

    await maybe_start_random_group_event(update, context)

    now = ts()
    row = get_user(user.id)

    if row and now - int(row[6]) < ROLE_COOLDOWN_SECONDS:
        left = ROLE_COOLDOWN_SECONDS - (now - int(row[6]))
        await send_result(update, context, f'⏲ Подождите еще <b>{format_time_left(left)}</b> перед получением новой роли.')
        return

    phrase, rarity = roll_phrase()
    rarity_label = RARITY_LABELS.get(rarity, rarity)
    reward_milli = ROLE_REWARDS_MILLI.get(rarity, 0)

    base_exp = ROLE_EXP_REWARDS.get(rarity, 1)
    multiplier = GROUP_EVENT_EXP_MULTIPLIER if is_group(chat) and is_group_event_active(chat.id) else 1
    exp_added = base_exp * multiplier

    with db() as conn:
        conn.execute(
            "UPDATE users SET balance_milli=balance_milli+?, openings=openings+1, last_role_at=? WHERE user_id=?",
            (reward_milli, now, user.id),
        )
        conn.execute(
            "INSERT INTO user_roles (user_id, phrase, rarity, received_at) VALUES (?, ?, ?, ?)",
            (user.id, phrase, rarity, now),
        )
        conn.commit()

    add_user_exp(user.id, exp_added)

    event_line = ""
    if multiplier > 1:
        event_line = "\n❗️ Событие активно: <b>EXP x2</b>"

    await send_result(
        update,
        context,
        f"{mention(user)} — <b>{html.escape(phrase)}</b>\n"
        f"Редкость: <b>{html.escape(rarity_label)}</b>\n"
        f"Добавлено: <b>+{money(reward_milli)}</b>\n"
        f"👏 Добавлено EXP: <b>+{exp_added}</b>"
        f"{event_line}",
        reply_markup=role_menu(group=is_group(chat))
    )

    await send_role_log(context, user, phrase, rarity_label, reward_milli)


async def clear_money_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    with db() as conn:
        cur = conn.execute("UPDATE users SET balance_milli=0")
        conn.commit()
        count = cur.rowcount if cur.rowcount is not None else 0

    await update.message.reply_text(
        pe(f'💵 <b>Баланс очищен у всех игроков.</b>\nИгроков обновлено: <b>{count}</b>'),
        parse_mode='HTML'
    )

# ===== END FINAL EXP EVENTS OVERRIDE =====


# ===== FINAL TRIGGER BUTTONS FIX =====

async def trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    raw_text = update.message.text.strip()
    txt = raw_text.lower()

    if context.user_data.get('waiting_promo_activate'):
        context.user_data['waiting_promo_activate'] = False
        ok, msg = activate_promo_code(update.effective_user.id, raw_text)
        await update.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')
        return

    if txt in ('🏠 главное меню', 'главное меню'):
        await open_main_screen(update, context)
        return

    if txt in TRIGGERS or txt in ('я', 'кто', 'кто я', 'кто я?', '🎭 кто я'):
        await send_role(update, context)
        return

    if txt in ('играть', '🎮 играть', 'казино', '🎰 казино'):
        await show_casino(update, context)
        return

    if txt in ('профиль', '👤 профиль'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('Профиль доступен только в личке с ботом.'), parse_mode='HTML')
            return

        await send_result(update, context, profile_text(update.effective_user.id), reply_markup=profile_actions_menu())
        return

    if txt in ('топ 3', '🏆 топ 3', 'топ'):
        await send_clean_group_result(update, context, top_text())
        return

    if txt in ('передача денег', '💵 передача денег'):
        await send_result(update, context, transfer_usage_text())
        return

    if txt in ('промокод', '🎁 промокод'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('Промокоды доступны только в личке с ботом.'), parse_mode='HTML')
            return

        await update.message.reply_text(pe('🎁 Введите промокод одним сообщением:'), parse_mode='HTML')
        context.user_data['waiting_promo_activate'] = True
        return


async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ''

    register_user(q.from_user)

    if q.message:
        remember_group(q.message.chat)

    if data.startswith('wd_ok:') or data.startswith('wd_no:'):
        await q.answer()

        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return

        try:
            wid = int(data.split(':', 1)[1])
        except Exception:
            await q.message.reply_text(pe('❌ Ошибка заявки.'), parse_mode='HTML')
            return

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
            return

        if set_withdrawal(wid, 'declined', q.from_user.id):
            add_balance(target, amount)
            await q.edit_message_text(pe(f'❌ Заявка #{wid} отклонена.\nСумма возвращена пользователю: {money(amount)}'), parse_mode='HTML')
            try:
                await context.bot.send_message(target, pe(f'❌ Ваша заявка на вывод {money(amount)} отклонена. Средства возвращены на баланс.'), parse_mode='HTML')
            except Exception:
                pass
        return

    if data == 'whoami':
        await q.answer()
        await send_role(update, context)
        return

    if data == 'top3':
        await q.answer()
        await send_clean_group_result(update, context, top_text())
        return

    if data == 'casino':
        await q.answer()
        await show_casino(update, context)
        return

    if data == 'profile':
        await q.answer()

        if q.message.chat.type != 'private':
            await q.message.reply_text(pe('Профиль доступен только в личке с ботом.'), parse_mode='HTML')
            return

        await q.edit_message_text(pe(profile_text(q.from_user.id)), parse_mode='HTML', reply_markup=profile_actions_menu())
        return

    if data == 'profile_stats':
        await q.answer()

        if q.message.chat.type != 'private':
            await q.message.reply_text(pe('Статистика доступна только в личке с ботом.'), parse_mode='HTML')
            return

        await q.edit_message_text(pe(profile_stats_text(q.from_user.id)), parse_mode='HTML', reply_markup=stats_actions_menu())
        return

    if data == 'back':
        await q.answer()

        if is_group(q.message.chat):
            await q.edit_message_text(pe('Главное меню:'), parse_mode='HTML', reply_markup=main_menu(is_admin(q.from_user.id), group=True))
        else:
            await q.edit_message_text(pe(main_dashboard_text()), parse_mode='HTML', reply_markup=dashboard_message_menu())
        return

    if data == 'transfer_money':
        await q.answer()
        await send_result(update, context, transfer_usage_text())
        return

    if data == 'promo_list':
        await q.answer()

        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return

        await q.message.reply_text(pe(promo_codes_text()), parse_mode='HTML')
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
            await send_result(update, context, '❌ Ошибка ставки.')
            return

        side = normalize_coin_side(parts[1])
        amount = parse_money(parts[2])

        if side is None or amount is None:
            await send_result(update, context, '❌ Ошибка ставки.')
            return

        await play_coin(update, context, side, amount)
        return

    if data.startswith('repeat:'):
        await q.answer()
        parts = data.split(':')
        game = parts[1] if len(parts) > 1 else ''

        try:
            if game == 'slots' and len(parts) == 3:
                return await play_slots(update, context, int(parts[2]))

            if game == 'coin' and len(parts) == 4:
                return await play_coin(update, context, parts[2], int(parts[3]))

            if game == 'ball' and len(parts) == 3:
                context.args = [str(int(parts[2]) / 1000)]
                return await ball_cmd(update, context)

            if game == 'football' and len(parts) == 3:
                context.args = [str(int(parts[2]) / 1000)]
                return await football_cmd(update, context)

        except Exception:
            await q.message.reply_text(pe('❌ Не удалось повторить игру.'), parse_mode='HTML')
            return

    if data.startswith('bonus:'):
        msg = claim_bonus(data.split(':', 1)[1], q.from_user.id)
        await q.answer(msg, show_alert=True)
        return

    if data == 'admin_menu':
        await q.answer()

        if is_admin(q.from_user.id):
            await q.edit_message_text(pe(admin_panel_text()), parse_mode='HTML')
        else:
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    if data == 'last_phrases':
        await q.answer()
        rows = last_phrases(10)
        msg = 'Фраз пока нет.' if not rows else '📋 Последние фразы:\n\n' + '\n'.join(
            f'{pid}. [{RARITY_LABELS.get(rarity, rarity)}] {html.escape(txt)}'
            for pid, txt, rarity in rows
        )
        await q.edit_message_text(pe(msg), parse_mode='HTML', reply_markup=admin_menu())
        return

    if data == 'phrase_count':
        await q.answer()
        await q.edit_message_text(
            pe(f'🔢 В базе фраз: {phrase_count()}'),
            reply_markup=admin_menu(),
            parse_mode='HTML'
        )
        return

    if data == 'admin_stats':
        await q.answer()

        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return

        await send_long_message(context.bot, q.message.chat.id, admin_stats_text(), reply_markup=admin_menu())
        return

    if data == 'groups':
        await q.answer()

        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return

        await q.message.reply_text(pe(groups_text()), parse_mode='HTML')
        return

    await q.answer()

# ===== END FINAL TRIGGER BUTTONS FIX =====


# ===== FINAL DAILY EXP ADMIN EVENTS FIX =====

DAILY_EXP_AMOUNT = 60
DAILY_EXP_COOLDOWN_SECONDS = 24 * 60 * 60


def today_key() -> int:
    return day_start()


def ensure_daily_exp_table():
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_exp_claims (
                user_id INTEGER PRIMARY KEY,
                claimed_day INTEGER NOT NULL,
                claimed_at INTEGER NOT NULL
            )
            """
        )
        user_cols = columns(conn, 'users')
        if 'exp' not in user_cols:
            conn.execute("ALTER TABLE users ADD COLUMN exp INTEGER NOT NULL DEFAULT 0")
        conn.commit()


def can_claim_daily_exp(user_id: int) -> tuple[bool, int]:
    ensure_daily_exp_table()
    today = today_key()

    with db() as conn:
        row = conn.execute(
            "SELECT claimed_day FROM daily_exp_claims WHERE user_id=?",
            (user_id,),
        ).fetchone()

    if row and int(row[0]) == today:
        return False, day_start() + DAY_SECONDS - ts()

    return True, 0


def claim_daily_exp(user_id: int) -> tuple[bool, str]:
    ensure_daily_exp_table()
    ok, left = can_claim_daily_exp(user_id)

    if not ok:
        return False, f"Ежедневный EXP уже получен.\nВернитесь через <b>{format_time_left(left)}</b>."

    add_user_exp(user_id, DAILY_EXP_AMOUNT)

    with db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO daily_exp_claims (user_id, claimed_day, claimed_at) VALUES (?, ?, ?)",
            (user_id, today_key(), ts()),
        )
        conn.commit()

    level = level_info_by_exp(get_user_exp(user_id))

    return True, (
        f"👏 <b>Ежедневный EXP получен!</b>\n\n"
        f"Добавлено EXP: <b>+{DAILY_EXP_AMOUNT}</b>\n"
        f"Ваш опыт: <b>{level['exp']} EXP</b>\n"
        f"До следующего LvL: <b>{level['need']} EXP</b>"
    )


async def daily_exp_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    ok, msg = claim_daily_exp(update.effective_user.id)
    await send_result(update, context, ("✅ " if ok else "⏱ ") + msg)


def dashboard_message_menu():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton('👤 Профиль', callback_data='profile'),
            InlineKeyboardButton('📊 Статистика', callback_data='profile_stats'),
        ],
        [
            InlineKeyboardButton('👏 Ежедневный EXP', callback_data='daily_exp'),
            InlineKeyboardButton('💸 Вывести', callback_data='withdraw'),
        ],
        [
            InlineKeyboardButton('👥 Группа', url='https://t.me/bezdnao'),
        ],
    ])


def reply_main_menu(admin=False, group=False):
    if group:
        rows = []
    else:
        rows = [
            ['🎭 Кто я', '🎮 Играть'],
            ['👤 Профиль', '🏆 Топ 3'],
            ['👏 Ежедневный EXP', '🎁 Промокод'],
            ['💵 Передача денег', '🏠 Главное меню'],
        ]

    return ReplyKeyboardMarkup(
        rows,
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder='Выберите действие...'
    )


def main_dashboard_text() -> str:
    return (
        f"👋 <b>Добро пожаловать в {BOT_PUBLIC_USERNAME}</b>\n\n"
        "🎮 Получай роли, копи EXP и открывай новые уровни.\n"
        "👏 Ежедневный EXP: <b>+60 EXP</b>\n"
        f"💬 Новости и конкурсы — {BOT_GROUP_LINK}"
    )


async def startchat_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)

    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    if update.effective_chat.type != 'private':
        await update.message.reply_text(
            pe('❗️ Команду /startchat нужно использовать в ЛС с ботом.\nОна запускает событие сразу во всех группах.'),
            parse_mode='HTML'
        )
        return

    with db() as conn:
        rows = conn.execute("SELECT chat_id FROM groups").fetchall()

    if not rows:
        await update.message.reply_text(pe('❗️ Группы с ботом не найдены.'), parse_mode='HTML')
        return

    started = 0
    for (chat_id,) in rows:
        try:
            start_group_event(int(chat_id))
            await context.bot.send_message(
                chat_id=int(chat_id),
                text=pe(
                    "❗️ <b>Групповое событие началось!</b>\n\n"
                    "Длительность: <b>1 час</b>\n"
                    "Бонус: <b>EXP x2 за получение ролей</b>"
                ),
                parse_mode='HTML'
            )
            started += 1
        except Exception:
            pass

    await update.message.reply_text(
        pe(f'❗️ <b>Групповое событие запущено.</b>\nГрупп: <b>{started}</b>'),
        parse_mode='HTML'
    )


async def expgive_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    if len(context.args) < 2:
        await update.message.reply_text(
            pe('👏 <b>Выдать EXP</b>\n\nКоманда: <code>/expgive USER_ID EXP</code>\nПример: <code>/expgive 123456789 100</code>'),
            parse_mode='HTML'
        )
        return

    try:
        user_id = int(context.args[0])
        amount = int(context.args[1])
    except Exception:
        await update.message.reply_text(pe('❌ USER_ID и EXP должны быть числами.'), parse_mode='HTML')
        return

    if amount <= 0:
        await update.message.reply_text(pe('❌ EXP должен быть больше 0.'), parse_mode='HTML')
        return

    row = get_user_full(user_id)
    if not row:
        await update.message.reply_text(pe('❌ Пользователь не найден в базе.'), parse_mode='HTML')
        return

    new_exp = add_user_exp(user_id, amount)
    level = level_info_by_exp(new_exp)

    await update.message.reply_text(
        pe(
            f'👏 <b>EXP выдан.</b>\n\n'
            f'ID: <code>{user_id}</code>\n'
            f'Добавлено: <b>+{amount} EXP</b>\n'
            f'Теперь: <b>{new_exp} EXP</b>\n'
            f'Уровень: <b>{level["current"]["emoji"]} {level["current"]["name"]}</b>'
        ),
        parse_mode='HTML'
    )


async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ''

    register_user(q.from_user)

    if q.message:
        remember_group(q.message.chat)

    if data == 'daily_exp':
        await q.answer()
        ok, msg = claim_daily_exp(q.from_user.id)
        await q.message.reply_text(pe(("✅ " if ok else "⏱ ") + msg), parse_mode='HTML')
        return

    if data == 'profile_stats' or data == 'stats':
        await q.answer()

        if q.message.chat.type != 'private':
            await q.message.reply_text(pe('📊 Статистика доступна только в личке с ботом.'), parse_mode='HTML')
            return

        await q.edit_message_text(pe(profile_stats_text(q.from_user.id)), parse_mode='HTML', reply_markup=stats_actions_menu())
        return

    if data.startswith('wd_ok:') or data.startswith('wd_no:'):
        await q.answer()

        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return

        try:
            wid = int(data.split(':', 1)[1])
        except Exception:
            await q.message.reply_text(pe('❌ Ошибка заявки.'), parse_mode='HTML')
            return

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
            return

        if set_withdrawal(wid, 'declined', q.from_user.id):
            add_balance(target, amount)
            await q.edit_message_text(pe(f'❌ Заявка #{wid} отклонена.\nСумма возвращена пользователю: {money(amount)}'), parse_mode='HTML')
            try:
                await context.bot.send_message(target, pe(f'❌ Ваша заявка на вывод {money(amount)} отклонена. Средства возвращены на баланс.'), parse_mode='HTML')
            except Exception:
                pass
        return

    if data == 'whoami':
        await q.answer()
        await send_role(update, context)
        return

    if data == 'top3':
        await q.answer()
        await send_clean_group_result(update, context, top_text())
        return

    if data == 'casino':
        await q.answer()
        await show_casino(update, context)
        return

    if data == 'profile':
        await q.answer()

        if q.message.chat.type != 'private':
            await q.message.reply_text(pe('👤 Профиль доступен только в личке с ботом.'), parse_mode='HTML')
            return

        await q.edit_message_text(pe(profile_text(q.from_user.id)), parse_mode='HTML', reply_markup=profile_actions_menu())
        return

    if data == 'back':
        await q.answer()
        if is_group(q.message.chat):
            await q.edit_message_text(pe('Главное меню:'), parse_mode='HTML', reply_markup=main_menu(is_admin(q.from_user.id), group=True))
        else:
            await q.edit_message_text(pe(main_dashboard_text()), parse_mode='HTML', reply_markup=dashboard_message_menu())
        return

    if data == 'transfer_money':
        await q.answer()
        await send_result(update, context, transfer_usage_text())
        return

    if data == 'promo_list':
        await q.answer()
        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return
        await q.message.reply_text(pe(promo_codes_text()), parse_mode='HTML')
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
            await send_result(update, context, '❌ Ошибка ставки.')
            return
        side = normalize_coin_side(parts[1])
        amount = parse_money(parts[2])
        if side is None or amount is None:
            await send_result(update, context, '❌ Ошибка ставки.')
            return
        await play_coin(update, context, side, amount)
        return

    if data.startswith('repeat:'):
        await q.answer()
        parts = data.split(':')
        game = parts[1] if len(parts) > 1 else ''
        try:
            if game == 'slots' and len(parts) == 3:
                return await play_slots(update, context, int(parts[2]))
            if game == 'coin' and len(parts) == 4:
                return await play_coin(update, context, parts[2], int(parts[3]))
            if game == 'ball' and len(parts) == 3:
                context.args = [str(int(parts[2]) / 1000)]
                return await ball_cmd(update, context)
            if game == 'football' and len(parts) == 3:
                context.args = [str(int(parts[2]) / 1000)]
                return await football_cmd(update, context)
        except Exception:
            await q.message.reply_text(pe('❌ Не удалось повторить игру.'), parse_mode='HTML')
            return

    if data.startswith('bonus:'):
        msg = claim_bonus(data.split(':', 1)[1], q.from_user.id)
        await q.answer(msg, show_alert=True)
        return

    if data == 'admin_menu':
        await q.answer()
        if is_admin(q.from_user.id):
            await q.edit_message_text(pe(admin_panel_text()), parse_mode='HTML')
        else:
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    if data == 'last_phrases':
        await q.answer()
        rows = last_phrases(10)
        msg = 'Фраз пока нет.' if not rows else '📋 Последние фразы:\n\n' + '\n'.join(
            f'{pid}. [{RARITY_LABELS.get(rarity, rarity)}] {html.escape(txt)}'
            for pid, txt, rarity in rows
        )
        await q.edit_message_text(pe(msg), parse_mode='HTML', reply_markup=admin_menu())
        return

    if data == 'phrase_count':
        await q.answer()
        await q.edit_message_text(pe(f'🔢 В базе фраз: {phrase_count()}'), reply_markup=admin_menu(), parse_mode='HTML')
        return

    if data == 'admin_stats':
        await q.answer()
        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return
        await send_long_message(context.bot, q.message.chat.id, admin_stats_text(), reply_markup=admin_menu())
        return

    if data == 'groups':
        await q.answer()
        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return
        await q.message.reply_text(pe(groups_text()), parse_mode='HTML')
        return

    await q.answer()


async def trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    raw_text = update.message.text.strip()
    txt = raw_text.lower()

    if context.user_data.get('waiting_promo_activate'):
        context.user_data['waiting_promo_activate'] = False
        ok, msg = activate_promo_code(update.effective_user.id, raw_text)
        await update.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')
        return

    if txt in ('🏠 главное меню', 'главное меню'):
        await open_main_screen(update, context)
        return

    if txt in ('👏 ежедневный exp', 'ежедневный exp', 'ежедневный опыт', '/dailyexp'):
        await daily_exp_cmd(update, context)
        return

    if txt in TRIGGERS or txt in ('я', 'кто', 'кто я', 'кто я?', '🎭 кто я'):
        await send_role(update, context)
        return

    if txt in ('играть', '🎮 играть', 'казино', '🎰 казино'):
        await show_casino(update, context)
        return

    if txt in ('профиль', '👤 профиль'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('👤 Профиль доступен только в личке с ботом.'), parse_mode='HTML')
            return
        await send_result(update, context, profile_text(update.effective_user.id), reply_markup=profile_actions_menu())
        return

    if txt in ('топ 3', '🏆 топ 3', 'топ'):
        await send_clean_group_result(update, context, top_text())
        return

    if txt in ('передача денег', '💵 передача денег'):
        await send_result(update, context, transfer_usage_text())
        return

    if txt in ('промокод', '🎁 промокод'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('🎁 Промокоды доступны только в личке с ботом.'), parse_mode='HTML')
            return
        await update.message.reply_text(pe('🎁 Введите промокод одним сообщением:'), parse_mode='HTML')
        context.user_data['waiting_promo_activate'] = True
        return

# ===== END FINAL DAILY EXP ADMIN EVENTS FIX =====


# ===== FINAL GOLD CASINO 6 PAYLIMIT EXPTAKE =====

PAY_DAILY_LIMIT = 5

def level_info_by_exp(exp: int) -> dict:
    exp = int(exp or 0)
    levels = [
        {'level': 0, 'emoji': '0⃣', 'name': 'None', 'start': 0, 'end': 50},
        {'level': 1, 'emoji': '1⃣', 'name': 'Bronze', 'start': 50, 'end': 150},
        {'level': 2, 'emoji': '2⃣', 'name': 'Silver', 'start': 150, 'end': 350},
        {'level': 3, 'emoji': '3⃣', 'name': 'Gold', 'start': 350, 'end': 700},
        {'level': 4, 'emoji': '⭐', 'name': 'Ruby', 'start': 700, 'end': None},
    ]
    current = levels[0]
    for item in levels:
        if item['end'] is None:
            if exp >= item['start']:
                current = item
        elif item['start'] <= exp < item['end']:
            current = item
            break
    next_level = levels[min(current['level'] + 1, len(levels) - 1)]
    if current['end'] is None:
        percent, need = 100, 0
    else:
        span = max(1, current['end'] - current['start'])
        percent = int(max(0, min(100, ((exp - current['start']) / span) * 100)))
        need = max(0, current['end'] - exp)
    return {'current': current, 'next': next_level, 'percent': percent, 'need': need, 'exp': exp}


def user_has_gold_level(user_id: int) -> bool:
    level_name = level_info_by_exp(get_user_exp(user_id))['current']['name']
    return level_name in ('Gold', 'Ruby')


async def require_gold_casino(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if user_has_gold_level(update.effective_user.id):
        return True
    level = level_info_by_exp(get_user_exp(update.effective_user.id))
    await send_result(
        update,
        context,
        "🔒 <b>Казино доступно с уровня Gold.</b>\n\n"
        f"Ваш уровень: <b>{level['current']['emoji']} {level['current']['name']}</b>\n"
        f"Ваш опыт: <b>{level['exp']} EXP</b>\n"
        f"До Gold: <b>{max(0, 350 - level['exp'])} EXP</b>"
    )
    return False


async def require_ruby_casino(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    return await require_gold_casino(update, context)


def ensure_pay_limits_table():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pay_daily_limits (
                user_id INTEGER NOT NULL,
                day INTEGER NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (user_id, day)
            )
        """)
        conn.commit()


def get_pay_count_today(user_id: int) -> int:
    ensure_pay_limits_table()
    with db() as conn:
        row = conn.execute("SELECT count FROM pay_daily_limits WHERE user_id=? AND day=?", (user_id, day_start())).fetchone()
    return int(row[0] or 0) if row else 0


def increment_pay_count_today(user_id: int):
    ensure_pay_limits_table()
    with db() as conn:
        conn.execute("""
            INSERT INTO pay_daily_limits (user_id, day, count)
            VALUES (?, ?, 1)
            ON CONFLICT(user_id, day) DO UPDATE SET count=count+1
        """, (user_id, day_start()))
        conn.commit()


async def exptake_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return
    if len(context.args) < 2:
        await update.message.reply_text(pe('👏 <b>Забрать EXP</b>\n\nКоманда: <code>/exptake USER_ID EXP</code>\nПример: <code>/exptake 123456789 100</code>'), parse_mode='HTML')
        return
    try:
        user_id = int(context.args[0])
        amount = int(context.args[1])
    except Exception:
        await update.message.reply_text(pe('❌ USER_ID и EXP должны быть числами.'), parse_mode='HTML')
        return
    if amount <= 0:
        await update.message.reply_text(pe('❌ EXP должен быть больше 0.'), parse_mode='HTML')
        return
    row = get_user_full(user_id)
    if not row:
        await update.message.reply_text(pe('❌ Пользователь не найден в базе.'), parse_mode='HTML')
        return
    current = get_user_exp(user_id)
    new_exp = max(0, current - amount)
    with db() as conn:
        conn.execute("UPDATE users SET exp=? WHERE user_id=?", (new_exp, user_id))
        conn.commit()
    level = level_info_by_exp(new_exp)
    await update.message.reply_text(
        pe(f'👏 <b>EXP забран.</b>\n\nID: <code>{user_id}</code>\nЗабрано: <b>-{amount} EXP</b>\nТеперь: <b>{new_exp} EXP</b>\nУровень: <b>{level["current"]["emoji"]} {level["current"]["name"]}</b>'),
        parse_mode='HTML'
    )


async def show_casino(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)
    if await handle_banned_action(update, context):
        return
    if not await require_gold_casino(update, context):
        return
    await send_clean_group_result(update, context,
        '🎮 <b>Играть</b>\n\n'
        '🎰 <code>/slots 1</code> — слоты\n'
        '🪙 <code>/coin орел 1</code> — орел / решка\n'
        '🏀 <code>/ball 1</code> — баскетбол\n'
        '⚽️ <code>/football 1</code> — футбол\n'
        '🎁 <code>/case open</code> — кейс\n\n'
        'Минимальная ставка: <b>1 💵</b>\n'
        'Максимальная ставка: <b>100 💵</b>\n'
        'Доступ: <b>с уровня Gold</b>'
    )


async def pay_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)
    if await handle_banned_action(update, context):
        return
    used = get_pay_count_today(update.effective_user.id)
    if used >= PAY_DAILY_LIMIT:
        await send_result(update, context, f"💵 <b>Лимит переводов на сегодня исчерпан.</b>\n\nИспользовано: <b>{used}/{PAY_DAILY_LIMIT}</b>")
        return
    if len(context.args) < 2:
        await send_result(update, context, transfer_usage_text())
        return
    target_raw = context.args[0]
    amount = parse_money(context.args[1])
    comment = ' '.join(context.args[2:]).strip() or 'без комментария'
    if amount is None or amount <= 0:
        await send_result(update, context, '❌ Введите сумму числом.')
        return
    target_id = resolve_user_id(target_raw)
    if not target_id:
        await send_result(update, context, '❌ Получатель не найден. Используй USER_ID или @username.')
        return
    if target_id == update.effective_user.id:
        await send_result(update, context, '❌ Нельзя переводить самому себе.')
        return
    sender = get_user(update.effective_user.id)
    if not sender:
        await send_result(update, context, '❌ Профиль не найден. Напиши /start.')
        return
    sender_balance = int(sender[4])
    if sender_balance < amount:
        await send_result(update, context, f'❌ Недостаточно средств.\nВаш баланс: <b>{money(sender_balance)}</b>')
        return
    ok, msg = take_balance(update.effective_user.id, amount)
    if not ok:
        await send_result(update, context, f'❌ {html.escape(msg)}')
        return
    add_balance(target_id, amount)
    increment_pay_count_today(update.effective_user.id)
    sender_name = mention(update.effective_user)
    await send_result(update, context,
        f"💵 <b>Перевод выполнен</b>\n\n"
        f"Сумма: <b>{money(amount)}</b>\n"
        f"Получатель: <code>{html.escape(str(target_raw))}</code>\n"
        f"Комментарий: <b>{html.escape(comment)}</b>\n"
        f"Лимит сегодня: <b>{used + 1}/{PAY_DAILY_LIMIT}</b>"
    )
    try:
        await context.bot.send_message(target_id, pe(
            f"🎁 <b>Новый перевод</b>\n\n"
            f"👤 От кого: {sender_name}\n"
            f"💵 Сумма: <b>{money(amount)}</b>\n"
            f"💬 Сообщение: <b>{html.escape(comment)}</b>"
        ), parse_mode='HTML')
    except Exception:
        pass

# ===== END FINAL GOLD CASINO 6 PAYLIMIT EXPTAKE =====





# ===== FINAL PAY REPLY_AND_ID GROUPS FIX =====

def transfer_usage_text() -> str:
    return (
        '💵 <b>Передача денег</b>\n\n'
        '<b>В группе реплаем:</b>\n'
        '<code>/pay сумма комментарий</code>\n\n'
        '<b>В группе по ID / username:</b>\n'
        '<code>/pay USER_ID сумма комментарий</code>\n'
        '<code>/pay @username сумма комментарий</code>\n\n'
        '<b>В ЛС:</b>\n'
        '<code>/pay USER_ID сумма комментарий</code>\n'
        '<code>/pay @username сумма комментарий</code>\n\n'
        'Пример реплаем: <code>/pay 1 подарок</code>\n'
        'Пример по ID: <code>/pay 123456789 1 подарок</code>'
    )


async def pay_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    used = get_pay_count_today(update.effective_user.id)
    if used >= PAY_DAILY_LIMIT:
        await send_result(
            update,
            context,
            f"💵 <b>Лимит переводов на сегодня исчерпан.</b>\n\n"
            f"Использовано: <b>{used}/{PAY_DAILY_LIMIT}</b>"
        )
        return

    target_id = None
    target_label = None
    amount = None
    comment = 'без комментария'

    if is_group(update.effective_chat):
        # В группе два варианта:
        # 1) Реплай: /pay сумма комментарий
        # 2) По ID/@username: /pay USER_ID сумма комментарий
        if not context.args:
            await send_result(update, context, transfer_usage_text())
            return

        first_arg_amount = parse_money(context.args[0])

        if update.message.reply_to_message and update.message.reply_to_message.from_user and first_arg_amount is not None:
            target_user = update.message.reply_to_message.from_user

            if target_user.is_bot:
                await send_result(update, context, '❌ Нельзя переводить деньги боту.')
                return

            register_user(target_user)

            target_id = target_user.id
            target_label = mention(target_user)
            amount = first_arg_amount
            comment = ' '.join(context.args[1:]).strip() or 'без комментария'

        else:
            if len(context.args) < 2:
                await send_result(
                    update,
                    context,
                    '💵 <b>Передача денег в группе</b>\n\n'
                    'Реплаем: <code>/pay сумма комментарий</code>\n'
                    'По ID: <code>/pay USER_ID сумма комментарий</code>'
                )
                return

            target_raw = context.args[0]
            amount = parse_money(context.args[1])
            comment = ' '.join(context.args[2:]).strip() or 'без комментария'
            target_id = resolve_user_id(target_raw)
            target_label = html.escape(str(target_raw))

            if not target_id:
                await send_result(update, context, '❌ Получатель не найден. Используй USER_ID или @username, либо сделай реплай.')
                return

    else:
        # В ЛС только старый формат: /pay USER_ID сумма комментарий
        if len(context.args) < 2:
            await send_result(update, context, transfer_usage_text())
            return

        target_raw = context.args[0]
        amount = parse_money(context.args[1])
        comment = ' '.join(context.args[2:]).strip() or 'без комментария'
        target_id = resolve_user_id(target_raw)
        target_label = html.escape(str(target_raw))

        if not target_id:
            await send_result(update, context, '❌ Получатель не найден. Используй USER_ID или @username.')
            return

    if amount is None or amount <= 0:
        await send_result(update, context, '❌ Введите сумму числом.')
        return

    if target_id == update.effective_user.id:
        await send_result(update, context, '❌ Нельзя переводить самому себе.')
        return

    sender = get_user(update.effective_user.id)
    if not sender:
        await send_result(update, context, '❌ Профиль не найден. Напиши /start.')
        return

    sender_balance = int(sender[4])
    if sender_balance < amount:
        await send_result(update, context, f'❌ Недостаточно средств.\nВаш баланс: <b>{money(sender_balance)}</b>')
        return

    ok, msg = take_balance(update.effective_user.id, amount)
    if not ok:
        await send_result(update, context, f'❌ {html.escape(msg)}')
        return

    add_balance(target_id, amount)
    increment_pay_count_today(update.effective_user.id)

    sender_name = mention(update.effective_user)

    await send_result(
        update,
        context,
        f"💵 <b>Перевод выполнен</b>\n\n"
        f"От кого: {sender_name}\n"
        f"Кому: {target_label}\n"
        f"Сумма: <b>{money(amount)}</b>\n"
        f"Комментарий: <b>{html.escape(comment)}</b>\n"
        f"Лимит сегодня: <b>{used + 1}/{PAY_DAILY_LIMIT}</b>"
    )

    try:
        await context.bot.send_message(
            target_id,
            pe(
                f"🎁 <b>Новый перевод</b>\n\n"
                f"👤 От кого: {sender_name}\n"
                f"💵 Сумма: <b>{money(amount)}</b>\n"
                f"💬 Сообщение: <b>{html.escape(comment)}</b>"
            ),
            parse_mode='HTML'
        )
    except Exception:
        pass

# ===== END FINAL PAY REPLY_AND_ID GROUPS FIX =====


# ===== FINAL CASINO RUSSIAN_BASKET_CUBE FIX =====

CUBE_ANIMATION_DELAY = 4


async def require_gold_casino(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    return True


async def require_ruby_casino(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    return True


def cube_multiplier(sides_count: int) -> float:
    if sides_count <= 1:
        return 5.0
    if sides_count == 2:
        return 2.5
    return 1.7


def cube_result_text(user, bet_milli: int, chosen_sides: list[int], dice_value: int, win_milli: int, balance_after: int) -> str:
    sides_text = ', '.join(str(x) for x in chosen_sides)

    if dice_value in chosen_sides:
        headline = f'Выигрыш <b>{money(win_milli)}</b> в игре 🎲'
        detail = 'Куб выпал на выбранную сторону!'
    else:
        headline = f'Проигрыш <b>{money(bet_milli)}</b> в игре 🎲'
        detail = 'Куб выпал не на твою сторону :('

    return (
        f'{mention(user)}\n'
        f'{headline}\n'
        f'Вы выбрали: <b>{html.escape(sides_text)}</b>\n'
        f'Выпало: <b>{dice_value}</b>\n\n'
        f'{detail}\n\n'
        f'💵 Баланс <b>{money_balance(balance_after)}</b>'
    )


async def send_cube_result_later(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, user, bet_milli: int, chosen_sides: list[int], dice_value: int, win_milli: int, balance_after: int):
    await asyncio.sleep(CUBE_ANIMATION_DELAY)
    await context.bot.send_message(
        chat_id=chat_id,
        text=pe(cube_result_text(user, bet_milli, chosen_sides, dice_value, win_milli, balance_after)),
        parse_mode='HTML',
        reply_to_message_id=message_id,
        reply_markup=repeat_game_menu('cube', bet_milli, ','.join(str(x) for x in chosen_sides)),
    )


def repeat_game_menu(game: str, bet_milli: int, side: str | None = None):
    if game == 'ball':
        data = f'repeat:ball:{bet_milli}'
    elif game == 'cube':
        data = f'repeat:cube:{side}:{bet_milli}'
    else:
        return None

    return InlineKeyboardMarkup([[InlineKeyboardButton('🔁 Повторить игру', callback_data=data)]])


async def show_casino(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    text = (
        '🎮 <b>Играть</b>\n\n'
        '🏀 <code>баскетбол 1</code> — баскетбол\n'
        '🎲 <code>куб 2 3 4 1</code> — куб\n\n'
        'В кубе можно выбрать от <b>1</b> до <b>3</b> сторон.\n'
        'Чем меньше сторон выбрано — тем больше выигрыш.\n\n'
        'Минимальная ставка: <b>1 💵</b>\n'
        'Максимальная ставка: <b>100 💵</b>'
    )

    await send_clean_group_result(update, context, text)


async def ball_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    register_user(user)
    remember_group(chat)

    if await handle_banned_action(update, context):
        return

    if not context.args:
        await send_result(update, context, '🏀 <b>Баскетбол</b>\n\nКоманда: <code>баскетбол сумма</code>\nПример: <code>баскетбол 1</code>')
        return

    bet_milli = parse_money(context.args[0])
    if bet_milli is None or bet_milli <= 0:
        await send_result(update, context, 'Введите ставку числом. Например: <code>баскетбол 1</code>')
        return

    if bet_milli < MIN_BALL_BET_MILLI:
        await send_result(update, context, f'❗️ Минимальная ставка: <b>{money(MIN_BALL_BET_MILLI)}</b>')
        return

    if bet_milli > MAX_BALL_BET_MILLI:
        await send_result(update, context, f'❗️ Максимальная ставка: <b>{money(MAX_BALL_BET_MILLI)}</b>')
        return

    row = get_user(user.id)
    if not row:
        await send_result(update, context, 'Профиль не найден. Напиши /start.')
        return

    balance_milli = int(row[4])
    if balance_milli < bet_milli:
        await send_result(update, context, f'❌ Недостаточно средств.\nВаш баланс: <b>{money(balance_milli)}</b>')
        return

    ok, msg = take_balance(user.id, bet_milli)
    if not ok:
        await send_result(update, context, f'❌ {html.escape(msg)}')
        return

    add_game_stats(user.id, bet_milli)

    dice_msg = await context.bot.send_dice(
        chat_id=chat.id,
        emoji='🏀',
        reply_to_message_id=update.message.message_id if update.message else None
    )

    dice_value = dice_msg.dice.value if dice_msg.dice else 1
    is_hit = dice_value >= 4
    win_milli = bet_milli * 2 if is_hit else 0

    if win_milli > 0:
        add_balance(user.id, win_milli)

    updated = get_user(user.id)
    balance_after = int(updated[4]) if updated else 0

    context.application.create_task(
        send_ball_result_later(context, chat.id, dice_msg.message_id, user, bet_milli, dice_value, win_milli, balance_after)
    )


async def cube_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    register_user(user)
    remember_group(chat)

    if await handle_banned_action(update, context):
        return

    if len(context.args) < 2:
        await send_result(update, context, '🎲 <b>Куб</b>\n\nКоманда: <code>куб стороны ставка</code>\nПример: <code>куб 2 3 4 1</code>\n\nМожно выбрать максимум <b>3</b> стороны.')
        return

    raw_sides = []
    amount_arg = context.args[-1]

    for item in context.args[:-1]:
        try:
            raw_sides.append(int(item))
        except Exception:
            await send_result(update, context, '❌ Стороны куба должны быть числами от 1 до 6.')
            return

    chosen_sides = sorted(set(raw_sides))

    if len(chosen_sides) < 1:
        await send_result(update, context, '❌ Выбери хотя бы одну сторону куба.')
        return

    if len(chosen_sides) > 3:
        await send_result(update, context, '❌ Можно выбрать максимум <b>3</b> стороны куба.')
        return

    if any(x < 1 or x > 6 for x in chosen_sides):
        await send_result(update, context, '❌ Стороны куба должны быть от <b>1</b> до <b>6</b>.')
        return

    bet_milli = parse_money(amount_arg)
    if bet_milli is None or bet_milli <= 0:
        await send_result(update, context, 'Введите ставку числом. Например: <code>куб 2 3 4 1</code>')
        return

    if bet_milli < MIN_BALL_BET_MILLI:
        await send_result(update, context, f'❗️ Минимальная ставка: <b>{money(MIN_BALL_BET_MILLI)}</b>')
        return

    if bet_milli > MAX_BALL_BET_MILLI:
        await send_result(update, context, f'❗️ Максимальная ставка: <b>{money(MAX_BALL_BET_MILLI)}</b>')
        return

    row = get_user(user.id)
    if not row:
        await send_result(update, context, 'Профиль не найден. Напиши /start.')
        return

    balance_milli = int(row[4])
    if balance_milli < bet_milli:
        await send_result(update, context, f'❌ Недостаточно средств.\nВаш баланс: <b>{money(balance_milli)}</b>')
        return

    ok, msg = take_balance(user.id, bet_milli)
    if not ok:
        await send_result(update, context, f'❌ {html.escape(msg)}')
        return

    add_game_stats(user.id, bet_milli)

    dice_msg = await context.bot.send_dice(
        chat_id=chat.id,
        emoji='🎲',
        reply_to_message_id=update.message.message_id if update.message else None
    )

    dice_value = dice_msg.dice.value if dice_msg.dice else 1
    is_win = dice_value in chosen_sides
    win_milli = int(round(bet_milli * cube_multiplier(len(chosen_sides)))) if is_win else 0

    if win_milli > 0:
        add_balance(user.id, win_milli)

    updated = get_user(user.id)
    balance_after = int(updated[4]) if updated else 0

    context.application.create_task(
        send_cube_result_later(context, chat.id, dice_msg.message_id, user, bet_milli, chosen_sides, dice_value, win_milli, balance_after)
    )


async def trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    raw_text = update.message.text.strip()
    txt = raw_text.lower()
    parts = raw_text.split()
    lower_parts = txt.split()

    if context.user_data.get('waiting_promo_activate'):
        context.user_data['waiting_promo_activate'] = False
        ok, msg = activate_promo_code(update.effective_user.id, raw_text)
        await update.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')
        return

    if txt in ('🏠 главное меню', 'главное меню'):
        await open_main_screen(update, context)
        return

    if lower_parts and lower_parts[0] == 'баскетбол':
        context.args = parts[1:]
        await ball_cmd(update, context)
        return

    if lower_parts and lower_parts[0] == 'куб':
        context.args = parts[1:]
        await cube_cmd(update, context)
        return

    if txt in ('👏 ежедневный exp', 'ежедневный exp', 'ежедневный опыт', '/dailyexp'):
        await daily_exp_cmd(update, context)
        return

    if txt in TRIGGERS or txt in ('я', 'кто', 'кто я', 'кто я?', '🎭 кто я'):
        await send_role(update, context)
        return

    if txt in ('играть', '🎮 играть', 'казино', '🎰 казино'):
        await show_casino(update, context)
        return

    if txt in ('профиль', '👤 профиль'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('👤 Профиль доступен только в личке с ботом.'), parse_mode='HTML')
            return
        await send_result(update, context, profile_text(update.effective_user.id), reply_markup=profile_actions_menu())
        return

    if txt in ('топ 3', '🏆 топ 3', 'топ'):
        await send_clean_group_result(update, context, top_text())
        return

    if txt in ('передача денег', '💵 передача денег'):
        await send_result(update, context, transfer_usage_text())
        return

    if txt in ('промокод', '🎁 промокод'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('🎁 Промокоды доступны только в личке с ботом.'), parse_mode='HTML')
            return
        await update.message.reply_text(pe('🎁 Введите промокод одним сообщением:'), parse_mode='HTML')
        context.user_data['waiting_promo_activate'] = True
        return


async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ''

    register_user(q.from_user)
    if q.message:
        remember_group(q.message.chat)

    if data == 'casino':
        await q.answer()
        await show_casino(update, context)
        return

    if data.startswith('repeat:'):
        await q.answer()
        parts = data.split(':')
        game = parts[1] if len(parts) > 1 else ''
        try:
            if game == 'ball' and len(parts) == 3:
                context.args = [str(int(parts[2]) / 1000)]
                return await ball_cmd(update, context)
            if game == 'cube' and len(parts) == 4:
                sides = parts[2].split(',')
                context.args = sides + [str(int(parts[3]) / 1000)]
                return await cube_cmd(update, context)
        except Exception:
            await q.message.reply_text(pe('❌ Не удалось повторить игру.'), parse_mode='HTML')
            return

    return await old_buttons_visual(update, context)

# ===== END FINAL CASINO RUSSIAN_BASKET_CUBE FIX =====


# ===== FINAL DISABLE OLD CASINO GAMES =====

OLD_CASINO_GAME_DISABLED_TEXT = (
    "❌ <b>Эта игра удалена.</b>\n\n"
    "Доступные игры:\n"
    "🏀 <code>баскетбол 1</code>\n"
    "🎲 <code>куб 2 3 4 1</code>"
)


async def old_casino_game_disabled(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_result(update, context, OLD_CASINO_GAME_DISABLED_TEXT)


async def football_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await old_casino_game_disabled(update, context)


async def coin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await old_casino_game_disabled(update, context)


async def slots_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await old_casino_game_disabled(update, context)


async def play_coin(update: Update, context: ContextTypes.DEFAULT_TYPE, side: str, bet_milli: int):
    await old_casino_game_disabled(update, context)


async def play_slots(update: Update, context: ContextTypes.DEFAULT_TYPE, bet_milli: int):
    await old_casino_game_disabled(update, context)


async def show_casino(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    text = (
        '🎮 <b>Играть</b>\n\n'
        '🏀 <code>баскетбол 1</code> — баскетбол\n'
        '🎲 <code>куб 2 3 4 1</code> — куб\n\n'
        'В кубе можно выбрать от <b>1</b> до <b>3</b> сторон.\n'
        'Чем меньше сторон выбрано — тем больше выигрыш.\n\n'
        'Минимальная ставка: <b>1 💵</b>\n'
        'Максимальная ставка: <b>100 💵</b>'
    )

    await send_clean_group_result(update, context, text)


async def trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    raw_text = update.message.text.strip()
    txt = raw_text.lower()
    parts = raw_text.split()
    lower_parts = txt.split()

    if context.user_data.get('waiting_promo_activate'):
        context.user_data['waiting_promo_activate'] = False
        ok, msg = activate_promo_code(update.effective_user.id, raw_text)
        await update.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')
        return

    if txt in ('🏠 главное меню', 'главное меню'):
        await open_main_screen(update, context)
        return

    if lower_parts and lower_parts[0] == 'баскетбол':
        context.args = parts[1:]
        await ball_cmd(update, context)
        return

    if lower_parts and lower_parts[0] == 'куб':
        context.args = parts[1:]
        await cube_cmd(update, context)
        return

    if lower_parts and lower_parts[0] in (
        'футбол', '/football', 'football',
        'орел', 'орёл', 'решка', '/coin', 'coin',
        'слоты', 'слот', '/slots', 'slots', 'spin', 'spins'
    ):
        await old_casino_game_disabled(update, context)
        return

    if txt in ('👏 ежедневный exp', 'ежедневный exp', 'ежедневный опыт', '/dailyexp'):
        await daily_exp_cmd(update, context)
        return

    if txt in TRIGGERS or txt in ('я', 'кто', 'кто я', 'кто я?', '🎭 кто я'):
        await send_role(update, context)
        return

    if txt in ('играть', '🎮 играть', 'казино', '🎰 казино'):
        await show_casino(update, context)
        return

    if txt in ('профиль', '👤 профиль'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('👤 Профиль доступен только в личке с ботом.'), parse_mode='HTML')
            return
        await send_result(update, context, profile_text(update.effective_user.id), reply_markup=profile_actions_menu())
        return

    if txt in ('топ 3', '🏆 топ 3', 'топ'):
        await send_clean_group_result(update, context, top_text())
        return

    if txt in ('передача денег', '💵 передача денег'):
        await send_result(update, context, transfer_usage_text())
        return

    if txt in ('промокод', '🎁 промокод'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('🎁 Промокоды доступны только в личке с ботом.'), parse_mode='HTML')
            return
        await update.message.reply_text(pe('🎁 Введите промокод одним сообщением:'), parse_mode='HTML')
        context.user_data['waiting_promo_activate'] = True
        return


async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ''

    register_user(q.from_user)
    if q.message:
        remember_group(q.message.chat)

    if data == 'casino':
        await q.answer()
        await show_casino(update, context)
        return

    if data.startswith('slots_bet:') or data.startswith('coin_bet:'):
        await q.answer()
        await q.message.reply_text(pe(OLD_CASINO_GAME_DISABLED_TEXT), parse_mode='HTML')
        return

    if data.startswith('repeat:'):
        await q.answer()
        parts = data.split(':')
        game = parts[1] if len(parts) > 1 else ''

        if game in ('slots', 'coin', 'football'):
            await q.message.reply_text(pe(OLD_CASINO_GAME_DISABLED_TEXT), parse_mode='HTML')
            return

        try:
            if game == 'ball' and len(parts) == 3:
                context.args = [str(int(parts[2]) / 1000)]
                return await ball_cmd(update, context)

            if game == 'cube' and len(parts) == 4:
                sides = parts[2].split(',')
                context.args = sides + [str(int(parts[3]) / 1000)]
                return await cube_cmd(update, context)

        except Exception:
            await q.message.reply_text(pe('❌ Не удалось повторить игру.'), parse_mode='HTML')
            return

    return await old_buttons_visual(update, context)

# ===== END FINAL DISABLE OLD CASINO GAMES =====


# ===== HARD REMOVE OLD CASINO GAMES FINAL =====

OLD_CASINO_GAME_DISABLED_TEXT = (
    "❌ <b>Эта игра удалена.</b>\n\n"
    "Сейчас доступны только:\n"
    "🏀 <code>баскетбол 1</code>\n"
    "🎲 <code>куб 2 3 4 1</code>"
)


async def safe_reply_game(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    try:
        chat = update.effective_chat
        if not chat and getattr(update, 'callback_query', None) and update.callback_query.message:
            chat = update.callback_query.message.chat

        reply_to = None
        if getattr(update, 'message', None):
            reply_to = update.message.message_id

        if not chat:
            return

        await context.bot.send_message(
            chat_id=chat.id,
            text=pe(text),
            parse_mode='HTML',
            reply_to_message_id=reply_to,
            reply_markup=reply_markup
        )
    except Exception:
        try:
            if getattr(update, 'message', None):
                await update.message.reply_text(pe(text), parse_mode='HTML', reply_markup=reply_markup)
        except Exception:
            pass


async def old_casino_game_disabled(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply_game(update, context, OLD_CASINO_GAME_DISABLED_TEXT)


# Старые игры физически больше не запускаются.
async def football_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await old_casino_game_disabled(update, context)


async def coin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await old_casino_game_disabled(update, context)


async def slots_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await old_casino_game_disabled(update, context)


async def play_coin(update: Update, context: ContextTypes.DEFAULT_TYPE, side: str, bet_milli: int):
    await old_casino_game_disabled(update, context)


async def play_slots(update: Update, context: ContextTypes.DEFAULT_TYPE, bet_milli: int):
    await old_casino_game_disabled(update, context)


async def football_removed_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await old_casino_game_disabled(update, context)


async def show_casino(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    text = (
        '🎮 <b>Играть</b>\n\n'
        '🏀 <code>баскетбол 1</code> — баскетбол\n'
        '🎲 <code>куб 2 3 4 1</code> — куб\n\n'
        'В кубе можно выбрать от <b>1</b> до <b>3</b> сторон.\n'
        'Чем меньше сторон выбрано — тем больше выигрыш.\n\n'
        'Минимальная ставка: <b>1 💵</b>\n'
        'Максимальная ставка: <b>100 💵</b>'
    )

    await safe_reply_game(update, context, text)


async def trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    raw_text = update.message.text.strip()
    txt = raw_text.lower()
    parts = raw_text.split()
    lower_parts = txt.split()

    if context.user_data.get('waiting_promo_activate'):
        context.user_data['waiting_promo_activate'] = False
        ok, msg = activate_promo_code(update.effective_user.id, raw_text)
        await update.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')
        return

    if not lower_parts:
        return

    cmd = lower_parts[0]

    if txt in ('🏠 главное меню', 'главное меню'):
        await open_main_screen(update, context)
        return

    # Новые игры
    if cmd == 'баскетбол':
        context.args = parts[1:]
        await ball_cmd(update, context)
        return

    if cmd == 'куб':
        context.args = parts[1:]
        await cube_cmd(update, context)
        return

    # Старые игры: полностью удалены, играть нельзя.
    if cmd in (
        'футбол', '/football', 'football',
        'орел', 'орёл', 'решка', '/coin', 'coin',
        'слоты', 'слот', '/slots', 'slots', 'spin', 'spins'
    ):
        await old_casino_game_disabled(update, context)
        return

    if txt in ('👏 ежедневный exp', 'ежедневный exp', 'ежедневный опыт', '/dailyexp'):
        await daily_exp_cmd(update, context)
        return

    if txt in TRIGGERS or txt in ('я', 'кто', 'кто я', 'кто я?', '🎭 кто я'):
        await send_role(update, context)
        return

    if txt in ('играть', '🎮 играть', 'казино', '🎰 казино'):
        await show_casino(update, context)
        return

    if txt in ('профиль', '👤 профиль'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('👤 Профиль доступен только в личке с ботом.'), parse_mode='HTML')
            return

        await safe_reply_game(update, context, profile_text(update.effective_user.id), reply_markup=profile_actions_menu())
        return

    if txt in ('топ 3', '🏆 топ 3', 'топ'):
        await send_clean_group_result(update, context, top_text())
        return

    if txt in ('передача денег', '💵 передача денег'):
        await safe_reply_game(update, context, transfer_usage_text())
        return

    if txt in ('промокод', '🎁 промокод'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('🎁 Промокоды доступны только в личке с ботом.'), parse_mode='HTML')
            return

        await update.message.reply_text(pe('🎁 Введите промокод одним сообщением:'), parse_mode='HTML')
        context.user_data['waiting_promo_activate'] = True
        return


async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ''

    register_user(q.from_user)
    if q.message:
        remember_group(q.message.chat)

    # Новая кнопка казино
    if data == 'casino':
        await q.answer()
        await show_casino(update, context)
        return

    # Старые callback-кнопки ставок и повторов полностью выключены.
    if (
        data.startswith('slots_bet:')
        or data.startswith('coin_bet:')
        or data.startswith('football')
        or data in ('slots', 'coin', 'football')
    ):
        await q.answer()
        await q.message.reply_text(pe(OLD_CASINO_GAME_DISABLED_TEXT), parse_mode='HTML')
        return

    if data.startswith('repeat:'):
        await q.answer()
        parts = data.split(':')
        game = parts[1] if len(parts) > 1 else ''

        if game in ('slots', 'coin', 'football'):
            await q.message.reply_text(pe(OLD_CASINO_GAME_DISABLED_TEXT), parse_mode='HTML')
            return

        try:
            if game == 'ball' and len(parts) == 3:
                context.args = [str(int(parts[2]) / 1000)]
                return await ball_cmd(update, context)

            if game == 'cube' and len(parts) == 4:
                sides = parts[2].split(',')
                context.args = sides + [str(int(parts[3]) / 1000)]
                return await cube_cmd(update, context)

        except Exception:
            await q.message.reply_text(pe('❌ Не удалось повторить игру.'), parse_mode='HTML')
            return

    # Остальное — старый обработчик.
    try:
        return await old_buttons_visual(update, context)
    except Exception:
        await q.answer()


# ===== END HARD REMOVE OLD CASINO GAMES FINAL =====


# ===== FINAL RETURN FOOTBALL RU COMMAND =====

def football_result_text(user, bet_milli: int, dice_value: int, win_milli: int, balance_after: int) -> str:
    is_goal = dice_value >= 3

    if is_goal:
        headline = f'Выигрыш <b>{money(win_milli)}</b> в игре ⚽️'
        detail = 'ГОООЛ! Мяч в воротах!'
    else:
        headline = f'Проигрыш <b>{money(bet_milli)}</b> в игре ⚽️'
        detail = 'Мяч попал в штангу :(' if int(dice_value or 0) == 2 else 'Мяч не попал в ворота :('

    return (
        f'{mention(user)}\n'
        f'{headline}\n\n'
        f'{detail}\n\n'
        f'💵 Баланс <b>{money_balance(balance_after)}</b>'
    )


async def send_football_result_later(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, user, bet_milli: int, dice_value: int, win_milli: int, balance_after: int):
    await asyncio.sleep(FOOTBALL_ANIMATION_DELAY)

    await context.bot.send_message(
        chat_id=chat_id,
        text=pe(football_result_text(user, bet_milli, dice_value, win_milli, balance_after)),
        parse_mode='HTML',
        reply_to_message_id=message_id,
        reply_markup=repeat_game_menu('football', bet_milli),
    )


def repeat_game_menu(game: str, bet_milli: int, side: str | None = None):
    if game == 'ball':
        data = f'repeat:ball:{bet_milli}'
    elif game == 'cube':
        data = f'repeat:cube:{side}:{bet_milli}'
    elif game == 'football':
        data = f'repeat:football:{bet_milli}'
    else:
        return None

    return InlineKeyboardMarkup([[InlineKeyboardButton('🔁 Повторить игру', callback_data=data)]])


async def football_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    register_user(user)
    remember_group(chat)

    if await handle_banned_action(update, context):
        return

    if not context.args:
        await safe_reply_game(
            update,
            context,
            '⚽️ <b>Футбол</b>\n\n'
            'Команда: <code>футбол сумма</code>\n'
            'Пример: <code>футбол 2</code>\n\n'
            'Минимальная ставка: <b>2 💵</b>'
        )
        return

    bet_milli = parse_money(context.args[0])

    if bet_milli is None or bet_milli <= 0:
        await safe_reply_game(update, context, 'Введите ставку числом. Например: <code>футбол 2</code>')
        return

    if bet_milli < MIN_FOOTBALL_BET_MILLI:
        await safe_reply_game(update, context, f'❗️ Минимальная ставка в футболе: <b>{money(MIN_FOOTBALL_BET_MILLI)}</b>')
        return

    if bet_milli > MAX_FOOTBALL_BET_MILLI:
        await safe_reply_game(update, context, f'❗️ Максимальная ставка: <b>{money(MAX_FOOTBALL_BET_MILLI)}</b>')
        return

    row = get_user(user.id)
    if not row:
        await safe_reply_game(update, context, 'Профиль не найден. Напиши /start.')
        return

    balance_milli = int(row[4])
    if balance_milli < bet_milli:
        await safe_reply_game(update, context, f'❌ Недостаточно средств.\nВаш баланс: <b>{money(balance_milli)}</b>')
        return

    ok, msg = take_balance(user.id, bet_milli)
    if not ok:
        await safe_reply_game(update, context, f'❌ {html.escape(msg)}')
        return

    add_game_stats(user.id, bet_milli)

    dice_msg = await context.bot.send_dice(
        chat_id=chat.id,
        emoji='⚽',
        reply_to_message_id=update.message.message_id if update.message else None
    )

    dice_value = dice_msg.dice.value if dice_msg.dice else 1
    is_goal = dice_value >= 3
    win_milli = int(round(bet_milli * 1.5)) if is_goal else 0

    if win_milli > 0:
        add_balance(user.id, win_milli)

    updated = get_user(user.id)
    balance_after = int(updated[4]) if updated else 0

    context.application.create_task(
        send_football_result_later(context, chat.id, dice_msg.message_id, user, bet_milli, dice_value, win_milli, balance_after)
    )


async def show_casino(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    text = (
        '🎮 <b>Играть</b>\n\n'
        '🏀 <code>баскетбол 1</code> — баскетбол\n'
        '⚽️ <code>футбол 2</code> — футбол\n'
        '🎲 <code>куб 2 3 4 1</code> — куб\n\n'
        'В кубе можно выбрать от <b>1</b> до <b>3</b> сторон.\n'
        'Чем меньше сторон выбрано — тем больше выигрыш.\n\n'
        'Минимальная ставка: <b>1 💵</b>\n'
        'Футбол от: <b>2 💵</b>\n'
        'Максимальная ставка: <b>100 💵</b>'
    )

    await safe_reply_game(update, context, text)


async def trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    register_user(update.effective_user)
    remember_group(update.effective_chat)

    if await handle_banned_action(update, context):
        return

    raw_text = update.message.text.strip()
    txt = raw_text.lower()
    parts = raw_text.split()
    lower_parts = txt.split()

    if context.user_data.get('waiting_promo_activate'):
        context.user_data['waiting_promo_activate'] = False
        ok, msg = activate_promo_code(update.effective_user.id, raw_text)
        await update.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')
        return

    if not lower_parts:
        return

    cmd = lower_parts[0]

    if txt in ('🏠 главное меню', 'главное меню'):
        await open_main_screen(update, context)
        return

    if cmd == 'баскетбол':
        context.args = parts[1:]
        await ball_cmd(update, context)
        return

    if cmd == 'футбол':
        context.args = parts[1:]
        await football_cmd(update, context)
        return

    if cmd == 'куб':
        context.args = parts[1:]
        await cube_cmd(update, context)
        return

    # Старые игры без русского футбола.
    if cmd in (
        '/football', 'football',
        'орел', 'орёл', 'решка', '/coin', 'coin',
        'слоты', 'слот', '/slots', 'slots', 'spin', 'spins'
    ):
        await old_casino_game_disabled(update, context)
        return

    if txt in ('👏 ежедневный exp', 'ежедневный exp', 'ежедневный опыт', '/dailyexp'):
        await daily_exp_cmd(update, context)
        return

    if txt in TRIGGERS or txt in ('я', 'кто', 'кто я', 'кто я?', '🎭 кто я'):
        await send_role(update, context)
        return

    if txt in ('играть', '🎮 играть', 'казино', '🎰 казино'):
        await show_casino(update, context)
        return

    if txt in ('профиль', '👤 профиль'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('👤 Профиль доступен только в личке с ботом.'), parse_mode='HTML')
            return

        await safe_reply_game(update, context, profile_text(update.effective_user.id), reply_markup=profile_actions_menu())
        return

    if txt in ('топ 3', '🏆 топ 3', 'топ'):
        await send_clean_group_result(update, context, top_text())
        return

    if txt in ('передача денег', '💵 передача денег'):
        await safe_reply_game(update, context, transfer_usage_text())
        return

    if txt in ('промокод', '🎁 промокод'):
        if update.effective_chat.type != 'private':
            await update.message.reply_text(pe('🎁 Промокоды доступны только в личке с ботом.'), parse_mode='HTML')
            return

        await update.message.reply_text(pe('🎁 Введите промокод одним сообщением:'), parse_mode='HTML')
        context.user_data['waiting_promo_activate'] = True
        return


async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ''

    register_user(q.from_user)
    if q.message:
        remember_group(q.message.chat)

    if data == 'casino':
        await q.answer()
        await show_casino(update, context)
        return

    if (
        data.startswith('slots_bet:')
        or data.startswith('coin_bet:')
        or data in ('slots', 'coin')
    ):
        await q.answer()
        await q.message.reply_text(pe(OLD_CASINO_GAME_DISABLED_TEXT), parse_mode='HTML')
        return

    if data.startswith('repeat:'):
        await q.answer()
        parts = data.split(':')
        game = parts[1] if len(parts) > 1 else ''

        if game in ('slots', 'coin'):
            await q.message.reply_text(pe(OLD_CASINO_GAME_DISABLED_TEXT), parse_mode='HTML')
            return

        try:
            if game == 'ball' and len(parts) == 3:
                context.args = [str(int(parts[2]) / 1000)]
                return await ball_cmd(update, context)

            if game == 'football' and len(parts) == 3:
                context.args = [str(int(parts[2]) / 1000)]
                return await football_cmd(update, context)

            if game == 'cube' and len(parts) == 4:
                sides = parts[2].split(',')
                context.args = sides + [str(int(parts[3]) / 1000)]
                return await cube_cmd(update, context)

        except Exception:
            await q.message.reply_text(pe('❌ Не удалось повторить игру.'), parse_mode='HTML')
            return

    try:
        return await old_buttons_visual(update, context)
    except Exception:
        await q.answer()

# ===== END FINAL RETURN FOOTBALL RU COMMAND =====


# ===== FINAL CASINO 5 SEC COOLDOWN =====

async def check_casino_cooldown(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    left = CASINO_COOLDOWN_SECONDS - (ts() - get_casino_last_spin(user_id))
    if left > 0:
        await safe_reply_game(update, context, f"⏱ Подождите еще <b>{left} сек.</b> перед следующей игрой.")
        return False
    return True


async def ball_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    register_user(user)
    remember_group(chat)

    if await handle_banned_action(update, context):
        return

    if not await check_casino_cooldown(update, context, user.id):
        return

    if not context.args:
        await safe_reply_game(update, context, '🏀 <b>Баскетбол</b>\n\nКоманда: <code>баскетбол сумма</code>\nПример: <code>баскетбол 1</code>')
        return

    bet_milli = parse_money(context.args[0])
    if bet_milli is None or bet_milli <= 0:
        await safe_reply_game(update, context, 'Введите ставку числом. Например: <code>баскетбол 1</code>')
        return
    if bet_milli < MIN_BALL_BET_MILLI:
        await safe_reply_game(update, context, f'❗️ Минимальная ставка: <b>{money(MIN_BALL_BET_MILLI)}</b>')
        return
    if bet_milli > MAX_BALL_BET_MILLI:
        await safe_reply_game(update, context, f'❗️ Максимальная ставка: <b>{money(MAX_BALL_BET_MILLI)}</b>')
        return

    row = get_user(user.id)
    if not row:
        await safe_reply_game(update, context, 'Профиль не найден. Напиши /start.')
        return

    balance_milli = int(row[4])
    if balance_milli < bet_milli:
        await safe_reply_game(update, context, f'❌ Недостаточно средств.\nВаш баланс: <b>{money(balance_milli)}</b>')
        return

    ok, msg = take_balance(user.id, bet_milli)
    if not ok:
        await safe_reply_game(update, context, f'❌ {html.escape(msg)}')
        return

    add_game_stats(user.id, bet_milli)
    set_casino_last_spin(user.id)

    dice_msg = await context.bot.send_dice(chat_id=chat.id, emoji='🏀', reply_to_message_id=update.message.message_id if update.message else None)
    dice_value = dice_msg.dice.value if dice_msg.dice else 1
    win_milli = bet_milli * 2 if dice_value >= 4 else 0
    if win_milli > 0:
        add_balance(user.id, win_milli)

    updated = get_user(user.id)
    balance_after = int(updated[4]) if updated else 0
    context.application.create_task(send_ball_result_later(context, chat.id, dice_msg.message_id, user, bet_milli, dice_value, win_milli, balance_after))


async def football_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    register_user(user)
    remember_group(chat)

    if await handle_banned_action(update, context):
        return

    if not await check_casino_cooldown(update, context, user.id):
        return

    if not context.args:
        await safe_reply_game(update, context, '⚽️ <b>Футбол</b>\n\nКоманда: <code>футбол сумма</code>\nПример: <code>футбол 2</code>\n\nМинимальная ставка: <b>2 💵</b>')
        return

    bet_milli = parse_money(context.args[0])
    if bet_milli is None or bet_milli <= 0:
        await safe_reply_game(update, context, 'Введите ставку числом. Например: <code>футбол 2</code>')
        return
    if bet_milli < MIN_FOOTBALL_BET_MILLI:
        await safe_reply_game(update, context, f'❗️ Минимальная ставка в футболе: <b>{money(MIN_FOOTBALL_BET_MILLI)}</b>')
        return
    if bet_milli > MAX_FOOTBALL_BET_MILLI:
        await safe_reply_game(update, context, f'❗️ Максимальная ставка: <b>{money(MAX_FOOTBALL_BET_MILLI)}</b>')
        return

    row = get_user(user.id)
    if not row:
        await safe_reply_game(update, context, 'Профиль не найден. Напиши /start.')
        return

    balance_milli = int(row[4])
    if balance_milli < bet_milli:
        await safe_reply_game(update, context, f'❌ Недостаточно средств.\nВаш баланс: <b>{money(balance_milli)}</b>')
        return

    ok, msg = take_balance(user.id, bet_milli)
    if not ok:
        await safe_reply_game(update, context, f'❌ {html.escape(msg)}')
        return

    add_game_stats(user.id, bet_milli)
    set_casino_last_spin(user.id)

    dice_msg = await context.bot.send_dice(chat_id=chat.id, emoji='⚽', reply_to_message_id=update.message.message_id if update.message else None)
    dice_value = dice_msg.dice.value if dice_msg.dice else 1
    win_milli = int(round(bet_milli * 1.5)) if dice_value >= 3 else 0
    if win_milli > 0:
        add_balance(user.id, win_milli)

    updated = get_user(user.id)
    balance_after = int(updated[4]) if updated else 0
    context.application.create_task(send_football_result_later(context, chat.id, dice_msg.message_id, user, bet_milli, dice_value, win_milli, balance_after))


async def cube_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    register_user(user)
    remember_group(chat)

    if await handle_banned_action(update, context):
        return

    if not await check_casino_cooldown(update, context, user.id):
        return

    if len(context.args) < 2:
        await safe_reply_game(update, context, '🎲 <b>Куб</b>\n\nКоманда: <code>куб стороны ставка</code>\nПример: <code>куб 2 3 4 1</code>\n\nМожно выбрать максимум <b>3</b> стороны.')
        return

    raw_sides = []
    amount_arg = context.args[-1]
    for item in context.args[:-1]:
        try:
            raw_sides.append(int(item))
        except Exception:
            await safe_reply_game(update, context, '❌ Стороны куба должны быть числами от 1 до 6.')
            return

    chosen_sides = sorted(set(raw_sides))
    if len(chosen_sides) < 1:
        await safe_reply_game(update, context, '❌ Выбери хотя бы одну сторону куба.')
        return
    if len(chosen_sides) > 3:
        await safe_reply_game(update, context, '❌ Можно выбрать максимум <b>3</b> стороны куба.')
        return
    if any(x < 1 or x > 6 for x in chosen_sides):
        await safe_reply_game(update, context, '❌ Стороны куба должны быть от <b>1</b> до <b>6</b>.')
        return

    bet_milli = parse_money(amount_arg)
    if bet_milli is None or bet_milli <= 0:
        await safe_reply_game(update, context, 'Введите ставку числом. Например: <code>куб 2 3 4 1</code>')
        return
    if bet_milli < MIN_BALL_BET_MILLI:
        await safe_reply_game(update, context, f'❗️ Минимальная ставка: <b>{money(MIN_BALL_BET_MILLI)}</b>')
        return
    if bet_milli > MAX_BALL_BET_MILLI:
        await safe_reply_game(update, context, f'❗️ Максимальная ставка: <b>{money(MAX_BALL_BET_MILLI)}</b>')
        return

    row = get_user(user.id)
    if not row:
        await safe_reply_game(update, context, 'Профиль не найден. Напиши /start.')
        return

    balance_milli = int(row[4])
    if balance_milli < bet_milli:
        await safe_reply_game(update, context, f'❌ Недостаточно средств.\nВаш баланс: <b>{money(balance_milli)}</b>')
        return

    ok, msg = take_balance(user.id, bet_milli)
    if not ok:
        await safe_reply_game(update, context, f'❌ {html.escape(msg)}')
        return

    add_game_stats(user.id, bet_milli)
    set_casino_last_spin(user.id)

    dice_msg = await context.bot.send_dice(chat_id=chat.id, emoji='🎲', reply_to_message_id=update.message.message_id if update.message else None)
    dice_value = dice_msg.dice.value if dice_msg.dice else 1
    win_milli = int(round(bet_milli * cube_multiplier(len(chosen_sides)))) if dice_value in chosen_sides else 0
    if win_milli > 0:
        add_balance(user.id, win_milli)

    updated = get_user(user.id)
    balance_after = int(updated[4]) if updated else 0
    context.application.create_task(send_cube_result_later(context, chat.id, dice_msg.message_id, user, bet_milli, chosen_sides, dice_value, win_milli, balance_after))

# ===== END FINAL CASINO 5 SEC COOLDOWN =====


# ===== FINAL CLANS SYSTEM =====

CLAN_MIN_NAME = 3
CLAN_MAX_NAME = 6


def normalize_clan_name(name: str) -> str:
    return re.sub(r'[^A-Za-zА-Яа-я0-9_]', '', (name or '').strip()).upper()


def ensure_clan_tables():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS clans (
                clan_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                owner_id INTEGER NOT NULL,
                is_closed INTEGER NOT NULL DEFAULT 0,
                exp INTEGER NOT NULL DEFAULT 0,
                balance_milli INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS clan_members (
                user_id INTEGER PRIMARY KEY,
                clan_id INTEGER NOT NULL,
                role TEXT NOT NULL DEFAULT 'member',
                joined_at INTEGER NOT NULL
            )
        """)
        conn.commit()


def get_user_clan(user_id: int):
    ensure_clan_tables()
    with db() as conn:
        return conn.execute("""
            SELECT c.clan_id, c.name, c.owner_id, c.is_closed, c.exp, c.balance_milli, c.created_at, m.role, m.joined_at
            FROM clan_members m
            JOIN clans c ON c.clan_id=m.clan_id
            WHERE m.user_id=?
        """, (user_id,)).fetchone()


def get_clan_by_name_or_id(raw: str):
    ensure_clan_tables()
    raw = (raw or '').strip()
    with db() as conn:
        if raw.isdigit():
            row = conn.execute("SELECT clan_id, name, owner_id, is_closed, exp, balance_milli, created_at FROM clans WHERE clan_id=?", (int(raw),)).fetchone()
            if row:
                return row
        name = normalize_clan_name(raw)
        return conn.execute("SELECT clan_id, name, owner_id, is_closed, exp, balance_milli, created_at FROM clans WHERE name=?", (name,)).fetchone()


def clan_member_count(clan_id: int) -> int:
    ensure_clan_tables()
    with db() as conn:
        row = conn.execute("SELECT COUNT(*) FROM clan_members WHERE clan_id=?", (clan_id,)).fetchone()
    return int(row[0] or 0) if row else 0


def clan_menu_text(user_id: int) -> str:
    row = get_user_clan(user_id)
    if not row:
        return (
            "🏰 <b>Кланы</b>\n\n"
            "Создай свой клан или вступи в уже существующий.\n\n"
            "Команды:\n"
            "<code>клан создать TAG</code>\n"
            "<code>клан создать TAG закрытый</code>\n"
            "<code>войти в клан TAG</code>\n"
            "<code>покинуть клан</code>\n"
            "<code>клан топ 3</code>\n\n"
            f"Ник клана: <b>{CLAN_MIN_NAME}-{CLAN_MAX_NAME}</b> символов."
        )

    clan_id, name, owner_id, is_closed, exp, balance, created_at, role, joined_at = row
    status = '🔒 закрытый' if int(is_closed or 0) else '🔓 открытый'
    role_text = '👑 владелец' if role == 'owner' else '👤 участник'
    return (
        "🏰 <b>Мой клан</b>\n\n"
        f"Название: <b>{html.escape(name)}</b>\n"
        f"ID: <code>{clan_id}</code>\n"
        f"Статус: <b>{status}</b>\n"
        f"Роль: <b>{role_text}</b>\n"
        f"Участников: <b>{clan_member_count(clan_id)}</b>\n"
        f"EXP клана: <b>{int(exp or 0)}</b>\n"
        f"Банк: <b>{money(int(balance or 0))}</b>"
    )


def clan_menu_keyboard(user_id: int):
    row = get_user_clan(user_id)
    rows = [
        [InlineKeyboardButton('🏰 Создать клан', callback_data='clan_create_help'), InlineKeyboardButton('🚪 Войти в клан', callback_data='clan_join_help')],
        [InlineKeyboardButton('👑 Мой клан', callback_data='clan_my'), InlineKeyboardButton('🏆 Топ 3 кланов', callback_data='clan_top3')],
        [InlineKeyboardButton('🚪 Покинуть клан', callback_data='clan_leave')],
    ]
    if row and row[7] == 'owner':
        rows.append([InlineKeyboardButton('🔒 Закрыть / открыть', callback_data='clan_toggle')])
    return InlineKeyboardMarkup(rows)


def create_clan(owner_id: int, name: str, closed: bool = False) -> tuple[bool, str]:
    ensure_clan_tables()
    name = normalize_clan_name(name)
    if len(name) < CLAN_MIN_NAME or len(name) > CLAN_MAX_NAME:
        return False, f"Ник клана должен быть от {CLAN_MIN_NAME} до {CLAN_MAX_NAME} символов."
    if get_user_clan(owner_id):
        return False, "Вы уже состоите в клане."

    with db() as conn:
        exists = conn.execute("SELECT clan_id FROM clans WHERE name=?", (name,)).fetchone()
        if exists:
            return False, "Клан с таким ником уже существует."
        cur = conn.execute(
            "INSERT INTO clans (name, owner_id, is_closed, exp, balance_milli, created_at) VALUES (?, ?, ?, 0, 0, ?)",
            (name, owner_id, 1 if closed else 0, ts()),
        )
        clan_id = cur.lastrowid
        conn.execute("INSERT INTO clan_members (user_id, clan_id, role, joined_at) VALUES (?, ?, 'owner', ?)", (owner_id, clan_id, ts()))
        conn.commit()

    status = "закрытый 🔒" if closed else "открытый 🔓"
    return True, f"🏰 <b>Клан создан!</b>\n\nНазвание: <b>{html.escape(name)}</b>\nID: <code>{clan_id}</code>\nСтатус: <b>{status}</b>"


def join_clan(user_id: int, raw: str) -> tuple[bool, str]:
    ensure_clan_tables()
    if get_user_clan(user_id):
        return False, "Вы уже состоите в клане."
    clan = get_clan_by_name_or_id(raw)
    if not clan:
        return False, "Клан не найден."

    clan_id, name, owner_id, is_closed, exp, balance, created_at = clan
    if int(is_closed or 0):
        return False, "Этот клан закрытый. Вступление недоступно."

    with db() as conn:
        conn.execute("INSERT INTO clan_members (user_id, clan_id, role, joined_at) VALUES (?, ?, 'member', ?)", (user_id, clan_id, ts()))
        conn.commit()
    return True, f"🚪 Вы вступили в клан <b>{html.escape(name)}</b>."


def leave_clan(user_id: int) -> tuple[bool, str]:
    row = get_user_clan(user_id)
    if not row:
        return False, "Вы не состоите в клане."

    clan_id, name, owner_id, is_closed, exp, balance, created_at, role, joined_at = row
    with db() as conn:
        members = conn.execute("SELECT user_id FROM clan_members WHERE clan_id=? ORDER BY joined_at ASC", (clan_id,)).fetchall()
        if role == 'owner':
            others = [int(m[0]) for m in members if int(m[0]) != int(user_id)]
            if others:
                new_owner = others[0]
                conn.execute("UPDATE clan_members SET role='owner' WHERE user_id=?", (new_owner,))
                conn.execute("UPDATE clans SET owner_id=? WHERE clan_id=?", (new_owner, clan_id))
                conn.execute("DELETE FROM clan_members WHERE user_id=?", (user_id,))
                conn.commit()
                return True, f"🚪 Вы покинули клан <b>{html.escape(name)}</b>.\n👑 Новый владелец назначен автоматически."
            conn.execute("DELETE FROM clan_members WHERE clan_id=?", (clan_id,))
            conn.execute("DELETE FROM clans WHERE clan_id=?", (clan_id,))
            conn.commit()
            return True, f"🚪 Вы покинули клан <b>{html.escape(name)}</b>.\nКлан удалён."
        conn.execute("DELETE FROM clan_members WHERE user_id=?", (user_id,))
        conn.commit()
    return True, f"🚪 Вы покинули клан <b>{html.escape(name)}</b>."


def toggle_clan_closed(user_id: int) -> tuple[bool, str]:
    row = get_user_clan(user_id)
    if not row:
        return False, "Вы не состоите в клане."
    clan_id, name, owner_id, is_closed, exp, balance, created_at, role, joined_at = row
    if role != 'owner':
        return False, "Только владелец клана может менять открытость."

    new_value = 0 if int(is_closed or 0) else 1
    with db() as conn:
        conn.execute("UPDATE clans SET is_closed=? WHERE clan_id=?", (new_value, clan_id))
        conn.commit()
    status = "закрытый 🔒" if new_value else "открытый 🔓"
    return True, f"🏰 Клан <b>{html.escape(name)}</b> теперь <b>{status}</b>."


def clan_top3_text() -> str:
    ensure_clan_tables()
    with db() as conn:
        rows = conn.execute("""
            SELECT c.clan_id, c.name, c.exp, c.balance_milli, COUNT(m.user_id) AS members
            FROM clans c
            LEFT JOIN clan_members m ON m.clan_id=c.clan_id
            GROUP BY c.clan_id
            ORDER BY c.exp DESC, members DESC, c.balance_milli DESC
            LIMIT 3
        """).fetchall()
    if not rows:
        return "🏆 <b>Топ 3 кланов</b>\n\nКланов пока нет."

    medals = ["🥇", "🥈", "🥉"]
    lines = ["🏆 <b>Топ 3 кланов</b>"]
    for i, row in enumerate(rows):
        clan_id, name, exp, balance, members = row
        lines.append(f"{medals[i]} <b>{html.escape(name)}</b> — <b>{int(exp or 0)} EXP</b>\nID: <code>{clan_id}</code> · Участников: <b>{int(members or 0)}</b>")
    return "\n\n".join(lines)


async def show_clans(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_result(update, context, clan_menu_text(update.effective_user.id), reply_markup=clan_menu_keyboard(update.effective_user.id))


async def clan_command_text(update: Update, context: ContextTypes.DEFAULT_TYPE, raw_text: str):
    txt = raw_text.strip()
    low = txt.lower()
    parts = txt.split()

    if low in ('кланы', 'клан', 'clan'):
        await show_clans(update, context)
        return True
    if low in ('клан топ 3', 'топ 3 кланов', 'топ кланов'):
        await send_result(update, context, clan_top3_text())
        return True
    if low in ('мой клан', 'клан мой'):
        await send_result(update, context, clan_menu_text(update.effective_user.id), reply_markup=clan_menu_keyboard(update.effective_user.id))
        return True
    if low in ('покинуть клан', 'клан покинуть'):
        ok, msg = leave_clan(update.effective_user.id)
        await send_result(update, context, ('✅ ' if ok else '❌ ') + msg)
        return True
    if low in ('клан открыть', 'открыть клан', 'клан закрыть', 'закрыть клан'):
        ok, msg = toggle_clan_closed(update.effective_user.id)
        await send_result(update, context, ('✅ ' if ok else '❌ ') + msg)
        return True
    if len(parts) >= 3 and parts[0].lower() == 'клан' and parts[1].lower() == 'создать':
        closed = any(p.lower() in ('закрытый', 'закрыт', 'closed') for p in parts[3:])
        ok, msg = create_clan(update.effective_user.id, parts[2], closed)
        await send_result(update, context, ('✅ ' if ok else '❌ ') + msg)
        return True
    if len(parts) >= 4 and parts[0].lower() == 'создать' and parts[1].lower() == 'клан':
        closed = any(p.lower() in ('закрытый', 'закрыт', 'closed') for p in parts[3:])
        ok, msg = create_clan(update.effective_user.id, parts[2], closed)
        await send_result(update, context, ('✅ ' if ok else '❌ ') + msg)
        return True
    if len(parts) >= 4 and parts[0].lower() == 'войти' and parts[1].lower() == 'в' and parts[2].lower() == 'клан':
        ok, msg = join_clan(update.effective_user.id, parts[3])
        await send_result(update, context, ('✅ ' if ok else '❌ ') + msg)
        return True
    if len(parts) >= 3 and parts[0].lower() == 'клан' and parts[1].lower() == 'войти':
        ok, msg = join_clan(update.effective_user.id, parts[2])
        await send_result(update, context, ('✅ ' if ok else '❌ ') + msg)
        return True
    return False


def reply_main_menu(admin=False, group=False):
    if group:
        rows = []
    else:
        rows = [
            ['🎭 Кто я', '🎮 Играть'],
            ['👤 Профиль', '🏆 Топ 3'],
            ['🏰 Кланы', '👏 Ежедневный EXP'],
            ['💵 Передача денег', '🏠 Главное меню'],
        ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, is_persistent=True, input_field_placeholder='Выберите действие...')


def dashboard_message_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('👤 Профиль', callback_data='profile'), InlineKeyboardButton('📊 Статистика', callback_data='profile_stats')],
        [InlineKeyboardButton('🏰 Кланы', callback_data='clans'), InlineKeyboardButton('👏 Ежедневный EXP', callback_data='daily_exp')],
        [InlineKeyboardButton('💸 Вывести', callback_data='withdraw'), InlineKeyboardButton('👥 Группа', url='https://t.me/bezdnao')],
    ])


_previous_buttons_for_clans = buttons

async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ''
    register_user(q.from_user)
    if q.message:
        remember_group(q.message.chat)

    if data == 'clans':
        await q.answer()
        await q.message.reply_text(pe(clan_menu_text(q.from_user.id)), parse_mode='HTML', reply_markup=clan_menu_keyboard(q.from_user.id))
        return
    if data == 'clan_create_help':
        await q.answer()
        await q.message.reply_text(pe("🏰 <b>Создать клан</b>\n\n<code>клан создать TAG</code>\n<code>клан создать TAG закрытый</code>\n\nНик: <b>3-6</b> символов."), parse_mode='HTML')
        return
    if data == 'clan_join_help':
        await q.answer()
        await q.message.reply_text(pe("🚪 <b>Войти в клан</b>\n\n<code>войти в клан TAG</code>\n<code>клан войти TAG</code>"), parse_mode='HTML')
        return
    if data == 'clan_leave':
        await q.answer()
        ok, msg = leave_clan(q.from_user.id)
        await q.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')
        return
    if data == 'clan_top3':
        await q.answer()
        await q.message.reply_text(pe(clan_top3_text()), parse_mode='HTML')
        return
    if data == 'clan_my':
        await q.answer()
        await q.message.reply_text(pe(clan_menu_text(q.from_user.id)), parse_mode='HTML', reply_markup=clan_menu_keyboard(q.from_user.id))
        return
    if data == 'clan_toggle':
        await q.answer()
        ok, msg = toggle_clan_closed(q.from_user.id)
        await q.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')
        return

    return await _previous_buttons_for_clans(update, context)


_previous_trigger_for_clans = trigger

async def trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text:
        register_user(update.effective_user)
        remember_group(update.effective_chat)
        if await clan_command_text(update, context, update.message.text):
            return
    return await _previous_trigger_for_clans(update, context)

# ===== END FINAL CLANS SYSTEM =====


# ===== FINAL CLAN ADMIN COMMANDS =====

def all_clans_text() -> str:
    ensure_clan_tables()

    with db() as conn:
        rows = conn.execute(
            """
            SELECT c.clan_id, c.name, c.owner_id, c.is_closed, c.exp, c.balance_milli,
                   c.created_at, COUNT(m.user_id) AS members
            FROM clans c
            LEFT JOIN clan_members m ON m.clan_id=c.clan_id
            GROUP BY c.clan_id
            ORDER BY c.clan_id ASC
            """
        ).fetchall()

    if not rows:
        return "🏰 <b>Все кланы</b>\n\nКланов пока нет."

    lines = [f"🏰 <b>Все кланы</b>\n\nВсего: <b>{len(rows)}</b>"]
    for clan_id, name, owner_id, is_closed, exp, balance, created_at, members in rows:
        status = "🔒 закрытый" if int(is_closed or 0) else "🔓 открытый"
        lines.append(
            f"<b>{html.escape(str(name))}</b> · ID: <code>{clan_id}</code>\n"
            f"👑 Владелец: <code>{owner_id}</code>\n"
            f"Статус: <b>{status}</b>\n"
            f"Участников: <b>{int(members or 0)}</b> · EXP: <b>{int(exp or 0)}</b>\n"
            f"Банк: <b>{money(int(balance or 0))}</b>"
        )

    return "\n\n".join(lines)


async def allclans_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    await send_long_message(context.bot, update.effective_chat.id, all_clans_text())


async def clandelete_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    if not context.args:
        await update.message.reply_text(
            pe(
                "🏰 <b>Удалить клан</b>\n\n"
                "Команда:\n"
                "<code>/clandelete ID</code>\n"
                "<code>/clandelete TAG</code>\n\n"
                "Пример: <code>/clandelete 1</code>"
            ),
            parse_mode='HTML'
        )
        return

    raw = context.args[0]
    clan = get_clan_by_name_or_id(raw)

    if not clan:
        await update.message.reply_text(pe('❌ Клан не найден.'), parse_mode='HTML')
        return

    clan_id, name, owner_id, is_closed, exp, balance, created_at = clan
    members = clan_member_count(clan_id)

    with db() as conn:
        conn.execute("DELETE FROM clan_members WHERE clan_id=?", (clan_id,))
        conn.execute("DELETE FROM clans WHERE clan_id=?", (clan_id,))
        conn.commit()

    await update.message.reply_text(
        pe(
            f"✅ <b>Клан удалён</b>\n\n"
            f"Название: <b>{html.escape(str(name))}</b>\n"
            f"ID: <code>{clan_id}</code>\n"
            f"Участников было: <b>{members}</b>"
        ),
        parse_mode='HTML'
    )


# ===== END FINAL CLAN ADMIN COMMANDS =====


# ===== FINAL CLAN_MENU_INLINE_FIX =====

def reply_main_menu(admin=False, group=False):
    if group:
        rows = []
    else:
        rows = [
            ['🎭 Кто я', '🎮 Играть'],
            ['👤 Профиль', '🏆 Топ 3'],
            ['👏 Ежедневный EXP', '🎁 Промокод'],
            ['💵 Передача денег', '🏠 Главное меню'],
        ]

    return ReplyKeyboardMarkup(
        rows,
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder='Выберите действие...'
    )


def dashboard_message_menu():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton('👤 Профиль', callback_data='profile'),
            InlineKeyboardButton('📊 Статистика', callback_data='profile_stats'),
        ],
        [
            InlineKeyboardButton('🏰 Кланы', callback_data='clans'),
            InlineKeyboardButton('👏 Ежедневный EXP', callback_data='daily_exp'),
        ],
        [
            InlineKeyboardButton('💸 Вывести', callback_data='withdraw'),
            InlineKeyboardButton('👥 Группа', url='https://t.me/bezdnao'),
        ],
    ])


def clan_menu_text(user_id: int) -> str:
    row = get_user_clan(user_id)

    if not row:
        return (
            "🏰 <b>Кланы</b>\n\n"
            "Создай свой клан или вступи в уже существующий.\n\n"
            "Команды:\n"
            "<code>клан создать TAG</code>\n"
            "<code>клан создать TAG закрытый</code>\n"
            "<code>войти в клан TAG</code>\n"
            "<code>покинуть клан</code>\n"
            "<code>клан топ 3</code>\n\n"
            f"Ник клана: <b>{CLAN_MIN_NAME}-{CLAN_MAX_NAME}</b> символов."
        )

    clan_id, name, owner_id, is_closed, exp, balance, created_at, role, joined_at = row
    status = '🔒 закрытый' if int(is_closed or 0) else '🔓 открытый'
    role_text = '👑 владелец' if role == 'owner' else '👤 участник'

    return (
        "🏰 <b>Мой клан</b>\n\n"
        f"Название: <b>{html.escape(name)}</b>\n"
        f"ID: <code>{clan_id}</code>\n"
        f"Статус: <b>{status}</b>\n"
        f"Роль: <b>{role_text}</b>\n"
        f"Участников: <b>{clan_member_count(clan_id)}</b>"
    )


def clan_top3_text() -> str:
    ensure_clan_tables()

    with db() as conn:
        rows = conn.execute(
            """
            SELECT c.clan_id, c.name, c.owner_id, c.is_closed, COUNT(m.user_id) AS members
            FROM clans c
            LEFT JOIN clan_members m ON m.clan_id=c.clan_id
            GROUP BY c.clan_id
            ORDER BY members DESC, c.created_at ASC
            LIMIT 3
            """
        ).fetchall()

    if not rows:
        return "🏆 <b>Топ 3 кланов</b>\n\nКланов пока нет."

    medals = ["🥇", "🥈", "🥉"]
    lines = ["🏆 <b>Топ 3 кланов</b>\n\nРейтинг считается по количеству участников."]

    for i, row in enumerate(rows):
        clan_id, name, owner_id, is_closed, members = row
        status = '🔒' if int(is_closed or 0) else '🔓'
        lines.append(
            f"{medals[i]} {status} <b>{html.escape(str(name))}</b>\n"
            f"ID: <code>{clan_id}</code> · Участников: <b>{int(members or 0)}</b>"
        )

    return "\n\n".join(lines)


def all_clans_text() -> str:
    ensure_clan_tables()

    with db() as conn:
        rows = conn.execute(
            """
            SELECT c.clan_id, c.name, c.owner_id, c.is_closed, c.created_at,
                   COUNT(m.user_id) AS members
            FROM clans c
            LEFT JOIN clan_members m ON m.clan_id=c.clan_id
            GROUP BY c.clan_id
            ORDER BY members DESC, c.clan_id ASC
            """
        ).fetchall()

    if not rows:
        return "🏰 <b>Все кланы</b>\n\nКланов пока нет."

    lines = [f"🏰 <b>Все кланы</b>\n\nВсего: <b>{len(rows)}</b>"]

    for clan_id, name, owner_id, is_closed, created_at, members in rows:
        status = "🔒 закрытый" if int(is_closed or 0) else "🔓 открытый"
        lines.append(
            f"<b>{html.escape(str(name))}</b> · ID: <code>{clan_id}</code>\n"
            f"👑 Владелец: <code>{owner_id}</code>\n"
            f"Статус: <b>{status}</b>\n"
            f"Участников: <b>{int(members or 0)}</b>"
        )

    return "\n\n".join(lines)


async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ''

    register_user(q.from_user)

    if q.message:
        remember_group(q.message.chat)

    if data == 'profile':
        await q.answer()
        if q.message.chat.type != 'private':
            await q.message.reply_text(pe('👤 Профиль доступен только в личке с ботом.'), parse_mode='HTML')
            return

        await q.edit_message_text(
            pe(profile_text(q.from_user.id)),
            parse_mode='HTML',
            reply_markup=profile_actions_menu()
        )
        return

    if data in ('profile_stats', 'stats'):
        await q.answer()
        if q.message.chat.type != 'private':
            await q.message.reply_text(pe('📊 Статистика доступна только в личке с ботом.'), parse_mode='HTML')
            return

        await q.edit_message_text(
            pe(profile_stats_text(q.from_user.id)),
            parse_mode='HTML',
            reply_markup=stats_actions_menu()
        )
        return

    if data == 'daily_exp':
        await q.answer()
        ok, msg = claim_daily_exp(q.from_user.id)
        await q.message.reply_text(pe(('✅ ' if ok else '⏱ ') + msg), parse_mode='HTML')
        return

    if data == 'clans':
        await q.answer()
        await q.message.reply_text(
            pe(clan_menu_text(q.from_user.id)),
            parse_mode='HTML',
            reply_markup=clan_menu_keyboard(q.from_user.id)
        )
        return

    if data == 'clan_create_help':
        await q.answer()
        await q.message.reply_text(
            pe(
                "🏰 <b>Создать клан</b>\n\n"
                "<code>клан создать TAG</code>\n"
                "<code>клан создать TAG закрытый</code>\n\n"
                "Ник: <b>3-6</b> символов."
            ),
            parse_mode='HTML'
        )
        return

    if data == 'clan_join_help':
        await q.answer()
        await q.message.reply_text(
            pe("🚪 <b>Войти в клан</b>\n\n<code>войти в клан TAG</code>\n<code>клан войти TAG</code>"),
            parse_mode='HTML'
        )
        return

    if data in ('clan_leave', 'clan_leave_help'):
        await q.answer()
        ok, msg = leave_clan(q.from_user.id)
        await q.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')
        return

    if data == 'clan_top3':
        await q.answer()
        await q.message.reply_text(pe(clan_top3_text()), parse_mode='HTML')
        return

    if data == 'clan_my':
        await q.answer()
        await q.message.reply_text(
            pe(clan_menu_text(q.from_user.id)),
            parse_mode='HTML',
            reply_markup=clan_menu_keyboard(q.from_user.id)
        )
        return

    if data == 'clan_toggle':
        await q.answer()
        ok, msg = toggle_clan_closed(q.from_user.id)
        await q.message.reply_text(pe(('✅ ' if ok else '❌ ') + msg), parse_mode='HTML')
        return

    if data == 'back':
        await q.answer()
        if is_group(q.message.chat):
            await q.edit_message_text(
                pe('Главное меню:'),
                parse_mode='HTML',
                reply_markup=main_menu(is_admin(q.from_user.id), group=True)
            )
        else:
            await q.edit_message_text(
                pe(main_dashboard_text()),
                parse_mode='HTML',
                reply_markup=dashboard_message_menu()
            )
        return

    if data == 'casino':
        await q.answer()
        await show_casino(update, context)
        return

    if data == 'whoami':
        await q.answer()
        await send_role(update, context)
        return

    if data == 'top3':
        await q.answer()
        await send_clean_group_result(update, context, top_text())
        return

    if data == 'transfer_money':
        await q.answer()
        await send_result(update, context, transfer_usage_text())
        return

    if data == 'withdraw':
        await q.answer()
        try:
            await withdraw_start(update, context)
        except Exception:
            await q.message.reply_text(
                pe('💸 <b>Функция временно недоступна.</b>\n<b>Пожалуйста, вернитесь через 48 часов.</b>'),
                parse_mode='HTML'
            )
        return

    if data == 'promo_list':
        await q.answer()
        if not is_admin(q.from_user.id):
            await q.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
            return
        await q.message.reply_text(pe(promo_codes_text()), parse_mode='HTML')
        return

    if data.startswith('slots_bet:') or data.startswith('coin_bet:') or data in ('slots', 'coin'):
        await q.answer()
        await q.message.reply_text(pe(OLD_CASINO_GAME_DISABLED_TEXT), parse_mode='HTML')
        return

    if data.startswith('repeat:'):
        await q.answer()
        parts = data.split(':')
        game = parts[1] if len(parts) > 1 else ''

        if game in ('slots', 'coin'):
            await q.message.reply_text(pe(OLD_CASINO_GAME_DISABLED_TEXT), parse_mode='HTML')
            return

        try:
            if game == 'ball' and len(parts) == 3:
                context.args = [str(int(parts[2]) / 1000)]
                return await ball_cmd(update, context)

            if game == 'football' and len(parts) == 3:
                context.args = [str(int(parts[2]) / 1000)]
                return await football_cmd(update, context)

            if game == 'cube' and len(parts) == 4:
                sides = parts[2].split(',')
                context.args = sides + [str(int(parts[3]) / 1000)]
                return await cube_cmd(update, context)

        except Exception:
            await q.message.reply_text(pe('❌ Не удалось повторить игру.'), parse_mode='HTML')
            return

    if data.startswith('wd_ok:') or data.startswith('wd_no:'):
        try:
            return await _previous_buttons_for_clans(update, context)
        except Exception:
            await q.answer()
            return

    try:
        return await _previous_buttons_for_clans(update, context)
    except Exception:
        await q.answer()


_previous_trigger_for_clan_menu_inline_fix = trigger

async def trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text:
        register_user(update.effective_user)
        remember_group(update.effective_chat)

        # Кнопки в нижней клавиатуре.
        if update.message.text.strip().lower() in ('кланы', '🏰 кланы'):
            await show_clans(update, context)
            return

        # Клановые команды.
        if await clan_command_text(update, context, update.message.text):
            return

    return await _previous_trigger_for_clan_menu_inline_fix(update, context)

# ===== END FINAL CLAN_MENU_INLINE_FIX =====


# ===== FINAL PHOTO_ROLES_ADD_RESETFRAZ =====

PHOTO_ADD_STATE_PHOTO = 'photo_role_wait_photo'
PHOTO_ADD_STATE_RARITY = 'photo_role_wait_rarity'
PHOTO_ADD_STATE_NAME = 'photo_role_wait_name'
PHOTO_ADD_STATE_CONFIRM = 'photo_role_wait_confirm'

RARITY_INPUT_MAP = {
    'обычная': 'common', 'обычный': 'common', 'common': 'common',
    'редкая': 'rare', 'редкий': 'rare', 'rare': 'rare',
    'эпическая': 'epic', 'эпик': 'epic', 'epic': 'epic',
    'легендарная': 'legendary', 'легенда': 'legendary', 'legendary': 'legendary',
    'секретная': 'secret', 'секретный': 'secret', 'secret': 'secret',
}


def ensure_phrase_photo_columns():
    with db() as conn:
        phrase_cols = columns(conn, 'phrases')
        if 'photo_file_id' not in phrase_cols:
            conn.execute("ALTER TABLE phrases ADD COLUMN photo_file_id TEXT")
        if 'description' not in phrase_cols:
            conn.execute("ALTER TABLE phrases ADD COLUMN description TEXT")
        conn.commit()


def reset_all_phrases() -> int:
    ensure_phrase_photo_columns()
    with db() as conn:
        row = conn.execute("SELECT COUNT(*) FROM phrases").fetchone()
        count = int(row[0] or 0) if row else 0
        conn.execute("DELETE FROM phrases")
        conn.commit()
    return count


async def resetfraz_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return
    count = reset_all_phrases()
    await update.message.reply_text(pe(f'✅ <b>Все роли/фразы удалены.</b>\n\nУдалено: <b>{count}</b>'), parse_mode='HTML')


def rarity_help_text() -> str:
    return (
        "Выбери редкость роли одним сообщением:\n\n"
        "🩶 обычная\n💚 редкая\n🩷 эпическая\n💛 легендарная\n🖤 секретная"
    )


async def add_photo_role_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return
    ensure_phrase_photo_columns()
    context.user_data['photo_role_add'] = {'state': PHOTO_ADD_STATE_PHOTO}
    await update.message.reply_text(pe('🖼 <b>Добавление роли с фото</b>\n\nОтправь фото роли.'), parse_mode='HTML')


async def photo_role_receive_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    data = context.user_data.get('photo_role_add')
    if not data or data.get('state') != PHOTO_ADD_STATE_PHOTO:
        return False
    if not update.message.photo:
        await update.message.reply_text(pe('❌ Нужно отправить именно фото.'), parse_mode='HTML')
        return True
    data['photo_file_id'] = update.message.photo[-1].file_id
    data['state'] = PHOTO_ADD_STATE_RARITY
    context.user_data['photo_role_add'] = data
    await update.message.reply_text(pe(rarity_help_text()), parse_mode='HTML')
    return True


async def photo_role_receive_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    data = context.user_data.get('photo_role_add')
    if not data:
        return False

    raw = (update.message.text or '').strip()
    low = raw.lower()

    if low in ('отмена', 'cancel', '/cancel'):
        context.user_data.pop('photo_role_add', None)
        await update.message.reply_text(pe('❌ Добавление роли отменено.'), parse_mode='HTML')
        return True

    state = data.get('state')

    if state == PHOTO_ADD_STATE_RARITY:
        rarity = RARITY_INPUT_MAP.get(low)
        if not rarity:
            await update.message.reply_text(pe('❌ Такой редкости нет.\n\n' + rarity_help_text()), parse_mode='HTML')
            return True
        data['rarity'] = rarity
        data['state'] = PHOTO_ADD_STATE_NAME
        context.user_data['photo_role_add'] = data
        await update.message.reply_text(pe('📖 Теперь отправь название/описание роли одним сообщением.'), parse_mode='HTML')
        return True

    if state == PHOTO_ADD_STATE_NAME:
        if len(raw) < 1:
            await update.message.reply_text(pe('❌ Название не может быть пустым.'), parse_mode='HTML')
            return True
        data['name'] = raw
        data['description'] = raw
        data['state'] = PHOTO_ADD_STATE_CONFIRM
        context.user_data['photo_role_add'] = data
        rarity_label = RARITY_LABELS.get(data['rarity'], data['rarity'])
        await update.message.reply_photo(
            photo=data['photo_file_id'],
            caption=pe(
                "✅ <b>Подтвердить добавление роли?</b>\n\n"
                f"Название: <b>{html.escape(data['name'])}</b>\n"
                f"Редкость: <b>{html.escape(rarity_label)}</b>\n"
                f"📖 Описание: <b>{html.escape(data['description'])}</b>\n\n"
                "Напиши <b>да</b> для добавления или <b>нет</b> для отмены."
            ),
            parse_mode='HTML'
        )
        return True

    if state == PHOTO_ADD_STATE_CONFIRM:
        if low not in ('да', 'yes', 'y', 'нет', 'no', 'n'):
            await update.message.reply_text(pe('Напиши <b>да</b> или <b>нет</b>.'), parse_mode='HTML')
            return True
        if low in ('нет', 'no', 'n'):
            context.user_data.pop('photo_role_add', None)
            await update.message.reply_text(pe('❌ Добавление роли отменено.'), parse_mode='HTML')
            return True

        ensure_phrase_photo_columns()
        with db() as conn:
            conn.execute(
                "INSERT INTO phrases (text, rarity, photo_file_id, description, created_at) VALUES (?, ?, ?, ?, ?)",
                (data['name'], data['rarity'], data['photo_file_id'], data.get('description') or data['name'], ts()),
            )
            conn.commit()

        context.user_data.pop('photo_role_add', None)
        await update.message.reply_text(
            pe(f"✅ <b>Роль добавлена!</b>\n\nНазвание: <b>{html.escape(data['name'])}</b>\nРедкость: <b>{html.escape(RARITY_LABELS.get(data['rarity'], data['rarity']))}</b>"),
            parse_mode='HTML'
        )
        return True

    return False


def roll_phrase() -> tuple:
    ensure_phrase_photo_columns()
    rarity = roll_role_rarity()
    with db() as conn:
        row = conn.execute(
            "SELECT text, rarity, photo_file_id, description FROM phrases WHERE rarity=? ORDER BY RANDOM() LIMIT 1",
            (rarity,),
        ).fetchone()
        if not row:
            row = conn.execute("SELECT text, rarity, photo_file_id, description FROM phrases ORDER BY RANDOM() LIMIT 1").fetchone()
        if row:
            return row[0] or "Неизвестная роль", row[1] or 'common', row[2], row[3] or (row[0] or "Описание отсутствует.")
    return "Неизвестная роль", "common", None, "Описание отсутствует."


async def send_role(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    register_user(user)
    remember_group(chat)

    if await handle_banned_action(update, context):
        return

    await maybe_start_random_group_event(update, context)

    now = ts()
    row = get_user(user.id)
    if row and now - int(row[6]) < ROLE_COOLDOWN_SECONDS:
        left = ROLE_COOLDOWN_SECONDS - (now - int(row[6]))
        await send_result(update, context, f'⏲ Подождите еще <b>{format_time_left(left)}</b> перед получением новой роли.')
        return

    phrase, rarity, photo_file_id, description = roll_phrase()
    rarity_label = RARITY_LABELS.get(rarity, rarity)
    reward_milli = ROLE_REWARDS_MILLI.get(rarity, 0)
    base_exp = ROLE_EXP_REWARDS.get(rarity, 1)
    multiplier = GROUP_EVENT_EXP_MULTIPLIER if is_group(chat) and is_group_event_active(chat.id) else 1
    exp_added = base_exp * multiplier

    with db() as conn:
        conn.execute(
            "UPDATE users SET balance_milli=balance_milli+?, openings=openings+1, last_role_at=? WHERE user_id=?",
            (reward_milli, now, user.id),
        )
        conn.execute(
            "INSERT INTO user_roles (user_id, phrase, rarity, received_at) VALUES (?, ?, ?, ?)",
            (user.id, phrase, rarity, now),
        )
        conn.commit()

    add_user_exp(user.id, exp_added)
    event_line = "\n❗️ Событие активно: <b>EXP x2</b>" if multiplier > 1 else ""

    caption = (
        f"{mention(user)} — <b>{html.escape(phrase)}</b>\n"
        f"📖 Описание: <b>{html.escape(description)}</b>\n"
        f"Редкость: <b>{html.escape(rarity_label)}</b>\n"
        f"Добавлено: <b>+{money(reward_milli)}</b>\n"
        f"👏 Добавлено EXP: <b>+{exp_added}</b>"
        f"{event_line}"
    )

    if photo_file_id:
        try:
            await context.bot.send_photo(
                chat_id=chat.id,
                photo=photo_file_id,
                caption=pe(caption),
                parse_mode='HTML',
                reply_markup=role_menu(group=is_group(chat)),
                reply_to_message_id=update.message.message_id if update.message else None
            )
        except Exception:
            await send_result(update, context, caption, reply_markup=role_menu(group=is_group(chat)))
    else:
        await send_result(update, context, caption, reply_markup=role_menu(group=is_group(chat)))

    await send_role_log(context, user, phrase, rarity_label, reward_milli)


_previous_trigger_for_photo_roles = trigger

async def trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        if update.message.photo and await photo_role_receive_photo(update, context):
            return
        if update.message.text and await photo_role_receive_text(update, context):
            return

    return await _previous_trigger_for_photo_roles(update, context)

# ===== END FINAL PHOTO_ROLES_ADD_RESETFRAZ =====



async def document_photo_hint_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get('photo_role_add')
    if data and data.get('state') == PHOTO_ADD_STATE_PHOTO:
        await update.message.reply_text(
            pe('❌ Отправь изображение именно как <b>фото</b>, не как файл.'),
            parse_mode='HTML'
        )

# ===== FINAL PHOTO_HANDLER_REGISTER_FIX =====

async def photo_add_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.photo:
        if await photo_role_receive_photo(update, context):
            return

    # Если фото пришло не в процессе /add, просто игнорируем.
    return

# ===== END FINAL PHOTO_HANDLER_REGISTER_FIX =====


# ===== FINAL ADD_CONFIRM_YES_FIX =====

def normalize_yes_no_text(value: str) -> str:
    value = (value or '').strip().lower()

    # На случай, если Telegram/клавиатура вставила похожие латинские буквы.
    value = (
        value.replace('a', 'а')
             .replace('e', 'е')
             .replace('o', 'о')
             .replace('c', 'с')
             .replace('p', 'р')
             .replace('x', 'х')
             .replace('y', 'у')
    )

    # Оставляем только буквы.
    value = re.sub(r'[^a-zа-яё]', '', value, flags=re.IGNORECASE)
    return value


async def photo_role_receive_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    data = context.user_data.get('photo_role_add')
    if not data:
        return False

    raw = (update.message.text or '').strip()
    low = raw.lower()
    normalized = normalize_yes_no_text(raw)

    if normalized in ('отмена', 'cancel'):
        context.user_data.pop('photo_role_add', None)
        await update.message.reply_text(pe('❌ Добавление роли отменено.'), parse_mode='HTML')
        return True

    state = data.get('state')

    if state == PHOTO_ADD_STATE_RARITY:
        rarity = RARITY_INPUT_MAP.get(low) or RARITY_INPUT_MAP.get(normalized)
        if not rarity:
            await update.message.reply_text(pe('❌ Такой редкости нет.\n\n' + rarity_help_text()), parse_mode='HTML')
            return True

        data['rarity'] = rarity
        data['state'] = PHOTO_ADD_STATE_NAME
        context.user_data['photo_role_add'] = data
        await update.message.reply_text(pe('📖 Теперь отправь название/описание роли одним сообщением.'), parse_mode='HTML')
        return True

    if state == PHOTO_ADD_STATE_NAME:
        if len(raw) < 1:
            await update.message.reply_text(pe('❌ Название не может быть пустым.'), parse_mode='HTML')
            return True

        data['name'] = raw
        data['description'] = raw
        data['state'] = PHOTO_ADD_STATE_CONFIRM
        context.user_data['photo_role_add'] = data

        rarity_label = RARITY_LABELS.get(data['rarity'], data['rarity'])
        await update.message.reply_photo(
            photo=data['photo_file_id'],
            caption=pe(
                "✅ <b>Подтвердить добавление роли?</b>\n\n"
                f"Название: <b>{html.escape(data['name'])}</b>\n"
                f"Редкость: <b>{html.escape(rarity_label)}</b>\n"
                f"📖 Описание: <b>{html.escape(data['description'])}</b>\n\n"
                "Напиши <b>да</b> или <b>нет</b>."
            ),
            parse_mode='HTML'
        )
        return True

    if state == PHOTO_ADD_STATE_CONFIRM:
        yes_values = ('да', 'yes', 'y', 'lf')   # lf = да на английской раскладке
        no_values = ('нет', 'no', 'n', 'ytn')   # ytn = нет на английской раскладке

        if normalized not in yes_values and normalized not in no_values:
            await update.message.reply_text(
                pe(
                    '❌ Не понял ответ.\n\n'
                    'Напиши просто: <b>да</b> или <b>нет</b>.'
                ),
                parse_mode='HTML'
            )
            return True

        if normalized in no_values:
            context.user_data.pop('photo_role_add', None)
            await update.message.reply_text(pe('❌ Добавление роли отменено.'), parse_mode='HTML')
            return True

        ensure_phrase_photo_columns()
        with db() as conn:
            conn.execute(
                "INSERT INTO phrases (text, rarity, photo_file_id, description, created_at) VALUES (?, ?, ?, ?, ?)",
                (data['name'], data['rarity'], data['photo_file_id'], data.get('description') or data['name'], ts()),
            )
            conn.commit()

        context.user_data.pop('photo_role_add', None)
        await update.message.reply_text(
            pe(
                f"✅ <b>Роль добавлена!</b>\n\n"
                f"Название: <b>{html.escape(data['name'])}</b>\n"
                f"Редкость: <b>{html.escape(RARITY_LABELS.get(data['rarity'], data['rarity']))}</b>"
            ),
            parse_mode='HTML'
        )
        return True

    return False

# ===== END FINAL ADD_CONFIRM_YES_FIX =====


# ===== FINAL ADD_TEXT_HANDLER_PRIORITY_FIX =====

async def photo_add_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Отдельный приоритетный обработчик текста для /add.
    Нужен, чтобы ответы 'да' / 'нет' не перехватывал обычный trigger.
    """
    if update.message and update.message.text:
        if await photo_role_receive_text(update, context):
            return

    return


# ===== END FINAL ADD_TEXT_HANDLER_PRIORITY_FIX =====


# ===== FINAL ADD_TEXT_HANDLER_STOP_FIX =====

async def photo_add_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text:
        if await photo_role_receive_text(update, context):
            raise ApplicationHandlerStop

    return

# ===== END FINAL ADD_TEXT_HANDLER_STOP_FIX =====


# ===== FINAL ADD_NO_CONFIRM_FIX =====

async def photo_role_receive_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    data = context.user_data.get('photo_role_add')
    if not data:
        return False

    raw = (update.message.text or '').strip()
    low = raw.lower()

    if low in ('отмена', 'cancel', '/cancel'):
        context.user_data.pop('photo_role_add', None)
        await update.message.reply_text(pe('❌ Добавление роли отменено.'), parse_mode='HTML')
        return True

    state = data.get('state')

    if state == PHOTO_ADD_STATE_RARITY:
        normalized = normalize_yes_no_text(raw) if 'normalize_yes_no_text' in globals() else low
        rarity = RARITY_INPUT_MAP.get(low) or RARITY_INPUT_MAP.get(normalized)

        if not rarity:
            await update.message.reply_text(pe('❌ Такой редкости нет.\n\n' + rarity_help_text()), parse_mode='HTML')
            return True

        data['rarity'] = rarity
        data['state'] = PHOTO_ADD_STATE_NAME
        context.user_data['photo_role_add'] = data

        await update.message.reply_text(
            pe('📖 Теперь отправь название/описание роли одним сообщением.\n\nПосле этого роль сразу добавится.'),
            parse_mode='HTML'
        )
        return True

    if state == PHOTO_ADD_STATE_NAME:
        if len(raw) < 1:
            await update.message.reply_text(pe('❌ Название не может быть пустым.'), parse_mode='HTML')
            return True

        data['name'] = raw
        data['description'] = raw

        ensure_phrase_photo_columns()
        with db() as conn:
            conn.execute(
                "INSERT INTO phrases (text, rarity, photo_file_id, description, created_at) VALUES (?, ?, ?, ?, ?)",
                (data['name'], data['rarity'], data['photo_file_id'], data.get('description') or data['name'], ts()),
            )
            conn.commit()

        context.user_data.pop('photo_role_add', None)

        rarity_label = RARITY_LABELS.get(data['rarity'], data['rarity'])

        try:
            await update.message.reply_photo(
                photo=data['photo_file_id'],
                caption=pe(
                    f"✅ <b>Роль добавлена!</b>\n\n"
                    f"Название: <b>{html.escape(data['name'])}</b>\n"
                    f"Редкость: <b>{html.escape(rarity_label)}</b>\n"
                    f"📖 Описание: <b>{html.escape(data['description'])}</b>"
                ),
                parse_mode='HTML'
            )
        except Exception:
            await update.message.reply_text(
                pe(
                    f"✅ <b>Роль добавлена!</b>\n\n"
                    f"Название: <b>{html.escape(data['name'])}</b>\n"
                    f"Редкость: <b>{html.escape(rarity_label)}</b>\n"
                    f"📖 Описание: <b>{html.escape(data['description'])}</b>"
                ),
                parse_mode='HTML'
            )

        return True

    # Старое состояние подтверждения больше не используется.
    if state == PHOTO_ADD_STATE_CONFIRM:
        context.user_data.pop('photo_role_add', None)
        await update.message.reply_text(
            pe('❌ Старое подтверждение сброшено. Запусти /add заново.'),
            parse_mode='HTML'
        )
        return True

    return False

# ===== END FINAL ADD_NO_CONFIRM_FIX =====


# ===== FINAL_ADD_CANCEL_AND_TEXT_LOGS =====

async def add_photo_role_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return

    ensure_phrase_photo_columns()
    context.user_data['photo_role_add'] = {'state': PHOTO_ADD_STATE_PHOTO}

    await update.message.reply_text(
        pe(
            '🖼 <b>Добавление роли с фото</b>\n\n'
            'Отправь фото роли.\n\n'
            'Чтобы отменить добавление, напиши: <b>Отменить</b>'
        ),
        parse_mode='HTML'
    )


async def photo_role_receive_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    data = context.user_data.get('photo_role_add')
    if not data or data.get('state') != PHOTO_ADD_STATE_PHOTO:
        return False

    if not update.message.photo:
        await update.message.reply_text(
            pe('❌ Нужно отправить именно фото.\n\nЧтобы отменить, напиши: <b>Отменить</b>'),
            parse_mode='HTML'
        )
        return True

    data['photo_file_id'] = update.message.photo[-1].file_id
    data['state'] = PHOTO_ADD_STATE_RARITY
    context.user_data['photo_role_add'] = data

    await update.message.reply_text(
        pe(rarity_help_text() + '\n\nЧтобы отменить добавление, напиши: <b>Отменить</b>'),
        parse_mode='HTML'
    )
    return True


async def photo_role_receive_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    data = context.user_data.get('photo_role_add')
    if not data:
        return False

    raw = (update.message.text or '').strip()
    low = raw.lower()

    if low in ('отменить', 'отмена', 'cancel', '/cancel'):
        context.user_data.pop('photo_role_add', None)
        await update.message.reply_text(pe('❌ Добавление роли отменено.'), parse_mode='HTML')
        return True

    state = data.get('state')

    if state == PHOTO_ADD_STATE_RARITY:
        normalized = normalize_yes_no_text(raw) if 'normalize_yes_no_text' in globals() else low
        rarity = RARITY_INPUT_MAP.get(low) or RARITY_INPUT_MAP.get(normalized)

        if not rarity:
            await update.message.reply_text(
                pe('❌ Такой редкости нет.\n\n' + rarity_help_text() + '\n\nЧтобы отменить, напиши: <b>Отменить</b>'),
                parse_mode='HTML'
            )
            return True

        data['rarity'] = rarity
        data['state'] = PHOTO_ADD_STATE_NAME
        context.user_data['photo_role_add'] = data

        await update.message.reply_text(
            pe(
                '📖 Теперь отправь название/описание роли одним сообщением.\n\n'
                'После этого роль сразу добавится.\n\n'
                'Чтобы отменить добавление, напиши: <b>Отменить</b>'
            ),
            parse_mode='HTML'
        )
        return True

    if state == PHOTO_ADD_STATE_NAME:
        if len(raw) < 1:
            await update.message.reply_text(
                pe('❌ Название не может быть пустым.\n\nЧтобы отменить, напиши: <b>Отменить</b>'),
                parse_mode='HTML'
            )
            return True

        data['name'] = raw
        data['description'] = raw

        ensure_phrase_photo_columns()
        with db() as conn:
            conn.execute(
                "INSERT INTO phrases (text, rarity, photo_file_id, description, created_at) VALUES (?, ?, ?, ?, ?)",
                (data['name'], data['rarity'], data['photo_file_id'], data.get('description') or data['name'], ts()),
            )
            conn.commit()

        context.user_data.pop('photo_role_add', None)

        rarity_label = RARITY_LABELS.get(data['rarity'], data['rarity'])

        try:
            await update.message.reply_photo(
                photo=data['photo_file_id'],
                caption=pe(
                    f"✅ <b>Роль добавлена!</b>\n\n"
                    f"Название: <b>{html.escape(data['name'])}</b>\n"
                    f"Редкость: <b>{html.escape(rarity_label)}</b>\n"
                    f"📖 Описание: <b>{html.escape(data['description'])}</b>"
                ),
                parse_mode='HTML'
            )
        except Exception:
            await update.message.reply_text(
                pe(
                    f"✅ <b>Роль добавлена!</b>\n\n"
                    f"Название: <b>{html.escape(data['name'])}</b>\n"
                    f"Редкость: <b>{html.escape(rarity_label)}</b>\n"
                    f"📖 Описание: <b>{html.escape(data['description'])}</b>"
                ),
                parse_mode='HTML'
            )

        return True

    if state == PHOTO_ADD_STATE_CONFIRM:
        context.user_data.pop('photo_role_add', None)
        await update.message.reply_text(pe('❌ Старое подтверждение сброшено. Запусти /add заново.'), parse_mode='HTML')
        return True

    return False


async def send_role_log(context: ContextTypes.DEFAULT_TYPE, user, phrase: str, rarity_label: str, reward_milli: int):
    """
    Логи получения ролей отправляются только текстом.
    Фото роли в лог-чат не отправляется.
    """
    try:
        username = f'@{user.username}' if getattr(user, 'username', None) else html.escape(getattr(user, 'first_name', '') or 'Без username')
        log_text = (
            "📰 <b>Получение роли</b>\n\n"
            f"👤 Игрок: <b>{username}</b>\n"
            f"ID: <code>{user.id}</code>\n"
            f"Роль: <b>{html.escape(str(phrase))}</b>\n"
            f"Редкость: <b>{html.escape(str(rarity_label))}</b>\n"
            f"Добавлено: <b>+{money(reward_milli)}</b>\n"
            f"Время: <b>{datetime.now().strftime('%d.%m.%Y %H:%M:%S')}</b>"
        )

        await context.bot.send_message(
            chat_id=ROLE_LOG_CHAT_ID,
            text=pe(log_text),
            parse_mode='HTML'
        )
    except Exception:
        pass

# ===== END_FINAL_ADD_CANCEL_AND_TEXT_LOGS =====


# ===== FINAL_ADD_DESCRIPTION_STEP_FIX =====

async def photo_add_text_priority_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Ловит текстовые шаги /add до обычного trigger.
    Особенно важен шаг названия/описания роли.
    """
    if not update.message or not update.message.text:
        return

    data = context.user_data.get('photo_role_add')
    if not data:
        return

    handled = await photo_role_receive_text(update, context)
    if handled:
        raise ApplicationHandlerStop


async def photo_add_photo_priority_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Ловит фото для /add до обычных обработчиков.
    """
    if not update.message or not update.message.photo:
        return

    data = context.user_data.get('photo_role_add')
    if not data:
        return

    handled = await photo_role_receive_photo(update, context)
    if handled:
        raise ApplicationHandlerStop


# ===== END_FINAL_ADD_DESCRIPTION_STEP_FIX =====


# ===== FINAL_ADD_CONVERSATION_HANDLER_FIX =====

ADD_PHOTO, ADD_RARITY, ADD_NAME = range(9000, 9003)

RARITY_INPUT_MAP_FINAL = {
    'обычная': 'common', 'обычный': 'common', 'common': 'common',
    'редкая': 'rare', 'редкий': 'rare', 'rare': 'rare',
    'эпическая': 'epic', 'эпик': 'epic', 'epic': 'epic',
    'легендарная': 'legendary', 'легенда': 'legendary', 'legendary': 'legendary',
    'секретная': 'secret', 'секретный': 'secret', 'secret': 'secret',
}


def ensure_phrase_photo_columns_final():
    with db() as conn:
        phrase_cols = columns(conn, 'phrases')
        if 'photo_file_id' not in phrase_cols:
            conn.execute("ALTER TABLE phrases ADD COLUMN photo_file_id TEXT")
        if 'description' not in phrase_cols:
            conn.execute("ALTER TABLE phrases ADD COLUMN description TEXT")
        conn.commit()


async def add_photo_role_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(pe('⛔ У тебя нет доступа.'), parse_mode='HTML')
        return ConversationHandler.END

    ensure_phrase_photo_columns_final()
    context.user_data['photo_role_add'] = {}

    await update.message.reply_text(
        pe(
            '🖼 <b>Добавление роли с фото</b>\n\n'
            'Отправь фото роли.\n\n'
            'Чтобы отменить добавление, напиши: <b>Отменить</b>'
        ),
        parse_mode='HTML'
    )
    return ADD_PHOTO


async def add_photo_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text and update.message.text.strip().lower() in ('отменить', 'отмена', 'cancel', '/cancel'):
        context.user_data.pop('photo_role_add', None)
        await update.message.reply_text(pe('❌ Добавление роли отменено.'), parse_mode='HTML')
        return ConversationHandler.END

    if not update.message or not update.message.photo:
        await update.message.reply_text(
            pe('❌ Отправь изображение именно как <b>фото</b>.\n\nЧтобы отменить, напиши: <b>Отменить</b>'),
            parse_mode='HTML'
        )
        return ADD_PHOTO

    context.user_data['photo_role_add'] = {
        'photo_file_id': update.message.photo[-1].file_id
    }

    await update.message.reply_text(
        pe(
            "Выбери редкость роли одним сообщением:\n\n"
            "🩶 обычная\n"
            "💚 редкая\n"
            "🩷 эпическая\n"
            "💛 легендарная\n"
            "🖤 секретная\n\n"
            "Чтобы отменить добавление, напиши: <b>Отменить</b>"
        ),
        parse_mode='HTML'
    )
    return ADD_RARITY


async def add_rarity_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = (update.message.text or '').strip()
    low = raw.lower()

    if low in ('отменить', 'отмена', 'cancel', '/cancel'):
        context.user_data.pop('photo_role_add', None)
        await update.message.reply_text(pe('❌ Добавление роли отменено.'), parse_mode='HTML')
        return ConversationHandler.END

    rarity = RARITY_INPUT_MAP_FINAL.get(low)
    if not rarity:
        await update.message.reply_text(
            pe(
                "❌ Такой редкости нет.\n\n"
                "Напиши одну из редкостей:\n"
                "обычная, редкая, эпическая, легендарная, секретная\n\n"
                "Чтобы отменить добавление, напиши: <b>Отменить</b>"
            ),
            parse_mode='HTML'
        )
        return ADD_RARITY

    data = context.user_data.get('photo_role_add', {})
    data['rarity'] = rarity
    context.user_data['photo_role_add'] = data

    await update.message.reply_text(
        pe(
            '📖 Теперь отправь название/описание роли одним сообщением.\n\n'
            'После этого роль сразу добавится.\n\n'
            'Чтобы отменить добавление, напиши: <b>Отменить</b>'
        ),
        parse_mode='HTML'
    )
    return ADD_NAME


async def add_name_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = (update.message.text or '').strip()

    if raw.lower() in ('отменить', 'отмена', 'cancel', '/cancel'):
        context.user_data.pop('photo_role_add', None)
        await update.message.reply_text(pe('❌ Добавление роли отменено.'), parse_mode='HTML')
        return ConversationHandler.END

    if not raw:
        await update.message.reply_text(
            pe('❌ Название не может быть пустым.\n\nЧтобы отменить, напиши: <b>Отменить</b>'),
            parse_mode='HTML'
        )
        return ADD_NAME

    data = context.user_data.get('photo_role_add') or {}
    photo_file_id = data.get('photo_file_id')
    rarity = data.get('rarity')

    if not photo_file_id or not rarity:
        context.user_data.pop('photo_role_add', None)
        await update.message.reply_text(pe('❌ Сессия добавления сломалась. Запусти /add заново.'), parse_mode='HTML')
        return ConversationHandler.END

    ensure_phrase_photo_columns_final()
    with db() as conn:
        conn.execute(
            "INSERT INTO phrases (text, rarity, photo_file_id, description, created_at) VALUES (?, ?, ?, ?, ?)",
            (raw, rarity, photo_file_id, raw, ts()),
        )
        conn.commit()

    context.user_data.pop('photo_role_add', None)
    rarity_label = RARITY_LABELS.get(rarity, rarity)

    try:
        await update.message.reply_photo(
            photo=photo_file_id,
            caption=pe(
                f"✅ <b>Роль добавлена!</b>\n\n"
                f"Название: <b>{html.escape(raw)}</b>\n"
                f"Редкость: <b>{html.escape(rarity_label)}</b>\n"
                f"📖 Описание: <b>{html.escape(raw)}</b>"
            ),
            parse_mode='HTML'
        )
    except Exception:
        await update.message.reply_text(
            pe(
                f"✅ <b>Роль добавлена!</b>\n\n"
                f"Название: <b>{html.escape(raw)}</b>\n"
                f"Редкость: <b>{html.escape(rarity_label)}</b>\n"
                f"📖 Описание: <b>{html.escape(raw)}</b>"
            ),
            parse_mode='HTML'
        )

    return ConversationHandler.END


async def add_cancel_conv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop('photo_role_add', None)
    await update.message.reply_text(pe('❌ Добавление роли отменено.'), parse_mode='HTML')
    return ConversationHandler.END


# ===== END_FINAL_ADD_CONVERSATION_HANDLER_FIX =====


# ===== FINAL_ADD_CREATED_AT_AND_CLEAN_HANDLERS =====

def ensure_phrase_photo_columns_final():
    with db() as conn:
        phrase_cols = columns(conn, 'phrases')
        if 'photo_file_id' not in phrase_cols:
            conn.execute("ALTER TABLE phrases ADD COLUMN photo_file_id TEXT")
        if 'description' not in phrase_cols:
            conn.execute("ALTER TABLE phrases ADD COLUMN description TEXT")
        conn.commit()


async def photo_role_receive_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Старый обработчик /add отключён.
    Новый /add работает через ConversationHandler ниже.
    """
    return False


async def photo_role_receive_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Старый обработчик фото /add отключён.
    Новый /add работает через ConversationHandler ниже.
    """
    return False


async def add_name_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = (update.message.text or '').strip()

    if raw.lower() in ('отменить', 'отмена', 'cancel', '/cancel'):
        context.user_data.pop('photo_role_add', None)
        await update.message.reply_text(pe('❌ Добавление роли отменено.'), parse_mode='HTML')
        return ConversationHandler.END

    if not raw:
        await update.message.reply_text(
            pe('❌ Название не может быть пустым.\n\nЧтобы отменить, напиши: <b>Отменить</b>'),
            parse_mode='HTML'
        )
        return ADD_NAME

    data = context.user_data.get('photo_role_add') or {}
    photo_file_id = data.get('photo_file_id')
    rarity = data.get('rarity')

    if not photo_file_id or not rarity:
        context.user_data.pop('photo_role_add', None)
        await update.message.reply_text(pe('❌ Сессия добавления сломалась. Запусти /add заново.'), parse_mode='HTML')
        return ConversationHandler.END

    ensure_phrase_photo_columns_final()
    with db() as conn:
        conn.execute(
            "INSERT INTO phrases (text, rarity, photo_file_id, description, created_at) VALUES (?, ?, ?, ?, ?)",
            (raw, rarity, photo_file_id, raw, ts()),
        )
        conn.commit()

    context.user_data.pop('photo_role_add', None)
    rarity_label = RARITY_LABELS.get(rarity, rarity)

    try:
        await update.message.reply_photo(
            photo=photo_file_id,
            caption=pe(
                f"✅ <b>Роль добавлена!</b>\n\n"
                f"Название: <b>{html.escape(raw)}</b>\n"
                f"Редкость: <b>{html.escape(rarity_label)}</b>\n"
                f"📖 Описание: <b>{html.escape(raw)}</b>"
            ),
            parse_mode='HTML'
        )
    except Exception:
        await update.message.reply_text(
            pe(
                f"✅ <b>Роль добавлена!</b>\n\n"
                f"Название: <b>{html.escape(raw)}</b>\n"
                f"Редкость: <b>{html.escape(rarity_label)}</b>\n"
                f"📖 Описание: <b>{html.escape(raw)}</b>"
            ),
            parse_mode='HTML'
        )

    return ConversationHandler.END

# ===== END_FINAL_ADD_CREATED_AT_AND_CLEAN_HANDLERS =====


# ===== FINAL_ADD_NAME_SQL_SAFE_FIX =====

def insert_phrase_safe(text_value: str, rarity: str, photo_file_id: str | None = None, description: str | None = None):
    """
    Безопасная вставка роли в phrases.
    Заполняет только те колонки, которые реально есть в базе.
    created_at заполняется, если колонка существует.
    """
    ensure_phrase_photo_columns_final()

    with db() as conn:
        phrase_cols = columns(conn, 'phrases')

        values_by_col = {
            'text': text_value,
            'rarity': rarity,
            'photo_file_id': photo_file_id,
            'description': description or text_value,
            'created_at': ts(),
        }

        insert_cols = []
        insert_values = []

        for col in ('text', 'rarity', 'photo_file_id', 'description', 'created_at'):
            if col in phrase_cols:
                insert_cols.append(col)
                insert_values.append(values_by_col[col])

        if 'text' not in insert_cols:
            raise RuntimeError("phrases table has no text column")

        placeholders = ', '.join(['?'] * len(insert_cols))
        sql = f"INSERT INTO phrases ({', '.join(insert_cols)}) VALUES ({placeholders})"
        conn.execute(sql, tuple(insert_values))
        conn.commit()


async def add_name_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = (update.message.text or '').strip()

    if raw.lower() in ('отменить', 'отмена', 'cancel', '/cancel'):
        context.user_data.pop('photo_role_add', None)
        await update.message.reply_text(pe('❌ Добавление роли отменено.'), parse_mode='HTML')
        return ConversationHandler.END

    if not raw:
        await update.message.reply_text(
            pe('❌ Название не может быть пустым.\n\nЧтобы отменить, напиши: <b>Отменить</b>'),
            parse_mode='HTML'
        )
        return ADD_NAME

    data = context.user_data.get('photo_role_add') or {}
    photo_file_id = data.get('photo_file_id')
    rarity = data.get('rarity')

    if not photo_file_id or not rarity:
        context.user_data.pop('photo_role_add', None)
        await update.message.reply_text(pe('❌ Сессия добавления сломалась. Запусти /add заново.'), parse_mode='HTML')
        return ConversationHandler.END

    try:
        insert_phrase_safe(raw, rarity, photo_file_id, raw)
    except Exception as e:
        await update.message.reply_text(
            pe(f'❌ Не удалось добавить роль.\nОшибка: <code>{html.escape(str(e))}</code>'),
            parse_mode='HTML'
        )
        return ADD_NAME

    context.user_data.pop('photo_role_add', None)
    rarity_label = RARITY_LABELS.get(rarity, rarity)

    try:
        await update.message.reply_photo(
            photo=photo_file_id,
            caption=pe(
                f"✅ <b>Роль добавлена!</b>\n\n"
                f"Название: <b>{html.escape(raw)}</b>\n"
                f"Редкость: <b>{html.escape(rarity_label)}</b>\n"
                f"📖 Описание: <b>{html.escape(raw)}</b>"
            ),
            parse_mode='HTML'
        )
    except Exception:
        await update.message.reply_text(
            pe(
                f"✅ <b>Роль добавлена!</b>\n\n"
                f"Название: <b>{html.escape(raw)}</b>\n"
                f"Редкость: <b>{html.escape(rarity_label)}</b>\n"
                f"📖 Описание: <b>{html.escape(raw)}</b>"
            ),
            parse_mode='HTML'
        )

    return ConversationHandler.END

# ===== END_FINAL_ADD_NAME_SQL_SAFE_FIX =====











# ===== FINAL_UNSUPPORTED_CHARS_AND_TOPDAY_REMOVE_FIX =====

import unicodedata as _unicodedata_for_safe_text


def safe_tg_text(value) -> str:
    """
    Убирает символы, из-за которых Telegram иногда отвечает Unsupported characters.
    Особенно актуально для странных никнеймов/имён пользователей.
    """
    value = str(value or '')

    cleaned = []
    for ch in value:
        code = ord(ch)
        cat = _unicodedata_for_safe_text.category(ch)

        # Суррогаты и невалидные unicode-символы.
        if cat == 'Cs':
            continue

        # Управляющие символы, кроме переносов и табов.
        if cat == 'Cc' and ch not in ('\n', '\t', '\r'):
            continue

        # Невидимые bidi/format-control символы, которые часто ломают Telegram.
        if code in (
            0x200B, 0x200C, 0x200D, 0x2060, 0xFEFF,
            0x202A, 0x202B, 0x202C, 0x202D, 0x202E,
            0x2066, 0x2067, 0x2068, 0x2069,
        ):
            continue

        # Неподдерживаемые несимвольные codepoints.
        if 0xFDD0 <= code <= 0xFDEF or (code & 0xFFFE) == 0xFFFE:
            continue

        cleaned.append(ch)

    return ''.join(cleaned)


# Заворачиваем pe(), чтобы все сообщения с HTML/premium emoji проходили очистку.
_old_pe_unsupported_fix = pe

def pe(text: str) -> str:
    return safe_tg_text(_old_pe_unsupported_fix(text))


def safe_name_text(value: str, fallback: str = 'Игрок') -> str:
    value = safe_tg_text(value or '').strip()
    return value if value else fallback


def mention(user) -> str:
    name = safe_name_text(getattr(user, 'first_name', None) or getattr(user, 'username', None) or 'Игрок')
    return f'<a href="tg://user?id={int(user.id)}">{html.escape(name)}</a>'


def dashboard_message_menu():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton('👤 Профиль', callback_data='profile'),
            InlineKeyboardButton('📊 Статистика', callback_data='profile_stats'),
        ],
        [
            InlineKeyboardButton('🏰 Кланы', callback_data='clans'),
            InlineKeyboardButton('👏 Ежедневный EXP', callback_data='daily_exp'),
        ],
        [
            InlineKeyboardButton('💸 Вывести', callback_data='withdraw'),
            InlineKeyboardButton('👥 Группа', url='https://t.me/bezdnao'),
        ],
    ])


async def send_result(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    chat = update.effective_chat
    if not chat and getattr(update, 'callback_query', None) and update.callback_query.message:
        chat = update.callback_query.message.chat

    if not chat:
        return None

    reply_to = update.message.message_id if getattr(update, 'message', None) else None
    safe_html = pe(text)

    try:
        return await context.bot.send_message(
            chat.id,
            safe_html,
            parse_mode='HTML',
            reply_markup=reply_markup,
            reply_to_message_id=reply_to
        )
    except Exception:
        # Fallback без HTML и tg-emoji, если Telegram всё равно ругнулся.
        plain = re.sub(r'<tg-emoji emoji-id="\d+">(.*?)</tg-emoji>', r'\1', safe_html)
        plain = re.sub(r'<a href="[^"]+">(.*?)</a>', r'\1', plain)
        plain = re.sub(r'</?(?:b|i|u|s|code|pre|blockquote|span)[^>]*>', '', plain)
        plain = safe_tg_text(plain)

        return await context.bot.send_message(
            chat.id,
            plain,
            reply_markup=reply_markup,
            reply_to_message_id=reply_to
        )


async def send_clean_group_result(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    return await send_result(update, context, text, reply_markup=reply_markup)


async def safe_reply_game(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    return await send_result(update, context, text, reply_markup=reply_markup)


# Топ дня удалён. Оставляем обычный Топ 3.
async def topday_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_result(update, context, '🏆 <b>Топ дня удалён.</b>\n\nИспользуй обычный <b>Топ 3</b>.')


# ===== END_FINAL_UNSUPPORTED_CHARS_AND_TOPDAY_REMOVE_FIX =====


# ===== FINAL_CASINO_RESULT_PHOTOS_ASSETS =====

from pathlib import Path as _AssetsPath

CASINO_ASSETS_DIR = _AssetsPath("/app/data/assets/casino")

CASINO_PHOTOS = {
    "football_win": CASINO_ASSETS_DIR / "football_win.jpg",
    "football_lose": CASINO_ASSETS_DIR / "football_lose.jpg",
    "basketball_win": CASINO_ASSETS_DIR / "basketball_win.jpg",
    "basketball_lose": CASINO_ASSETS_DIR / "basketball_lose.jpg",
    "cube_win": CASINO_ASSETS_DIR / "cube_win.jpg",
    "cube_lose": CASINO_ASSETS_DIR / "cube_lose.jpg",
    "top3": CASINO_ASSETS_DIR / "top3.jpg",
}


def casino_photo_path(key: str):
    path = CASINO_PHOTOS.get(key)
    if path and path.exists() and path.is_file():
        return str(path)
    return None


async def send_photo_or_text(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, photo_key: str | None = None, reply_to_message_id: int | None = None, reply_markup=None):
    photo_path = casino_photo_path(photo_key) if photo_key else None
    safe_html = pe(text)

    if photo_path:
        try:
            with open(photo_path, "rb") as photo:
                return await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=photo,
                    caption=safe_html,
                    parse_mode='HTML',
                    reply_to_message_id=reply_to_message_id,
                    reply_markup=reply_markup,
                )
        except Exception:
            pass

    try:
        return await context.bot.send_message(
            chat_id=chat_id,
            text=safe_html,
            parse_mode='HTML',
            reply_to_message_id=reply_to_message_id,
            reply_markup=reply_markup,
        )
    except Exception:
        plain = re.sub(r'<tg-emoji emoji-id="\d+">(.*?)</tg-emoji>', r'\1', safe_html)
        plain = re.sub(r'<a href="[^"]+">(.*?)</a>', r'\1', plain)
        plain = re.sub(r'</?(?:b|i|u|s|code|pre|blockquote|span)[^>]*>', '', plain)
        plain = safe_tg_text(plain) if 'safe_tg_text' in globals() else plain
        return await context.bot.send_message(
            chat_id=chat_id,
            text=plain,
            reply_to_message_id=reply_to_message_id,
            reply_markup=reply_markup,
        )


def basketball_result_text(user, bet_milli: int, dice_value: int, win_milli: int, balance_after: int) -> str:
    if win_milli > 0:
        headline = f'Выигрыш <b>{money(win_milli)}</b> в игре 🏀'
        detail = 'Мяч попал в корзину!'
    else:
        headline = f'Проигрыш <b>{money(bet_milli)}</b> в игре 🏀'
        detail = 'Мяч не попал в корзину :('

    return (
        f'{mention(user)}\n'
        f'{headline}\n\n'
        f'{detail}\n\n'
        f'💵 Баланс <b>{money_balance(balance_after)}</b>'
    )


async def send_ball_result_later(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, user, bet_milli: int, dice_value: int, win_milli: int, balance_after: int):
    await asyncio.sleep(BASKETBALL_ANIMATION_DELAY if 'BASKETBALL_ANIMATION_DELAY' in globals() else 4)

    await send_photo_or_text(
        context=context,
        chat_id=chat_id,
        text=basketball_result_text(user, bet_milli, dice_value, win_milli, balance_after),
        photo_key="basketball_win" if win_milli > 0 else "basketball_lose",
        reply_to_message_id=message_id,
        reply_markup=repeat_game_menu('ball', bet_milli),
    )


def football_result_text(user, bet_milli: int, dice_value: int, win_milli: int, balance_after: int) -> str:
    if win_milli > 0:
        headline = f'Выигрыш <b>{money(win_milli)}</b> в игре ⚽️'
        detail = 'ГОООЛ! Мяч в воротах!'
    else:
        headline = f'Проигрыш <b>{money(bet_milli)}</b> в игре ⚽️'
        detail = 'Мяч попал в штангу :(' if int(dice_value or 0) == 2 else 'Мяч не попал в ворота :('

    return (
        f'{mention(user)}\n'
        f'{headline}\n\n'
        f'{detail}\n\n'
        f'💵 Баланс <b>{money_balance(balance_after)}</b>'
    )


async def send_football_result_later(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, user, bet_milli: int, dice_value: int, win_milli: int, balance_after: int):
    await asyncio.sleep(FOOTBALL_ANIMATION_DELAY if 'FOOTBALL_ANIMATION_DELAY' in globals() else 4)

    await send_photo_or_text(
        context=context,
        chat_id=chat_id,
        text=football_result_text(user, bet_milli, dice_value, win_milli, balance_after),
        photo_key="football_win" if win_milli > 0 else "football_lose",
        reply_to_message_id=message_id,
        reply_markup=repeat_game_menu('football', bet_milli),
    )


def cube_result_text(user, bet_milli: int, chosen_sides: list[int], dice_value: int, win_milli: int, balance_after: int) -> str:
    sides_text = ', '.join(str(x) for x in chosen_sides)

    if win_milli > 0:
        headline = f'Выигрыш <b>{money(win_milli)}</b> в игре 🎲'
        detail = 'Куб выпал на выбранную сторону!'
    else:
        headline = f'Проигрыш <b>{money(bet_milli)}</b> в игре 🎲'
        detail = 'Куб выпал не на твою сторону :('

    return (
        f'{mention(user)}\n'
        f'{headline}\n'
        f'Вы выбрали: <b>{html.escape(sides_text)}</b>\n'
        f'Выпало: <b>{dice_value}</b>\n\n'
        f'{detail}\n\n'
        f'💵 Баланс <b>{money_balance(balance_after)}</b>'
    )


async def send_cube_result_later(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, user, bet_milli: int, chosen_sides: list[int], dice_value: int, win_milli: int, balance_after: int):
    await asyncio.sleep(CUBE_ANIMATION_DELAY if 'CUBE_ANIMATION_DELAY' in globals() else 4)

    await send_photo_or_text(
        context=context,
        chat_id=chat_id,
        text=cube_result_text(user, bet_milli, chosen_sides, dice_value, win_milli, balance_after),
        photo_key="cube_win" if win_milli > 0 else "cube_lose",
        reply_to_message_id=message_id,
        reply_markup=repeat_game_menu('cube', bet_milli, ','.join(str(x) for x in chosen_sides)),
    )


async def send_clean_group_result(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    chat = update.effective_chat
    if not chat and getattr(update, 'callback_query', None) and update.callback_query.message:
        chat = update.callback_query.message.chat

    if not chat:
        return None

    reply_to = update.message.message_id if getattr(update, 'message', None) else None

    top_photo = False
    try:
        plain = re.sub(r'<[^>]+>', '', str(text)).lower()
        top_photo = ('топ 3' in plain or 'топ3' in plain) and 'топ дня' not in plain
    except Exception:
        top_photo = False

    return await send_photo_or_text(
        context=context,
        chat_id=chat.id,
        text=text,
        photo_key="top3" if top_photo else None,
        reply_to_message_id=reply_to,
        reply_markup=reply_markup,
    )


async def safe_reply_game(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    return await send_clean_group_result(update, context, text, reply_markup=reply_markup)


# ===== END_FINAL_CASINO_RESULT_PHOTOS_ASSETS =====


# ===== FINAL_TOP3_CMD_NAME_FIX =====

async def top3_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Совместимость для регистрации app.add_handler(CommandHandler('top3', top3_cmd)).
    """
    await top_cmd(update, context)

# ===== END_FINAL_TOP3_CMD_NAME_FIX =====


# ===== FINAL_NO_AMOUNT_ROUNDING_FIX =====

def format_milli_no_round(milli: int) -> str:
    """
    Показывает сумму без округления:
    1000 -> 1
    1500 -> 1.5
    1234 -> 1.234
    """
    milli = int(milli or 0)
    sign = '-' if milli < 0 else ''
    milli = abs(milli)

    whole = milli // 1000
    frac = milli % 1000

    if frac == 0:
        return f'{sign}{whole}'

    frac_text = f'{frac:03d}'.rstrip('0')
    return f'{sign}{whole}.{frac_text}'


def money(milli: int) -> str:
    return f'{format_milli_no_round(milli)} 💵'


def money_balance(milli: int) -> str:
    return f'{format_milli_no_round(milli)} 💵'

# ===== END_FINAL_NO_AMOUNT_ROUNDING_FIX =====


# ===== FINAL_COMMA_DECIMAL_AMOUNTS =====

def format_milli_no_round(milli: int) -> str:
    """
    Показывает сумму без округления и с запятой:
    1000 -> 1
    1500 -> 1,5
    1234 -> 1,234
    """
    milli = int(milli or 0)
    sign = '-' if milli < 0 else ''
    milli = abs(milli)

    whole = milli // 1000
    frac = milli % 1000

    if frac == 0:
        return f'{sign}{whole}'

    frac_text = f'{frac:03d}'.rstrip('0')
    return f'{sign}{whole},{frac_text}'


def money(milli: int) -> str:
    return f'{format_milli_no_round(milli)} 💵'


def money_balance(milli: int) -> str:
    return f'{format_milli_no_round(milli)} 💵'

# ===== END_FINAL_COMMA_DECIMAL_AMOUNTS =====


# ===== FINAL_VIRTUAL_CRYPTO_MARKET =====

from pathlib import Path as _CryptoAssetsPath

CRYPTO_ASSETS_DIR = _CryptoAssetsPath("/app/data/assets/crypto")

CRYPTO_PHOTOS = {
    "market": CRYPTO_ASSETS_DIR / "market.jpg",
    "portfolio": CRYPTO_ASSETS_DIR / "portfolio.jpg",
    "BTC": CRYPTO_ASSETS_DIR / "btc.jpg",
    "ETH": CRYPTO_ASSETS_DIR / "eth.jpg",
    "TON": CRYPTO_ASSETS_DIR / "ton.jpg",
    "SOL": CRYPTO_ASSETS_DIR / "sol.jpg",
    "DOGE": CRYPTO_ASSETS_DIR / "doge.jpg",
}

CRYPTO_COINS = {
    "BTC": {"name": "Bitcoin", "start_milli": 65000000, "vol_bps": 500},
    "ETH": {"name": "Ethereum", "start_milli": 3200000, "vol_bps": 650},
    "TON": {"name": "Toncoin", "start_milli": 5500, "vol_bps": 850},
    "SOL": {"name": "Solana", "start_milli": 150000, "vol_bps": 950},
    "DOGE": {"name": "Dogecoin", "start_milli": 150, "vol_bps": 1200},
}

CRYPTO_UPDATE_SECONDS = 15 * 60
CRYPTO_FEE_BPS = 20
CRYPTO_MIN_BUY_MILLI = 1000
CRYPTO_MAX_PORTFOLIO_INVESTED_MILLI = 10**18
CRYPTO_MIN_SELL_AGE_SECONDS = 5 * 60


def crypto_photo_path(key: str):
    path = CRYPTO_PHOTOS.get(key)
    if path and path.exists() and path.is_file():
        return str(path)
    return None


async def crypto_send_photo_or_text(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, photo_key: str | None = None, reply_markup=None, reply_to_message_id: int | None = None):
    photo_path = crypto_photo_path(photo_key) if photo_key else None
    safe_html = pe(text)

    if photo_path:
        try:
            with open(photo_path, "rb") as photo:
                return await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=photo,
                    caption=safe_html,
                    parse_mode='HTML',
                    reply_markup=reply_markup,
                    reply_to_message_id=reply_to_message_id,
                )
        except Exception:
            pass

    try:
        return await context.bot.send_message(
            chat_id=chat_id,
            text=safe_html,
            parse_mode='HTML',
            reply_markup=reply_markup,
            reply_to_message_id=reply_to_message_id,
        )
    except Exception:
        plain = re.sub(r'<tg-emoji emoji-id="\d+">(.*?)</tg-emoji>', r'\1', safe_html)
        plain = re.sub(r'<a href="[^"]+">(.*?)</a>', r'\1', plain)
        plain = re.sub(r'</?(?:b|i|u|s|code|pre|blockquote|span)[^>]*>', '', plain)
        if 'safe_tg_text' in globals():
            plain = safe_tg_text(plain)
        return await context.bot.send_message(
            chat_id=chat_id,
            text=plain,
            reply_markup=reply_markup,
            reply_to_message_id=reply_to_message_id,
        )


def ensure_crypto_tables():
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS crypto_market (
                symbol TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                price_milli INTEGER NOT NULL,
                last_price_milli INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS crypto_holdings (
                user_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                qty_micro INTEGER NOT NULL DEFAULT 0,
                invested_milli INTEGER NOT NULL DEFAULT 0,
                avg_price_milli INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (user_id, symbol)
            )
            """
        )
        now = ts()
        for symbol, info in CRYPTO_COINS.items():
            conn.execute(
                """
                INSERT OR IGNORE INTO crypto_market (symbol, name, price_milli, last_price_milli, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (symbol, info["name"], info["start_milli"], info["start_milli"], now),
            )
        conn.commit()


def crypto_update_market_if_needed():
    ensure_crypto_tables()
    now = ts()

    with db() as conn:
        rows = conn.execute("SELECT symbol, price_milli, updated_at FROM crypto_market").fetchall()

        for symbol, price_milli, updated_at in rows:
            if now - int(updated_at or 0) < CRYPTO_UPDATE_SECONDS:
                continue

            info = CRYPTO_COINS.get(symbol)
            if not info:
                continue

            old_price = max(1, int(price_milli or info["start_milli"]))
            vol_bps = int(info["vol_bps"])
            change_bps = random.randint(-vol_bps, vol_bps)

            # Маленький шанс пампа/дампа.
            if random.randint(1, 100) <= 8:
                change_bps += random.choice([-1, 1]) * random.randint(vol_bps // 2, vol_bps)

            new_price = max(1, int(old_price * (10000 + change_bps) / 10000))

            conn.execute(
                "UPDATE crypto_market SET last_price_milli=?, price_milli=?, updated_at=? WHERE symbol=?",
                (old_price, new_price, now, symbol),
            )

        conn.commit()


def crypto_market_rows():
    crypto_update_market_if_needed()
    with db() as conn:
        return conn.execute(
            "SELECT symbol, name, price_milli, last_price_milli, updated_at FROM crypto_market ORDER BY CASE symbol WHEN 'BTC' THEN 1 WHEN 'ETH' THEN 2 WHEN 'TON' THEN 3 WHEN 'SOL' THEN 4 WHEN 'DOGE' THEN 5 ELSE 99 END"
        ).fetchall()


def crypto_price(symbol: str) -> int | None:
    crypto_update_market_if_needed()
    symbol = symbol.upper()
    with db() as conn:
        row = conn.execute("SELECT price_milli FROM crypto_market WHERE symbol=?", (symbol,)).fetchone()
    return int(row[0]) if row else None


def crypto_format_price(price_milli: int) -> str:
    return money(int(price_milli))


def crypto_format_qty(qty_micro: int) -> str:
    qty_micro = int(qty_micro or 0)
    whole = qty_micro // 1_000_000
    frac = qty_micro % 1_000_000
    if frac == 0:
        return str(whole)
    return f"{whole},{str(frac).zfill(6).rstrip('0')}"


def crypto_change_text(price_milli: int, last_price_milli: int) -> str:
    price_milli = int(price_milli or 0)
    last_price_milli = int(last_price_milli or price_milli or 1)

    if last_price_milli <= 0:
        return "0%"

    diff_bps = int((price_milli - last_price_milli) * 10000 / last_price_milli)
    sign = "+" if diff_bps >= 0 else "-"
    abs_bps = abs(diff_bps)
    percent_whole = abs_bps // 100
    percent_frac = abs_bps % 100
    arrow = "📈" if diff_bps >= 0 else "📉"
    return f"{arrow} {sign}{percent_whole},{str(percent_frac).zfill(2)}%"


def crypto_menu_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("₿ BTC", callback_data="crypto_coin:BTC"),
            InlineKeyboardButton("♦️ ETH", callback_data="crypto_coin:ETH"),
        ],
        [
            InlineKeyboardButton("💎 TON", callback_data="crypto_coin:TON"),
            InlineKeyboardButton("☀️ SOL", callback_data="crypto_coin:SOL"),
        ],
        [
            InlineKeyboardButton("🐶 DOGE", callback_data="crypto_coin:DOGE"),
        ],
        [
            InlineKeyboardButton("💼 Портфель", callback_data="crypto_portfolio"),
            InlineKeyboardButton("🔄 Обновить курс", callback_data="crypto_menu"),
        ],
    ])


def crypto_coin_keyboard(symbol: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Купить 1 💵", callback_data=f"crypto_buy:{symbol}:1000"),
            InlineKeyboardButton("Купить 5 💵", callback_data=f"crypto_buy:{symbol}:5000"),
            InlineKeyboardButton("Купить 10 💵", callback_data=f"crypto_buy:{symbol}:10000"),
        ],
        [
            InlineKeyboardButton("Продать всё", callback_data=f"crypto_sell_all:{symbol}"),
            InlineKeyboardButton("💼 Портфель", callback_data="crypto_portfolio"),
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data="crypto_menu"),
        ],
    ])


def crypto_menu_text() -> str:
    rows = crypto_market_rows()

    lines = [
        "🪙 <b>Криптовалюта</b>\n",
        "Виртуальный рынок с игровыми курсами, похожими на реальные.",
        "Курс обновляется каждые <b>15 минут</b>.\n",
    ]

    for symbol, name, price_milli, last_price_milli, updated_at in rows:
        lines.append(
            f"<b>{symbol}</b> — {html.escape(name)}\n"
            f"Курс: <b>{crypto_format_price(price_milli)}</b> · {crypto_change_text(price_milli, last_price_milli)}"
        )

    lines.append(
        "\nКоманды:\n"
        "<code>крипта купить BTC 10</code>\n"
        "<code>крипта продать BTC все</code>\n"
        "<code>крипта портфель</code>"
    )

    return "\n\n".join(lines)


def crypto_coin_text(symbol: str) -> str:
    rows = crypto_market_rows()
    data = None
    for row in rows:
        if row[0] == symbol:
            data = row
            break

    if not data:
        return "❌ Валюта не найдена."

    symbol, name, price_milli, last_price_milli, updated_at = data
    info = CRYPTO_COINS.get(symbol, {})
    vol = int(info.get("vol_bps", 0)) / 100

    return (
        f"🪙 <b>{symbol}</b> — <b>{html.escape(name)}</b>\n\n"
        f"Курс: <b>{crypto_format_price(price_milli)}</b>\n"
        f"Изменение: <b>{crypto_change_text(price_milli, last_price_milli)}</b>\n"
        f"Волатильность: <b>до {str(vol).replace('.', ',')}%</b>\n\n"
        f"Команды:\n"
        f"<code>крипта купить {symbol} 10</code>\n"
        f"<code>крипта продать {symbol} все</code>\n"
        f"<code>крипта продать {symbol} 50%</code>\n\n"
        f"Комиссия покупки/продажи: <b>0,2%</b>\n"
        f"Минимальная покупка: <b>1 💵</b>\n"
        f""
    )


def user_crypto_portfolio_rows(user_id: int):
    crypto_update_market_if_needed()
    with db() as conn:
        return conn.execute(
            """
            SELECT h.symbol, h.qty_micro, h.invested_milli, h.avg_price_milli, h.updated_at,
                   m.price_milli, m.name
            FROM crypto_holdings h
            JOIN crypto_market m ON m.symbol=h.symbol
            WHERE h.user_id=? AND h.qty_micro > 0
            ORDER BY h.symbol ASC
            """,
            (user_id,),
        ).fetchall()


def crypto_portfolio_text(user_id: int) -> str:
    rows = user_crypto_portfolio_rows(user_id)

    if not rows:
        return (
            "💼 <b>Портфель</b>\n\n"
            "У тебя пока нет криптовалюты.\n\n"
            "Пример покупки:\n"
            "<code>крипта купить BTC 10</code>"
        )

    lines = ["💼 <b>Портфель</b>\n"]
    total_value = 0
    total_invested = 0

    for symbol, qty_micro, invested_milli, avg_price_milli, updated_at, price_milli, name in rows:
        value_milli = int(int(qty_micro) * int(price_milli) / 1_000_000)
        profit_milli = value_milli - int(invested_milli or 0)
        total_value += value_milli
        total_invested += int(invested_milli or 0)
        sign = "+" if profit_milli >= 0 else ""

        lines.append(
            f"<b>{symbol}</b> — {html.escape(str(name))}\n"
            f"Количество: <b>{crypto_format_qty(qty_micro)}</b>\n"
            f"Куплено на: <b>{money(invested_milli)}</b>\n"
            f"Сейчас стоит: <b>{money(value_milli)}</b>\n"
            f"Профит: <b>{sign}{money(profit_milli)}</b>"
        )

    total_profit = total_value - total_invested
    total_sign = "+" if total_profit >= 0 else ""

    lines.append(
        f"<b>Итого</b>\n"
        f"Стоимость: <b>{money(total_value)}</b>\n"
        f"Профит: <b>{total_sign}{money(total_profit)}</b>"
    )

    return "\n\n".join(lines)


def crypto_total_invested(user_id: int) -> int:
    with db() as conn:
        row = conn.execute("SELECT COALESCE(SUM(invested_milli), 0) FROM crypto_holdings WHERE user_id=?", (user_id,)).fetchone()
    return int(row[0] or 0) if row else 0


def crypto_buy(user_id: int, symbol: str, amount_milli: int) -> tuple[bool, str]:
    ensure_crypto_tables()
    symbol = symbol.upper()

    if symbol not in CRYPTO_COINS:
        return False, "Такой криптовалюты нет."

    if amount_milli < CRYPTO_MIN_BUY_MILLI:
        return False, f"Минимальная покупка: {money(CRYPTO_MIN_BUY_MILLI)}."

    if crypto_total_invested(user_id) + amount_milli > CRYPTO_MAX_PORTFOLIO_INVESTED_MILLI:
        return False, f"Лимит вложений в крипту: {money(CRYPTO_MAX_PORTFOLIO_INVESTED_MILLI)}."

    user_row = get_user(user_id)
    if not user_row:
        return False, "Профиль не найден. Напиши /start."

    balance_milli = int(user_row[4])
    if balance_milli < amount_milli:
        return False, f"Недостаточно средств. Баланс: {money(balance_milli)}."

    price_milli = crypto_price(symbol)
    if not price_milli:
        return False, "Не удалось получить курс."

    fee_milli = int(amount_milli * CRYPTO_FEE_BPS / 10000)
    net_milli = amount_milli - fee_milli
    qty_micro = int(net_milli * 1_000_000 / price_milli)

    if qty_micro <= 0:
        return False, "Сумма слишком маленькая для покупки."

    ok, msg = take_balance(user_id, amount_milli)
    if not ok:
        return False, msg

    now = ts()
    with db() as conn:
        old = conn.execute(
            "SELECT qty_micro, invested_milli FROM crypto_holdings WHERE user_id=? AND symbol=?",
            (user_id, symbol),
        ).fetchone()

        if old:
            old_qty, old_invested = int(old[0] or 0), int(old[1] or 0)
            new_qty = old_qty + qty_micro
            new_invested = old_invested + amount_milli
            avg_price = int(new_invested * 1_000_000 / max(1, new_qty))
            conn.execute(
                "UPDATE crypto_holdings SET qty_micro=?, invested_milli=?, avg_price_milli=?, updated_at=? WHERE user_id=? AND symbol=?",
                (new_qty, new_invested, avg_price, now, user_id, symbol),
            )
        else:
            conn.execute(
                "INSERT INTO crypto_holdings (user_id, symbol, qty_micro, invested_milli, avg_price_milli, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (user_id, symbol, qty_micro, amount_milli, price_milli, now),
            )

        conn.commit()

    return True, (
        f"✅ <b>Покупка выполнена</b>\n\n"
        f"Валюта: <b>{symbol}</b>\n"
        f"Потрачено: <b>{money(amount_milli)}</b>\n"
        f"Комиссия: <b>{money(fee_milli)}</b>\n"
        f"Получено: <b>{crypto_format_qty(qty_micro)} {symbol}</b>\n"
        f"Курс: <b>{crypto_format_price(price_milli)}</b>"
    )


def crypto_sell(user_id: int, symbol: str, part_raw: str) -> tuple[bool, str]:
    ensure_crypto_tables()
    symbol = symbol.upper()

    if symbol not in CRYPTO_COINS:
        return False, "Такой криптовалюты нет."

    price_milli = crypto_price(symbol)
    if not price_milli:
        return False, "Не удалось получить курс."

    with db() as conn:
        row = conn.execute(
            "SELECT qty_micro, invested_milli, updated_at FROM crypto_holdings WHERE user_id=? AND symbol=?",
            (user_id, symbol),
        ).fetchone()

        if not row or int(row[0] or 0) <= 0:
            return False, f"У тебя нет {symbol}."

        qty_micro, invested_milli, updated_at = int(row[0]), int(row[1] or 0), int(row[2] or 0)

        if ts() - updated_at < CRYPTO_MIN_SELL_AGE_SECONDS:
            left = CRYPTO_MIN_SELL_AGE_SECONDS - (ts() - updated_at)
            return False, f"Продажа будет доступна через {left} сек."

        part = (part_raw or '').strip().lower()

        if part in ('все', 'all', '100', '100%'):
            sell_qty = qty_micro
        elif part.endswith('%'):
            try:
                pct = int(part[:-1])
            except Exception:
                return False, "Процент продажи должен быть числом."
            if pct <= 0 or pct > 100:
                return False, "Процент должен быть от 1% до 100%."
            sell_qty = int(qty_micro * pct / 100)
        else:
            try:
                pct = int(part)
                if pct <= 0 or pct > 100:
                    return False, "Можно продать процент от 1 до 100 или написать «все»."
                sell_qty = int(qty_micro * pct / 100)
            except Exception:
                return False, "Напиши: <code>крипта продать BTC все</code> или <code>крипта продать BTC 50%</code>."

        sell_qty = max(1, min(qty_micro, sell_qty))
        gross_milli = int(sell_qty * price_milli / 1_000_000)
        fee_milli = int(gross_milli * CRYPTO_FEE_BPS / 10000)
        receive_milli = max(0, gross_milli - fee_milli)

        invested_part = int(invested_milli * sell_qty / max(1, qty_micro))
        new_qty = qty_micro - sell_qty
        new_invested = max(0, invested_milli - invested_part)

        if new_qty <= 0:
            conn.execute("DELETE FROM crypto_holdings WHERE user_id=? AND symbol=?", (user_id, symbol))
        else:
            avg_price = int(new_invested * 1_000_000 / max(1, new_qty))
            conn.execute(
                "UPDATE crypto_holdings SET qty_micro=?, invested_milli=?, avg_price_milli=?, updated_at=? WHERE user_id=? AND symbol=?",
                (new_qty, new_invested, avg_price, ts(), user_id, symbol),
            )

        conn.commit()

    add_balance(user_id, receive_milli)
    profit_milli = receive_milli - invested_part
    sign = "+" if profit_milli >= 0 else ""

    return True, (
        f"✅ <b>Продажа выполнена</b>\n\n"
        f"Валюта: <b>{symbol}</b>\n"
        f"Продано: <b>{crypto_format_qty(sell_qty)} {symbol}</b>\n"
        f"Получено: <b>{money(receive_milli)}</b>\n"
        f"Комиссия: <b>{money(fee_milli)}</b>\n"
        f"Профит: <b>{sign}{money(profit_milli)}</b>\n"
        f"Курс: <b>{crypto_format_price(price_milli)}</b>"
    )


async def show_crypto_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    chat_id = update.effective_chat.id if update.effective_chat else update.callback_query.message.chat.id
    reply_to = update.message.message_id if getattr(update, 'message', None) else None

    await crypto_send_photo_or_text(
        context=context,
        chat_id=chat_id,
        text=crypto_menu_text(),
        photo_key="market",
        reply_markup=crypto_menu_keyboard(),
        reply_to_message_id=reply_to,
    )


async def show_crypto_coin(update: Update, context: ContextTypes.DEFAULT_TYPE, symbol: str):
    chat_id = update.effective_chat.id if update.effective_chat else update.callback_query.message.chat.id
    reply_to = update.message.message_id if getattr(update, 'message', None) else None

    await crypto_send_photo_or_text(
        context=context,
        chat_id=chat_id,
        text=crypto_coin_text(symbol),
        photo_key=symbol,
        reply_markup=crypto_coin_keyboard(symbol),
        reply_to_message_id=reply_to,
    )


async def show_crypto_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)

    chat_id = update.effective_chat.id if update.effective_chat else update.callback_query.message.chat.id
    reply_to = update.message.message_id if getattr(update, 'message', None) else None

    await crypto_send_photo_or_text(
        context=context,
        chat_id=chat_id,
        text=crypto_portfolio_text(update.effective_user.id),
        photo_key="portfolio",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад к рынку", callback_data="crypto_menu")]]),
        reply_to_message_id=reply_to,
    )


def dashboard_message_menu():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton('👤 Профиль', callback_data='profile'),
            InlineKeyboardButton('📊 Статистика', callback_data='profile_stats'),
        ],
        [
            InlineKeyboardButton('🪙 Криптовалюта', callback_data='crypto_menu'),
            InlineKeyboardButton('👏 Ежедневный EXP', callback_data='daily_exp'),
        ],
        [
            InlineKeyboardButton('🏰 Кланы', callback_data='clans'),
            InlineKeyboardButton('💸 Вывести', callback_data='withdraw'),
        ],
        [
            InlineKeyboardButton('👥 Группа', url='https://t.me/bezdnao'),
        ],
    ])


_previous_buttons_for_crypto_market = buttons

async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ''

    if data == 'crypto_menu':
        await q.answer()
        await show_crypto_menu(update, context)
        return

    if data == 'crypto_portfolio':
        await q.answer()
        await show_crypto_portfolio(update, context)
        return

    if data.startswith('crypto_coin:'):
        await q.answer()
        symbol = data.split(':', 1)[1].upper()
        await show_crypto_coin(update, context, symbol)
        return

    if data.startswith('crypto_buy:'):
        await q.answer()
        _, symbol, amount_raw = data.split(':')
        ok, msg = crypto_buy(q.from_user.id, symbol, int(amount_raw))
        await q.message.reply_text(pe(('' if ok else '❌ ') + msg), parse_mode='HTML')
        return

    if data.startswith('crypto_sell_all:'):
        await q.answer()
        symbol = data.split(':', 1)[1].upper()
        ok, msg = crypto_sell(q.from_user.id, symbol, 'все')
        await q.message.reply_text(pe(('' if ok else '❌ ') + msg), parse_mode='HTML')
        return

    return await _previous_buttons_for_crypto_market(update, context)


_previous_trigger_for_crypto_market = trigger

async def trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text:
        raw = update.message.text.strip()
        low = raw.lower()
        parts = raw.split()

        if low in ('крипта', 'криптовалюта', '🪙 криптовалюта'):
            await show_crypto_menu(update, context)
            return

        if low in ('крипта курс', 'курс крипты', 'криптовалюта курс'):
            await show_crypto_menu(update, context)
            return

        if low in ('крипта портфель', 'портфель крипты', 'криптовалюта портфель'):
            await show_crypto_portfolio(update, context)
            return

        if len(parts) >= 4 and parts[0].lower() in ('крипта', 'криптовалюта') and parts[1].lower() == 'купить':
            symbol = parts[2].upper()
            amount_milli = parse_money(parts[3])
            if amount_milli is None:
                await send_result(update, context, '❌ Введите сумму числом. Пример: <code>крипта купить BTC 10</code>')
                return
            ok, msg = crypto_buy(update.effective_user.id, symbol, amount_milli)
            await send_result(update, context, ('' if ok else '❌ ') + msg)
            return

        if len(parts) >= 4 and parts[0].lower() in ('крипта', 'криптовалюта') and parts[1].lower() == 'продать':
            symbol = parts[2].upper()
            part = parts[3]
            ok, msg = crypto_sell(update.effective_user.id, symbol, part)
            await send_result(update, context, ('' if ok else '❌ ') + msg)
            return

        if len(parts) >= 2 and parts[0].lower() in ('крипта', 'криптовалюта') and parts[1].upper() in CRYPTO_COINS:
            await show_crypto_coin(update, context, parts[1].upper())
            return

    return await _previous_trigger_for_crypto_market(update, context)


# ===== END_FINAL_VIRTUAL_CRYPTO_MARKET =====


# ===== FINAL_CRYPTO_MINIMAL_UI_FIX =====

def crypto_money_short(milli: int) -> str:
    milli = int(milli or 0)
    sign = '-' if milli < 0 else ''
    milli = abs(milli)
    value = milli / 1000

    if value >= 1000:
        txt = f'{value:,.0f}'.replace(',', ' ')
    elif value >= 100:
        txt = f'{value:.0f}'
    elif value >= 10:
        txt = f'{value:.1f}'.rstrip('0').rstrip('.')
    else:
        txt = f'{value:.2f}'.rstrip('0').rstrip('.')

    return sign + txt.replace('.', ',') + ' 💵'


def crypto_qty_short(qty_micro: int) -> str:
    qty = int(qty_micro or 0) / 1_000_000
    if qty >= 100:
        txt = f'{qty:.0f}'
    elif qty >= 1:
        txt = f'{qty:.3f}'
    elif qty >= 0.01:
        txt = f'{qty:.4f}'
    else:
        txt = f'{qty:.6f}'
    return txt.rstrip('0').rstrip('.').replace('.', ',')


def crypto_change_short(price_milli: int, last_price_milli: int) -> str:
    price_milli = int(price_milli or 0)
    last_price_milli = int(last_price_milli or price_milli or 1)
    if last_price_milli <= 0:
        return '0%'
    diff = ((price_milli - last_price_milli) / last_price_milli) * 100
    sign = '+' if diff >= 0 else ''
    return f'{sign}{diff:.1f}%'.replace('.', ',')


def crypto_menu_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("BTC", callback_data="crypto_coin:BTC"),
            InlineKeyboardButton("ETH", callback_data="crypto_coin:ETH"),
            InlineKeyboardButton("TON", callback_data="crypto_coin:TON"),
        ],
        [
            InlineKeyboardButton("SOL", callback_data="crypto_coin:SOL"),
            InlineKeyboardButton("DOGE", callback_data="crypto_coin:DOGE"),
        ],
        [
            InlineKeyboardButton("Портфель", callback_data="crypto_portfolio"),
            InlineKeyboardButton("Команды", callback_data="crypto_commands"),
        ],
        [
            InlineKeyboardButton("Обновить", callback_data="crypto_menu"),
        ],
    ])


def crypto_coin_keyboard(symbol: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Купить 1", callback_data=f"crypto_buy:{symbol}:1000"),
            InlineKeyboardButton("Купить 5", callback_data=f"crypto_buy:{symbol}:5000"),
            InlineKeyboardButton("Купить 10", callback_data=f"crypto_buy:{symbol}:10000"),
        ],
        [
            InlineKeyboardButton("Продать всё", callback_data=f"crypto_sell_all:{symbol}"),
            InlineKeyboardButton("Портфель", callback_data="crypto_portfolio"),
        ],
        [
            InlineKeyboardButton("Назад", callback_data="crypto_menu"),
        ],
    ])


def crypto_portfolio_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Продать всё", callback_data="crypto_sell_all_portfolio")],
        [
            InlineKeyboardButton("Рынок", callback_data="crypto_menu"),
            InlineKeyboardButton("Команды", callback_data="crypto_commands"),
        ],
    ])


def crypto_commands_text() -> str:
    return (
        "🪙 <b>Команды</b>\n\n"
        "<code>крипта</code> — рынок\n"
        "<code>крипта портфель</code> — портфель\n"
        "<code>крипта BTC</code> — карточка валюты\n"
        "<code>крипта купить BTC 10</code> — купить\n"
        "<code>крипта продать BTC все</code> — продать всё\n"
        "<code>крипта продать BTC 50%</code> — продать часть\n\n"
        "Валюты: <b>BTC, ETH, TON, SOL, DOGE</b>"
    )


def crypto_menu_text() -> str:
    rows = crypto_market_rows()
    lines = ["🪙 <b>Криптовалюта</b>\n"]

    for symbol, name, price_milli, last_price_milli, updated_at in rows:
        lines.append(f"<b>{symbol}</b> — {crypto_money_short(price_milli)} · {crypto_change_short(price_milli, last_price_milli)}")

    lines.append("\nПокупка от <b>1 💵</b> · комиссия <b>0,2%</b>")
    return "\n".join(lines)


def crypto_coin_text(symbol: str) -> str:
    rows = crypto_market_rows()
    data = None
    for row in rows:
        if row[0] == symbol:
            data = row
            break

    if not data:
        return "❌ Валюта не найдена."

    symbol, name, price_milli, last_price_milli, updated_at = data
    return (
        f"🪙 <b>{symbol}</b>\n\n"
        f"Курс — <b>{crypto_money_short(price_milli)}</b>\n"
        f"Изменение — <b>{crypto_change_short(price_milli, last_price_milli)}</b>\n\n"
        f"Покупка от <b>1 💵</b>\n"
        f"Комиссия — <b>0,2%</b>"
    )


def crypto_portfolio_text(user_id: int) -> str:
    rows = user_crypto_portfolio_rows(user_id)
    if not rows:
        return (
            "💼 <b>Портфель</b>\n\n"
            "Пока пусто.\n\n"
            "<code>крипта купить BTC 10</code>"
        )

    lines = ["💼 <b>Портфель</b>\n"]
    total_value = 0
    total_invested = 0

    for symbol, qty_micro, invested_milli, avg_price_milli, updated_at, price_milli, name in rows:
        value_milli = int(int(qty_micro) * int(price_milli) / 1_000_000)
        profit_milli = value_milli - int(invested_milli or 0)
        total_value += value_milli
        total_invested += int(invested_milli or 0)
        sign = "+" if profit_milli >= 0 else ""

        lines.append(
            f"<b>{symbol}</b> — {crypto_qty_short(qty_micro)}\n"
            f"Стоимость: <b>{crypto_money_short(value_milli)}</b>\n"
            f"Профит: <b>{sign}{crypto_money_short(profit_milli)}</b>"
        )

    total_profit = total_value - total_invested
    sign = "+" if total_profit >= 0 else ""
    lines.append(
        f"<b>Итого</b>\n"
        f"Стоимость: <b>{crypto_money_short(total_value)}</b>\n"
        f"Профит: <b>{sign}{crypto_money_short(total_profit)}</b>"
    )
    return "\n\n".join(lines)


def crypto_buy(user_id: int, symbol: str, amount_milli: int) -> tuple[bool, str]:
    ensure_crypto_tables()
    symbol = symbol.upper()

    if symbol not in CRYPTO_COINS:
        return False, "Такой криптовалюты нет."
    if amount_milli < CRYPTO_MIN_BUY_MILLI:
        return False, f"Минимальная покупка: {crypto_money_short(CRYPTO_MIN_BUY_MILLI)}."
    if crypto_total_invested(user_id) + amount_milli > CRYPTO_MAX_PORTFOLIO_INVESTED_MILLI:
        return False, f"Лимит вложений: {crypto_money_short(CRYPTO_MAX_PORTFOLIO_INVESTED_MILLI)}."

    user_row = get_user(user_id)
    if not user_row:
        return False, "Профиль не найден. Напиши /start."

    balance_milli = int(user_row[4])
    if balance_milli < amount_milli:
        return False, f"Недостаточно средств. Баланс: {crypto_money_short(balance_milli)}."

    price_milli = crypto_price(symbol)
    if not price_milli:
        return False, "Не удалось получить курс."

    fee_milli = int(amount_milli * CRYPTO_FEE_BPS / 10000)
    net_milli = amount_milli - fee_milli
    qty_micro = int(net_milli * 1_000_000 / price_milli)

    if qty_micro <= 0:
        return False, "Сумма слишком маленькая для покупки."

    ok, msg = take_balance(user_id, amount_milli)
    if not ok:
        return False, msg

    now = ts()
    with db() as conn:
        old = conn.execute("SELECT qty_micro, invested_milli FROM crypto_holdings WHERE user_id=? AND symbol=?", (user_id, symbol)).fetchone()
        if old:
            old_qty, old_invested = int(old[0] or 0), int(old[1] or 0)
            new_qty = old_qty + qty_micro
            new_invested = old_invested + amount_milli
            avg_price = int(new_invested * 1_000_000 / max(1, new_qty))
            conn.execute("UPDATE crypto_holdings SET qty_micro=?, invested_milli=?, avg_price_milli=?, updated_at=? WHERE user_id=? AND symbol=?", (new_qty, new_invested, avg_price, now, user_id, symbol))
        else:
            conn.execute("INSERT INTO crypto_holdings (user_id, symbol, qty_micro, invested_milli, avg_price_milli, updated_at) VALUES (?, ?, ?, ?, ?, ?)", (user_id, symbol, qty_micro, amount_milli, price_milli, now))
        conn.commit()

    return True, (
        f"✅ <b>Куплено</b>\n\n"
        f"{symbol}: <b>{crypto_qty_short(qty_micro)}</b>\n"
        f"Сумма: <b>{crypto_money_short(amount_milli)}</b>\n"
        f"Комиссия: <b>{crypto_money_short(fee_milli)}</b>\n"
        f"Курс: <b>{crypto_money_short(price_milli)}</b>"
    )


def crypto_sell(user_id: int, symbol: str, part_raw: str) -> tuple[bool, str]:
    ensure_crypto_tables()
    symbol = symbol.upper()

    if symbol not in CRYPTO_COINS:
        return False, "Такой криптовалюты нет."

    price_milli = crypto_price(symbol)
    if not price_milli:
        return False, "Не удалось получить курс."

    with db() as conn:
        row = conn.execute("SELECT qty_micro, invested_milli, updated_at FROM crypto_holdings WHERE user_id=? AND symbol=?", (user_id, symbol)).fetchone()
        if not row or int(row[0] or 0) <= 0:
            return False, f"У тебя нет {symbol}."

        qty_micro, invested_milli, updated_at = int(row[0]), int(row[1] or 0), int(row[2] or 0)
        if ts() - updated_at < CRYPTO_MIN_SELL_AGE_SECONDS:
            left = CRYPTO_MIN_SELL_AGE_SECONDS - (ts() - updated_at)
            return False, f"Продажа будет доступна через {left} сек."

        part = (part_raw or '').strip().lower()
        if part in ('все', 'all', '100', '100%'):
            sell_qty = qty_micro
        elif part.endswith('%'):
            try:
                pct = int(part[:-1])
            except Exception:
                return False, "Процент должен быть числом."
            if pct <= 0 or pct > 100:
                return False, "Процент от 1% до 100%."
            sell_qty = int(qty_micro * pct / 100)
        else:
            try:
                pct = int(part)
                if pct <= 0 or pct > 100:
                    return False, "Можно продать от 1% до 100% или «все»."
                sell_qty = int(qty_micro * pct / 100)
            except Exception:
                return False, "Напиши: <code>крипта продать BTC все</code>."

        sell_qty = max(1, min(qty_micro, sell_qty))
        gross_milli = int(sell_qty * price_milli / 1_000_000)
        fee_milli = int(gross_milli * CRYPTO_FEE_BPS / 10000)
        receive_milli = max(0, gross_milli - fee_milli)
        invested_part = int(invested_milli * sell_qty / max(1, qty_micro))
        new_qty = qty_micro - sell_qty
        new_invested = max(0, invested_milli - invested_part)

        if new_qty <= 0:
            conn.execute("DELETE FROM crypto_holdings WHERE user_id=? AND symbol=?", (user_id, symbol))
        else:
            avg_price = int(new_invested * 1_000_000 / max(1, new_qty))
            conn.execute("UPDATE crypto_holdings SET qty_micro=?, invested_milli=?, avg_price_milli=?, updated_at=? WHERE user_id=? AND symbol=?", (new_qty, new_invested, avg_price, ts(), user_id, symbol))
        conn.commit()

    add_balance(user_id, receive_milli)
    profit_milli = receive_milli - invested_part
    sign = "+" if profit_milli >= 0 else ""
    return True, (
        f"✅ <b>Продано</b>\n\n"
        f"{symbol}: <b>{crypto_qty_short(sell_qty)}</b>\n"
        f"Получено: <b>{crypto_money_short(receive_milli)}</b>\n"
        f"Комиссия: <b>{crypto_money_short(fee_milli)}</b>\n"
        f"Профит: <b>{sign}{crypto_money_short(profit_milli)}</b>"
    )


def crypto_sell_all_portfolio(user_id: int) -> tuple[bool, str]:
    rows = user_crypto_portfolio_rows(user_id)
    if not rows:
        return False, "Портфель пуст."

    sold = []
    errors = []
    for symbol, qty_micro, invested_milli, avg_price_milli, updated_at, price_milli, name in rows:
        ok, msg = crypto_sell(user_id, symbol, 'все')
        if ok:
            sold.append(symbol)
        else:
            errors.append(f"{symbol}: {msg}")

    if not sold and errors:
        return False, "\n".join(errors[:5])

    result = "✅ <b>Портфель продан</b>\n\nПродано: <b>" + ", ".join(sold) + "</b>"
    if errors:
        result += "\n\nНе продано:\n" + "\n".join(errors[:3])
    return True, result


async def show_crypto_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)
    chat_id = update.effective_chat.id if update.effective_chat else update.callback_query.message.chat.id
    reply_to = update.message.message_id if getattr(update, 'message', None) else None
    await crypto_send_photo_or_text(context=context, chat_id=chat_id, text=crypto_portfolio_text(update.effective_user.id), photo_key="portfolio", reply_markup=crypto_portfolio_keyboard(), reply_to_message_id=reply_to)


def dashboard_message_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('Профиль', callback_data='profile'), InlineKeyboardButton('Статистика', callback_data='profile_stats')],
        [InlineKeyboardButton('Криптовалюта', callback_data='crypto_menu'), InlineKeyboardButton('Ежедневный EXP', callback_data='daily_exp')],
        [InlineKeyboardButton('Кланы', callback_data='clans'), InlineKeyboardButton('Вывести', callback_data='withdraw')],
        [InlineKeyboardButton('Группа', url='https://t.me/bezdnao')],
    ])


_previous_buttons_for_crypto_minimal_ui = buttons

async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ''

    if data == 'crypto_commands':
        await q.answer()
        await q.message.reply_text(pe(crypto_commands_text()), parse_mode='HTML')
        return

    if data == 'crypto_sell_all_portfolio':
        await q.answer()
        ok, msg = crypto_sell_all_portfolio(q.from_user.id)
        await q.message.reply_text(pe(('' if ok else '❌ ') + msg), parse_mode='HTML')
        return

    return await _previous_buttons_for_crypto_minimal_ui(update, context)

# ===== END_FINAL_CRYPTO_MINIMAL_UI_FIX =====


# ===== FINAL_CRYPTO_BINANCE_MINIMAL_UI =====

CRYPTO_TRADE_BUY = 'crypto_trade_buy'
CRYPTO_TRADE_SELL = 'crypto_trade_sell'


def crypto_pair(symbol: str) -> str:
    return f"{symbol.upper()}/USDT"


def crypto_pair_keyboard(symbol: str):
    symbol = symbol.upper()
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Купить", callback_data=f"crypto_buy_open:{symbol}"),
            InlineKeyboardButton("Продать", callback_data=f"crypto_sell_open:{symbol}"),
        ],
        [
            InlineKeyboardButton("25%", callback_data=f"crypto_sell_pct:{symbol}:25"),
            InlineKeyboardButton("50%", callback_data=f"crypto_sell_pct:{symbol}:50"),
            InlineKeyboardButton("75%", callback_data=f"crypto_sell_pct:{symbol}:75"),
            InlineKeyboardButton("100%", callback_data=f"crypto_sell_pct:{symbol}:100"),
        ],
        [
            InlineKeyboardButton("Портфель", callback_data="crypto_portfolio"),
            InlineKeyboardButton("Назад", callback_data="crypto_menu"),
        ],
    ])


def crypto_market_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("BTC/USDT", callback_data="crypto_coin:BTC"),
            InlineKeyboardButton("ETH/USDT", callback_data="crypto_coin:ETH"),
        ],
        [
            InlineKeyboardButton("TON/USDT", callback_data="crypto_coin:TON"),
            InlineKeyboardButton("SOL/USDT", callback_data="crypto_coin:SOL"),
        ],
        [InlineKeyboardButton("DOGE/USDT", callback_data="crypto_coin:DOGE")],
        [
            InlineKeyboardButton("Портфель", callback_data="crypto_portfolio"),
            InlineKeyboardButton("История", callback_data="crypto_history"),
        ],
        [
            InlineKeyboardButton("Команды", callback_data="crypto_commands"),
            InlineKeyboardButton("Обновить", callback_data="crypto_menu"),
        ],
    ])


def crypto_trade_buy_keyboard(symbol: str):
    symbol = symbol.upper()
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("1 💵", callback_data=f"crypto_buy:{symbol}:1000"),
            InlineKeyboardButton("5 💵", callback_data=f"crypto_buy:{symbol}:5000"),
            InlineKeyboardButton("10 💵", callback_data=f"crypto_buy:{symbol}:10000"),
        ],
        [
            InlineKeyboardButton("25 💵", callback_data=f"crypto_buy:{symbol}:25000"),
            InlineKeyboardButton("50 💵", callback_data=f"crypto_buy:{symbol}:50000"),
        ],
        [InlineKeyboardButton("Назад", callback_data=f"crypto_coin:{symbol}")],
    ])


def crypto_trade_sell_keyboard(symbol: str):
    symbol = symbol.upper()
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("25%", callback_data=f"crypto_sell_pct:{symbol}:25"),
            InlineKeyboardButton("50%", callback_data=f"crypto_sell_pct:{symbol}:50"),
            InlineKeyboardButton("75%", callback_data=f"crypto_sell_pct:{symbol}:75"),
            InlineKeyboardButton("100%", callback_data=f"crypto_sell_pct:{symbol}:100"),
        ],
        [InlineKeyboardButton("Назад", callback_data=f"crypto_coin:{symbol}")],
    ])


def crypto_portfolio_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Продать всё", callback_data="crypto_sell_all_portfolio")],
        [
            InlineKeyboardButton("Рынок", callback_data="crypto_menu"),
            InlineKeyboardButton("История", callback_data="crypto_history"),
        ],
    ])


def crypto_market_text() -> str:
    rows = crypto_market_rows()
    lines = ["📊 <b>Рынок</b>\n"]
    for symbol, name, price_milli, last_price_milli, updated_at in rows:
        lines.append(f"<b>{crypto_pair(symbol)}</b>    {crypto_money_short(price_milli)}    {crypto_change_short(price_milli, last_price_milli)}")
    return "\n".join(lines)


def crypto_menu_text() -> str:
    return crypto_market_text()


def crypto_user_holding(user_id: int, symbol: str):
    ensure_crypto_tables()
    with db() as conn:
        return conn.execute(
            "SELECT qty_micro, invested_milli, avg_price_milli, updated_at FROM crypto_holdings WHERE user_id=? AND symbol=?",
            (user_id, symbol.upper()),
        ).fetchone()


def crypto_coin_text_for_user(user_id: int, symbol: str) -> str:
    symbol = symbol.upper()
    data = None
    for row in crypto_market_rows():
        if row[0] == symbol:
            data = row
            break
    if not data:
        return "❌ Валюта не найдена."

    symbol, name, price_milli, last_price_milli, updated_at = data
    holding = crypto_user_holding(user_id, symbol)
    qty_micro = int(holding[0]) if holding else 0
    invested_milli = int(holding[1]) if holding else 0
    value_milli = int(qty_micro * int(price_milli) / 1_000_000)
    pnl_milli = value_milli - invested_milli
    sign = "+" if pnl_milli >= 0 else ""

    return (
        f"<b>{crypto_pair(symbol)}</b>\n\n"
        f"Цена: <b>{crypto_money_short(price_milli)}</b>\n"
        f"24ч: <b>{crypto_change_short(price_milli, last_price_milli)}</b>\n\n"
        f"Ваш {symbol}: <b>{crypto_qty_short(qty_micro)}</b>\n"
        f"Стоимость: <b>{crypto_money_short(value_milli)}</b>\n"
        f"PNL: <b>{sign}{crypto_money_short(pnl_milli)}</b>"
    )


def crypto_coin_text(symbol: str) -> str:
    return crypto_coin_text_for_user(0, symbol)


def crypto_buy_screen_text(user_id: int, symbol: str) -> str:
    symbol = symbol.upper()
    price = crypto_price(symbol) or 0
    row = get_user(user_id)
    balance = int(row[4]) if row else 0
    return (
        f"🟢 <b>Купить {crypto_pair(symbol)}</b>\n\n"
        f"Цена: <b>{crypto_money_short(price)}</b>\n"
        f"Доступно: <b>{crypto_money_short(balance)}</b>\n\n"
        f"Выбери сумму или напиши:\n"
        f"<code>крипта купить {symbol} 10</code>"
    )


def crypto_sell_screen_text(user_id: int, symbol: str) -> str:
    symbol = symbol.upper()
    price = crypto_price(symbol) or 0
    holding = crypto_user_holding(user_id, symbol)
    qty_micro = int(holding[0]) if holding else 0
    value_milli = int(qty_micro * price / 1_000_000)
    return (
        f"🔴 <b>Продать {crypto_pair(symbol)}</b>\n\n"
        f"Цена: <b>{crypto_money_short(price)}</b>\n"
        f"Доступно: <b>{crypto_qty_short(qty_micro)} {symbol}</b>\n"
        f"Стоимость: <b>{crypto_money_short(value_milli)}</b>"
    )


def crypto_portfolio_text(user_id: int) -> str:
    rows = user_crypto_portfolio_rows(user_id)
    if not rows:
        return "💼 <b>Портфель</b>\n\nБаланс пуст.\n\n<code>крипта купить BTC 10</code>"

    total_value = 0
    total_invested = 0
    lines = ["💼 <b>Портфель</b>\n"]
    for symbol, qty_micro, invested_milli, avg_price_milli, updated_at, price_milli, name in rows:
        value_milli = int(int(qty_micro) * int(price_milli) / 1_000_000)
        pnl_milli = value_milli - int(invested_milli or 0)
        total_value += value_milli
        total_invested += int(invested_milli or 0)
        sign = "+" if pnl_milli >= 0 else ""
        lines.append(
            f"<b>{symbol}</b>  {crypto_qty_short(qty_micro)}\n"
            f"{crypto_money_short(value_milli)} · PNL {sign}{crypto_money_short(pnl_milli)}"
        )

    total_pnl = total_value - total_invested
    sign = "+" if total_pnl >= 0 else ""
    lines.insert(1, f"Стоимость: <b>{crypto_money_short(total_value)}</b>\nPNL: <b>{sign}{crypto_money_short(total_pnl)}</b>\n")
    return "\n\n".join(lines)


def ensure_crypto_history_table():
    ensure_crypto_tables()
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS crypto_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                side TEXT NOT NULL,
                symbol TEXT NOT NULL,
                amount_milli INTEGER NOT NULL DEFAULT 0,
                qty_micro INTEGER NOT NULL DEFAULT 0,
                price_milli INTEGER NOT NULL DEFAULT 0,
                fee_milli INTEGER NOT NULL DEFAULT 0,
                pnl_milli INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL
            )
        """)
        conn.commit()


def add_crypto_history(user_id: int, side: str, symbol: str, amount_milli: int, qty_micro: int, price_milli: int, fee_milli: int, pnl_milli: int = 0):
    ensure_crypto_history_table()
    with db() as conn:
        conn.execute(
            "INSERT INTO crypto_history (user_id, side, symbol, amount_milli, qty_micro, price_milli, fee_milli, pnl_milli, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, side, symbol.upper(), int(amount_milli or 0), int(qty_micro or 0), int(price_milli or 0), int(fee_milli or 0), int(pnl_milli or 0), ts()),
        )
        conn.commit()


def crypto_history_text(user_id: int) -> str:
    ensure_crypto_history_table()
    with db() as conn:
        rows = conn.execute(
            "SELECT side, symbol, amount_milli, qty_micro, price_milli, pnl_milli FROM crypto_history WHERE user_id=? ORDER BY created_at DESC LIMIT 10",
            (user_id,),
        ).fetchall()

    if not rows:
        return "📜 <b>История</b>\n\nСделок пока нет."

    lines = ["📜 <b>История</b>\n"]
    for side, symbol, amount_milli, qty_micro, price_milli, pnl_milli in rows:
        side_text = "BUY" if side == "buy" else "SELL"
        pnl = ""
        if side == "sell":
            sign = "+" if int(pnl_milli or 0) >= 0 else ""
            pnl = f" · PNL {sign}{crypto_money_short(pnl_milli)}"
        lines.append(f"<b>{side_text} {symbol}</b>\n{crypto_money_short(amount_milli)} · {crypto_qty_short(qty_micro)} · {crypto_money_short(price_milli)}{pnl}")
    return "\n\n".join(lines)


_old_crypto_buy_binance_ui = crypto_buy

def crypto_buy(user_id: int, symbol: str, amount_milli: int) -> tuple[bool, str]:
    symbol = symbol.upper()
    price_before = crypto_price(symbol) or 0
    fee_milli = int(amount_milli * CRYPTO_FEE_BPS / 10000)
    net_milli = amount_milli - fee_milli
    qty_micro = int(net_milli * 1_000_000 / max(1, price_before))
    ok, msg = _old_crypto_buy_binance_ui(user_id, symbol, amount_milli)
    if ok:
        add_crypto_history(user_id, "buy", symbol, amount_milli, qty_micro, price_before, fee_milli, 0)
        msg = (
            f"✅ <b>Ордер исполнен</b>\n\n"
            f"BUY <b>{crypto_pair(symbol)}</b>\n"
            f"Сумма: <b>{crypto_money_short(amount_milli)}</b>\n"
            f"Получено: <b>{crypto_qty_short(qty_micro)} {symbol}</b>"
        )
    return ok, msg


_old_crypto_sell_binance_ui = crypto_sell

def crypto_sell(user_id: int, symbol: str, part_raw: str) -> tuple[bool, str]:
    symbol = symbol.upper()
    before = crypto_user_holding(user_id, symbol)
    before_qty = int(before[0]) if before else 0
    before_invested = int(before[1]) if before else 0
    price_before = crypto_price(symbol) or 0
    ok, msg = _old_crypto_sell_binance_ui(user_id, symbol, part_raw)
    after = crypto_user_holding(user_id, symbol)
    after_qty = int(after[0]) if after else 0
    sold_qty = max(0, before_qty - after_qty)
    gross_milli = int(sold_qty * price_before / 1_000_000)
    fee_milli = int(gross_milli * CRYPTO_FEE_BPS / 10000)
    receive_milli = max(0, gross_milli - fee_milli)
    invested_part = int(before_invested * sold_qty / max(1, before_qty)) if before_qty else 0
    pnl_milli = receive_milli - invested_part
    if ok:
        add_crypto_history(user_id, "sell", symbol, receive_milli, sold_qty, price_before, fee_milli, pnl_milli)
        sign = "+" if pnl_milli >= 0 else ""
        msg = (
            f"✅ <b>Ордер исполнен</b>\n\n"
            f"SELL <b>{crypto_pair(symbol)}</b>\n"
            f"Получено: <b>{crypto_money_short(receive_milli)}</b>\n"
            f"PNL: <b>{sign}{crypto_money_short(pnl_milli)}</b>"
        )
    return ok, msg


async def show_crypto_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)
    chat_id = update.effective_chat.id if update.effective_chat else update.callback_query.message.chat.id
    reply_to = update.message.message_id if getattr(update, 'message', None) else None
    await crypto_send_photo_or_text(context=context, chat_id=chat_id, text=crypto_market_text(), photo_key="market", reply_markup=crypto_market_keyboard(), reply_to_message_id=reply_to)


async def show_crypto_coin(update: Update, context: ContextTypes.DEFAULT_TYPE, symbol: str):
    register_user(update.effective_user)
    remember_group(update.effective_chat)
    chat_id = update.effective_chat.id if update.effective_chat else update.callback_query.message.chat.id
    reply_to = update.message.message_id if getattr(update, 'message', None) else None
    await crypto_send_photo_or_text(context=context, chat_id=chat_id, text=crypto_coin_text_for_user(update.effective_user.id, symbol), photo_key=symbol.upper(), reply_markup=crypto_pair_keyboard(symbol), reply_to_message_id=reply_to)


async def show_crypto_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    register_user(update.effective_user)
    remember_group(update.effective_chat)
    chat_id = update.effective_chat.id if update.effective_chat else update.callback_query.message.chat.id
    reply_to = update.message.message_id if getattr(update, 'message', None) else None
    await crypto_send_photo_or_text(context=context, chat_id=chat_id, text=crypto_portfolio_text(update.effective_user.id), photo_key="portfolio", reply_markup=crypto_portfolio_keyboard(), reply_to_message_id=reply_to)


_previous_buttons_for_crypto_binance_ui = buttons

async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ''

    if data == 'crypto_menu':
        await q.answer()
        await show_crypto_menu(update, context)
        return
    if data == 'crypto_portfolio':
        await q.answer()
        await show_crypto_portfolio(update, context)
        return
    if data == 'crypto_history':
        await q.answer()
        await q.message.reply_text(pe(crypto_history_text(q.from_user.id)), parse_mode='HTML')
        return
    if data == 'crypto_commands':
        await q.answer()
        await q.message.reply_text(pe(crypto_commands_text()), parse_mode='HTML')
        return
    if data.startswith('crypto_coin:'):
        await q.answer()
        symbol = data.split(':', 1)[1].upper()
        await show_crypto_coin(update, context, symbol)
        return
    if data.startswith('crypto_buy_open:'):
        await q.answer()
        symbol = data.split(':', 1)[1].upper()
        await q.message.reply_text(pe(crypto_buy_screen_text(q.from_user.id, symbol)), parse_mode='HTML', reply_markup=crypto_trade_buy_keyboard(symbol))
        return
    if data.startswith('crypto_sell_open:'):
        await q.answer()
        symbol = data.split(':', 1)[1].upper()
        await q.message.reply_text(pe(crypto_sell_screen_text(q.from_user.id, symbol)), parse_mode='HTML', reply_markup=crypto_trade_sell_keyboard(symbol))
        return
    if data.startswith('crypto_sell_pct:'):
        await q.answer()
        _, symbol, pct = data.split(':')
        part = 'все' if pct == '100' else f'{pct}%'
        ok, msg = crypto_sell(q.from_user.id, symbol, part)
        await q.message.reply_text(pe(('' if ok else '❌ ') + msg), parse_mode='HTML')
        return

    return await _previous_buttons_for_crypto_binance_ui(update, context)

# ===== END_FINAL_CRYPTO_BINANCE_MINIMAL_UI =====


# ===== FINAL_CRYPTO_NO_LIMIT_FEE_02 =====

CRYPTO_FEE_BPS = 20
CRYPTO_MAX_PORTFOLIO_INVESTED_MILLI = 10**18


def crypto_fee_text() -> str:
    return "0,2%"


def crypto_menu_text() -> str:
    rows = crypto_market_rows()
    lines = ["📊 <b>Рынок</b>\n"]

    for symbol, name, price_milli, last_price_milli, updated_at in rows:
        lines.append(
            f"<b>{crypto_pair(symbol)}</b>    {crypto_money_short(price_milli)}    {crypto_change_short(price_milli, last_price_milli)}"
        )

    lines.append(f"\nПокупка от <b>1 💵</b> · комиссия <b>{crypto_fee_text()}</b>")
    return "\n".join(lines)


def crypto_coin_text_for_user(user_id: int, symbol: str) -> str:
    symbol = symbol.upper()
    data = None

    for row in crypto_market_rows():
        if row[0] == symbol:
            data = row
            break

    if not data:
        return "❌ Валюта не найдена."

    symbol, name, price_milli, last_price_milli, updated_at = data
    holding = crypto_user_holding(user_id, symbol)

    qty_micro = int(holding[0]) if holding else 0
    invested_milli = int(holding[1]) if holding else 0
    value_milli = int(qty_micro * int(price_milli) / 1_000_000)
    pnl_milli = value_milli - invested_milli
    sign = "+" if pnl_milli >= 0 else ""

    return (
        f"<b>{crypto_pair(symbol)}</b>\n\n"
        f"Цена: <b>{crypto_money_short(price_milli)}</b>\n"
        f"24ч: <b>{crypto_change_short(price_milli, last_price_milli)}</b>\n\n"
        f"Ваш {symbol}: <b>{crypto_qty_short(qty_micro)}</b>\n"
        f"Стоимость: <b>{crypto_money_short(value_milli)}</b>\n"
        f"PNL: <b>{sign}{crypto_money_short(pnl_milli)}</b>\n\n"
        f"Комиссия: <b>{crypto_fee_text()}</b>"
    )


def crypto_buy_screen_text(user_id: int, symbol: str) -> str:
    symbol = symbol.upper()
    price = crypto_price(symbol) or 0
    row = get_user(user_id)
    balance = int(row[4]) if row else 0

    return (
        f"🟢 <b>Купить {crypto_pair(symbol)}</b>\n\n"
        f"Цена: <b>{crypto_money_short(price)}</b>\n"
        f"Доступно: <b>{crypto_money_short(balance)}</b>\n"
        f"Комиссия: <b>{crypto_fee_text()}</b>\n\n"
        f"Выбери сумму или напиши:\n"
        f"<code>крипта купить {symbol} 10</code>"
    )


def crypto_buy(user_id: int, symbol: str, amount_milli: int) -> tuple[bool, str]:
    ensure_crypto_tables()
    symbol = symbol.upper()

    if symbol not in CRYPTO_COINS:
        return False, "Такой криптовалюты нет."

    if amount_milli < CRYPTO_MIN_BUY_MILLI:
        return False, f"Минимальная покупка: {crypto_money_short(CRYPTO_MIN_BUY_MILLI)}."

    user_row = get_user(user_id)
    if not user_row:
        return False, "Профиль не найден. Напиши /start."

    balance_milli = int(user_row[4])
    if balance_milli < amount_milli:
        return False, f"Недостаточно средств. Баланс: {crypto_money_short(balance_milli)}."

    price_milli = crypto_price(symbol)
    if not price_milli:
        return False, "Не удалось получить курс."

    fee_milli = int(amount_milli * CRYPTO_FEE_BPS / 10000)
    net_milli = amount_milli - fee_milli
    qty_micro = int(net_milli * 1_000_000 / price_milli)

    if qty_micro <= 0:
        return False, "Сумма слишком маленькая для покупки."

    ok, msg = take_balance(user_id, amount_milli)
    if not ok:
        return False, msg

    now = ts()
    with db() as conn:
        old = conn.execute(
            "SELECT qty_micro, invested_milli FROM crypto_holdings WHERE user_id=? AND symbol=?",
            (user_id, symbol),
        ).fetchone()

        if old:
            old_qty, old_invested = int(old[0] or 0), int(old[1] or 0)
            new_qty = old_qty + qty_micro
            new_invested = old_invested + amount_milli
            avg_price = int(new_invested * 1_000_000 / max(1, new_qty))
            conn.execute(
                "UPDATE crypto_holdings SET qty_micro=?, invested_milli=?, avg_price_milli=?, updated_at=? WHERE user_id=? AND symbol=?",
                (new_qty, new_invested, avg_price, now, user_id, symbol),
            )
        else:
            conn.execute(
                "INSERT INTO crypto_holdings (user_id, symbol, qty_micro, invested_milli, avg_price_milli, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (user_id, symbol, qty_micro, amount_milli, price_milli, now),
            )

        conn.commit()

    return True, (
        f"✅ <b>Ордер исполнен</b>\n\n"
        f"BUY <b>{crypto_pair(symbol)}</b>\n"
        f"Сумма: <b>{crypto_money_short(amount_milli)}</b>\n"
        f"Комиссия: <b>{crypto_money_short(fee_milli)}</b>\n"
        f"Получено: <b>{crypto_qty_short(qty_micro)} {symbol}</b>"
    )

# ===== END_FINAL_CRYPTO_NO_LIMIT_FEE_02 =====


# ===== FINAL_CRYPTO_PREMIUM_EMOJI_PACK =====

_old_pe_crypto_premium_pack = pe

def pe(text: str) -> str:
    text = _old_pe_crypto_premium_pack(text)

    replacements = {
        '📊': PE_CRYPTO_CHART,
        '💼': PE_CRYPTO_PORTFOLIO,
        '📜': PE_CRYPTO_HISTORY,
        '📰': PE_CRYPTO_HISTORY,
        '🟢': PE_CRYPTO_GREEN,
        '🔴': PE_CRYPTO_RED,
        '✅': PE_CRYPTO_OK,
        '❌': PE_CRYPTO_NO,
    }

    for normal, premium in replacements.items():
        text = text.replace(normal, premium)

    return text

# ===== END_FINAL_CRYPTO_PREMIUM_EMOJI_PACK =====


# ===== FINAL_CRYPTO_BUTTON_EMOJI_FIX =====

def crypto_pair_keyboard(symbol: str):
    symbol = symbol.upper()
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🟢 Купить", callback_data=f"crypto_buy_open:{symbol}"),
            InlineKeyboardButton("🔴 Продать", callback_data=f"crypto_sell_open:{symbol}"),
        ],
        [
            InlineKeyboardButton("25%", callback_data=f"crypto_sell_pct:{symbol}:25"),
            InlineKeyboardButton("50%", callback_data=f"crypto_sell_pct:{symbol}:50"),
            InlineKeyboardButton("75%", callback_data=f"crypto_sell_pct:{symbol}:75"),
            InlineKeyboardButton("100%", callback_data=f"crypto_sell_pct:{symbol}:100"),
        ],
        [
            InlineKeyboardButton("💼 Портфель", callback_data="crypto_portfolio"),
            InlineKeyboardButton("⬅️ Назад", callback_data="crypto_menu"),
        ],
    ])


def crypto_market_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🟠 BTC/USDT", callback_data="crypto_coin:BTC"),
            InlineKeyboardButton("♦️ ETH/USDT", callback_data="crypto_coin:ETH"),
        ],
        [
            InlineKeyboardButton("💎 TON/USDT", callback_data="crypto_coin:TON"),
            InlineKeyboardButton("☀️ SOL/USDT", callback_data="crypto_coin:SOL"),
        ],
        [
            InlineKeyboardButton("🐶 DOGE/USDT", callback_data="crypto_coin:DOGE"),
        ],
        [
            InlineKeyboardButton("💼 Портфель", callback_data="crypto_portfolio"),
            InlineKeyboardButton("📜 История", callback_data="crypto_history"),
        ],
        [
            InlineKeyboardButton("📰 Команды", callback_data="crypto_commands"),
            InlineKeyboardButton("🔄 Обновить", callback_data="crypto_menu"),
        ],
    ])


def crypto_trade_buy_keyboard(symbol: str):
    symbol = symbol.upper()
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🟢 1 💵", callback_data=f"crypto_buy:{symbol}:1000"),
            InlineKeyboardButton("🟢 5 💵", callback_data=f"crypto_buy:{symbol}:5000"),
            InlineKeyboardButton("🟢 10 💵", callback_data=f"crypto_buy:{symbol}:10000"),
        ],
        [
            InlineKeyboardButton("🟢 25 💵", callback_data=f"crypto_buy:{symbol}:25000"),
            InlineKeyboardButton("🟢 50 💵", callback_data=f"crypto_buy:{symbol}:50000"),
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data=f"crypto_coin:{symbol}"),
        ],
    ])


def crypto_trade_sell_keyboard(symbol: str):
    symbol = symbol.upper()
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔴 25%", callback_data=f"crypto_sell_pct:{symbol}:25"),
            InlineKeyboardButton("🔴 50%", callback_data=f"crypto_sell_pct:{symbol}:50"),
            InlineKeyboardButton("🔴 75%", callback_data=f"crypto_sell_pct:{symbol}:75"),
            InlineKeyboardButton("🔴 100%", callback_data=f"crypto_sell_pct:{symbol}:100"),
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data=f"crypto_coin:{symbol}"),
        ],
    ])


def crypto_portfolio_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔴 Продать всё", callback_data="crypto_sell_all_portfolio"),
        ],
        [
            InlineKeyboardButton("📊 Рынок", callback_data="crypto_menu"),
            InlineKeyboardButton("📜 История", callback_data="crypto_history"),
        ],
    ])


def dashboard_message_menu():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton('👤 Профиль', callback_data='profile'),
            InlineKeyboardButton('📊 Статистика', callback_data='profile_stats'),
        ],
        [
            InlineKeyboardButton('🪙 Криптовалюта', callback_data='crypto_menu'),
            InlineKeyboardButton('👏 Ежедневный EXP', callback_data='daily_exp'),
        ],
        [
            InlineKeyboardButton('🏰 Кланы', callback_data='clans'),
            InlineKeyboardButton('💸 Вывести', callback_data='withdraw'),
        ],
        [
            InlineKeyboardButton('👥 Группа', url='https://t.me/bezdnao'),
        ],
    ])

# ===== END_FINAL_CRYPTO_BUTTON_EMOJI_FIX =====

if __name__ == '__main__':
    main()
