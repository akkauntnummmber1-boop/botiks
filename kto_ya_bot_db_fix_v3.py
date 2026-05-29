import asyncio
import html
import os
import random
import sqlite3
import time
import io
import string

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message, BufferedInputFile
from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
MAIN_ADMIN = 6991875
MAIN_ADMINS = {6991875, 5037478748}
WITHDRAW_LOG_CHAT_ID = -1003992200445

if not BOT_TOKEN:
    raise RuntimeError("Не найден BOT_TOKEN в файле .env")

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

db = sqlite3.connect("bot.db")
cur = db.cursor()

withdraw_state = {}
bet_state = {}
promo_state = {}
transfer_state = {}
last_game_state = {}


RARITIES = [
    ("Пепельная", 55, 1),
    ("Кровавая", 35, 3),
    ("Проклятая", 20, 5),
    ("Адская", 8, 7),
    ("Бездна", 5, 10),
]

TRIGGERS = {"кто", "я", "me", "кто я", "ya"}



P_EMOJI = {
    "profile": "5260399854500191689",
    "play": "5258508428212445001",
    "top": "5199574192646797590",
    "promo": "5264710902153767489",
    "transfer": "5472030678633684592",
    "privacy": "5787313834012184077",
    "info": "5258503720928288433",
    "roles": "5276239041052828276",
    "rarity": "5226858719718953055",
    "bag": "5445221832074483553",
    "withdraw": "5258204546391351475",
    "back": "5258236805890710909",
    "bet": "5193052409361871390",
    "commands": "5258328383183396223",
    "repeat": "5258420634785947640",
    "cross": "5258318620722733379",
    "check": "5260416304224936047",
    "hide": "5467370583282950466",
    "hello": "5193197184119487084",
    "game_text": "5361741454685256344",
    "comment": "5260535596941582167",
    "cash": "5409048419211682843",
    "money": "5258204546391351475",
    "dollar": "5845761381063727410",
    "opened": "5280826864988873394",
    "played": "5287606810168028257",
    "account": "5285439518130857782",
    "mail": "5285184156555306745",
    "clock": "5258258882022612173",
    "captcha": "5257974976094412956",
    "football": "5258169263235013408",
    "basketball": "5384088040677319401",
    "bowling": "5370853837689070338",
    "cube": "5404728536810398694",
}

def pe(key: str, fallback: str) -> str:
    return f'<tg-emoji emoji-id="{P_EMOJI[key]}">{fallback}</tg-emoji>'

def dollar() -> str:
    return pe("dollar", "💲")

def choose_rarity():
    return random.choices(RARITIES, weights=[x[1] for x in RARITIES], k=1)[0]


def reward_by_rarity(rarity_name: str) -> int:
    for rarity, _, money in RARITIES:
        if rarity == rarity_name:
            return money
    return 1


def init_db():
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid INTEGER UNIQUE,
        tg_id INTEGER UNIQUE,
        username TEXT,
        balance REAL DEFAULT 0,
        roles_opened INTEGER DEFAULT 0,
        games INTEGER DEFAULT 0,
        created_at INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS admins(
        tg_id INTEGER PRIMARY KEY
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS bans(
        tg_id INTEGER PRIMARY KEY,
        until INTEGER,
        reason TEXT,
        warned INTEGER DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS roles(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        rarity TEXT,
        reward REAL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_roles(
        user_tg_id INTEGER,
        role_name TEXT,
        rarity TEXT,
        count INTEGER DEFAULT 1,
        PRIMARY KEY(user_tg_id, role_name)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS withdraws(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_tg_id INTEGER,
        wallet TEXT,
        amount REAL,
        source TEXT,
        status TEXT DEFAULT 'pending',
        created_at INTEGER
    )
    """)


    cur.execute("""
    CREATE TABLE IF NOT EXISTS promos(
        code TEXT PRIMARY KEY,
        amount REAL,
        max_uses INTEGER,
        uses INTEGER DEFAULT 0,
        active INTEGER DEFAULT 1,
        created_at INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS promo_uses(
        promo_code TEXT,
        user_tg_id INTEGER,
        activated_at INTEGER,
        PRIMARY KEY(promo_code, user_tg_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS bot_groups(
        chat_id INTEGER PRIMARY KEY,
        title TEXT,
        chat_type TEXT,
        updated_at INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS settings(
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)

    cur.execute("INSERT OR IGNORE INTO settings(key, value) VALUES('techper', '0')")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS greeting_messages(
        user_tg_id INTEGER PRIMARY KEY,
        chat_id INTEGER,
        message_id INTEGER
    )
    """)

    cur.execute("INSERT OR IGNORE INTO admins(tg_id) VALUES(?)", (MAIN_ADMIN,))
    cur.execute("INSERT OR IGNORE INTO admins(tg_id) VALUES(?)", (5037478748,))

    cur.execute("PRAGMA table_info(users)")
    user_columns = [x[1] for x in cur.fetchall()]
    if "show_in_top" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN show_in_top INTEGER DEFAULT 1")
    if "show_username_top" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN show_username_top INTEGER DEFAULT 1")
    if "show_uid_top" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN show_uid_top INTEGER DEFAULT 1")
    if "current_bet" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN current_bet REAL DEFAULT 2")
    if "last_role_time" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN last_role_time INTEGER DEFAULT 0")

    cur.execute("PRAGMA table_info(roles)")
    role_columns = [x[1] for x in cur.fetchall()]
    if "rarity" not in role_columns:
        cur.execute("ALTER TABLE roles ADD COLUMN rarity TEXT")
    if "reward" not in role_columns:
        cur.execute("ALTER TABLE roles ADD COLUMN reward REAL")

    cur.execute("SELECT id FROM roles WHERE rarity IS NULL OR reward IS NULL")
    old_roles = cur.fetchall()
    for (role_id,) in old_roles:
        rarity, _, reward = choose_rarity()
        cur.execute("UPDATE roles SET rarity=?, reward=? WHERE id=?", (rarity, reward, role_id))

    db.commit()


init_db()


def user_mention(user) -> str:
    name = user.full_name or user.username or str(user.id)
    return f'<a href="tg://user?id={user.id}">{html.escape(name)}</a>'


def fmt_money(value) -> str:
    try:
        value = float(value)
        if value.is_integer():
            return f"{int(value):,}".replace(",", " ")

        text = f"{value:,.3f}"
        text = text.rstrip("0").rstrip(".")
        text = text.replace(",", " ").replace(".", ",")
        return text
    except Exception:
        return str(value)




def promo_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [make_button("Назад", callback_data="main_menu", emoji_fallback="⬅️")]
    ])

def captcha_text(length: int = 5) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(random.choice(alphabet) for _ in range(length))


def make_captcha_image(text: str) -> bytes:
    width, height = 220, 90
    image = Image.new("RGB", (width, height), (245, 245, 245))
    draw = ImageDraw.Draw(image)

    try:
        font = ImageFont.truetype("arial.ttf", 38)
    except Exception:
        font = ImageFont.load_default()

    for _ in range(18):
        x1 = random.randint(0, width)
        y1 = random.randint(0, height)
        x2 = random.randint(0, width)
        y2 = random.randint(0, height)
        draw.line((x1, y1, x2, y2), fill=(random.randint(80, 180), random.randint(80, 180), random.randint(80, 180)), width=1)

    bbox = draw.textbbox((0, 0), text, font=font)
    text_w = bbox[2] - bbox[0]
    text_h = bbox[3] - bbox[1]
    draw.text(((width - text_w) / 2, (height - text_h) / 2 - 3), text, fill=(20, 20, 20), font=font)

    for _ in range(300):
        x = random.randint(0, width - 1)
        y = random.randint(0, height - 1)
        image.putpixel((x, y), (random.randint(0, 220), random.randint(0, 220), random.randint(0, 220)))

    output = io.BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


def is_admin(tg_id: int) -> bool:
    if tg_id in MAIN_ADMINS:
        return True
    cur.execute("SELECT 1 FROM admins WHERE tg_id=?", (tg_id,))
    return cur.fetchone() is not None


def get_user(tg_id: int, username: str | None):
    cur.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,))
    user = cur.fetchone()

    if user is None:
        cur.execute("SELECT COALESCE(MAX(uid), 0) + 1 FROM users")
        uid = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO users(uid, tg_id, username, created_at, current_bet) VALUES(?,?,?,?,?)",
            (uid, tg_id, username or "none", int(time.time()), 2)
        )
        db.commit()
        cur.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,))
        user = cur.fetchone()
    else:
        cur.execute("UPDATE users SET username=? WHERE tg_id=?", (username or "none", tg_id))
        db.commit()

    return user


def user_balance(tg_id: int) -> float:
    cur.execute("SELECT balance FROM users WHERE tg_id=?", (tg_id,))
    row = cur.fetchone()
    return float(row[0]) if row else 0


def user_current_bet(tg_id: int) -> float:
    cur.execute("SELECT current_bet FROM users WHERE tg_id=?", (tg_id,))
    row = cur.fetchone()
    if not row or row[0] is None:
        return 2
    return float(row[0])


def set_current_bet(tg_id: int, amount: float):
    cur.execute("UPDATE users SET current_bet=? WHERE tg_id=?", (amount, tg_id))
    db.commit()




def is_main_admin(tg_id: int) -> bool:
    return tg_id in MAIN_ADMINS


def is_techper_enabled() -> bool:
    cur.execute("SELECT value FROM settings WHERE key='techper'")
    row = cur.fetchone()
    return row is not None and row[0] == "1"


def set_techper(value: bool):
    cur.execute("REPLACE INTO settings(key, value) VALUES('techper', ?)", ("1" if value else "0",))
    db.commit()


def save_group(chat):
    if chat.type not in ("group", "supergroup"):
        return

    cur.execute(
        "REPLACE INTO bot_groups(chat_id, title, chat_type, updated_at) VALUES(?,?,?,?)",
        (chat.id, chat.title or "Без названия", chat.type, int(time.time()))
    )
    db.commit()


def get_uid(tg_id: int):
    cur.execute("SELECT uid FROM users WHERE tg_id=?", (tg_id,))
    row = cur.fetchone()
    return row[0] if row else "none"


def user_info_text_by_row(row) -> str:
    if not row:
        return "❌ Пользователь не найден."

    uid = row[1]
    tg_id = row[2]
    username = row[3]
    balance = row[4]
    roles_opened = row[5]
    games = row[6]
    created_at = row[7] or int(time.time())
    days = max(0, (int(time.time()) - created_at) // 86400)

    cur.execute("SELECT COUNT(*) FROM user_roles WHERE user_tg_id=?", (tg_id,))
    roles_count_row = cur.fetchone()
    unique_roles = roles_count_row[0] if roles_count_row else 0

    promo_count = 0
    try:
        cur.execute("SELECT COUNT(*) FROM promo_uses WHERE user_tg_id=?", (tg_id,))
        promo_row = cur.fetchone()
        promo_count = promo_row[0] if promo_row else 0
    except Exception:
        promo_count = 0

    return (
        "👤 <b>Информация о пользователе</b>\n\n"
        f"<b>UID:</b> {uid}\n"
        f"<b>ID:</b> <code>{tg_id}</code>\n"
        f"<b>Username:</b> @{html.escape(username or 'none')}\n"
        f"💵 <b>Баланс:</b> {fmt_money(balance)}💲 \n"
        f"📰 <b>Открыто ролей:</b> {roles_opened}\n"
        f"🎮 <b>Сыграно:</b> {games}\n"
        f"🎭 <b>Уникальных ролей:</b> {unique_roles}\n"
        f"🎁 <b>Промокодов:</b> {promo_count}\n"
        f"⏱ <b>Аккаунту:</b> {days} дней"
    )


async def techper_guard(message: Message) -> bool:
    if is_techper_enabled() and not is_admin(message.from_user.id):
        await message.answer("☝️ <b>Бот пока что не работает</b>")
        return True
    return False


def check_ban(tg_id: int):
    cur.execute("SELECT until, reason, warned FROM bans WHERE tg_id=?", (tg_id,))
    ban = cur.fetchone()

    if not ban:
        return None

    until, reason, warned = ban

    if until != 0 and until < int(time.time()):
        cur.execute("DELETE FROM bans WHERE tg_id=?", (tg_id,))
        db.commit()
        return None

    return ban


async def ban_handler(message: Message) -> bool:
    ban = check_ban(message.from_user.id)

    if not ban:
        return False

    until, reason, warned = ban

    if message.chat.type == "private" and warned == 0:
        await message.answer(f"⛔ Вы заблокированы.\nПричина: {html.escape(reason)}")
        cur.execute("UPDATE bans SET warned=1 WHERE tg_id=?", (message.from_user.id,))
        db.commit()

    return True


def parse_time(value: str) -> int:
    value = value.lower().strip()
    if value.endswith("m"):
        return int(value[:-1]) * 60
    if value.endswith("h"):
        return int(value[:-1]) * 3600
    if value.endswith("d"):
        return int(value[:-1]) * 86400
    return int(value)


async def delete_greeting(user_id: int):
    return


def start_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👤 Профиль", callback_data="profile"),
            InlineKeyboardButton(text="🎮 Играть", callback_data="play_menu")
        ],
        [
            InlineKeyboardButton(text="🏆 Топ игроков", callback_data="top_players"),
            InlineKeyboardButton(text="🎁 Промокоды", callback_data="promo_start")
        ],
        [
            InlineKeyboardButton(text="💸 Передать деньги", callback_data="transfer_money"),
            InlineKeyboardButton(text="🔐 Приватность", callback_data="privacy")
        ],
        [
            InlineKeyboardButton(text="ℹ️ Подробнее о боте", callback_data="about_bot")
        ]
    ])

def is_group_chat(message_or_callback) -> bool:
    chat = message_or_callback.chat if hasattr(message_or_callback, "chat") else message_or_callback.message.chat
    return chat.type in ("group", "supergroup")


def menu_markup_for_chat(message: Message):
    if message.chat.type in ("group", "supergroup"):
        return None
    return main_menu_keyboard()


def main_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
    ])

def withdraw_cancel_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить", callback_data="withdraw_cancel")]
    ])

def about_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
    ])

def privacy_keyboard(user_id: int):
    cur.execute(
        "SELECT show_in_top, show_username_top, show_uid_top FROM users WHERE tg_id=?",
        (user_id,)
    )
    row = cur.fetchone() or (1, 1, 1)
    show_in_top, show_username_top, show_uid_top = row

    top_text = "✅ Показ в топе" if show_in_top else "❌ Показ в топе"
    username_text = "✅ Юзернейм в топе" if show_username_top else "❌ Юзернейм в топе"
    uid_text = "✅ UID в топе" if show_uid_top else "❌ UID в топе"

    return InlineKeyboardMarkup(inline_keyboard=[
        [make_button(top_text, callback_data="privacy_toggle:top", emoji_fallback="🏆")],
        [make_button(username_text, callback_data="privacy_toggle:username", emoji_fallback="👤")],
        [make_button(uid_text, callback_data="privacy_toggle:uid", emoji_fallback="ℹ️")],
        [make_button("Полностью скрыться", callback_data="privacy_hide_all", emoji_fallback="🙈")],
        [make_button("Сохранить", callback_data="privacy_save", emoji_fallback="✅")]
    ])


def privacy_text():
    return (
        f"{pe('privacy', '🔐')} <b>Приватность</b> :\n\n"
        "- Показ в <b>топе</b>.\n"
        "- Отображать <b>юзернейм</b> в топе\n"
        "- Отображать <b>UID в топе</b>\n"
        "- Полностью <b>скрыться</b>"
    )

def profile_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎭 Твои роли", callback_data="my_roles")],
        [InlineKeyboardButton(text="💰 Вывод", callback_data="withdraw")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
    ])

def roles_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Вернуться", callback_data="profile")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
    ])

def play_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Изменить ставку", callback_data="change_bet")],
        [InlineKeyboardButton(text="📖 Команды", callback_data="game_commands")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
    ])

def casino_back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить", callback_data="casino_cancel")]
    ])

def repeat_game_keyboard(owner_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Повторить ставку", callback_data=f"repeat_game:{owner_id}")]
    ])

def withdraw_review_keyboard(wid: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Одобрить", callback_data=f"withdraw_ok:{wid}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"withdraw_no:{wid}")
        ]
    ])

def roles_summary_text(tg_id: int) -> str:
    lines = [f"{pe('roles', '🎭')} <b>Твои роли</b>\n"]

    for rarity, _, _ in RARITIES:
        cur.execute("SELECT COUNT(*) FROM user_roles WHERE user_tg_id=? AND rarity=?", (tg_id, rarity))
        count = cur.fetchone()[0]
        lines.append(f"<b>{html.escape(rarity)}</b> - {count}")

    return "\n".join(lines)

def top_players_text() -> str:
    cur.execute(
        "SELECT uid, username, balance, show_username_top, show_uid_top FROM users "
        "WHERE COALESCE(show_in_top, 1)=1 "
        "ORDER BY balance DESC, uid ASC LIMIT 10"
    )
    rows = cur.fetchall()

    if not rows:
        return f"{pe('top', '🏆')} <b>Топ игроков</b>\n\nПока нет игроков."

    lines = [f"{pe('top', '🏆')} <b>Топ игроков</b>\n"]

    for uid, username, balance, show_username_top, show_uid_top in rows:
        uid_text = f"UID {uid}" if show_uid_top else "Неизвестно"
        username_text = f"@{html.escape(username)}" if username and username != "none" and show_username_top else "Неизвестно"
        lines.append(f"{uid_text} | {username_text} | {fmt_money(balance)} {pe('money', '💰')}")

    return "\n".join(lines)

def top_back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
    ])

def welcome_text():
    return (
        f"{pe('hello', '👋')} <b>Добро пожаловать</b> в @Ktoyaro_bot\n\n"
        f"{pe('game_text', '🎮')} Получай <b>роли</b>, копи <b>виртуальную валюту</b> и выводи их.\n"
        f"{pe('comment', '💬')} <b>Новости и конкурсы</b> — @bezdnao"
    )

def play_menu_text(tg_id: int):
    return (
        f"{pe('game_text', '🎮')} Выбирайте <b>игру</b> или <b>режим</b>!\n\n"
        f"<b>Баланс</b> — {fmt_money(user_balance(tg_id))} {pe('money', '💰')}\n"
        f"<b>Ставка</b> — {fmt_money(user_current_bet(tg_id))} {pe('money', '💰')}"
    )

def commands_text():
    return '📘 <a href="https://telegra.ph/Ktoyaro-bot-05-25">Ссылка на статью</a>'


async def send_main_menu(message: Message):
    get_user(message.from_user.id, message.from_user.username)
    withdraw_state.pop(message.from_user.id, None)
    bet_state.pop(message.from_user.id, None)
    promo_state.pop(message.from_user.id, None)

    greeting = await message.answer_sticker("CAACAgIAAxkBAAEELI1qFGNeUd5CwE0n_oXE9isNFaQCvwACwAgAAgi3GQLlT2zOhLW5xzsE")

    cur.execute(
        "REPLACE INTO greeting_messages(user_tg_id, chat_id, message_id) VALUES(?,?,?)",
        (message.from_user.id, message.chat.id, greeting.message_id)
    )
    db.commit()

    await message.answer(welcome_text(), reply_markup=start_keyboard())


@dp.message(Command("start"))
async def start(message: Message):
    save_group(message.chat)

    if await ban_handler(message):
        return

    if await techper_guard(message):
        return

    await send_main_menu(message)


@dp.callback_query(F.data == "main_menu")
async def main_menu_inline(callback: CallbackQuery):
    if callback.message.chat.type in ("group", "supergroup"):
        await callback.answer()
        return
    await delete_greeting(callback.from_user.id)
    withdraw_state.pop(callback.from_user.id, None)
    bet_state.pop(callback.from_user.id, None)
    promo_state.pop(callback.from_user.id, None)
    transfer_state.pop(callback.from_user.id, None)

    try:
        await callback.message.edit_text(welcome_text(), reply_markup=start_keyboard())
    except Exception:
        await callback.message.answer(welcome_text(), reply_markup=start_keyboard())

    await callback.answer()





@dp.callback_query(F.data == "transfer_money")
async def transfer_money(callback: CallbackQuery):
    await delete_greeting(callback.from_user.id)
    get_user(callback.from_user.id, callback.from_user.username)

    transfer_state[callback.from_user.id] = {
        "step": "target"
    }

    await callback.message.edit_text(
        "💵 <b>Введите</b> <b>USERNAME/ID</b> <b>человека</b> кому вы хотите <b>передать</b> деньги."
    )
    await callback.answer()


@dp.callback_query(F.data == "about_bot")
async def about_bot(callback: CallbackQuery):
    await delete_greeting(callback.from_user.id)
    await callback.message.edit_text(
        "ℹ️ <a href=\"https://telegra.ph/Ktoyaro-bot-05-25-2\">Ссылка на статью</a>",
        reply_markup=about_keyboard(),
        disable_web_page_preview=True
    )
    await callback.answer()


@dp.callback_query(F.data == "privacy")
async def privacy(callback: CallbackQuery):
    await delete_greeting(callback.from_user.id)
    get_user(callback.from_user.id, callback.from_user.username)
    await callback.message.edit_text(
        privacy_text(),
        reply_markup=privacy_keyboard(callback.from_user.id)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("privacy_toggle:"))
async def privacy_toggle(callback: CallbackQuery):
    get_user(callback.from_user.id, callback.from_user.username)
    field = callback.data.split(":")[1]

    column_map = {
        "top": "show_in_top",
        "username": "show_username_top",
        "uid": "show_uid_top"
    }

    column = column_map.get(field)
    if not column:
        await callback.answer()
        return

    cur.execute(f"SELECT {column} FROM users WHERE tg_id=?", (callback.from_user.id,))
    current = cur.fetchone()[0]
    new_value = 0 if current else 1

    cur.execute(f"UPDATE users SET {column}=? WHERE tg_id=?", (new_value, callback.from_user.id))
    db.commit()

    await callback.message.edit_text(
        privacy_text(),
        reply_markup=privacy_keyboard(callback.from_user.id)
    )
    await callback.answer()


@dp.callback_query(F.data == "privacy_hide_all")
async def privacy_hide_all(callback: CallbackQuery):
    get_user(callback.from_user.id, callback.from_user.username)
    cur.execute(
        "UPDATE users SET show_in_top=0, show_username_top=0, show_uid_top=0 WHERE tg_id=?",
        (callback.from_user.id,)
    )
    db.commit()

    await callback.message.edit_text(
        privacy_text(),
        reply_markup=privacy_keyboard(callback.from_user.id)
    )
    await callback.answer("Скрыто")


@dp.callback_query(F.data == "privacy_save")
async def privacy_save(callback: CallbackQuery):
    await callback.answer("✅ Сохранено", show_alert=True)


@dp.callback_query(F.data == "top_players")
async def top_players(callback: CallbackQuery):
    await delete_greeting(callback.from_user.id)
    await callback.message.edit_text(top_players_text(), reply_markup=top_back_keyboard())
    await callback.answer()



@dp.callback_query(F.data == "promo_start")
async def promo_start(callback: CallbackQuery):
    await delete_greeting(callback.from_user.id)
    get_user(callback.from_user.id, callback.from_user.username)

    promo_state[callback.from_user.id] = {"step": "promo_code"}

    await callback.message.edit_text(
        "🎁 <b>Введите промокод</b> одним <b>сообщением</b>:"
    )
    await callback.answer()


@dp.callback_query(F.data == "play_menu")
async def play_menu(callback: CallbackQuery):
    if callback.message.chat.type in ("group", "supergroup"):
        await callback.answer()
        return
    await delete_greeting(callback.from_user.id)
    get_user(callback.from_user.id, callback.from_user.username)
    await callback.message.edit_text(play_menu_text(callback.from_user.id), reply_markup=play_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "change_bet")
async def change_bet(callback: CallbackQuery):
    if is_techper_enabled() and not is_admin(callback.from_user.id):
        await callback.answer("Бот пока что не работает", show_alert=True)
        return

    await delete_greeting(callback.from_user.id)
    bet_state[callback.from_user.id] = True
    await callback.message.edit_text(
        f"{pe('money', '💰')} Чтобы изменить ставку напишите в чат новую сумму.\n"
        f"Например: <code>Ставка 911{dollar()}</code>",
        reply_markup=casino_back_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data == "casino_cancel")
async def casino_cancel(callback: CallbackQuery):
    bet_state.pop(callback.from_user.id, None)
    await callback.message.edit_text(f"{pe('cross', '❌')} <b>Отменено</b>")
    await callback.answer()

@dp.callback_query(F.data == "game_commands")
async def game_commands(callback: CallbackQuery):
    if callback.message.chat.type in ("group", "supergroup"):
        await callback.answer()
        return
    await delete_greeting(callback.from_user.id)
    await callback.message.edit_text(commands_text(), reply_markup=casino_back_keyboard(), disable_web_page_preview=True)
    await callback.answer()


@dp.callback_query(F.data == "profile")
async def profile(callback: CallbackQuery):
    if is_techper_enabled() and not is_admin(callback.from_user.id):
        await callback.answer("Бот пока что не работает", show_alert=True)
        return

    await delete_greeting(callback.from_user.id)

    user = get_user(callback.from_user.id, callback.from_user.username)
    created_at = user[7] or int(time.time())
    days = max(0, (int(time.time()) - created_at) // 86400)

    text = (
        f"<b>UID:</b> {user[1]} @{callback.from_user.username or 'none'}\n\n"
        f"{pe('cash', '💵')} <b>Баланс</b> — {fmt_money(user[4])} {pe('cash', '💵')}\n\n"
        f"{pe('opened', '💝')} <b>Открыто ролей</b> — {user[5]}\n"
        f"{pe('played', '🗓')} <b>Сыграно</b> — {user[6]} ставок\n"
        f"{pe('account', '❤️')} <b>Аккаунту</b> — {days} дней"
    )

    await callback.message.edit_text(text, reply_markup=profile_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "my_roles")
async def my_roles(callback: CallbackQuery):
    if callback.message.chat.type in ("group", "supergroup"):
        await callback.answer()
        return
    await delete_greeting(callback.from_user.id)
    await callback.message.edit_text(roles_summary_text(callback.from_user.id), reply_markup=roles_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "withdraw")
async def withdraw(callback: CallbackQuery):
    if callback.message.chat.type in ("group", "supergroup"):
        await callback.answer()
        return
    await delete_greeting(callback.from_user.id)

    user = get_user(callback.from_user.id, callback.from_user.username)

    if user[4] < 500:
        await callback.answer("Минимальная сумма вывода — 500💲 ", show_alert=True)
        return

    withdraw_state[callback.from_user.id] = {"step": "wallet"}

    await callback.message.edit_text(
        "💵 <b>Вывод средств</b>\n\n"
        "Памятка:\n"
        "500💲 - 20💲 \n"
        "1000💲 - 40💲 \n\n"
        "<b>Отправьте кошелёк для вывода.</b>",
        reply_markup=withdraw_cancel_keyboard()
    )
    await callback.answer()



@dp.callback_query(F.data == "withdraw_cancel")
async def withdraw_cancel(callback: CallbackQuery):
    withdraw_state.pop(callback.from_user.id, None)
    await callback.message.edit_text("<b>❌ Отменено</b>")
    await callback.answer()


@dp.message(Command("admin"))
async def admin(message: Message):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    await message.answer(
        "Админские команды:\n\n"
        "/addadmin ID — добавить админа\n"
        "/admindelete ID — снять админа. Только главные админы\n"
        "/adminlist — список администраторов\n"
        "/ban ID время причина — забанить пользователя\n"
        "/banlist — список забаненных пользователей\n"
        "/unban ID — разбанить пользователя\n"
        "/give ID сумма — выдать деньги пользователю\n"
        "/take ID сумма — снять деньги у пользователя\n"
        "/clearmoney — очистить деньги у всех пользователей\n"
        "/allusers — количество зарегистрированных пользователей\n"
        "/allgroups — список групп, где существует бот\n"
        "/uidfounder UID — поиск пользователя по UID\n"
        "/idfounder ID — поиск пользователя по Telegram ID\n"
        "/idfouder ID — поиск пользователя по Telegram ID\n"
        "/broadcast текст — отправить сообщение всем пользователям\n"
        "/setuid USER_ID UID — выдать кастомный UID\n\n"
        "Роли/фразы:\n"
        "/add роль — добавить одну роль\n"
        "/deletefraz фраза — удалить роль/фразу\n"
        "/resetfraz — удалить все роли/фразы\n\n"
        "Промокоды:\n"
        "/addpromo название сумма активации — создать промокод\n"
        "Пример: <code>/addpromo TEST 100 5</code>\n"
        "/promolist — показать все промокоды и их статус\n"
        "/delpromo промокод — удалить промокод\n"
        "Пример: <code>/delpromo TEST</code>\n\n"
        "Техперерыв:\n"
        "/techper — включить техперерыв\n"
        "/untechper — выключить техперерыв\n\n"
        "TXT файл — каждая строка добавляется как роль.\n"
        "Админ-команды работают только в ЛС.\n"
        "Время бана: 60, 10m, 2h, 7d"
    )



@dp.message(Command("banlist"))
async def banlist(message: Message):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    cur.execute("SELECT tg_id, until, reason FROM bans ORDER BY tg_id")
    rows = cur.fetchall()

    if not rows:
        await message.answer("✅ Забаненных пользователей нет.")
        return

    now = int(time.time())
    lines = ["⛔ <b>Забаненные пользователи</b>\n"]

    for tg_id, until, reason in rows:
        if until == 0:
            time_text = "навсегда"
        else:
            left = max(0, until - now)
            time_text = f"{left // 3600} ч. {(left % 3600) // 60} мин."
        lines.append(f"<code>{tg_id}</code> — {time_text} — {html.escape(reason or '')}")

    await message.answer("\n".join(lines))


@dp.message(Command("adminlist"))
async def adminlist(message: Message):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    cur.execute("SELECT tg_id FROM admins ORDER BY tg_id")
    rows = [x[0] for x in cur.fetchall()]

    lines = ["👮 <b>Администраторы</b>\n"]

    for admin_id in rows:
        status = "главный админ" if admin_id in MAIN_ADMINS else "админ"
        lines.append(f"<code>{admin_id}</code> — {status}")

    await message.answer("\n".join(lines))


@dp.message(Command("clearmoney"))
async def clearmoney(message: Message):
    if message.chat.type != "private":
        return
    if not is_main_admin(message.from_user.id):
        return

    cur.execute("UPDATE users SET balance=0")
    db.commit()

    await message.answer("✅ Деньги очищены у всех пользователей.")


@dp.message(Command("allgroups"))
async def allgroups(message: Message):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    cur.execute("SELECT chat_id, title, chat_type FROM bot_groups ORDER BY updated_at DESC")
    rows = cur.fetchall()

    if not rows:
        await message.answer("Групп пока нет.")
        return

    lines = [f"👥 <b>Группы бота:</b> {len(rows)}\n"]

    for chat_id, title, chat_type in rows[:80]:
        lines.append(f"{html.escape(title or 'Без названия')} — <code>{chat_id}</code> — {chat_type}")

    await message.answer("\n".join(lines))


@dp.message(Command("allusers"))
async def allusers(message: Message):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    cur.execute("SELECT COUNT(*) FROM users")
    count = cur.fetchone()[0]

    await message.answer(f"👤 Всего зарегистрированных в боте: <b>{count}</b>")


@dp.message(Command("uidfounder"))
async def uidfounder(message: Message):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    args = message.text.split()

    if len(args) < 2:
        await message.answer("Использование: /uidfounder UID")
        return

    cur.execute("SELECT * FROM users WHERE uid=?", (int(args[1]),))
    row = cur.fetchone()

    await message.answer(user_info_text_by_row(row))


@dp.message(Command("idfounder", "idfouder"))
async def idfounder(message: Message):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    args = message.text.split()

    if len(args) < 2:
        await message.answer("Использование: /idfounder ID")
        return

    cur.execute("SELECT * FROM users WHERE tg_id=?", (int(args[1]),))
    row = cur.fetchone()

    await message.answer(user_info_text_by_row(row))


@dp.message(Command("broadcast"))
async def broadcast(message: Message):
    if message.chat.type != "private":
        return
    if not is_main_admin(message.from_user.id):
        return

    text = message.text.replace("/broadcast", "", 1).strip()

    if not text:
        await message.answer("Использование: /broadcast текст")
        return

    cur.execute("SELECT tg_id FROM users")
    user_ids = [x[0] for x in cur.fetchall()]

    sent = 0
    failed = 0

    for user_id in user_ids:
        try:
            await bot.send_message(user_id, text)
            sent += 1
            await asyncio.sleep(0.03)
        except Exception:
            failed += 1

    await message.answer(f"✅ Рассылка завершена.\nОтправлено: {sent}\nОшибок: {failed}")


@dp.message(Command("resetfraz"))
async def resetfraz(message: Message):
    if message.chat.type != "private":
        return
    if not is_main_admin(message.from_user.id):
        return

    cur.execute("DELETE FROM roles")
    cur.execute("DELETE FROM user_roles")
    db.commit()

    await message.answer("✅ Все роли/фразы удалены.")


@dp.message(Command("setuid"))
async def setuid(message: Message):
    if message.chat.type != "private":
        return
    if not is_main_admin(message.from_user.id):
        return

    args = message.text.split()

    if len(args) < 3:
        await message.answer("Использование: /setuid USER_ID UID")
        return

    user_id = int(args[1])
    new_uid = int(args[2])

    get_user(user_id, "none")

    try:
        cur.execute("UPDATE users SET uid=? WHERE tg_id=?", (new_uid, user_id))
        db.commit()
        await message.answer(f"✅ Пользователю <code>{user_id}</code> установлен UID <b>{new_uid}</b>.")
    except sqlite3.IntegrityError:
        await message.answer("❌ Такой UID уже занят.")


@dp.message(Command("deletefraz"))
async def deletefraz(message: Message):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    phrase = message.text.replace("/deletefraz", "", 1).strip()

    if not phrase:
        await message.answer("Использование: /deletefraz фраза")
        return

    cur.execute("DELETE FROM roles WHERE name=?", (phrase,))
    cur.execute("DELETE FROM user_roles WHERE role_name=?", (phrase,))
    db.commit()

    await message.answer(f"✅ Фраза удалена: {html.escape(phrase)}")


@dp.message(Command("techper"))
async def techper(message: Message):
    if message.chat.type != "private":
        return
    if not is_main_admin(message.from_user.id):
        return

    set_techper(True)
    await message.answer("✅ Техперерыв включён.")


@dp.message(Command("untechper"))
async def untechper(message: Message):
    if message.chat.type != "private":
        return
    if not is_main_admin(message.from_user.id):
        return

    set_techper(False)
    await message.answer("✅ Техперерыв выключен.")


@dp.message(Command("addpromo"))
async def add_promo(message: Message):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    args = message.text.split()
    if len(args) < 4:
        await message.answer("Использование: /addpromo название сумма активации", reply_markup=main_menu_keyboard())
        return

    code_name = args[1].strip().upper()
    try:
        amount = float(args[2].replace(",", "."))
        max_uses = int(args[3])
    except Exception:
        await message.answer("Сумма и активации должны быть числами.", reply_markup=main_menu_keyboard())
        return

    if amount <= 0 or max_uses <= 0:
        await message.answer("Сумма и активации должны быть больше 0.", reply_markup=main_menu_keyboard())
        return

    cur.execute(
        "REPLACE INTO promos(code, amount, max_uses, uses, active, created_at) VALUES(?,?,?,?,1,?)",
        (code_name, amount, max_uses, 0, int(time.time()))
    )
    db.commit()

    await message.answer(
        f"✅ Промокод создан: <code>{html.escape(code_name)}</code>\n"
        f"Сумма: {fmt_money(amount)}💲 \n"
        f"Активаций: {max_uses}",
        reply_markup=main_menu_keyboard()
    )


@dp.message(Command("promolist"))
async def promo_list(message: Message):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    cur.execute("SELECT code, amount, max_uses, uses, active FROM promos ORDER BY created_at DESC")
    rows = cur.fetchall()

    if not rows:
        await message.answer("Промокодов пока нет.", reply_markup=main_menu_keyboard())
        return

    lines = ["🎁 <b>Промокоды</b>\n"]
    for code_name, amount, max_uses, uses, active in rows:
        status = "активно" if active and uses < max_uses else "не активно"
        lines.append(f"<code>{html.escape(code_name)}</code> — {fmt_money(amount)}💲 — {uses}/{max_uses} — {status}")

    await message.answer("\n".join(lines), reply_markup=main_menu_keyboard())


@dp.message(Command("delpromo"))
async def del_promo(message: Message):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /delpromo промокод", reply_markup=main_menu_keyboard())
        return

    code_name = args[1].strip().upper()
    cur.execute("DELETE FROM promos WHERE code=?", (code_name,))
    cur.execute("DELETE FROM promo_uses WHERE promo_code=?", (code_name,))
    db.commit()

    await message.answer(f"✅ Промокод удалён: <code>{html.escape(code_name)}</code>", reply_markup=main_menu_keyboard())


@dp.message(Command("addadmin"))
async def add_admin(message: Message):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /addadmin ID", reply_markup=menu_markup_for_chat(message))
        return

    cur.execute("INSERT OR IGNORE INTO admins(tg_id) VALUES(?)", (int(args[1]),))
    db.commit()
    await message.answer("✅ Админ добавлен.", reply_markup=menu_markup_for_chat(message))


@dp.message(Command("admindelete"))
async def admin_delete(message: Message):
    if message.chat.type != "private":
        return
    if not is_main_admin(message.from_user.id):
        return

    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /admindelete ID", reply_markup=menu_markup_for_chat(message))
        return

    admin_id = int(args[1])
    if admin_id in MAIN_ADMINS:
        await message.answer("Главного админа нельзя снять.", reply_markup=menu_markup_for_chat(message))
        return

    cur.execute("DELETE FROM admins WHERE tg_id=?", (admin_id,))
    db.commit()
    await message.answer("✅ Админ снят.", reply_markup=menu_markup_for_chat(message))


@dp.message(Command("ban"))
async def ban(message: Message):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    args = message.text.split(maxsplit=3)
    if len(args) < 4:
        await message.answer("Использование: /ban ID время причина", reply_markup=menu_markup_for_chat(message))
        return

    user_id = int(args[1])
    seconds = parse_time(args[2])
    reason = args[3]
    until = int(time.time()) + seconds if seconds > 0 else 0

    cur.execute("REPLACE INTO bans(tg_id, until, reason, warned) VALUES(?,?,?,0)", (user_id, until, reason))
    db.commit()
    await message.answer("⛔ Пользователь забанен.", reply_markup=menu_markup_for_chat(message))


@dp.message(Command("unban"))
async def unban(message: Message):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /unban ID", reply_markup=menu_markup_for_chat(message))
        return

    cur.execute("DELETE FROM bans WHERE tg_id=?", (int(args[1]),))
    db.commit()
    await message.answer("✅ Пользователь разбанен.", reply_markup=menu_markup_for_chat(message))


@dp.message(Command("give"))
async def give(message: Message):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    args = message.text.split()
    if len(args) < 3:
        await message.answer("Использование: /give ID сумма", reply_markup=menu_markup_for_chat(message))
        return

    user_id = int(args[1])
    amount = float(args[2].replace(",", "."))
    get_user(user_id, "none")

    cur.execute("UPDATE users SET balance = balance + ? WHERE tg_id=?", (amount, user_id))
    db.commit()
    await message.answer("✅ Баланс пополнен.", reply_markup=menu_markup_for_chat(message))


@dp.message(Command("take"))
async def take(message: Message):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    args = message.text.split()
    if len(args) < 3:
        await message.answer("Использование: /take ID сумма", reply_markup=menu_markup_for_chat(message))
        return

    user_id = int(args[1])
    amount = float(args[2].replace(",", "."))
    get_user(user_id, "none")

    cur.execute("UPDATE users SET balance = MAX(balance - ?, 0) WHERE tg_id=?", (amount, user_id))
    db.commit()
    await message.answer("✅ Деньги сняты.", reply_markup=menu_markup_for_chat(message))


@dp.message(Command("add"))
async def add_role(message: Message):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    role_name = message.text.replace("/add", "", 1).strip()
    if not role_name:
        await message.answer("Использование: /add роль", reply_markup=menu_markup_for_chat(message))
        return

    rarity, _, reward = choose_rarity()

    try:
        cur.execute("INSERT INTO roles(name, rarity, reward) VALUES(?,?,?)", (role_name, rarity, reward))
        db.commit()
        await message.answer(
            f"✅ Роль добавлена: {html.escape(role_name)}\n"
            f"Редкость: {rarity}\n"
            f"Награда: {reward}💲 ",
            reply_markup=main_menu_keyboard()
        )
    except sqlite3.IntegrityError:
        await message.answer("Такая роль уже есть.", reply_markup=menu_markup_for_chat(message))


@dp.message(F.document)
async def txt_upload(message: Message):
    if await ban_handler(message):
        return
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        return

    document = message.document
    if not document.file_name or not document.file_name.lower().endswith(".txt"):
        await message.answer("Отправьте TXT файл.", reply_markup=menu_markup_for_chat(message))
        return

    file = await bot.get_file(document.file_id)
    data = await bot.download_file(file.file_path)
    text = data.read().decode("utf-8", errors="ignore")

    added = 0

    for line in text.splitlines():
        role_name = line.strip()
        if not role_name:
            continue

        rarity, _, reward = choose_rarity()

        try:
            cur.execute("INSERT INTO roles(name, rarity, reward) VALUES(?,?,?)", (role_name, rarity, reward))
            added += 1
        except sqlite3.IntegrityError:
            pass

    db.commit()
    await message.answer(f"✅ Добавлено ролей: {added}", reply_markup=menu_markup_for_chat(message))


@dp.callback_query(F.data.startswith("withdraw_ok:"))
async def withdraw_ok(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    wid = int(callback.data.split(":")[1])
    cur.execute("UPDATE withdraws SET status='approved' WHERE id=?", (wid,))
    db.commit()

    await callback.message.edit_text(callback.message.text + "\n\n✅ Одобрено")
    await callback.answer()


@dp.callback_query(F.data.startswith("withdraw_no:"))
async def withdraw_no(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    wid = int(callback.data.split(":")[1])
    cur.execute("UPDATE withdraws SET status='declined' WHERE id=?", (wid,))
    db.commit()

    await callback.message.edit_text(callback.message.text + "\n\n❌ Отклонено")
    await callback.answer()


def parse_bet_from_text(text: str):
    cleaned = text.lower().replace("💲 ", "").replace("💰 ", "").replace(",", ".")
    parts = cleaned.split()

    if len(parts) >= 2 and parts[0] == "ставка":
        try:
            return float(parts[1])
        except Exception:
            return None

    try:
        return float(cleaned.strip())
    except Exception:
        return None


def parse_simple_game(text: str, game_name: str):
    parts = text.lower().replace("💲 ", "").replace(",", ".").split()
    if len(parts) < 1 or parts[0] != game_name:
        return None

    if len(parts) >= 2:
        try:
            return float(parts[1])
        except Exception:
            return None

    return None


def parse_cube_game(text: str):
    parts = text.lower().replace("💲 ", "").replace(",", ".").split()

    if len(parts) < 3 or parts[0] != "куб":
        return None

    try:
        nums = [int(x) for x in parts[1:]]
    except Exception:
        return None

    stake = nums[-1]
    sides = nums[:-1]

    if not sides:
        return None

    unique_sides = []
    for side in sides:
        if side not in unique_sides:
            unique_sides.append(side)

    if any(side < 1 or side > 6 for side in unique_sides):
        return None

    return unique_sides, float(stake)


async def process_game(message: Message, game: str, emoji: str, stake: float, multiplier: float, win_values: set[int]):
    if stake < 2 or stake > 40:
        await message.answer("💰 Ставка должна быть от 2 до 40💲 .", reply_markup=menu_markup_for_chat(message))
        return True

    balance = user_balance(message.from_user.id)

    if balance < stake:
        await message.answer("💰 Недостаточно средств.", reply_markup=menu_markup_for_chat(message))
        return True

    last_game_state[message.from_user.id] = {"type": "simple", "game": game, "emoji": emoji, "stake": stake, "multiplier": multiplier, "win_values": list(win_values)}

    dice_msg = await bot.send_dice(chat_id=message.chat.id, emoji=emoji, reply_to_message_id=message.message_id)
    await asyncio.sleep(4)

    value = dice_msg.dice.value
    win = value in win_values

    if win:
        amount = round(stake * multiplier, 2)
        result_word = "Выигрыш"
        cur.execute("UPDATE users SET balance = balance + ?, games = games + 1 WHERE tg_id=?", (amount, message.from_user.id))
    else:
        amount = stake
        result_word = "Проигрыш"
        cur.execute("UPDATE users SET balance = MAX(balance - ?, 0), games = games + 1 WHERE tg_id=?", (amount, message.from_user.id))

    db.commit()

    await message.answer(
        f"{user_mention(message.from_user)} - <b>{result_word}</b> {fmt_money(amount)} {pe('money', '💰')} в игре {emoji}\n\n"
        f"💰 <b>Баланс:</b> {fmt_money(user_balance(message.from_user.id))} 💰 ",
        reply_markup=repeat_game_keyboard(message.from_user.id),
        reply_to_message_id=message.message_id
    )
    return True


async def process_cube(message: Message, sides: list[int], stake: float):
    if stake < 2 or stake > 40:
        await message.answer("💰 Ставка должна быть от 2 до 40💲 .", reply_markup=menu_markup_for_chat(message))
        return True

    balance = user_balance(message.from_user.id)

    if balance < stake:
        await message.answer("💰 Недостаточно средств.", reply_markup=menu_markup_for_chat(message))
        return True

    last_game_state[message.from_user.id] = {"type": "cube", "sides": sides, "stake": stake}

    dice_msg = await bot.send_dice(chat_id=message.chat.id, emoji="🎲", reply_to_message_id=message.message_id)
    await asyncio.sleep(4)

    value = dice_msg.dice.value
    win = value in sides
    multiplier = round(6 / len(sides), 2)

    if win:
        amount = round(stake * multiplier, 2)
        result_word = "Выигрыш"
        cur.execute("UPDATE users SET balance = balance + ?, games = games + 1 WHERE tg_id=?", (amount, message.from_user.id))
    else:
        amount = stake
        result_word = "Проигрыш"
        cur.execute("UPDATE users SET balance = MAX(balance - ?, 0), games = games + 1 WHERE tg_id=?", (amount, message.from_user.id))

    db.commit()

    await message.answer(
        f"{user_mention(message.from_user)} - <b>{result_word}</b> {fmt_money(amount)} 💰 в игре 🧊 \n\n"
        f"💰 <b>Баланс:</b> {fmt_money(user_balance(message.from_user.id))} 💰 ",
        reply_markup=repeat_game_keyboard(message.from_user.id),
        reply_to_message_id=message.message_id
    )
    return True


@dp.callback_query(F.data.startswith("repeat_game:"))
async def repeat_game(callback: CallbackQuery):
    owner_id = int(callback.data.split(":")[1])
    if callback.from_user.id != owner_id:
        await callback.answer("Повторить ставку может только владелец.", show_alert=True)
        return

    get_user(callback.from_user.id, callback.from_user.username)
    data = last_game_state.get(callback.from_user.id)

    if not data:
        await callback.answer("Нет последней ставки.", show_alert=True)
        return

    await callback.answer()

    class FakeMessage:
        def __init__(self, callback):
            self.from_user = callback.from_user
            self.chat = callback.message.chat
            self.message_id = callback.message.message_id

        async def answer(self, text, **kwargs):
            return await callback.message.answer(text, **kwargs)

        async def answer_dice(self, emoji):
            return await callback.message.answer_dice(emoji=emoji)

    fake = FakeMessage(callback)

    if data["type"] == "simple":
        await process_game(
            fake,
            data["game"],
            data["emoji"],
            float(data["stake"]),
            float(data["multiplier"]),
            set(data["win_values"])
        )
    elif data["type"] == "cube":
        await process_cube(fake, list(data["sides"]), float(data["stake"]))


@dp.message()
async def all_messages(message: Message):
    save_group(message.chat)

    if await ban_handler(message):
        return

    if await techper_guard(message):
        return

    get_user(message.from_user.id, message.from_user.username)



    if message.chat.type == "private" and message.from_user.id in transfer_state:
        state = transfer_state[message.from_user.id]

        if state["step"] == "target":
            text = (message.text or "").strip()

            target_id = None

            if text.startswith("@"):
                username = text[1:]

                cur.execute(
                    "SELECT tg_id, username FROM users WHERE LOWER(username)=LOWER(?)",
                    (username,)
                )
                row = cur.fetchone()

                if row:
                    target_id = row[0]

            elif text.isdigit():
                target_id = int(text)

            if not target_id:
                await message.answer("❌ Пользователь не найден.")
                transfer_state.pop(message.from_user.id, None)
                return

            if target_id == message.from_user.id:
                await message.answer("<b>❌ Нельзя перевести самому себе.</b>")
                transfer_state.pop(message.from_user.id, None)
                return

            cur.execute("SELECT tg_id FROM users WHERE tg_id=?", (target_id,))
            exists = cur.fetchone()

            if not exists:
                await message.answer("❌ Пользователь не найден.")
                transfer_state.pop(message.from_user.id, None)
                return

            state["target_id"] = target_id
            state["step"] = "amount"

            await message.answer(
                f"💵 Сколько <b>денег</b> вы хотите <b>передать</b>?\n"
                f"💰 <b>Ваш баланс:</b> {fmt_money(user_balance(message.from_user.id))} 💰 "
            )
            return

        if state["step"] == "amount":
            try:
                amount = float((message.text or "0").replace(",", "."))
            except Exception:
                amount = 0

            if amount <= 0:
                await message.answer("❌ Неверная сумма.")
                transfer_state.pop(message.from_user.id, None)
                return

            balance = user_balance(message.from_user.id)

            if amount > balance:
                await message.answer("❌ Недостаточно средств.")
                transfer_state.pop(message.from_user.id, None)
                return

            state["amount"] = amount
            state["step"] = "comment"

            await message.answer(
                "💬 Комментарий который вы хотите оставить."
            )
            return

        if state["step"] == "comment":
            comment = (message.text or "").strip()

            target_id = state["target_id"]
            amount = state["amount"]

            cur.execute(
                "UPDATE users SET balance = balance - ? WHERE tg_id=?",
                (amount, message.from_user.id)
            )

            cur.execute(
                "UPDATE users SET balance = balance + ? WHERE tg_id=?",
                (amount, target_id)
            )

            db.commit()

            transfer_state.pop(message.from_user.id, None)

            sender_username = (
                f"@{message.from_user.username}"
                if message.from_user.username
                else message.from_user.full_name
            )

            await message.answer("<b>✅ Отправлено!</b>")

            try:
                await bot.send_message(
                    target_id,
                    f"{pe('mail', '💌')} <b>Вам перевели деньги!</b>\n"
                    f"{dollar()} <b>От кого:</b> {sender_username}\n"
                    f"💬 <i>{html.escape(sender_username)} - {html.escape(comment)}</i>"
                )
            except:
                pass

            return


    if message.chat.type == "private" and message.from_user.id in promo_state:
        state = promo_state[message.from_user.id]

        if state["step"] == "promo_code":
            code_name = (message.text or "").strip().upper()

            cur.execute("SELECT amount, max_uses, uses, active FROM promos WHERE code=?", (code_name,))
            promo = cur.fetchone()

            if not promo:
                promo_state.pop(message.from_user.id, None)
                await message.answer("❌ Промокод <b>не найден</b>.", reply_markup=main_menu_keyboard())
                return

            amount, max_uses, uses, active = promo

            cur.execute(
                "SELECT 1 FROM promo_uses WHERE promo_code=? AND user_tg_id=?",
                (code_name, message.from_user.id)
            )
            already_used = cur.fetchone() is not None

            if already_used or not active or uses >= max_uses:
                promo_state.pop(message.from_user.id, None)
                await message.answer("❌ Промокод <b>не найден</b>.", reply_markup=main_menu_keyboard())
                return

            captcha = captcha_text()
            promo_state[message.from_user.id] = {
                "step": "captcha",
                "promo_code": code_name,
                "captcha": captcha
            }

            image_bytes = make_captcha_image(captcha)
            await message.answer_photo(
                BufferedInputFile(image_bytes, filename="captcha.png"),
                caption="🖼 <b>Введите текст с картинки</b>, чтобы <b>активировать промокод</b>:"
            )
            return

        if state["step"] == "captcha":
            answer = (message.text or "").strip().upper()
            expected = state["captcha"].upper()
            code_name = state["promo_code"]

            if answer != expected:
                promo_state.pop(message.from_user.id, None)
                await message.answer("❌ Неверная капча.", reply_markup=main_menu_keyboard())
                return

            cur.execute("SELECT amount, max_uses, uses, active FROM promos WHERE code=?", (code_name,))
            promo = cur.fetchone()

            if not promo:
                promo_state.pop(message.from_user.id, None)
                await message.answer("❌ Промокод <b>не найден</b>.", reply_markup=main_menu_keyboard())
                return

            amount, max_uses, uses, active = promo

            cur.execute(
                "SELECT 1 FROM promo_uses WHERE promo_code=? AND user_tg_id=?",
                (code_name, message.from_user.id)
            )
            already_used = cur.fetchone() is not None

            if already_used or not active or uses >= max_uses:
                promo_state.pop(message.from_user.id, None)
                await message.answer("❌ Промокод <b>не найден</b>.", reply_markup=main_menu_keyboard())
                return

            cur.execute("UPDATE users SET balance = balance + ? WHERE tg_id=?", (amount, message.from_user.id))
            cur.execute("UPDATE promos SET uses = uses + 1 WHERE code=?", (code_name,))
            cur.execute(
                "INSERT INTO promo_uses(promo_code, user_tg_id, activated_at) VALUES(?,?,?)",
                (code_name, message.from_user.id, int(time.time()))
            )

            cur.execute("SELECT uses, max_uses FROM promos WHERE code=?", (code_name,))
            new_uses, max_uses_now = cur.fetchone()
            if new_uses >= max_uses_now:
                cur.execute("UPDATE promos SET active=0 WHERE code=?", (code_name,))

            db.commit()
            promo_state.pop(message.from_user.id, None)

            await message.answer(f"<b>✅ Начислено: {fmt_money(amount)}💲 </b>", reply_markup=main_menu_keyboard())
            return

    if message.chat.type == "private" and message.from_user.id in bet_state:
        amount = parse_bet_from_text(message.text or "")

        if amount is None:
            await message.answer("💰 <b>Напишите</b> <b>ставку так</b>: <code>Ставка 40💲 </code>", reply_markup=menu_markup_for_chat(message))
            return

        if amount < 2 or amount > 40:
            await message.answer("💰 Ставка должна быть от 2 до 40💲 .", reply_markup=menu_markup_for_chat(message))
            return

        set_current_bet(message.from_user.id, amount)
        bet_state.pop(message.from_user.id, None)

        await message.answer(
            f"✅ <b>Ставка</b> изменена на {fmt_money(amount)} 💰 ",
            reply_markup=main_menu_keyboard()
        )
        return

    if message.chat.type == "private" and message.from_user.id in withdraw_state:
        state = withdraw_state[message.from_user.id]

        if state["step"] == "wallet":
            state["wallet"] = message.text
            state["step"] = "amount"
            await message.answer("Теперь отправьте сумму вывода. Минимум 500💲 .", reply_markup=menu_markup_for_chat(message))
            return

        if state["step"] == "amount":
            try:
                amount = float(message.text.replace(",", "."))
            except Exception:
                await message.answer("Введите сумму числом.", reply_markup=menu_markup_for_chat(message))
                return

            user = get_user(message.from_user.id, message.from_user.username)

            if amount < 500:
                await message.answer("Минимальная сумма вывода — 500💲 .", reply_markup=menu_markup_for_chat(message))
                return

            if user[4] < amount:
                await message.answer("Недостаточно средств.", reply_markup=menu_markup_for_chat(message))
                return

            state["amount"] = amount
            state["step"] = "source"
            await message.answer("Откуда вы узнали об этом боте?", reply_markup=menu_markup_for_chat(message))
            return

        if state["step"] == "source":
            wallet = state["wallet"]
            amount = state["amount"]
            source = message.text

            cur.execute("UPDATE users SET balance = balance - ? WHERE tg_id=?", (amount, message.from_user.id))
            cur.execute(
                "INSERT INTO withdraws(user_tg_id, wallet, amount, source, created_at) VALUES(?,?,?,?,?)",
                (message.from_user.id, wallet, amount, source, int(time.time()))
            )

            wid = cur.lastrowid
            db.commit()

            await bot.send_message(
                WITHDRAW_LOG_CHAT_ID,
                f"🆕 Заявка на вывод #{wid}\n\n"
                f"Пользователь: @{message.from_user.username or 'none'}\n"
                f"ID: {message.from_user.id}\n"
                f"Сумма: {amount}💲 \n"
                f"Кошелёк: {html.escape(wallet)}\n"
                f"Откуда узнал: {html.escape(source)}",
                reply_markup=withdraw_review_keyboard(wid)
            )

            withdraw_state.pop(message.from_user.id, None)
            await message.answer("✅ Заявка отправлена на рассмотрение.", reply_markup=menu_markup_for_chat(message))
            return

    if not message.text:
        return

    text = message.text.strip()
    lowered = text.lower().strip()

    football_bet = parse_simple_game(text, "футбол")
    if football_bet is not None:
        await process_game(message, "футбол", "⚽️ ", football_bet, 1.5, {3, 4, 5})
        return

    basketball_bet = parse_simple_game(text, "баскетбол")
    if basketball_bet is not None:
        await process_game(message, "баскетбол", "🏀 ", basketball_bet, 2, {4, 5})
        return

    bowling_bet = parse_simple_game(text, "боулинг")
    if bowling_bet is not None:
        await process_game(message, "боулинг", "🎳 ", bowling_bet, 2, {6})
        return

    cube_data = parse_cube_game(text)
    if cube_data is not None:
        sides, stake = cube_data
        await process_cube(message, sides, stake)
        return

    if lowered not in TRIGGERS:
        return

    ROLE_COOLDOWN = 6 * 60
    now = int(time.time())

    cur.execute("SELECT last_role_time FROM users WHERE tg_id=?", (message.from_user.id,))
    row = cur.fetchone()
    last_role_time = int(row[0] or 0) if row else 0

    remaining = ROLE_COOLDOWN - (now - last_role_time)

    if remaining > 0:
        hours = remaining // 3600
        minutes = (remaining % 3600 + 59) // 60

        await message.answer(
            f"⏲️ <b>Подождите</b> еще {hours} ч. <b>{minutes} мин.</b> перед получением <b>новой роли</b>."
        )
        return

    cur.execute("SELECT name, rarity, reward FROM roles")
    roles = cur.fetchall()

    if not roles:
        await message.answer("Ролей пока нет. Админ должен добавить роли в ЛС через /add или TXT файл.", reply_markup=menu_markup_for_chat(message))
        return

    role_name, rarity, reward = random.choice(roles)

    if not rarity:
        rarity, _, reward = choose_rarity()
        cur.execute("UPDATE roles SET rarity=?, reward=? WHERE name=?", (rarity, reward, role_name))
        db.commit()

    if reward is None:
        reward = reward_by_rarity(rarity)
        cur.execute("UPDATE roles SET reward=? WHERE name=?", (reward, role_name))
        db.commit()

    cur.execute(
        "SELECT count, rarity FROM user_roles WHERE user_tg_id=? AND role_name=?",
        (message.from_user.id, role_name)
    )
    existing = cur.fetchone()
    is_repeat = existing is not None

    if is_repeat:
        saved_rarity = existing[1] or rarity
        rarity = saved_rarity
        reward = reward_by_rarity(rarity)

        cur.execute(
            "UPDATE user_roles SET count=count+1 WHERE user_tg_id=? AND role_name=?",
            (message.from_user.id, role_name)
        )
    else:
        cur.execute(
            "INSERT INTO user_roles(user_tg_id, role_name, rarity, count) VALUES(?,?,?,1)",
            (message.from_user.id, role_name, rarity)
        )

    if is_repeat:
        cur.execute("UPDATE users SET balance = balance + ?, last_role_time = ? WHERE tg_id=?", (reward, now, message.from_user.id))
    else:
        cur.execute(
            "UPDATE users SET balance = balance + ?, roles_opened = roles_opened + 1, last_role_time = ? WHERE tg_id=?",
            (reward, now, message.from_user.id)
        )

    db.commit()

    await message.answer(
        f"{pe('roles', '🎭')}<b>{user_mention(message.from_user)} — {html.escape(role_name)}</b>\n\n"
        f"{pe('rarity', '💤')} Редкость: <b>{html.escape(str(rarity))}</b>\n"
        f"{pe('bag', '💼')} <b>Добавлено:</b> +{fmt_money(reward)} {pe('cash', '💵')}",
        reply_markup=menu_markup_for_chat(message)
    )



async def main():
    print("Бот запущен. Для триггеров в группе отключи Privacy Mode в BotFather.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
