import asyncio
import sqlite3
import aiosqlite
import os
import logging
import aiohttp
import urllib.parse
from datetime import datetime
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from contextlib import asynccontextmanager
import random
import string
import time

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
DATABASE_FILE = os.getenv("DATABASE_FILE", "taxi_bot.db")
DB_TIMEOUT = 30.0

ADMIN_PHONE = os.getenv("ADMIN_PHONE", "")
ADMIN_USER_LOGIN = os.getenv("ADMIN_USER_LOGIN", "")

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
db_lock = asyncio.Lock()

# Connection pool
db_lock = asyncio.Lock()

# ==================== LOGGING ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('taxi_bot.log'),
              logging.StreamHandler()])
logger = logging.getLogger(__name__)


def log_action(user_id: int, action: str, details: str = ""):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"[{timestamp}] User {user_id}: {action}"
    if details:
        log_msg += f" | {details}"
    logger.info(log_msg)


# ==================== STATES ====================


class DriverReg(StatesGroup):
    confirm_data = State()
    phone_number = State()
    car_number = State()
    car_model = State()
    seats = State()
    current_city = State()


class ClientOrder(StatesGroup):
    confirm_data = State()
    phone_number = State()
    from_city = State()
    to_city = State()
    direction = State()
    passengers_count = State()
    add_another = State()


class RatingStates(StatesGroup):
    select_rating = State()
    write_review = State()


class ChatState(StatesGroup):
    waiting_message_to_driver = State()
    waiting_message_to_client = State()


# ==================== DATABASE AND MIGRATIONS ====================


async def init_db():
    """Initialize database if not exists, otherwise reuse existing."""
    if os.path.exists(DATABASE_FILE):
        logger.info("ℹ️ Existing database found — skipping initialization.")
        return

    # Create a new database only if missing
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()

    # === Tables ===
    c.execute('''CREATE TABLE IF NOT EXISTS drivers
                 (user_id INTEGER PRIMARY KEY,
                  full_name TEXT NOT NULL,
                  phone TEXT NOT NULL,
                  car_number TEXT NOT NULL,
                  car_model TEXT NOT NULL,
                  total_seats INTEGER NOT NULL,
                  direction TEXT NOT NULL,
                  queue_position INTEGER NOT NULL,
                  is_active INTEGER DEFAULT 0,
                  is_verified INTEGER DEFAULT 0,
                  verification_code TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  avg_rating REAL DEFAULT 0,
                  rating_count INTEGER DEFAULT 0,
                  occupied_seats INTEGER DEFAULT 0,
                  is_on_trip INTEGER DEFAULT 0,
                  payment_methods TEXT DEFAULT '')''')

    c.execute('''CREATE TABLE IF NOT EXISTS clients
                 (user_id INTEGER PRIMARY KEY,
                  full_name TEXT NOT NULL,
                  phone TEXT NOT NULL,
                  direction TEXT NOT NULL,
                  queue_position INTEGER NOT NULL,
                  passengers_count INTEGER DEFAULT 1,
                  is_verified INTEGER DEFAULT 0,
                  verification_code TEXT,
                  status TEXT DEFAULT 'waiting',
                  assigned_driver_id INTEGER,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  avg_rating REAL DEFAULT 0,
                  rating_count INTEGER DEFAULT 0,
                  cancellation_count INTEGER DEFAULT 0,
                  order_for TEXT DEFAULT 'self',
                  order_number INTEGER DEFAULT 1,
                  parent_user_id INTEGER,
                  from_city TEXT DEFAULT '',
                  to_city TEXT DEFAULT '')''')

    c.execute('''CREATE TABLE IF NOT EXISTS admins
                 (user_id INTEGER PRIMARY KEY,
                  added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    c.execute('''CREATE TABLE IF NOT EXISTS ratings
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  from_user_id INTEGER,
                  to_user_id INTEGER,
                  user_type TEXT,
                  trip_id INTEGER,
                  rating INTEGER CHECK(rating >= 1 AND rating <= 5),
                  review TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    c.execute('''CREATE TABLE IF NOT EXISTS trips
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  driver_id INTEGER,
                  client_id INTEGER,
                  direction TEXT,
                  passengers_count INTEGER,
                  status TEXT,
                  driver_arrived_at TIMESTAMP,
                  trip_started_at TIMESTAMP,
                  trip_completed_at TIMESTAMP,
                  cancelled_by TEXT,
                  cancelled_at TIMESTAMP,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    c.execute('''CREATE TABLE IF NOT EXISTS actions_log
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  action TEXT,
                  details TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    c.execute('''CREATE TABLE IF NOT EXISTS blacklist
                 (user_id INTEGER PRIMARY KEY,
                  reason TEXT,
                  cancellation_count INTEGER DEFAULT 0,
                  banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    # === Indexes and pragmas ===
    c.execute("CREATE INDEX IF NOT EXISTS idx_clients_status ON clients(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_clients_direction ON clients(direction)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_drivers_direction ON drivers(direction)")
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")

    conn.commit()
    conn.close()
    logger.info("✅ Database initialized successfully (first-time creation).")


@asynccontextmanager
async def get_db(write: bool = False):
    """Async context manager for working with SQLite database"""
    async with db_lock:
        db = await aiosqlite.connect(DATABASE_FILE, timeout=DB_TIMEOUT)
        await db.execute("PRAGMA foreign_keys = ON;")
        try:
            yield db
            if write:
                await db.commit()
        finally:
            await db.close()


# ==================== UTILITIES ====================


async def is_admin(user_id: int) -> bool:
    async with get_db() as db:
        async with db.execute("SELECT user_id FROM admins WHERE user_id=?",
                              (user_id, )) as cursor:
            result = await cursor.fetchone()
            return result is not None


async def save_log_action(user_id: int, action: str, details: str = ""):
    """Save action log - ASYNC VERSION"""
    try:
        async with get_db(write=True) as db:
            await db.execute(
                '''INSERT INTO actions_log (user_id, action, details) 
                               VALUES (?, ?, ?)''', (user_id, action, details))
        log_action(user_id, action, details)
    except Exception as e:
        logger.error(f"Failed to save log: {e}")


async def get_driver_available_seats(driver_id: int) -> tuple:
    """Returns (occupied seats, total seats, available seats) - ASYNC VERSION"""
    async with get_db() as db:
        # Check for column existence
        async with db.execute("PRAGMA table_info(drivers)") as cursor:
            columns = [col[1] for col in await cursor.fetchall()]

        if 'occupied_seats' in columns and 'total_seats' in columns:
            async with db.execute(
                    "SELECT total_seats, COALESCE(occupied_seats, 0) FROM drivers WHERE user_id=?",
                (driver_id, )) as cursor:
                result = await cursor.fetchone()

            if not result:
                return (0, 0, 0)

            total = result[0]
            occupied = result[1]
            available = total - occupied
            return (occupied, total, available)
        else:
            async with db.execute(
                    "SELECT total_seats FROM drivers WHERE user_id=?",
                (driver_id, )) as cursor:
                result = await cursor.fetchone()

            if not result:
                return (0, 0, 0)

            total = result[0]
            return (0, total, total)


async def check_blacklist(user_id: int) -> tuple:
    """
    Checks if the user is in the blacklist
    Returns (is_banned: bool, reason: str)
    """
    async with get_db() as db:
        async with db.execute("SELECT reason FROM blacklist WHERE user_id=?",
                              (user_id, )) as cursor:
            result = await cursor.fetchone()
            if result:
                return (True, result[0])
            return (False, None)


async def get_cancellation_count(user_id: int) -> int:
    """Returns the number of cancellations for the client"""
    async with get_db() as db:
        async with db.execute(
                "SELECT COALESCE(cancellation_count, 0) FROM clients WHERE user_id=?",
            (user_id, )) as cursor:
            result = await cursor.fetchone()
            return result[0] if result else 0


async def add_to_blacklist(user_id: int, reason: str, cancellation_count: int):
    """Adds a user to the blacklist"""
    async with get_db(write=True) as db:
        await db.execute(
            '''INSERT OR REPLACE INTO blacklist (user_id, reason, cancellation_count)
               VALUES (?, ?, ?)''', (user_id, reason, cancellation_count))
    await save_log_action(user_id, "blacklisted", reason)


async def get_user_active_orders(user_id: int) -> list:
    """Returns all active orders for the user"""
    async with get_db() as db:
        async with db.execute(
                '''SELECT user_id, full_name, order_for, order_number, status, 
                      direction, passengers_count, from_city, to_city,
                      assigned_driver_id
               FROM clients 
               WHERE parent_user_id=? OR user_id=?
               ORDER BY order_number''', (user_id, user_id)) as cursor:
            return await cursor.fetchall()


async def count_user_orders(user_id: int) -> int:
    """Counts the number of active orders for the user"""
    async with get_db() as db:
        async with db.execute(
                '''SELECT COUNT(*) FROM clients 
               WHERE (parent_user_id=? OR user_id=?) 
               AND status IN ('waiting', 'accepted', 'driver_arrived')''',
            (user_id, user_id)) as cursor:
            result = await cursor.fetchone()
            return result[0] if result else 0


def main_menu_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🚗 Жүргізуші ретінде кіру")],
                  [KeyboardButton(text="🧍‍♂️ Такси шақыру")],
                  [KeyboardButton(text="⭐ Профиль")],
                  [KeyboardButton(text="ℹ️ Ақпарат")]],
        resize_keyboard=True)


def from_city_keyboard():
    """Choosing the direction for clients"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Ақтау → Жаңаөзен", callback_data="dir_aktau_janaozen")],
        [InlineKeyboardButton(text="Жаңаөзен → Ақтау", callback_data="dir_janaozen_aktau")],
        [InlineKeyboardButton(text="Ақтау → Шетпе", callback_data="dir_aktau_shetpe")],
        [InlineKeyboardButton(text="Шетпе → Ақтау", callback_data="dir_shetpe_aktau")],
        [InlineKeyboardButton(text="🔙 Артқа", callback_data="back_main")]
    ])


def get_rating_stars(rating: float) -> str:
    if not rating:
        return "❌ Баға жоқ"
    stars = int(rating)
    return "⭐" * stars + "☆" * (5 - stars)


# ==================== DRIVERS ====================


@dp.message(F.text == "🚗 Жүргізуші ретінде кіру")
async def driver_start_telegram_auth(message: types.Message,
                                     state: FSMContext):
    user_id = message.from_user.id

    async with get_db() as db:
        async with db.execute("SELECT * FROM drivers WHERE user_id=?",
                              (user_id, )) as cursor:
            driver = await cursor.fetchone()

    if driver:
        # Show driver menu instead of error message
        await show_driver_menu(message, user_id)
        return

    full_name = message.from_user.full_name
    username = message.from_user.username

    await state.update_data(telegram_id=user_id,
                            full_name=full_name,
                            username=username or "",
                            verified_by='telegram',
                            is_verified=True)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Бәрі дұрыс",
                             callback_data="confirm_telegram_data")
    ]])

    await message.answer(
        f"👤 <b>Сіздің Telegram деректеріңіз:</b>\n\n"
        f"Аты: {full_name}\n"
        f"Username: @{username if username else 'орнатылмаған'}\n"
        f"ID: <code>{user_id}</code>",
        reply_markup=keyboard,
        parse_mode="HTML")
    await state.set_state(DriverReg.confirm_data)


@dp.callback_query(DriverReg.confirm_data, F.data == "confirm_telegram_data")
async def confirm_telegram_data(callback: types.CallbackQuery,
                                state: FSMContext):
    await callback.message.edit_text("✅ Керемет! Тіркеуді жалғастырамыз...")
    await callback.message.answer(
        "📱 Телефон нөміріңізді енгізіңіз (мысалы: +7 777 123 45 67):")
    await state.set_state(DriverReg.phone_number)
    await callback.answer()


@dp.message(DriverReg.phone_number)
async def driver_phone_number(message: types.Message, state: FSMContext):
    await state.update_data(phone_number=message.text.strip())
    await message.answer("🚗 Көлік нөмірі (мысалы: 870 ABC 09)")
    await state.set_state(DriverReg.car_number)


@dp.callback_query(DriverReg.confirm_data, F.data == "continue_no_username")
async def continue_without_username(callback: types.CallbackQuery,
                                    state: FSMContext):
    """Continue without username"""
    await callback.message.edit_text("✅ Керемет! Тіркеуді жалғастырамыз...")
    await callback.message.answer("🚗 Көлік нөмірі (мысалы: 870 ABC 09)")
    await state.set_state(DriverReg.car_number)
    await callback.answer()


@dp.message(DriverReg.car_number)
async def driver_car_number(message: types.Message, state: FSMContext):
    await state.update_data(car_number=message.text)
    await message.answer("Көлік маркасы (мысалы: Toyota Camry)")
    await state.set_state(DriverReg.car_model)


@dp.message(DriverReg.car_model)
async def driver_car_model(message: types.Message, state: FSMContext):
    await state.update_data(car_model=message.text)
    await message.answer("Көлікте қанша орын бар? (1-8)")
    await state.set_state(DriverReg.seats)


@dp.message(DriverReg.seats)
async def driver_seats(message: types.Message, state: FSMContext):
    try:
        seats = int(message.text)
        if seats < 1 or seats > 7:  # Changed from 8 to 7
            await message.answer("Қате! Орын саны 1-ден 7-ге дейін болуы керек")
            return
        await state.update_data(seats=seats)
        await message.answer("📍 Қай бағытты таңдайсыз?",  # Changed text
                             reply_markup=current_city_keyboard())
        await state.set_state(DriverReg.current_city)
    except ValueError:
        await message.answer("Сан енгізіңіз!")


@dp.callback_query(DriverReg.current_city, F.data.startswith("dir_"))
async def driver_current_city(callback: types.CallbackQuery, state: FSMContext):
    direction_map = {
        "dir_aktau_janaozen": "Ақтау → Жаңаөзен",
        "dir_janaozen_aktau": "Жаңаөзен → Ақтау",
        "dir_aktau_shetpe": "Ақтау → Шетпе",
        "dir_shetpe_aktau": "Шетпе → Ақтау"
    }
    
    direction = direction_map.get(callback.data, "Ақтау → Жаңаөзен")
    data = await state.get_data()
    
    # Extract from_city from direction
    from_city = direction.split(" → ")[0]

    # Use provided phone number
    phone = data.get('phone_number', f"tg_{callback.from_user.id}")

    async with get_db(write=True) as db:
        # Driver registers with chosen direction
        await db.execute(
            '''INSERT INTO drivers 
                     (user_id, full_name, phone, car_number, car_model, total_seats, 
                      direction, queue_position, is_active, is_verified, occupied_seats)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (callback.from_user.id, data['full_name'], phone,
             data['car_number'], data['car_model'], data['seats'],
             direction, 0, 1, 1, 0))

    await save_log_action(callback.from_user.id, "driver_registered",
                          f"Direction: {direction}")

    await callback.message.edit_text(
        f"✅ <b>Сіз тіркелдіңіз!</b>\n\n"
        f"👤 {data['full_name']}\n"
        f"🚗 {data['car_model']} ({data['car_number']})\n"
        f"💺 Орын саны: {data['seats']}\n"
        f"📍 Бағыт: {direction}\n\n"
        f"Сіз {direction} бағыты бойынша тапсырыстарды көре аласыз\n\n"
        f"💡 Кеңес: Жүргізуші мәзіріне тез өту үшін /driver командасын пайдаланыңыз",
        parse_mode="HTML")

    await state.clear()

    # ✅ Automatically show driver menu
    await show_driver_menu(callback.message, callback.from_user.id)

    await callback.answer()


def current_city_keyboard():
    """Choosing the direction for the driver"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Ақтау → Жаңаөзен", callback_data="dir_aktau_janaozen")],
        [InlineKeyboardButton(text="Жаңаөзен → Ақтау", callback_data="dir_janaozen_aktau")],
        [InlineKeyboardButton(text="Ақтау → Шетпе", callback_data="dir_aktau_shetpe")],
        [InlineKeyboardButton(text="Шетпе → Ақтау", callback_data="dir_shetpe_aktau")],
        [InlineKeyboardButton(text="🔙 Артқа", callback_data="back_main")]
    ])


async def show_driver_menu(message: types.Message, user_id: int):
    async with get_db() as db:
        async with db.execute("PRAGMA table_info(drivers)") as cursor:
            columns = [col[1] for col in await cursor.fetchall()]

        async with db.execute("SELECT * FROM drivers WHERE user_id=?",
                              (user_id, )) as cursor:
            driver = await cursor.fetchone()

    if not driver:
        await message.answer("Қате: сіз тіркелмегенсіз",
                             reply_markup=main_menu_keyboard())
        return

    full_name_idx = columns.index('full_name') if 'full_name' in columns else 1
    car_number_idx = columns.index(
        'car_number') if 'car_number' in columns else 2
    car_model_idx = columns.index('car_model') if 'car_model' in columns else 3
    direction_idx = columns.index(
        'direction') if 'direction' in columns else 6

    if 'occupied_seats' in columns and 'total_seats' in columns:
        occupied, total, available = await get_driver_available_seats(user_id)
        seats_text = f"💺 Бос емес: {occupied}/{total} (бос: {available})\n"
    else:
        total_seats_idx = columns.index(
            'total_seats') if 'total_seats' in columns else 5
        total = driver[total_seats_idx] if len(driver) > total_seats_idx else 4
        seats_text = f"💺 Мест: {total}\n"

    avg_rating_idx = columns.index(
        'avg_rating') if 'avg_rating' in columns else None
    rating_text = get_rating_stars(driver[avg_rating_idx] if avg_rating_idx
                                   and len(driver) > avg_rating_idx else 0)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статус", callback_data="driver_status")],
        [
            InlineKeyboardButton(text="👥 Менің жолаушыларым",
                                 callback_data="driver_passengers")
        ],
        [
            InlineKeyboardButton(text="📋 Тапсырыстар",
                                 callback_data="driver_available_orders")
        ],
        [
            InlineKeyboardButton(text="✅ Сапарды аяқтау",
                                 callback_data="driver_complete_trip")
        ],
        [
            InlineKeyboardButton(text="🔄 Бағытты өзгерту",
                                 callback_data="driver_change_direction")
        ],
        [InlineKeyboardButton(text="🔙 Меню", callback_data="back_main")]
    ])

    await message.answer(
        f"🚗 <b>Жүргізуші профилі</b>\n\n"
        f"👤 {driver[full_name_idx]}\n"
        f"🚗 {driver[car_model_idx]} ({driver[car_number_idx]})\n"
        f"{seats_text}"
        f"📍 Бағыт: {driver[direction_idx]}\n"
        f"{rating_text}\n\n"
        "Сіз өз бағытыңыз бойынша тапсырыстарды көре аласыз",
        reply_markup=keyboard,
        parse_mode="HTML")


@dp.callback_query(F.data == "driver_status")
async def driver_status(callback: types.CallbackQuery):
    async with get_db() as db:
        async with db.execute("SELECT * FROM drivers WHERE user_id=?",
                              (callback.from_user.id, )) as cursor:
            driver = await cursor.fetchone()

        # Counting waiting orders on same direction
        async with db.execute(
                "SELECT COUNT(*) FROM clients WHERE direction=? AND status='waiting'",
            (driver[6], )) as cursor:
            waiting = (await cursor.fetchone())[0]

    occupied, total, available = await get_driver_available_seats(
        callback.from_user.id)

    await callback.message.edit_text(
        f"📊 <b>Статус</b>\n\n"
        f"🚗 {driver[4]} ({driver[3]})\n"
        f"📍 Бағыт: {driver[6]}\n"
        f"💺 Бос емес: {occupied}/{total}\n"
        f"💺 Бос орындар: {available}\n"
        f"⏳ Сіздің бағытыңыз бойынша тапсырыстар: {waiting}\n"
        f"{get_rating_stars(driver[13] if len(driver) > 13 else 0)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Артқа", callback_data="driver_menu")
        ]]),
        parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "driver_passengers")
async def driver_passengers(callback: types.CallbackQuery):
    async with get_db() as db:
        async with db.execute(
                '''SELECT user_id, full_name, from_city, to_city, passengers_count FROM clients WHERE assigned_driver_id=? AND status='accepted' ''',
            (callback.from_user.id, )) as cursor:
            clients = await cursor.fetchall()

    if not clients:
        msg = "❌ Жолаушылар жоқ"
    else:
        total_passengers = sum(c[4] for c in clients)
        msg = f"👥 <b>Менің жолаушыларым ({total_passengers} чел.):</b>\n\n"
        for i, client in enumerate(clients, 1):
            msg += f"{i}. {client[1]} ({client[4]} чел.)\n"
            msg += f"   📍 {client[2]} → {client[3]}\n\n"

    await callback.message.edit_text(
        msg,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Артқа", callback_data="driver_menu")
        ]]),
        parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "driver_available_orders")
async def driver_available_orders(callback: types.CallbackQuery):
    """Show available orders for the driver based on their direction"""
    # Answer immediately to prevent timeout
    await callback.answer("⏳ Жүктелуде...")
    
    try:
        async with get_db() as db:
            # Get driver's direction
            async with db.execute("SELECT direction FROM drivers WHERE user_id=?",
                                  (callback.from_user.id, )) as cursor:
                result = await cursor.fetchone()
                
            if not result:
                await callback.message.edit_text("❌ Жүргізуші табылмады")
                return
                
            driver_direction = result[0]
            logger.info(f"Driver {callback.from_user.id} direction: {driver_direction}")

        # Get available seats (outside the db context)
        occupied, total, available = await get_driver_available_seats(callback.from_user.id)
        logger.info(f"Driver seats: {occupied}/{total}, available: {available}")

        # Get waiting clients with same direction
        async with get_db() as db:
            async with db.execute(
                '''SELECT user_id, full_name, passengers_count, queue_position, 
                          direction, from_city, to_city
                   FROM clients
                   WHERE direction=? AND status='waiting'
                   ORDER BY queue_position''', (driver_direction,)) as cursor:
                clients = await cursor.fetchall()

        logger.info(f"Found {len(clients)} waiting clients")

        if not clients:
            msg = f"❌ Сіздің бағытыңыз бойынша тапсырыстар жоқ: {driver_direction}\n\n💺 Бос орындар: {available}"
            await callback.message.edit_text(
                msg,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔙 Артқа", callback_data="driver_menu")
                ]]),
                parse_mode="HTML")
            return

        msg = f"🔔 <b>{driver_direction} бағыты бойынша тапсырыстар:</b>\n"
        msg += f"💺 Бос орындар: {available}\n\n"

        keyboard_buttons = []
        for client in clients:
            can_fit = client[2] <= available
            fit_emoji = "✅" if can_fit else "⚠️"
            warning = "" if can_fit else " (орын жетпейді!)"

            msg += f"{fit_emoji} №{client[3]} - {client[1]} ({client[2]} адам.){warning}\n"
            msg += f"   🎯 {client[4]}\n\n"

            button_text = f"✅ №{client[3]} алу ({client[2]} адам.)"
            if not can_fit:
                button_text = f"⚠️ №{client[3]} алу ({client[2]} адам.) - орын жетпейді!"

            keyboard_buttons.append([
                InlineKeyboardButton(text=button_text, callback_data=f"accept_client_{client[0]}")
            ])

        keyboard_buttons.append([
            InlineKeyboardButton(text="🔙 Артқа", callback_data="driver_menu")
        ])

        await callback.message.edit_text(
            msg,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons),
            parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Error in driver_available_orders: {e}", exc_info=True)
        await callback.message.answer(f"❌ Қате: {str(e)}")


@dp.callback_query(F.data.startswith("accept_client_"))
async def accept_client(callback: types.CallbackQuery):
    """Driver accepts a client"""
    client_id = int(callback.data.split("_")[2])
    driver_id = callback.from_user.id

    try:
        async with get_db() as db:
            # Get client data - remove order_for
            async with db.execute(
                '''SELECT passengers_count, full_name, direction, from_city, to_city, phone, parent_user_id
                   FROM clients 
                   WHERE user_id=? AND status='waiting' ''',
                (client_id,)) as cursor:
                client = await cursor.fetchone()

            if not client:
                await callback.answer("❌ Клиентті басқа жүргізуші алып қойды!", show_alert=True)
                return

            passengers_count = client[0]
            client_name = client[1]
            direction = client[2]
            from_city = client[3]
            to_city = client[4]
            client_phone = client[5] if client[5] and not client[5].startswith("tg_") else "Нөмір көрсетілмеген"
            parent_user_id = client[6] if len(client) > 6 else client_id

            # Check driver's available seats
            async with db.execute(
                "SELECT total_seats, COALESCE(occupied_seats, 0), car_model, car_number, phone FROM drivers WHERE user_id=?",
                (driver_id,)) as cursor:
                driver_data = await cursor.fetchone()

            if not driver_data:
                await callback.answer("❌ Қате: жүргізуші жоқ", show_alert=True)
                return

            total, occupied, car_model, car_number, driver_phone = driver_data
            available = total - occupied

            if passengers_count > available:
                await callback.answer(
                    f"❌ Орын жетпейді! {passengers_count} орын қажет, {available} орын бар",
                    show_alert=True)
                return

        async with get_db(write=True) as db:
            await db.execute(
                "UPDATE clients SET status='accepted', assigned_driver_id=? WHERE user_id=?",
                (driver_id, client_id))

            await db.execute(
                '''UPDATE drivers 
                   SET occupied_seats = COALESCE(occupied_seats, 0) + ? 
                   WHERE user_id=?''', (passengers_count, driver_id))

            await db.execute(
                '''INSERT INTO trips (driver_id, client_id, direction, status, passengers_count)
                   VALUES (?, ?, ?, 'accepted', ?)''',
                (driver_id, client_id, direction, passengers_count))

        await save_log_action(driver_id, "client_accepted", f"Client: {client_id}")

        # Notify client
        try:
            await bot.send_message(
                client_id, 
                f"✅ <b>Жүргізуші тапсырысыңызды қабылдады!</b>\n\n"
                f"🚗 {car_model} ({car_number})\n"
                f"📍 {from_city} → {to_city}\n\n"
                f"📞 Жүргізуші байланысы: {driver_phone}\n\n"
                f"Жүргізушінің қоңырауын күтіңіз!",
                parse_mode="HTML")
        except Exception as e:
            logger.warning(f"Couldn't notify client {client_id}: {e}")

        # Notify driver
        await callback.message.edit_text(
            f"✅ <b>Тапсырыс қабылданды!</b>\n\n"
            f"👤 Жолаушы: {client_name}\n"
            f"📞 Байланыс: {client_phone}\n"
            f"📍 {from_city} → {to_city}\n"
            f"👥 Орын: {passengers_count}",
            parse_mode="HTML")

        await callback.answer(f"✅ Клиент {client_name} қосылды!", show_alert=True)

    except Exception as e:
        logger.error(f"Error in accept_client: {e}", exc_info=True)
        await callback.answer("❌ Қате. Тағы бір рет көріңіз.", show_alert=True)


@dp.callback_query(F.data == "driver_change_direction")
async def driver_change_direction(callback: types.CallbackQuery):
    """Driver changes direction"""
    async with get_db() as db:
        # Check active trips
        async with db.execute(
                '''SELECT COUNT(*) FROM clients 
                     WHERE assigned_driver_id=? AND status IN ('accepted', 'driver_arrived')''',
            (callback.from_user.id, )) as cursor:
            active_trips = (await cursor.fetchone())[0]

        if active_trips > 0:
            await callback.answer(
                "❌ Бағытты өзгерту мүмкін емес - белсенді сапарлар бар!",
                show_alert=True)
            return

    await callback.message.edit_text(
        "📍 <b>Бағытты өзгерту</b>\n\n"
        "Жаңа бағытты таңдаңыз:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Ақтау → Жаңаөзен", callback_data="change_dir_aktau_janaozen")],
            [InlineKeyboardButton(text="Жаңаөзен → Ақтау", callback_data="change_dir_janaozen_aktau")],
            [InlineKeyboardButton(text="Ақтау → Шетпе", callback_data="change_dir_aktau_shetpe")],
            [InlineKeyboardButton(text="Шетпе → Ақтау", callback_data="change_dir_shetpe_aktau")],
            [InlineKeyboardButton(text="🔙 Артқа", callback_data="driver_menu")]
        ]),
        parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data.startswith("change_dir_"))
async def confirm_change_direction(callback: types.CallbackQuery):
    """Verify and change driver's direction"""
    direction_map = {
        "change_dir_aktau_janaozen": "Ақтау → Жаңаөзен",
        "change_dir_janaozen_aktau": "Жаңаөзен → Ақтау",
        "change_dir_aktau_shetpe": "Ақтау → Шетпе",
        "change_dir_shetpe_aktau": "Шетпе → Ақтау"
    }

    new_direction = direction_map[callback.data]

    async with get_db(write=True) as db:
        # Get old direction
        async with db.execute("SELECT direction FROM drivers WHERE user_id=?",
                              (callback.from_user.id, )) as cursor:
            old_direction = (await cursor.fetchone())[0]

        # Update driver direction
        await db.execute(
            "UPDATE drivers SET direction=?, queue_position=0 WHERE user_id=?",
            (new_direction, callback.from_user.id))

        # Reorder queues for both old and new directions
        for direction in [old_direction, new_direction]:
            async with db.execute(
                    "SELECT user_id FROM drivers WHERE direction=? ORDER BY queue_position",
                (direction, )) as cursor:
                drivers = await cursor.fetchall()

            for pos, (driver_id, ) in enumerate(drivers, 1):
                await db.execute(
                    "UPDATE drivers SET queue_position=? WHERE user_id=?",
                    (pos, driver_id))

    await save_log_action(callback.from_user.id, "direction_changed",
                          f"New direction: {new_direction}")

    await callback.message.edit_text(
        f"✅ <b>Бағыт өзгертілді!</b>\n\n"
        f"📍 Жаңа бағыт: {new_direction}\n\n"
        f"Енді сіз {new_direction} бағыты бойынша тапсырыстарды көре аласыз",
        parse_mode="HTML")

    await asyncio.sleep(2)
    await show_driver_menu(callback.message, callback.from_user.id)
    await callback.answer()


@dp.callback_query(F.data == "driver_complete_trip")
async def driver_complete_trip(callback: types.CallbackQuery):
    """Driver completes the trip"""
    async with get_db(write=True) as db:
        # Get all clients in the trip
        async with db.execute(
                '''SELECT user_id, passengers_count, full_name, parent_user_id
                     FROM clients 
                     WHERE assigned_driver_id=? AND status IN ('accepted', 'driver_arrived')''',
            (callback.from_user.id, )) as cursor:
            clients = await cursor.fetchall()

        if not clients:
            await callback.answer("❌ Белсенді сапар жоқ!", show_alert=True)
            return

        total_freed = sum(c[1] for c in clients)

        # Get trip IDs before completing
        async with db.execute(
            '''SELECT id FROM trips 
               WHERE driver_id=? AND status IN ('accepted', 'driver_arrived')''',
            (callback.from_user.id,)) as cursor:
            trip_ids = await cursor.fetchall()

        # End trips
        await db.execute(
            '''UPDATE trips 
                     SET status='completed', trip_completed_at=CURRENT_TIMESTAMP 
                     WHERE driver_id=? AND status IN ('accepted', 'driver_arrived')''',
            (callback.from_user.id, ))

        # Delete clients from active trips
        await db.execute(
            '''DELETE FROM clients 
                     WHERE assigned_driver_id=? AND status IN ('accepted', 'driver_arrived')''',
            (callback.from_user.id, ))

        # Free up occupied seats
        await db.execute(
            '''UPDATE drivers 
                     SET occupied_seats = COALESCE(occupied_seats, 0) - ? 
                     WHERE user_id=?''', (total_freed, callback.from_user.id))

    await save_log_action(callback.from_user.id, "trip_completed",
                          f"Freed {total_freed} seats")

    # Notify clients with rating buttons
    for client in clients:
        client_user_id = client[0]
        parent_user_id = client[3] if len(client) > 3 else client_user_id
        
        # Notify the actual passenger (if it's a sub-order)
        if client_user_id != parent_user_id:
            try:
                await bot.send_message(
                    client_user_id,
                    f"✅ <b>Сапар аяқталды!</b>\n\n"
                    f"Рақмет, жолаушы!",
                    parse_mode="HTML")
            except:
                pass
        
        # Notify parent user (who made the order) with rating option
        if trip_ids:
            trip_id = trip_ids[0][0]  # Get first trip ID
            
            rating_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⭐", callback_data=f"quick_rate_{trip_id}_1")],
                [InlineKeyboardButton(text="⭐⭐", callback_data=f"quick_rate_{trip_id}_2")],
                [InlineKeyboardButton(text="⭐⭐⭐", callback_data=f"quick_rate_{trip_id}_3")],
                [InlineKeyboardButton(text="⭐⭐⭐⭐", callback_data=f"quick_rate_{trip_id}_4")],
                [InlineKeyboardButton(text="⭐⭐⭐⭐⭐", callback_data=f"quick_rate_{trip_id}_5")],
                [InlineKeyboardButton(text="❌ Кейінірек", callback_data="rate_later")]
            ])
            
            try:
                await bot.send_message(
                    parent_user_id,
                    f"✅ <b>Сапар аяқталды!</b>\n\n"
                    f"Жүргізушіге баға беріңіз:",
                    reply_markup=rating_keyboard,
                    parse_mode="HTML")
            except Exception as e:
                logger.warning(f"Couldn't notify client {parent_user_id}: {e}")

    await callback.answer(f"✅ Сапар аяқталды! {total_freed} орын босады",
                          show_alert=True)
    await show_driver_menu(callback.message, callback.from_user.id)
    
@dp.callback_query(F.data.startswith("quick_rate_"))
async def quick_rate_handler(callback: types.CallbackQuery, state: FSMContext):
    """Handle quick rating from notification"""
    parts = callback.data.split("_")
    trip_id = int(parts[2])
    rating = int(parts[3])
    
    # Store trip_id and rating in state
    await state.update_data(trip_id=trip_id, rating=rating)
    
    # Ask if they want to leave a comment
    comment_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✍️ Пікір жазу", callback_data="add_comment")],
        [InlineKeyboardButton(text="✅ Пікірсіз жіберу", callback_data="skip_comment")]
    ])
    
    await callback.message.edit_text(
        f"{'⭐' * rating}\n\n"
        f"Пікір қалдырғыңыз келе ме?",
        reply_markup=comment_keyboard)
    await callback.answer()


@dp.callback_query(F.data == "add_comment")
async def add_comment_prompt(callback: types.CallbackQuery, state: FSMContext):
    """Prompt user to write a comment"""
    data = await state.get_data()
    rating = data.get('rating', 5)
    
    await callback.message.edit_text(
        f"{'⭐' * rating}\n\n"
        f"Пікіріңізді жазыңыз:")
    await state.set_state(RatingStates.write_review)
    await callback.answer()


@dp.callback_query(F.data == "skip_comment")
async def skip_comment_handler(callback: types.CallbackQuery, state: FSMContext):
    """Submit rating without comment"""
    data = await state.get_data()
    trip_id = data.get('trip_id')
    rating = data.get('rating', 5)
    
    await save_rating_to_db(callback.from_user.id, trip_id, rating, None)
    
    await callback.message.edit_text(
        f"✅ Рақмет сіздің бағаңызға!\n\n"
        f"{'⭐' * rating}")
    await state.clear()
    await callback.answer("Баға сақталды!")


@dp.callback_query(F.data == "rate_later")
async def rate_later_handler(callback: types.CallbackQuery):
    """User chooses to rate later"""
    await callback.message.edit_text(
        "👌 Кейінірек баға беруге болады:\n"
        "/rate командасын пайдаланыңыз")
    await callback.answer()


async def save_rating_to_db(user_id: int, trip_id: int, rating: int, review: str = None):
    """Helper function to save rating to database"""
    async with get_db(write=True) as db:
        # Get trip details
        async with db.execute(
            '''SELECT driver_id, client_id FROM trips WHERE id=?''',
            (trip_id,)) as cursor:
            trip = await cursor.fetchone()
        
        if not trip:
            logger.error(f"Trip {trip_id} not found")
            return
        
        is_driver = trip[0] == user_id
        target_id = trip[1] if is_driver else trip[0]
        user_type = "driver" if not is_driver else "client"
        
        # Check if rating already exists
        async with db.execute(
            '''SELECT id FROM ratings 
               WHERE from_user_id=? AND trip_id=?''',
            (user_id, trip_id)) as cursor:
            existing = await cursor.fetchone()
        
        if existing:
            logger.warning(f"Rating already exists for trip {trip_id} from user {user_id}")
            return
        
        # Insert rating
        await db.execute(
            '''INSERT INTO ratings (from_user_id, to_user_id, user_type, trip_id, rating, review)
               VALUES (?, ?, ?, ?, ?, ?)''',
            (user_id, target_id, user_type, trip_id, rating, review))
        
        # Update average rating
        table = "drivers" if user_type == "driver" else "clients"
        await db.execute(
            f'''UPDATE {table} 
                SET avg_rating = (SELECT AVG(rating) FROM ratings WHERE to_user_id=?),
                    rating_count = (SELECT COUNT(*) FROM ratings WHERE to_user_id=?)
                WHERE user_id=?''', (target_id, target_id, target_id))
    
    await save_log_action(user_id, "rating_submitted", f"Target: {target_id}, Rating: {rating}")

@dp.callback_query(F.data == "driver_menu")
async def driver_menu_back(callback: types.CallbackQuery):
    await show_driver_menu(callback.message, callback.from_user.id)
    await callback.answer()


@dp.callback_query(F.data.startswith("driver_accept_"))
async def driver_accept_new_order(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    # data format: driver_accept_<client_id>_<from_city>_<to_city>_<count>
    if len(parts) < 6:
        await callback.answer("Қате дерек!", show_alert=True)
        return

    client_id = int(parts[2])
    from_city = parts[3]
    to_city = parts[4]
    passengers_count = int(parts[5])
    driver_id = callback.from_user.id

    async with get_db(write=True) as db:
        # Ensure client still waiting
        async with db.execute(
                "SELECT c.parent_user_id, u.full_name, u.phone FROM clients c JOIN clients u ON u.user_id = c.parent_user_id WHERE c.parent_user_id=? AND c.from_city=? AND c.to_city=? AND c.status='waiting'",
            (client_id, from_city, to_city)) as cursor:
            client = await cursor.fetchone()

        if not client:
            await callback.answer("❌ Бұл тапсырыс енді қолжетімді емес",
                                  show_alert=True)
            return

        # Assign driver
        await db.execute(
            "UPDATE clients SET status='accepted', assigned_driver_id=? WHERE parent_user_id=? AND status='waiting'",
            (driver_id, client_id))

        # Update occupied seats
        await db.execute(
            "UPDATE drivers SET occupied_seats = COALESCE(occupied_seats, 0) + ? WHERE user_id=?",
            (passengers_count, driver_id))

        # Get driver info
        async with db.execute(
            "SELECT full_name, phone, car_model, car_number FROM drivers WHERE user_id=?",
            (driver_id, )) as cursor:
            driver_data = await cursor.fetchone()

    # ====== Notify both sides ======
    client_user_id = client[0]
    client_name = client[1]
    client_phone = client[2] if client[2] and not client[2].startswith(
        "tg_") else "Нөмір көрсетілмеген"
    driver_name, driver_phone, car_model, car_number = driver_data

    # ✅ Notify driver
    await callback.message.edit_text(
        f"✅ <b>Сіз тапсырысты қабылдадыңыз!</b>\n\n"
        f"📍 {from_city} → {to_city}\n"
        f"👥 Жолаушылар саны: {passengers_count}\n\n"
        f"👤 <b>Жолаушы:</b> {client_name}\n"
        f"📞 Телефон: {client_phone}",
        parse_mode="HTML")

    # ✅ Notify client
    try:
        await bot.send_message(
            client_user_id, f"✅ <b>Жүргізуші тапсырысыңызды қабылдады!</b>\n\n"
            f"🚗 {car_model} ({car_number})\n"
            f"👤 {driver_name}\n"
            f"📞 Телефон: {driver_phone}\n"
            f"📍 Маршрут: {from_city} → {to_city}\n\n"
            f"Жүргізушінің қоңырауын күтіңіз немесе өзіңіз хабарласа аласыз.",
            parse_mode="HTML")
    except Exception as e:
        logger.warning(f"Couldn't notify client {client_user_id}: {e}")

    await callback.answer("Тапсырыс қабылданды!")


@dp.callback_query(F.data.startswith("driver_reject_"))
async def driver_reject_new_order(callback: types.CallbackQuery):
    await callback.message.edit_text("❌ Сіз бұл тапсырыстан бас тарттыңыз.")
    await callback.answer("Тапсырыс қабылданбады")


# ==================== CLIENTS ====================


@dp.message(F.text == "🧍‍♂️ Такси шақыру")
async def client_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    # Clear any previous state
    await state.clear()

    # Check if client profile exists
    async with get_db() as db:
        async with db.execute(
                "SELECT user_id, full_name, phone FROM clients WHERE user_id=? AND status='registered'",
            (user_id,)) as cursor:
            client = await cursor.fetchone()

    if client:
        # Client already registered, show menu
        await show_client_menu(message, user_id)
    else:
        # New client, need verification
        full_name = message.from_user.full_name
        username = message.from_user.username

        await state.update_data(telegram_id=user_id,
                                full_name=full_name,
                                username=username or "",
                                verified_by='telegram',
                                is_verified=True)

        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Бәрі дұрыс",
                                 callback_data="confirm_client_telegram_data")
        ]])

        await message.answer(
            f"👤 <b>Сіздің Telegram деректеріңіз:</b>\n\n"
            f"Аты: {full_name}\n"
            f"Username: @{username if username else 'орнатылмаған'}\n"
            f"ID: <code>{user_id}</code>",
            reply_markup=keyboard,
            parse_mode="HTML")
        await state.set_state(ClientOrder.confirm_data)


async def start_new_order(message: types.Message, state: FSMContext):
    """Helper function to start a new order"""
    # Use the same direction keyboard as drivers
    direction_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Ақтау → Жаңаөзен", callback_data="dir_aktau_janaozen")],
        [InlineKeyboardButton(text="Жаңаөзен → Ақтау", callback_data="dir_janaozen_aktau")],
        [InlineKeyboardButton(text="Ақтау → Шетпе", callback_data="dir_aktau_shetpe")],
        [InlineKeyboardButton(text="Шетпе → Ақтау", callback_data="dir_shetpe_aktau")],
        [InlineKeyboardButton(text="🔙 Артқа", callback_data="back_main")]
    ])

    await message.answer(
        "🧍‍♂️ <b>Такси шақыру</b>\n\nБағытты таңдаңыз:",
        reply_markup=direction_keyboard,
        parse_mode="HTML")
    await state.set_state(ClientOrder.from_city)



@dp.callback_query(ClientOrder.confirm_data,
                   F.data == "confirm_client_telegram_data")
async def confirm_client_telegram_data(callback: types.CallbackQuery,
                                       state: FSMContext):
    await callback.message.edit_text("✅ Керемет! Тіркеуді жалғастырамыз...")
    await callback.message.answer(
        "📱 Телефон нөміріңізді енгізіңіз (мысалы: +7 777 123 45 67):")
    await state.set_state(ClientOrder.phone_number)
    await callback.answer()


@dp.message(ClientOrder.phone_number)
async def client_phone_number(message: types.Message, state: FSMContext):
    data = await state.get_data()
    phone = message.text.strip()
    
    # Сохраняем телефон в state
    await state.update_data(phone_number=phone)

    # Создаем профиль клиента СРАЗУ
    async with get_db(write=True) as db:
        await db.execute(
          '''INSERT OR IGNORE INTO clients
            (user_id, full_name, phone, direction, queue_position,
             passengers_count, is_verified, status, from_city, to_city)
             VALUES (?, ?, ?, '', 0, 1, 1, 'registered', '', '')''',
          (message.from_user.id,
           data.get('full_name', message.from_user.full_name or "Клиент"),
           phone, ))

    await save_log_action(message.from_user.id, "client_registered",
                          f"Phone: {phone}")

    await message.answer(
        "✅ <b>Тіркелу аяқталды!</b>\n\n"
        "💡 Кеңес: Клиент мәзіріне тез өту үшін /client командасын пайдаланыңыз",
        parse_mode="HTML")
    
    # Показываем меню клиента сразу
    await show_client_menu(message, message.from_user.id)
    await state.clear()


@dp.callback_query(F.data == "add_new_order")
async def add_new_order(callback: types.CallbackQuery, state: FSMContext):
    """Add new taxi order"""
    direction_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Ақтау → Жаңаөзен", callback_data="dir_aktau_janaozen")],
        [InlineKeyboardButton(text="Жаңаөзен → Ақтау", callback_data="dir_janaozen_aktau")],
        [InlineKeyboardButton(text="Ақтау → Шетпе", callback_data="dir_aktau_shetpe")],
        [InlineKeyboardButton(text="Шетпе → Ақтау", callback_data="dir_shetpe_aktau")],
        [InlineKeyboardButton(text="🔙 Артқа", callback_data="back_main")]
    ])
    
    await callback.message.edit_text(
        "🧍‍♂️ <b>Жаңа такси шақыру</b>\n\nБағытты таңдаңыз:",
        reply_markup=direction_keyboard,
        parse_mode="HTML")
    await state.set_state(ClientOrder.from_city)
    await callback.answer()



@dp.callback_query(F.data == "view_my_orders")
async def view_my_orders(callback: types.CallbackQuery):
    """Show user's active orders"""
    active_orders = await get_user_active_orders(callback.from_user.id)

    if not active_orders:
        await callback.answer("❌ Белсенді тапсырыстар жоқ", show_alert=True)
        return

    msg = "🚖 <b>Сіздің белсенді тапсырыстарыңыз:</b>\n\n"
    keyboard_buttons = []

    for order in active_orders:
        status_emoji = {'waiting': '⏳', 'accepted': '✅', 'driver_arrived': '🚗'}
        emoji = status_emoji.get(order[4], '❓')

        msg += f"{emoji} <b>Тапсырыс #{order[3]}</b>\n"
        msg += f"   👥 {order[6]} адам | 📍 {order[5]}\n"

        keyboard_buttons.append([
            InlineKeyboardButton(
                text=f"❌ Тапсырысты жою #{order[3]}",
                callback_data=f"cancel_order_{order[0]}")
        ])

    keyboard_buttons.append(
        [InlineKeyboardButton(text="🔙 Артқа", callback_data="back_main")])

    await callback.message.edit_text(
        msg,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons),
        parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data.startswith("cancel_order_"))
async def cancel_specific_order(callback: types.CallbackQuery):
    """Cancel a specific order"""
    order_user_id = int(callback.data.split("_")[2])
    parent_user_id = callback.from_user.id

    try:
        # First, get client data in read-only mode
        async with get_db() as db:
            # Get column names
            async with db.execute("PRAGMA table_info(clients)") as cursor:
                columns = [col[1] for col in await cursor.fetchall()]
            
            # Get client data
            async with db.execute("SELECT * FROM clients WHERE user_id=?",
                                  (order_user_id, )) as cursor:
                client = await cursor.fetchone()

        if not client:
            await callback.answer("❌ Тапсырыс табылмады", show_alert=True)
            return

        # Get column indices
        user_id_idx = columns.index('user_id')
        parent_user_id_idx = columns.index('parent_user_id') if 'parent_user_id' in columns else user_id_idx
        assigned_driver_id_idx = columns.index('assigned_driver_id') if 'assigned_driver_id' in columns else None
        passengers_count_idx = columns.index('passengers_count')
        direction_idx = columns.index('direction')
        order_number_idx = columns.index('order_number') if 'order_number' in columns else None
        full_name_idx = columns.index('full_name')

        # Check if this order belongs to the user
        order_owner = client[parent_user_id_idx] if parent_user_id_idx and len(client) > parent_user_id_idx else client[user_id_idx]
        
        if order_owner != parent_user_id:
            await callback.answer("❌ Бұл сіздің тапсырысыңыз емес!", show_alert=True)
            return

        # Extract order details
        driver_id = client[assigned_driver_id_idx] if assigned_driver_id_idx and len(client) > assigned_driver_id_idx else None
        passengers_count = client[passengers_count_idx]
        direction = client[direction_idx]
        order_number = client[order_number_idx] if order_number_idx and len(client) > order_number_idx else 1
        client_name = client[full_name_idx]

        # Get current cancellation count
        cancellation_count = await get_cancellation_count(parent_user_id)
        new_count = cancellation_count + 1

        # Now perform write operations
        async with get_db(write=True) as db:
            # If assigned to a driver, free up seats
            if driver_id:
                await db.execute(
                    '''UPDATE drivers 
                       SET occupied_seats = COALESCE(occupied_seats, 0) - ? 
                       WHERE user_id=?''', (passengers_count, driver_id))

                # Notify driver
                try:
                    await bot.send_message(
                        driver_id, 
                        f"⚠️ <b>Клиент тапсырысты жойды</b>\n\n"
                        f"👤 {client_name}\n"
                        f"👥 Босатылған орындар: {passengers_count}",
                        parse_mode="HTML")
                except Exception as e:
                    logger.warning(f"Couldn't notify driver {driver_id}: {e}")

            # Update trip status
            await db.execute(
                '''UPDATE trips SET status='cancelled', cancelled_by='client', 
                   cancelled_at=CURRENT_TIMESTAMP 
                   WHERE client_id=? AND status IN ('waiting', 'accepted', 'driver_arrived')''',
                (order_user_id, ))

            # Update cancellation count BEFORE deleting
            await db.execute(
                '''UPDATE clients SET cancellation_count=? 
                   WHERE user_id=? OR parent_user_id=?''',
                (new_count, parent_user_id, parent_user_id))

            # Delete the client order
            await db.execute("DELETE FROM clients WHERE user_id=?",
                             (order_user_id, ))

            # Reorder queue positions
            async with db.execute(
                '''SELECT user_id FROM clients 
                   WHERE direction=? AND status='waiting'
                   ORDER BY queue_position''',
                (direction, )) as cursor:
                clients = await cursor.fetchall()

            for pos, (client_id,) in enumerate(clients, 1):
                await db.execute(
                    "UPDATE clients SET queue_position=? WHERE user_id=?",
                    (pos, client_id))

        await save_log_action(parent_user_id, "order_cancelled",
                              f"Order #{order_number}, Cancellation #{new_count}")

        # Blocking logic
        if new_count == 1:
            await callback.answer(
                "⚠️ ЕСКЕРТУ! Екінші тапсырыс жойылған жағдайда, сіз бұғатталасыз!",
                show_alert=True)
            
            # Show remaining orders
            remaining_orders = await count_user_orders(parent_user_id)
            if remaining_orders > 0:
                await view_my_orders(callback)
            else:
                await callback.message.edit_text(
                    "✅ <b>Тапсырыс жойылды</b>\n\n"
                    "⚠️ Бұл сіздің бірінші жойылған тапсырысыңыз.\n"
                    "Екінші жойылған тапсырыста бұғатталасыз!\n\n"
                    "Сіздің белсенді тапсырыстарыңыз жоқ.",
                    parse_mode="HTML")
                    
        elif new_count >= 2:
            reason = f"Тапсырыстарды жиі жою: ({new_count} рет)"
            await add_to_blacklist(parent_user_id, reason, new_count)

            await callback.message.edit_text(
                "🚫 <b>СІЗ БҰҒАТТАЛДЫҢЫЗ</b>\n\n"
                f"Себеп: {reason}\n\n"
                "Бұғаттан шығу үшін админге хабарласыңыз.",
                parse_mode="HTML")
        else:
            # First cancellation (new_count == 0 shouldn't happen, but just in case)
            remaining_orders = await count_user_orders(parent_user_id)
            if remaining_orders > 0:
                await view_my_orders(callback)
            else:
                await callback.message.edit_text(
                    "✅ <b>Тапсырыс жойылды</b>\n\n"
                    "Сіздің белсенді тапсырыстарыңыз жоқ.",
                    parse_mode="HTML")
    
    except Exception as e:
        logger.error(f"Error in cancel_specific_order: {e}", exc_info=True)
        await callback.answer("❌ Қате орын алды. Қайта көріңіз немесе админге хабарласыңыз.", show_alert=True)


@dp.callback_query(ClientOrder.from_city, F.data.startswith("dir_"))
async def client_from_city(callback: types.CallbackQuery, state: FSMContext):
    direction_map = {
        "dir_aktau_janaozen": "Ақтау → Жаңаөзен",
        "dir_janaozen_aktau": "Жаңаөзен → Ақтау",
        "dir_aktau_shetpe": "Ақтау → Шетпе",
        "dir_shetpe_aktau": "Шетпе → Ақтау"
    }
    
    direction = direction_map[callback.data]
    from_city, to_city = direction.split(" → ")
    
    await state.update_data(from_city=from_city, to_city=to_city, direction=direction)

    # Show available drivers and seats
    async with get_db() as db:
        async with db.execute(
            '''SELECT COUNT(*), SUM(total_seats - COALESCE(occupied_seats, 0))
               FROM drivers 
               WHERE direction=? AND is_active=1''',
            (direction,)) as cursor:
            result = await cursor.fetchone()

    drivers_count = result[0] or 0
    available_seats = result[1] or 0

    # Create seat selection buttons (1-7)
    seat_buttons = []
    for i in range(1, 8):  # Changed from 9 to 8
        seat_buttons.append([
            InlineKeyboardButton(text=f"👥 {i} орын", callback_data=f"seats_{i}")
        ])
    
    seat_buttons.append([
        InlineKeyboardButton(text="🔙 Артқа", callback_data="back_from_city")
    ])

    await callback.message.edit_text(
        f"✅ Маршрут: {direction}\n\n"
        f"🚗 Бос жүргізушілер: {drivers_count}\n"
        f"💺 Бос орындар: {available_seats}\n\n"
        f"👥 Қанша орын керек?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=seat_buttons))
    await state.set_state(ClientOrder.passengers_count)
    await callback.answer()

# Add new callback handler for seat buttons
@dp.callback_query(ClientOrder.passengers_count, F.data.startswith("seats_"))
async def client_select_seats(callback: types.CallbackQuery, state: FSMContext):
    count = int(callback.data.split("_")[1])
    data = await state.get_data()
    from_city = data.get("from_city")
    to_city = data.get("to_city")
    direction = data.get("direction")

    # Calculate price
    if {"Ақтау", "Шетпе"} == {from_city, to_city}:
        price = 2000 * count
    elif {"Ақтау", "Жаңаөзен"} == {from_city, to_city}:
        price = 2500 * count
    else:
        price = 0

    # ✅ Save passengers_count WITHOUT order_for
    await state.update_data(passengers_count=count)
    
    # Check suitable drivers
    async with get_db() as db:
        async with db.execute(
            '''SELECT COUNT(*) FROM drivers 
               WHERE direction=? AND is_active=1 
               AND (total_seats - COALESCE(occupied_seats, 0)) >= ?''',
            (direction, count)) as cursor:
            suitable_cars = (await cursor.fetchone())[0]

    if suitable_cars == 0:
        msg = (f"⚠️ Жолаушылар саны: {count}\n"
               f"💰 Баға: {price} теңге\n"
               f"⚠️ Қазір {count} бос орны бар көліктер жоқ\n\n"
               f"Бірақ сіздің тапсырысыңыз сақталады!\n"
               f"Жүргізушілер оны бос орындар пайда болғанда көретін болады.\n\n"
               f"Тапсырысты растайсыз ба?")
    else:
        msg = (f"✅ Жолаушылар саны: {count}\n"
               f"💰 Баға: {price} теңге\n"
               f"🚗 Бос жүргізушілер: {suitable_cars}\n\n"
               f"Тапсырысты растайсыз ба?")
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Растау", callback_data="confirm_order")],
        [InlineKeyboardButton(text="🔙 Артқа", callback_data="back_from_city")]
    ])
    
    await callback.message.edit_text(msg, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data == "back_from_city")
async def back_from_city(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Қай қаладан шығасыз?",
                                     reply_markup=from_city_keyboard())
    await state.set_state(ClientOrder.from_city)
    await callback.answer()

async with get_db(write=True) as db:
    # Set queue position
    async with db.execute(
            "SELECT MAX(queue_position) FROM clients WHERE direction=?",
        (direction, )) as cursor:
        max_pos = (await cursor.fetchone())[0]

    queue_pos = (max_pos or 0) + 1

    # Get client profile
    async with db.execute(
            "SELECT full_name, phone FROM clients WHERE user_id=? AND status='registered'",
        (callback.from_user.id,)) as cursor:
        profile = await cursor.fetchone()
    
    if not profile:
        await callback.answer("❌ Ошибка: профиль не найден. Попробуйте /start", show_alert=True)
        await state.clear()
        return
    
    client_name, client_phone = profile

    # Create new order entry (separate from profile)
    await db.execute(
        '''INSERT INTO clients 
        (user_id, full_name, phone, direction, from_city, to_city, 
         queue_position, passengers_count, 
         is_verified, status, order_number, parent_user_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 'waiting', ?, ?)''',
        (order_user_id,  # unique ID for this order
         client_name,
         client_phone,
         direction, from_city, to_city,
         queue_pos, data['passengers_count'], 
         order_number, callback.from_user.id))  # parent_user_id = actual user

        # Check suitable drivers
        async with db.execute(
            '''SELECT COUNT(*) FROM drivers 
               WHERE direction=? AND is_active=1 
               AND (total_seats - COALESCE(occupied_seats, 0)) >= ?''',
            (direction, data['passengers_count'])) as cursor:
            suitable = (await cursor.fetchone())[0]

        # Get drivers to notify
        async with db.execute(
                '''SELECT user_id FROM drivers 
               WHERE direction=? AND is_active=1 
               AND (total_seats - COALESCE(occupied_seats, 0)) >= ?''',
            (direction, data['passengers_count'])) as cursor:
            drivers = await cursor.fetchall()

    await save_log_action(callback.from_user.id, "order_created",
                          f"Order #{order_number}")

    # Notify drivers
    for driver in drivers:
        try:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="✅ Қабылдау",
                    callback_data=f"accept_client_{order_user_id}"),  # use order_user_id
                InlineKeyboardButton(
                    text="❌ Бас тарту",
                    callback_data=f"driver_reject_{order_user_id}")  # use order_user_id
            ]])

            await bot.send_message(
                driver[0], f"🔔 <b>Жаңа тапсырыс!</b>\n\n"
                f"👥 Жолаушылар саны: {data['passengers_count']}\n"
                f"📍 {from_city} → {to_city}\n\n"
                f"Төмендегі батырмалардың бірін таңдаңыз:",
                reply_markup=keyboard,
                parse_mode="HTML")

        except:
            pass

    # Offer to add another order
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="➕ Жаңа тапсырыс жасау",
                             callback_data="add_another_yes")
    ], [InlineKeyboardButton(text="✅ Аяқтау", callback_data="add_another_no")]
                                                     ])

    await callback.message.edit_text(
        f"✅ <b>Тапсырыс #{order_number} жасалды!</b>\n\n"
        f"📍 {from_city} → {to_city}\n"
        f"👥 Жолаушылар саны: {data['passengers_count']}\n"
        f"📊 Кезектегі орын: №{queue_pos}\n\n"
        f"🚗 Бос жүргізушілер: {suitable}\n\n"
        f"Тағы бір тапсырыс жасағыңыз келеді ме?",
        reply_markup=keyboard,
        parse_mode="HTML")
    await state.set_state(ClientOrder.add_another)
    
@dp.callback_query(F.data == "confirm_order")
async def confirm_order(callback: types.CallbackQuery, state: FSMContext):
    """Confirm and finalize order"""
    await callback.answer()
    await finalize_order(callback, state)

async def finalize_order_from_message(message: types.Message, state: FSMContext):
    """Same as finalize_order but from message context"""
    data = await state.get_data()
    
    from_city = data.get('from_city', '')
    to_city = data.get('to_city', '')
    
    current_orders = await count_user_orders(message.from_user.id)
    order_number = current_orders + 1
    
    order_user_id = int(f"{message.from_user.id}{int(time.time() * 1000) % 100000}")

    async with get_db(write=True) as db:

        direction = data.get(
            'direction',
            f"{data.get('from_city', '')} → {data.get('to_city', '')}")

        async with db.execute(
                "SELECT MAX(queue_position) FROM clients WHERE direction=?",
            (direction, )) as cursor:
            max_pos = (await cursor.fetchone())[0]

        queue_pos = (max_pos or 0) + 1
        
        # Get client profile data
        async with db.execute(
                "SELECT full_name, phone FROM clients WHERE user_id=? AND status='registered'",
            (message.from_user.id,)) as cursor:
            profile = await cursor.fetchone()
        
        if not profile:
            await message.answer("❌ Профиль табылмады! /start командасын пайдаланыңыз", show_alert=True)
            return
        
        client_name, client_phone = profile

        # Create new order entry (separate from profile)
        await db.execute(
            '''INSERT INTO clients 
            (user_id, full_name, phone, direction, from_city, to_city, 
             queue_position, passengers_count, 
             is_verified, status, order_number, parent_user_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 'waiting', ?, ?)''',
            (order_user_id,  # unique ID for this order
             client_name,
             client_phone,
             direction, from_city, to_city,
             queue_pos, data['passengers_count'],
             order_number, message.from_user.id))  # parent_user_id = actual user

        async with db.execute(
                '''SELECT COUNT(*) FROM drivers 
               WHERE direction=? AND is_active=1 
               AND (total_seats - COALESCE(occupied_seats, 0)) >= ?''',
            (direction, data['passengers_count'])) as cursor:
            suitable = (await cursor.fetchone())[0]

        async with db.execute(
                '''SELECT user_id FROM drivers 
               WHERE direction=? AND is_active=1 
               AND (total_seats - COALESCE(occupied_seats, 0)) >= ?''',
            (direction, data['passengers_count'])) as cursor:
            drivers = await cursor.fetchall()

    await save_log_action(message.from_user.id, "order_created",
                          f"Order #{order_number}")

    # Notify drivers
    for driver in drivers:
        try:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="✅ Қабылдау",
                    callback_data=f"accept_client_{order_user_id}"),
                InlineKeyboardButton(
                    text="❌ Бас тарту",
                    callback_data=f"driver_reject_{order_user_id}")
            ]])

            await bot.send_message(
                driver[0], f"🔔 <b>Жаңа тапсырыс!</b>\n\n"
                f"👥 Жолаушылар саны: {data['passengers_count']}\n"
                f"📍 {from_city} → {to_city}\n\n"
                f"Төмендегі батырмалардың бірін таңдаңыз:",
                reply_markup=keyboard,
                parse_mode="HTML")
        except Exception as e:
            logger.warning(f"Couldn't notify driver {driver[0]}: {e}")

    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="➕ Жаңа тапсырыс жасау",
                             callback_data="add_another_yes")
    ], [InlineKeyboardButton(text="✅ Аяқтау", callback_data="add_another_no")]
                                                     ])

    await message.answer(
        f"✅ <b>Тапсырыс #{order_number} жасалды!</b>\n\n"
        f"📍 {from_city} → {to_city}\n"
        f"👥 Жолаушылар саны: {data['passengers_count']}\n"
        f"📊 Кезектгі орын: №{queue_pos}\n\n"
        f"🚗 Бос жүргізушілер: {suitable}\n\n"
        f"Тағы бір тапсырыс жасағыңыз келеді ме?",
        reply_markup=keyboard,
        parse_mode="HTML")
    await state.set_state(ClientOrder.add_another)

@dp.callback_query(F.data == "add_another_yes")
async def add_another_order_callback(callback: types.CallbackQuery, state: FSMContext):
    """Add another taxi order"""
    direction_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Ақтау → Жаңаөзен", callback_data="dir_aktau_janaozen")],
        [InlineKeyboardButton(text="Жаңаөзен → Ақтау", callback_data="dir_janaozen_aktau")],
        [InlineKeyboardButton(text="Ақтау → Шетпе", callback_data="dir_aktau_shetpe")],
        [InlineKeyboardButton(text="Шетпе → Ақтау", callback_data="dir_shetpe_aktau")],
        [InlineKeyboardButton(text="🔙 Артқа", callback_data="back_main")]
    ])
    
    await callback.message.edit_text(
        "🧍‍♂️ <b>Жаңа тапсырыс</b>\n\nБағытты таңдаңыз:",
        reply_markup=direction_keyboard,
        parse_mode="HTML")
    await state.set_state(ClientOrder.from_city)
    await callback.answer()


@dp.callback_query(F.data == "add_another_no")
async def finish_ordering(callback: types.CallbackQuery, state: FSMContext):
    """End order process"""
    total_orders = await count_user_orders(callback.from_user.id)

    await callback.message.edit_text(
        f"✅ <b>Дайын!</b>\n\n"
        f"Сіздің {total_orders} белсенді тапсырысыңыз бар.\n\n"
        f"Статусты қарау үшін:\n"
        f"• Басты мәзірден 🧍‍♂️ Такси шақыру батырмасын басыңыз\n"
        f"• Содан кейін \"Менің тапсырыстарым\" таңдаңыз",
        parse_mode="HTML")
    await state.clear()
    await callback.answer()
    
@dp.message(Command("client"))
async def cmd_client(message: types.Message):
    """Client menu shortcut"""
    await show_client_menu(message, message.from_user.id)


async def show_client_menu(message: types.Message, user_id: int):
    """Show client profile and menu"""
    async with get_db() as db:
        async with db.execute(
            "SELECT full_name, phone, avg_rating, rating_count FROM clients WHERE user_id=? AND status='registered'",
            (user_id,)) as cursor:
            client = await cursor.fetchone()
    
    if not client:
        await message.answer("❌ Сіз клиент ретінде тіркелмегенсіз",
                           reply_markup=main_menu_keyboard())
        return
    
    # Get active orders count
    active_orders = await count_user_orders(user_id)
    
    # Get completed trips count
    async with get_db() as db:
        async with db.execute(
            "SELECT COUNT(*) FROM trips WHERE client_id=? AND status='completed'",
            (user_id,)) as cursor:
            completed = (await cursor.fetchone())[0]
    
    msg = f"🧍‍♂️ <b>Клиент профилі</b>\n\n"
    msg += f"👤 {client[0]}\n"
    msg += f"📞 {client[1]}\n"
    msg += f"{get_rating_stars(client[2] or 0)}\n"
    msg += f"📊 Бағалар: {client[3] or 0}\n"
    msg += f"✅ Аяқталған сапарлар: {completed}\n"
    msg += f"⏳ Белсенді тапсырыстар: {active_orders}\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Менің тапсырыстарым", callback_data="view_my_orders")],
        [InlineKeyboardButton(text="⭐ Бағалау", callback_data="rate_start")],
        [InlineKeyboardButton(text="➕ Жаңа тапсырыс", callback_data="add_new_order")],
        [InlineKeyboardButton(text="🔙 Меню", callback_data="back_main")]
    ])
    
    await message.answer(msg, reply_markup=keyboard, parse_mode="HTML")


# ==================== RATINGS ====================


@dp.message(F.text == "⭐ Профиль")
async def show_profile(message: types.Message):
    user_id = message.from_user.id
    
    async with get_db() as db:
        # Check if user is a driver
        async with db.execute(
                "SELECT user_id FROM drivers WHERE user_id=?",
            (user_id, )) as cursor:
            driver = await cursor.fetchone()

        # Check if user is a client - ИСПРАВЛЕНО ТУТ!
        async with db.execute(
                "SELECT user_id FROM clients WHERE user_id=? AND status='registered'",
            (user_id,)) as cursor:
            client = await cursor.fetchone()

    # No profile found
    if not driver and not client:
        await message.answer("❌ Сіздің профиліңіз табылмады.")
        return

    # Both profiles exist - ask which one to show
    if driver and client:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚗 Жүргізуші профилі", callback_data="profile_driver")],
            [InlineKeyboardButton(text="🧍‍♂️ Клиент профилі", callback_data="profile_client")]
        ])
        await message.answer(
            "👤 <b>Қай профильді көргіңіз келеді?</b>",
            reply_markup=keyboard,
            parse_mode="HTML")
        return

    # Only driver profile
    if driver:
        await show_driver_menu(message, user_id)
        return

    # Only client profile
    if client:
        await show_client_menu(message, user_id)
        return
    
@dp.callback_query(F.data == "profile_driver")
async def show_driver_profile(callback: types.CallbackQuery):
    """Show driver profile from profile selection"""
    await callback.message.delete()
    await show_driver_menu(callback.message, callback.from_user.id)
    await callback.answer()


@dp.callback_query(F.data == "profile_client")
async def show_client_profile(callback: types.CallbackQuery):
    """Show client profile from profile selection"""
    await callback.message.delete()
    await show_client_menu(callback.message, callback.from_user.id)
    await callback.answer()

@dp.callback_query(F.data == "rate_start")
async def rate_start(callback: types.CallbackQuery, state: FSMContext):
    async with get_db() as db:
        async with db.execute(
                '''SELECT t.id, t.driver_id, d.full_name, t.client_id
                     FROM trips t
                     JOIN drivers d ON t.driver_id = d.user_id
                     WHERE (t.driver_id=? OR t.client_id=?)
                     AND t.status='completed'
                     AND t.id NOT IN (SELECT trip_id FROM ratings WHERE from_user_id=? AND trip_id IS NOT NULL)
                     ORDER BY t.trip_completed_at DESC LIMIT 5''',
            (callback.from_user.id, callback.from_user.id,
             callback.from_user.id)) as cursor:
            trips = await cursor.fetchall()

    if not trips:
        await callback.answer("❌ Сапар табылмады", show_alert=True)
        return

    keyboard_buttons = []
    for trip in trips:
        is_driver = trip[1] == callback.from_user.id
        target_name = "Клиентті" if is_driver else f"Жүргізушіні {trip[2]}"
        keyboard_buttons.append([
            InlineKeyboardButton(text=f"бағалау {target_name}",
                                 callback_data=f"rate_trip_{trip[0]}")
        ])

    await callback.message.edit_text(
        "✍️ <b>Сапарды бағалауды таңдаңыз:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons),
        parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data.startswith("rate_trip_"))
async def rate_trip(callback: types.CallbackQuery, state: FSMContext):
    trip_id = int(callback.data.split("_")[2])

    await state.update_data(trip_id=trip_id)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐", callback_data="rating_1")],
        [InlineKeyboardButton(text="⭐⭐", callback_data="rating_2")],
        [InlineKeyboardButton(text="⭐⭐⭐", callback_data="rating_3")],
        [InlineKeyboardButton(text="⭐⭐⭐⭐", callback_data="rating_4")],
        [InlineKeyboardButton(text="⭐⭐⭐⭐⭐", callback_data="rating_5")]
    ])

    await callback.message.edit_text("⭐ Бағаны таңдаңыз:",
                                     reply_markup=keyboard)
    await state.set_state(RatingStates.select_rating)
    await callback.answer()


@dp.callback_query(RatingStates.select_rating, F.data.startswith("rating_"))
async def save_rating(callback: types.CallbackQuery, state: FSMContext):
    rating = int(callback.data.split("_")[1])
    await state.update_data(rating=rating)

    await callback.message.edit_text(
        f"{'⭐' * rating}\n\n"
        "Пікір қалдыру (немесе өткізіп жіберу үшін /skip):")
    await state.set_state(RatingStates.write_review)
    await callback.answer()


@dp.message(RatingStates.write_review)
async def save_review(message: types.Message, state: FSMContext):
    """Save rating with review"""
    data = await state.get_data()
    review = None if message.text == "/skip" else message.text
    trip_id = data.get('trip_id')
    rating = data.get('rating')
    
    if not trip_id or not rating:
        await message.answer("❌ Қате: баға деректері жоқ")
        await state.clear()
        return
    
    await save_rating_to_db(message.from_user.id, trip_id, rating, review)
    
    await message.answer(
        f"✅ Пікір қалдырғаныңызға рақмет!\n\n"
        f"{'⭐' * rating}\n"
        f"{f'💬 {review}' if review else ''}",
        reply_markup=main_menu_keyboard())
    await state.clear()


# ==================== GENERAL ====================


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await save_log_action(message.from_user.id, "bot_started", "")

    await message.answer(
        f"Сәлем, {message.from_user.first_name}! 👋\n\n"
        "🚖 <b>Такси Ақтау</b>\n\n"
        "Таңдаңыз:",
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML")


@dp.message(Command("driver"))
async def cmd_driver(message: types.Message):
    """Driver menu shortcut"""
    await show_driver_menu(message, message.from_user.id)


@dp.message(Command("rate"))
async def cmd_rate(message: types.Message, state: FSMContext):
    """Rate menu shortcut"""
    async with get_db() as db:
        # Find trips where user was either driver or client
        async with db.execute(
            '''SELECT t.id, t.driver_id, t.client_id, t.direction, t.trip_completed_at
               FROM trips t
               WHERE (t.driver_id=? OR t.client_id=?)
               AND t.status='completed'
               AND t.id NOT IN (SELECT trip_id FROM ratings WHERE from_user_id=? AND trip_id IS NOT NULL)
               ORDER BY t.trip_completed_at DESC LIMIT 5''',
            (message.from_user.id, message.from_user.id, message.from_user.id)) as cursor:
            trips = await cursor.fetchall()

    if not trips:
        await message.answer("❌ Бағалау үшін сапарлар жоқ")
        return

    keyboard_buttons = []
    for trip in trips:
        is_driver = trip[1] == message.from_user.id
        
        # Get target user info
        target_id = trip[2] if is_driver else trip[1]
        
        async with get_db() as db:
            if is_driver:
                async with db.execute("SELECT full_name FROM clients WHERE user_id=?", (target_id,)) as cur:
                    target = await cur.fetchone()
                    target_name = f"Клиентті {target[0] if target else 'N/A'}"
            else:
                async with db.execute("SELECT full_name FROM drivers WHERE user_id=?", (target_id,)) as cur:
                    target = await cur.fetchone()
                    target_name = f"Жүргізушіні {target[0] if target else 'N/A'}"
        
        keyboard_buttons.append([
            InlineKeyboardButton(text=f"⭐ {target_name}",
                               callback_data=f"rate_trip_{trip[0]}")
        ])
    
    keyboard_buttons.append([
        InlineKeyboardButton(text="🔙 Артқа", callback_data="back_main")
    ])

    await message.answer(
        "✍️ <b>Кімді бағалағыңыз келеді:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons),
        parse_mode="HTML")


@dp.message(F.text == "ℹ️ Ақпарат")
async def info_command(message: types.Message):
    await message.answer(
        "ℹ️ <b>Құрметті такси бот желісін қолданушылар назарына! 🚖</b>\n"
        "Біздің желіні пайдаланып отырғандарыңызға зор алғыс білдіреміз.\n"
        "Назар аударыңыз, төмендегі ақпаратпен мұқият танысып шығыңыздар:\n\n"
        "📍 Бағыттар мен тарифтер:\n"
        "- Ақтау – Жаңаөзен – Ақтау → 1 орын – 2500 тг\n"
        "- Ақтау – Шетпе – Ақтау → 1 орын – 2000 тг\n\n"
        "⚠️ Маңызды:\n"
        "Жалған тапсырыс беру батырмасын негізсіз басу жағдайлары анықталған қолданушыларға 2 айға желіні пайдалану шектеуі қойылады.\n\n"
        "Сапарларыңыз сәтті, жолдарыңыз ашық болсын! 🚗💨\n\n"
        f"❌ Қате орын алған жағдайда админге хабарласыңыз: {ADMIN_USER_LOGIN} немесе Whatsapp: {ADMIN_PHONE}",
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML")


@dp.callback_query(F.data == "back_main")
async def back_main(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.message.answer("Басты меню:",
                                  reply_markup=main_menu_keyboard())
    await callback.answer()


# ==================== ADMIN ====================


def admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👥 Жүргізушілер",
                                 callback_data="admin_drivers")
        ],
        [
            InlineKeyboardButton(text="🧍‍♂️ Клиенттер",
                                 callback_data="admin_clients")
        ],
        [
            InlineKeyboardButton(text="📊 Статистика",
                                 callback_data="admin_stats")
        ], [InlineKeyboardButton(text="📜 Логтар", callback_data="admin_logs")],
        [InlineKeyboardButton(text="🔙 Артқа", callback_data="back_main")]
    ])


@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Тыйым салынған")
        return

    await message.answer("🔐 <b>Админ панелі</b>",
                         reply_markup=admin_keyboard(),
                         parse_mode="HTML")


@dp.callback_query(F.data == "admin_drivers")
async def admin_drivers(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Тыйым салынған", show_alert=True)
        return

    async with get_db() as db:
        async with db.execute(
                "SELECT * FROM drivers ORDER BY direction, queue_position"
        ) as cursor:
            drivers = await cursor.fetchall()

    if not drivers:
        msg = "❌ Жүргізушілер жоқ"
    else:
        msg = "👥 <b>Жүргізушілер:</b>\n\n"
        for driver in drivers:
            occupied, total, available = await get_driver_available_seats(
                driver[0])
            msg += f"№{driver[7]} - {driver[1]}\n"
            msg += f"   🚗 {driver[4]} ({driver[3]})\n"
            msg += f"   💺 {occupied}/{total} (бос: {available})\n"
            msg += f"   📍 {driver[6]}\n"
            msg += f"   {get_rating_stars(driver[13] if len(driver) > 13 else 0)}\n\n"

    await callback.message.edit_text(msg,
                                     reply_markup=admin_keyboard(),
                                     parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "admin_clients")
async def admin_clients(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Тыйым салынған", show_alert=True)
        return

    async with get_db() as db:
        async with db.execute(
                "SELECT * FROM clients ORDER BY direction, queue_position"
        ) as cursor:
            clients = await cursor.fetchall()

    if not clients:
        msg = "❌ Клиенттер жоқ"
    else:
        msg = "🧍‍♂️ <b>Кезектегі клиенттер:</b>\n\n"
        for client in clients:
            status_emoji = {
                "waiting": "⏳",
                "accepted": "✅",
                "driver_arrived": "🚗"
            }
            msg += f"№{client[4]} {status_emoji.get(client[10], '❓')} - {client[1]}\n"
            msg += f"   📍 {client[3]}\n"
            msg += f"   👥 {client[5]} адам.\n"
            if client[11]:
                msg += f"   🚗 Жүргізуші: ID {client[11]}\n"
            msg += "\n"

    await callback.message.edit_text(msg,
                                     reply_markup=admin_keyboard(),
                                     parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Тыйым салынған", show_alert=True)
        return

    async with get_db() as db:
        async with db.execute("SELECT COUNT(*) FROM drivers") as cursor:
            total_drivers = (await cursor.fetchone())[0]

        async with db.execute(
                "SELECT COUNT(*) FROM clients WHERE status='waiting'"
        ) as cursor:
            waiting_clients = (await cursor.fetchone())[0]

        async with db.execute(
                "SELECT COUNT(*) FROM clients WHERE status='accepted'"
        ) as cursor:
            accepted_clients = (await cursor.fetchone())[0]

        async with db.execute(
                "SELECT COUNT(*) FROM trips WHERE status='completed'"
        ) as cursor:
            completed_trips = (await cursor.fetchone())[0]

        async with db.execute(
                "SELECT COUNT(*) FROM trips WHERE cancelled_by IS NOT NULL"
        ) as cursor:
            cancelled_trips = (await cursor.fetchone())[0]

        async with db.execute("SELECT AVG(rating) FROM ratings") as cursor:
            avg_rating = (await cursor.fetchone())[0] or 0

        async with db.execute(
                "SELECT SUM(total_seats - COALESCE(occupied_seats, 0)) FROM drivers WHERE is_active=1"
        ) as cursor:
            total_available_seats = (await cursor.fetchone())[0] or 0

        async with db.execute("SELECT COUNT(*) FROM blacklist") as cursor:
            blacklisted_users = (await cursor.fetchone())[0]

    msg = "📊 <b>Статистика:</b>\n\n"
    msg += f"👥 Жүргізушілер: {total_drivers}\n"
    msg += f"💺 Бос орындар: {total_available_seats}\n\n"
    msg += f"🧍‍♂️ Күтімдегі клиенттер: {waiting_clients}\n"
    msg += f"✅ Қабылданған клиенттер: {accepted_clients}\n\n"
    msg += f"✅ Аяқталған сапарлар: {completed_trips}\n"
    msg += f"❌ Жойылған сапарлар: {cancelled_trips}\n"
    msg += f"⭐ Орташа рейтинг: {avg_rating:.1f}\n"
    msg += f"🚫 Бұғатталған пайдаланушылар: {blacklisted_users}\n"

    await callback.message.edit_text(msg,
                                     reply_markup=admin_keyboard(),
                                     parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "admin_logs")
async def admin_logs(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Тыйым салынған", show_alert=True)
        return

    async with get_db() as db:
        async with db.execute('''SELECT user_id, action, details, created_at 
                     FROM actions_log 
                     ORDER BY created_at DESC LIMIT 20''') as cursor:
            logs = await cursor.fetchall()

    msg = "📜 <b>Соңғы әрекеттер:</b>\n\n"
    for log in logs:
        try:
            time = datetime.fromisoformat(log[3]).strftime("%H:%M")
        except:
            time = "??:??"
        msg += f"🕐 {time} | User {log[0]}\n"
        msg += f"   {log[1]}\n"
        if log[2]:
            msg += f"   {log[2]}\n"
        msg += "\n"

    await callback.message.edit_text(msg,
                                     reply_markup=admin_keyboard(),
                                     parse_mode="HTML")
    await callback.answer()


@dp.message(Command("addadmin"))
async def add_admin_command(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Тыйым салынған")
        return

    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Осы команданы пайдаланыңыз: /addadmin USER_ID")
        return

    try:
        new_admin_id = int(parts[1])
        async with get_db(write=True) as db:
            await db.execute("INSERT INTO admins (user_id) VALUES (?)",
                             (new_admin_id, ))

        await save_log_action(message.from_user.id, "admin_added",
                              f"New admin: {new_admin_id}")
        await message.answer(f"✅ Админ қосылды: {new_admin_id}")
    except Exception as e:
        await message.answer(f"❌ Қате: {e}")


@dp.message(Command("blacklist"))
async def show_blacklist(message: types.Message):
    """Show blacklisted users (admin only)"""
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Тыйым салынған")
        return

    async with get_db() as db:
        async with db.execute(
                '''SELECT user_id, reason, cancellation_count, banned_at 
               FROM blacklist ORDER BY banned_at DESC''') as cursor:
            blacklist = await cursor.fetchall()

    if not blacklist:
        await message.answer("✅ Қара тізім бос")
        return

    msg = "🚫 <b>Қара тізім:</b>\n\n"
    for entry in blacklist:
        try:
            banned_time = datetime.fromisoformat(
                entry[3]).strftime("%Y-%m-%d %H:%M")
        except:
            banned_time = "???"

        msg += f"👤 User ID: <code>{entry[0]}</code>\n"
        msg += f"   Себеп: {entry[1]}\n"
        msg += f"   Жойылған сапарлар: {entry[2]}\n"
        msg += f"   Күні: {banned_time}\n\n"

    msg += "\n💡 Рұқсат беру: /unban USER_ID"

    await message.answer(msg, parse_mode="HTML")


@dp.message(Command("unban"))
async def unban_user(message: types.Message):
    """Unban a user (admin only)"""
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Тыйым салынған")
        return

    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Осы команданы пайдаланыңыз: /unban USER_ID")
        return

    try:
        user_id = int(parts[1])

        async with get_db(write=True) as db:
            await db.execute("DELETE FROM blacklist WHERE user_id=?",
                             (user_id, ))

            # Reset cancellation count
            await db.execute(
                "UPDATE clients SET cancellation_count=0 WHERE user_id=?",
                (user_id, ))

        await save_log_action(message.from_user.id, "user_unbanned",
                              f"Unbanned user: {user_id}")

        await message.answer(
            f"✅ Пайдаланушы {user_id} қара тізімнен шығарылды.")

        # Notify user
        try:
            await bot.send_message(
                user_id, "✅ <b>Сіз қара тізімнен шығарылдыңыз!</b>\n\n"
                "Енді сіз қайтадан тапсырыс бере аласыз.\n"
                "Өтініш, тапсырысты жауапкершілікпен жасаңыз.",
                parse_mode="HTML")
        except:
            pass

    except ValueError:
        await message.answer("❌ Қате USER_ID")
    except Exception as e:
        await message.answer(f"❌ Қате: {e}")


@dp.message(Command("resetcancel"))
async def reset_cancellation(message: types.Message):
    """Reset cancellation count for a user (admin only)"""
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Тыйым салынған")
        return

    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Осы команданы пайдаланыңыз: /resetcancel USER_ID"
                             )
        return

    try:
        user_id = int(parts[1])

        async with get_db(write=True) as db:
            await db.execute(
                "UPDATE clients SET cancellation_count=0 WHERE user_id=?",
                (user_id, ))

        await save_log_action(message.from_user.id, "cancellation_reset",
                              f"Reset for user: {user_id}")

        await message.answer(f"✅ Санақ {user_id} жойылды.")

    except ValueError:
        await message.answer("❌ Қате USER_ID")
    except Exception as e:
        await message.answer(f"❌ Қате: {e}")
        
@dp.message(Command("removedriver"))
async def remove_driver_command(message: types.Message):
    """Remove a driver (admin only)"""
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Тыйым салынған")
        return

    parts = message.text.split()
    if len(parts) != 2:
        await message.answer(
            "❌ Қате пайдалану!\n\n"
            "Дұрыс формат: /removedriver USER_ID\n\n"
            "Мысал: /removedriver 123456789")
        return

    try:
        driver_id = int(parts[1])

        # First check if driver exists and get info
        async with get_db() as db:
            async with db.execute("SELECT full_name, car_model, car_number FROM drivers WHERE user_id=?",
                                 (driver_id,)) as cursor:
                driver = await cursor.fetchone()

            if not driver:
                await message.answer(f"❌ Жүргізуші табылмады: {driver_id}")
                return

            driver_name = driver[0]
            car_model = driver[1]
            car_number = driver[2]

            # Check for active trips
            async with db.execute(
                    '''SELECT COUNT(*) FROM clients 
                       WHERE assigned_driver_id=? AND status IN ('accepted', 'driver_arrived')''',
                (driver_id,)) as cursor:
                active_trips = (await cursor.fetchone())[0]

            if active_trips > 0:
                await message.answer(
                    f"⚠️ <b>Жүргізушіні жою мүмкін емес!</b>\n\n"
                    f"👤 {driver_name}\n"
                    f"🚗 {car_model} ({car_number})\n\n"
                    f"Себебі: {active_trips} белсенді сапар бар.\n"
                    f"Алдымен жүргізуші сапарларды аяқтауы керек.",
                    parse_mode="HTML")
                return

        # Now remove driver in write mode
        async with get_db(write=True) as db:
            # Remove driver
            await db.execute("DELETE FROM drivers WHERE user_id=?", (driver_id,))

        await save_log_action(message.from_user.id, "driver_removed",
                             f"Removed driver: {driver_id} ({driver_name})")

        await message.answer(
            f"✅ <b>Жүргізуші жойылды!</b>\n\n"
            f"👤 {driver_name}\n"
            f"🚗 {car_model} ({car_number})\n"
            f"ID: <code>{driver_id}</code>",
            parse_mode="HTML")

        # Notify driver
        try:
            await bot.send_message(
                driver_id,
                "⚠️ <b>Сіздің жүргізуші профиліңіз жойылды</b>\n\n"
                "Себебі: Админ тарапынан жойылды\n\n"
                "Егер бұл қателік деп ойласаңыз немесе "
                "қайта тіркелгіңіз келсе, админге хабарласыңыз.",
                parse_mode="HTML")
        except Exception as e:
            logger.warning(f"Couldn't notify driver {driver_id}: {e}")
            await message.answer(
                f"ℹ️ Жүргізушіге хабарлама жіберу мүмкін болмады (бот бұғатталған немесе жойылған)")

    except ValueError:
        await message.answer(
            "❌ Қате USER_ID!\n\n"
            "USER_ID сан болуы керек.\n"
            "Мысал: /removedriver 123456789")
    except Exception as e:
        logger.error(f"Error in remove_driver_command: {e}", exc_info=True)
        await message.answer(f"❌ Қате орын алды: {str(e)}")


@dp.message(Command("listdrivers"))
async def list_drivers_command(message: types.Message):
    """List all drivers with their IDs (admin only)"""
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Тыйым салынған")
        return

    async with get_db() as db:
        async with db.execute(
                '''SELECT user_id, full_name, car_model, car_number, direction, 
                          is_active, occupied_seats, total_seats
                   FROM drivers 
                   ORDER BY direction, queue_position''') as cursor:
            drivers = await cursor.fetchall()

    if not drivers:
        await message.answer("❌ Жүргізушілер жоқ")
        return

    msg = "👥 <b>Барлық жүргізушілер:</b>\n\n"
    
    for driver in drivers:
        user_id = driver[0]
        full_name = driver[1]
        car_model = driver[2]
        car_number = driver[3]
        direction = driver[4]
        is_active = driver[5]
        occupied = driver[6] if driver[6] else 0
        total = driver[7]
        available = total - occupied
        
        status = "✅ Белсенді" if is_active else "❌ Белсенді емес"
        
        msg += f"👤 <b>{full_name}</b>\n"
        msg += f"   ID: <code>{user_id}</code>\n"
        msg += f"   🚗 {car_model} ({car_number})\n"
        msg += f"   📍 {direction}\n"
        msg += f"   💺 {occupied}/{total} (бос: {available})\n"
        msg += f"   {status}\n"
        msg += f"   Жою: /removedriver {user_id}\n\n"

    # Split message if too long
    if len(msg) > 4000:
        parts = msg.split('\n\n')
        current_msg = "👥 <b>Барлық жүргізушілер:</b>\n\n"
        
        for part in parts[1:]:  # Skip header
            if len(current_msg) + len(part) > 4000:
                await message.answer(current_msg, parse_mode="HTML")
                current_msg = part + "\n\n"
            else:
                current_msg += part + "\n\n"
        
        if current_msg.strip():
            await message.answer(current_msg, parse_mode="HTML")
    else:
        await message.answer(msg, parse_mode="HTML")

@dp.message()
async def handle_unknown(message: types.Message):
    logger.warning(
        f"Unhandled message from {message.from_user.id}: {message.text}")
    await message.answer(
        "❓ <b>Мен бұл команданы түсінбедім.</b>\n\n"
        "Меню батырмаларын пайдаланыңыз:",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard())

# ==================== START ====================


async def main():
    await init_db()
    logger.info("🚀 Бот запущен")

    await bot.delete_webhook(drop_pending_updates=True)

    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")