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
DB_TIMEOUT = 10.0

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
    handlers=[
        logging.FileHandler('taxi_bot.log'),
        logging.StreamHandler()
    ]
)
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
    payment_method = State()
    kaspi_number = State()


class ClientOrder(StatesGroup):
    confirm_data = State()
    phone_number = State()  
    from_city = State()
    to_city = State()
    direction = State()
    passengers_count = State()
    order_for = State()
    passenger_name = State()
    passenger_phone = State()
    add_another = State()

class RatingStates(StatesGroup):
    select_rating = State()
    write_review = State()

class ChatState(StatesGroup):
    waiting_message_to_driver = State()
    waiting_message_to_client = State()

# ==================== DATABASE AND MIGRATIONS ====================

@asynccontextmanager
async def get_db(write=False):
    db = None
    try:
        if write:
            async with db_lock:
                db = await aiosqlite.connect(DATABASE_FILE, timeout=30.0)
                await db.execute("PRAGMA journal_mode=WAL")
                await db.execute("PRAGMA busy_timeout=30000")
                yield db
                await db.commit()
        else:
            db = await aiosqlite.connect(DATABASE_FILE, timeout=30.0)
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA busy_timeout=30000")
            yield db
    except Exception as e:
        if db:
            await db.rollback()
        logger.error(f"Database error: {e}", exc_info=True)
        raise
    finally:
        if db:
            await db.close()

class DBMigration:
    
    @staticmethod
    def get_db_version() -> int:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute("PRAGMA user_version")
        version = c.fetchone()[0]
        conn.close()
        return version
    
    @staticmethod
    def set_db_version(version: int):
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute(f"PRAGMA user_version = {version}")
        conn.commit()
        conn.close()
    
    @staticmethod
    def migrate():
        current_version = DBMigration.get_db_version()
        
        if current_version < 1:
            DBMigration.migration_v1()
        if current_version < 2:
            DBMigration.migration_v2()
        if current_version < 3:
            DBMigration.migration_v3()
        if current_version < 4:
            DBMigration.migration_v4()
        if current_version < 5:
            DBMigration.migration_v5()
        if current_version < 6:
            DBMigration.migration_v6()
    
    @staticmethod
    def migration_v1():
        logger.info("Migration v1...")
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
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
                      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
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
                      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS admins
                     (user_id INTEGER PRIMARY KEY,
                      added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
        conn.commit()
        conn.close()
        DBMigration.set_db_version(1)
        logger.info("Migration done")
    
    @staticmethod
    def migration_v2():
        logger.info("Migration v2...")
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        c.execute('''CREATE TABLE IF NOT EXISTS ratings
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      from_user_id INTEGER,
                      to_user_id INTEGER,
                      user_type TEXT,
                      trip_id INTEGER,
                      rating INTEGER CHECK(rating >= 1 AND rating <= 5),
                      review TEXT,
                      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
        c.execute("PRAGMA table_info(drivers)")
        driver_columns = [column[1] for column in c.fetchall()]
        if 'avg_rating' not in driver_columns:
            c.execute("ALTER TABLE drivers ADD COLUMN avg_rating REAL DEFAULT 0")
        if 'rating_count' not in driver_columns:
            c.execute("ALTER TABLE drivers ADD COLUMN rating_count INTEGER DEFAULT 0")
        
        c.execute("PRAGMA table_info(clients)")
        client_columns = [column[1] for column in c.fetchall()]
        if 'avg_rating' not in client_columns:
            c.execute("ALTER TABLE clients ADD COLUMN avg_rating REAL DEFAULT 0")
        if 'rating_count' not in client_columns:
            c.execute("ALTER TABLE clients ADD COLUMN rating_count INTEGER DEFAULT 0")
        
        conn.commit()
        conn.close()
        DBMigration.set_db_version(2)
        logger.info("Migration completed")
    
    @staticmethod
    def migration_v3():
        logger.info("Migration v3...")
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
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
        
        conn.commit()
        conn.close()
        DBMigration.set_db_version(3)
        logger.info("Migration completed")
    
    @staticmethod
    def migration_v4():
        """Migration v4: Adding occupied_seats for drivers"""
        logger.info("Migration v4...")
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        c.execute("PRAGMA table_info(drivers)")
        driver_columns = [column[1] for column in c.fetchall()]
        
        if 'occupied_seats' not in driver_columns:
            c.execute("ALTER TABLE drivers ADD COLUMN occupied_seats INTEGER DEFAULT 0")
        
        if 'is_on_trip' not in driver_columns:
            c.execute("ALTER TABLE drivers ADD COLUMN is_on_trip INTEGER DEFAULT 0")
        
        conn.commit()
        conn.close()
        DBMigration.set_db_version(4)
        logger.info("Migration completed")

    @staticmethod
    def migration_v5():
        """Migration v5: Adding blacklist system"""
        logger.info("Migration v5...")
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
    
        # Blacklist table
        c.execute('''CREATE TABLE IF NOT EXISTS blacklist
                  (user_id INTEGER PRIMARY KEY,
                   reason TEXT,
                   cancellation_count INTEGER DEFAULT 0,
                   banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
        # Adding cancellation_count to clients
        c.execute("PRAGMA table_info(clients)")
        client_columns = [column[1] for column in c.fetchall()]
    
        if 'cancellation_count' not in client_columns:
            c.execute("ALTER TABLE clients ADD COLUMN cancellation_count INTEGER DEFAULT 0")
    
        conn.commit()
        conn.close()
        DBMigration.set_db_version(5)
        logger.info("Migration completed")
        
    @staticmethod
    def migration_v6():
        """Migration v6: Adding support for multiple orders"""
        logger.info("Migration v6...")
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()

        # Adding order_for field to clients (who the order is for)
        c.execute("PRAGMA table_info(clients)")
        client_columns = [column[1] for column in c.fetchall()]
    
        if 'order_for' not in client_columns:
            c.execute("ALTER TABLE clients ADD COLUMN order_for TEXT DEFAULT 'self'")
    
        if 'order_number' not in client_columns:
            c.execute("ALTER TABLE clients ADD COLUMN order_number INTEGER DEFAULT 1")
    
        if 'parent_user_id' not in client_columns:
            c.execute("ALTER TABLE clients ADD COLUMN parent_user_id INTEGER")
            
        if 'from_city' not in client_columns:
            c.execute("ALTER TABLE clients ADD COLUMN from_city TEXT DEFAULT ''")
            
        if 'to_city' not in client_columns:
            c.execute("ALTER TABLE clients ADD COLUMN to_city TEXT DEFAULT ''")
        
        if 'payment_methods' not in driver_columns:
            c.execute("ALTER TABLE drivers ADD COLUMN payment_methods TEXT DEFAULT ''")
            
        if 'kaspi_number' not in driver_columns:
            c.execute("ALTER TABLE drivers ADD COLUMN kaspi_number TEXT DEFAULT ''")

    
        conn.commit()
        conn.close()
        DBMigration.set_db_version(6)
        logger.info("Migration completed")

async def init_db():
    """Initialize database with WAL mode"""
    async with get_db(write=False) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=30000")
        
        # Add indexes for better performance
        await db.execute("CREATE INDEX IF NOT EXISTS idx_clients_status ON clients(status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_clients_direction ON clients(direction)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_drivers_direction ON drivers(direction)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_trips_driver ON trips(driver_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_trips_client ON trips(client_id)")
        
        await db.commit()
    
    # Run migrations (keep your existing DBMigration class)
    DBMigration.migrate()

# ==================== UTILITIES ====================

async def is_admin(user_id: int) -> bool:
    async with get_db() as db:
        async with db.execute("SELECT user_id FROM admins WHERE user_id=?", (user_id,)) as cursor:
            result = await cursor.fetchone()
            return result is not None

async def save_log_action(user_id: int, action: str, details: str = ""):
    """Save action log - ASYNC VERSION"""
    try:
        async with get_db(write=True) as db:
            await db.execute('''INSERT INTO actions_log (user_id, action, details) 
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
                (driver_id,)
            ) as cursor:
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
                (driver_id,)
            ) as cursor:
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
        async with db.execute(
            "SELECT reason FROM blacklist WHERE user_id=?",
            (user_id,)
        ) as cursor:
            result = await cursor.fetchone()
            if result:
                return (True, result[0])
            return (False, None)
        
async def get_cancellation_count(user_id: int) -> int:
    """Returns the number of cancellations for the client"""
    async with get_db() as db:
        async with db.execute(
            "SELECT COALESCE(cancellation_count, 0) FROM clients WHERE user_id=?",
            (user_id,)
        ) as cursor:
            result = await cursor.fetchone()
            return result[0] if result else 0

async def add_to_blacklist(user_id: int, reason: str, cancellation_count: int):
    """Adds a user to the blacklist"""
    async with get_db(write=True) as db:
        await db.execute(
            '''INSERT OR REPLACE INTO blacklist (user_id, reason, cancellation_count)
               VALUES (?, ?, ?)''',
            (user_id, reason, cancellation_count)
        )
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
               ORDER BY order_number''',
            (user_id, user_id)
        ) as cursor:
            return await cursor.fetchall()

async def count_user_orders(user_id: int) -> int:
    """Counts the number of active orders for the user"""
    async with get_db() as db:
        async with db.execute(
            '''SELECT COUNT(*) FROM clients 
               WHERE (parent_user_id=? OR user_id=?) 
               AND status IN ('waiting', 'accepted', 'driver_arrived')''',
            (user_id, user_id)
        ) as cursor:
            result = await cursor.fetchone()
            return result[0] if result else 0

def main_menu_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚗 Жүргізуші ретінде кіру")],
            [KeyboardButton(text="🧍‍♂️ Такси шақыру")],
            [KeyboardButton(text="⭐ Профиль")],
            [KeyboardButton(text="ℹ️ Ақпарат")]
        ],
        resize_keyboard=True
    )

def from_city_keyboard():
    """Choosing the departure city (only Aktau, Zhanaozen, or Shetpe)"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Ақтау", callback_data="from_aktau")],
            [InlineKeyboardButton(text="Жаңаөзен", callback_data="from_janaozen")],
            [InlineKeyboardButton(text="Шетпе", callback_data="from_shetpe")],
            [InlineKeyboardButton(text="🔙 Артқа", callback_data="back_main")]
        ]
    )

def to_city_keyboard(from_city: str):
    """Choosing the destination city based on departure"""
    buttons = []

    if from_city == "Ақтау":
        # From Aktau → can go to Shetpe or Zhanaozen
        buttons.append([InlineKeyboardButton(text="Шетпе", callback_data="to_shetpe")])
        buttons.append([InlineKeyboardButton(text="Жаңаөзен", callback_data="to_janaozen")])
    elif from_city == "Жаңаөзен":
        # From Zhanaozen → can go only to Aktau
        buttons.append([InlineKeyboardButton(text="Ақтау", callback_data="to_aktau")])
    elif from_city == "Шетпе":
        # From Shetpe → can go only to Aktau
        buttons.append([InlineKeyboardButton(text="Ақтау", callback_data="to_aktau")])

    # Add back button
    buttons.append([InlineKeyboardButton(text="🔙 Артқа", callback_data="back_from_city")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_rating_stars(rating: float) -> str:
    if not rating:
        return "❌ Баға жоқ"
    stars = int(rating)
    return "⭐" * stars + "☆" * (5 - stars)

# ==================== DRIVERS ====================

@dp.message(F.text == "🚗 Жүргізуші ретінде кіру")
async def driver_start_telegram_auth(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    async with get_db() as db:
        async with db.execute("SELECT * FROM drivers WHERE user_id=?", (user_id,)) as cursor:
            driver = await cursor.fetchone()
    
    if driver:
        # Show driver menu instead of error message
        await show_driver_menu(message, user_id)
        return
    
    full_name = message.from_user.full_name
    username = message.from_user.username
    
    await state.update_data(
        telegram_id=user_id,
        full_name=full_name,
        username=username or "",
        verified_by='telegram',
        is_verified=True
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Бәрі дұрыс", callback_data="confirm_telegram_data")]
    ])
    
    await message.answer(
        f"👤 <b>Сіздің Telegram деректеріңіз:</b>\n\n"
        f"Аты: {full_name}\n"
        f"Username: @{username if username else 'орнатылмаған'}\n"
        f"ID: <code>{user_id}</code>",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await state.set_state(DriverReg.confirm_data)

@dp.callback_query(DriverReg.confirm_data, F.data == "confirm_telegram_data")
async def confirm_telegram_data(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("✅ Керемет! Тіркеуді жалғастырамыз...")
    await callback.message.answer("📱 Телефон нөміріңізді енгізіңіз (мысалы: +7 777 123 45 67):")
    await state.set_state(DriverReg.phone_number)
    await callback.answer()

@dp.message(DriverReg.phone_number)
async def driver_phone_number(message: types.Message, state: FSMContext):
    await state.update_data(phone_number=message.text.strip())
    await message.answer("🚗 Көлік нөмірі (мысалы: 870 ABC 09)")
    await state.set_state(DriverReg.car_number)

@dp.callback_query(DriverReg.confirm_data, F.data == "continue_no_username")
async def continue_without_username(callback: types.CallbackQuery, state: FSMContext):
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
        if seats < 1 or seats > 8:
            await message.answer("Қате! Орын саны 1-ден 8-ге дейін болуы керек")
            return
        await state.update_data(seats=seats)
        await message.answer(
            "📍 Қай қаладан шығасыз?",
            reply_markup=current_city_keyboard()
        )
        await state.set_state(DriverReg.current_city)
    except ValueError:
        await message.answer("Сан енгізіңіз!")

@dp.callback_query(DriverReg.current_city, F.data.startswith("city_"))
async def driver_current_city(callback: types.CallbackQuery, state: FSMContext):
    city_map = {
        "city_aktau": "Ақтау",
        "city_janaozen": "Жаңаөзен",
        "city_shetpe": "Шетпе"
    }
    
    current_city = city_map.get(callback.data, "Ақтау")
    data = await state.get_data()

    # Use provided phone number
    phone = data.get('phone_number', f"tg_{callback.from_user.id}")
    
    async with get_db(write=True) as db:
        # Driver registers with current city (without destination)
        # direction is current_city now (where he can take the orders)
        await db.execute('''INSERT INTO drivers 
                     (user_id, full_name, phone, car_number, car_model, total_seats, 
                      direction, queue_position, is_active, is_verified, occupied_seats)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (callback.from_user.id, data['full_name'], phone, 
                   data['car_number'], data['car_model'], data['seats'], 
                   current_city, 0, 1, 1, 0))
    
    await save_log_action(callback.from_user.id, "driver_registered", 
                   f"Current city: {current_city}")
    
    await callback.message.edit_text(
        f"✅ <b>Сіз тіркелдіңіз!</b>\n\n"
        f"👤 {data['full_name']}\n"
        f"🚗 {data['car_model']} ({data['car_number']})\n"
        f"💺 Орын саны: {data['seats']}\n"
        f"📍 Қазіргі қала: {current_city}\n\n"
        f"Сіз {current_city} қаласынан шығатын барлық тапсырыстарды көре аласыз",
        parse_mode="HTML"
    )
    await state.clear()
    await callback.answer()
    
def current_city_keyboard():
    """Choosing the current city for the driver"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Ақтау", callback_data="city_aktau")],
            [InlineKeyboardButton(text="Жаңаөзен", callback_data="city_janaozen")],
            [InlineKeyboardButton(text="Шетпе", callback_data="city_shetpe")],
            [InlineKeyboardButton(text="🔙 Артқа", callback_data="back_main")]
        ]
    )

async def show_driver_menu(message: types.Message, user_id: int):
    async with get_db() as db:
        async with db.execute("PRAGMA table_info(drivers)") as cursor:
            columns = [col[1] for col in await cursor.fetchall()]
        
        async with db.execute("SELECT * FROM drivers WHERE user_id=?", (user_id,)) as cursor:
            driver = await cursor.fetchone()
    
    if not driver:
        await message.answer("Қате: сіз тіркелмегенсіз", reply_markup=main_menu_keyboard())
        return
    
    full_name_idx = columns.index('full_name') if 'full_name' in columns else 1
    car_number_idx = columns.index('car_number') if 'car_number' in columns else 2
    car_model_idx = columns.index('car_model') if 'car_model' in columns else 3
    direction_idx = columns.index('direction') if 'direction' in columns else 6  # Теперь это current_city
    
    if 'occupied_seats' in columns and 'total_seats' in columns:
        occupied, total, available = await get_driver_available_seats(user_id)
        seats_text = f"💺 Бос емес: {occupied}/{total} (бос: {available})\n"
    else:
        total_seats_idx = columns.index('total_seats') if 'total_seats' in columns else 5
        total = driver[total_seats_idx] if len(driver) > total_seats_idx else 4
        seats_text = f"💺 Мест: {total}\n"
    
    avg_rating_idx = columns.index('avg_rating') if 'avg_rating' in columns else None
    rating_text = get_rating_stars(driver[avg_rating_idx] if avg_rating_idx and len(driver) > avg_rating_idx else 0)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статус", callback_data="driver_status")],
        [InlineKeyboardButton(text="👥 Менің жолаушыларым", callback_data="driver_passengers")],
        [InlineKeyboardButton(text="📋 Тапсырыстар", callback_data="driver_available_orders")],
        [InlineKeyboardButton(text="✅ Сапарды аяқтау", callback_data="driver_complete_trip")],
        [InlineKeyboardButton(text="🔄 Қаланы өзгерту", callback_data="driver_change_city")],
        [InlineKeyboardButton(text="❌ Кезектен шығу", callback_data="driver_exit")],
        [InlineKeyboardButton(text="🔙 Меню", callback_data="back_main")]
    ])
    
    await message.answer(
        f"🚗 <b>Жүргізуші профилі</b>\n\n"
        f"👤 {driver[full_name_idx]}\n"
        f"🚗 {driver[car_model_idx]} ({driver[car_number_idx]})\n"
        f"{seats_text}"
        f"📍 Қазіргі қала: {driver[direction_idx]}\n"
        f"{rating_text}\n\n"
        "Сіз өз қалаңыздан шығатын тапсырыстарды көре аласыз",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "driver_status")
async def driver_status(callback: types.CallbackQuery):
    async with get_db() as db:
        async with db.execute("SELECT * FROM drivers WHERE user_id=?", (callback.from_user.id,)) as cursor:
            driver = await cursor.fetchone()
        
        # Counting waiting orders from driver's current city
        async with db.execute("SELECT COUNT(*) FROM clients WHERE from_city=? AND status='waiting'", (driver[6],)) as cursor:
            waiting = (await cursor.fetchone())[0]
    
    occupied, total, available = await get_driver_available_seats(callback.from_user.id)
    
    await callback.message.edit_text(
        f"📊 <b>Статус</b>\n\n"
        f"🚗 {driver[4]} ({driver[3]})\n"
        f"📍 Қазіргі қала: {driver[6]}\n"
        f"💺 Бос емес: {occupied}/{total}\n"
        f"💺 Бос орындар: {available}\n"
        f"⏳ Сіздің қалаңыздан шығатын тапсырыстар: {waiting}\n"
        f"{get_rating_stars(driver[13] if len(driver) > 13 else 0)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Артқа", callback_data="driver_menu")]
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "driver_passengers")
async def driver_passengers(callback: types.CallbackQuery):
    async with get_db() as db:
        async with db.execute('''SELECT c.user_id, c.full_name, c.passengers_count
                     FROM clients c
                     WHERE c.assigned_driver_id=? AND c.status IN ('accepted', 'driver_arrived')
                     ORDER BY c.created_at''', (callback.from_user.id,)) as cursor:
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
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Артқа", callback_data="driver_menu")]
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "driver_available_orders")
async def driver_available_orders(callback: types.CallbackQuery):
    """Show available orders for the driver based on their current city"""
    async with get_db() as db:
        # direction from driver = current_city
        async with db.execute("SELECT direction FROM drivers WHERE user_id=?", (callback.from_user.id,)) as cursor:
            driver_city = (await cursor.fetchone())[0]
        
        occupied, total, available = await get_driver_available_seats(callback.from_user.id)
        
        # Show orders, where from_city matches driver's current city
        async with db.execute('''SELECT user_id, full_name, passengers_count, queue_position, direction, from_city, to_city
                     FROM clients
                     WHERE from_city=? AND status='waiting'
                     ORDER BY queue_position''', (driver_city,)) as cursor:
            clients = await cursor.fetchall()
    
    if not clients:
        msg = f"❌ Cіздің қалаңыздан шығатын тапсырыстар жоқ {driver_city}\n\n💺 Бос орындар: {available}"
    else:
        msg = f"🔔 <b>{driver_city} қаласынан шығатын тапсырыстар:</b>\n"
        msg += f"💺 Бос орындар: {available}\n\n"

        keyboard_buttons = []
        for client in clients:
            can_fit = client[4] <= available
            fit_emoji = "✅" if can_fit else "⚠️"
            warning = "" if can_fit else " (орын жетпейді!)"
            
            # client[6] = direction (толық маршрут), client[8] = to_city
            msg += f"{fit_emoji} №{client[5]} - {client[1]} ({client[4]} адам.){warning}\n"
            msg += f"   🎯 {client[6]}\n"  # Толық маршрут
            msg += f"   📍 {client[2]} → {client[3]}\n\n"
            
            button_text = f"✅ №{client[5]} алу ({client[4]} адам.)"
            if not can_fit:
                button_text = f"⚠️ №{client[5]} алу ({client[4]} адам.) - орын жетпейді!"
            
            keyboard_buttons.append([
                InlineKeyboardButton(
                    text=button_text,
                    callback_data=f"accept_client_{client[0]}"
                )
            ])
        
        keyboard_buttons.append([InlineKeyboardButton(text="🔙 Артқа", callback_data="driver_menu")])
        
        await callback.message.edit_text(
            msg,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons),
            parse_mode="HTML"
        )
        return
    
    await callback.message.edit_text(
        msg,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Артқа", callback_data="driver_menu")]
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("accept_client_"))
async def accept_client(callback: types.CallbackQuery):
    """Driver accepts a client"""
    client_id = int(callback.data.split("_")[2])
    driver_id = callback.from_user.id
    
    try:
        async with get_db(write=True) as db:
            cursor = await db.execute(
                '''UPDATE clients 
                   SET status='accepted', assigned_driver_id=? 
                   WHERE user_id=? AND status='waiting'
                   RETURNING passengers_count, full_name, direction''', 
                (driver_id, client_id)
            )
            client = await cursor.fetchone()
            
            if not client:
                await callback.answer("❌ Клиентті басқа жүргізуші алып қойды!", show_alert=True)
                return
            
            passengers_count, full_name, direction = client
            
            # Check available seats
            cursor = await db.execute(
                "SELECT total_seats, COALESCE(occupied_seats, 0), car_model, car_number FROM drivers WHERE user_id=?",
                (driver_id,)
            )
            driver_data = await cursor.fetchone()
            
            if not driver_data:
                await callback.answer("❌ Қате: жүргізуші жоқ", show_alert=True)
                return
            
            total, occupied, car_model, car_number = driver_data
            available = total - occupied
            
            if passengers_count > available:
                await db.execute(
                    "UPDATE clients SET status='waiting', assigned_driver_id=NULL WHERE user_id=?",
                    (client_id,)
                )
                await callback.answer(
                    f"❌ Орын жетпейді! {passengers_count} орын қажет, {available} орын бар", 
                    show_alert=True
                )
                return
            
            # Update occupied seats
            await db.execute(
                '''UPDATE drivers 
                   SET occupied_seats = COALESCE(occupied_seats, 0) + ? 
                   WHERE user_id=?''', 
                (passengers_count, driver_id)
            )
            
            # Create trip record
            await db.execute(
                '''INSERT INTO trips (driver_id, client_id, direction, status, passengers_count)
                   VALUES (?, ?, ?, 'accepted', ?)''',
                (driver_id, client_id, direction, passengers_count)
            )
        
        await save_log_action(driver_id, "client_accepted", f"Client: {client_id}")
        
        # Get driver and client contact info
        async with get_db() as db:
            async with db.execute("SELECT phone FROM drivers WHERE user_id=?", (driver_id,)) as cursor:
                driver_phone = (await cursor.fetchone())[0]
            async with db.execute("SELECT phone, order_for, full_name FROM clients WHERE user_id=?", (client_id,)) as cursor:
                client_data = await cursor.fetchone()
                client_phone = client_data[0] if client_data else "N/A"
                order_for_info = client_data[1] if client_data and len(client_data) > 1 else "Өзіне"
                client_full_name = client_data[2] if client_data and len(client_data) > 2 else full_name

        # Notify client
        try:
            await bot.send_message(
                client_id,
                f"✅ <b>Жүргізуші тапсырысыңызды қабылдады!</b>\n\n"
                f"🚗 {car_model} ({car_number})\n"
                f"📍 {direction}\n\n"
                f"📞 Жүргізуші байланысы: {driver_phone}\n\n"
                f"Жүргізушінің қоңырауын күтіңіз!",
                parse_mode="HTML"
            )
        except:
            pass

        # Notify driver with passenger contact
        try:
            await bot.send_message(
                driver_id,
                f"✅ <b>Тапсырыс қабылданды!</b>\n\n"
                f"👤 Жолаушы: {client_full_name}\n"
                f"📞 Байланыс: {client_phone}\n"
                f"📍 {data['from_city']} → {data['to_city']}\n"
                f"👥 Орын: {passengers_count}\n"
                f"ℹ️ Кімге: {order_for_info}",
                parse_mode="HTML"
            )
        except:
            pass
        
        await callback.answer(f"✅ Клиент {full_name} қосылды!", show_alert=True)
        await driver_available_orders(callback)
        
    except Exception as e:
        logger.error(f"Error in accept_client: {e}", exc_info=True)
        await callback.answer("❌ Қате. Тағы бір рет көріңіз.", show_alert=True)
        
@dp.callback_query(F.data == "driver_change_city")
async def driver_change_city(callback: types.CallbackQuery):
    """Driver changes current city"""
    async with get_db() as db:
        # Check active trips
        async with db.execute('''SELECT COUNT(*) FROM clients 
                     WHERE assigned_driver_id=? AND status IN ('accepted', 'driver_arrived')''',
                  (callback.from_user.id,)) as cursor:
            active_trips = (await cursor.fetchone())[0]
        
        if active_trips > 0:
            await callback.answer(
                "❌ Қаланы өзгерту мүмкін емес - белсенді сапарлар бар!",
                show_alert=True
            )
            return
    
    await callback.message.edit_text(
        "📍 <b>Қаланы өзгерту</b>\n\n"
        "Сіз орналасқан қаланы таңдаңыз:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Ақтау", callback_data="change_to_aktau")],
            [InlineKeyboardButton(text="Жаңаөзен", callback_data="change_to_janaozen")],
            [InlineKeyboardButton(text="Шетпе", callback_data="change_to_shetpe")],
            [InlineKeyboardButton(text="🔙 Артқа", callback_data="driver_menu")]
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("change_to_"))
async def confirm_change_city(callback: types.CallbackQuery):
    """Verify and change driver's current city"""
    city_map = {
        "change_to_aktau": "Ақтау",
        "change_to_janaozen": "Жаңаөзен",
        "change_to_shetpe": "Шетпе"
    }
    
    new_city = city_map[callback.data]
    
    async with get_db(write=True) as db:
        await db.execute(
            "UPDATE drivers SET direction=? WHERE user_id=?",
            (new_city, callback.from_user.id)
        )
    
    await save_log_action(callback.from_user.id, "city_changed", f"New city: {new_city}")
    
    await callback.message.edit_text(
        f"✅ <b>Қала өзгертілді!</b>\n\n"
        f"📍 Жаңа қала: {new_city}\n\n"
        f"Енді сіз {new_city} қаласынан тапсырыстарды көре аласыз",
        parse_mode="HTML"
    )
    
    await asyncio.sleep(2)
    await show_driver_menu(callback.message, callback.from_user.id)
    await callback.answer()

@dp.callback_query(F.data == "driver_complete_trip")
async def driver_complete_trip(callback: types.CallbackQuery):
    """Driver completes the trip"""
    async with get_db(write=True) as db:
        # Get all clients in the trip
        async with db.execute('''SELECT user_id, passengers_count 
                     FROM clients 
                     WHERE assigned_driver_id=? AND status IN ('accepted', 'driver_arrived')''',
                  (callback.from_user.id,)) as cursor:
            clients = await cursor.fetchall()
        
        if not clients:
            await callback.answer("❌ Белсенді сапар жоқ!", show_alert=True)
            return
        
        total_freed = sum(c[1] for c in clients)
        
        # End trips
        await db.execute('''UPDATE trips 
                     SET status='completed', trip_completed_at=CURRENT_TIMESTAMP 
                     WHERE driver_id=? AND status IN ('accepted', 'driver_arrived')''',
                  (callback.from_user.id,))
        
        # Delete clients from active trips
        await db.execute('''DELETE FROM clients 
                     WHERE assigned_driver_id=? AND status IN ('accepted', 'driver_arrived')''',
                  (callback.from_user.id,))
        
        # Free up occupied seats
        await db.execute('''UPDATE drivers 
                     SET occupied_seats = COALESCE(occupied_seats, 0) - ? 
                     WHERE user_id=?''', (total_freed, callback.from_user.id))
    
    await save_log_action(callback.from_user.id, "trip_completed", f"Freed {total_freed} seats")
    
    for client in clients:
        try:
            await bot.send_message(
                client[0],
                f"✅ <b>Сапар аяқталды!</b>\n\n"
                f"Жүргізушіге баға беріңіз болады:\n"
                f"/rate",
                parse_mode="HTML"
            )
        except:
            pass
    
    await callback.answer(f"✅ Сапар аяқталды! {total_freed} орын босады", show_alert=True)
    await show_driver_menu(callback.message, callback.from_user.id)

@dp.callback_query(F.data == "driver_exit")
async def driver_exit(callback: types.CallbackQuery):
    async with get_db(write=True) as db:
        async with db.execute('''SELECT COUNT(*) FROM clients 
                     WHERE assigned_driver_id=? AND status IN ('accepted', 'driver_arrived')''',
                  (callback.from_user.id,)) as cursor:
            active_trips = (await cursor.fetchone())[0]
        
        if active_trips > 0:
            await callback.answer(
                "❌ Шығу мүмкін емес - белсенді сапарлар бар!",
                show_alert=True
            )
            return
        
        await db.execute("DELETE FROM drivers WHERE user_id=?", (callback.from_user.id,))
    
    await save_log_action(callback.from_user.id, "driver_exit", "")
    
    await callback.message.delete()
    await callback.message.answer(
        "❌ Сіз жүйеден шықтыңыз",
        reply_markup=main_menu_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "driver_menu")
async def driver_menu_back(callback: types.CallbackQuery):
    await show_driver_menu(callback.message, callback.from_user.id)
    await callback.answer()

# ==================== CLIENTS ====================

@dp.message(F.text == "🧍‍♂️ Такси шақыру")
async def client_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    # Check if client is already registered
    async with get_db() as db:
        async with db.execute("SELECT user_id FROM clients WHERE user_id=? AND status='registered'", (user_id,)) as cursor:
            client = await cursor.fetchone()
    
    if client:
        # Client already registered, proceed to order
        await start_new_order(message, state)
    else:
        # New client, need verification
        full_name = message.from_user.full_name
        username = message.from_user.username
        
        await state.update_data(
            telegram_id=user_id,
            full_name=full_name,
            username=username or "",
            verified_by='telegram',
            is_verified=True
        )
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Бәрі дұрыс", callback_data="confirm_client_telegram_data")]
        ])
        
        await message.answer(
            f"👤 <b>Сіздің Telegram деректеріңіз:</b>\n\n"
            f"Аты: {full_name}\n"
            f"Username: @{username if username else 'орнатылмаған'}\n"
            f"ID: <code>{user_id}</code>",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await state.set_state(ClientOrder.confirm_data)

async def start_new_order(message: types.Message, state: FSMContext):
    """Helper function to start a new order"""
    from_city_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Ақтау", callback_data="from_aktau")],
        [InlineKeyboardButton(text="Жаңаөзен", callback_data="from_janaozen")],
        [InlineKeyboardButton(text="Шетпе", callback_data="from_shetpe")]
    ])
    
    await message.answer(
        "🧍‍♂️ <b>Такси шақыру</b>\n\nҚай қаладан шығасыз?",
        reply_markup=from_city_keyboard,
        parse_mode="HTML"
    )
    await state.set_state(ClientOrder.from_city)
    
@dp.callback_query(ClientOrder.confirm_data, F.data == "confirm_client_telegram_data")
async def confirm_client_telegram_data(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("✅ Керемет! Тіркеуді жалғастырамыз...")
    await callback.message.answer("📱 Телефон нөміріңізді енгізіңіз (мысалы: +7 777 123 45 67):")
    await state.set_state(ClientOrder.phone_number)
    await callback.answer()

@dp.message(ClientOrder.phone_number)
async def client_phone_number(message: types.Message, state: FSMContext):
    data = await state.get_data()
    phone = message.text.strip()
    
    data = await state.get_data()
    phone = message.text.strip()
    
    # Save client as registered (without active order)
    async with get_db(write=True) as db:
        await db.execute(
            '''INSERT OR REPLACE INTO clients
               (user_id, full_name, phone, direction, queue_position,
                passengers_count, is_verified, status, from_city, to_city)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (message.from_user.id,
             data.get('full_name', message.from_user.full_name or "Клиент"),
             phone,
             '',        # direction - пустая строка, т.к. ещё нет заказа
             0,         # queue_position - 0 для не в очереди
             1,         # passengers_count - по умолчанию 1
             1,         # is_verified
             'registered',
             '',        # from_city
             ''         # to_city
            )
        )
    
    await save_log_action(message.from_user.id, "client_registered", f"Phone: {phone}")
    
    await message.answer("✅ Тіркелу аяқталды!")
    await start_new_order(message, state)
    
@dp.callback_query(F.data == "add_new_order")
async def add_new_order(callback: types.CallbackQuery, state: FSMContext):
    """Add new taxi order"""
    await callback.message.edit_text(
        "🧍‍♂️ <b>Жаңа такси шақыру</b>\n\n"
        "Қай қаладан шығасыз?",
        reply_markup=from_city_keyboard(),
        parse_mode="HTML"
    )
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
        status_emoji = {
            'waiting': '⏳',
            'accepted': '✅',
            'driver_arrived': '🚗'
        }
        emoji = status_emoji.get(order[4], '❓')
        
        msg += f"{emoji} <b>Заказ #{order[3]} - {order[2]}</b>\n"
        msg += f"   👥 {order[6]} чел. | 📍 {order[5]}\n"
        msg += f"   От: {order[7]}\n"
        msg += f"   До: {order[8]}\n\n"
        
        # Cancel button
        keyboard_buttons.append([
            InlineKeyboardButton(
                text=f"❌ Тапсырысты жою #{order[3]}",
                callback_data=f"cancel_order_{order[0]}"  # user_id of the order
            )
        ])
    
    keyboard_buttons.append([InlineKeyboardButton(text="🔙 Артқа", callback_data="back_main")])
    
    await callback.message.edit_text(
        msg,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("cancel_order_"))
async def cancel_specific_order(callback: types.CallbackQuery):
    """Cancel a specific order"""
    order_user_id = int(callback.data.split("_")[2])
    parent_user_id = callback.from_user.id
    
    async with get_db(write=True) as db:
        async with db.execute(
            "SELECT * FROM clients WHERE user_id=?",
            (order_user_id,)
        ) as cursor:
            client = await cursor.fetchone()
        
        if not client:
            await callback.answer("❌ Тапсырыс табылмады", show_alert=True)
            return
        
        # Check if this order belongs to the user
        if client[0] != parent_user_id and (len(client) <= 15 or client[15] != parent_user_id):
            await callback.answer("❌ Бұл сіздің тапсырысыңыз емес!", show_alert=True)
            return
        
        # Get current cancellation count
        cancellation_count = await get_cancellation_count(parent_user_id)
        new_count = cancellation_count + 1
        
        driver_id = client[11]  # assigned_driver_id
        
        # If assigned to a driver, free up seats
        if driver_id:
            await db.execute(
                '''UPDATE drivers 
                   SET occupied_seats = COALESCE(occupied_seats, 0) - ? 
                   WHERE user_id=?''',
                (client[5], driver_id)
            )
            
            # Notify driver
            try:
                await bot.send_message(
                    driver_id,
                    f"⚠️ Клиент тапсырысты жойды\n"
                    f"{client[13] if len(client) > 13 else 'үшін'}\n"
                    f"Бос орындар: {client[5]}",
                    parse_mode="HTML"
                )
            except:
                pass
        
        # Update trip status
        await db.execute(
            '''UPDATE trips SET status='cancelled', cancelled_by='client', 
               cancelled_at=CURRENT_TIMESTAMP 
               WHERE client_id=? AND status IN ('waiting', 'accepted', 'driver_arrived')''',
            (order_user_id,)
        )
        
        # Delete the client order
        direction = client[3]
        order_number = client[14] if len(client) > 14 else 1
        
        await db.execute("DELETE FROM clients WHERE user_id=?", (order_user_id,))
        
        # Reorder queue positions
        async with db.execute(
            '''SELECT user_id FROM clients 
               WHERE direction=? ORDER BY queue_position''',
            (direction,)
        ) as cursor:
            clients = await cursor.fetchall()
        
        for pos, client_id in enumerate(clients, 1):
            await db.execute(
                "UPDATE clients SET queue_position=? WHERE user_id=?",
                (pos, client_id[0])
            )
    
    await save_log_action(
        parent_user_id,
        "order_cancelled",
        f"Order #{order_number}, Cancellation #{new_count}"
    )
    
    # Blocking logic
    if new_count == 1:
        await callback.answer(
            "⚠️ ЕСКЕРТУ! Екінші тапсырыс жойылған жағдайда, сіз бұғатталасыз!",
            show_alert=True
        )
    elif new_count >= 2:
        reason = f"Тапсырыстарды жиі жою: ({new_count} рет)"
        await add_to_blacklist(parent_user_id, reason, new_count)
        
        await callback.message.edit_text(
            "🚫 <b>СІЗ БҰҒАТТАЛДЫҢЫЗ</b>\n\n"
            f"Себеп: {reason}\n\n"
            "Бұғаттан шығу үшін админге хабарласыңыз.",
            parse_mode="HTML"
        )
        return

    # Show remaining orders or notify no active orders
    remaining_orders = await count_user_orders(parent_user_id)
    
    if remaining_orders > 0:
        await view_my_orders(callback)
    else:
        await callback.message.edit_text(
            "❌ Тапсырыс жойылды\n\n"
            "Сіздің белсенді тапсырыстарыңыз жоқ.",
            parse_mode="HTML"
        )

@dp.callback_query(ClientOrder.from_city, F.data.startswith("from_"))
async def client_from_city(callback: types.CallbackQuery, state: FSMContext):
    city_map = {
        "from_aktau": "Ақтау",
        "from_janaozen": "Жаңаөзен",
        "from_shetpe": "Шетпе"
    }
    from_city = city_map[callback.data]
    await state.update_data(from_city=from_city)

    # Add keyboard for destination city
    await callback.message.edit_text(
        f"✅ Қайдан: {from_city}\n\nҚайда барасыз?",
        reply_markup=to_city_keyboard(from_city)
    )
    await state.set_state(ClientOrder.to_city)
    await callback.answer()
    
@dp.callback_query(ClientOrder.to_city, F.data.startswith("to_"))
async def client_to_city(callback: types.CallbackQuery, state: FSMContext):
    city_map = {
        "to_aktau": "Ақтау",
        "to_janaozen": "Жаңаөзен",
        "to_shetpe": "Шетпе"
    }
    
    to_city = city_map[callback.data]
    data = await state.get_data()
    
    direction = f"{data['from_city']} → {to_city}"
    await state.update_data(to_city=to_city, direction=direction)
    
    # Show available drivers and seats
    async with get_db() as db:
        async with db.execute('''SELECT COUNT(*), SUM(total_seats - occupied_seats) 
                     FROM drivers 
                     WHERE direction=? AND is_active=1''', (data['from_city'],)) as cursor:
            result = await cursor.fetchone()
    
    drivers_count = result[0] or 0
    available_seats = result[1] or 0
    
    await callback.message.edit_text(
        f"✅ Маршрут: {direction}\n\n"
        f"🚗 Бос жүргізушілер: {drivers_count}\n"
        f"💺 Бос орындар: {available_seats}\n\n"
        f"👥 Қанша орын керек? (1-8)"
    )
    await state.set_state(ClientOrder.passengers_count)
    await callback.answer()

@dp.callback_query(F.data == "back_from_city")
async def back_from_city(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Қай қаладан шығасыз?",
        reply_markup=from_city_keyboard()
    )
    await state.set_state(ClientOrder.from_city)
    await callback.answer()

@dp.message(ClientOrder.passengers_count)
async def client_passengers_count(message: types.Message, state: FSMContext):
    try:
        count = int(message.text)
        if count < 1 or count > 8:
            await message.answer("Қате! 1-ден 8-ге дейінгі сандды енгізіңіз")
            return
        
        data = await state.get_data()
        from_city, to_city = data.get("from_city"), data.get("to_city")

        if {"Ақтау", "Шетпе"} == {from_city, to_city}:
            price = 2000
        elif {"Ақтау", "Жаңаөзен"} == {from_city, to_city}:
            price = 2500
        else:
            price = 0

        await message.answer(f"💰 Баға: {price} теңге")

        
        # Check suitable cars
        async with get_db() as db:
            async with db.execute('''SELECT COUNT(*) 
                         FROM drivers 
                         WHERE direction=? AND is_active=1 
                         AND (total_seats - occupied_seats) >= ?''',
                      (data['from_city'], count)) as cursor:
                suitable_cars = (await cursor.fetchone())[0]
        
        await state.update_data(passengers_count=count)

        
        # Skip address questions and go directly to "order for whom"
        await state.set_state(ClientOrder.order_for)
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👤 Маған", callback_data="order_for_self")],
            [InlineKeyboardButton(text="👥 Басқа адамға", callback_data="order_for_other")]
        ])
        
        if suitable_cars == 0:
            await message.answer(
                f"⚠️ Жолаушылар саны: {count}\n"
                f"⚠️ Қазір {count} бос орны бар көліктер жоқ\n\n"
                f"Бірақ сіздің тапсырысыңыз сақталады!\n"
                f"Жүргізушілер оны бос орындар пайда болғанда көретін болады.\n\n"
                f"👤 <b>Бұл тапсырыс кімге?</b>",
                reply_markup=keyboard,
                parse_mode="HTML"
            )
        else:
            await message.answer(
                f"✅ Жолаушылар саны: {count}\n"
                f"🚗 Бос жүргізушілер: {suitable_cars}\n\n"
                f"👤 <b>Бұл тапсырыс кімге?</b>",
                reply_markup=keyboard,
                parse_mode="HTML"
            )
    except ValueError:
        await message.answer("Сан енгізіңіз!")
        
@dp.message(ClientOrder.passengers_count)
async def ask_order_for(message: types.Message, state: FSMContext):
    """After entering number of passengers, ask who the order is for"""
    try:
        count = int(message.text)
        if count < 1 or count > 8:
            await message.answer("Қате! 1-ден 8-ге дейінгі санды енгізіңіз")
            return

        await state.update_data(passengers_count=count)

        # Calculate price
        data = await state.get_data()
        from_city, to_city = data.get("from_city"), data.get("to_city")
        if {"Ақтау", "Шетпе"} == {from_city, to_city}:
            price = 2000
        elif {"Ақтау", "Жаңаөзен"} == {from_city, to_city}:
            price = 2500
        else:
            price = 0

        await message.answer(f"💰 Баға: {price} теңге")

        # Ask who the order is for
        await state.set_state(ClientOrder.order_for)
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👤 Маған", callback_data="order_for_self")],
            [InlineKeyboardButton(text="👥 Басқа адамға", callback_data="order_for_other")]
        ])
        await message.answer(
            "👤 <b>Бұл тапсырыс кімге?</b>",
            reply_markup=keyboard,
            parse_mode="HTML"
        )

    except ValueError:
        await message.answer("Сан енгізіңіз!")


@dp.callback_query(ClientOrder.order_for, F.data == "order_for_self")
async def order_for_self(callback: types.CallbackQuery, state: FSMContext):
    """Order for self"""
    await state.update_data(order_for="Маған")
    await callback.answer()
    await finalize_order(callback, state)

@dp.callback_query(ClientOrder.order_for, F.data == "order_for_other")
async def order_for_other(callback: types.CallbackQuery, state: FSMContext):
    """Order for another person"""
    await callback.message.edit_text(
        "👥 <b>Жолаушы деректері</b>\n\n"
        "Жолаушының атын енгізіңіз:",
        parse_mode="HTML"
    )
    await state.set_state(ClientOrder.passenger_name)
    await callback.answer()
    
@dp.message(ClientOrder.passenger_name, F.text)
async def save_passenger_name(message: types.Message, state: FSMContext):
    """Save passenger name and ask for phone"""
    await state.update_data(passenger_name=message.text)
    await message.answer(
        "📱 Жолаушының телефон нөмірін енгізіңіз:\n"
        "(мысалы: +7 777 123 45 67)"
    )
    await state.set_state(ClientOrder.passenger_phone)

@dp.message(ClientOrder.passenger_phone, F.text)
async def save_passenger_phone(message: types.Message, state: FSMContext):
    """Save passenger phone and finalize order"""
    data = await state.get_data()
    passenger_info = f"{data['passenger_name']} ({message.text.strip()})"
    await state.update_data(order_for=passenger_info, passenger_phone=message.text.strip())
    await finalize_order_from_message(message, state)

@dp.message(ClientOrder.order_for, F.text)
async def save_order_for_name(message: types.Message, state: FSMContext):
    """Save the name of the person for whom the order is made"""
    await state.update_data(order_for=message.text)
    await finalize_order_from_message(message, state)

async def finalize_order(callback: types.CallbackQuery, state: FSMContext):
    """Finalize the order and offer to add another"""
    data = await state.get_data()
    
    # Count existing orders to assign order number
    current_orders = await count_user_orders(callback.from_user.id)
    order_number = current_orders + 1
    
    async with get_db(write=True) as db:
        # Set queue position
        async with db.execute(
            "SELECT MAX(queue_position) FROM clients WHERE direction=?",
            (data['direction'],)
        ) as cursor:
            max_pos = (await cursor.fetchone())[0]
        
        queue_pos = (max_pos or 0) + 1
        
        # Delete old entry if exists (client may be registered but without active orders)
        await db.execute('DELETE FROM clients WHERE user_id=? AND status="registered"', (callback.from_user.id,))
        
        # Add client
        unique_order_id = int(f"{callback.from_user.id}{int(time.time() * 1000) % 100000}")

        await db.execute('''INSERT INTO clients 
            (user_id, full_name, phone, direction, from_city, to_city, 
             queue_position, passengers_count, 
             is_verified, status, order_for, order_number, parent_user_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 'waiting', ?, ?, ?)''',
             (unique_order_id, 
             callback.from_user.full_name or "Клиент",
             f"@{callback.from_user.username}" if callback.from_user.username else f"tg_{callback.from_user.id}",
             data['direction'],
             data['from_city'],
             data['to_city'],
             queue_pos, 
             data['passengers_count'],
             data['order_for'],
             order_number,
             callback.from_user.id))

        
        # Check suitable drivers
        async with db.execute(
            '''SELECT COUNT(*) FROM drivers 
               WHERE direction=? AND is_active=1 
               AND (total_seats - COALESCE(occupied_seats, 0)) >= ?''',
            (data['from_city'], data['passengers_count'])
        ) as cursor:
            suitable = (await cursor.fetchone())[0]
        
        # Get drivers to notify
        async with db.execute(
            '''SELECT user_id FROM drivers 
               WHERE direction=? AND is_active=1 
               AND (total_seats - COALESCE(occupied_seats, 0)) >= ?''',
            (data['from_city'], data['passengers_count'])
        ) as cursor:
            drivers = await cursor.fetchall()
    
    await save_log_action(
        callback.from_user.id,
        "order_created",
        f"Order #{order_number} for {data['order_for']}"
    )
    
    # Notify drivers
    for driver in drivers:
        try:
            await bot.send_message(
                driver[0],
                f"🔔 <b>:Жаңа тапсырыс!</b>\n\n"
                f"👥 Жолаушылар саны: {data['passengers_count']}\n"
                f"📍 {data['from_city']} → {data['to_city']}\n"
                f"Кімге: {data['order_for']}\n\n"
                f"Тапсырыстарды тексеріңіз: /driver",
                parse_mode="HTML"
            )
        except:
            pass
    
    # Offer to add another order
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Жаңа тапсырып жасау", callback_data="add_another_yes")],
        [InlineKeyboardButton(text="✅ Аяқтау", callback_data="add_another_no")]
    ])
    
    await callback.message.edit_text(
        f"✅ <b>Тапсырыс #{order_number} жасалды!</b>\n\n"
        f"📍 {data['from_city']} → {data['to_city']}\n"
        f"👤 Кімге: {data['order_for']}\n"
        f"👥 Жолаушылар саны: {data['passengers_count']}\n"
        f"📍 Қайдан: {data['from_city']}\n"
        f"📍 Қайда: {data['to_city']}\n"
        f"📊 Кезектегі орын: №{queue_pos}\n\n"
        f"🚗 Бос жүргізушілер: {suitable}\n\n"
        f"Тағы бір тапсырыс жасағыңыз келеді ме?",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await state.set_state(ClientOrder.add_another)

async def finalize_order_from_message(message: types.Message, state: FSMContext):
    """Same as finalize_order but from message context"""
    data = await state.get_data()
    current_orders = await count_user_orders(message.from_user.id)
    order_number = current_orders + 1
    
    async with get_db(write=True) as db:
        async with db.execute(
            "SELECT MAX(queue_position) FROM clients WHERE direction=?",
            (data['direction'],)
        ) as cursor:
            max_pos = (await cursor.fetchone())[0]
        
        queue_pos = (max_pos or 0) + 1
        
        # Delete old entry if exists
        await db.execute('DELETE FROM clients WHERE user_id=? AND status="registered"', 
                (message.from_user.id,))

        # Add client
        unique_order_id = int(f"{message.from_user.id}{int(time.time() * 1000) % 100000}")

        await db.execute('''INSERT INTO clients 
            (user_id, full_name, phone, direction, from_city, to_city, 
             queue_position, passengers_count, 
             is_verified, status, order_for, order_number, parent_user_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 'waiting', ?, ?, ?)''',
                (unique_order_id, 
                message.from_user.full_name or "Клиент",
                f"@{message.from_user.username}" if message.from_user.username else f"tg_{message.from_user.id}",
                data['direction'],
                data['from_city'],
                data['to_city'],
                queue_pos, 
                data['passengers_count'],
                data['order_for'],
                order_number,
                message.from_user.id))
        
        async with db.execute(
            '''SELECT COUNT(*) FROM drivers 
               WHERE direction=? AND is_active=1 
               AND (total_seats - COALESCE(occupied_seats, 0)) >= ?''',
            (data['from_city'], data['passengers_count'])
        ) as cursor:
            suitable = (await cursor.fetchone())[0]
        
        async with db.execute(
            '''SELECT user_id FROM drivers 
               WHERE direction=? AND is_active=1 
               AND (total_seats - COALESCE(occupied_seats, 0)) >= ?''',
            (data['from_city'], data['passengers_count'])
        ) as cursor:
            drivers = await cursor.fetchall()
    
    await save_log_action(
        message.from_user.id,
        "order_created",
        f"Order #{order_number} for {data['order_for']}"
    )
    
    for driver in drivers:
        try:
            await bot.send_message(
                driver[0],
                f"🔔 <b>Жаңа тапсырыс!</b>\n\n"
                f"👥 Жолаушылар саны: {data['passengers_count']}\n"
                f"📍 {data['direction']}\n"
                f"Кімге: {data['order_for']}\n\n"
                f"Тапсырыстарды тексеріңіз: /driver",
                parse_mode="HTML"
            )
        except:
            pass
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Жаңа тапсырыс жасау", callback_data="add_another_yes")],
        [InlineKeyboardButton(text="✅ Аяқтау", callback_data="add_another_no")]
    ])
    
    await message.answer(
        f"✅ <b>Тапсырыс #{order_number} жасалды!</b>\n\n"
        f"📍 {data['direction']}\n"
        f"👤 Кімге: {data['order_for']}\n"
        f"👥 Жолаушылар саны: {data['passengers_count']}\n"
        f"📍 {data['direction']}\n"
        f"📊 Кезектгі орын: №{queue_pos}\n\n"
        f"🚗 Бос жүргізушілер: {suitable}\n\n"
        f"Тағы бір тапсырыс жасағыңыз келеді ме?",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await state.set_state(ClientOrder.add_another)

@dp.callback_query(ClientOrder.add_another, F.data == "add_another_yes")
async def add_another_order_yes(callback: types.CallbackQuery, state: FSMContext):
    """Add another taxi order"""
    await callback.message.edit_text(
        "🧍‍♂️ <b>Жаңа тапсырыс</b>\n\n"
        "Қай қаладан шығасыз?",
        reply_markup=from_city_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(ClientOrder.from_city)
    await callback.answer()

@dp.callback_query(ClientOrder.add_another, F.data == "add_another_no")
async def add_another_order_no(callback: types.CallbackQuery, state: FSMContext):
    """End order process"""
    total_orders = await count_user_orders(callback.from_user.id)
    
    await callback.message.edit_text(
        f"✅ <b>Дайын!</b>\n\n"
        f"Сіздің {total_orders} белсенді тапсырысыңыз бар.\n\n"
        f"Статусты қарау үшін /driver командасын пайдаланыңыз.",
        parse_mode="HTML"
    )
    await state.clear()
    await callback.answer()

# ==================== RATINGS ====================

@dp.message(F.text == "⭐ Профиль")
async def show_profile(message: types.Message):
    async with get_db() as db:
        async with db.execute("SELECT avg_rating, rating_count FROM drivers WHERE user_id=?", (message.from_user.id,)) as cursor:
            driver = await cursor.fetchone()
        
        async with db.execute("SELECT avg_rating, rating_count FROM clients WHERE user_id=?", (message.from_user.id,)) as cursor:
            client = await cursor.fetchone()
    
    if not driver and not client:
        await message.answer("❌ Сіздің профиліңіз табылмады.")
        return

    msg = "⭐ <b>Сіздің профиліңіз</b>\n\n"

    if driver:
        msg += f"<b>Жүргізуші ретінде:</b>\n"
        msg += f"{get_rating_stars(driver[0] or 0)}\n"
        msg += f"📊 Бағалар: {driver[1] or 0}\n\n"

    if client:
        msg += f"<b>Клиент ретінде:</b>\n"
        msg += f"{get_rating_stars(client[0] or 0)}\n"
        msg += f"📊 Бағалар: {client[1] or 0}\n\n"

    async with get_db() as db:
        async with db.execute('''SELECT from_user_id, rating, review, created_at 
                     FROM ratings WHERE to_user_id=? 
                     ORDER BY created_at DESC LIMIT 5''', (message.from_user.id,)) as cursor:
            reviews = await cursor.fetchall()
    
    if reviews:
        msg += "<b>Соңғы пікірлер:</b>\n"
        for review in reviews:
            stars = "⭐" * review[1]
            msg += f"\n{stars}\n"
            if review[2]:
                msg += f"💬 {review[2]}\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✍️ Пікір қалдыру", callback_data="rate_start")]
    ])
    
    await message.answer(msg, reply_markup=keyboard, parse_mode="HTML")

@dp.callback_query(F.data == "rate_start")
async def rate_start(callback: types.CallbackQuery, state: FSMContext):
    async with get_db() as db:
        async with db.execute('''SELECT t.id, t.driver_id, d.full_name, t.client_id
                     FROM trips t
                     JOIN drivers d ON t.driver_id = d.user_id
                     WHERE (t.driver_id=? OR t.client_id=?)
                     AND t.status='completed'
                     AND t.id NOT IN (SELECT trip_id FROM ratings WHERE from_user_id=? AND trip_id IS NOT NULL)
                     ORDER BY t.trip_completed_at DESC LIMIT 5''',
                  (callback.from_user.id, callback.from_user.id, callback.from_user.id)) as cursor:
            trips = await cursor.fetchall()
    
    if not trips:
        await callback.answer("❌ Сапар табылмады", show_alert=True)
        return
    
    keyboard_buttons = []
    for trip in trips:
        is_driver = trip[1] == callback.from_user.id
        target_name = "Клиентті" if is_driver else f"Жүргізушіні {trip[2]}"
        keyboard_buttons.append([
            InlineKeyboardButton(
                text=f"бағалау {target_name}",
                callback_data=f"rate_trip_{trip[0]}"
            )
        ])
    
    await callback.message.edit_text(
        "✍️ <b>Сапарды бағалауды таңдаңыз:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons),
        parse_mode="HTML"
    )
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
    
    await callback.message.edit_text(
        "⭐ Бағаны таңдаңыз:",
        reply_markup=keyboard
    )
    await state.set_state(RatingStates.select_rating)
    await callback.answer()

@dp.callback_query(RatingStates.select_rating, F.data.startswith("rating_"))
async def save_rating(callback: types.CallbackQuery, state: FSMContext):
    rating = int(callback.data.split("_")[1])
    await state.update_data(rating=rating)
    
    await callback.message.edit_text(
        f"{'⭐' * rating}\n\n"
        "Пікір қалдыру (немесе өткізіп жіберу үшін /skip):"
    )
    await state.set_state(RatingStates.write_review)
    await callback.answer()

@dp.message(RatingStates.write_review)
async def save_review(message: types.Message, state: FSMContext):
    data = await state.get_data()
    review = None if message.text == "/skip" else message.text
    
    async with get_db(write=True) as db:
        async with db.execute('''SELECT driver_id, client_id FROM trips WHERE id=?''', (data['trip_id'],)) as cursor:
            trip = await cursor.fetchone()
        
        is_driver = trip[0] == message.from_user.id
        target_id = trip[1] if is_driver else trip[0]
        user_type = "driver" if not is_driver else "client"
        
        await db.execute('''INSERT INTO ratings (from_user_id, to_user_id, user_type, trip_id, rating, review)
                     VALUES (?, ?, ?, ?, ?, ?)''',
                  (message.from_user.id, target_id, user_type, data['trip_id'], data['rating'], review))
        
        table = "drivers" if user_type == "driver" else "clients"
        await db.execute(f'''UPDATE {table} 
                      SET avg_rating = (SELECT AVG(rating) FROM ratings WHERE to_user_id=?),
                          rating_count = (SELECT COUNT(*) FROM ratings WHERE to_user_id=?)
                      WHERE user_id=?''',
                  (target_id, target_id, target_id))
    
    await save_log_action(message.from_user.id, "rating_submitted", 
                   f"Target: {target_id}, Rating: {data['rating']}")
    
    await message.answer(
        f"✅ Пікір қалдырғаныңға рақмет!\n\n"
        f"{'⭐' * data['rating']}",
        reply_markup=main_menu_keyboard()
    )
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
        parse_mode="HTML"
    )

@dp.message(Command("driver"))
async def cmd_driver(message: types.Message):
    """Driver menu shortcut"""
    await show_driver_menu(message, message.from_user.id)

@dp.message(Command("rate"))
async def cmd_rate(message: types.Message, state: FSMContext):
    """Rate menu shortcut"""
    async with get_db() as db:
        async with db.execute('''SELECT t.id, t.driver_id, d.full_name, t.client_id
                     FROM trips t
                     JOIN drivers d ON t.driver_id = d.user_id
                     WHERE (t.driver_id=? OR t.client_id=?)
                     AND t.status='completed'
                     AND t.id NOT IN (SELECT trip_id FROM ratings WHERE from_user_id=? AND trip_id IS NOT NULL)
                     ORDER BY t.trip_completed_at DESC LIMIT 5''',
                  (message.from_user.id, message.from_user.id, message.from_user.id)) as cursor:
            trips = await cursor.fetchall()

    if not trips:
        await message.answer("❌ Сапар табылмады")
        return

    keyboard_buttons = []
    for trip in trips:
        is_driver = trip[1] == message.from_user.id
        target_name = "Клиентті" if is_driver else f"Жүргізушіні {trip[2]}"
        keyboard_buttons.append([
            InlineKeyboardButton(
                text=f"Бағалау {target_name}",
                callback_data=f"rate_trip_{trip[0]}"
            )
        ])

    await message.answer(
        "✍️ <b>Қай сапарды бағалағыңыз келеді:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons),
        parse_mode="HTML"
    )

@dp.message(F.text == "ℹ️ Ақпарат")
async def info_command(message: types.Message):
    await message.answer(
        "ℹ️ <b>Біз туралы</b>\n\n"
        "🚖 Такси тапсырыс беру жүйесі\n\n"
        "Жылдам және оңай! ⚡",
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "back_main")
async def back_main(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.message.answer(
        "Басты меню:",
        reply_markup=main_menu_keyboard()
    )
    await callback.answer()

# ==================== ADMIN ====================

def admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Жүргізушілер", callback_data="admin_drivers")],
        [InlineKeyboardButton(text="🧍‍♂️ Клиенттер", callback_data="admin_clients")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="📜 Логтар", callback_data="admin_logs")],
        [InlineKeyboardButton(text="🔙 Артқа", callback_data="back_main")]
    ])

@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Тыйым салынған")
        return
    
    await message.answer(
        "🔐 <b>Админ панелі</b>",
        reply_markup=admin_keyboard(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "admin_drivers")
async def admin_drivers(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Тыйым салынған", show_alert=True)
        return
    
    async with get_db() as db:
        async with db.execute("SELECT * FROM drivers ORDER BY direction, queue_position") as cursor:
            drivers = await cursor.fetchall()
    
    if not drivers:
        msg = "❌ Жүргізушілер жоқ"
    else:
        msg = "👥 <b>Жүргізушілер:</b>\n\n"
        for driver in drivers:
            occupied, total, available = await get_driver_available_seats(driver[0])
            msg += f"№{driver[7]} - {driver[1]}\n"
            msg += f"   🚗 {driver[4]} ({driver[3]})\n"
            msg += f"   💺 {occupied}/{total} (бос: {available})\n"
            msg += f"   📍 {driver[6]}\n"
            msg += f"   {get_rating_stars(driver[13] if len(driver) > 13 else 0)}\n\n"
    
    await callback.message.edit_text(msg, reply_markup=admin_keyboard(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_clients")
async def admin_clients(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Тыйым салынған", show_alert=True)
        return
    
    async with get_db() as db:
        async with db.execute("SELECT * FROM clients ORDER BY direction, queue_position") as cursor:
            clients = await cursor.fetchall()
    
    if not clients:
        msg = "❌ Клиенттер жоқ"
    else:
        msg = "🧍‍♂️ <b>Кезектегі клиенттер:</b>\n\n"
        for client in clients:
            status_emoji = {"waiting": "⏳", "accepted": "✅", "driver_arrived": "🚗"}
            msg += f"№{client[4]} {status_emoji.get(client[10], '❓')} - {client[1]}\n"
            msg += f"   📍 {client[3]}\n"
            msg += f"   👥 {client[5]} адам.\n"
            msg += f"   Қайдан: {client[6]}\n"
            msg += f"   Қайда: {client[7]}\n"
            if client[11]:
                msg += f"   🚗 Жүргізуші: ID {client[11]}\n"
            msg += "\n"
    
    await callback.message.edit_text(msg, reply_markup=admin_keyboard(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Тыйым салынған", show_alert=True)
        return
    
    async with get_db() as db:
        async with db.execute("SELECT COUNT(*) FROM drivers") as cursor:
            total_drivers = (await cursor.fetchone())[0]
        
        async with db.execute("SELECT COUNT(*) FROM clients WHERE status='waiting'") as cursor:
            waiting_clients = (await cursor.fetchone())[0]
        
        async with db.execute("SELECT COUNT(*) FROM clients WHERE status='accepted'") as cursor:
            accepted_clients = (await cursor.fetchone())[0]
        
        async with db.execute("SELECT COUNT(*) FROM trips WHERE status='completed'") as cursor:
            completed_trips = (await cursor.fetchone())[0]
        
        async with db.execute("SELECT COUNT(*) FROM trips WHERE cancelled_by IS NOT NULL") as cursor:
            cancelled_trips = (await cursor.fetchone())[0]
        
        async with db.execute("SELECT AVG(rating) FROM ratings") as cursor:
            avg_rating = (await cursor.fetchone())[0] or 0
        
        async with db.execute("SELECT SUM(total_seats - COALESCE(occupied_seats, 0)) FROM drivers WHERE is_active=1") as cursor:
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
    
    await callback.message.edit_text(msg, reply_markup=admin_keyboard(), parse_mode="HTML")
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
    
    await callback.message.edit_text(msg, reply_markup=admin_keyboard(), parse_mode="HTML")
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
            await db.execute("INSERT INTO admins (user_id) VALUES (?)", (new_admin_id,))
        
        await save_log_action(message.from_user.id, "admin_added", f"New admin: {new_admin_id}")
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
               FROM blacklist ORDER BY banned_at DESC'''
        ) as cursor:
            blacklist = await cursor.fetchall()
    
    if not blacklist:
        await message.answer("✅ Қара тізім бос")
        return

    msg = "🚫 <b>Қара тізім:</b>\n\n"
    for entry in blacklist:
        try:
            banned_time = datetime.fromisoformat(entry[3]).strftime("%Y-%m-%d %H:%M")
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
            await db.execute("DELETE FROM blacklist WHERE user_id=?", (user_id,))

            # Reset cancellation count
            await db.execute(
                "UPDATE clients SET cancellation_count=0 WHERE user_id=?",
                (user_id,)
            )
        
        await save_log_action(
            message.from_user.id, 
            "user_unbanned", 
            f"Unbanned user: {user_id}"
        )

        await message.answer(f"✅ Пайдаланушы {user_id} қара тізімнен шығарылды.")

        # Notify user
        try:
            await bot.send_message(
                user_id,
                "✅ <b>Сіз қара тізімнен шығарылдыңыз!</b>\n\n"
                "Енді сіз қайтадан тапсырыс бере аласыз.\n"
                "Өтініш, тапсырысты жауапкершілікпен жасаңыз.",
                parse_mode="HTML"
            )
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
        await message.answer("Осы команданы пайдаланыңыз: /resetcancel USER_ID")
        return
    
    try:
        user_id = int(parts[1])
        
        async with get_db(write=True) as db:
            await db.execute(
                "UPDATE clients SET cancellation_count=0 WHERE user_id=?",
                (user_id,)
            )
        
        await save_log_action(
            message.from_user.id,
            "cancellation_reset",
            f"Reset for user: {user_id}"
        )

        await message.answer(f"✅ Санақ {user_id} жойылды.")

    except ValueError:
        await message.answer("❌ Қате USER_ID")
    except Exception as e:
        await message.answer(f"❌ Қате: {e}")
        
@dp.message()
async def handle_unknown(message: types.Message):
    logger.warning(f"Unhandled message from {message.from_user.id}: {message.text}")
    await message.answer(
        "❓ <b>Мен бұл команданы түсінбедім.</b>\n\n"
        "Меню батырмаларын пайдаланыңыз:",
        reply_markup=main_menu_keyboard()
    )

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