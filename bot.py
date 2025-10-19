import asyncio
import sqlite3
import aiosqlite
import os
import logging
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

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
DATABASE_FILE = os.getenv("DATABASE_FILE", "taxi_bot.db")
DB_TIMEOUT = 10.0
SMS_API_KEY = os.getenv("SMS_API_KEY", "")

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Connection pool
db_lock = asyncio.Lock()

@asynccontextmanager
async def get_db(write=False):
    """
    Async context manager for database connections
    write=True: uses exclusive lock for writes
    write=False: allows concurrent reads
    """
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

# ==================== ЛОГИРОВАНИЕ ====================

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

# ==================== УТИЛИТЫ SMS ====================

def generate_verification_code() -> str:
    return ''.join(random.choices(string.digits, k=4))

async def send_sms(phone: str, message: str) -> bool:
    logger.info(f"SMS отправлено на {phone}: {message}")
    return True

# ==================== БД И МИГРАЦИИ ====================

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
        logger.info("Применяю миграцию v1...")
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
                      pickup_location TEXT NOT NULL,
                      dropoff_location TEXT NOT NULL,
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
        logger.info("Миграция v1 завершена")
    
    @staticmethod
    def migration_v2():
        logger.info("Применяю миграцию v2...")
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
        logger.info("Миграция v2 завершена")
    
    @staticmethod
    def migration_v3():
        logger.info("Применяю миграцию v3...")
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        c.execute('''CREATE TABLE IF NOT EXISTS trips
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      driver_id INTEGER,
                      client_id INTEGER,
                      direction TEXT,
                      pickup_location TEXT,
                      dropoff_location TEXT,
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
        logger.info("Миграция v3 завершена")
    
    @staticmethod
    def migration_v4():
        """Миграция v4: Добавляем occupied_seats для водителей"""
        logger.info("Применяю миграцию v4...")
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
        logger.info("Миграция v4 завершена")

    @staticmethod
    def migration_v5():
        """Миграция v5: Добавляем систему черного списка"""
        logger.info("Применяю миграцию v5...")
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
    
        # Таблица черного списка
        c.execute('''CREATE TABLE IF NOT EXISTS blacklist
                  (user_id INTEGER PRIMARY KEY,
                   reason TEXT,
                   cancellation_count INTEGER DEFAULT 0,
                   banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
        # Добавляем счетчик отмен к клиентам
        c.execute("PRAGMA table_info(clients)")
        client_columns = [column[1] for column in c.fetchall()]
    
        if 'cancellation_count' not in client_columns:
            c.execute("ALTER TABLE clients ADD COLUMN cancellation_count INTEGER DEFAULT 0")
    
        conn.commit()
        conn.close()
        DBMigration.set_db_version(5)
        logger.info("Миграция v5 завершена")
    @staticmethod
    def migration_v6():
        """Миграция v6: Поддержка множественных заказов"""
        logger.info("Применяю миграцию v6...")
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
    
        # Добавляем поле order_for к клиентам (для кого заказ)
        c.execute("PRAGMA table_info(clients)")
        client_columns = [column[1] for column in c.fetchall()]
    
        if 'order_for' not in client_columns:
            c.execute("ALTER TABLE clients ADD COLUMN order_for TEXT DEFAULT 'self'")
    
        if 'order_number' not in client_columns:
            c.execute("ALTER TABLE clients ADD COLUMN order_number INTEGER DEFAULT 1")
    
        if 'parent_user_id' not in client_columns:
            c.execute("ALTER TABLE clients ADD COLUMN parent_user_id INTEGER")
    
        conn.commit()
        conn.close()
        DBMigration.set_db_version(6)
        logger.info("Миграция v6 завершена")

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

# ==================== УТИЛИТЫ ====================

async def is_admin(user_id: int) -> bool:
    """Check if user is admin - ASYNC VERSION"""
    async with get_db() as db:
        async with db.execute(
            "SELECT user_id FROM admins WHERE user_id=?", 
            (user_id,)
        ) as cursor:
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
    """Возвращает (занято мест, всего мест, свободно мест) - ASYNC VERSION"""
    async with get_db() as db:
        # Проверяем наличие колонок
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
    Проверяет, находится ли пользователь в черном списке
    Возвращает (is_banned: bool, reason: str)
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
    """Получает количество отмен для клиента"""
    async with get_db() as db:
        async with db.execute(
            "SELECT COALESCE(cancellation_count, 0) FROM clients WHERE user_id=?",
            (user_id,)
        ) as cursor:
            result = await cursor.fetchone()
            return result[0] if result else 0

async def add_to_blacklist(user_id: int, reason: str, cancellation_count: int):
    """Добавляет пользователя в черный список"""
    async with get_db(write=True) as db:
        await db.execute(
            '''INSERT OR REPLACE INTO blacklist (user_id, reason, cancellation_count)
               VALUES (?, ?, ?)''',
            (user_id, reason, cancellation_count)
        )
    await save_log_action(user_id, "blacklisted", reason)
    
async def get_user_active_orders(user_id: int) -> list:
    """Получает все активные заказы пользователя"""
    async with get_db() as db:
        async with db.execute(
            '''SELECT user_id, full_name, order_for, order_number, status, 
                      direction, passengers_count, pickup_location, dropoff_location,
                      assigned_driver_id
               FROM clients 
               WHERE parent_user_id=? OR user_id=?
               ORDER BY order_number''',
            (user_id, user_id)
        ) as cursor:
            return await cursor.fetchall()

async def count_user_orders(user_id: int) -> int:
    """Считает количество активных заказов пользователя"""
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
            [KeyboardButton(text="🚗 Я водитель")],
            [KeyboardButton(text="🧍‍♂️ Мне нужно такси")],
            [KeyboardButton(text="⭐ Мой профиль")],
            [KeyboardButton(text="ℹ️ Информация")]
        ],
        resize_keyboard=True
    )

def from_city_keyboard():
    """Выбор города отправления"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Ақтау", callback_data="from_aktau")],
            [InlineKeyboardButton(text="Жаңаөзен", callback_data="from_janaozen")],
            [InlineKeyboardButton(text="Шетпе", callback_data="from_shetpe")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_main")]
        ]
    )

def to_city_keyboard(from_city: str):
    """Выбор города назначения (исключая город отправления)"""
    cities = {
        "Ақтау": "aktau",
        "Жаңаөзен": "janaozen", 
        "Шетпе": "shetpe"
    }
    
    buttons = []
    for city_name, city_code in cities.items():
        if city_name != from_city:
            buttons.append([InlineKeyboardButton(
                text=city_name, 
                callback_data=f"to_{city_code}"
            )])
    
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_from_city")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_rating_stars(rating: float) -> str:
    if not rating:
        return "❌ Нет оценок"
    stars = int(rating)
    return "⭐" * stars + "☆" * (5 - stars)

# ==================== ВОДИТЕЛИ ====================

class DriverReg(StatesGroup):
    phone = State()
    verify_code = State()
    full_name = State()
    car_number = State()
    car_model = State()
    seats = State()
    direction = State()

@dp.message(F.text == "🚗 Я водитель")
async def driver_start(message: types.Message, state: FSMContext):
    async with get_db() as db:
        async with db.execute("SELECT * FROM drivers WHERE user_id=?", (message.from_user.id,)) as cursor:
            driver = await cursor.fetchone()
    
    if driver and driver[9]:  # is_verified
        await show_driver_menu(message, message.from_user.id)
        return
    
    if driver and not driver[9]:
        await message.answer(
            "⏳ Вы уже зарегистрированы, но не верифицированы.\n"
            "Введите код из SMS:"
        )
        await state.set_state(DriverReg.verify_code)
        return
    
    await message.answer(
        "🚗 <b>Регистрация водителя</b>\n\n"
        "Введите номер телефона в формате +7XXXXXXXXXX:",
        parse_mode="HTML"
    )
    await state.set_state(DriverReg.phone)

@dp.message(DriverReg.phone)
async def driver_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    
    if not phone.startswith('+7') or len(phone) != 12:
        await message.answer("❌ Неверный формат! Используйте +7XXXXXXXXXX")
        return
    
    async with get_db() as db:
        async with db.execute("SELECT user_id FROM drivers WHERE phone=?", (phone,)) as cursor:
            existing = await cursor.fetchone()
    
    if existing:
        await message.answer("❌ Этот номер уже зарегистрирован!")
        return
    
    code = generate_verification_code()
    await state.update_data(phone=phone, verification_code=code)
    
    await send_sms(phone, f"Код подтверждения: {code}")
    
    await message.answer(
        f"✅ SMS отправлено на {phone}\n\n"
        "Введите код из SMS:"
    )
    await state.set_state(DriverReg.verify_code)

@dp.message(DriverReg.verify_code)
async def driver_verify(message: types.Message, state: FSMContext):
    data = await state.get_data()
    
    if message.text.strip() != data['verification_code']:
        await message.answer("❌ Неверный код!")
        return
    
    await message.answer("✅ Номер подтвержден!\n\nВведите ваше полное имя:")
    await state.set_state(DriverReg.full_name)

@dp.message(DriverReg.full_name)
async def driver_name(message: types.Message, state: FSMContext):
    await state.update_data(full_name=message.text)
    await message.answer("Номер авто (например: 870 ABC 09)")
    await state.set_state(DriverReg.car_number)

@dp.message(DriverReg.car_number)
async def driver_car_number(message: types.Message, state: FSMContext):
    await state.update_data(car_number=message.text)
    await message.answer("Марка авто (например: Toyota Camry)")
    await state.set_state(DriverReg.car_model)

@dp.message(DriverReg.car_model)
async def driver_car_model(message: types.Message, state: FSMContext):
    await state.update_data(car_model=message.text)
    await message.answer("Сколько мест в машине? (1-8)")
    await state.set_state(DriverReg.seats)

@dp.message(DriverReg.seats)
async def driver_seats(message: types.Message, state: FSMContext):
    try:
        seats = int(message.text)
        if seats < 1 or seats > 8:
            await message.answer("Ошибка! Мест должно быть от 1 до 8")
            return
        await state.update_data(seats=seats)
        await message.answer(
            "Выберите маршрут:",
            reply_markup=direction_keyboard()
        )
        await state.set_state(DriverReg.direction)
    except ValueError:
        await message.answer("Введите число!")

@dp.callback_query(DriverReg.direction, F.data.startswith("dir_"))
async def driver_direction(callback: types.CallbackQuery, state: FSMContext):
    direction = "Шетпе → Ақтау" if callback.data == "dir_shetpe_aktau" else "Ақтау → Шетпе"
    data = await state.get_data()
    
    async with get_db(write=True) as db:
        async with db.execute(
            "SELECT MAX(queue_position) FROM drivers WHERE direction=? AND is_active=1",
            (direction,)
        ) as cursor:
            max_pos = (await cursor.fetchone())[0]
        
        queue_pos = (max_pos or 0) + 1
        
        await db.execute('''INSERT INTO drivers 
                     (user_id, full_name, phone, car_number, car_model, total_seats, 
                      direction, queue_position, is_active, is_verified, occupied_seats)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 1, 0)''',
                  (callback.from_user.id, data['full_name'], data['phone'], data['car_number'],
                   data['car_model'], data['seats'], direction, queue_pos))
    
    await save_log_action(callback.from_user.id, "driver_registered", 
                   f"Direction: {direction}, Queue: {queue_pos}")
    
    await callback.message.edit_text(
        f"✅ <b>Вы зарегистрированы!</b>\n\n"
        f"👤 {data['full_name']}\n"
        f"🚗 {data['car_model']} ({data['car_number']})\n"
        f"💺 Мест: {data['seats']}\n"
        f"📍 {direction}\n"
        f"📊 Позиция: №{queue_pos}\n\n"
        f"⏳ Ждите клиентов!",
        parse_mode="HTML"
    )
    await state.clear()

async def show_driver_menu(message: types.Message, user_id: int):
    async with get_db() as db:
        # Получаем колонки для безопасной работы
        async with db.execute("PRAGMA table_info(drivers)") as cursor:
            columns = [col[1] for col in await cursor.fetchall()]
        
        async with db.execute("SELECT * FROM drivers WHERE user_id=?", (user_id,)) as cursor:
            driver = await cursor.fetchone()
    
    if not driver:
        await message.answer("Ошибка: вы не зарегистрированы", reply_markup=main_menu_keyboard())
        return
    
    # Безопасно получаем индексы
    full_name_idx = columns.index('full_name') if 'full_name' in columns else 1
    car_number_idx = columns.index('car_number') if 'car_number' in columns else 2
    car_model_idx = columns.index('car_model') if 'car_model' in columns else 3
    direction_idx = columns.index('direction') if 'direction' in columns else 6
    queue_pos_idx = columns.index('queue_position') if 'queue_position' in columns else 7
    
    # Проверяем наличие occupied_seats
    if 'occupied_seats' in columns and 'total_seats' in columns:
        occupied, total, available = await get_driver_available_seats(user_id)
        seats_text = f"💺 Места: {occupied}/{total} (свободно: {available})\n"
    else:
        total_seats_idx = columns.index('total_seats') if 'total_seats' in columns else 5
        total = driver[total_seats_idx] if len(driver) > total_seats_idx else 4
        seats_text = f"💺 Мест: {total}\n"
    
    # Рейтинг
    avg_rating_idx = columns.index('avg_rating') if 'avg_rating' in columns else None
    rating_text = get_rating_stars(driver[avg_rating_idx] if avg_rating_idx and len(driver) > avg_rating_idx else 0)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Мой статус", callback_data="driver_status")],
        [InlineKeyboardButton(text="👥 Мои пассажиры", callback_data="driver_passengers")],
        [InlineKeyboardButton(text="🔔 Доступные заказы", callback_data="driver_available_orders")],
        [InlineKeyboardButton(text="🚗 Я приехал!", callback_data="driver_arrived")],
        [InlineKeyboardButton(text="✅ Завершить поездку", callback_data="driver_complete_trip")],
        [InlineKeyboardButton(text="❌ Выйти из очереди", callback_data="driver_exit")],
        [InlineKeyboardButton(text="🔙 Меню", callback_data="back_main")]
    ])
    
    await message.answer(
        f"🚗 <b>Профиль водителя</b>\n\n"
        f"👤 {driver[full_name_idx]}\n"
        f"🚗 {driver[car_model_idx]} ({driver[car_number_idx]})\n"
        f"{seats_text}"
        f"📍 {driver[direction_idx]}\n"
        f"📊 Позиция: №{driver[queue_pos_idx]}\n"
        f"{rating_text}\n\n"
        "Выберите действие:",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "driver_status")
async def driver_status(callback: types.CallbackQuery):
    async with get_db() as db:
        async with db.execute("SELECT * FROM drivers WHERE user_id=?", (callback.from_user.id,)) as cursor:
            driver = await cursor.fetchone()
        
        async with db.execute("SELECT COUNT(*) FROM clients WHERE direction=? AND status='waiting'", (driver[6],)) as cursor:
            waiting = (await cursor.fetchone())[0]
    
    occupied, total, available = await get_driver_available_seats(callback.from_user.id)
    
    await callback.message.edit_text(
        f"📊 <b>Ваш статус</b>\n\n"
        f"🚗 {driver[4]} ({driver[3]})\n"
        f"📍 {driver[6]}\n"
        f"📊 Позиция: №{driver[7]}\n"
        f"💺 Занято: {occupied}/{total}\n"
        f"💺 Свободно: {available}\n"
        f"⏳ Клиентов в очереди: {waiting}\n"
        f"{get_rating_stars(driver[13] if len(driver) > 13 else 0)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="driver_menu")]
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "driver_passengers")
async def driver_passengers(callback: types.CallbackQuery):
    async with get_db() as db:
        async with db.execute('''SELECT c.user_id, c.full_name, c.pickup_location, c.dropoff_location, c.passengers_count
                     FROM clients c
                     WHERE c.assigned_driver_id=? AND c.status IN ('accepted', 'driver_arrived')
                     ORDER BY c.created_at''', (callback.from_user.id,)) as cursor:
            clients = await cursor.fetchall()
    
    if not clients:
        msg = "❌ Нет пассажиров"
    else:
        total_passengers = sum(c[4] for c in clients)
        msg = f"👥 <b>Мои пассажиры ({total_passengers} чел.):</b>\n\n"
        for i, client in enumerate(clients, 1):
            msg += f"{i}. {client[1]} ({client[4]} чел.)\n"
            msg += f"   📍 {client[2]} → {client[3]}\n\n"
    
    await callback.message.edit_text(
        msg,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="driver_menu")]
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "driver_available_orders")
async def driver_available_orders(callback: types.CallbackQuery):
    """Показывает ВСЕ заказы, с пометкой о нехватке мест"""
    async with get_db() as db:
        async with db.execute("SELECT direction FROM drivers WHERE user_id=?", (callback.from_user.id,)) as cursor:
            driver_dir = (await cursor.fetchone())[0]
        
        occupied, total, available = await get_driver_available_seats(callback.from_user.id)
        
        # ИЗМЕНЕНО: Показываем ВСЕ заказы, не фильтруем по местам
        async with db.execute('''SELECT user_id, full_name, pickup_location, dropoff_location, 
                            passengers_count, queue_position
                     FROM clients 
                     WHERE direction=? AND status='waiting'
                     ORDER BY queue_position''', (driver_dir,)) as cursor:
            clients = await cursor.fetchall()
    
    if not clients:
        msg = f"❌ Нет заказов\n\n💺 У вас свободно: {available} мест"
    else:
        msg = f"🔔 <b>Все заказы:</b>\n"
        msg += f"💺 Свободно мест: {available}\n\n"
        
        keyboard_buttons = []
        for client in clients:
            # ИЗМЕНЕНО: Помечаем заказы, которые не влезают
            can_fit = client[4] <= available
            fit_emoji = "✅" if can_fit else "⚠️"
            warning = "" if can_fit else " (не хватает мест!)"
            
            msg += f"{fit_emoji} №{client[5]} - {client[1]} ({client[4]} чел.){warning}\n"
            msg += f"   📍 {client[2]} → {client[3]}\n\n"
            
            # ИЗМЕНЕНО: Показываем кнопки для всех, но с предупреждением
            button_text = f"✅ Взять №{client[5]} ({client[4]} чел.)"
            if not can_fit:
                button_text = f"⚠️ Взять №{client[5]} ({client[4]} чел.) - мало мест!"
            
            keyboard_buttons.append([
                InlineKeyboardButton(
                    text=button_text,
                    callback_data=f"accept_client_{client[0]}"
                )
            ])
        
        keyboard_buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="driver_menu")])
        
        await callback.message.edit_text(
            msg,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons),
            parse_mode="HTML"
        )
        return
    
    await callback.message.edit_text(
        msg,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="driver_menu")]
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("accept_client_"))
async def accept_client(callback: types.CallbackQuery):
    """Водитель принимает клиента - FIXED VERSION WITH PROPER LOCKING"""
    client_id = int(callback.data.split("_")[2])
    driver_id = callback.from_user.id
    
    try:
        async with get_db(write=True) as db:
            # First, lock the client row by checking and updating in one atomic operation
            cursor = await db.execute(
                '''UPDATE clients 
                   SET status='accepted', assigned_driver_id=? 
                   WHERE user_id=? AND status='waiting'
                   RETURNING passengers_count, full_name, pickup_location, direction''', 
                (driver_id, client_id)
            )
            client = await cursor.fetchone()
            
            if not client:
                await callback.answer("❌ Клиент уже взят другим водителем!", show_alert=True)
                return
            
            passengers_count, full_name, pickup_location, direction = client
            
            # Check available seats
            cursor = await db.execute(
                "SELECT total_seats, COALESCE(occupied_seats, 0) FROM drivers WHERE user_id=?",
                (driver_id,)
            )
            driver_data = await cursor.fetchone()
            
            if not driver_data:
                await callback.answer("❌ Ошибка: водитель не найден", show_alert=True)
                return
            
            total, occupied = driver_data
            available = total - occupied
            
            if passengers_count > available:
                # Rollback the client status
                await db.execute(
                    "UPDATE clients SET status='waiting', assigned_driver_id=NULL WHERE user_id=?",
                    (client_id,)
                )
                await callback.answer(
                    f"❌ Недостаточно мест! Нужно: {passengers_count}, есть: {available}", 
                    show_alert=True
                )
                return
            
            # Update driver's occupied seats
            await db.execute(
                '''UPDATE drivers 
                   SET occupied_seats = COALESCE(occupied_seats, 0) + ? 
                   WHERE user_id=?''', 
                (passengers_count, driver_id)
            )
            
            # Create trip
            await db.execute(
                '''INSERT INTO trips (driver_id, client_id, direction, status, passengers_count, pickup_location)
                   VALUES (?, ?, ?, 'accepted', ?, ?)''', 
                (driver_id, client_id, direction, passengers_count, pickup_location)
            )
            
            # Get car info
            cursor = await db.execute(
                "SELECT car_model, car_number FROM drivers WHERE user_id=?", 
                (driver_id,)
            )
            car_info = await cursor.fetchone()
        
        await save_log_action(
            driver_id, 
            "client_accepted", 
            f"Client: {client_id}, Passengers: {passengers_count}"
        )
        
        # Notify client (outside transaction)
        try:
            await bot.send_message(
                client_id,
                f"✅ <b>Водитель принял ваш заказ!</b>\n\n"
                f"🚗 {car_info[0]} ({car_info[1]})\n"
                f"📍 Встреча: {pickup_location}\n\n"
                f"Ожидайте водителя!",
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Failed to notify client {client_id}: {e}")
        
        await callback.answer(f"✅ Клиент {full_name} добавлен!", show_alert=True)
        await driver_available_orders(callback)
        
    except Exception as e:
        logger.error(f"Error in accept_client: {e}", exc_info=True)
        await callback.answer("❌ Произошла ошибка. Попробуйте снова.", show_alert=True)

@dp.callback_query(F.data == "driver_arrived")
async def driver_arrived(callback: types.CallbackQuery):
    """Водитель уведомляет всех своих клиентов о прибытии"""
    async with get_db(write=True) as db:
        async with db.execute('''SELECT user_id, full_name 
                     FROM clients 
                     WHERE assigned_driver_id=? AND status='accepted' ''',
                  (callback.from_user.id,)) as cursor:
            clients = await cursor.fetchall()
        
        if not clients:
            await callback.answer("❌ Нет принятых клиентов!", show_alert=True)
            return
        
        # Обновляем статус всех клиентов
        await db.execute('''UPDATE clients 
                     SET status='driver_arrived' 
                     WHERE assigned_driver_id=? AND status='accepted' ''',
                  (callback.from_user.id,))
        
        await db.execute('''UPDATE trips 
                     SET status='driver_arrived', driver_arrived_at=CURRENT_TIMESTAMP 
                     WHERE driver_id=? AND status='accepted' ''',
                  (callback.from_user.id,))
    
    await save_log_action(callback.from_user.id, "driver_arrived", f"Clients: {len(clients)}")
    
    # Уведомляем всех клиентов
    for client in clients:
        try:
            await bot.send_message(
                client[0],
                f"🚗 <b>Водитель приехал!</b>\n\n"
                f"Выходите к машине!",
                parse_mode="HTML"
            )
        except:
            pass
    
    await callback.answer(f"✅ Уведомлены {len(clients)} пассажиров!", show_alert=True)
    await show_driver_menu(callback.message, callback.from_user.id)

@dp.callback_query(F.data == "driver_complete_trip")
async def driver_complete_trip(callback: types.CallbackQuery):
    """Водитель завершает поездку"""
    async with get_db(write=True) as db:
        # Получаем всех клиентов в поездке
        async with db.execute('''SELECT user_id, passengers_count 
                     FROM clients 
                     WHERE assigned_driver_id=? AND status IN ('accepted', 'driver_arrived')''',
                  (callback.from_user.id,)) as cursor:
            clients = await cursor.fetchall()
        
        if not clients:
            await callback.answer("❌ Нет активных поездок!", show_alert=True)
            return
        
        total_freed = sum(c[1] for c in clients)
        
        # Завершаем поездки
        await db.execute('''UPDATE trips 
                     SET status='completed', trip_completed_at=CURRENT_TIMESTAMP 
                     WHERE driver_id=? AND status IN ('accepted', 'driver_arrived')''',
                  (callback.from_user.id,))
        
        # Удаляем клиентов
        await db.execute('''DELETE FROM clients 
                     WHERE assigned_driver_id=? AND status IN ('accepted', 'driver_arrived')''',
                  (callback.from_user.id,))
        
        # Освобождаем места
        await db.execute('''UPDATE drivers 
                     SET occupied_seats = COALESCE(occupied_seats, 0) - ? 
                     WHERE user_id=?''', (total_freed, callback.from_user.id))
    
    await save_log_action(callback.from_user.id, "trip_completed", f"Freed {total_freed} seats")
    
    # Уведомляем клиентов о завершении
    for client in clients:
        try:
            await bot.send_message(
                client[0],
                f"✅ <b>Поездка завершена!</b>\n\n"
                f"Оцените водителя:\n"
                f"/rate",
                parse_mode="HTML"
            )
        except:
            pass
    
    await callback.answer(f"✅ Поездка завершена! Освобождено {total_freed} мест", show_alert=True)
    await show_driver_menu(callback.message, callback.from_user.id)

@dp.callback_query(F.data == "driver_exit")
async def driver_exit(callback: types.CallbackQuery):
    async with get_db(write=True) as db:
        # Проверяем активные поездки
        async with db.execute('''SELECT COUNT(*) FROM clients 
                     WHERE assigned_driver_id=? AND status IN ('accepted', 'driver_arrived')''',
                  (callback.from_user.id,)) as cursor:
            active_trips = (await cursor.fetchone())[0]
        
        if active_trips > 0:
            await callback.answer(
                "❌ Вы не можете выйти - есть активные поездки!",
                show_alert=True
            )
            return
        
        # Get direction before deleting
        async with db.execute("SELECT direction FROM drivers WHERE user_id=?", (callback.from_user.id,)) as cursor:
            direction_row = await cursor.fetchone()
            direction = direction_row[0] if direction_row else None
        
        await db.execute("DELETE FROM drivers WHERE user_id=?", (callback.from_user.id,))
        
        # Пересчитываем позиции
        if direction:
            async with db.execute('''SELECT user_id FROM drivers 
                         WHERE direction=? ORDER BY queue_position''', (direction,)) as cursor:
                drivers = await cursor.fetchall()
            
            for pos, driver in enumerate(drivers, 1):
                await db.execute("UPDATE drivers SET queue_position=? WHERE user_id=?", (pos, driver[0]))
    
    await save_log_action(callback.from_user.id, "driver_exit", "")
    
    await callback.message.delete()
    await callback.message.answer(
        "❌ Вы вышли из очереди",
        reply_markup=main_menu_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "driver_menu")
async def driver_menu_back(callback: types.CallbackQuery):
    await show_driver_menu(callback.message, callback.from_user.id)
    await callback.answer()

# ==================== КЛИЕНТЫ ====================

class ClientOrder(StatesGroup):
    phone = State()
    verify_code = State()
    from_city = State()
    to_city = State()
    direction = State()
    passengers_count = State()
    pickup_location = State()
    dropoff_location = State()
    order_for = State()
    add_another = State()

@dp.message(F.text == "🧍‍♂️ Мне нужно такси")
async def client_start(message: types.Message, state: FSMContext):
    async with get_db() as db:
        async with db.execute("SELECT * FROM clients WHERE user_id=?", (message.from_user.id,)) as cursor:
            client = await cursor.fetchone()
    
    # Если клиент уже верифицирован и имеет активный заказ
    if client and client[10]:  # is_verified
        if client[12] in ('waiting', 'accepted', 'driver_arrived'):  # status
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отменить заказ", callback_data="client_cancel")]
            ])
            
            status_text = {
                'waiting': '⏳ Ваш заказ в очереди',
                'accepted': '✅ Водитель принял заказ',
                'driver_arrived': '🚗 Водитель на месте!'
            }
            
            await message.answer(
                f"{status_text[client[12]]}\n\n"
                f"Вы уже в системе!",
                reply_markup=keyboard
            )
            return
        else:
            # Верифицирован, но нет активного заказа - сразу к выбору городов
            await message.answer(
                "🧍‍♂️ <b>Вызов такси</b>\n\n"
                "Из какого города поедете?",
                reply_markup=from_city_keyboard(),
                parse_mode="HTML"
            )
            await state.set_state(ClientOrder.from_city)
            return
    
    # Если не верифицирован - запрашиваем телефон
    if client and not client[10]:  # не верифицирован
        await message.answer(
            "⏳ Вы уже начали регистрацию.\n"
            "Введите код из SMS:"
        )
        await state.set_state(ClientOrder.verify_code)
        return
    
    # Новый пользователь
    await message.answer(
        "🧍‍♂️ <b>Регистрация клиента</b>\n\n"
        "Введите номер телефона в формате +7XXXXXXXXXX:",
        parse_mode="HTML"
    )
    await state.set_state(ClientOrder.phone)
    
@dp.message(ClientOrder.phone)
async def client_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    
    if not phone.startswith('+7') or len(phone) != 12:
        await message.answer("❌ Неверный формат! Используйте +7XXXXXXXXXX")
        return
    
    async with get_db() as db:
        async with db.execute("SELECT user_id FROM clients WHERE phone=?", (phone,)) as cursor:
            existing = await cursor.fetchone()
    
    if existing:
        await message.answer("❌ Этот номер уже зарегистрирован!")
        return
    
    code = generate_verification_code()
    await state.update_data(phone=phone, verification_code=code)
    
    await send_sms(phone, f"Код подтверждения: {code}")
    
    await message.answer(
        f"✅ SMS отправлено на {phone}\n\n"
        "Введите код из SMS:"
    )
    await state.set_state(ClientOrder.verify_code)

@dp.message(ClientOrder.verify_code)
async def client_verify(message: types.Message, state: FSMContext):
    data = await state.get_data()
    
    if message.text.strip() != data['verification_code']:
        await message.answer("❌ Неверный код!")
        return
    
    await state.update_data(is_verified=True)
    await message.answer(
        "✅ Номер подтвержден!\n\n"
        "Из какого города поедете?",
        reply_markup=from_city_keyboard()
    )
    await state.set_state(ClientOrder.from_city)
    
@dp.callback_query(F.data == "add_new_order")
async def add_new_order(callback: types.CallbackQuery, state: FSMContext):
    """Добавить еще один заказ"""
    await callback.message.edit_text(
        "🧍‍♂️ <b>Новый заказ такси</b>\n\n"
        "Выберите маршрут:",
        reply_markup=direction_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(ClientOrder.direction)
    await callback.answer()

@dp.callback_query(F.data == "view_my_orders")
async def view_my_orders(callback: types.CallbackQuery):
    """Показать все заказы с возможностью отмены"""
    active_orders = await get_user_active_orders(callback.from_user.id)
    
    if not active_orders:
        await callback.answer("❌ Нет активных заказов", show_alert=True)
        return
    
    msg = "🚖 <b>Ваши активные заказы:</b>\n\n"
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
        
        # Кнопка отмены для каждого заказа
        keyboard_buttons.append([
            InlineKeyboardButton(
                text=f"❌ Отменить заказ #{order[3]}",
                callback_data=f"cancel_order_{order[0]}"  # user_id конкретного заказа
            )
        ])
    
    keyboard_buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_main")])
    
    await callback.message.edit_text(
        msg,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("cancel_order_"))
async def cancel_specific_order(callback: types.CallbackQuery):
    """Отмена конкретного заказа"""
    order_user_id = int(callback.data.split("_")[2])
    parent_user_id = callback.from_user.id
    
    async with get_db(write=True) as db:
        async with db.execute(
            "SELECT * FROM clients WHERE user_id=?",
            (order_user_id,)
        ) as cursor:
            client = await cursor.fetchone()
        
        if not client:
            await callback.answer("❌ Заказ не найден", show_alert=True)
            return
        
        # Проверяем права на отмену
        if client[0] != parent_user_id and (len(client) <= 15 or client[15] != parent_user_id):
            await callback.answer("❌ Это не ваш заказ!", show_alert=True)
            return
        
        # Получаем текущий счетчик отмен РОДИТЕЛЯ
        cancellation_count = await get_cancellation_count(parent_user_id)
        new_count = cancellation_count + 1
        
        driver_id = client[11]  # assigned_driver_id
        
        # Если клиент был принят водителем, освобождаем места
        if driver_id:
            await db.execute(
                '''UPDATE drivers 
                   SET occupied_seats = COALESCE(occupied_seats, 0) - ? 
                   WHERE user_id=?''',
                (client[5], driver_id)
            )
            
            # Уведомляем водителя
            try:
                await bot.send_message(
                    driver_id,
                    f"⚠️ Клиент отменил заказ\n"
                    f"Для: {client[13] if len(client) > 13 else 'клиента'}\n"
                    f"Освобождено мест: {client[5]}",
                    parse_mode="HTML"
                )
            except:
                pass
        
        # Обновляем trip
        await db.execute(
            '''UPDATE trips SET status='cancelled', cancelled_by='client', 
               cancelled_at=CURRENT_TIMESTAMP 
               WHERE client_id=? AND status IN ('waiting', 'accepted', 'driver_arrived')''',
            (order_user_id,)
        )
        
        # Удаляем заказ
        direction = client[3]
        order_number = client[14] if len(client) > 14 else 1
        
        await db.execute("DELETE FROM clients WHERE user_id=?", (order_user_id,))
        
        # Пересчитываем позиции в очереди
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
    
    # ЛОГИКА БЛОКИРОВКИ
    if new_count == 1:
        await callback.answer(
            "⚠️ ПРЕДУПРЕЖДЕНИЕ! При второй отмене вы будете заблокированы!",
            show_alert=True
        )
    elif new_count >= 2:
        reason = f"Частые отмены заказов ({new_count} раз)"
        await add_to_blacklist(parent_user_id, reason, new_count)
        
        await callback.message.edit_text(
            "🚫 <b>ВЫ ЗАБЛОКИРОВАНЫ</b>\n\n"
            f"Причина: {reason}\n\n"
            "Для разблокировки обратитесь к администратору.",
            parse_mode="HTML"
        )
        return
    
    # Показываем обновленный список заказов
    remaining_orders = await count_user_orders(parent_user_id)
    
    if remaining_orders > 0:
        await view_my_orders(callback)
    else:
        await callback.message.edit_text(
            "❌ Заказ отменен\n\n"
            "У вас больше нет активных заказов.",
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
    
    await callback.message.edit_text(
        f"✅ Откуда: {from_city}\n\n"
        "Куда поедете?",
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
    
    # Показываем доступных водителей
    async with get_db() as db:
        async with db.execute('''SELECT COUNT(*), SUM(total_seats - occupied_seats) 
                     FROM drivers 
                     WHERE direction=? AND is_active=1''', (direction,)) as cursor:
            result = await cursor.fetchone()
    
    drivers_count = result[0] or 0
    available_seats = result[1] or 0
    
    await callback.message.edit_text(
        f"✅ Маршрут: {direction}\n\n"
        f"🚗 Водителей доступно: {drivers_count}\n"
        f"💺 Свободных мест: {available_seats}\n\n"
        f"👥 Сколько человек поедет? (1-8)"
    )
    await state.set_state(ClientOrder.passengers_count)
    await callback.answer()

@dp.callback_query(F.data == "back_from_city")
async def back_from_city(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Из какого города поедете?",
        reply_markup=from_city_keyboard()
    )
    await state.set_state(ClientOrder.from_city)
    await callback.answer()

@dp.message(ClientOrder.passengers_count)
async def client_passengers_count(message: types.Message, state: FSMContext):
    try:
        count = int(message.text)
        if count < 1 or count > 8:
            await message.answer("Ошибка! От 1 до 8 человек")
            return
        
        data = await state.get_data()
        
        # ИЗМЕНЕНО: Теперь просто показываем информацию, НЕ блокируем заказ
        async with get_db() as db:
            async with db.execute('''SELECT COUNT(*) 
                         FROM drivers 
                         WHERE direction=? AND is_active=1 
                         AND (total_seats - occupied_seats) >= ?''',
                      (data['direction'], count)) as cursor:
                suitable_cars = (await cursor.fetchone())[0]
        
        await state.update_data(passengers_count=count)
        
        # ИЗМЕНЕНО: Предупреждаем, но продолжаем оформление
        if suitable_cars == 0:
            await message.answer(
                f"⚠️ Пассажиров: {count}\n"
                f"❗️ Сейчас нет машин с {count} свободными местами\n\n"
                f"Но ваш заказ будет сохранён!\n"
                f"Водители увидят его, когда освободятся места.\n\n"
                f"📍 Откуда вас забрать?\n\nВведите адрес:"
            )
        else:
            await message.answer(
                f"✅ Пассажиров: {count}\n"
                f"🚗 Подходящих машин: {suitable_cars}\n\n"
                f"📍 Откуда вас забрать?\n\nВведите адрес:"
            )
        
        await state.set_state(ClientOrder.pickup_location)
    except ValueError:
        await message.answer("Введите число!")

@dp.message(ClientOrder.pickup_location)
async def client_pickup(message: types.Message, state: FSMContext):
    await state.update_data(pickup_location=message.text)
    await message.answer("📍 Куда вас везти?\n\nВведите адрес:")
    await state.set_state(ClientOrder.dropoff_location)

@dp.message(ClientOrder.dropoff_location)
async def client_dropoff(message: types.Message, state: FSMContext):
    """Сохраняем место назначения и спрашиваем для кого заказ"""
    await state.update_data(dropoff_location=message.text)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Для себя", callback_data="order_for_self")],
        [InlineKeyboardButton(text="👥 Для другого человека", callback_data="order_for_other")]
    ])
    
    await message.answer(
        "👤 <b>Для кого этот заказ?</b>",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await state.set_state(ClientOrder.order_for)

@dp.callback_query(ClientOrder.order_for, F.data == "order_for_self")
async def order_for_self(callback: types.CallbackQuery, state: FSMContext):
    """Заказ для себя"""
    await state.update_data(order_for="Для себя")
    await finalize_order(callback, state)

@dp.callback_query(ClientOrder.order_for, F.data == "order_for_other")
async def order_for_other(callback: types.CallbackQuery, state: FSMContext):
    """Заказ для другого человека"""
    await callback.message.edit_text(
        "👥 Введите имя человека, для которого заказываете такси:"
    )
    await callback.answer()

@dp.message(ClientOrder.order_for)
async def save_order_for_name(message: types.Message, state: FSMContext):
    """Сохраняем имя человека"""
    await state.update_data(order_for=message.text)
    await finalize_order_from_message(message, state)

async def finalize_order(callback: types.CallbackQuery, state: FSMContext):
    """Финализирует заказ и предлагает добавить еще"""
    data = await state.get_data()
    
    # Считаем текущее количество заказов
    current_orders = await count_user_orders(callback.from_user.id)
    order_number = current_orders + 1
    
    # Создаем уникальный ID для заказа (timestamp + random)
    import time
    unique_id = int(time.time() * 1000) + callback.from_user.id + order_number
    
    async with get_db(write=True) as db:
        # Вычисляем позицию в очереди
        async with db.execute(
            "SELECT MAX(queue_position) FROM clients WHERE direction=?",
            (data['direction'],)
        ) as cursor:
            max_pos = (await cursor.fetchone())[0]
        
        queue_pos = (max_pos or 0) + 1
        
        # Добавляем клиента
        data_to_save = await state.get_data()
        await db.execute('''INSERT INTO clients 
                     (user_id, full_name, phone, direction, from_city, to_city, 
                    queue_position, passengers_count, pickup_location, dropoff_location, 
                    is_verified, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'waiting')''',
                (callback.from_user.id, 
                callback.from_user.full_name or "Клиент",
                data_to_save.get('phone', '+77777777777'),
                data_to_save['direction'],
                data_to_save['from_city'],
                data_to_save['to_city'],
                queue_pos, 
                data_to_save['passengers_count'],
                data_to_save['pickup_location'], 
                callback.text))
        
        # Проверяем подходящих водителей
        async with db.execute(
            '''SELECT COUNT(*) FROM drivers 
               WHERE direction=? AND is_active=1 
               AND (total_seats - COALESCE(occupied_seats, 0)) >= ?''',
            (data['direction'], data['passengers_count'])
        ) as cursor:
            suitable = (await cursor.fetchone())[0]
        
        # Получаем водителей для уведомления
        async with db.execute(
            '''SELECT user_id FROM drivers 
               WHERE direction=? AND is_active=1 
               AND (total_seats - COALESCE(occupied_seats, 0)) >= ?''',
            (data['direction'], data['passengers_count'])
        ) as cursor:
            drivers = await cursor.fetchall()
    
    await save_log_action(
        callback.from_user.id,
        "order_created",
        f"Order #{order_number} for {data['order_for']}"
    )
    
    # Уведомляем водителей
    for driver in drivers:
        try:
            await bot.send_message(
                driver[0],
                f"🔔 <b>Новый заказ!</b>\n\n"
                f"👥 Пассажиров: {data['passengers_count']}\n"
                f"📍 {data['pickup_location']} → {data['dropoff_location']}\n"
                f"Для: {data['order_for']}\n\n"
                f"Проверьте доступные заказы: /driver",
                parse_mode="HTML"
            )
        except:
            pass
    
    # Предлагаем добавить еще заказ
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Заказать еще одно такси", callback_data="add_another_yes")],
        [InlineKeyboardButton(text="✅ Завершить", callback_data="add_another_no")]
    ])
    
    await callback.message.edit_text(
        f"✅ <b>Заказ #{order_number} создан!</b>\n\n"
        f"📍 {data['direction']}\n"
        f"👤 Для: {data['order_for']}\n"
        f"👥 Пассажиров: {data['passengers_count']}\n"
        f"📍 От: {data['pickup_location']}\n"
        f"📍 До: {data['dropoff_location']}\n"
        f"📊 Позиция в очереди: №{queue_pos}\n\n"
        f"🚗 Подходящих водителей: {suitable}\n\n"
        f"Хотите заказать еще одно такси?",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await state.set_state(ClientOrder.add_another)

async def finalize_order_from_message(message: types.Message, state: FSMContext):
    """Та же логика, но для message вместо callback"""
    data = await state.get_data()
    current_orders = await count_user_orders(message.from_user.id)
    order_number = current_orders + 1
    
    import time
    unique_id = int(time.time() * 1000) + message.from_user.id + order_number
    
    async with get_db(write=True) as db:
        async with db.execute(
            "SELECT MAX(queue_position) FROM clients WHERE direction=?",
            (data['direction'],)
        ) as cursor:
            max_pos = (await cursor.fetchone())[0]
        
        queue_pos = (max_pos or 0) + 1
        
        # Добавляем клиента
        data_to_save = await state.get_data()
        await db.execute('''INSERT INTO clients 
                     (user_id, full_name, phone, direction, from_city, to_city, 
                    queue_position, passengers_count, pickup_location, dropoff_location, 
                    is_verified, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'waiting')''',
                (message.from_user.id, 
                message.from_user.full_name or "Клиент",
                data_to_save.get('phone', '+77777777777'),
                data_to_save['direction'],
                data_to_save['from_city'],
                data_to_save['to_city'],
                queue_pos, 
                data_to_save['passengers_count'],
                data_to_save['pickup_location'], 
                message.text))
        
        async with db.execute(
            '''SELECT COUNT(*) FROM drivers 
               WHERE direction=? AND is_active=1 
               AND (total_seats - COALESCE(occupied_seats, 0)) >= ?''',
            (data['direction'], data['passengers_count'])
        ) as cursor:
            suitable = (await cursor.fetchone())[0]
        
        async with db.execute(
            '''SELECT user_id FROM drivers 
               WHERE direction=? AND is_active=1 
               AND (total_seats - COALESCE(occupied_seats, 0)) >= ?''',
            (data['direction'], data['passengers_count'])
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
                f"🔔 <b>Новый заказ!</b>\n\n"
                f"👥 Пассажиров: {data['passengers_count']}\n"
                f"📍 {data['pickup_location']} → {data['dropoff_location']}\n"
                f"Для: {data['order_for']}\n\n"
                f"Проверьте доступные заказы: /driver",
                parse_mode="HTML"
            )
        except:
            pass
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Заказать еще одно такси", callback_data="add_another_yes")],
        [InlineKeyboardButton(text="✅ Завершить", callback_data="add_another_no")]
    ])
    
    await message.answer(
        f"✅ <b>Заказ #{order_number} создан!</b>\n\n"
        f"📍 {data['direction']}\n"
        f"👤 Для: {data['order_for']}\n"
        f"👥 Пассажиров: {data['passengers_count']}\n"
        f"📍 От: {data['pickup_location']}\n"
        f"📍 До: {data['dropoff_location']}\n"
        f"📊 Позиция в очереди: №{queue_pos}\n\n"
        f"🚗 Подходящих водителей: {suitable}\n\n"
        f"Хотите заказать еще одно такси?",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await state.set_state(ClientOrder.add_another)

@dp.callback_query(ClientOrder.add_another, F.data == "add_another_yes")
async def add_another_order_yes(callback: types.CallbackQuery, state: FSMContext):
    """Добавить еще один заказ"""
    await callback.message.edit_text(
        "🧍‍♂️ <b>Новый заказ такси</b>\n\n"
        "Выберите маршрут:",
        reply_markup=direction_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(ClientOrder.direction)
    await callback.answer()

@dp.callback_query(ClientOrder.add_another, F.data == "add_another_no")
async def add_another_order_no(callback: types.CallbackQuery, state: FSMContext):
    """Завершить создание заказов"""
    total_orders = await count_user_orders(callback.from_user.id)
    
    await callback.message.edit_text(
        f"✅ <b>Готово!</b>\n\n"
        f"У вас {total_orders} активных заказов.\n\n"
        f"Используйте /driver для просмотра статуса.",
        parse_mode="HTML"
    )
    await state.clear()
    await callback.answer()

# ==================== РЕЙТИНГИ ====================

class RatingStates(StatesGroup):
    select_rating = State()
    write_review = State()

@dp.message(F.text == "⭐ Мой профиль")
async def show_profile(message: types.Message):
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    c.execute("SELECT avg_rating, rating_count FROM drivers WHERE user_id=?", (message.from_user.id,))
    driver = c.fetchone()
    
    c.execute("SELECT avg_rating, rating_count FROM clients WHERE user_id=?", (message.from_user.id,))
    client = c.fetchone()
    
    if not driver and not client:
        await message.answer("❌ Вы еще не зарегистрированы")
        conn.close()
        return
    
    msg = "⭐ <b>Ваш профиль</b>\n\n"
    
    if driver:
        msg += f"<b>Как водитель:</b>\n"
        msg += f"{get_rating_stars(driver[0] or 0)}\n"
        msg += f"📊 Оценок: {driver[1] or 0}\n\n"
    
    if client:
        msg += f"<b>Как клиент:</b>\n"
        msg += f"{get_rating_stars(client[0] or 0)}\n"
        msg += f"📊 Оценок: {client[1] or 0}\n\n"
    
    c.execute('''SELECT from_user_id, rating, review, created_at 
                 FROM ratings WHERE to_user_id=? 
                 ORDER BY created_at DESC LIMIT 5''', (message.from_user.id,))
    reviews = c.fetchall()
    
    if reviews:
        msg += "<b>Последние отзывы:</b>\n"
        for review in reviews:
            stars = "⭐" * review[1]
            msg += f"\n{stars}\n"
            if review[2]:
                msg += f"💬 {review[2]}\n"
    
    conn.close()
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✍️ Оставить отзыв", callback_data="rate_start")]
    ])
    
    await message.answer(msg, reply_markup=keyboard, parse_mode="HTML")

@dp.callback_query(F.data == "rate_start")
async def rate_start(callback: types.CallbackQuery, state: FSMContext):
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    c.execute('''SELECT t.id, t.driver_id, d.full_name, t.client_id
                 FROM trips t
                 JOIN drivers d ON t.driver_id = d.user_id
                 WHERE (t.driver_id=? OR t.client_id=?)
                 AND t.status='completed'
                 AND t.id NOT IN (SELECT trip_id FROM ratings WHERE from_user_id=? AND trip_id IS NOT NULL)
                 ORDER BY t.trip_completed_at DESC LIMIT 5''',
              (callback.from_user.id, callback.from_user.id, callback.from_user.id))
    trips = c.fetchall()
    conn.close()
    
    if not trips:
        await callback.answer("❌ Нет поездок для оценки", show_alert=True)
        return
    
    keyboard_buttons = []
    for trip in trips:
        is_driver = trip[1] == callback.from_user.id
        target_name = "Клиента" if is_driver else f"Водителя {trip[2]}"
        keyboard_buttons.append([
            InlineKeyboardButton(
                text=f"Оценить {target_name}",
                callback_data=f"rate_trip_{trip[0]}"
            )
        ])
    
    await callback.message.edit_text(
        "✍️ <b>Выберите поездку для оценки:</b>",
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
        "⭐ Выберите оценку:",
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
        "Напишите отзыв (или /skip чтобы пропустить):"
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
        f"✅ Спасибо за отзыв!\n\n"
        f"{'⭐' * data['rating']}",
        reply_markup=main_menu_keyboard()
    )
    await state.clear()

# ==================== ОБЩЕЕ ====================

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await save_log_action(message.from_user.id, "bot_started", "")
    
    await message.answer(
        f"Привет, {message.from_user.first_name}! 👋\n\n"
        "🚖 <b>Такси Шетпе–Ақтау</b>\n\n"
        "Выберите, кто вы:",
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML"
    )

@dp.message(Command("driver"))
async def cmd_driver(message: types.Message):
    """Быстрый доступ к меню водителя"""
    await show_driver_menu(message, message.from_user.id)

@dp.message(Command("rate"))
async def cmd_rate(message: types.Message, state: FSMContext):
    """Быстрый доступ к оценке"""
    await rate_start(types.CallbackQuery(
        id="fake",
        from_user=message.from_user,
        chat_instance="fake",
        message=message,
        data="rate_start"
    ), state)

@dp.message(F.text == "ℹ️ Информация")
async def info_command(message: types.Message):
    await message.answer(
        "ℹ️ <b>О нас</b>\n\n"
        "🚖 Система заказа такси в реальном времени\n\n"
        "<b>Для водителей:</b>\n"
        "• Верификация по SMS\n"
        "• Принимайте несколько клиентов\n"
        "• Видите свободные места\n"
        "• Получайте рейтинги\n\n"
        "<b>Для клиентов:</b>\n"
        "• Верификация по SMS\n"
        "• Укажите кол-во пассажиров\n"
        "• Видите доступные машины\n"
        "• Отменяйте при необходимости\n"
        "• Оценивайте водителей\n\n"
        "Просто и быстро! ⚡",
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "back_main")
async def back_main(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.message.answer(
        "Главное меню:",
        reply_markup=main_menu_keyboard()
    )
    await callback.answer()

# ==================== АДМИН ====================

def admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Водители", callback_data="admin_drivers")],
        [InlineKeyboardButton(text="🧍‍♂️ Клиенты", callback_data="admin_clients")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="📜 Логи", callback_data="admin_logs")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_main")]
    ])

@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Нет доступа")
        return
    
    await message.answer(
        "🔐 <b>Админ панель</b>",
        reply_markup=admin_keyboard(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "admin_drivers")
async def admin_drivers(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    async with get_db() as db:
        async with db.execute("SELECT * FROM drivers ORDER BY direction, queue_position") as cursor:
            drivers = await cursor.fetchall()
    
    if not drivers:
        msg = "❌ Водителей нет"
    else:
        msg = "👥 <b>Водители:</b>\n\n"
        for driver in drivers:
            occupied, total, available = await get_driver_available_seats(driver[0])
            msg += f"№{driver[7]} - {driver[1]}\n"
            msg += f"   🚗 {driver[4]} ({driver[3]})\n"
            msg += f"   💺 {occupied}/{total} (своб: {available})\n"
            msg += f"   📍 {driver[6]}\n"
            msg += f"   {get_rating_stars(driver[13] if len(driver) > 13 else 0)}\n\n"
    
    await callback.message.edit_text(msg, reply_markup=admin_keyboard(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_clients")
async def admin_clients(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    async with get_db() as db:
        async with db.execute("SELECT * FROM clients ORDER BY direction, queue_position") as cursor:
            clients = await cursor.fetchall()
    
    if not clients:
        msg = "❌ Клиентов нет"
    else:
        msg = "🧍‍♂️ <b>Клиенты в очереди:</b>\n\n"
        for client in clients:
            status_emoji = {"waiting": "⏳", "accepted": "✅", "driver_arrived": "🚗"}
            msg += f"№{client[4]} {status_emoji.get(client[10], '❓')} - {client[1]}\n"
            msg += f"   📍 {client[3]}\n"
            msg += f"   👥 {client[5]} чел.\n"
            msg += f"   От: {client[6]}\n"
            msg += f"   До: {client[7]}\n"
            if client[11]:
                msg += f"   🚗 Водитель: ID {client[11]}\n"
            msg += "\n"
    
    await callback.message.edit_text(msg, reply_markup=admin_keyboard(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
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
    msg += f"👥 Водителей: {total_drivers}\n"
    msg += f"💺 Свободных мест: {total_available_seats}\n\n"
    msg += f"🧍‍♂️ Клиентов в ожидании: {waiting_clients}\n"
    msg += f"✅ Клиентов принято: {accepted_clients}\n\n"
    msg += f"✅ Завершено поездок: {completed_trips}\n"
    msg += f"❌ Отменено поездок: {cancelled_trips}\n"
    msg += f"⭐ Средний рейтинг: {avg_rating:.1f}\n"
    msg += f"🚫 Заблокировано пользователей: {blacklisted_users}\n"
    
    await callback.message.edit_text(msg, reply_markup=admin_keyboard(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_logs")
async def admin_logs(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    async with get_db() as db:
        async with db.execute('''SELECT user_id, action, details, created_at 
                     FROM actions_log 
                     ORDER BY created_at DESC LIMIT 20''') as cursor:
            logs = await cursor.fetchall()
    
    msg = "📜 <b>Последние действия:</b>\n\n"
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
        await message.answer("❌ Нет доступа")
        return
    
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Используйте: /addadmin USER_ID")
        return
    
    try:
        new_admin_id = int(parts[1])
        async with get_db(write=True) as db:
            await db.execute("INSERT INTO admins (user_id) VALUES (?)", (new_admin_id,))
        
        await save_log_action(message.from_user.id, "admin_added", f"New admin: {new_admin_id}")
        await message.answer(f"✅ Админ добавлен: {new_admin_id}")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        
@dp.message(Command("blacklist"))
async def show_blacklist(message: types.Message):
    """Показать черный список (только для админов)"""
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Нет доступа")
        return
    
    async with get_db() as db:
        async with db.execute(
            '''SELECT user_id, reason, cancellation_count, banned_at 
               FROM blacklist ORDER BY banned_at DESC'''
        ) as cursor:
            blacklist = await cursor.fetchall()
    
    if not blacklist:
        await message.answer("✅ Черный список пуст")
        return
    
    msg = "🚫 <b>Черный список:</b>\n\n"
    for entry in blacklist:
        try:
            banned_time = datetime.fromisoformat(entry[3]).strftime("%Y-%m-%d %H:%M")
        except:
            banned_time = "???"
        
        msg += f"👤 User ID: <code>{entry[0]}</code>\n"
        msg += f"   Причина: {entry[1]}\n"
        msg += f"   Отмен: {entry[2]}\n"
        msg += f"   Дата: {banned_time}\n\n"
    
    msg += "\n💡 Для разблокировки: /unban USER_ID"
    
    await message.answer(msg, parse_mode="HTML")

@dp.message(Command("unban"))
async def unban_user(message: types.Message):
    """Разблокировать пользователя (только для админов)"""
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Нет доступа")
        return
    
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Используйте: /unban USER_ID")
        return
    
    try:
        user_id = int(parts[1])
        
        async with get_db(write=True) as db:
            await db.execute("DELETE FROM blacklist WHERE user_id=?", (user_id,))
            
            # Сброс счетчика отмен
            await db.execute(
                "UPDATE clients SET cancellation_count=0 WHERE user_id=?",
                (user_id,)
            )
        
        await save_log_action(
            message.from_user.id, 
            "user_unbanned", 
            f"Unbanned user: {user_id}"
        )
        
        await message.answer(f"✅ Пользователь {user_id} разблокирован")
        
        # Уведомляем пользователя
        try:
            await bot.send_message(
                user_id,
                "✅ <b>Вы разблокированы!</b>\n\n"
                "Теперь вы можете снова пользоваться такси.\n"
                "Пожалуйста, будьте ответственнее при заказе.",
                parse_mode="HTML"
            )
        except:
            pass
            
    except ValueError:
        await message.answer("❌ Неверный USER_ID")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(Command("resetcancel"))
async def reset_cancellation(message: types.Message):
    """Сбросить счетчик отмен пользователю (только для админов)"""
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Нет доступа")
        return
    
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Используйте: /resetcancel USER_ID")
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
        
        await message.answer(f"✅ Счетчик отмен сброшен для пользователя {user_id}")
        
    except ValueError:
        await message.answer("❌ Неверный USER_ID")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

# ==================== СТАРТ ====================

async def main():
    await init_db()
    logger.info("🚀 Бот запущен")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())