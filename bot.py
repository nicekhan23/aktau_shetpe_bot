import asyncio
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

@asynccontextmanager
async def get_db():
    async with aiosqlite.connect(DATABASE_FILE, timeout=30.0) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=30000")
        try:
            yield db
            await db.commit()
        except Exception:
            await db.rollback()
            raise

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('taxi_bot.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def log_action(user_id: int, action: str, details: str = ""):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"[{timestamp}] User {user_id}: {action}"
    if details:
        log_msg += f" | {details}"
    logger.info(log_msg)

def generate_verification_code() -> str:
    return ''.join(random.choices(string.digits, k=4))

async def send_sms(phone: str, message: str) -> bool:
    logger.info(f"SMS отправлено на {phone}: {message}")
    return True

class DBMigration:

    @staticmethod
    async def get_db_version() -> int:
        async with aiosqlite.connect(DATABASE_FILE) as db:
            async with db.execute("PRAGMA user_version") as cursor:
                version = (await cursor.fetchone())[0]
            return version

    @staticmethod
    async def set_db_version(version: int):
        async with aiosqlite.connect(DATABASE_FILE) as db:
            await db.execute(f"PRAGMA user_version = {version}")
            await db.commit()

    @staticmethod
    async def migrate():
        current_version = await DBMigration.get_db_version()

        if current_version < 1:
            await DBMigration.migration_v1()
        if current_version < 2:
            await DBMigration.migration_v2()
        if current_version < 3:
            await DBMigration.migration_v3()
        if current_version < 4:
            await DBMigration.migration_v4()

    @staticmethod
    async def migration_v1():
        logger.info("Применяю миграцию v1...")
        async with aiosqlite.connect(DATABASE_FILE) as db:
            await db.execute('''CREATE TABLE IF NOT EXISTS drivers
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

            await db.execute('''CREATE TABLE IF NOT EXISTS clients
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

            await db.execute('''CREATE TABLE IF NOT EXISTS admins
                         (user_id INTEGER PRIMARY KEY,
                          added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

            await db.commit()
        await DBMigration.set_db_version(1)
        logger.info("Миграция v1 завершена")

    @staticmethod
    async def migration_v2():
        logger.info("Применяю миграцию v2...")
        async with aiosqlite.connect(DATABASE_FILE) as db:
            await db.execute('''CREATE TABLE IF NOT EXISTS ratings
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          from_user_id INTEGER,
                          to_user_id INTEGER,
                          user_type TEXT,
                          trip_id INTEGER,
                          rating INTEGER CHECK(rating >= 1 AND rating <= 5),
                          review TEXT,
                          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

            async with db.execute("PRAGMA table_info(drivers)") as cursor:
                driver_columns = [column[1] for column in await cursor.fetchall()]

            if 'avg_rating' not in driver_columns:
                await db.execute("ALTER TABLE drivers ADD COLUMN avg_rating REAL DEFAULT 0")
            if 'rating_count' not in driver_columns:
                await db.execute("ALTER TABLE drivers ADD COLUMN rating_count INTEGER DEFAULT 0")

            async with db.execute("PRAGMA table_info(clients)") as cursor:
                client_columns = [column[1] for column in await cursor.fetchall()]

            if 'avg_rating' not in client_columns:
                await db.execute("ALTER TABLE clients ADD COLUMN avg_rating REAL DEFAULT 0")
            if 'rating_count' not in client_columns:
                await db.execute("ALTER TABLE clients ADD COLUMN rating_count INTEGER DEFAULT 0")

            await db.commit()
        await DBMigration.set_db_version(2)
        logger.info("Миграция v2 завершена")

    @staticmethod
    async def migration_v3():
        logger.info("Применяю миграцию v3...")
        async with aiosqlite.connect(DATABASE_FILE) as db:
            await db.execute('''CREATE TABLE IF NOT EXISTS trips
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

            await db.execute('''CREATE TABLE IF NOT EXISTS actions_log
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          user_id INTEGER,
                          action TEXT,
                          details TEXT,
                          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

            await db.commit()
        await DBMigration.set_db_version(3)
        logger.info("Миграция v3 завершена")

    @staticmethod
    async def migration_v4():
        logger.info("Применяю миграцию v4...")
        async with aiosqlite.connect(DATABASE_FILE) as db:
            async with db.execute("PRAGMA table_info(drivers)") as cursor:
                driver_columns = [column[1] for column in await cursor.fetchall()]

            if 'occupied_seats' not in driver_columns:
                await db.execute("ALTER TABLE drivers ADD COLUMN occupied_seats INTEGER DEFAULT 0")
            if 'is_on_trip' not in driver_columns:
                await db.execute("ALTER TABLE drivers ADD COLUMN is_on_trip INTEGER DEFAULT 0")

            await db.commit()
        await DBMigration.set_db_version(4)
        logger.info("Миграция v4 завершена")

async def init_db():
    async with aiosqlite.connect(DATABASE_FILE) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=30000")
        await db.commit()

    await DBMigration.migrate()

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
    async with get_db() as db:
        await db.execute('''INSERT INTO actions_log (user_id, action, details) 
                           VALUES (?, ?, ?)''', (user_id, action, details))
    log_action(user_id, action, details)

async def get_driver_available_seats(driver_id: int) -> tuple:
    """Возвращает (занято мест, всего мест, свободно мест) - ASYNC VERSION"""
    async with get_db() as db:
        # Проверяем наличие колонок
        async with db.execute("PRAGMA table_info(drivers)") as cursor:
            columns = [col[1] for col in await cursor.fetchall()]
        
        if 'occupied_seats' in columns and 'total_seats' in columns:
            async with db.execute(
                "SELECT total_seats, occupied_seats FROM drivers WHERE user_id=?", 
                (driver_id,)
            ) as cursor:
                result = await cursor.fetchone()
            
            if not result:
                return (0, 0, 0)
            
            total = result[0]
            occupied = result[1] or 0
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


async def update_driver_seats(driver_id: int, add_passengers: int):
    """Обновляет количество занятых мест у водителя - ASYNC VERSION"""
    async with get_db() as db:
        await db.execute(
            '''UPDATE drivers 
               SET occupied_seats = occupied_seats + ? 
               WHERE user_id=?''', 
            (add_passengers, driver_id)
        )



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

def direction_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Шетпе → Ақтау", callback_data="dir_shetpe_aktau")],
            [InlineKeyboardButton(text="Ақтау → Шетпе", callback_data="dir_aktau_shetpe")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_main")]
        ]
    )

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
    
    async with get_db() as db:
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
    """Показывает доступные заказы с учетом свободных мест"""
    async with get_db() as db:
        async with db.execute("SELECT direction FROM drivers WHERE user_id=?", (callback.from_user.id,)) as cursor:
            driver_dir = (await cursor.fetchone())[0]
        
        occupied, total, available = await get_driver_available_seats(callback.from_user.id)
        
        if available == 0:
            await callback.answer("❌ Нет свободных мест!", show_alert=True)
            return
        
        # Показываем только тех клиентов, которые поместятся
        async with db.execute('''SELECT user_id, full_name, pickup_location, dropoff_location, 
                            passengers_count, queue_position
                     FROM clients 
                     WHERE direction=? AND status='waiting' AND passengers_count <= ?
                     ORDER BY queue_position''', (driver_dir, available)) as cursor:
            clients = await cursor.fetchall()
    
    if not clients:
        msg = f"❌ Нет подходящих заказов\n\n💺 У вас свободно: {available} мест"
    else:
        msg = f"🔔 <b>Доступные заказы:</b>\n"
        msg += f"💺 Свободно мест: {available}\n\n"
        
        keyboard_buttons = []
        for client in clients:
            msg += f"№{client[5]} - {client[1]} ({client[4]} чел.)\n"
            msg += f"   📍 {client[2]} → {client[3]}\n\n"
            
            keyboard_buttons.append([
                InlineKeyboardButton(
                    text=f"✅ Взять №{client[5]} ({client[4]} чел.)",
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
    """Водитель принимает клиента - FIXED VERSION"""
    client_id = int(callback.data.split("_")[2])
    
    async with get_db() as db:
        # Проверяем доступность клиента
        async with db.execute(
            '''SELECT passengers_count, full_name, pickup_location 
               FROM clients WHERE user_id=? AND status='waiting' ''', 
            (client_id,)
        ) as cursor:
            client = await cursor.fetchone()
        
        if not client:
            await callback.answer("❌ Клиент уже взят другим водителем!", show_alert=True)
            return
        
        occupied, total, available = await get_driver_available_seats(callback.from_user.id)
        
        if client[0] > available:
            await callback.answer(
                f"❌ Недостаточно мест! Нужно: {client[0]}, есть: {available}", 
                show_alert=True
            )
            return
        
        # Принимаем клиента
        await db.execute(
            '''UPDATE clients 
               SET status='accepted', assigned_driver_id=? 
               WHERE user_id=?''', 
            (callback.from_user.id, client_id)
        )
        
        # Обновляем занятость мест
        await update_driver_seats(callback.from_user.id, client[0])
        
        # Создаем trip
        await db.execute(
            '''INSERT INTO trips (driver_id, client_id, status, passengers_count)
               VALUES (?, ?, 'accepted', ?)''', 
            (callback.from_user.id, client_id, client[0])
        )
        
        await db.commit()
        
        # Получаем информацию о машине
        async with db.execute(
            "SELECT car_model, car_number FROM drivers WHERE user_id=?", 
            (callback.from_user.id,)
        ) as cursor:
            car_info = await cursor.fetchone()
    
    await save_log_action(
        callback.from_user.id, 
        "client_accepted", 
        f"Client: {client_id}, Passengers: {client[0]}"
    )
    
    # Уведомляем клиента
    try:
        await bot.send_message(
            client_id,
            f"✅ <b>Водитель принял ваш заказ!</b>\n\n"
            f"🚗 {car_info[0]} ({car_info[1]})\n"
            f"📍 Встреча: {client[2]}\n\n"
            f"Ожидайте водителя!",
            parse_mode="HTML"
        )
    except:
        pass
    
    await callback.answer(f"✅ Клиент {client[1]} добавлен!", show_alert=True)
    await driver_available_orders(callback)

@dp.callback_query(F.data == "driver_arrived")
async def driver_arrived(callback: types.CallbackQuery):
    """Водитель уведомляет всех своих клиентов о прибытии"""
    async with get_db() as db:
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
    async with get_db() as db:
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
                     SET occupied_seats = occupied_seats - ? 
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
    async with get_db() as db:
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
        
        await db.execute("DELETE FROM drivers WHERE user_id=?", (callback.from_user.id,))
        
        # Пересчитываем позиции
        async with db.execute("SELECT DISTINCT direction FROM drivers") as cursor:
            directions = await cursor.fetchall()
        
        for direction in directions:
            async with db.execute('''SELECT user_id FROM drivers 
                         WHERE direction=? ORDER BY queue_position''', direction) as cursor:
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
    direction = State()
    passengers_count = State()
    pickup_location = State()
    dropoff_location = State()

@dp.message(F.text == "🧍‍♂️ Мне нужно такси")
async def client_start(message: types.Message, state: FSMContext):
    async with get_db() as db:
        # Получаем колонки для безопасной работы
        async with db.execute("PRAGMA table_info(clients)") as cursor:
            columns = [col[1] for col in await cursor.fetchall()]
        
        status_idx = columns.index('status') if 'status' in columns else None
        
        async with db.execute("SELECT * FROM clients WHERE user_id=?", (message.from_user.id,)) as cursor:
            client = await cursor.fetchone()
    
    if client and status_idx and len(client) > status_idx:
        client_status = client[status_idx]
        if client_status in ('waiting', 'accepted', 'driver_arrived'):
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отменить заказ", callback_data="client_cancel")]
            ])
            
            status_text = {
                'waiting': '⏳ Ваш заказ в очереди',
                'accepted': '✅ Водитель принял заказ',
                'driver_arrived': '🚗 Водитель на месте!'
            }
            
            await message.answer(
                f"{status_text[client_status]}\n\n"
                f"Вы уже в системе!",
                reply_markup=keyboard
            )
            return
    
    await message.answer(
        "🧍‍♂️ <b>Вызов такси</b>\n\n"
        "Выберите маршрут:",
        reply_markup=direction_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(ClientOrder.direction)

@dp.callback_query(ClientOrder.direction, F.data.startswith("dir_"))
async def client_direction(callback: types.CallbackQuery, state: FSMContext):
    direction = "Шетпе → Ақтау" if callback.data == "dir_shetpe_aktau" else "Ақтау → Шетпе"
    await state.update_data(direction=direction)
    
    # Показываем доступных водителей
    async with get_db() as db:
        async with db.execute('''SELECT COUNT(*), SUM(total_seats - occupied_seats) 
                     FROM drivers 
                     WHERE direction=? AND is_active=1''', (direction,)) as cursor:
            result = await cursor.fetchone()
    
    drivers_count = result[0] or 0
    available_seats = result[1] or 0
    
    await callback.message.edit_text(
        f"✅ Направление: {direction}\n\n"
        f"🚗 Водителей доступно: {drivers_count}\n"
        f"💺 Свободных мест: {available_seats}\n\n"
        f"👥 Сколько человек поедет? (1-8)"
    )
    await state.set_state(ClientOrder.passengers_count)
    await callback.answer()

@dp.message(ClientOrder.passengers_count)
async def client_passengers_count(message: types.Message, state: FSMContext):
    try:
        count = int(message.text)
        if count < 1 or count > 8:
            await message.answer("Ошибка! От 1 до 8 человек")
            return
        
        data = await state.get_data()
        
        # Проверяем, есть ли машины с достаточным количеством мест
        async with get_db() as db:
            async with db.execute('''SELECT COUNT(*) 
                         FROM drivers 
                         WHERE direction=? AND is_active=1 
                         AND (total_seats - occupied_seats) >= ?''',
                      (data['direction'], count)) as cursor:
                suitable_cars = (await cursor.fetchone())[0]
        
        if suitable_cars == 0:
            await message.answer(
                f"❌ К сожалению, нет машин с {count} свободными местами.\n\n"
                f"Попробуйте указать меньше пассажиров или подождите.",
                reply_markup=main_menu_keyboard()
            )
            await state.clear()
            return
        
        await state.update_data(passengers_count=count)
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
    data = await state.get_data()
    
    async with get_db() as db:  # USE ASYNC
        # Вычисляем позицию в очереди
        async with db.execute(
            "SELECT MAX(queue_position) FROM clients WHERE direction=?",
            (data['direction'],)
        ) as cursor:
            max_pos = (await cursor.fetchone())[0]
        
        queue_pos = (max_pos or 0) + 1
        
        # Добавляем клиента
        await db.execute('''INSERT INTO clients 
                     (user_id, full_name, phone, direction, queue_position, passengers_count,
                      pickup_location, dropoff_location, is_verified, status)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 'waiting')''',
                  (message.from_user.id, message.from_user.full_name or "Клиент",
                   "+77777777777", data['direction'], queue_pos, data['passengers_count'],
                   data['pickup_location'], message.text))
        
        # Проверяем подходящих водителей
        async with db.execute('''SELECT COUNT(*), 
                            GROUP_CONCAT(car_model || ' (' || (total_seats - occupied_seats) || ' мест)', ', ')
                     FROM drivers 
                     WHERE direction=? AND is_active=1 
                     AND (total_seats - occupied_seats) >= ?''',
                  (data['direction'], data['passengers_count'])) as cursor:
            suitable = await cursor.fetchone()
        
        # Получаем водителей для уведомления
        async with db.execute('''SELECT user_id FROM drivers 
                     WHERE direction=? AND is_active=1 
                     AND (total_seats - occupied_seats) >= ?''',
                  (data['direction'], data['passengers_count'])) as cursor:
            drivers = await cursor.fetchall()
    
    await save_log_action(message.from_user.id, "client_ordered", 
                   f"Direction: {data['direction']}, Passengers: {data['passengers_count']}")
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить заказ", callback_data="client_cancel")]
    ])
    
    cars_info = suitable[1] if suitable[1] else "нет доступных"
    
    await message.answer(
        f"✅ <b>Такси вызвано!</b>\n\n"
        f"📍 {data['direction']}\n"
        f"👥 Пассажиров: {data['passengers_count']}\n"
        f"📍 От: {data['pickup_location']}\n"
        f"📍 До: {message.text}\n"
        f"📊 Ваша позиция: №{queue_pos}\n\n"
        f"🚗 Подходящие машины ({suitable[0]}):\n"
        f"{cars_info}\n\n"
        f"⏳ Ожидайте, водители видят ваш заказ!",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    
    # Уведомляем водителей о новом заказе
    for driver in drivers:
        try:
            await bot.send_message(
                driver[0],
                f"🔔 <b>Новый заказ!</b>\n\n"
                f"👥 Пассажиров: {data['passengers_count']}\n"
                f"📍 {data['pickup_location']} → {message.text}\n\n"
                f"Проверьте доступные заказы: /driver",
                parse_mode="HTML"
            )
        except:
            pass
    
    await state.clear()

@dp.callback_query(F.data == "client_cancel")
async def client_cancel_order(callback: types.CallbackQuery):
    async with get_db() as db:
        async with db.execute("SELECT * FROM clients WHERE user_id=?", (callback.from_user.id,)) as cursor:
            client = await cursor.fetchone()
        
        if not client:
            await callback.answer("❌ У вас нет активного заказа", show_alert=True)
            return
        
        # Если клиент был принят водителем, освобождаем места
        if client[11]:  # assigned_driver_id
            await update_driver_seats(client[11], -client[5])  # Отнимаем пассажиров
            
            # Уведомляем водителя
            try:
                await bot.send_message(
                    client[11],
                    f"⚠️ Клиент {client[1]} отменил заказ\n"
                    f"Освобождено мест: {client[5]}",
                    parse_mode="HTML"
                )
            except:
                pass
        
        # Обновляем trip
        await db.execute('''UPDATE trips SET status='cancelled', cancelled_by='client', 
                     cancelled_at=CURRENT_TIMESTAMP 
                     WHERE client_id=? AND status IN ('waiting', 'accepted', 'driver_arrived')''',
                  (callback.from_user.id,))
        
        # Удаляем клиента
        direction = client[3]
        await db.execute("DELETE FROM clients WHERE user_id=?", (callback.from_user.id,))
        
        # Пересчитываем позиции
        async with db.execute('''SELECT user_id FROM clients 
                     WHERE direction=? ORDER BY queue_position''', (direction,)) as cursor:
            clients = await cursor.fetchall()
        
        for pos, client_id in enumerate(clients, 1):
            await db.execute("UPDATE clients SET queue_position=? WHERE user_id=?", (pos, client_id[0]))
    
    await save_log_action(callback.from_user.id, "client_cancelled", "")
    
    await callback.message.edit_text(
        "❌ <b>Заказ отменен</b>\n\n"
        "Вы можете вызвать новое такси в любое время.",
        parse_mode="HTML"
    )
    await callback.answer()

# ==================== РЕЙТИНГИ ====================

class RatingStates(StatesGroup):
    select_rating = State()
    write_review = State()

@dp.message(F.text == "⭐ Мой профиль")
async def show_profile(message: types.Message):
    async with get_db() as db:
        async with db.execute("SELECT avg_rating, rating_count FROM drivers WHERE user_id=?", (message.from_user.id,)) as cursor:
            driver = await cursor.fetchone()
        async with db.execute("SELECT avg_rating, rating_count FROM clients WHERE user_id=?", (message.from_user.id,)) as cursor:
            client = await cursor.fetchone()
        if not driver and not client:
            await message.answer("❌ Вы еще не зарегистрированы")
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
        async with db.execute('''SELECT from_user_id, rating, review, created_at 
                                 FROM ratings WHERE to_user_id=? 
                                 ORDER BY created_at DESC LIMIT 5''', (message.from_user.id,)) as cursor:
            reviews = await cursor.fetchall()
        if reviews:
            msg += "<b>Последние отзывы:</b>\n"
            for review in reviews:
                stars = "⭐" * review[1]
                msg += f"\n{stars}\n"
                if review[2]:
                    msg += f"💬 {review[2]}\n"
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
    
    async with get_db() as db:
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
        
        async with db.execute("SELECT SUM(total_seats - occupied_seats) FROM drivers WHERE is_active=1") as cursor:
            total_available_seats = (await cursor.fetchone())[0] or 0
    
    msg = "📊 <b>Статистика:</b>\n\n"
    msg += f"👥 Водителей: {total_drivers}\n"
    msg += f"💺 Свободных мест: {total_available_seats}\n\n"
    msg += f"🧍‍♂️ Клиентов в ожидании: {waiting_clients}\n"
    msg += f"✅ Клиентов принято: {accepted_clients}\n\n"
    msg += f"✅ Завершено поездок: {completed_trips}\n"
    msg += f"❌ Отменено поездок: {cancelled_trips}\n"
    msg += f"⭐ Средний рейтинг: {avg_rating:.1f}\n"
    
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
        async with get_db() as db:
            await db.execute("INSERT INTO admins (user_id) VALUES (?)", (new_admin_id,))
        
        await save_log_action(message.from_user.id, "admin_added", f"New admin: {new_admin_id}")
        await message.answer(f"✅ Админ добавлен: {new_admin_id}")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

# ==================== СТАРТ ====================

async def main():
    await init_db()
    logger.info("🚀 Бот запущен")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())