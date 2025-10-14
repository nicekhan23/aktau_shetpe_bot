import asyncio
import sqlite3
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
import random
import string

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
DATABASE_FILE = os.getenv("DATABASE_FILE", "taxi_bot.db")
SMS_API_KEY = os.getenv("SMS_API_KEY", "")  # Для SMS сервиса

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

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
    """Логирует действия пользователей"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"[{timestamp}] User {user_id}: {action}"
    if details:
        log_msg += f" | {details}"
    logger.info(log_msg)

# ==================== УТИЛИТЫ SMS ====================

def generate_verification_code() -> str:
    """Генерирует 4-значный код"""
    return ''.join(random.choices(string.digits, k=4))

async def send_sms(phone: str, message: str) -> bool:
    """Отправляет SMS через внешний сервис"""
    # В реальности используйте API вашего SMS провайдера
    # Пример для Казахстана: Nexmo, Twilio, местные сервисы
    logger.info(f"SMS отправлено на {phone}: {message}")
    # return await actual_sms_service.send(phone, message)
    return True

# ==================== БД И МИГРАЦИИ ====================

class DBMigration:
    """Управление версионированием БД"""
    
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
        """Применяет миграции"""
        current_version = DBMigration.get_db_version()
        
        if current_version < 1:
            DBMigration.migration_v1()
        if current_version < 2:
            DBMigration.migration_v2()
        if current_version < 3:
            DBMigration.migration_v3()
    
    @staticmethod
    def migration_v1():
        """Миграция v1: Создание базовых таблиц"""
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
        """Миграция v2: Добавляем рейтинги и отзывы"""
        logger.info("Применяю миграцию v2...")
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        c.execute('''CREATE TABLE IF NOT EXISTS ratings
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      from_user_id INTEGER,
                      to_user_id INTEGER,
                      user_type TEXT,
                      rating INTEGER CHECK(rating >= 1 AND rating <= 5),
                      review TEXT,
                      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
        # Добавляем колонки для рейтингов в профили
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
        """Миграция v3: История поездок и отмены"""
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

def init_db():
    """Инициализирует БД и применяет миграции"""
    DBMigration.migrate()

# ==================== УТИЛИТЫ ====================

def is_admin(user_id: int) -> bool:
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute("SELECT user_id FROM admins WHERE user_id=?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result is not None

def save_log_action(user_id: int, action: str, details: str = ""):
    """Сохраняет действие в БД"""
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute('''INSERT INTO actions_log (user_id, action, details) 
                 VALUES (?, ?, ?)''', (user_id, action, details))
    conn.commit()
    conn.close()
    log_action(user_id, action, details)

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
    """Конвертирует рейтинг в звезды"""
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
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute("SELECT * FROM drivers WHERE user_id=?", (message.from_user.id,))
    driver = c.fetchone()
    conn.close()
    
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
    
    # Валидация номера
    if not phone.startswith('+7') or len(phone) != 12:
        await message.answer("❌ Неверный формат! Используйте +7XXXXXXXXXX")
        return
    
    # Проверяем, не зарегистрирован ли уже
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute("SELECT user_id FROM drivers WHERE phone=?", (phone,))
    existing = c.fetchone()
    conn.close()
    
    if existing:
        await message.answer("❌ Этот номер уже зарегистрирован!")
        return
    
    # Генерируем код и отправляем SMS
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
    
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    # Вычисляем позицию в очереди
    c.execute(
        "SELECT MAX(queue_position) FROM drivers WHERE direction=? AND is_active=1",
        (direction,)
    )
    max_pos = c.fetchone()[0]
    queue_pos = (max_pos or 0) + 1
    
    # Добавляем водителя
    c.execute('''INSERT INTO drivers 
                 (user_id, full_name, phone, car_number, car_model, total_seats, 
                  direction, queue_position, is_active, is_verified)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 1)''',
              (callback.from_user.id, data['full_name'], data['phone'], data['car_number'],
               data['car_model'], data['seats'], direction, queue_pos))
    conn.commit()
    conn.close()
    
    save_log_action(callback.from_user.id, "driver_registered", 
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
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute("SELECT * FROM drivers WHERE user_id=?", (user_id,))
    driver = c.fetchone()
    conn.close()
    
    if not driver:
        await message.answer("Ошибка: вы не зарегистрированы", reply_markup=main_menu_keyboard())
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Мой статус", callback_data="driver_status")],
        [InlineKeyboardButton(text="👥 Мои пассажиры", callback_data="driver_passengers")],
        [InlineKeyboardButton(text="🚗 Я приехал!", callback_data="driver_arrived")],
        [InlineKeyboardButton(text="❌ Выйти из очереди", callback_data="driver_exit")],
        [InlineKeyboardButton(text="🔙 Меню", callback_data="back_main")]
    ])
    
    await message.answer(
        f"🚗 <b>Профиль водителя</b>\n\n"
        f"👤 {driver[1]}\n"
        f"🚗 {driver[4]} ({driver[3]})\n"
        f"💺 Мест: {driver[5]}\n"
        f"📍 {driver[6]}\n"
        f"📊 Позиция: №{driver[7]}\n"
        f"{get_rating_stars(driver[19] if len(driver) > 19 else 0)}\n\n"
        "Выберите действие:",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "driver_status")
async def driver_status(callback: types.CallbackQuery):
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute("SELECT * FROM drivers WHERE user_id=?", (callback.from_user.id,))
    driver = c.fetchone()
    
    c.execute("SELECT COUNT(*) FROM clients WHERE direction=?", (driver[6],))
    waiting = c.fetchone()[0]
    conn.close()
    
    await callback.message.edit_text(
        f"📊 <b>Ваш статус</b>\n\n"
        f"🚗 {driver[4]} ({driver[3]})\n"
        f"📍 {driver[6]}\n"
        f"📊 Позиция: №{driver[7]}\n"
        f"⏳ Клиентов в очереди: {waiting}\n"
        f"{get_rating_stars(driver[19] if len(driver) > 19 else 0)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="driver_menu")]
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "driver_passengers")
async def driver_passengers(callback: types.CallbackQuery):
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    c.execute("SELECT direction FROM drivers WHERE user_id=?", (callback.from_user.id,))
    driver_dir = c.fetchone()[0]
    
    c.execute('''SELECT user_id, full_name, pickup_location, dropoff_location, passengers_count
                 FROM clients WHERE direction=? AND status='waiting' ORDER BY queue_position''', (driver_dir,))
    clients = c.fetchall()
    conn.close()
    
    if not clients:
        msg = "❌ Нет клиентов в очереди"
    else:
        msg = f"👥 <b>Клиенты в очереди ({len(clients)}):</b>\n\n"
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

@dp.callback_query(F.data == "driver_arrived")
async def driver_arrived(callback: types.CallbackQuery):
    """Водитель нажал что он приехал"""
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    c.execute("SELECT direction FROM drivers WHERE user_id=?", (callback.from_user.id,))
    driver_dir = c.fetchone()[0]
    
    # Берем первого клиента в очереди
    c.execute('''SELECT user_id, full_name, pickup_location, dropoff_location 
                 FROM clients WHERE direction=? AND status='waiting' 
                 ORDER BY queue_position LIMIT 1''', (driver_dir,))
    client = c.fetchone()
    
    if not client:
        await callback.answer("❌ Нет клиентов в очереди!", show_alert=True)
        conn.close()
        return
    
    client_id = client[0]
    
    # Обновляем статус клиента
    c.execute("UPDATE clients SET status='driver_arrived' WHERE user_id=?", (client_id,))
    
    # Создаем запись о поездке
    c.execute('''INSERT INTO trips (driver_id, client_id, status, driver_arrived_at)
                 SELECT user_id, ?, 'driver_arrived', CURRENT_TIMESTAMP 
                 FROM drivers WHERE user_id=?''', (client_id, callback.from_user.id))
    
    conn.commit()
    conn.close()
    
    save_log_action(callback.from_user.id, "driver_arrived", f"Client: {client_id}")
    
    # Отправляем сообщение клиенту
    try:
        await bot.send_message(
            client_id,
            f"🚗 <b>Водитель приехал!</b>\n\n"
            f"📍 Место встречи: {client[2]}\n"
            f"🆔 Номер машины: смотрите профиль водителя",
            parse_mode="HTML"
        )
    except:
        pass
    
    await callback.answer(f"✅ Уведомление отправлено клиенту {client[1]}", show_alert=True)

@dp.callback_query(F.data == "driver_exit")
async def driver_exit(callback: types.CallbackQuery):
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    # Проверяем, есть ли активные поездки
    c.execute('''SELECT COUNT(*) FROM trips 
                 WHERE driver_id=? AND status IN ('waiting', 'driver_arrived')''', 
              (callback.from_user.id,))
    active_trips = c.fetchone()[0]
    
    if active_trips > 0:
        await callback.answer(
            "❌ Вы не можете выйти - есть активные поездки!",
            show_alert=True
        )
        conn.close()
        return
    
    c.execute("DELETE FROM drivers WHERE user_id=?", (callback.from_user.id,))
    
    # Пересчитываем позиции
    c.execute("SELECT DISTINCT direction FROM drivers")
    directions = c.fetchall()
    
    for direction in directions:
        c.execute('''SELECT user_id FROM drivers 
                     WHERE direction=? ORDER BY queue_position''', direction)
        drivers = c.fetchall()
        for pos, driver in enumerate(drivers, 1):
            c.execute("UPDATE drivers SET queue_position=? WHERE user_id=?", (pos, driver[0]))
    
    conn.commit()
    conn.close()
    
    save_log_action(callback.from_user.id, "driver_exit", "")
    
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
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute("SELECT * FROM clients WHERE user_id=?", (message.from_user.id,))
    client = c.fetchone()
    conn.close()
    
    if client and client[8]:  # is_verified
        if client[10] == 'waiting':  # status
            await message.answer(
                "❌ Вы уже в очереди!",
                reply_markup=main_menu_keyboard()
            )
            return
    
    if client and not client[8]:
        # Переотправляем код
        code = generate_verification_code()
        await send_sms(client[2], f"Код подтверждения: {code}")
        await message.answer(
            f"⏳ Вы уже зарегистрированы.\n"
            f"SMS отправлено на {client[2]}\n"
            "Введите код:"
        )
        await state.set_state(ClientOrder.direction)  # Переиспользуем state
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
    
    await callback.message.edit_text(
        "👥 Сколько человек поедет? (1-8)"
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
        await state.update_data(passengers_count=count)
        await message.answer("📍 Откуда вас забрать?\n\nВведите адрес:")
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
    
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    # Проверяем верификацию
    c.execute("SELECT is_verified FROM clients WHERE user_id=?", (message.from_user.id,))
    existing = c.fetchone()
    
    if existing and not existing[0]:
        # Отправляем код
        code = generate_verification_code()
        c.execute("UPDATE clients SET verification_code=? WHERE user_id=?", 
                 (code, message.from_user.id))
        conn.commit()
        await send_sms(message.from_user.id, f"Код: {code}")  # В реальности номер телефона
        await message.answer("Введите код из 