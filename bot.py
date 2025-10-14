import asyncio
import sqlite3
import os
from datetime import datetime
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
DATABASE_FILE = os.getenv("DATABASE_FILE", "taxi_bot.db")

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ==================== ДБ ====================

def init_db():
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    # Водители - простая очередь
    c.execute('''CREATE TABLE IF NOT EXISTS drivers
                 (user_id INTEGER PRIMARY KEY,
                  full_name TEXT NOT NULL,
                  car_number TEXT NOT NULL,
                  car_model TEXT NOT NULL,
                  total_seats INTEGER NOT NULL,
                  direction TEXT NOT NULL,
                  queue_position INTEGER NOT NULL,
                  is_active INTEGER DEFAULT 1,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    # Клиенты - простая очередь
    c.execute('''CREATE TABLE IF NOT EXISTS clients
                 (user_id INTEGER PRIMARY KEY,
                  full_name TEXT,
                  direction TEXT NOT NULL,
                  queue_position INTEGER NOT NULL,
                  pickup_location TEXT NOT NULL,
                  dropoff_location TEXT NOT NULL,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    # Админы
    c.execute('''CREATE TABLE IF NOT EXISTS admins
                 (user_id INTEGER PRIMARY KEY)''')
    
    conn.commit()
    conn.close()

# ==================== УТИЛИТЫ ====================

def is_admin(user_id: int) -> bool:
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute("SELECT user_id FROM admins WHERE user_id=?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result is not None

def main_menu_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚗 Я водитель")],
            [KeyboardButton(text="🧍‍♂️ Мне нужно такси")],
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

# ==================== ВОДИТЕЛИ ====================

class DriverReg(StatesGroup):
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
    
    if driver:
        await show_driver_menu(message, message.from_user.id)
        return
    
    await message.answer(
        "🚗 <b>Регистрация водителя</b>\n\n"
        "Введите ваше полное имя:",
        parse_mode="HTML"
    )
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
                 (user_id, full_name, car_number, car_model, total_seats, direction, queue_position)
                 VALUES (?, ?, ?, ?, ?, ?, ?)''',
              (callback.from_user.id, data['full_name'], data['car_number'],
               data['car_model'], data['seats'], direction, queue_pos))
    conn.commit()
    conn.close()
    
    await callback.message.edit_text(
        f"✅ <b>Вы зарегистрированы!</b>\n\n"
        f"👤 {data['full_name']}\n"
        f"🚗 {data['car_model']} ({data['car_number']})\n"
        f"💺 Мест: {data['seats']}\n"
        f"📍 {direction}\n"
        f"📊 Ваша позиция в очереди: №{queue_pos}\n\n"
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
        [InlineKeyboardButton(text="❌ Выйти из очереди", callback_data="driver_exit")],
        [InlineKeyboardButton(text="🔙 Меню", callback_data="back_main")]
    ])
    
    queue_info = f"📊 Позиция в очереди: №{driver[6]}"
    
    await message.answer(
        f"🚗 <b>Профиль водителя</b>\n\n"
        f"👤 {driver[1]}\n"
        f"🚗 {driver[3]} ({driver[2]})\n"
        f"💺 Мест: {driver[4]}\n"
        f"📍 {driver[5]}\n"
        f"{queue_info}\n\n"
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
    
    # Сколько клиентов ждут в этом направлении
    c.execute("SELECT COUNT(*) FROM clients WHERE direction=?", (driver[5],))
    waiting = c.fetchone()[0]
    conn.close()
    
    await callback.message.edit_text(
        f"📊 <b>Ваш статус</b>\n\n"
        f"🚗 {driver[3]} ({driver[2]})\n"
        f"📍 {driver[5]}\n"
        f"📊 Позиция: №{driver[6]}\n"
        f"⏳ Клиентов в очереди: {waiting}",
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
    
    c.execute('''SELECT full_name, pickup_location, dropoff_location 
                 FROM clients WHERE direction=? ORDER BY queue_position''', (driver_dir,))
    clients = c.fetchall()
    conn.close()
    
    if not clients:
        msg = "❌ Нет клиентов в очереди"
    else:
        msg = f"👥 <b>Клиенты в очереди ({len(clients)}):</b>\n\n"
        for i, client in enumerate(clients, 1):
            msg += f"{i}. {client[0]}\n"
            msg += f"   📍 {client[1]} → {client[2]}\n\n"
    
    await callback.message.edit_text(
        msg,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="driver_menu")]
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "driver_exit")
async def driver_exit(callback: types.CallbackQuery):
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    # Удаляем водителя
    c.execute("DELETE FROM drivers WHERE user_id=?", (callback.from_user.id,))
    
    # Пересчитываем позиции оставшихся водителей в каждом направлении
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
    pickup_location = State()
    dropoff_location = State()

@dp.message(F.text == "🧍‍♂️ Мне нужно такси")
async def client_start(message: types.Message, state: FSMContext):
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute("SELECT * FROM clients WHERE user_id=?", (message.from_user.id,))
    client = c.fetchone()
    conn.close()
    
    if client:
        await message.answer(
            "❌ Вы уже в очереди!",
            reply_markup=main_menu_keyboard()
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
    
    await callback.message.edit_text(
        "📍 Откуда вас забрать?\n\n"
        "Введите адрес:"
    )
    await state.set_state(ClientOrder.pickup_location)
    await callback.answer()

@dp.message(ClientOrder.pickup_location)
async def client_pickup(message: types.Message, state: FSMContext):
    await state.update_data(pickup_location=message.text)
    await message.answer(
        "📍 Куда вас везти?\n\n"
        "Введите адрес:"
    )
    await state.set_state(ClientOrder.dropoff_location)

@dp.message(ClientOrder.dropoff_location)
async def client_dropoff(message: types.Message, state: FSMContext):
    data = await state.get_data()
    
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    # Вычисляем позицию в очереди
    c.execute(
        "SELECT MAX(queue_position) FROM clients WHERE direction=?",
        (data['direction'],)
    )
    max_pos = c.fetchone()[0]
    queue_pos = (max_pos or 0) + 1
    
    # Добавляем клиента
    c.execute('''INSERT INTO clients 
                 (user_id, full_name, direction, queue_position, pickup_location, dropoff_location)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (message.from_user.id, message.from_user.full_name or "Клиент",
               data['direction'], queue_pos, data['pickup_location'], message.text))
    conn.commit()
    
    # Проверяем свободных водителей
    c.execute("SELECT COUNT(*) FROM drivers WHERE direction=? AND is_active=1", (data['direction'],))
    drivers = c.fetchone()[0]
    
    conn.close()
    
    await message.answer(
        f"✅ <b>Такси вызвано!</b>\n\n"
        f"📍 {data['direction']}\n"
        f"📍 От: {data['pickup_location']}\n"
        f"📍 До: {message.text}\n"
        f"📊 Ваша позиция: №{queue_pos}\n"
        f"🚗 Свободных водителей: {drivers}\n\n"
        f"⏳ Такси скоро приедет!",
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML"
    )
    await state.clear()

# ==================== ОБЩЕЕ ====================

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        f"Привет, {message.from_user.first_name}! 👋\n\n"
        "🚖 <b>Такси Шетпе–Ақтау</b>\n\n"
        "Выберите, кто вы:",
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML"
    )

@dp.message(F.text == "ℹ️ Информация")
async def info_command(message: types.Message):
    await message.answer(
        "ℹ️ <b>О нас</b>\n\n"
        "🚖 Система заказа такси в реальном времени\n\n"
        "<b>Для водителей:</b>\n"
        "• Встаньте в очередь\n"
        "• Смотрите, кто вас ждет\n"
        "• Везите клиентов\n\n"
        "<b>Для клиентов:</b>\n"
        "• Вызовите такси\n"
        "• Укажите адреса\n"
        "• Ждите водителя\n\n"
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
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_main")]
    ])

@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Нет доступа")
        return
    
    await message.answer(
        "🔐 <b>Админ панель</b>",
        reply_markup=admin_keyboard(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "admin_drivers")
async def admin_drivers(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute("SELECT * FROM drivers ORDER BY direction, queue_position")
    drivers = c.fetchall()
    conn.close()
    
    if not drivers:
        msg = "❌ Водителей нет"
    else:
        msg = "👥 <b>Водители:</b>\n\n"
        for driver in drivers:
            msg += f"№{driver[6]} - {driver[1]}\n"
            msg += f"   🚗 {driver[3]} ({driver[2]})\n"
            msg += f"   📍 {driver[5]}\n"
    
    await callback.message.edit_text(msg, reply_markup=admin_keyboard(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_clients")
async def admin_clients(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute("SELECT * FROM clients ORDER BY direction, queue_position")
    clients = c.fetchall()
    conn.close()
    
    if not clients:
        msg = "❌ Клиентов нет"
    else:
        msg = "🧍‍♂️ <b>Клиенты в очереди:</b>\n\n"
        for client in clients:
            msg += f"№{client[3]} - {client[1]}\n"
            msg += f"   📍 {client[2]}\n"
            msg += f"   От: {client[4]}\n"
            msg += f"   До: {client[5]}\n\n"
    
    await callback.message.edit_text(msg, reply_markup=admin_keyboard(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    c.execute("SELECT COUNT(*) FROM drivers")
    total_drivers = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM clients")
    total_clients = c.fetchone()[0]
    
    c.execute("SELECT direction, COUNT(*) FROM drivers GROUP BY direction")
    by_direction = c.fetchall()
    
    conn.close()
    
    msg = "📊 <b>Статистика:</b>\n\n"
    msg += f"👥 Водителей: {total_drivers}\n"
    msg += f"🧍‍♂️ Клиентов: {total_clients}\n\n"
    msg += "<b>По маршрутам:</b>\n"
    for direction, count in by_direction:
        msg += f"   {direction}: {count}\n"
    
    await callback.message.edit_text(msg, reply_markup=admin_keyboard(), parse_mode="HTML")
    await callback.answer()

# ==================== СТАРТ ====================

async def main():
    init_db()
    print("🚀 Бот запущен")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())