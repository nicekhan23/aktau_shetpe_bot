import asyncio
import sqlite3
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

# .env файлын жүктеу
load_dotenv()

# Конфигурация
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
KASPI_PHONE = os.getenv("KASPI_PHONE", "+7_XXX_XXX_XX_XX")
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "@support")
DATABASE_FILE = os.getenv("DATABASE_FILE", "taxi_bot.db")
PAYMENT_AMOUNT = int(os.getenv("PAYMENT_AMOUNT", "1000"))

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Админдерді тексеру
def is_admin(user_id: int) -> bool:
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute("SELECT user_id FROM admins WHERE user_id=?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result is not None

# FSM States
class DriverRegistration(StatesGroup):
    full_name = State()
    car_number = State()
    car_model = State()
    seats = State()
    direction = State()
    datetime_select = State()  # Күн және уақыт таңдау

class ClientBooking(StatesGroup):
    direction = State()
    datetime_select = State()  # Күн және уақыт таңдау
    pickup_location = State()
    dropoff_location = State()
    select_car = State()

class DriverEdit(StatesGroup):
    change_datetime = State()
    change_direction = State()
    change_car_model = State()
    change_car_number = State()
    change_seats = State()

# Дерекқорды инициализациялау
def init_db():
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS drivers
                 (user_id INTEGER PRIMARY KEY,
                  full_name TEXT NOT NULL,
                  car_number TEXT NOT NULL,
                  car_model TEXT NOT NULL,
                  total_seats INTEGER NOT NULL,
                  direction TEXT NOT NULL,
                  departure_date TEXT NOT NULL,
                  departure_time TEXT NOT NULL,
                  queue_position INTEGER NOT NULL,
                  is_active INTEGER DEFAULT 0,
                  payment_status INTEGER DEFAULT 0,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS clients
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  full_name TEXT,
                  phone TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS bookings
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  client_id INTEGER,
                  driver_id INTEGER,
                  direction TEXT,
                  departure_date TEXT,
                  pickup_location TEXT,
                  dropoff_location TEXT,
                  booking_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  status TEXT DEFAULT 'active',
                  FOREIGN KEY (client_id) REFERENCES clients(id),
                  FOREIGN KEY (driver_id) REFERENCES drivers(user_id))''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS admins
                 (user_id INTEGER PRIMARY KEY)''')
    
    conn.commit()
    conn.close()

# Басты мәзір
def main_menu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚗 Жүргізуші ретінде кіру")],
            [KeyboardButton(text="🧍‍♂️ Клиент ретінде кіру")],
            [KeyboardButton(text="📊 Менің брондарым")],
            [KeyboardButton(text="ℹ️ Анықтама")]
        ],
        resize_keyboard=True
    )
    return keyboard

# Бағыт таңдау
def direction_keyboard():
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Шетпе → Ақтау", callback_data="dir_shetpe_aktau")],
            [InlineKeyboardButton(text="Ақтау → Шетпе", callback_data="dir_aktau_shetpe")],
            [InlineKeyboardButton(text="🔙 Артқа", callback_data="back_main")]
        ]
    )
    return keyboard

# Күн және уақыт таңдау (біріктірілген)
def datetime_keyboard(show_back_to_direction=True):
    keyboard_buttons = []
    today = datetime.now()
    
    # Күн опциялары
    for i in range(7):
        date = today + timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")
        
        if i == 0:
            display_text = f"🟢 Бүгін ({date.strftime('%d.%m')})"
        elif i == 1:
            display_text = f"🟡 Ертең ({date.strftime('%d.%m')})"
        else:
            weekdays = ["Дс", "Бс", "Сс", "Ср", "Бм", "Жм", "Сб"]
            weekday = weekdays[date.weekday()]
            display_text = f"{weekday} ({date.strftime('%d.%m')})"
        
        keyboard_buttons.append([InlineKeyboardButton(
            text=display_text,
            callback_data=f"dt_date_{date_str}"
        )])
    
    if show_back_to_direction:
        keyboard_buttons.append([InlineKeyboardButton(text="🔙 Артқа", callback_data="back_direction")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

# Уақыт таңдау (күн таңдағаннан кейін)
def time_keyboard(selected_date):
    keyboard_buttons = []
    
    # Уақыт опциялары (06:00-ден 22:00-ге дейін, әр 2 сағат сайын)
    times = ["06:00", "08:00", "10:00", "12:00", "14:00", "16:00", "18:00", "20:00", "22:00"]
    
    # 2 бағанға бөлу
    for i in range(0, len(times), 2):
        row = []
        for j in range(2):
            if i + j < len(times):
                time = times[i + j]
                row.append(InlineKeyboardButton(
                    text=f"🕐 {time}",
                    callback_data=f"dt_time_{selected_date}_{time}"
                ))
        keyboard_buttons.append(row)
    
    # Басқа уақыт қолмен енгізу опциясы
    keyboard_buttons.append([InlineKeyboardButton(
        text="⏰ Басқа уақыт (қолмен енгізу)",
        callback_data=f"dt_custom_{selected_date}"
    )])
    
    keyboard_buttons.append([InlineKeyboardButton(text="🔙 Күн таңдауға", callback_data="back_datetime")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

# Орын таңдау (Шетпе аудандары)
def shetpe_locations_keyboard():
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Шетпе орталығы", callback_data="loc_shetpe_center")],
            [InlineKeyboardButton(text="Қызылсай", callback_data="loc_kyzylsay")],
            [InlineKeyboardButton(text="Қарақия", callback_data="loc_karakiya")],
            [InlineKeyboardButton(text="Сайын", callback_data="loc_saiyn")],
            [InlineKeyboardButton(text="Басқа жер", callback_data="loc_other")],
            [InlineKeyboardButton(text="🔙 Артқа", callback_data="back_datetime")]
        ]
    )
    return keyboard

# Ақтау орындары
def aktau_locations_keyboard():
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="15 микрорайон", callback_data="loc_15mkr")],
            [InlineKeyboardButton(text="9 микрорайон", callback_data="loc_9mkr")],
            [InlineKeyboardButton(text="Автовокзал", callback_data="loc_avtovokzal")],
            [InlineKeyboardButton(text="Базар", callback_data="loc_bazar")],
            [InlineKeyboardButton(text="Басқа жер", callback_data="loc_other")],
            [InlineKeyboardButton(text="🔙 Артқа", callback_data="back_datetime")]
        ]
    )
    return keyboard

# Күнді форматтау
def format_date_display(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)
    
    if date_obj.date() == today:
        return f"Бүгін ({date_obj.strftime('%d.%m.%Y')})"
    elif date_obj.date() == tomorrow:
        return f"Ертең ({date_obj.strftime('%d.%m.%Y')})"
    else:
        weekdays = ["Дүйсенбі", "Сейсенбі", "Сәрсенбі", "Бейсенбі", "Жұма", "Сенбі", "Жексенбі"]
        weekday = weekdays[date_obj.weekday()]
        return f"{weekday} ({date_obj.strftime('%d.%m.%Y')})"

# /start командасы
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        f"Сәлеметсіз бе, {message.from_user.first_name}! 👋\n\n"
        "🚖 <b>Шетпе–Ақтау–Шетпе такси жүйесіне қош келдіңіз!</b>\n\n"
        "Бұл бот арқылы сіз:\n"
        "✅ Жүргізуші ретінде көлікті кезекке қоса аласыз\n"
        "✅ Клиент ретінде орын брондай аласыз\n"
        "✅ Өзіңіздің брондарыңызды бақылай аласыз\n"
        "✅ 7 күнге дейін алдын ала брондай аласыз\n\n"
        "Төмендегі мәзірден қажеттісін таңдаңыз:",
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML"
    )

# Жүргізуші тіркеуі
@dp.message(F.text == "🚗 Жүргізуші ретінде кіру")
async def driver_registration_start(message: types.Message, state: FSMContext):
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    c.execute("SELECT * FROM drivers WHERE user_id=?", (message.from_user.id,))
    driver = c.fetchone()
    conn.close()
    
    if driver:
        await message.answer(
            "Сіз жүргізуші ретінде тіркелгенсіз!\n\n"
            "Қосымша опциялар үшін /driver командасын пайдаланыңыз."
        )
        return
    
    await message.answer(
        "🚗 <b>Жүргізуші тіркеуі</b>\n\n"
        "Аты-жөніңізді толық енгізіңіз:\n"
        "(Мысалы: Айдос Нұрланұлы)",
        parse_mode="HTML"
    )
    await state.set_state(DriverRegistration.full_name)

@dp.message(DriverRegistration.full_name)
async def driver_full_name(message: types.Message, state: FSMContext):
    await state.update_data(full_name=message.text)
    await message.answer(
        "Көлік нөміріңізді енгізіңіз:\n"
        "(Мысалы: 870 ABC 09)"
    )
    await state.set_state(DriverRegistration.car_number)

@dp.message(DriverRegistration.car_number)
async def driver_car_number(message: types.Message, state: FSMContext):
    await state.update_data(car_number=message.text)
    await message.answer(
        "Көлік маркасын енгізіңіз:\n"
        "(Мысалы: Toyota Camry)"
    )
    await state.set_state(DriverRegistration.car_model)

@dp.message(DriverRegistration.car_model)
async def driver_car_model(message: types.Message, state: FSMContext):
    await state.update_data(car_model=message.text)
    await message.answer(
        "Бос орын санын енгізіңіз:\n"
        "(Мысалы: 4)"
    )
    await state.set_state(DriverRegistration.seats)

@dp.message(DriverRegistration.seats)
async def driver_seats(message: types.Message, state: FSMContext):
    try:
        seats = int(message.text)
        if seats < 1 or seats > 8:
            await message.answer("Орын саны 1-ден 8-ге дейін болуы керек. Қайта енгізіңіз:")
            return
        await state.update_data(seats=seats)
        await message.answer(
            "Бағытты таңдаңыз:",
            reply_markup=direction_keyboard()
        )
        await state.set_state(DriverRegistration.direction)
    except ValueError:
        await message.answer("Тек сан енгізіңіз. Қайта көріңіз:")

@dp.callback_query(DriverRegistration.direction, F.data.startswith("dir_"))
async def driver_direction(callback: types.CallbackQuery, state: FSMContext):
    direction = "Шетпе → Ақтау" if callback.data == "dir_shetpe_aktau" else "Ақтау → Шетпе"
    await state.update_data(direction=direction)
    await callback.message.edit_text(
        f"✅ Бағыт: {direction}\n\n"
        "📅 Қай күні жүресіз?",
        reply_markup=datetime_keyboard()
    )
    await callback.answer()
    await state.set_state(DriverRegistration.datetime_select)

# Күн таңдау (жүргізуші)
@dp.callback_query(DriverRegistration.datetime_select, F.data.startswith("dt_date_"))
async def driver_date_select(callback: types.CallbackQuery, state: FSMContext):
    date_str = callback.data.replace("dt_date_", "")
    await state.update_data(departure_date=date_str)
    
    date_display = format_date_display(date_str)
    
    await callback.message.edit_text(
        f"✅ Күні: {date_display}\n\n"
        "🕐 Шығу уақытын таңдаңыз:",
        reply_markup=time_keyboard(date_str)
    )
    await callback.answer()

# Уақыт таңдау (жүргізуші)
@dp.callback_query(DriverRegistration.datetime_select, F.data.startswith("dt_time_"))
async def driver_time_select(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    time_str = parts[-1]
    
    await state.update_data(departure_time=time_str)
    data = await state.get_data()
    
    # Кезек позициясын анықтау
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    c.execute("""SELECT MAX(queue_position) FROM drivers 
                 WHERE direction=? AND departure_date=?""", 
              (data['direction'], data['departure_date']))
    max_pos = c.fetchone()[0]
    queue_pos = (max_pos or 0) + 1
    
    # Жүргізушіні сақтау
    c.execute('''INSERT INTO drivers 
                 (user_id, full_name, car_number, car_model, total_seats, direction, 
                  departure_date, departure_time, queue_position)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
              (callback.from_user.id, data['full_name'], data['car_number'], 
               data['car_model'], data['seats'], data['direction'], 
               data['departure_date'], data['departure_time'], queue_pos))
    conn.commit()
    conn.close()
    
    date_display = format_date_display(data['departure_date'])
    
    await callback.message.edit_text(
        "✅ <b>Тіркеу сәтті аяқталды!</b>\n\n"
        f"👤 Аты-жөні: {data['full_name']}\n"
        f"🚗 Көлік: {data['car_model']} ({data['car_number']})\n"
        f"💺 Орын саны: {data['seats']}\n"
        f"📍 Бағыт: {data['direction']}\n"
        f"📅 Күні: {date_display}\n"
        f"🕐 Кету уақыты: {data['departure_time']}\n"
        f"📊 Кезектегі орын: №{queue_pos}\n\n"
        "⚠️ <b>Назар аударыңыз!</b>\n"
        "Көлікті белсендіру үшін төлем жасау қажет:\n"
        "💰 Төлем: 1000 тг немесе 5%\n\n"
        "Төлем жасау үшін /payment командасын пайдаланыңыз.",
        parse_mode="HTML"
    )
    await callback.answer()
    await state.clear()

# Қолмен уақыт енгізу (жүргізуші)
@dp.callback_query(DriverRegistration.datetime_select, F.data.startswith("dt_custom_"))
async def driver_custom_time(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "⏰ Шығу уақытын қолмен енгізіңіз:\n"
        "(Мысалы: 08:30 немесе 14:15)"
    )
    await callback.answer()
    await state.set_state(DriverRegistration.datetime_select)

# Клиент брондауы
@dp.message(F.text == "🧍‍♂️ Клиент ретінде кіру")
async def client_booking_start(message: types.Message, state: FSMContext):
    await message.answer(
        "🧍‍♂️ <b>Орын брондау</b>\n\n"
        "Бағытты таңдаңыз:",
        reply_markup=direction_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(ClientBooking.direction)

@dp.callback_query(ClientBooking.direction, F.data.startswith("dir_"))
async def client_direction(callback: types.CallbackQuery, state: FSMContext):
    direction = "Шетпе → Ақтау" if callback.data == "dir_shetpe_aktau" else "Ақтау → Шетпе"
    await state.update_data(direction=direction)
    
    await callback.message.edit_text(
        f"✅ Бағыт: {direction}\n\n"
        "📅 Қай күні жүресіз?",
        reply_markup=datetime_keyboard()
    )
    
    await callback.answer()
    await state.set_state(ClientBooking.datetime_select)

# Күн таңдау (клиент)
@dp.callback_query(ClientBooking.datetime_select, F.data.startswith("dt_date_"))
async def client_date_select(callback: types.CallbackQuery, state: FSMContext):
    date_str = callback.data.replace("dt_date_", "")
    await state.update_data(departure_date=date_str)
    
    date_display = format_date_display(date_str)
    data = await state.get_data()
    
    if data['direction'] == "Шетпе → Ақтау":
        await callback.message.edit_text(
            f"✅ Бағыт: {data['direction']}\n"
            f"✅ Күні: {date_display}\n\n"
            "Қай жерден мінесіз?",
            reply_markup=shetpe_locations_keyboard()
        )
    else:
        await callback.message.edit_text(
            f"✅ Бағыт: {data['direction']}\n"
            f"✅ Күні: {date_display}\n\n"
            "Қай жерден мінесіз?",
            reply_markup=aktau_locations_keyboard()
        )
    
    await callback.answer()
    await state.set_state(ClientBooking.pickup_location)

@dp.callback_query(ClientBooking.pickup_location, F.data.startswith("loc_"))
async def client_pickup(callback: types.CallbackQuery, state: FSMContext):
    location_map = {
        "loc_shetpe_center": "Шетпе орталығы",
        "loc_kyzylsay": "Қызылсай",
        "loc_karakiya": "Қарақия",
        "loc_saiyn": "Сайын",
        "loc_15mkr": "15 микрорайон",
        "loc_9mkr": "9 микрорайон",
        "loc_avtovokzal": "Автовокзал",
        "loc_bazar": "Базар",
        "loc_other": "Басқа жер"
    }
    
    pickup = location_map.get(callback.data, "Белгісіз")
    await state.update_data(pickup_location=pickup)
    
    data = await state.get_data()
    
    if data['direction'] == "Шетпе → Ақтау":
        await callback.message.edit_text(
            f"✅ Мінетін жер: {pickup}\n\n"
            "Қай жерде түсесіз?",
            reply_markup=aktau_locations_keyboard()
        )
    else:
        await callback.message.edit_text(
            f"✅ Мінетін жер: {pickup}\n\n"
            "Қай жерде түсесіз?",
            reply_markup=shetpe_locations_keyboard()
        )
    
    await callback.answer()
    await state.set_state(ClientBooking.dropoff_location)

@dp.callback_query(ClientBooking.dropoff_location, F.data.startswith("loc_"))
async def client_dropoff(callback: types.CallbackQuery, state: FSMContext):
    location_map = {
        "loc_shetpe_center": "Шетпе орталығы",
        "loc_kyzylsay": "Қызылсай",
        "loc_karakiya": "Қарақия",
        "loc_saiyn": "Сайын",
        "loc_15mkr": "15 микрорайон",
        "loc_9mkr": "9 микрорайон",
        "loc_avtovokzal": "Автовокзал",
        "loc_bazar": "Базар",
        "loc_other": "Басқа жер"
    }
    
    dropoff = location_map.get(callback.data, "Белгісіз")
    await state.update_data(dropoff_location=dropoff)
    
    data = await state.get_data()
    date_display = format_date_display(data['departure_date'])
    
    # Қолжетімді көліктерді көрсету
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    c.execute('''SELECT d.user_id, d.full_name, d.car_model, d.car_number, 
                        d.total_seats, d.departure_time, d.queue_position,
                        COUNT(b.id) as booked_seats
                 FROM drivers d
                 LEFT JOIN bookings b ON d.user_id = b.driver_id 
                    AND b.status = 'active' AND b.departure_date = d.departure_date
                 WHERE d.direction = ? AND d.departure_date = ? 
                    AND d.is_active = 1 AND d.payment_status = 1
                 GROUP BY d.user_id
                 ORDER BY d.queue_position''', 
              (data['direction'], data['departure_date']))
    drivers = c.fetchall()
    conn.close()
    
    if not drivers:
        await callback.message.edit_text(
            f"❌ {date_display} күні қолжетімді көліктер жоқ.\n\n"
            "Басқа күн таңдап көріңіз немесе кейінірек қайталаңыз.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Басты мәзірге", callback_data="back_main")]
            ])
        )
        await state.clear()
        return
    
    message_text = f"📅 Күні: {date_display}\n"
    message_text += f"✅ Мінетін жер: {data['pickup_location']}\n"
    message_text += f"✅ Түсетін жер: {dropoff}\n\n"
    message_text += "🚗 <b>Бос көліктер:</b>\n\n"
    
    keyboard_buttons = []
    for driver in drivers:
        available_seats = driver[4] - driver[7]
        if available_seats > 0:
            message_text += f"🚖 <b>№{driver[6]} көлік</b>\n"
            message_text += f"   👤 {driver[1]}\n"
            message_text += f"   🚗 {driver[2]} ({driver[3]})\n"
            message_text += f"   💺 Бос орын: {available_seats}/{driver[4]}\n"
            message_text += f"   🕐 Кету: {driver[5]}\n\n"
            
            keyboard_buttons.append([InlineKeyboardButton(
                text=f"Орнымды №{driver[6]} көлікте брондау ({driver[5]})",
                callback_data=f"book_{driver[0]}"
            )])
    
    keyboard_buttons.append([InlineKeyboardButton(text="🔙 Артқа", callback_data="back_datetime")])
    
    await callback.message.edit_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons),
        parse_mode="HTML"
    )
    await callback.answer()
    await state.set_state(ClientBooking.select_car)

@dp.callback_query(ClientBooking.select_car, F.data.startswith("book_"))
async def client_book_car(callback: types.CallbackQuery, state: FSMContext):
    driver_id = int(callback.data.split("_")[1])
    data = await state.get_data()
    
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    
    # Клиентті тексеру/қосу
    c.execute("SELECT id FROM clients WHERE user_id=?", (callback.from_user.id,))
    client = c.fetchone()
    
    if not client:
        c.execute("INSERT INTO clients (user_id, full_name) VALUES (?, ?)",
                  (callback.from_user.id, callback.from_user.full_name))
        client_id = c.lastrowid
    else:
        client_id = client[0]
    
    # Брондау
    c.execute('''INSERT INTO bookings 
                 (client_id, driver_id, direction, departure_date, pickup_location, dropoff_location)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (client_id, driver_id, data['direction'], data['departure_date'],
               data['pickup_location'], data['dropoff_location']))
    
    # Жүргізуші мәліметтері
    c.execute('''SELECT d.full_name, d.car_model, d.car_number, d.departure_time,
                        d.total_seats, d.departure_date, COUNT(b.id) as booked_seats
                 FROM drivers d
                 LEFT JOIN bookings b ON d.user_id = b.driver_id 
                    AND b.status = 'active' AND b.departure_date = d.departure_date
                 WHERE d.user_id = ?
                 GROUP BY d.user_id''', (driver_id,))
    driver = c.fetchone()
    
    conn.commit()
    conn.close()
    
    booked_seats = driver[6] + 1
    total_seats = driver[4]
    date_display = format_date_display(driver[5])
    
    await callback.message.edit_text(
        "✅ <b>Брондау сәтті!</b>\n\n"
        f"🚗 Жүргізуші: {driver[0]}\n"
        f"🚙 Көлік: {driver[1]} ({driver[2]})\n"
        f"📍 Бағыт: {data['direction']}\n"
        f"📅 Күні: {date_display}\n"
        f"🕐 Шығу уақыты: {driver[3]}\n"
        f"📍 Мінетін жер: {data['pickup_location']}\n"
        f"📍 Түсетін жер: {data['dropoff_location']}\n"
        f"💺 Орын: {booked_seats}/{total_seats}\n\n"
        "Жолыңыз болсын! 🚗💨",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Басты мәзірге", callback_data="back_main")]
        ]),
        parse_mode="HTML"
    )
    
    # Көлік толса жүргізушіге хабарлама
    if booked_seats == total_seats:
        await bot.send_message(
            driver_id,
            f"✅ <b>Көлік толды!</b>\n\n"
            f"📅 Күні: {date_display}\n"
            f"💺 {total_seats} адам тіркелді.\n"
            f"🕐 Кету уақыты: {driver[3]}\n\n"
            "Сіз жолға шыға аласыз! 🚗",
            parse_mode="HTML"
        )
    
    await state.clear()

# Артқа қайту callback-тері
@dp.callback_query(F.data == "back_main")
async def back_to_main(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.message.answer(
        "Басты мәзірге оралдыңыз.",
        reply_markup=main_menu_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "back_direction")
async def back_to_direction(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Бағытты таңдаңыз:",
        reply_markup=direction_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "back_datetime")
async def back_to_datetime(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    direction = data.get('direction', '')
    
    await callback.message.edit_text(
        f"✅ Бағыт: {direction}\n\n"
        "📅 Қай күні жүресіз?",
        reply_markup=datetime_keyboard()
    )
    await callback.answer()

# Менің брондарым
@dp.message(F.text == "📊 Менің брондарым")
async def my_bookings(message: types.Message):
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    
    # Клиент брондарын тексеру
    c.execute('''SELECT b.id, d.full_name, d.car_model, d.car_number, 
                        b.direction, b.departure_date, d.departure_time,
                        b.pickup_location, b.dropoff_location, b.status
                 FROM bookings b
                 JOIN drivers d ON b.driver_id = d.user_id
                 JOIN clients c ON b.client_id = c.id
                 WHERE c.user_id = ? AND b.status = 'active'
                 ORDER BY b.departure_date, d.departure_time''', 
              (message.from_user.id,))
    bookings = c.fetchall()
    
    # Жүргізуші ретінде көліктерді тексеру
    c.execute('''SELECT user_id, car_model, car_number, direction, 
                        departure_date, departure_time, total_seats, is_active
                 FROM drivers
                 WHERE user_id = ?
                 ORDER BY departure_date, departure_time''', 
              (message.from_user.id,))
    driver_cars = c.fetchall()
    
    conn.close()
    
    if not bookings and not driver_cars:
        await message.answer(
            "📊 Сізде әзірше брондар жоқ.\n\n"
            "Орын брондау үшін «🧍‍♂️ Клиент ретінде кіру» немесе\n"
            "көлік қосу үшін «🚗 Жүргізуші ретінде кіру» батырмасын басыңыз.",
            reply_markup=main_menu_keyboard()
        )
        return
    
    message_text = "📊 <b>Менің брондарым:</b>\n\n"
    
    # Клиент брондары
    if bookings:
        message_text += "👤 <b>Клиент ретінде:</b>\n\n"
        for booking in bookings:
            date_display = format_date_display(booking[5])
            message_text += f"🎫 Бронд #{booking[0]}\n"
            message_text += f"🚗 Жүргізуші: {booking[1]}\n"
            message_text += f"🚙 Көлік: {booking[2]} ({booking[3]})\n"
            message_text += f"📍 {booking[4]}\n"
            message_text += f"📅 {date_display}\n"
            message_text += f"🕐 Шығу: {booking[6]}\n"
            message_text += f"📍 {booking[7]} → {booking[8]}\n\n"
    
    # Жүргізуші көліктері
    if driver_cars:
        message_text += "🚗 <b>Жүргізуші ретінде:</b>\n\n"
        for car in driver_cars:
            date_display = format_date_display(car[4])
            status = "✅ Белсенді" if car[7] else "⏳ Күтілуде"
            message_text += f"🚖 Көлік: {car[1]} ({car[2]})\n"
            message_text += f"📍 {car[3]}\n"
            message_text += f"📅 {date_display}\n"
            message_text += f"🕐 Шығу: {car[5]}\n"
            message_text += f"💺 Орын: {car[6]}\n"
            message_text += f"📊 Статус: {status}\n\n"
    
    await message.answer(message_text, parse_mode="HTML", reply_markup=main_menu_keyboard())

# Жүргізуші профилі
@dp.message(Command("driver"))
async def driver_profile(message: types.Message):
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    c.execute("SELECT * FROM drivers WHERE user_id=?", (message.from_user.id,))
    driver = c.fetchone()
    conn.close()
    
    if not driver:
        await message.answer(
            "❌ Сіз жүргізуші ретінде тіркелмегенсіз.\n\n"
            "Тіркелу үшін '🚗 Жүргізуші ретінде кіру' батырмасын басыңыз.",
            reply_markup=main_menu_keyboard()
        )
        return
    
    date_display = format_date_display(driver[6])
    status = "✅ Белсенді" if driver[9] else "⏳ Күтілуде"
    payment = "✅ Төленген" if driver[10] else "❌ Төленбеген"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Менің жолаушыларым", callback_data="driver_passengers")],
        [InlineKeyboardButton(text="✏️ Күн/уақыт өзгерту", callback_data="driver_change_datetime")],
        [InlineKeyboardButton(text="🔄 Бағыт өзгерту", callback_data="driver_change_direction")],
        [InlineKeyboardButton(text="🚗 Көлік мәліметтерін өзгерту", callback_data="driver_change_car")],
        [InlineKeyboardButton(text="❌ Тіркеуден шығу", callback_data="driver_unregister")],
        [InlineKeyboardButton(text="🔙 Басты мәзір", callback_data="back_main")]
    ])
    
    await message.answer(
        f"🚗 <b>Жүргізуші профилі</b>\n\n"
        f"👤 Аты-жөні: {driver[1]}\n"
        f"🚙 Көлік: {driver[3]} ({driver[2]})\n"
        f"💺 Орын саны: {driver[4]}\n"
        f"📍 Бағыт: {driver[5]}\n"
        f"📅 Күні: {date_display}\n"
        f"🕐 Шығу уақыты: {driver[7]}\n"
        f"📊 Кезектегі орын: №{driver[8]}\n"
        f"📊 Статус: {status}\n"
        f"💰 Төлем: {payment}\n\n"
        "Басқару опцияларын таңдаңыз:",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

# Жүргізушінің жолаушыларын көрсету
@dp.callback_query(F.data == "driver_passengers")
async def show_driver_passengers(callback: types.CallbackQuery):
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    c.execute('''SELECT c.full_name, b.pickup_location, b.dropoff_location, b.departure_date
                 FROM bookings b
                 JOIN clients c ON b.client_id = c.id
                 WHERE b.driver_id = ? AND b.status = 'active'
                 ORDER BY b.booking_time''', (callback.from_user.id,))
    passengers = c.fetchall()
    conn.close()
    
    if not passengers:
        await callback.message.edit_text(
            "❌ Әзірше жолаушылар жоқ.\n\n"
            "Клиенттер брондағаннан кейін мұнда көрінеді.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Профильге", callback_data="back_to_driver_profile")]
            ])
        )
        await callback.answer()
        return
    
    date_display = format_date_display(passengers[0][3])
    msg = f"👥 <b>Менің жолаушыларым</b>\n\n"
    msg += f"📅 Күні: {date_display}\n\n"
    
    for i, passenger in enumerate(passengers, 1):
        msg += f"{i}. {passenger[0]}\n"
        msg += f"   📍 {passenger[1]} → {passenger[2]}\n\n"
    
    msg += f"<b>Жалпы:</b> {len(passengers)} жолаушы"
    
    await callback.message.edit_text(
        msg,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Профильге", callback_data="back_to_driver_profile")]
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

# Күн/уақыт өзгерту
class DriverEdit(StatesGroup):
    change_datetime = State()
    change_direction = State()
    change_car_model = State()
    change_car_number = State()
    change_seats = State()

@dp.callback_query(F.data == "driver_change_datetime")
async def driver_change_datetime_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📅 Жаңа күнді таңдаңыз:",
        reply_markup=datetime_keyboard(show_back_to_direction=False)
    )
    await state.set_state(DriverEdit.change_datetime)
    await callback.answer()

@dp.callback_query(DriverEdit.change_datetime, F.data.startswith("dt_date_"))
async def driver_new_date(callback: types.CallbackQuery, state: FSMContext):
    date_str = callback.data.replace("dt_date_", "")
    await state.update_data(departure_date=date_str)
    date_display = format_date_display(date_str)
    
    await callback.message.edit_text(
        f"✅ Күні: {date_display}\n\n"
        "🕐 Жаңа уақытты таңдаңыз:",
        reply_markup=time_keyboard(date_str)
    )
    await callback.answer()

@dp.callback_query(DriverEdit.change_datetime, F.data.startswith("dt_time_"))
async def driver_new_time(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    time_str = parts[-1]
    data = await state.get_data()
    
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    
    # Жаңа кезек позициясын анықтау
    c.execute("""SELECT MAX(queue_position) FROM drivers 
                 WHERE direction = (SELECT direction FROM drivers WHERE user_id = ?)
                 AND departure_date = ?""", 
              (callback.from_user.id, data['departure_date']))
    max_pos = c.fetchone()[0]
    new_queue_pos = (max_pos or 0) + 1
    
    # Өзгерту
    c.execute('''UPDATE drivers 
                 SET departure_date = ?, departure_time = ?, queue_position = ?
                 WHERE user_id = ?''',
              (data['departure_date'], time_str, new_queue_pos, callback.from_user.id))
    conn.commit()
    conn.close()
    
    date_display = format_date_display(data['departure_date'])
    
    await callback.message.edit_text(
        f"✅ <b>Күн мен уақыт өзгертілді!</b>\n\n"
        f"📅 Жаңа күн: {date_display}\n"
        f"🕐 Жаңа уақыт: {time_str}\n"
        f"📊 Жаңа кезек: №{new_queue_pos}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Профильге", callback_data="back_to_driver_profile")]
        ]),
        parse_mode="HTML"
    )
    await state.clear()
    await callback.answer()

# Бағыт өзгерту
@dp.callback_query(F.data == "driver_change_direction")
async def driver_change_direction_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "🔄 Жаңа бағытты таңдаңыз:",
        reply_markup=direction_keyboard()
    )
    await state.set_state(DriverEdit.change_direction)
    await callback.answer()

@dp.callback_query(DriverEdit.change_direction, F.data.startswith("dir_"))
async def driver_new_direction(callback: types.CallbackQuery, state: FSMContext):
    direction = "Шетпе → Ақтау" if callback.data == "dir_shetpe_aktau" else "Ақтау → Шетпе"
    
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    
    # Жаңа бағыт бойынша кезек
    c.execute("SELECT departure_date FROM drivers WHERE user_id = ?", (callback.from_user.id,))
    current_date = c.fetchone()[0]
    
    c.execute("""SELECT MAX(queue_position) FROM drivers 
                 WHERE direction = ? AND departure_date = ?""", (direction, current_date))
    max_pos = c.fetchone()[0]
    new_queue_pos = (max_pos or 0) + 1
    
    c.execute('''UPDATE drivers 
                 SET direction = ?, queue_position = ?
                 WHERE user_id = ?''',
              (direction, new_queue_pos, callback.from_user.id))
    conn.commit()
    conn.close()
    
    await callback.message.edit_text(
        f"✅ <b>Бағыт өзгертілді!</b>\n\n"
        f"📍 Жаңа бағыт: {direction}\n"
        f"📊 Жаңа кезек: №{new_queue_pos}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Профильге", callback_data="back_to_driver_profile")]
        ]),
        parse_mode="HTML"
    )
    await state.clear()
    await callback.answer()

# Көлік мәліметтерін өзгерту
@dp.callback_query(F.data == "driver_change_car")
async def driver_change_car_start(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚗 Көлік маркасы", callback_data="change_car_model")],
        [InlineKeyboardButton(text="🔢 Көлік нөмірі", callback_data="change_car_number")],
        [InlineKeyboardButton(text="💺 Орын саны", callback_data="change_seats")],
        [InlineKeyboardButton(text="🔙 Профильге", callback_data="back_to_driver_profile")]
    ])
    
    await callback.message.edit_text(
        "🚗 <b>Көлік мәліметтерін өзгерту</b>\n\n"
        "Не өзгерткіңіз келеді?",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "change_car_model")
async def change_car_model_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "🚗 Жаңа көлік маркасын енгізіңіз:\n"
        "(Мысалы: Toyota Camry)"
    )
    await state.set_state(DriverEdit.change_car_model)
    await callback.answer()

@dp.message(DriverEdit.change_car_model)
async def change_car_model_save(message: types.Message, state: FSMContext):
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    c.execute("UPDATE drivers SET car_model = ? WHERE user_id = ?", 
              (message.text, message.from_user.id))
    conn.commit()
    conn.close()
    
    await message.answer(
        f"✅ Көлік маркасы өзгертілді: {message.text}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Профильге", callback_data="back_to_driver_profile")]
        ])
    )
    await state.clear()

@dp.callback_query(F.data == "change_car_number")
async def change_car_number_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "🔢 Жаңа көлік нөмірін енгізіңіз:\n"
        "(Мысалы: 870 ABC 09)"
    )
    await state.set_state(DriverEdit.change_car_number)
    await callback.answer()

@dp.message(DriverEdit.change_car_number)
async def change_car_number_save(message: types.Message, state: FSMContext):
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    c.execute("UPDATE drivers SET car_number = ? WHERE user_id = ?", 
              (message.text, message.from_user.id))
    conn.commit()
    conn.close()
    
    await message.answer(
        f"✅ Көлік нөмірі өзгертілді: {message.text}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Профильге", callback_data="back_to_driver_profile")]
        ])
    )
    await state.clear()

@dp.callback_query(F.data == "change_seats")
async def change_seats_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "💺 Жаңа орын санын енгізіңіз:\n"
        "(1-ден 8-ге дейін)"
    )
    await state.set_state(DriverEdit.change_seats)
    await callback.answer()

@dp.message(DriverEdit.change_seats)
async def change_seats_save(message: types.Message, state: FSMContext):
    try:
        seats = int(message.text)
        if seats < 1 or seats > 8:
            await message.answer("❌ Орын саны 1-ден 8-ге дейін болуы керек!")
            return
        
        conn = sqlite3.connect('taxi_bot.db')
        c = conn.cursor()
        c.execute("UPDATE drivers SET total_seats = ? WHERE user_id = ?", 
                  (seats, message.from_user.id))
        conn.commit()
        conn.close()
        
        await message.answer(
            f"✅ Орын саны өзгертілді: {seats}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Профильге", callback_data="back_to_driver_profile")]
            ])
        )
        await state.clear()
    except ValueError:
        await message.answer("❌ Тек сан енгізіңіз!")

# Тіркеуден шығу
@dp.callback_query(F.data == "driver_unregister")
async def driver_unregister_confirm(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Иә, шығамын", callback_data="driver_unregister_yes")],
        [InlineKeyboardButton(text="❌ Жоқ, қалу", callback_data="back_to_driver_profile")]
    ])
    
    await callback.message.edit_text(
        "⚠️ <b>Тіркеуден шығу</b>\n\n"
        "Сіз шынымен жүргізуші тіркеуінен шығғыңыз келе ме?\n\n"
        "❗️ Барлық брондар жойылады!",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "driver_unregister_yes")
async def driver_unregister_execute(callback: types.CallbackQuery):
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    
    # Брондарды жою
    c.execute("DELETE FROM bookings WHERE driver_id = ?", (callback.from_user.id,))
    
    # Жүргізушіні жою
    c.execute("DELETE FROM drivers WHERE user_id = ?", (callback.from_user.id,))
    
    conn.commit()
    conn.close()
    
    await callback.message.delete()
    await callback.message.answer(
        "✅ Сіз жүргізуші тіркеуінен шықтыңыз.\n\n"
        "Қайта тіркелу үшін '🚗 Жүргізуші ретінде кіру' батырмасын басыңыз.",
        reply_markup=main_menu_keyboard()
    )
    await callback.answer()

# Профильге қайту
@dp.callback_query(F.data == "back_to_driver_profile")
async def back_to_driver_profile(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    # driver_profile функциясының мазмұнын қайталау
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    c.execute("SELECT * FROM drivers WHERE user_id=?", (callback.from_user.id,))
    driver = c.fetchone()
    conn.close()
    
    if not driver:
        await callback.message.edit_text("❌ Қате орын алды.")
        return
    
    date_display = format_date_display(driver[6])
    status = "✅ Белсенді" if driver[9] else "⏳ Күтілуде"
    payment = "✅ Төленген" if driver[10] else "❌ Төленбеген"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Менің жолаушыларым", callback_data="driver_passengers")],
        [InlineKeyboardButton(text="✏️ Күн/уақыт өзгерту", callback_data="driver_change_datetime")],
        [InlineKeyboardButton(text="🔄 Бағыт өзгерту", callback_data="driver_change_direction")],
        [InlineKeyboardButton(text="🚗 Көлік мәліметтерін өзгерту", callback_data="driver_change_car")],
        [InlineKeyboardButton(text="❌ Тіркеуден шығу", callback_data="driver_unregister")],
        [InlineKeyboardButton(text="🔙 Басты мәзір", callback_data="back_main")]
    ])
    
    await callback.message.edit_text(
        f"🚗 <b>Жүргізуші профилі</b>\n\n"
        f"👤 Аты-жөні: {driver[1]}\n"
        f"🚙 Көлік: {driver[3]} ({driver[2]})\n"
        f"💺 Орын саны: {driver[4]}\n"
        f"📍 Бағыт: {driver[5]}\n"
        f"📅 Күні: {date_display}\n"
        f"🕐 Шығу уақыты: {driver[7]}\n"
        f"📊 Кезектегі орын: №{driver[8]}\n"
        f"📊 Статус: {status}\n"
        f"💰 Төлем: {payment}\n\n"
        "Басқару опцияларын таңдаңыз:",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await callback.answer()

# ==================== АДМИН ПАНЕЛІ ====================

# Админ мәзірі
def admin_keyboard():
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👥 Жүргізушілер тізімі", callback_data="admin_drivers")],
            [InlineKeyboardButton(text="🧍‍♂️ Клиенттер тізімі", callback_data="admin_clients")],
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton(text="💰 Төлемдер", callback_data="admin_payments")],
            [InlineKeyboardButton(text="👑 Админдер", callback_data="admin_list")],
            [InlineKeyboardButton(text="🔙 Басты мәзір", callback_data="back_main")]
        ]
    )
    return keyboard

# /admin командасы
@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Сізде админ құқығы жоқ.")
        return
    
    await message.answer(
        "🔐 <b>Админ панелі</b>\n\n"
        "Басқару опцияларын таңдаңыз:",
        reply_markup=admin_keyboard(),
        parse_mode="HTML"
    )

# Админдер тізімі
@dp.callback_query(F.data == "admin_list")
async def admin_list_view(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Сізде құқық жоқ", show_alert=True)
        return
    
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute("SELECT user_id, added_at FROM admins ORDER BY added_at")
    admins = c.fetchall()
    conn.close()
    
    msg = "👑 <b>Админдер тізімі:</b>\n\n"
    
    for i, admin in enumerate(admins, 1):
        msg += f"{i}. User ID: <code>{admin[0]}</code>\n"
        if admin[1]:
            date = datetime.fromisoformat(admin[1]).strftime('%d.%m.%Y')
            msg += f"   📅 Қосылған: {date}\n"
        msg += "\n"
    
    msg += f"<b>Жалпы:</b> {len(admins)} админ\n\n"
    msg += "💡 Жаңа админ қосу үшін:\n"
    msg += "<code>/addadmin USER_ID</code>"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Артқа", callback_data="admin_back")]
    ])
    
    await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()

# Админ қосу командасы
@dp.message(Command("addadmin"))
async def add_admin_command(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Сізде админ құқығы жоқ.")
        return
    
    # Командадан User ID алу
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer(
            "❌ <b>Қате формат!</b>\n\n"
            "Дұрыс пайдалану:\n"
            "<code>/addadmin USER_ID</code>\n\n"
            "Мысалы:\n"
            "<code>/addadmin 123456789</code>\n\n"
            "💡 User ID табу үшін:\n"
            "1. @userinfobot ботына өтіңіз\n"
            "2. /start жіберіңіз",
            parse_mode="HTML"
        )
        return
    
    try:
        new_admin_id = int(parts[1])
    except ValueError:
        await message.answer("❌ User ID тек сандардан тұруы керек!")
        return
    
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    try:
        c.execute("INSERT INTO admins (user_id) VALUES (?)", (new_admin_id,))
        conn.commit()
        
        await message.answer(
            f"✅ <b>Жаңа админ қосылды!</b>\n\n"
            f"👤 User ID: <code>{new_admin_id}</code>\n\n"
            f"Енді бұл қолданушы /admin командасын пайдалана алады.",
            parse_mode="HTML"
        )
        
        # Жаңа админге хабарлама жіберу
        try:
            await bot.send_message(
                new_admin_id,
                "🎉 <b>Сізге админ құқығы берілді!</b>\n\n"
                "Енді сіз админ панелін пайдалана аласыз:\n"
                "/admin - Админ панелін ашу\n\n"
                "Жауапкершілікпен пайдаланыңыз! 🔐",
                parse_mode="HTML"
            )
        except:
            await message.answer(
                "⚠️ Жаңа админге хабарлама жібере алмадым.\n"
                "Ол әлі ботты бастамаған шығар (/start)."
            )
            
    except sqlite3.IntegrityError:
        await message.answer(
            f"❌ User ID <code>{new_admin_id}</code> қазірдің өзінде админ!",
            parse_mode="HTML"
        )
    finally:
        conn.close()

# Админді жою командасы
@dp.message(Command("removeadmin"))
async def remove_admin_command(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Сізде админ құқығы жоқ.")
        return
    
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer(
            "❌ <b>Қате формат!</b>\n\n"
            "Дұрыс пайдалану:\n"
            "<code>/removeadmin USER_ID</code>\n\n"
            "Мысалы:\n"
            "<code>/removeadmin 123456789</code>",
            parse_mode="HTML"
        )
        return
    
    try:
        admin_to_remove = int(parts[1])
    except ValueError:
        await message.answer("❌ User ID тек сандардан тұруы керек!")
        return
    
    # Өзін жоюға тыйым салу
    if admin_to_remove == message.from_user.id:
        await message.answer("❌ Өзіңізді админдер тізімінен жоя алмайсыз!")
        return
    
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    # Тексеру
    c.execute("SELECT COUNT(*) FROM admins WHERE user_id=?", (admin_to_remove,))
    exists = c.fetchone()[0]
    
    if not exists:
        await message.answer(
            f"❌ User ID <code>{admin_to_remove}</code> админ емес!",
            parse_mode="HTML"
        )
        conn.close()
        return
    
    # Жою
    c.execute("DELETE FROM admins WHERE user_id=?", (admin_to_remove,))
    conn.commit()
    conn.close()
    
    await message.answer(
        f"✅ <b>Админ жойылды!</b>\n\n"
        f"👤 User ID: <code>{admin_to_remove}</code>\n\n"
        f"Бұл қолданушы енді админ панелін пайдалана алмайды.",
        parse_mode="HTML"
    )
    
    # Жойылған админге хабарлама
    try:
        await bot.send_message(
            admin_to_remove,
            "⚠️ <b>Админ құқығыңыз алынып тасталды!</b>\n\n"
            "Енді сіз админ панелін пайдалана алмайсыз.",
            parse_mode="HTML"
        )
    except:
        pass

# ==================== СОҢЫ АДМИН ПАНЕЛІ ====================

# Жүргізушілер тізімі
@dp.callback_query(F.data == "admin_drivers")
async def admin_drivers_list(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Сізде құқық жоқ", show_alert=True)
        return
    
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute('''SELECT user_id, full_name, car_model, car_number, 
                        direction, departure_date, departure_time, queue_position, 
                        is_active, payment_status
                 FROM drivers
                 ORDER BY direction, departure_date, queue_position''')
    drivers = c.fetchall()
    conn.close()
    
    if not drivers:
        await callback.message.edit_text(
            "❌ Тіркелген жүргізушілер жоқ.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Артқа", callback_data="admin_back")]
            ])
        )
        return
    
    msg = "👥 <b>Жүргізушілер тізімі:</b>\n\n"
    
    current_direction = None
    for driver in drivers:
        if driver[4] != current_direction:
            current_direction = driver[4]
            msg += f"\n📍 <b>{current_direction}</b>\n\n"
        
        status = "✅ Белсенді" if driver[8] else "❌ Белсенді емес"
        payment = "✅ Төленген" if driver[9] else "❌ Төленбеген"
        date_display = format_date_display(driver[5])
        
        msg += f"<b>№{driver[7]}</b> - {driver[1]}\n"
        msg += f"   🚗 {driver[2]} ({driver[3]})\n"
        msg += f"   📅 {date_display} | 🕐 {driver[6]}\n"
        msg += f"   📊 Статус: {status}\n"
        msg += f"   💰 Төлем: {payment}\n"
        msg += f"   🆔 ID: <code>{driver[0]}</code>\n\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Артқа", callback_data="admin_back")]
    ])
    
    await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()

# Клиенттер тізімі
@dp.callback_query(F.data == "admin_clients")
async def admin_clients_list(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Сізде құқық жоқ", show_alert=True)
        return
    
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute('''SELECT c.user_id, c.full_name, COUNT(b.id) as total_bookings
                 FROM clients c
                 LEFT JOIN bookings b ON c.id = b.client_id
                 GROUP BY c.id
                 ORDER BY total_bookings DESC''')
    clients = c.fetchall()
    conn.close()
    
    if not clients:
        await callback.message.edit_text(
            "❌ Тіркелген клиенттер жоқ.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Артқа", callback_data="admin_back")]
            ])
        )
        return
    
    msg = "🧍‍♂️ <b>Клиенттер тізімі:</b>\n\n"
    
    for i, client in enumerate(clients, 1):
        msg += f"{i}. {client[1]}\n"
        msg += f"   📊 Брондар: {client[2]}\n"
        msg += f"   🆔 ID: <code>{client[0]}</code>\n\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Артқа", callback_data="admin_back")]
    ])
    
    await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()

# Статистика
@dp.callback_query(F.data == "admin_stats")
async def admin_statistics(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Сізде құқық жоқ", show_alert=True)
        return
    
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    # Жалпы статистика
    c.execute("SELECT COUNT(*) FROM drivers")
    total_drivers = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM drivers WHERE is_active=1")
    active_drivers = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM clients")
    total_clients = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM bookings WHERE status='active'")
    active_bookings = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM drivers WHERE payment_status=1")
    paid_drivers = c.fetchone()[0]
    
    # Бағыттар бойынша
    c.execute('''SELECT direction, COUNT(*) 
                 FROM drivers 
                 WHERE is_active=1 
                 GROUP BY direction''')
    directions = c.fetchall()
    
    # Бүгінгі брондар
    c.execute('''SELECT COUNT(*) 
                 FROM bookings 
                 WHERE DATE(booking_time) = DATE('now')''')
    today_bookings = c.fetchone()[0]
    
    conn.close()
    
    msg = "📊 <b>Жүйе статистикасы</b>\n\n"
    msg += f"👥 <b>Жүргізушілер:</b>\n"
    msg += f"   • Жалпы: {total_drivers}\n"
    msg += f"   • Белсенді: {active_drivers}\n"
    msg += f"   • Төлем жасаған: {paid_drivers}\n\n"
    
    msg += f"🧍‍♂️ <b>Клиенттер:</b> {total_clients}\n\n"
    
    msg += f"📋 <b>Брондар:</b>\n"
    msg += f"   • Активті: {active_bookings}\n"
    msg += f"   • Бүгін: {today_bookings}\n\n"
    
    if directions:
        msg += "📍 <b>Бағыттар бойынша:</b>\n"
        for direction in directions:
            msg += f"   • {direction[0]}: {direction[1]} көлік\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Жаңарту", callback_data="admin_stats")],
        [InlineKeyboardButton(text="🔙 Артқа", callback_data="admin_back")]
    ])
    
    await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()

# Төлемдер
@dp.callback_query(F.data == "admin_payments")
async def admin_payments(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Сізде құқық жоқ", show_alert=True)
        return
    
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute('''SELECT user_id, full_name, car_number, departure_date, departure_time
                 FROM drivers
                 WHERE payment_status = 0
                 ORDER BY created_at DESC''')
    pending_payments = c.fetchall()
    conn.close()
    
    if not pending_payments:
        await callback.message.edit_text(
            "✅ Барлық төлемдер расталған!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Артқа", callback_data="admin_back")]
            ])
        )
        return
    
    msg = "💰 <b>Төленбеген жүргізушілер:</b>\n\n"
    
    keyboard_buttons = []
    for driver in pending_payments:
        date_display = format_date_display(driver[3])
        msg += f"👤 {driver[1]}\n"
        msg += f"   🚗 {driver[2]}\n"
        msg += f"   📅 {date_display} | 🕐 {driver[4]}\n"
        msg += f"   🆔 ID: <code>{driver[0]}</code>\n\n"
        
        keyboard_buttons.append([
            InlineKeyboardButton(
                text=f"✅ Растау: {driver[1]}",
                callback_data=f"payment_approve_{driver[0]}"
            )
        ])
    
    keyboard_buttons.append([InlineKeyboardButton(text="🔙 Артқа", callback_data="admin_back")])
    
    await callback.message.edit_text(
        msg,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons),
        parse_mode="HTML"
    )
    await callback.answer()

# Төлемді растау
@dp.callback_query(F.data.startswith("payment_approve_"))
async def approve_payment(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Сізде құқық жоқ", show_alert=True)
        return
    
    driver_id = int(callback.data.split("_")[2])
    
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute("UPDATE drivers SET payment_status=1, is_active=1 WHERE user_id=?", (driver_id,))
    conn.commit()
    
    c.execute("SELECT full_name FROM drivers WHERE user_id=?", (driver_id,))
    driver_name = c.fetchone()[0]
    conn.close()
    
    # Жүргізушіге хабарлама
    try:
        await bot.send_message(
            driver_id,
            "✅ <b>Төлеміңіз расталды!</b>\n\n"
            "Көлігіңіз енді белсенді.\n"
            "Клиенттер сізге брондай алады.",
            parse_mode="HTML"
        )
    except:
        pass
    
    await callback.answer(f"✅ {driver_name} төлемі расталды!", show_alert=True)
    
    # Төлемдер тізіміне қайту
    await admin_payments(callback)

# Админ панеліне қайту
@dp.callback_query(F.data == "admin_back")
async def admin_back(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Сізде құқық жоқ", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🔐 <b>Админ панелі</b>\n\n"
        "Басқару опцияларын таңдаңыз:",
        reply_markup=admin_keyboard(),
        parse_mode="HTML"
    )
    await callback.answer()

# ==================== СОҢЫ АДМИН ПАНЕЛІ ====================

# Анықтама
@dp.message(F.text == "ℹ️ Анықтама")
async def info_command(message: types.Message):
    await message.answer(
        "ℹ️ <b>Бот туралы ақпарат:</b>\n\n"
        "🚖 Бұл бот Шетпе-Ақтау арасындағы такси қызметін ұйымдастыруға арналған.\n\n"
        "<b>Жүргізушілер үшін:</b>\n"
        "• Көлікті жүйеге тіркеу\n"
        "• Күн мен уақытты таңдау\n"
        "• Кезекті бақылау\n"
        "• Орын толғанда хабарлама алу\n"
        "• /driver - Профильді басқару\n\n"
        "<b>Клиенттер үшін:</b>\n"
        "• 7 күнге дейін алдын ала брондау\n"
        "• Қолайлы уақытты таңдау\n"
        "• Мінетін/түсетін жерді белгілеу\n"
        "• Брондарды бақылау\n\n"
        "<b>Қолдау қызметі:</b>\n"
        f"📞 Байланыс: {SUPPORT_USERNAME}\n\n"
        "Сұрақтарыңыз болса, қолдау қызметіне жазыңыз! 👋",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard()
    )

# Басты функция
async def main():
    init_db()
    print("🚀 Бот іске қосылды...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())