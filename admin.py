import sqlite3
from aiogram import types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# Админдерді тексеру
def is_admin(user_id: int) -> bool:
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    c.execute("SELECT user_id FROM admins WHERE user_id=?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result is not None

# Админ мәзірі
def admin_keyboard():
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👥 Жүргізушілер тізімі", callback_data="admin_drivers")],
            [InlineKeyboardButton(text="🧍‍♂️ Клиенттер тізімі", callback_data="admin_clients")],
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton(text="💰 Төлемдер", callback_data="admin_payments")],
            [InlineKeyboardButton(text="🔧 Кезек басқару", callback_data="admin_queue")],
            [InlineKeyboardButton(text="🔙 Басты мәзір", callback_data="back_main")]
        ]
    )
    return keyboard

# Админ панелін ашу
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

# Жүргізушілер тізімі
async def admin_drivers_list(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Сізде құқық жоқ", show_alert=True)
        return
    
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    c.execute('''SELECT user_id, full_name, car_model, car_number, 
                        direction, queue_position, is_active, payment_status
                 FROM drivers
                 ORDER BY direction, queue_position''')
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
        
        status = "✅ Белсенді" if driver[6] else "❌ Белсенді емес"
        payment = "✅ Төленген" if driver[7] else "❌ Төленбеген"
        
        msg += f"<b>№{driver[5]}</b> - {driver[1]}\n"
        msg += f"   🚗 {driver[2]} ({driver[3]})\n"
        msg += f"   📊 Статус: {status}\n"
        msg += f"   💰 Төлем: {payment}\n"
        msg += f"   🆔 ID: <code>{driver[0]}</code>\n\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Артқа", callback_data="admin_back")]
    ])
    
    await callback.message.edit_text(msg, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()

# Клиенттер тізімі
async def admin_clients_list(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Сізде құқық жоқ", show_alert=True)
        return
    
    conn = sqlite3.connect('taxi_bot.db')
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
async def admin_statistics(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Сізде құқық жоқ", show_alert=True)
        return
    
    conn = sqlite3.connect('taxi_bot.db')
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
async def admin_payments(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Сізде құқық жоқ", show_alert=True)
        return
    
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    c.execute('''SELECT user_id, full_name, car_number, payment_status
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
        msg += f"👤 {driver[1]}\n"
        msg += f"   🚗 {driver[2]}\n"
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
async def approve_payment(callback: types.CallbackQuery, bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Сізде құқық жоқ", show_alert=True)
        return
    
    driver_id = int(callback.data.split("_")[2])
    
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    c.execute("UPDATE drivers SET payment_status=1, is_active=1 WHERE user_id=?", (driver_id,))
    conn.commit()
    
    c.execute("SELECT full_name FROM drivers WHERE user_id=?", (driver_id,))
    driver_name = c.fetchone()[0]
    conn.close()
    
    # Жүргізушіге хабарлама
    await bot.send_message(
        driver_id,
        "✅ <b>Төлеміңіз расталды!</b>\n\n"
        "Көлігіңіз енді белсенді.\n"
        "Клиенттер сізге брондай алады.",
        parse_mode="HTML"
    )
    
    await callback.answer(f"✅ {driver_name} төлемі расталды!", show_alert=True)
    await admin_payments(callback)

# Кезек басқару
async def admin_queue_management(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Сізде құқық жоқ", show_alert=True)
        return
    
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    c.execute('''SELECT user_id, full_name, car_number, direction, queue_position
                 FROM drivers
                 WHERE is_active=1
                 ORDER BY direction, queue_position''')
    drivers = c.fetchall()
    conn.close()
    
    if not drivers:
        await callback.message.edit_text(
            "❌ Белсенді жүргізушілер жоқ.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Артқа", callback_data="admin_back")]
            ])
        )
        return
    
    msg = "🔧 <b>Кезек басқару</b>\n\n"
    
    current_direction = None
    keyboard_buttons = []
    
    for driver in drivers:
        if driver[3] != current_direction:
            current_direction = driver[3]
            msg += f"\n📍 <b>{current_direction}</b>\n\n"
        
        msg += f"№{driver[4]} - {driver[1]} ({driver[2]})\n"
        
        keyboard_buttons.append([
            InlineKeyboardButton(text=f"⬆️ №{driver[4]}", callback_data=f"queue_up_{driver[0]}"),
            InlineKeyboardButton(text=f"⬇️ №{driver[4]}", callback_data=f"queue_down_{driver[0]}")
        ])
    
    keyboard_buttons.append([InlineKeyboardButton(text="🔙 Артқа", callback_data="admin_back")])
    
    await callback.message.edit_text(
        msg,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons),
        parse_mode="HTML"
    )
    await callback.answer()

# Кезекті жоғары жылжыту
async def queue_move_up(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Сізде құқық жоқ", show_alert=True)
        return
    
    driver_id = int(callback.data.split("_")[2])
    
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    
    # Ағымдағы позиция
    c.execute("SELECT queue_position, direction FROM drivers WHERE user_id=?", (driver_id,))
    current = c.fetchone()
    
    if current[0] <= 1:
        await callback.answer("❌ Бұл бірінші орында!", show_alert=True)
        conn.close()
        return
    
    # Алдыңғы көлікпен орын ауыстыру
    c.execute('''UPDATE drivers SET queue_position = queue_position + 1 
                 WHERE direction=? AND queue_position=?''', 
              (current[1], current[0] - 1))
    
    c.execute("UPDATE drivers SET queue_position=? WHERE user_id=?", 
              (current[0] - 1, driver_id))
    
    conn.commit()
    conn.close()
    
    await callback.answer("✅ Кезек жоғары жылжыды!")
    await admin_queue_management(callback)

# Кезекті төмен жылжыту
async def queue_move_down(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Сізде құқық жоқ", show_alert=True)
        return
    
    driver_id = int(callback.data.split("_")[2])
    
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    
    # Ағымдағы позиция
    c.execute("SELECT queue_position, direction FROM drivers WHERE user_id=?", (driver_id,))
    current = c.fetchone()
    
    # Соңғы позицияны тексеру
    c.execute("SELECT MAX(queue_position) FROM drivers WHERE direction=?", (current[1],))
    max_pos = c.fetchone()[0]
    
    if current[0] >= max_pos:
        await callback.answer("❌ Бұл соңғы орында!", show_alert=True)
        conn.close()
        return
    
    # Келесі көлікпен орын ауыстыру
    c.execute('''UPDATE drivers SET queue_position = queue_position - 1 
                 WHERE direction=? AND queue_position=?''', 
              (current[1], current[0] + 1))
    
    c.execute("UPDATE drivers SET queue_position=? WHERE user_id=?", 
              (current[0] + 1, driver_id))
    
    conn.commit()
    conn.close()
    
    await callback.answer("✅ Кезек төмен жылжыды!")
    await admin_queue_management(callback)

# Админ панеліне қайту
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

# Админ қосу функциясы (бұл функцияны тек қолмен шақыру керек)
def add_admin(user_id: int):
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    try:
        c.execute("INSERT INTO admins (user_id) VALUES (?)", (user_id,))
        conn.commit()
        print(f"✅ Админ қосылды: {user_id}")
    except sqlite3.IntegrityError:
        print(f"❌ Бұл қолданушы қазірдің өзінде админ: {user_id}")
    finally:
        conn.close()