#!/usr/bin/env python3
"""
Шетпе-Ақтау Такси Бот - Бастапқы орнату скрипті
"""

import sqlite3
import sys
import os

def init_database():
    """Дерекқорды инициализациялау"""
    print("📊 Дерекқор құрылуда...")
    
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    
    # Жүргізушілер кестесі
    c.execute('''CREATE TABLE IF NOT EXISTS drivers
                 (user_id INTEGER PRIMARY KEY,
                  full_name TEXT NOT NULL,
                  car_number TEXT NOT NULL,
                  car_model TEXT NOT NULL,
                  total_seats INTEGER NOT NULL,
                  direction TEXT NOT NULL,
                  departure_time TEXT NOT NULL,
                  queue_position INTEGER NOT NULL,
                  is_active INTEGER DEFAULT 0,
                  payment_status INTEGER DEFAULT 0,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    # Клиенттер кестесі
    c.execute('''CREATE TABLE IF NOT EXISTS clients
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER UNIQUE NOT NULL,
                  full_name TEXT,
                  phone TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    # Брондар кестесі
    c.execute('''CREATE TABLE IF NOT EXISTS bookings
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  client_id INTEGER NOT NULL,
                  driver_id INTEGER NOT NULL,
                  direction TEXT NOT NULL,
                  pickup_location TEXT NOT NULL,
                  dropoff_location TEXT NOT NULL,
                  booking_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  status TEXT DEFAULT 'active',
                  FOREIGN KEY (client_id) REFERENCES clients(id),
                  FOREIGN KEY (driver_id) REFERENCES drivers(user_id))''')
    
    # Админдер кестесі
    c.execute('''CREATE TABLE IF NOT EXISTS admins
                 (user_id INTEGER PRIMARY KEY,
                  added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    # Төлемдер тарихы кестесі
    c.execute('''CREATE TABLE IF NOT EXISTS payment_history
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  driver_id INTEGER NOT NULL,
                  amount INTEGER NOT NULL,
                  payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  approved_by INTEGER,
                  FOREIGN KEY (driver_id) REFERENCES drivers(user_id),
                  FOREIGN KEY (approved_by) REFERENCES admins(user_id))''')
    
    # Хабарламалар журналы
    c.execute('''CREATE TABLE IF NOT EXISTS notification_log
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER NOT NULL,
                  message TEXT NOT NULL,
                  sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    conn.commit()
    conn.close()
    
    print("✅ Дерекқор сәтті құрылды!")

def add_admin():
    """Бірінші админді қосу"""
    print("\n👤 Админ қосу")
    print("─" * 40)
    
    user_id = input("Telegram User ID енгізіңіз: ").strip()
    
    if not user_id.isdigit():
        print("❌ Қате! User ID тек сандардан тұруы керек.")
        return
    
    user_id = int(user_id)
    
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    
    try:
        c.execute("INSERT INTO admins (user_id) VALUES (?)", (user_id,))
        conn.commit()
        print(f"✅ Админ сәтті қосылды: {user_id}")
        print("\n💡 Кеңес: Өзіңіздің User ID-ді білу үшін:")
        print("   1. @userinfobot ботына өтіңіз")
        print("   2. /start командасын жіберіңіз")
        print("   3. Бот сізге User ID-ңізді жібереді")
    except sqlite3.IntegrityError:
        print(f"❌ Бұл қолданушы қазірдің өзінде админ: {user_id}")
    finally:
        conn.close()

def check_requirements():
    """Қажетті пакеттерді тексеру"""
    print("📦 Қажетті пакеттер тексерілуде...")
    
    required = ['aiogram', 'aiohttp']
    missing = []
    
    for package in required:
        try:
            __import__(package)
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  ❌ {package}")
            missing.append(package)
    
    if missing:
        print(f"\n⚠️  Қажетті пакеттер табылмады: {', '.join(missing)}")
        print("Орнату үшін команда:")
        print(f"  pip install {' '.join(missing)}")
        return False
    
    print("✅ Барлық пакеттер орнатылған!")
    return True

def create_env_file():
    """".env файлын құру"""
    print("\n⚙️  .env файлы құрылуда...")
    
    if os.path.exists('.env'):
        overwrite = input(".env файлы бар. Үстінен жазу керек пе? (y/n): ").lower()
        if overwrite != 'y':
            print("❌ .env файлы өзгертілмеді.")
            return
    
    bot_token = input("Bot Token енгізіңіз (BotFather-ден): ").strip()
    admin_id = input("Админ User ID: ").strip()
    kaspi_phone = input("Kaspi телефон нөмірі (+7XXXXXXXXXX): ").strip()
    support_username = input("Қолдау username (@username): ").strip()
    
    env_content = f"""# Telegram Bot Configuration
BOT_TOKEN={bot_token}

# Admin Configuration
ADMIN_USER_ID={admin_id}

# Payment Configuration
KASPI_PHONE={kaspi_phone}

# Support Configuration
SUPPORT_USERNAME={support_username}
"""
    
    with open('.env', 'w') as f:
        f.write(env_content)
    
    print("✅ .env файлы сәтті құрылды!")

def show_menu():
    """Басты мәзір"""
    print("\n" + "="*50)
    print("🚖 ШЕТПЕ-АҚТАУ ТАКСИ БОТ - ОРНАТУ")
    print("="*50)
    print("\n1. Дерекқорды инициализациялау")
    print("2. Админ қосу")
    print("3. Қажетті пакеттерді тексеру")
    print("4. .env файлын құру")
    print("5. Толық орнату (барлығы)")
    print("0. Шығу")
    print("\n" + "─"*50)

def full_setup():
    """Толық орнату"""
    print("\n🚀 ТОЛЫҚ ОРНАТУ БАСТАЛДЫ")
    print("="*50)
    
    # 1. Пакеттерді тексеру
    if not check_requirements():
        print("\n❌ Алдымен қажетті пакеттерді орнатыңыз!")
        print("Команда: pip install -r requirements.txt")
        return
    
    # 2. Дерекқорды құру
    init_database()
    
    # 3. .env файлын құру
    create_env = input("\n.env файлын құру керек пе? (y/n): ").lower()
    if create_env == 'y':
        create_env_file()
    
    # 4. Админ қосу
    add_admin_confirm = input("\nАдмин қосу керек пе? (y/n): ").lower()
    if add_admin_confirm == 'y':
        add_admin()
    
    print("\n" + "="*50)
    print("✅ ТОЛЫҚ ОРНАТУ АЯҚТАЛДЫ!")
    print("="*50)
    print("\n📝 Келесі қадамдар:")
    print("1. bot.py файлында BOT_TOKEN ауыстырыңыз")
    print("2. python bot.py командасымен ботты іске қосыңыз")
    print("3. Telegram-да ботқа /start жіберіңіз")
    print("\n💡 Кеңес: Админ командалары:")
    print("   /admin - Админ панелі")
    print("   /driver - Жүргізуші панелі")
    print("   /payment - Төлем жасау")

def main():
    """Басты функция"""
    while True:
        show_menu()
        choice = input("Таңдау (0-5): ").strip()
        
        if choice == '1':
            init_database()
        elif choice == '2':
            add_admin()
        elif choice == '3':
            check_requirements()
        elif choice == '4':
            create_env_file()
        elif choice == '5':
            full_setup()
            break
        elif choice == '0':
            print("\n👋 Сау болыңыз!")
            sys.exit(0)
        else:
            print("\n❌ Қате таңдау! 0-5 арасынан таңдаңыз.")
        
        input("\nЖалғастыру үшін Enter басыңыз...")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Орнату тоқтатылды. Сау болыңыз!")
        sys.exit(0)