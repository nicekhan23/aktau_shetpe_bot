#!/usr/bin/env python3
"""
Шетпе-Ақтау Такси Бот - Бастапқы орнату скрипті
"""

import sqlite3
import sys
import os

DATABASE_FILE = 'taxi_bot.db'

def init_database():
    """Дерекқорды инициализациялау - СОВМЕСТИМО С bot.py"""
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    # Водители (из migration_v1 + v4)
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
                  is_on_trip INTEGER DEFAULT 0)''')
    
    # Клиенты (из migration_v1 + v2 + v5 + v6 + НОВЫЕ ГОРОДА)
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
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  avg_rating REAL DEFAULT 0,
                  rating_count INTEGER DEFAULT 0,
                  cancellation_count INTEGER DEFAULT 0,
                  order_for TEXT DEFAULT 'self',
                  order_number INTEGER DEFAULT 1,
                  parent_user_id INTEGER,
                  from_city TEXT NOT NULL,
                  to_city TEXT NOT NULL)''')
    
    # Админы
    c.execute('''CREATE TABLE IF NOT EXISTS admins
                 (user_id INTEGER PRIMARY KEY,
                  added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    # Рейтинги (из migration_v2)
    c.execute('''CREATE TABLE IF NOT EXISTS ratings
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  from_user_id INTEGER,
                  to_user_id INTEGER,
                  user_type TEXT,
                  trip_id INTEGER,
                  rating INTEGER CHECK(rating >= 1 AND rating <= 5),
                  review TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    # Поездки (из migration_v3)
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
    
    # Логи действий (из migration_v3)
    c.execute('''CREATE TABLE IF NOT EXISTS actions_log
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  action TEXT,
                  details TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    # Черный список (из migration_v5)
    c.execute('''CREATE TABLE IF NOT EXISTS blacklist
                 (user_id INTEGER PRIMARY KEY,
                  reason TEXT,
                  cancellation_count INTEGER DEFAULT 0,
                  banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    # Устанавливаем версию БД на 6 (все миграции применены)
    c.execute("PRAGMA user_version = 6")
    
    conn.commit()
    conn.close()
    
    print("✅ Дерекқор сәтті құрылды!")
    print("\n📋 Құрылған кестелер:")
    print("  • drivers (с occupied_seats, avg_rating)")
    print("  • clients (с from_city, to_city, status, cancellation_count)")
    print("  • admins")
    print("  • ratings")
    print("  • trips")
    print("  • actions_log")
    print("  • blacklist")
    print("\n✅ DB Version: 6 (все миграции применены)")

def migrate_existing_database():
    """Бар дерекқорды жаңарту (departure_date қосу)"""
    if not os.path.exists(DATABASE_FILE):
        print(f"❌ {DATABASE_FILE} файлы табылмады!")
        return
    
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    
    try:
        # Drivers кестесіне departure_date бағанын қосу
        c.execute("PRAGMA table_info(drivers)")
        columns = [column[1] for column in c.fetchall()]
        
        if 'departure_date' not in columns:
            print("  • drivers кестесіне departure_date қосылуда...")
            c.execute("ALTER TABLE drivers ADD COLUMN departure_date TEXT DEFAULT '2025-01-01'")
            print("  ✅ drivers кестесі жаңартылды")
        else:
            print("  ✅ drivers кестесінде departure_date бар")
        
        # Bookings кестесіне departure_date бағанын қосу
        c.execute("PRAGMA table_info(bookings)")
        columns = [column[1] for column in c.fetchall()]
        
        if 'departure_date' not in columns:
            print("  • bookings кестесіне departure_date қосылуда...")
            c.execute("ALTER TABLE bookings ADD COLUMN departure_date TEXT DEFAULT '2025-01-01'")
            print("  ✅ bookings кестесі жаңартылды")
        else:
            print("  ✅ bookings кестесінде departure_date бар")
        
        conn.commit()
        print("\n✅ Миграция сәтті аяқталды!")
        
    except Exception as e:
        print(f"\n❌ Миграция қатесі: {e}")
    finally:
        conn.close()

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
    print("\n📦 Қажетті пакеттер тексерілуде...")
    
    required = ['aiogram', 'aiohttp', 'python-dotenv']
    missing = []
    
    for package in required:
        try:
            if package == 'python-dotenv':
                __import__('dotenv')
            else:
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
PAYMENT_AMOUNT=1000

# Support Configuration
SUPPORT_USERNAME={support_username}

# Database Configuration
DATABASE_FILE=taxi_bot.db
"""
    
    with open('.env', 'w') as f:
        f.write(env_content)
    
    print("✅ .env файлы сәтті құрылды!")

def check_database_structure():
    """Дерекқор құрылымын тексеру"""
    if not os.path.exists(DATABASE_FILE):
        print(f"❌ {DATABASE_FILE} файлы табылмады!")
        print("💡 Алдымен '1. Дерекқорды инициализациялау' опциясын таңдаңыз")
        return
    
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    
    tables = ['drivers', 'clients', 'bookings', 'admins', 'payment_history', 'notification_log']
    
    print("\n📊 Кестелер мен бағандар:")
    for table in tables:
        try:
            c.execute(f"PRAGMA table_info({table})")
            columns = c.fetchall()
            print(f"\n  ✅ {table}:")
            for col in columns:
                print(f"     • {col[1]} ({col[2]})")
        except:
            print(f"\n  ❌ {table} кестесі жоқ")
    
    conn.close()

def show_menu():
    """Басты мәзір"""
    print("\n" + "="*50)
    print("🚖 ШЕТПЕ-АҚТАУ ТАКСИ БОТ - ОРНАТУ")
    print("="*50)
    print("\n1. Дерекқорды инициализациялау")
    print("2. Бар дерекқорды жаңарту (миграция)")
    print("3. Админ қосу")
    print("4. Қажетті пакеттерді тексеру")
    print("5. .env файлын құру")
    print("6. Дерекқор құрылымын тексеру")
    print("7. Толық орнату (барлығы)")
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
    if os.path.exists(DATABASE_FILE):
        update_db = input(f"\n{DATABASE_FILE} файлы бар. Жаңарту керек пе? (y/n): ").lower()
        if update_db == 'y':
            migrate_existing_database()
        else:
            print("⏭️  Дерекқор өзгертілмеді")
    else:
        init_database()
    
    # 3. .env файлын құру
    create_env = input("\n.env файлын құру керек пе? (y/n): ").lower()
    if create_env == 'y':
        create_env_file()
    
    # 4. Админ қосу
    add_admin_confirm = input("\nАдмин қосу керек пе? (y/n): ").lower()
    if add_admin_confirm == 'y':
        add_admin()
    
    # 5. Құрылымды тексеру
    check_database_structure()
    
    print("\n" + "="*50)
    print("✅ ТОЛЫҚ ОРНАТУ АЯҚТАЛДЫ!")
    print("="*50)
    print("\n📝 Келесі қадамдар:")
    print("1. .env файлында BOT_TOKEN тексеріңіз")
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
        choice = input("Таңдау (0-7): ").strip()
        
        if choice == '1':
            init_database()
        elif choice == '2':
            migrate_existing_database()
        elif choice == '3':
            add_admin()
        elif choice == '4':
            check_requirements()
        elif choice == '5':
            create_env_file()
        elif choice == '6':
            check_database_structure()
        elif choice == '7':
            full_setup()
            break
        elif choice == '0':
            print("\n👋 Сау болыңыз!")
            sys.exit(0)
        else:
            print("\n❌ Қате таңдау! 0-7 арасынан таңдаңыз.")
        
        input("\nЖалғастыру үшін Enter басыңыз...")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Орнату тоқтатылды. Сау болыңыз!")
        sys.exit(0)