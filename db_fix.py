import sqlite3
from datetime import datetime

def check_and_fix_database():
    conn = sqlite3.connect('taxi_bot.db')
    c = conn.cursor()
    
    print("🔍 Проверка базы данных...\n")
    
    # 1. Проверить структуру
    print("📊 Структура таблицы drivers:")
    c.execute("PRAGMA table_info(drivers)")
    columns = c.fetchall()
    for col in columns:
        print(f"  {col[1]} ({col[2]})")
    
    # 2. Найти проблемные записи
    print("\n🔍 Проверка на проблемные записи...")
    c.execute("SELECT user_id, full_name, departure_date, departure_time FROM drivers")
    drivers = c.fetchall()
    
    problems = []
    for driver in drivers:
        user_id, name, dep_date, dep_time = driver
        # Проверяем если departure_date содержит ':'
        if dep_date and ':' in str(dep_date):
            problems.append((user_id, name, dep_date, dep_time))
            print(f"  ⚠️ Проблема: User {user_id} ({name})")
            print(f"     departure_date = {dep_date} (должна быть дата!)")
            print(f"     departure_time = {dep_time} (должно быть время!)")
    
    if not problems:
        print("  ✅ Проблем не найдено!")
        conn.close()
        return
    
    # 3. Предложить исправить
    print(f"\n⚠️ Найдено {len(problems)} проблемных записей")
    action = input("\nЧто сделать?\n1 - Удалить проблемные записи\n2 - Только показать\n0 - Выход\nВыбор: ")
    
    if action == '1':
        for user_id, name, _, _ in problems:
            c.execute("DELETE FROM bookings WHERE driver_id=?", (user_id,))
            c.execute("DELETE FROM drivers WHERE user_id=?", (user_id,))
            print(f"✅ Удалена запись: {name} (ID: {user_id})")
        conn.commit()
        print("\n✅ Готово! Теперь можете зарегистрироваться заново.")
    
    conn.close()

if __name__ == "__main__":
    check_and_fix_database()