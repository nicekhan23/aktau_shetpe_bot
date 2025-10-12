import sqlite3

def add_admin(user_id):
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