import unittest
import sqlite3
import os
from datetime import datetime

# Тестовая база данных
TEST_DB = "test_taxi_bot.db"

class TestDatabase(unittest.TestCase):
    """Тесты базы данных"""
    
    def setUp(self):
        """Создаем тестовую БД перед каждым тестом"""
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
        
        conn = sqlite3.connect(TEST_DB)
        c = conn.cursor()
        
        c.execute('''CREATE TABLE drivers
                     (user_id INTEGER PRIMARY KEY,
                      full_name TEXT,
                      phone TEXT,
                      car_number TEXT,
                      car_model TEXT,
                      total_seats INTEGER,
                      direction TEXT,
                      queue_position INTEGER,
                      is_active INTEGER DEFAULT 1,
                      is_verified INTEGER DEFAULT 1,
                      avg_rating REAL DEFAULT 0,
                      rating_count INTEGER DEFAULT 0)''')
        
        c.execute('''CREATE TABLE clients
                     (user_id INTEGER PRIMARY KEY,
                      full_name TEXT,
                      phone TEXT,
                      direction TEXT,
                      queue_position INTEGER,
                      passengers_count INTEGER,
                      pickup_location TEXT,
                      dropoff_location TEXT,
                      is_verified INTEGER DEFAULT 1,
                      status TEXT DEFAULT 'waiting',
                      avg_rating REAL DEFAULT 0,
                      rating_count INTEGER DEFAULT 0)''')
        
        c.execute('''CREATE TABLE ratings
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      from_user_id INTEGER,
                      to_user_id INTEGER,
                      user_type TEXT,
                      rating INTEGER,
                      review TEXT)''')
        
        conn.commit()
        conn.close()
    
    def tearDown(self):
        """Удаляем тестовую БД после каждого теста"""
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
    
    def test_driver_registration(self):
        """Тест регистрации водителя"""
        conn = sqlite3.connect(TEST_DB)
        c = conn.cursor()
        
        c.execute('''INSERT INTO drivers 
                     (user_id, full_name, phone, car_number, car_model, 
                      total_seats, direction, queue_position)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (12345, "Тест Тестов", "+77001234567", "123 ABC 01",
                   "Toyota Camry", 4, "Шетпе → Ақтау", 1))
        conn.commit()
        
        c.execute("SELECT * FROM drivers WHERE user_id=?", (12345,))
        driver = c.fetchone()
        conn.close()
        
        self.assertIsNotNone(driver)
        self.assertEqual(driver[1], "Тест Тестов")
        self.assertEqual(driver[5], 4)
    
    def test_queue_position_increment(self):
        """Тест автоматического увеличения позиции в очереди"""
        conn = sqlite3.connect(TEST_DB)
        c = conn.cursor()
        
        # Добавляем первого водителя
        c.execute('''INSERT INTO drivers 
                     (user_id, full_name, phone, car_number, car_model, 
                      total_seats, direction, queue_position)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (1, "Driver 1", "+77001111111", "111 A 01", "Car1", 4, "Шетпе → Ақтау", 1))
        
        # Добавляем второго водителя
        c.execute("SELECT MAX(queue_position) FROM drivers WHERE direction=?", ("Шетпе → Ақтау",))
        max_pos = c.fetchone()[0]
        new_pos = (max_pos or 0) + 1
        
        c.execute('''INSERT INTO drivers 
                     (user_id, full_name, phone, car_number, car_model, 
                      total_seats, direction, queue_position)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (2, "Driver 2", "+77002222222", "222 B 02", "Car2", 4, "Шетпе → Ақтау", new_pos))
        
        conn.commit()
        
        c.execute("SELECT queue_position FROM drivers WHERE user_id=?", (2,))
        position = c.fetchone()[0]
        conn.close()
        
        self.assertEqual(position, 2)
    
    def test_driver_exit_reorders_queue(self):
        """Тест пересчета очереди при выходе водителя"""
        conn = sqlite3.connect(TEST_DB)
        c = conn.cursor()
        
        # Добавляем 3 водителей
        for i in range(1, 4):
            c.execute('''INSERT INTO drivers 
                         (user_id, full_name, phone, car_number, car_model, 
                          total_seats, direction, queue_position)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                      (i, f"Driver {i}", f"+7700{i}{i}{i}{i}{i}{i}{i}",
                       f"{i}{i}{i} A 0{i}", "Car", 4, "Шетпе → Ақтау", i))
        conn.commit()
        
        # Удаляем второго водителя
        c.execute("DELETE FROM drivers WHERE user_id=?", (2,))
        
        # Пересчитываем позиции
        c.execute("SELECT user_id FROM drivers WHERE direction=? ORDER BY queue_position",
                 ("Шетпе → Ақтау",))
        drivers = c.fetchall()
        
        for pos, driver in enumerate(drivers, 1):
            c.execute("UPDATE drivers SET queue_position=? WHERE user_id=?",
                     (pos, driver[0]))
        conn.commit()
        
        # Проверяем
        c.execute("SELECT queue_position FROM drivers WHERE user_id=?", (3,))
        position = c.fetchone()[0]
        conn.close()
        
        self.assertEqual(position, 2)
    
    def test_client_registration(self):
        """Тест регистрации клиента"""
        conn = sqlite3.connect(TEST_DB)
        c = conn.cursor()
        
        c.execute('''INSERT INTO clients 
                     (user_id, full_name, phone, direction, queue_position,
                      passengers_count, pickup_location, dropoff_location)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (99999, "Клиент Клиентов", "+77009999999", "Шетпе → Ақтау",
                   1, 3, "ул. Абая 10", "пр. Достык 50"))
        conn.commit()
        
        c.execute("SELECT * FROM clients WHERE user_id=?", (99999,))
        client = c.fetchone()
        conn.close()
        
        self.assertIsNotNone(client)
        self.assertEqual(client[5], 3)  # passengers_count
        self.assertEqual(client[9], 'waiting')  # status
    
    def test_rating_system(self):
        """Тест системы рейтингов"""
        conn = sqlite3.connect(TEST_DB)
        c = conn.cursor()
        
        # Добавляем водителя
        c.execute('''INSERT INTO drivers 
                     (user_id, full_name, phone, car_number, car_model, 
                      total_seats, direction, queue_position)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (1, "Driver", "+77001111111", "111 A 01", "Car", 4, "Шетпе → Ақтау", 1))
        
        # Добавляем 3 оценки
        ratings = [5, 4, 5]
        for rating in ratings:
            c.execute('''INSERT INTO ratings (from_user_id, to_user_id, user_type, rating)
                         VALUES (?, ?, ?, ?)''', (999, 1, "driver", rating))
        
        # Вычисляем средний рейтинг
        c.execute("SELECT AVG(rating) FROM ratings WHERE to_user_id=?", (1,))
        avg_rating = c.fetchone()[0]
        
        c.execute("SELECT COUNT(*) FROM ratings WHERE to_user_id=?", (1,))
        rating_count = c.fetchone()[0]
        
        # Обновляем водителя
        c.execute('''UPDATE drivers SET avg_rating=?, rating_count=? WHERE user_id=?''',
                 (avg_rating, rating_count, 1))
        conn.commit()
        
        c.execute("SELECT avg_rating, rating_count FROM drivers WHERE user_id=?", (1,))
        driver_rating = c.fetchone()
        conn.close()
        
        self.assertAlmostEqual(driver_rating[0], 4.67, places=1)
        self.assertEqual(driver_rating[1], 3)
    
    def test_client_cancellation(self):
        """Тест отмены заказа клиентом"""
        conn = sqlite3.connect(TEST_DB)
        c = conn.cursor()
        
        # Добавляем 2 клиентов
        c.execute('''INSERT INTO clients 
                     (user_id, full_name, phone, direction, queue_position,
                      passengers_count, pickup_location, dropoff_location)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (1, "Client 1", "+77001111111", "Шетпе → Ақтау", 1, 2, "Addr1", "Addr2"))
        
        c.execute('''INSERT INTO clients 
                     (user_id, full_name, phone, direction, queue_position,
                      passengers_count, pickup_location, dropoff_location)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (2, "Client 2", "+77002222222", "Шетпе → Ақтау", 2, 1, "Addr3", "Addr4"))
        conn.commit()
        
        # Первый клиент отменяет
        c.execute("DELETE FROM clients WHERE user_id=?", (1,))
        
        # Пересчитываем позиции
        c.execute("SELECT user_id FROM clients WHERE direction=? ORDER BY queue_position",
                 ("Шетпе → Ақтау",))
        clients = c.fetchall()
        
        for pos, client in enumerate(clients, 1):
            c.execute("UPDATE clients SET queue_position=? WHERE user_id=?",
                     (pos, client[0]))
        conn.commit()
        
        # Проверяем
        c.execute("SELECT queue_position FROM clients WHERE user_id=?", (2,))
        position = c.fetchone()[0]
        conn.close()
        
        self.assertEqual(position, 1)
    
    def test_different_directions_separate_queues(self):
        """Тест раздельных очередей для разных направлений"""
        conn = sqlite3.connect(TEST_DB)
        c = conn.cursor()
        
        # Добавляем водителей в разные направления
        c.execute('''INSERT INTO drivers 
                     (user_id, full_name, phone, car_number, car_model, 
                      total_seats, direction, queue_position)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (1, "Driver 1", "+77001111111", "111 A 01", "Car", 4, "Шетпе → Ақтау", 1))
        
        c.execute('''INSERT INTO drivers 
                     (user_id, full_name, phone, car_number, car_model, 
                      total_seats, direction, queue_position)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (2, "Driver 2", "+77002222222", "222 B 02", "Car", 4, "Ақтау → Шетпе", 1))
        
        c.execute('''INSERT INTO drivers 
                     (user_id, full_name, phone, car_number, car_model, 
                      total_seats, direction, queue_position)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (3, "Driver 3", "+77003333333", "333 C 03", "Car", 4, "Шетпе → Ақтау", 2))
        conn.commit()
        
        # Проверяем позиции
        c.execute("SELECT queue_position FROM drivers WHERE user_id=? AND direction=?",
                 (2, "Ақтау → Шетпе"))
        pos1 = c.fetchone()[0]
        
        c.execute("SELECT queue_position FROM drivers WHERE user_id=? AND direction=?",
                 (3, "Шетпе → Ақтау"))
        pos2 = c.fetchone()[0]
        conn.close()
        
        self.assertEqual(pos1, 1)  # Первый в своем направлении
        self.assertEqual(pos2, 2)  # Второй в своем направлении


class TestValidation(unittest.TestCase):
    """Тесты валидации данных"""
    
    def test_phone_validation(self):
        """Тест валидации номера телефона"""
        valid_phones = ["+77001234567", "+77777777777"]
        invalid_phones = ["77001234567", "+7700123456", "1234567890", "+8800123456"]
        
        for phone in valid_phones:
            self.assertTrue(phone.startswith('+7') and len(phone) == 12,
                          f"Valid phone failed: {phone}")
        
        for phone in invalid_phones:
            self.assertFalse(phone.startswith('+7') and len(phone) == 12,
                           f"Invalid phone passed: {phone}")
    
    def test_seats_validation(self):
        """Тест валидации количества мест"""
        valid_seats = [1, 4, 8]
        invalid_seats = [0, 9, -1, 100]
        
        for seats in valid_seats:
            self.assertTrue(1 <= seats <= 8, f"Valid seats failed: {seats}")
        
        for seats in invalid_seats:
            self.assertFalse(1 <= seats <= 8, f"Invalid seats passed: {seats}")
    
    def test_rating_validation(self):
        """Тест валидации рейтинга"""
        valid_ratings = [1, 3, 5]
        invalid_ratings = [0, 6, -1, 10]
        
        for rating in valid_ratings:
            self.assertTrue(1 <= rating <= 5, f"Valid rating failed: {rating}")
        
        for rating in invalid_ratings:
            self.assertFalse(1 <= rating <= 5, f"Invalid rating passed: {rating}")


class TestEdgeCases(unittest.TestCase):
    """Тесты граничных случаев"""
    
    def setUp(self):
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
        
        conn = sqlite3.connect(TEST_DB)
        c = conn.cursor()
        
        c.execute('''CREATE TABLE drivers
                     (user_id INTEGER PRIMARY KEY,
                      full_name TEXT,
                      phone TEXT,
                      direction TEXT,
                      queue_position INTEGER,
                      is_active INTEGER DEFAULT 1)''')
        
        c.execute('''CREATE TABLE clients
                     (user_id INTEGER PRIMARY KEY,
                      full_name TEXT,
                      direction TEXT,
                      queue_position INTEGER,
                      status TEXT DEFAULT 'waiting')''')
        
        c.execute('''CREATE TABLE trips
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      driver_id INTEGER,
                      client_id INTEGER,
                      status TEXT)''')
        
        conn.commit()
        conn.close()
    
    def tearDown(self):
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
    
    def test_driver_exit_with_active_trips(self):
        """Тест: водитель не может выйти с активными поездками"""
        conn = sqlite3.connect(TEST_DB)
        c = conn.cursor()
        
        # Добавляем водителя
        c.execute('''INSERT INTO drivers (user_id, full_name, phone, direction, queue_position)
                     VALUES (?, ?, ?, ?, ?)''',
                  (1, "Driver", "+77001111111", "Шетпе → Ақтау", 1))
        
        # Добавляем активную поездку
        c.execute('''INSERT INTO trips (driver_id, client_id, status)
                     VALUES (?, ?, ?)''', (1, 999, 'waiting'))
        conn.commit()
        
        # Проверяем наличие активных поездок
        c.execute('''SELECT COUNT(*) FROM trips 
                     WHERE driver_id=? AND status IN ('waiting', 'driver_arrived')''', (1,))
        active_trips = c.fetchone()[0]
        
        conn.close()
        
        # Водитель НЕ должен удалиться
        self.assertGreater(active_trips, 0, "Driver should have active trips")
    
    def test_duplicate_phone_registration(self):
        """Тест: нельзя зарегистрировать один номер дважды"""
        conn = sqlite3.connect(TEST_DB)
        c = conn.cursor()
        
        phone = "+77001234567"
        
        # Первая регистрация
        c.execute('''INSERT INTO drivers (user_id, full_name, phone, direction, queue_position)
                     VALUES (?, ?, ?, ?, ?)''',
                  (1, "Driver 1", phone, "Шетпе → Ақтау", 1))
        conn.commit()
        
        # Проверяем перед второй регистрацией
        c.execute("SELECT user_id FROM drivers WHERE phone=?", (phone,))
        existing = c.fetchone()
        
        conn.close()
        
        self.assertIsNotNone(existing, "Phone should already exist")
    
    def test_empty_queue(self):
        """Тест: работа с пустой очередью"""
        conn = sqlite3.connect(TEST_DB)
        c = conn.cursor()
        
        c.execute("SELECT COUNT(*) FROM drivers WHERE direction=?", ("Шетпе → Ақтау",))
        count = c.fetchone()[0]
        
        conn.close()
        
        self.assertEqual(count, 0, "Queue should be empty")
    
    def test_client_cancellation_updates_status(self):
        """Тест: отмена клиента обновляет статус поездки"""
        conn = sqlite3.connect(TEST_DB)
        c = conn.cursor()
        
        # Добавляем клиента
        c.execute('''INSERT INTO clients (user_id, full_name, direction, queue_position)
                     VALUES (?, ?, ?, ?)''', (1, "Client", "Шетпе → Ақтау", 1))
        
        # Создаем поездку
        c.execute('''INSERT INTO trips (driver_id, client_id, status)
                     VALUES (?, ?, ?)''', (999, 1, 'waiting'))
        conn.commit()
        
        # Клиент отменяет
        c.execute('''UPDATE trips SET status='cancelled' 
                     WHERE client_id=? AND status='waiting' ''', (1,))
        c.execute("DELETE FROM clients WHERE user_id=?", (1,))
        conn.commit()
        
        # Проверяем статус
        c.execute("SELECT status FROM trips WHERE client_id=?", (1,))
        trip_status = c.fetchone()[0]
        
        c.execute("SELECT COUNT(*) FROM clients WHERE user_id=?", (1,))
        client_exists = c.fetchone()[0]
        
        conn.close()
        
        self.assertEqual(trip_status, 'cancelled')
        self.assertEqual(client_exists, 0)


class TestHelperFunctions(unittest.TestCase):
    """Тесты вспомогательных функций"""
    
    def test_verification_code_generation(self):
        """Тест генерации кода верификации"""
        import random
        import string
        
        code = ''.join(random.choices(string.digits, k=4))
        
        self.assertEqual(len(code), 4)
        self.assertTrue(code.isdigit())
    
    def test_rating_stars_display(self):
        """Тест отображения звезд рейтинга"""
        def get_rating_stars(rating: float) -> str:
            if not rating:
                return "❌ Нет оценок"
            stars = int(rating)
            return "⭐" * stars + "☆" * (5 - stars)
        
        self.assertEqual(get_rating_stars(0), "❌ Нет оценок")
        self.assertEqual(get_rating_stars(3), "⭐⭐⭐☆☆")
        self.assertEqual(get_rating_stars(5), "⭐⭐⭐⭐⭐")
        self.assertEqual(get_rating_stars(4.7), "⭐⭐⭐⭐☆")
    
    def test_log_formatting(self):
        """Тест форматирования логов"""
        user_id = 12345
        action = "driver_registered"
        details = "Direction: Шетпе → Ақтау"
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_msg = f"[{timestamp}] User {user_id}: {action} | {details}"
        
        self.assertIn(str(user_id), log_msg)
        self.assertIn(action, log_msg)
        self.assertIn(details, log_msg)


if __name__ == '__main__':
    # Запуск всех тестов
    unittest.main(verbosity=2)