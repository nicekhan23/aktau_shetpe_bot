# 📁 Жоба құрылымы

```
shetpe-aktau-taxi-bot/
│
├── 📄 bot.py                    # Негізгі бот файлы
├── 📄 admin.py                  # Админ панелі
├── 📄 setup.py                  # Орнату скрипті
│
├── 📄 requirements.txt          # Python тәуелділіктері
├── 📄 .env.example              # Конфигурация үлгісі
├── 📄 .env                      # Конфигурация (git-те жоқ)
│
├── 📄 README.md                 # Жоба сипаттамасы
├── 📄 PROJECT_STRUCTURE.md      # Жоба құрылымы
│
├── 🗄️ taxi_bot.db              # SQLite дерекқоры
│
├── 📁 logs/                     # Логтар қалтасы
│   └── bot.log
│
└── 📁 backups/                  # Резервті көшірмелер
    └── taxi_bot_backup.db
```

---

## 📄 Файлдар сипаттамасы

### `bot.py`
**Негізгі бот файлы** - барлық функционалды қамтиды:
- ✅ Жүргізуші тіркеуі
- ✅ Клиент брондауы
- ✅ Кезек жүйесі
- ✅ Хабарламалар
- ✅ FSM (Finite State Machine) логикасы

### `admin.py`
**Админ панелі модулі**:
- 👥 Жүргізушілер басқару
- 🧍‍♂️ Клиенттер тізімі
- 📊 Статистика
- 💰 Төлемдер растау
- 🔧 Кезек басқару

### `setup.py`
**Орнату және конфигурация скрипті**:
- 🗄️ Дерекқорды инициализациялау
- 👤 Админ қосу
- ⚙️ Конфигурация орнату
- 📦 Тәуелділіктерді тексеру

### `requirements.txt`
**Python пакеттері**:
```
aiogram==3.4.1    # Telegram Bot framework
aiohttp==3.9.3    # Асинхронды HTTP клиент
```

### `.env.example` / `.env`
**Конфигурация параметрлері**:
- `BOT_TOKEN` - Telegram bot токені
- `ADMIN_USER_ID` - Админ User ID
- `KASPI_PHONE` - Төлем телефон нөмірі
- `SUPPORT_USERNAME` - Қолдау username

### `taxi_bot.db`
**SQLite дерекқоры** - 5 кесте:
1. **drivers** - Жүргізушілер
2. **clients** - Клиенттер
3. **bookings** - Брондар
4. **admins** - Админдер
5. **payment_history** - Төлемдер тарихы
6. **notification_log** - Хабарламалар журналы

---

## 🗄️ Дерекқор схемасы

### `drivers` кестесі
```sql
user_id INTEGER PRIMARY KEY       -- Telegram User ID
full_name TEXT NOT NULL           -- Аты-жөні
car_number TEXT NOT NULL          -- Көлік нөмірі
car_model TEXT NOT NULL           -- Көлік маркасы
total_seats INTEGER NOT NULL      -- Жалпы орын саны
direction TEXT NOT NULL           -- Бағыт
departure_time TEXT NOT NULL      -- Кету уақыты
queue_position INTEGER NOT NULL   -- Кезектегі орын
is_active INTEGER DEFAULT 0       -- Белсенді ме
payment_status INTEGER DEFAULT 0  -- Төлем статусы
created_at TIMESTAMP              -- Тіркеу уақыты
```

### `clients` кестесі
```sql
id INTEGER PRIMARY KEY            -- Автоинкремент ID
user_id INTEGER UNIQUE            -- Telegram User ID
full_name TEXT                    -- Аты-жөні
phone TEXT                        -- Телефон
created_at TIMESTAMP              -- Тіркеу уақыты
```

### `bookings` кестесі
```sql
id INTEGER PRIMARY KEY            -- Автоинкремент ID
client_id INTEGER                 -- Клиент ID (FK)
driver_id INTEGER                 -- Жүргізуші ID (FK)
direction TEXT                    -- Бағыт
pickup_location TEXT              -- Мінетін жер
dropoff_location TEXT             -- Түсетін жер
booking_time TIMESTAMP            -- Брондау уақыты
status TEXT DEFAULT 'active'      -- Статус
```

### `admins` кестесі
```sql
user_id INTEGER PRIMARY KEY       -- Telegram User ID
added_at TIMESTAMP                -- Қосылған уақыт
```

### `payment_history` кестесі
```sql
id INTEGER PRIMARY KEY            -- Автоинкремент ID
driver_id INTEGER                 -- Жүргізуші ID (FK)
amount INTEGER                    -- Төлем сомасы
payment_date TIMESTAMP            -- Төлем күні
approved_by INTEGER               -- Растаған админ ID (FK)
```

---

## 🔄 Бот жұмыс процесі

### 1️⃣ Жүргізуші тіркеуі
```
Пайдаланушы → "🚗 Жүргізуші ретінде кіру"
  ↓
Деректерді енгізу (аты-жөні, көлік, т.б.)
  ↓
Дерекқорға сақтау (payment_status=0)
  ↓
Төлем нұсқауын жіберу
  ↓
Төлем растау күту
```

### 2️⃣ Клиент брондауы
```
Пайдаланушы → "🧍‍♂️ Клиент ретінде кіру"
  ↓
Бағыт таңдау (Шетпе ↔ Ақтау)
  ↓
Мінетін/түсетін жерді көрсету
  ↓
Қолжетімді көліктерді көрсету
  ↓
Брондау растау
  ↓
Жүргізушіге хабарлама
```

### 3️⃣ Кезек жүйесі
```
№1 көлік (4/4) → ТОЛДЫ
  ↓
№2 көлік → БЕЛСЕНДІ
  ↓
Жаңа клиенттер → №2 көлікке
```

### 4️⃣ Төлем процесі
```
Жүргізуші → /payment
  ↓
Төлем деректерін көрсету
  ↓
"Төледім" батырмасын басу
  ↓
Админге хабарлама
  ↓
Админ растау
  ↓
Көлік белсенді
```

---

## 🚀 Орнату және іске қосу

### Қадам 1: Репозиторийді клондау
```bash
git clone https://github.com/yourusername/shetpe-aktau-taxi-bot.git
cd shetpe-aktau-taxi-bot
```

### Қадам 2: Виртуалды орта
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# немесе
venv\Scripts\activate     # Windows
```

### Қадам 3: Тәуелділіктер
```bash
pip install -r requirements.txt
```

### Қадам 4: Орнату скриптін іске қосу
```bash
python setup.py
```

Мәзірден таңдаңыз:
- `5` - Толық орнату (ұсынылады)

### Қадам 5: Конфигурация
`bot.py` файлында токенді ауыстырыңыз:
```python
BOT_TOKEN = "СІЗДІҢ_ТОКЕНІҢІЗ"
```

### Қадам 6: Ботты іске қосу
```bash
python bot.py
```

---

## 📱 Пайдаланушы интерфейсі

### Басты мәзір
```
🚗 Жүргізуші ретінде кіру
🧍‍♂️ Клиент ретінде кіру
📊 Менің брондарым
ℹ️ Анықтама
```

### Жүргізуші панелі (`/driver`)
```
👥 Менің жолаушыларым
🔄 Кері бағытқа ауысу
⏸ Тоқтату/Іске қосу
❌ Тіркеуден шығу
```

### Админ панелі (`/admin`)
```
👥 Жүргізушілер тізімі
🧍‍♂️ Клиенттер тізімі
📊 Статистика
💰 Төлемдер
🔧 Кезек басқару
```

---

## 🔐 Қауіпсіздік

### Деректерді қорғау
- ✅ SQLite дерекқоры жергілікті сақталады
- ✅ Жеке деректер шифрланбайды (GDPR сәйкес)
- ✅ Админ құқықтары тексеріледі

### Құпия деректер
`.env` файлында сақталады (git-те жоқ):
- Bot токені
- Төлем деректері
- Админ ID-лері

### Қауіпсіздік ұсыныстары
1. 🔒 `.env` файлын `.gitignore`-ге қосыңыз
2. 🔒 Bot токенін бөліспеңіз
3. 🔒 Дерекқордың резервті көшірмесін жасаңыз
4. 🔒 VPS серверде қауіпсіздік ережелерін орнатыңыз

---

## 📊 Мониторинг және логтар

### Логтар
```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bot.log'),
        logging.StreamHandler()
    ]
)
```

### Статистика
Админ панелінде көруге болады:
- 📈 Жалпы жүргізушілер
- 📈 Жалпы клиенттер
- 📈 Активті брондар
- 📈 Күнделікті статистика

---

## 🔧 Техникалық қолдау

### Қате туындаса:
1. `logs/bot.log` файлын тексеріңіз
2. Дерекқордың дұрыстығын тексеріңіз:
   ```bash
   sqlite3 taxi_bot.db ".tables"
   ```
3. GitHub Issues-ке жазыңыз
4. Қолдау қызметіне хабарласыңыз

### Жиі кездесетін қателер:

**Қате: "Bot token is invalid"**
```
Шешім: bot.py файлында токенді тексеріңіз
```

**Қате: "Database is locked"**
```
Шешім: Ботты қайта іске қосыңыз немесе дерекқорды тексеріңіз
```

**Қате: "Module 'aiogram' not found"**
```
Шешім: pip install -r requirements.txt
```

---

## 📈 Болашақ жаңартулар

### Фаза 2 (Жоспарланған)
- [ ] Kaspi API интеграциясы
- [ ] Автоматты төлем растау
- [ ] Push хабарламалары
- [ ] Рейтинг жүйесі

### Фаза 3 (Болашақ)
- [ ] GPS tracking
- [ ] Мобильді қолданба
- [ ] Қосымша бағыттар
- [ ] Көп тілді қолдау (орыс, ағылшын)

---

## 🤝 Үлес қосу

Жобаға үлес қосқыңыз келсе:
1. Fork жасаңыз
2. Feature branch құрыңыз (`git checkout -b feature/AmazingFeature`)
3. Өзгерістерді commit жасаңыз (`git commit -m 'Add some AmazingFeature'`)
4. Branch-ке push жасаңыз (`git push origin feature/AmazingFeature`)
5. Pull Request ашыңыз

---

## 📝 Лицензия

MIT License - толығырақ `LICENSE` файлында.

---

## 📞 Байланыс

- 📧 Email: support@example.com
- 💬 Telegram: @support_username
- 🐛 Issues: [GitHub Issues](https://github.com/yourusername/shetpe-aktau-taxi-bot/issues)

---

**Жасалған ❤️ арқылы Шетпе мен Ақтау жұртшылығы үшін**