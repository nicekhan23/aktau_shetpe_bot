# 🚖 Шетпе-Ақтау Такси Бот

Шетпе мен Ақтау арасында жүретін таксилерді ұйымдастыруға арналған Telegram бот.

## 📋 Мүмкіндіктер

### Жүргізушілер үшін:
- ✅ Көлік тіркеу
- ✅ Кезек жүйесі
- ✅ Автоматты клиент жинау
- ✅ Көлік толғанда хабарлама
- ✅ Кері бағытқа ауысу

### Клиенттер үшін:
- ✅ Бағыт таңдау
- ✅ Мінетін/түсетін жер көрсету
- ✅ Қолжетімді көліктерді көру
- ✅ Орын брондау
- ✅ Брондар тарихы

### Админдер үшін:
- ✅ Барлық жүргізушілерді көру
- ✅ Төлемдерді бақылау
- ✅ Кезек ретін басқару
- ✅ Статистика

## 🔧 Орнату

### 1. Репозиторийді клондау
```bash
git clone https://github.com/yourusername/shetpe-aktau-taxi-bot.git
cd shetpe-aktau-taxi-bot
```

### 2. Виртуалды орта құру
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

### 3. Тәуелділіктерді орнату
```bash
pip install -r requirements.txt
```

### 4. Telegram ботты құру

1. [@BotFather](https://t.me/BotFather) ботына өтіңіз
2. `/newbot` командасын жіберіңіз
3. Бот атын енгізіңіз (мысалы: "Шетпе-Ақтау Такси")
4. Username енгізіңіз (мысалы: "shetpe_aktau_bot")
5. Токенді алыңыз (мысалы: `1234567890:ABCdefGHIjklMNOpqrsTUVwxyz`)

### 5. Ботты конфигурациялау

`bot.py` файлында токенді ауыстырыңыз:
```python
BOT_TOKEN = "СІЗДІҢ_ТОКЕНІҢІЗ"
```

### 6. Ботты іске қосу
```bash
python bot.py
```

## 📱 Қолдану

### Жүргізуші ретінде:

1. Ботты іске қосу: `/start`
2. "🚗 Жүргізуші ретінде кіру" батырмасын басу
3. Көлік мәліметтерін енгізу:
   - Аты-жөні
   - Көлік нөмірі
   - Көлік маркасы
   - Бос орын саны
   - Бағыт (Шетпе → Ақтау немесе Ақтау → Шетпе)
   - Кету уақыты
4. Төлем жасау (1000 тг)
5. Клиенттер жиналуын күту

### Клиент ретінде:

1. "🧍‍♂️ Клиент ретінде кіру" батырмасын басу
2. Бағыт таңдау
3. Мінетін жерді таңдау
4. Түсетін жерді таңдау
5. Қолжетімді көліктерден таңдау
6. Брондау растау

## 🗄️ Дерекқор құрылымы

### Кестелер:

**drivers** - Жүргізушілер
- user_id (PRIMARY KEY)
- full_name
- car_number
- car_model
- total_seats
- direction
- departure_time
- queue_position
- is_active
- payment_status

**clients** - Клиенттер
- id (PRIMARY KEY)
- user_id
- full_name
- phone

**bookings** - Брондар
- id (PRIMARY KEY)
- client_id (FOREIGN KEY)
- driver_id (FOREIGN KEY)
- direction
- pickup_location
- dropoff_location
- booking_time
- status

**admins** - Админдер
- user_id (PRIMARY KEY)

## 💰 Төлем жүйесі

Қазіргі уақытта төлем қолмен расталады. Келесі кезеңде:
- Kaspi QR интеграциясы
- Автоматты төлем растау
- Төлем түбіртегі

## 🚀 Деплой

### Heroku-ға деплой:

1. Heroku аккаунты құру
2. Heroku CLI орнату
3. Қолданба құру:
```bash
heroku create shetpe-aktau-taxi
```

4. PostgreSQL қосу:
```bash
heroku addons:create heroku-postgresql:hobby-dev
```

5. Деплой:
```bash
git push heroku main
```

### VPS-ке деплой:

1. Серверге қосылу
2. Репозиторийді клондау
3. Systemd сервис құру:

```bash
sudo nano /etc/systemd/system/taxi-bot.service
```

Мазмұн:
```
[Unit]
Description=Shetpe-Aktau Taxi Bot
After=network.target

[Service]
Type=simple
User=yourusername
WorkingDirectory=/home/yourusername/shetpe-aktau-taxi-bot
ExecStart=/home/yourusername/shetpe-aktau-taxi-bot/venv/bin/python bot.py
Restart=always

[Install]
WantedBy=multi-user.target
```

4. Сервисті іске қосу:
```bash
sudo systemctl enable taxi-bot
sudo systemctl start taxi-bot
```

## 📊 Статистика

Админ панелінде көруге болады:
- Жалпы жүргізушілер саны
- Жалпы клиенттер саны
- Активті брондар
- Әр бағыттағы көліктер саны
- Күнделікті статистика

## 🔐 Қауіпсіздік

- Барлық деректер шифрланған дерекқорда сақталады
- Төлем деректері қауіпсіз өңделеді
- Жеке мәліметтер үшінші тараптармен бөлісілмейді

## 🆘 Қолдау

Сұрақтар туындаса:
- Telegram: @your_support_username
- Email: support@example.com
- GitHub Issues: [Мәселелер](https://github.com/yourusername/shetpe-aktau-taxi-bot/issues)

## 📝 Лицензия

MIT License

## 🤝 Үлес қосу

Pull request-тар қош келеді! Үлкен өзгерістер үшін алдымен issue ашып, өзгерткіңіз келетініңізді талқылаңыз.

## 📅 Жаңарту жоспары

### Фаза 1 (Аяқталды)
- ✅ Негізгі функционал
- ✅ Жүргізуші тіркеуі
- ✅ Клиент брондауы
- ✅ Кезек жүйесі

### Фаза 2 (Әзірлеу кезеңінде)
- 🔄 Төлем автоматтандыру
- 🔄 Админ панелі
- 🔄 Статистика
- 🔄 Хабарламалар жүйесі

### Фаза 3 (Жоспарланған)
- 📅 GPS tracking
- 📅 Рейтинг жүйесі
- 📅 Қосымша бағыттар
- 📅 Мобильді қолданба