# 🚀 Деплой нұсқаулары

Бұл нұсқаулық ботты әртүрлі платформаларға орналастыруға көмектеседі.

---

## 📋 Мазмұны

1. [VPS Серверге деплой](#1-vps-серверге-деплой)
2. [Heroku-ға деплой](#2-heroku-ға-деплой)
3. [Railway.app-қа деплой](#3-railwayapp-қа-деплой)
4. [Docker арқылы деплой](#4-docker-арқылы-деплой)

---

## 1️⃣ VPS Серверге деплой

### Қажетті нәрселер:
- Ubuntu 20.04+ немесе Debian 11+
- Root немесе sudo құқықтары
- SSH қатынасы

### Қадам 1: Серверді дайындау

```bash
# Серверге қосылу
ssh username@your-server-ip

# Жүйені жаңарту
sudo apt update && sudo apt upgrade -y

# Python және қажетті құралдарды орнату
sudo apt install python3 python3-pip python3-venv git -y
```

### Қадам 2: Жобаны клондау

```bash
# Үй директориясына өту
cd ~

# Репозиторийді клондау
git clone https://github.com/yourusername/shetpe-aktau-taxi-bot.git
cd shetpe-aktau-taxi-bot
```

### Қадам 3: Виртуалды орта құру

```bash
# Виртуалды орта құру
python3 -m venv venv

# Активациялау
source venv/bin/activate

# Тәуелділіктерді орнату
pip install -r requirements.txt
```

### Қадам 4: Конфигурация

```bash
# bot.py файлын редакциялау
nano bot.py

# BOT_TOKEN өзгерту:
BOT_TOKEN = "СІЗДІҢ_ТОКЕНІҢІЗ"

# Сақтау: Ctrl+X, Y, Enter
```

### Қадам 5: Дерекқорды инициализациялау

```bash
python setup.py
# Мәзірден 5 (Толық орнату) таңдаңыз
```

### Қадам 6: Ботты сынау

```bash
python bot.py
# Ctrl+C арқылы тоқтату
```

### Қадам 7: Systemd сервис құру

```bash
# Сервис файлын көшіру
sudo cp taxi-bot.service /etc/systemd/system/

# Пайдаланушы атын өзгерту
sudo nano /etc/systemd/system/taxi-bot.service
# User=ubuntu дегенді өз пайдаланушыңызға өзгертіңіз

# Логтар қалтасын құру
mkdir -p ~/shetpe-aktau-taxi-bot/logs

# Сервисті іске қосу
sudo systemctl daemon-reload
sudo systemctl enable taxi-bot
sudo systemctl start taxi-bot

# Статусты тексеру
sudo systemctl status taxi-bot
```

### Қадам 8: Логтарды қадағалау

```bash
# Жанды логтар
tail -f ~/shetpe-aktau-taxi-bot/logs/bot.log

# Systemd логтары
sudo journalctl -u taxi-bot -f
```

### Басқару командалары:

```bash
# Ботты тоқтату
sudo systemctl stop taxi-bot

# Ботты іске қосу
sudo systemctl start taxi-bot

# Ботты қайта іске қосу
sudo systemctl restart taxi-bot

# Автоматты іске қосуды өшіру
sudo systemctl disable taxi-bot
```

---

## 2️⃣ Heroku-ға деплой

### Қажетті нәрселер:
- Heroku аккаунты
- Heroku CLI
- Git

### Қадам 1: Heroku CLI орнату

**macOS:**
```bash
brew tap heroku/brew && brew install heroku
```

**Ubuntu/Debian:**
```bash
curl https://cli-assets.heroku.com/install.sh | sh
```

**Windows:**
[Heroku CLI жүктеп алу](https://devcenter.heroku.com/articles/heroku-cli)

### Қадам 2: Heroku-ға кіру

```bash
heroku login
```

### Қадам 3: Қосымша файлдар құру

**Procfile** жасау:
```bash
echo "worker: python bot.py" > Procfile
```

**runtime.txt** жасау:
```bash
echo "python-3.11.0" > runtime.txt
```

### Қадам 4: Heroku қолданбасын құру

```bash
heroku create shetpe-aktau-taxi-bot

# PostgreSQL қосу
heroku addons:create heroku-postgresql:mini
```

### Қадам 5: Конфигурация орнату

```bash
# Bot токенін орнату
heroku config:set BOT_TOKEN=your_bot_token_here

# Басқа конфигурациялар
heroku config:set ADMIN_USER_ID=your_admin_id
heroku config:set KASPI_PHONE=+7XXXXXXXXXX
```

### Қадам 6: Деплой жасау

```bash
git add .
git commit -m "Initial deploy"
git push heroku main
```

### Қадам 7: Worker-ді іске қосу

```bash
heroku ps:scale worker=1
```

### Қадам 8: Логтарды қарау

```bash
heroku logs --tail
```

### Басқару командалары:

```bash
# Ботты тоқтату
heroku ps:scale worker=0

# Ботты іске қосу
heroku ps:scale worker=1

# Қайта іске қосу
heroku restart

# Конфигурацияны көру
heroku config
```

---

## 3️⃣ Railway.app-қа деплой

### Қажетті нәрселер:
- Railway аккаунты (GitHub арқылы)
- Git

### Қадам 1: Railway-ге кіру

1. [Railway.app](https://railway.app) сайтына өту
2. "Login with GitHub" басу

### Қадам 2: Жаңа жоба құру

1. "New Project" басу
2. "Deploy from GitHub repo" таңдау
3. Репозиторийді таңдау

### Қадам 3: Конфигурация

1. "Variables" табына өту
2. Қоса:
   - `BOT_TOKEN` = токен
   - `ADMIN_USER_ID` = admin ID
   - `KASPI_PHONE` = телефон

### Қадам 4: Start командасын орнату

**Settings → Deploy:**
```
Start Command: python bot.py
```

### Қадам 5: Деплой

Railway автоматты түрде деплой жасайды. Логтарды "Deployments" табынан қараңыз.

---

## 4️⃣ Docker арқылы деплой

### Қадам 1: Dockerfile жасау

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "bot.py"]
```

### Қадам 2: .dockerignore жасау

```
venv/
__pycache__/
*.pyc
.env
.git/
.gitignore
taxi_bot.db
logs/
```

### Қадам 3: Docker образын құру

```bash
docker build -t shetpe-aktau-taxi-bot .
```

### Қадам 4: Контейнерді іске қосу

```bash
docker run -d \
  --name taxi-bot \
  -e BOT_TOKEN=your_token \
  -e ADMIN_USER_ID=your_id \
  -v $(pwd)/taxi_bot.db:/app/taxi_bot.db \
  shetpe-aktau-taxi-bot
```

### Қадам 5: docker-compose.yml (қосымша)

```yaml
version: '3.8'

services:
  bot:
    build: .
    container_name: taxi-bot
    environment:
      - BOT_TOKEN=${BOT_TOKEN}
      - ADMIN_USER_ID=${ADMIN_USER_ID}
      - KASPI_PHONE=${KASPI_PHONE}
    volumes:
      - ./taxi_bot.db:/app/taxi_bot.db
      - ./logs:/app/logs
    restart: unless-stopped
```

Іске қосу:
```bash
docker-compose up -d
```

### Docker басқару командалары:

```bash
# Логтарды қарау
docker logs -f taxi-bot

# Контейнерді тоқтату
docker stop taxi-bot

# Контейнерді іске қосу
docker start taxi-bot

# Контейнерді жою
docker rm -f taxi-bot
```

---

## 🔐 Қауіпсіздік ұсыныстары

### 1. Firewall орнату (VPS)

```bash
# UFW орнату
sudo apt install ufw

# SSH рұқсат ету
sudo ufw allow ssh

# Firewall іске қосу
sudo ufw enable
```

### 2. SSL/TLS (Webhook үшін)

Егер webhook пайдаланатын болсаңыз:
```bash
sudo apt install certbot
sudo certbot certonly --standalone -d yourdomain.com
```

### 3. Дерекқор резервті көшірмесі

```bash
# Автоматты резервті көшірме скрипті
#!/bin/bash
cp taxi_bot.db backups/taxi_bot_$(date +%Y%m%d_%H%M%S).db

# Crontab қосу
crontab -e
# Күн сайын түнгі 2:00-де
0 2 * * * /path/to/backup-script.sh
```

### 4. Логтарды тазалау

```bash
# Ескі логтарды тазалау (30 күннен ескі)
find logs/ -name "*.log" -mtime +30 -delete
```

---

## 📊 Мониторинг

### 1. Uptime мониторинг

**UptimeRobot** немесе **Pingdom** пайдаланыңыз:
- Ботты тексеру үшін webhook орнатыңыз
- Email/SMS хабарламалар орнатыңыз

### 2. Логтарды талдау

```bash
# Қателерді табу
grep -i error logs/bot.log

# Соңғы 100 жолды көрсету
tail -n 100 logs/bot.log
```

### 3. Дерекқор мониторингі

```bash
# Дерекқор көлемі
ls -lh taxi_bot.db

# SQLite статистикасы
sqlite3 taxi_bot.db "SELECT COUNT(*) FROM drivers;"
sqlite3 taxi_bot.db "SELECT COUNT(*) FROM bookings;"
```

---

## 🆘 Жиі кездесетін мәселелер

### Мәселе: Бот жауап бермейді

**Шешім:**
```bash
# Логтарды тексеру
tail -f logs/bot.log

# Процесті тексеру
ps aux | grep python

# Қайта іске қосу
sudo systemctl restart taxi-bot
```

### Мәселе: Database is locked

**Шешім:**
```bash
# Барлық процестерді тоқтату
sudo systemctl stop taxi-bot

# 5 секунд күту
sleep 5

# Қайта іске қосу
sudo systemctl start taxi-bot
```

### Мәселе: Out of memory

**Шешім:**
```bash
# Swap файл құру (2GB)
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

---

## 📞 Қолдау

Қосымша көмек қажет болса:
- 📧 Email: support@example.com
- 💬 Telegram: @support_username
- 🐛 GitHub Issues

---

**Сәттілік тілейміз! 🚀**