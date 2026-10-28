import asyncio, os, secrets, sqlite3, threading, time, urllib.request
from contextlib import asynccontextmanager

from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramForbiddenError
from aiogram.types import ChatMemberUpdated, Message, ReplyParameters
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel

load_dotenv()
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASS = os.environ["ADMIN_PASS"]
DB_PATH = os.environ.get("DB_PATH", "data.db")
# Render apne aap RENDER_EXTERNAL_URL set karta hai. Kahin aur ho to PING_URL do.
PING_URL = os.environ.get("PING_URL") or os.environ.get("RENDER_EXTERNAL_URL", "")
PING_EVERY = int(os.environ.get("PING_EVERY", "600"))  # seconds (10 min)

# ---------- Database ----------
db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.row_factory = sqlite3.Row
lock = threading.Lock()
db.executescript("""
CREATE TABLE IF NOT EXISTS chats(
  chat_id INTEGER PRIMARY KEY, type TEXT, title TEXT, username TEXT,
  active INTEGER DEFAULT 1, updated REAL, unread INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS messages(
  id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, sender TEXT,
  text TEXT, direction TEXT, ts REAL, tg_msg_id INTEGER);
""")
for alter in ("ALTER TABLE chats ADD COLUMN unread INTEGER DEFAULT 0",
              "ALTER TABLE messages ADD COLUMN tg_msg_id INTEGER"):
    try:
        db.execute(alter); db.commit()
    except sqlite3.OperationalError:
        pass  # column already exists


def q(sql, args=(), fetch=False):
    with lock:
        cur = db.execute(sql, args)
        db.commit()
        return [dict(r) for r in cur.fetchall()] if fetch else None


def upsert_chat(chat, active=1):
    title = chat.title or " ".join(filter(None, [chat.first_name, chat.last_name])) or str(chat.id)
    q("""INSERT INTO chats(chat_id,type,title,username,active,updated) VALUES(?,?,?,?,?,?)
         ON CONFLICT(chat_id) DO UPDATE SET type=excluded.type, title=excluded.title,
         username=excluded.username, active=excluded.active, updated=excluded.updated""",
      (chat.id, chat.type, title, chat.username, active, time.time()))


def save_msg(chat_id, sender, text, direction, tg_id):
    q("INSERT INTO messages(chat_id,sender,text,direction,ts,tg_msg_id) VALUES(?,?,?,?,?,?)",
      (chat_id, sender, text, direction, time.time(), tg_id))
    q("UPDATE chats SET updated=? WHERE chat_id=?", (time.time(), chat_id))


# ---------- Bot ----------
bot = Bot(BOT_TOKEN)
dp = Dispatcher()


@dp.my_chat_member()
async def on_membership(ev: ChatMemberUpdated):
    joined = ev.new_chat_member.status in ("member", "administrator")
    upsert_chat(ev.chat, active=1 if joined else 0)


@dp.message()
async def on_message(m: Message):
    upsert_chat(m.chat)
    sender = m.from_user.full_name if m.from_user else "unknown"
    if m.from_user and m.from_user.username:
        sender += f" (@{m.from_user.username})"
    save_msg(m.chat.id, sender, m.text or m.caption or f"[{m.content_type}]", "in", m.message_id)
    q("UPDATE chats SET unread=unread+1 WHERE chat_id=?", (m.chat.id,))


async def deliver(chat_id: int, text: str, reply_to: int | None = None):
    kw = {}
    if reply_to:
        kw["reply_parameters"] = ReplyParameters(message_id=reply_to, allow_sending_without_reply=True)
    msg = await bot.send_message(chat_id, text, **kw)
    save_msg(chat_id, "Dashboard", text, "out", msg.message_id)


# ---------- Keep-alive (Render free plan sleep na kare) ----------
async def keep_alive():
    if not PING_URL:
        return
    url = PING_URL.rstrip("/") + "/health"
    while True:
        await asyncio.sleep(PING_EVERY)
        try:
            await asyncio.to_thread(lambda: urllib.request.urlopen(url, timeout=20).read())
        except Exception as e:
            print("ping failed:", e)


# ---------- Web API ----------
security = HTTPBasic()


def auth(c: HTTPBasicCredentials = Depends(security)):
    ok = secrets.compare_digest(c.username, ADMIN_USER) and secrets.compare_digest(c.password, ADMIN_PASS)
    if not ok:
        raise HTTPException(401, "Wrong login", headers={"WWW-Authenticate": "Basic"})


@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = [
        asyncio.create_task(dp.start_polling(
            bot, allowed_updates=["message", "my_chat_member"], handle_signals=False)),
        asyncio.create_task(keep_alive()),
    ]
    yield
    for t in tasks:
        t.cancel()
    await bot.session.close()


app = FastAPI(lifespan=lifespan)


@app.get("/health", response_class=PlainTextResponse)
def health():  # login ke bina, ping ke liye
    return "ok"


@app.get("/", dependencies=[Depends(auth)])
def index():
    return FileResponse("static/index.html")


@app.get("/api/chats", dependencies=[Depends(auth)])
def chats():
    return q("""SELECT c.*, (SELECT text FROM messages m WHERE m.chat_id=c.chat_id ORDER BY id DESC LIMIT 1) AS last,
                (SELECT ts FROM messages m WHERE m.chat_id=c.chat_id ORDER BY id DESC LIMIT 1) AS last_ts
                FROM chats c ORDER BY updated DESC""", fetch=True)


@app.get("/api/messages/{chat_id}", dependencies=[Depends(auth)])
def messages(chat_id: int):
    q("UPDATE chats SET unread=0 WHERE chat_id=?", (chat_id,))
    return q("SELECT * FROM messages WHERE chat_id=? ORDER BY id DESC LIMIT 300", (chat_id,), fetch=True)[::-1]


class Out(BaseModel):
    chat_id: int
    text: str
    reply_to: int | None = None  # Telegram message_id jisko reply karna hai


@app.post("/api/send", dependencies=[Depends(auth)])
async def send(o: Out):
    try:
        await deliver(o.chat_id, o.text, o.reply_to)
    except Exception as e:
        raise HTTPException(400, str(e))
    return {"ok": True}


class Broadcast(BaseModel):
    text: str
    target: str = "all"  # all | dm | group


@app.post("/api/broadcast", dependencies=[Depends(auth)])
async def broadcast(b: Broadcast):
    rows = q("SELECT chat_id,type FROM chats WHERE active=1", fetch=True)
    if b.target == "dm":
        rows = [r for r in rows if r["type"] == "private"]
    elif b.target == "group":
        rows = [r for r in rows if r["type"] != "private"]
    sent = failed = 0
    for r in rows:
        try:
            await deliver(r["chat_id"], b.text)
            sent += 1
        except TelegramForbiddenError:
            q("UPDATE chats SET active=0 WHERE chat_id=?", (r["chat_id"],))
            failed += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)  # Telegram rate limit (~30 msg/sec)
    return {"sent": sent, "failed": failed}
