import asyncio, html, json, os, secrets, shutil, sqlite3, tempfile, threading, time, urllib.request
from contextlib import asynccontextmanager
from urllib.parse import quote

import aiohttp
from aiogram import BaseMiddleware, Bot, Dispatcher
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command, CommandObject
from aiogram.types import BufferedInputFile, ChatMemberUpdated, Message, ReplyParameters
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, PlainTextResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel

load_dotenv()
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASS = os.environ["ADMIN_PASS"]
DB_PATH = os.environ.get("DB_PATH", "data.db")
OWNER_IDS = {int(x) for x in os.environ.get("OWNER_ID", "").replace(" ", "").split(",") if x.strip("-").isdigit()}
TAG_DELAY = float(os.environ.get("TAG_DELAY", "3.2"))  # group limit ~20 msg/min
PING_URL = os.environ.get("PING_URL") or os.environ.get("RENDER_EXTERNAL_URL", "")
PING_EVERY = int(os.environ.get("PING_EVERY", "600"))

BASE_DIR = os.path.dirname(os.path.abspath(DB_PATH))
AV_DIR = os.path.join(BASE_DIR, "avatars")
MEDIA_DIR = os.path.join(BASE_DIR, "media")
os.makedirs(AV_DIR, exist_ok=True)
os.makedirs(MEDIA_DIR, exist_ok=True)

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
  text TEXT, direction TEXT, ts REAL, tg_msg_id INTEGER, sender_id INTEGER,
  media_type TEXT, file_id TEXT, mime TEXT, fname TEXT);
CREATE TABLE IF NOT EXISTS members(
  chat_id INTEGER, user_id INTEGER, name TEXT, username TEXT, last_seen REAL,
  PRIMARY KEY(chat_id, user_id));
""")
for alter in ("ALTER TABLE chats ADD COLUMN unread INTEGER DEFAULT 0",
              "ALTER TABLE messages ADD COLUMN tg_msg_id INTEGER",
              "ALTER TABLE messages ADD COLUMN sender_id INTEGER",
              "ALTER TABLE messages ADD COLUMN media_type TEXT",
              "ALTER TABLE messages ADD COLUMN file_id TEXT",
              "ALTER TABLE messages ADD COLUMN mime TEXT",
              "ALTER TABLE messages ADD COLUMN fname TEXT"):
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


def save_msg(chat_id, sender, text, direction, tg_id, sender_id=None, media=None):
    mt, fid, mime, fname = media or (None, None, None, None)
    with lock:
        cur = db.execute(
            """INSERT INTO messages(chat_id,sender,text,direction,ts,tg_msg_id,sender_id,media_type,file_id,mime,fname)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (chat_id, sender, text, direction, time.time(), tg_id, sender_id, mt, fid, mime, fname))
        db.execute("UPDATE chats SET updated=? WHERE chat_id=?", (time.time(), chat_id))
        db.commit()
        return cur.lastrowid


def track_member(chat_id, u):
    if not u or u.is_bot:
        return
    q("""INSERT INTO members(chat_id,user_id,name,username,last_seen) VALUES(?,?,?,?,?)
         ON CONFLICT(chat_id,user_id) DO UPDATE SET name=excluded.name, username=excluded.username,
         last_seen=excluded.last_seen""", (chat_id, u.id, u.full_name, u.username, time.time()))


def drop_member(chat_id, user_id):
    q("DELETE FROM members WHERE chat_id=? AND user_id=?", (chat_id, user_id))


def extract_media(m: Message):
    """(type, file_id, mime, filename) ya None."""
    if m.photo:
        return ("photo", m.photo[-1].file_id, "image/jpeg", "photo.jpg")
    if m.video:
        return ("video", m.video.file_id, m.video.mime_type or "video/mp4", m.video.file_name or "video.mp4")
    if m.animation:
        return ("animation", m.animation.file_id, m.animation.mime_type or "video/mp4", m.animation.file_name or "anim.mp4")
    if m.voice:
        return ("voice", m.voice.file_id, m.voice.mime_type or "audio/ogg", "voice.ogg")
    if m.audio:
        return ("audio", m.audio.file_id, m.audio.mime_type or "audio/mpeg", m.audio.file_name or "audio.mp3")
    if m.video_note:
        return ("video_note", m.video_note.file_id, "video/mp4", "note.mp4")
    if m.sticker and not m.sticker.is_animated:
        return ("sticker", m.sticker.file_id, "video/webm" if m.sticker.is_video else "image/webp", "sticker.webp")
    if m.document:
        return ("document", m.document.file_id, m.document.mime_type or "application/octet-stream",
                m.document.file_name or "file")
    return None


# ---------- Bot ----------
bot = Bot(BOT_TOKEN)
dp = Dispatcher()


class Track(BaseMiddleware):
    """Group ke jis member ne bhi message kiya, use /tag ke liye yaad rakho."""
    async def __call__(self, handler, event: Message, data):
        try:
            if event.chat.type in ("group", "supergroup"):
                track_member(event.chat.id, event.from_user)
                for u in event.new_chat_members or []:
                    track_member(event.chat.id, u)
                if event.left_chat_member:
                    drop_member(event.chat.id, event.left_chat_member.id)
        except Exception as e:
            print("track error:", e)
        return await handler(event, data)


dp.message.outer_middleware(Track())


@dp.my_chat_member()
async def on_membership(ev: ChatMemberUpdated):
    joined = ev.new_chat_member.status in ("member", "administrator")
    upsert_chat(ev.chat, active=1 if joined else 0)


@dp.chat_member()  # sirf tab aata hai jab bot group mein admin ho
async def on_member_change(ev: ChatMemberUpdated):
    if ev.new_chat_member.status in ("left", "kicked"):
        drop_member(ev.chat.id, ev.new_chat_member.user.id)
    else:
        track_member(ev.chat.id, ev.new_chat_member.user)


# ----- /tag aur /stop (sirf OWNER_ID) -----
tag_tasks: dict[int, asyncio.Task] = {}


def is_owner(m: Message) -> bool:
    return bool(m.from_user and m.from_user.id in OWNER_IDS)


async def run_tag(chat_id: int, text: str):
    try:
        try:
            for a in await bot.get_chat_administrators(chat_id):
                track_member(chat_id, a.user)
        except Exception:
            pass
        rows = q("SELECT user_id,name FROM members WHERE chat_id=? ORDER BY last_seen DESC", (chat_id,), fetch=True)
        if not rows:
            await bot.send_message(chat_id, "Abhi tak kisi member ko nahi dekha. Jab wo message karenge tab tag ho sakenge.")
            return
        await bot.send_message(chat_id, f"{len(rows)} members ko tag kar raha hoon. Rokne ke liye /stop")
        for r in rows:
            msg = f'<a href="tg://user?id={r["user_id"]}">{html.escape(r["name"] or "User")}</a> {html.escape(text)}'
            while True:
                try:
                    await bot.send_message(chat_id, msg, parse_mode="HTML")
                    break
                except TelegramRetryAfter as e:
                    await asyncio.sleep(e.retry_after + 1)
            await asyncio.sleep(TAG_DELAY)
        await bot.send_message(chat_id, "Tag poora hua.")
    except asyncio.CancelledError:
        raise
    except TelegramForbiddenError:
        pass
    except Exception as e:
        print("tag error:", e)
    finally:
        tag_tasks.pop(chat_id, None)


@dp.message(Command("tag"))
async def cmd_tag(m: Message, command: CommandObject):
    if not is_owner(m):
        return
    if m.chat.type == "private":
        await m.reply("Ye command group mein use karo.")
        return
    text = (command.args or "").strip()
    if not text:
        await m.reply("Use: /tag hello")
        return
    t = tag_tasks.get(m.chat.id)
    if t and not t.done():
        await m.reply("Tag pehle se chal raha hai. Rokne ke liye /stop")
        return
    upsert_chat(m.chat)
    tag_tasks[m.chat.id] = asyncio.create_task(run_tag(m.chat.id, text))


@dp.message(Command("stop"))
async def cmd_stop(m: Message):
    if not is_owner(m):
        return
    t = tag_tasks.get(m.chat.id)
    if t and not t.done():
        t.cancel()
        await m.reply("Tagging rok di.")
    else:
        await m.reply("Abhi koi tagging nahi chal rahi.")


# ----- baaki sab messages dashboard ke liye save karo (ye handler sabse last mein) -----
@dp.message()
async def on_message(m: Message):
    upsert_chat(m.chat)
    sender = m.from_user.full_name if m.from_user else "unknown"
    if m.from_user and m.from_user.username:
        sender += f" (@{m.from_user.username})"
    media = extract_media(m)
    text = m.text or m.caption or (f"[{media[0]}]" if media else f"[{m.content_type}]")
    save_msg(m.chat.id, sender, text, "in", m.message_id, m.from_user.id if m.from_user else None, media)
    q("UPDATE chats SET unread=unread+1 WHERE chat_id=?", (m.chat.id,))


def _reply_kw(reply_to):
    if not reply_to:
        return {}
    return {"reply_parameters": ReplyParameters(message_id=reply_to, allow_sending_without_reply=True)}


async def deliver(chat_id: int, text: str, reply_to: int | None = None):
    msg = await bot.send_message(chat_id, text, **_reply_kw(reply_to))
    save_msg(chat_id, "Dashboard", text, "out", msg.message_id)


async def deliver_media(chat_id, kind, data: bytes, filename, mime, caption, reply_to, dur):
    kw = _reply_kw(reply_to)
    cap = caption or None
    f = BufferedInputFile(data, filename=filename)
    if kind == "photo":
        try:
            msg = await bot.send_photo(chat_id, f, caption=cap, **kw)
        except TelegramBadRequest:  # bahut badi / ajeeb dimensions: file bana ke bhejo
            kind = "document"
            msg = await bot.send_document(chat_id, f, caption=cap, **kw)
    elif kind == "video":
        msg = await bot.send_video(chat_id, f, caption=cap, supports_streaming=True, **kw)
    elif kind == "animation":
        msg = await bot.send_animation(chat_id, f, caption=cap, **kw)
    elif kind == "voice":
        msg = await bot.send_voice(chat_id, f, caption=cap, duration=dur or None, **kw)
    elif kind == "audio":
        msg = await bot.send_audio(chat_id, f, caption=cap, **kw)
    else:
        kind = "document"
        msg = await bot.send_document(chat_id, f, caption=cap, **kw)
    media = extract_media(msg) or (kind, None, mime, filename)
    rid = save_msg(chat_id, "Dashboard", cap or f"[{kind}]", "out", msg.message_id, None, media)
    with open(os.path.join(MEDIA_DIR, str(rid)), "wb") as fh:  # preview ke liye apni copy
        fh.write(data)
    use_mime = "audio/ogg" if kind == "voice" else (mime or media[2])
    if use_mime:
        q("UPDATE messages SET mime=? WHERE id=?", (use_mime, rid))


async def to_ogg_opus(data: bytes, mime: str) -> bytes:
    """Browser ki recording (webm/mp4) ko Telegram voice note format (OGG/OPUS) mein badlo."""
    if "ogg" in mime and "opus" in mime:
        return data
    if not shutil.which("ffmpeg"):
        raise HTTPException(500, "Server par ffmpeg install nahi hai")
    with tempfile.TemporaryDirectory() as d:
        src, dst = os.path.join(d, "in"), os.path.join(d, "out.ogg")
        with open(src, "wb") as fh:
            fh.write(data)
        p = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-loglevel", "error", "-i", src, "-vn", "-c:a", "libopus",
            "-b:a", "32k", "-ar", "48000", "-ac", "1", "-application", "voip", dst,
            stderr=asyncio.subprocess.PIPE)
        _, err = await p.communicate()
        if p.returncode != 0:
            raise HTTPException(400, "Voice convert nahi hui: " + err.decode()[-200:])
        with open(dst, "rb") as fh:
            return fh.read()


# ---------- Avatars (DP) ----------
AV_TTL = 6 * 3600
_av_locks: dict[int, asyncio.Lock] = {}


def _fresh(path, ttl=AV_TTL):
    return os.path.exists(path) and time.time() - os.path.getmtime(path) < ttl


def av_forget(eid):
    for ext in ("jpg", "none"):
        try:
            os.remove(os.path.join(AV_DIR, f"{eid}.{ext}"))
        except FileNotFoundError:
            pass


async def fetch_avatar(eid: int):
    """eid > 0: user/bot ki DP, eid < 0: group/channel ki photo. Disk par cache hoti hai."""
    jpg, none = os.path.join(AV_DIR, f"{eid}.jpg"), os.path.join(AV_DIR, f"{eid}.none")
    if _fresh(jpg):
        return jpg
    if _fresh(none, 1800):
        return None
    async with _av_locks.setdefault(eid, asyncio.Lock()):
        if _fresh(jpg):
            return jpg
        file_id, failed = None, False
        try:
            if eid > 0:
                ph = await bot.get_user_profile_photos(eid, limit=1)
                if ph.total_count:
                    sizes = ph.photos[0]
                    file_id = (sizes[1] if len(sizes) > 1 else sizes[0]).file_id
            else:
                ch = await bot.get_chat(eid)
                if ch.photo:
                    file_id = ch.photo.small_file_id
            if file_id:
                await bot.download(file_id, destination=jpg)
                return jpg
        except Exception as e:
            failed = True
            print("avatar fetch failed:", eid, e)
        if not failed and os.path.exists(jpg):
            os.remove(jpg)  # user ne DP hata di
        open(none, "w").close()  # 30 min tak dobara try nahi
        return jpg if os.path.exists(jpg) else None


async def tg_call(method: str, form: aiohttp.FormData | None = None):
    """Raw Bot API call (profile photo ke liye, kisi bhi aiogram version par chalta hai)."""
    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", data=form) as r:
            j = await r.json()
    if not j.get("ok"):
        raise HTTPException(400, j.get("description", "Telegram error"))
    return j


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
            bot, allowed_updates=["message", "my_chat_member", "chat_member"], handle_signals=False)),
        asyncio.create_task(keep_alive()),
    ]
    yield
    for t in tasks + list(tag_tasks.values()):
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
    return q("""SELECT id,chat_id,sender,text,direction,ts,tg_msg_id,sender_id,media_type,mime,fname
                FROM messages WHERE chat_id=? ORDER BY id DESC LIMIT 300""", (chat_id,), fetch=True)[::-1]


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


@app.post("/api/send_media", dependencies=[Depends(auth)])
async def send_media(request: Request, chat_id: int, kind: str, filename: str = "file", mime: str = "",
                     caption: str = "", reply_to: int | None = None, dur: int = 0):
    data = await request.body()  # raw file bytes
    if not data or len(data) > 50_000_000:
        raise HTTPException(400, "File khaali hai ya 50 MB se badi hai")
    if kind not in ("photo", "video", "animation", "voice", "audio", "document"):
        raise HTTPException(400, "Galat kind")
    if kind == "voice":
        data = await to_ogg_opus(data, mime)
        filename, mime = "voice.ogg", "audio/ogg"
    try:
        await deliver_media(chat_id, kind, data, filename or "file", mime, caption[:1024], reply_to, dur)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, str(e))
    return {"ok": True}


class Typing(BaseModel):
    chat_id: int
    action: str = "typing"


TYPING_OK = {"typing", "record_voice", "upload_photo", "upload_video", "upload_voice", "upload_document"}


@app.post("/api/typing", dependencies=[Depends(auth)])
async def typing(o: Typing):
    """Dashboard mein type karte waqt Telegram chat mein 'typing...' dikhao."""
    if o.action in TYPING_OK:
        try:
            await bot.send_chat_action(o.chat_id, o.action)
        except Exception:
            pass
    return {"ok": True}


def ranged(path: str, mime: str, request: Request, filename: str | None = None):
    """Range support (Safari/iOS ko audio/video ke liye chahiye)."""
    size = os.path.getsize(path)
    h = {"Accept-Ranges": "bytes", "Cache-Control": "private, max-age=3600"}
    if filename:
        h["Content-Disposition"] = "attachment; filename*=UTF-8''" + quote(filename)
    rng = request.headers.get("range")
    if rng and rng.startswith("bytes="):
        try:
            a, b = rng[6:].split(",")[0].split("-")
            if a == "":
                start, end = max(0, size - int(b)), size - 1
            else:
                start, end = int(a), (int(b) if b else size - 1)
            end = min(end, size - 1, start + 2 * 1024 * 1024 - 1)
            if start > end:
                raise ValueError
        except ValueError:
            return Response(status_code=416, headers={"Content-Range": f"bytes */{size}"})
        with open(path, "rb") as fh:
            fh.seek(start)
            body = fh.read(end - start + 1)
        h["Content-Range"] = f"bytes {start}-{end}/{size}"
        return Response(body, status_code=206, headers=h, media_type=mime)
    return FileResponse(path, media_type=mime, headers=h)


@app.get("/api/media/{mid}", dependencies=[Depends(auth)])
async def media(mid: int, request: Request, dl: int = 0):
    rows = q("SELECT media_type,file_id,mime,fname FROM messages WHERE id=?", (mid,), fetch=True)
    if not rows or not rows[0]["media_type"]:
        raise HTTPException(404, "no media")
    r = rows[0]
    path = os.path.join(MEDIA_DIR, str(mid))
    if not os.path.exists(path):
        if not r["file_id"]:
            raise HTTPException(404, "file nahi mili")
        try:
            await bot.download(r["file_id"], destination=path)
        except Exception:
            if os.path.exists(path):
                os.remove(path)
            raise HTTPException(413, "Telegram se download nahi hui (20 MB se badi ho sakti hai)")
    return ranged(path, r["mime"] or "application/octet-stream", request, r["fname"] if dl else None)


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


# ---------- Avatars API ----------
@app.get("/api/avatar/{eid}", dependencies=[Depends(auth)])
async def avatar(eid: int):
    p = await fetch_avatar(eid)
    if not p:
        raise HTTPException(404, "no photo", headers={"Cache-Control": "private, max-age=600"})
    return FileResponse(p, media_type="image/jpeg", headers={"Cache-Control": "private, max-age=3600"})


# ---------- Bot profile (naam, bio, photo) ----------
@app.get("/api/bot", dependencies=[Depends(auth)])
async def bot_info():
    me = await bot.get_me()
    return {
        "id": me.id,
        "username": me.username,
        "name": (await bot.get_my_name()).name,
        "about": (await bot.get_my_short_description()).short_description,
        "description": (await bot.get_my_description()).description,
    }


class Profile(BaseModel):
    name: str | None = None
    about: str | None = None        # short description, max 120
    description: str | None = None  # max 512


@app.post("/api/bot/profile", dependencies=[Depends(auth)])
async def set_profile(p: Profile):
    try:
        if p.name is not None and p.name.strip():
            await bot.set_my_name(p.name.strip()[:64])
        if p.about is not None:
            await bot.set_my_short_description(p.about[:120])
        if p.description is not None:
            await bot.set_my_description(p.description[:512])
    except Exception as e:
        raise HTTPException(400, str(e))
    return {"ok": True}


@app.post("/api/bot/photo", dependencies=[Depends(auth)])
async def set_photo(request: Request):
    data = await request.body()  # browser se square JPEG aata hai
    if not data or len(data) > 10_000_000:
        raise HTTPException(400, "Photo khaali ya bahut badi hai")
    form = aiohttp.FormData()
    form.add_field("photo", json.dumps({"type": "static", "photo": "attach://pic"}))
    form.add_field("pic", data, filename="pic.jpg", content_type="image/jpeg")
    await tg_call("setMyProfilePhoto", form)
    av_forget((await bot.get_me()).id)
    return {"ok": True}


@app.post("/api/bot/photo/remove", dependencies=[Depends(auth)])
async def remove_photo():
    await tg_call("removeMyProfilePhoto")
    av_forget((await bot.get_me()).id)
    return {"ok": True}
