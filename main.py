import asyncio, glob, hashlib, html, json, os, re, secrets, shutil, tempfile, time, urllib.request
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
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
from pymongo import ReturnDocument

load_dotenv()
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASS = os.environ["ADMIN_PASS"]
DB_PATH = os.environ.get("DB_PATH", "data.db")
MONGO_URI = os.environ["MONGO_URI"]
MONGO_DB = os.environ.get("MONGO_DB", "tgbot")
TAG_DELAY = float(os.environ.get("TAG_DELAY", "3.2"))  # group limit ~20 msg/min
PING_URL = os.environ.get("PING_URL") or os.environ.get("RENDER_EXTERNAL_URL", "")
PING_EVERY = int(os.environ.get("PING_EVERY", "600"))

BASE_DIR = os.path.dirname(os.path.abspath(DB_PATH))
AV_DIR = os.path.join(BASE_DIR, "avatars")
MEDIA_DIR = os.path.join(BASE_DIR, "media")
os.makedirs(AV_DIR, exist_ok=True)
os.makedirs(MEDIA_DIR, exist_ok=True)
STK_DIR = os.path.join(BASE_DIR, "stk")
os.makedirs(STK_DIR, exist_ok=True)

# ---------- Database (MongoDB: restart par chat history bachi rehti hai) ----------
mongo = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=15000)
mdb = mongo[MONGO_DB]
C_CHATS, C_MSGS, C_MEM, C_CNT, C_PACKS = (mdb[n] for n in ("chats", "messages", "members", "counters", "packs"))
BOT_ME = {"id": 0, "name": "Bot", "username": ""}  # lifespan mein bhara jaata hai


async def init_db():
    await C_CHATS.create_index("chat_id", unique=True)
    await C_MSGS.create_index("id", unique=True)
    await C_MSGS.create_index([("chat_id", 1), ("id", -1)])
    await C_MSGS.create_index([("chat_id", 1), ("tg_msg_id", 1)])
    await C_MSGS.create_index([("media_type", 1), ("id", -1)])
    await C_MEM.create_index([("chat_id", 1), ("user_id", 1)], unique=True)
    await C_PACKS.create_index("name", unique=True)


async def next_id() -> int:
    d = await C_CNT.find_one_and_update({"_id": "msg"}, {"$inc": {"n": 1}}, upsert=True,
                                        return_document=ReturnDocument.AFTER)
    return d["n"]


async def upsert_chat(chat, active=1):
    title = chat.title or " ".join(filter(None, [chat.first_name, chat.last_name])) or str(chat.id)
    await C_CHATS.update_one(
        {"chat_id": chat.id},
        {"$set": {"type": chat.type, "title": title, "username": chat.username, "active": active,
                  "updated": time.time()}, "$setOnInsert": {"unread": 0}}, upsert=True)


async def save_msg(chat_id, sender, text, direction, tg_id, sender_id=None, media=None, reply=None):
    mt, fid, mime, fname = media or (None, None, None, None)
    now = time.time()
    doc = {"id": await next_id(), "chat_id": chat_id, "sender": sender, "text": text, "direction": direction,
           "ts": now, "tg_msg_id": tg_id, "sender_id": sender_id, "media_type": mt, "file_id": fid,
           "mime": mime, "fname": fname}
    if reply:
        doc.update(reply)  # reply_id, reply_who, reply_text
    await C_MSGS.insert_one(doc)
    await C_CHATS.update_one({"chat_id": chat_id}, {"$set": {"updated": now, "last": text, "last_ts": now}})
    return doc["id"]


def reply_info(m: Message):
    """Telegram message ne kisko reply kiya: (id, kiska, kya likha)."""
    r = m.reply_to_message
    if not r or r.forum_topic_created:
        return None
    who = r.from_user.full_name if r.from_user else (r.sender_chat.title if r.sender_chat else "unknown")
    mt = extract_media(r)
    txt = r.text or r.caption or (f"[{mt[0]}]" if mt else f"[{r.content_type}]")
    return {"reply_id": r.message_id, "reply_who": who, "reply_text": txt[:200]}


def is_mention(m: Message) -> bool:
    """Group mein bot ko @mention kiya ya bot ke message ka reply diya."""
    if m.chat.type == "private":
        return False
    r = m.reply_to_message
    if r and r.from_user and r.from_user.id == BOT_ME["id"]:
        return True
    text = m.text or m.caption or ""
    for e in (m.entities or m.caption_entities or []):
        if e.type == "mention" and e.extract_from(text).lower() == "@" + BOT_ME["username"]:
            return True
        if e.type == "text_mention" and e.user and e.user.id == BOT_ME["id"]:
            return True
    return False


async def quote_for(chat_id, reply_to):
    """Dashboard se reply bhejte waqt original message ka quote nikalo."""
    if not reply_to:
        return None
    d = await C_MSGS.find_one({"chat_id": chat_id, "tg_msg_id": reply_to}, {"sender": 1, "text": 1, "direction": 1})
    if not d:
        return {"reply_id": reply_to, "reply_who": "", "reply_text": ""}
    who = BOT_ME["name"] if d["direction"] == "out" else (d["sender"] or "").split(" (@")[0]
    return {"reply_id": reply_to, "reply_who": who, "reply_text": (d["text"] or "")[:200]}


async def track_member(chat_id, u):
    if not u or u.is_bot:
        return
    await C_MEM.update_one({"chat_id": chat_id, "user_id": u.id},
                           {"$set": {"name": u.full_name, "username": u.username, "last_seen": time.time()}},
                           upsert=True)


async def drop_member(chat_id, user_id):
    await C_MEM.delete_one({"chat_id": chat_id, "user_id": user_id})


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
    if m.sticker:
        st = m.sticker
        mime = "application/x-tgsticker" if st.is_animated else ("video/webm" if st.is_video else "image/webp")
        return ("sticker", st.file_id, mime, "sticker.tgs" if st.is_animated else "sticker.webp")
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
                await track_member(event.chat.id, event.from_user)
                for u in event.new_chat_members or []:
                    await track_member(event.chat.id, u)
                if event.left_chat_member:
                    await drop_member(event.chat.id, event.left_chat_member.id)
        except Exception as e:
            print("track error:", e)
        return await handler(event, data)


dp.message.outer_middleware(Track())


@dp.my_chat_member()
async def on_membership(ev: ChatMemberUpdated):
    joined = ev.new_chat_member.status in ("member", "administrator")
    await upsert_chat(ev.chat, active=1 if joined else 0)


@dp.chat_member()  # sirf tab aata hai jab bot group mein admin ho
async def on_member_change(ev: ChatMemberUpdated):
    if ev.new_chat_member.status in ("left", "kicked"):
        await drop_member(ev.chat.id, ev.new_chat_member.user.id)
    else:
        await track_member(ev.chat.id, ev.new_chat_member.user)


# ----- /tag aur /stop (sirf group admin) -----
tag_tasks: dict[int, asyncio.Task] = {}


async def is_owner(m: Message) -> bool:
    """OWNER_ID ki jagah: group ka admin/creator (ya anonymous admin) hi /tag aur /stop chala sakta hai."""
    if m.chat.type == "private" or not m.from_user:
        return False
    if m.sender_chat and m.sender_chat.id == m.chat.id:
        return True
    try:
        cm = await bot.get_chat_member(m.chat.id, m.from_user.id)
        return cm.status in ("creator", "administrator")
    except Exception:
        return False


async def run_tag(chat_id: int, text: str):
    try:
        try:
            for a in await bot.get_chat_administrators(chat_id):
                await track_member(chat_id, a.user)
        except Exception:
            pass
        rows = await C_MEM.find({"chat_id": chat_id}, {"_id": 0}).sort("last_seen", -1).to_list(100000)
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
    if m.chat.type == "private":
        await m.reply("Ye command group mein use karo.")
        return
    if not await is_owner(m):
        return
    text = (command.args or "").strip()
    if not text:
        await m.reply("Use: /tag hello")
        return
    t = tag_tasks.get(m.chat.id)
    if t and not t.done():
        await m.reply("Tag pehle se chal raha hai. Rokne ke liye /stop")
        return
    await upsert_chat(m.chat)
    tag_tasks[m.chat.id] = asyncio.create_task(run_tag(m.chat.id, text))


@dp.message(Command("stop"))
async def cmd_stop(m: Message):
    if not await is_owner(m):
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
    await upsert_chat(m.chat)
    sender = m.from_user.full_name if m.from_user else "unknown"
    if m.from_user and m.from_user.username:
        sender += f" (@{m.from_user.username})"
    media = extract_media(m)
    text = m.text or m.caption or (f"[{media[0]}]" if media else f"[{m.content_type}]")
    ment = is_mention(m)
    extra = reply_info(m) or {}
    if ment:
        extra["mention"] = 1
    await save_msg(m.chat.id, sender, text, "in", m.message_id, m.from_user.id if m.from_user else None, media,
                   extra or None)
    await C_CHATS.update_one({"chat_id": m.chat.id}, {"$inc": {"unread": 1, "mentions": 1 if ment else 0}})


def _reply_kw(reply_to):
    if not reply_to:
        return {}
    return {"reply_parameters": ReplyParameters(message_id=reply_to, allow_sending_without_reply=True)}


async def deliver(chat_id: int, text: str, reply_to: int | None = None):
    msg = await bot.send_message(chat_id, text, **_reply_kw(reply_to))
    await save_msg(chat_id, BOT_ME["name"], text, "out", msg.message_id, BOT_ME["id"], None,
                   await quote_for(chat_id, reply_to))


async def deliver_sticker(chat_id: int, file_id: str, reply_to: int | None = None):
    msg = await bot.send_sticker(chat_id, file_id, **_reply_kw(reply_to))
    media = extract_media(msg) or ("sticker", file_id, "image/webp", "sticker.webp")
    await save_msg(chat_id, BOT_ME["name"], "[sticker]", "out", msg.message_id, BOT_ME["id"], media,
                   await quote_for(chat_id, reply_to))


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
    rid = await save_msg(chat_id, BOT_ME["name"], cap or f"[{kind}]", "out", msg.message_id, BOT_ME["id"], media,
                         await quote_for(chat_id, reply_to))
    with open(os.path.join(MEDIA_DIR, str(rid)), "wb") as fh:  # preview ke liye apni copy
        fh.write(data)
    use_mime = "audio/ogg" if kind == "voice" else (mime or media[2])
    if use_mime:
        await C_MSGS.update_one({"id": rid}, {"$set": {"mime": use_mime}})


def fx_filter(fx: str) -> str:
    """Voice changer: ffmpeg audio filter (pitch badalne ke saath speed wapas theek karte hain)."""
    pitch = {"deep": 0.78, "female": 1.22, "child": 1.45, "giant": 0.62}
    if fx in pitch:
        f = pitch[fx]
        return f"aresample=48000,asetrate={int(48000 * f)},aresample=48000,atempo={1 / f:.4f}"
    if fx == "robot":
        return "afftfilt=real='hypot(re,im)*sin(0)':imag='hypot(re,im)*cos(0)':win_size=512:overlap=0.75"
    if fx == "echo":
        return "aecho=0.8:0.88:60:0.4"
    return ""


async def to_ogg_opus(data: bytes, mime: str, fx: str = "") -> bytes:
    """Browser ki recording (webm/mp4) ko Telegram voice note format (OGG/OPUS) mein badlo."""
    af = fx_filter(fx)
    if "ogg" in mime and "opus" in mime and not af:
        return data
    if not shutil.which("ffmpeg"):
        raise HTTPException(500, "Server par ffmpeg install nahi hai")
    with tempfile.TemporaryDirectory() as d:
        src, dst = os.path.join(d, "in"), os.path.join(d, "out.ogg")
        with open(src, "wb") as fh:
            fh.write(data)
        p = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-loglevel", "error", "-i", src, "-vn", *(["-af", af] if af else []), "-c:a", "libopus",
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
    await init_db()
    me = await bot.get_me()
    BOT_ME.update(id=me.id, name=me.first_name or me.username or "Bot", username=(me.username or "").lower())
    tasks = [
        asyncio.create_task(dp.start_polling(
            bot, allowed_updates=["message", "my_chat_member", "chat_member"], handle_signals=False)),
        asyncio.create_task(keep_alive()),
    ]
    yield
    for t in tasks + list(tag_tasks.values()):
        t.cancel()
    await bot.session.close()
    mongo.close()


app = FastAPI(lifespan=lifespan)


@app.get("/health", response_class=PlainTextResponse)
def health():  # login ke bina, ping ke liye
    return "ok"


@app.get("/", dependencies=[Depends(auth)])
def index():
    return FileResponse("static/index.html")


@app.get("/api/chats", dependencies=[Depends(auth)])
async def chats():
    return await C_CHATS.find({}, {"_id": 0}).sort("updated", -1).to_list(2000)


@app.get("/api/messages/{chat_id}", dependencies=[Depends(auth)])
async def messages(chat_id: int):
    await C_CHATS.update_one({"chat_id": chat_id}, {"$set": {"unread": 0}})
    rows = await C_MSGS.find({"chat_id": chat_id}, {"_id": 0, "file_id": 0}).sort("id", -1).limit(300).to_list(300)
    return rows[::-1]


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
                     caption: str = "", reply_to: int | None = None, dur: int = 0, fx: str = ""):
    data = await request.body()  # raw file bytes
    if not data or len(data) > 50_000_000:
        raise HTTPException(400, "File khaali hai ya 50 MB se badi hai")
    if kind not in ("photo", "video", "animation", "voice", "audio", "document"):
        raise HTTPException(400, "Galat kind")
    if kind == "voice":
        data = await to_ogg_opus(data, mime, fx)
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
    r = await C_MSGS.find_one({"id": mid}, {"_id": 0})
    if not r or not r.get("media_type"):
        raise HTTPException(404, "no media")
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
    rows = await C_CHATS.find({"active": 1}, {"_id": 0, "chat_id": 1, "type": 1}).to_list(100000)
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
            await C_CHATS.update_one({"chat_id": r["chat_id"]}, {"$set": {"active": 0}})
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


# ---------- Stickers ----------
MIME_BY_EXT = {".webp": "image/webp", ".webm": "video/webm", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
               ".png": "image/png"}


@app.get("/api/tgfile/{fid}", dependencies=[Depends(auth)])
async def tgfile(fid: str):
    """Kisi bhi Telegram file_id ko download karke cache karo (sticker preview ke liye)."""
    key = hashlib.md5(fid.encode()).hexdigest()
    hit = glob.glob(os.path.join(STK_DIR, key + ".*"))
    if hit:
        path = hit[0]
    else:
        try:
            f = await bot.get_file(fid)
            path = os.path.join(STK_DIR, key + (os.path.splitext(f.file_path or "")[1] or ".bin"))
            await bot.download_file(f.file_path, destination=path)
        except Exception as e:
            raise HTTPException(404, str(e))
    return FileResponse(path, media_type=MIME_BY_EXT.get(os.path.splitext(path)[1], "application/octet-stream"),
                        headers={"Cache-Control": "private, max-age=86400"})


def pack_name(s: str) -> str:
    m = re.search(r"addstickers/([A-Za-z0-9_]+)", s)
    return m.group(1) if m else s.strip().lstrip("@")


def _stk_item(st):
    plain = not (st.is_animated or st.is_video)
    prev = st.file_id if plain else (st.thumbnail.file_id if st.thumbnail else st.file_id)
    return {"file_id": st.file_id, "emoji": st.emoji or "", "preview": prev}


@app.get("/api/sticker_set", dependencies=[Depends(auth)])
async def sticker_set(name: str):
    try:
        st = await bot.get_sticker_set(pack_name(name))
    except Exception:
        raise HTTPException(404, "Ye sticker pack nahi mila")
    await C_PACKS.update_one({"name": st.name}, {"$set": {"name": st.name, "title": st.title, "t": time.time()}},
                             upsert=True)
    return {"name": st.name, "title": st.title, "stickers": [_stk_item(x) for x in st.stickers]}


@app.delete("/api/sticker_pack", dependencies=[Depends(auth)])
async def del_pack(name: str):
    await C_PACKS.delete_one({"name": name})
    return {"ok": True}


@app.get("/api/stickers", dependencies=[Depends(auth)])
async def stickers():
    """Saved packs + chats mein jo stickers aaye/gaye unki recent list."""
    packs = await C_PACKS.find({}, {"_id": 0, "name": 1, "title": 1}).sort("t", -1).to_list(60)
    seen, recent = set(), []
    async for d in C_MSGS.find({"media_type": "sticker", "file_id": {"$ne": None}},
                               {"_id": 0, "file_id": 1, "mime": 1}).sort("id", -1).limit(400):
        if d["file_id"] in seen:
            continue
        seen.add(d["file_id"])
        recent.append({"file_id": d["file_id"], "emoji": "", "preview": d["file_id"]})
        if len(recent) >= 60:
            break
    return {"packs": packs, "recent": recent}


class Stk(BaseModel):
    chat_id: int
    file_id: str
    reply_to: int | None = None


@app.post("/api/send_sticker", dependencies=[Depends(auth)])
async def send_sticker(o: Stk):
    try:
        await deliver_sticker(o.chat_id, o.file_id, o.reply_to)
    except Exception as e:
        raise HTTPException(400, str(e))
    return {"ok": True}


# ---------- Dashboard se /tag ----------
class TagReq(BaseModel):
    chat_id: int
    text: str


@app.post("/api/tag", dependencies=[Depends(auth)])
async def api_tag(o: TagReq):
    text = o.text.strip()
    if not text:
        raise HTTPException(400, "Tag ke saath kuch likho")
    t = tag_tasks.get(o.chat_id)
    if t and not t.done():
        raise HTTPException(400, "Tag pehle se chal raha hai")
    tag_tasks[o.chat_id] = asyncio.create_task(run_tag(o.chat_id, text))
    return {"ok": True}


@app.post("/api/tag/stop", dependencies=[Depends(auth)])
async def api_tag_stop(chat_id: int):
    t = tag_tasks.get(chat_id)
    if t and not t.done():
        t.cancel()
    return {"ok": True}


@app.get("/api/tag/status", dependencies=[Depends(auth)])
async def api_tag_status(chat_id: int):
    t = tag_tasks.get(chat_id)
    return {"running": bool(t and not t.done())}


# ---------- Mentions ----------
def _unread_mentions(chat_id):
    return {"chat_id": chat_id, "mention": 1, "mention_read": {"$ne": 1}}


@app.get("/api/mentions", dependencies=[Depends(auth)])
async def mentions(chat_id: int):
    rows = await C_MSGS.find(_unread_mentions(chat_id), {"_id": 0, "id": 1, "tg_msg_id": 1, "sender": 1}) \
        .sort("id", 1).to_list(1000)
    by = {}
    for r in rows:
        n = (r["sender"] or "").split(" (@")[0] or "?"
        by[n] = by.get(n, 0) + 1
    return {"count": len(rows), "next": rows[0] if rows else None,
            "by": [{"name": k, "count": v} for k, v in sorted(by.items(), key=lambda x: -x[1])]}


class MRead(BaseModel):
    chat_id: int
    id: int


@app.post("/api/mention_read", dependencies=[Depends(auth)])
async def mention_read(o: MRead):
    await C_MSGS.update_one({"id": o.id, "chat_id": o.chat_id}, {"$set": {"mention_read": 1}})
    n = await C_MSGS.count_documents(_unread_mentions(o.chat_id))
    await C_CHATS.update_one({"chat_id": o.chat_id}, {"$set": {"mentions": n}})
    return {"ok": True, "left": n}


# ---------- User / group info ----------
@app.get("/api/user/{uid}", dependencies=[Depends(auth)])
async def user_info(uid: int, chat_id: int | None = None):
    out = {"id": uid}
    mem = await C_MEM.find_one({"user_id": uid}, {"_id": 0}, sort=[("last_seen", -1)])
    if mem:
        out.update(name=mem.get("name"), username=mem.get("username"), last_seen=mem.get("last_seen"))
    ch = await C_CHATS.find_one({"chat_id": uid}, {"_id": 0, "title": 1, "username": 1})
    if ch:
        out["name"] = out.get("name") or ch.get("title")
        out["username"] = out.get("username") or ch.get("username")
    try:
        c = await bot.get_chat(uid)
        out["name"] = " ".join(filter(None, [c.first_name, c.last_name])) or out.get("name")
        out["username"] = c.username or out.get("username")
        out["bio"] = getattr(c, "bio", None)
    except Exception:
        pass
    q = {"sender_id": uid}
    if chat_id and chat_id != uid:
        q["chat_id"] = chat_id
        try:
            cm = await bot.get_chat_member(chat_id, uid)
            out["status"] = cm.status
            out["title"] = getattr(cm, "custom_title", None)
            out["is_bot"] = cm.user.is_bot
            out["name"] = out.get("name") or cm.user.full_name
            out["username"] = out.get("username") or cm.user.username
        except Exception:
            pass
    out["messages"] = await C_MSGS.count_documents(q)
    return out


@app.get("/api/group/{chat_id}", dependencies=[Depends(auth)])
async def group_info(chat_id: int):
    out = {"chat_id": chat_id, "members": None, "admins": []}
    try:
        c = await bot.get_chat(chat_id)
        out.update(title=c.title, type=c.type, username=c.username, description=getattr(c, "description", None))
    except Exception:
        pass
    try:
        out["members"] = await bot.get_chat_member_count(chat_id)  # privacy ho tab bhi count milta hai
    except Exception:
        pass
    try:
        adm = await bot.get_chat_administrators(chat_id)
        out["admins"] = [{"id": a.user.id, "name": a.user.full_name, "username": a.user.username,
                          "status": a.status, "title": getattr(a, "custom_title", None), "is_bot": a.user.is_bot}
                         for a in adm]
    except Exception:
        pass
    out["seen"] = await C_MEM.count_documents({"chat_id": chat_id})
    out["messages"] = await C_MSGS.count_documents({"chat_id": chat_id})
    return out
