import concurrent.futures
import asyncio
import io
import hashlib
import ipaddress
import json
import logging
import mimetypes
import os
import re
import socket
import shutil
import tempfile
import html
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse

import boto3
import certifi
import requests
from botocore.config import Config
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from pymongo import MongoClient, DESCENDING
from pymongo.server_api import ServerApi
import yt_dlp
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError
from requests_toolbelt.multipart.encoder import MultipartEncoder
from telethon import TelegramClient, utils as telethon_utils
from telethon.sessions import StringSession

try:
    import imageio_ffmpeg
except Exception:
    imageio_ffmpeg = None

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fattle-downloader")

ROLE = os.getenv("DOWNLOADER_ROLE", "worker").strip().lower()
COORDINATOR_ENABLED = ROLE in {"coordinator", "coordinator_worker", "all"}
API_SECRET = os.getenv("DOWNLOADER_API_SECRET", "").strip()
WORKER_SECRET = os.getenv("WORKER_SECRET", "").strip()
WORKER_URLS = [
    os.getenv("WORKER_1", "").strip().rstrip("/"),
    os.getenv("WORKER_2", "").strip().rstrip("/"),
    os.getenv("WORKER_3", "").strip().rstrip("/"),
    os.getenv("WORKER_4", "").strip().rstrip("/"),
]
WORKER_URLS = [x for x in WORKER_URLS if x]

MONGODB_URI = os.getenv("MONGODB_URI", "").strip()
MONGODB_DB = os.getenv("MONGODB_DB", "fattle_downloader").strip()

STORAGE_ENDPOINT = os.getenv("STORAGE_ENDPOINT", "").strip()
STORAGE_BUCKET = os.getenv("STORAGE_BUCKET", "").strip()
STORAGE_ACCESS_KEY = os.getenv("STORAGE_ACCESS_KEY", "").strip()
STORAGE_SECRET_KEY = os.getenv("STORAGE_SECRET_KEY", "").strip()
STORAGE_REGION = os.getenv("STORAGE_REGION", "auto").strip() or "auto"

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ARCHIVE_CHAT_ID = os.getenv("ARCHIVE_CHAT_ID", "").strip()

# Normal Bot API for small-file upload and Telegram server-side copyMessage.
TELEGRAM_DIRECT_MAX_BYTES = int(os.getenv("TELEGRAM_DIRECT_MAX_BYTES", "49000000"))

# MTProto for larger archive uploads.
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0") or "0")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "").strip()
TELEGRAM_MTPROTO_MAX_BYTES = int(os.getenv("TELEGRAM_MTPROTO_MAX_BYTES", "1900000000"))
MTPROTO_R2_BUFFER_BYTES = max(
    1024 * 1024,
    min(32 * 1024 * 1024, int(os.getenv("MTPROTO_R2_BUFFER_BYTES", str(8 * 1024 * 1024))))
)

R2_LINK_TTL_SECONDS = max(300, min(604800, int(os.getenv("R2_LINK_TTL_SECONDS", "86400"))))
ARCHIVE_LARGE_LINKS = os.getenv("ARCHIVE_LARGE_LINKS", "true").strip().lower() not in {"0", "false", "no", "off"}
DELETE_R2_AFTER_TELEGRAM_ARCHIVE = os.getenv("DELETE_R2_AFTER_TELEGRAM_ARCHIVE", "false").strip().lower() in {"1", "true", "yes", "on"}
R2_FALLBACK_LINKS = os.getenv("R2_FALLBACK_LINKS", "false").strip().lower() in {"1", "true", "yes", "on"}

MAX_FILE_BYTES = int(os.getenv("MAX_FILE_BYTES", "1900000000"))
MAX_REDIRECTS = max(1, min(10, int(os.getenv("MAX_REDIRECTS", "5"))))
DOWNLOAD_TIMEOUT = max(30, min(3600, int(os.getenv("DOWNLOAD_TIMEOUT_SECONDS", "900"))))
MIN_MULTIPART_PART = 5 * 1024 * 1024
MAX_WORKERS_PER_JOB = 4
MAX_ACTIVE_JOBS = max(1, min(16, int(os.getenv("MAX_ACTIVE_JOBS", "2"))))
_job_slots = threading.BoundedSemaphore(MAX_ACTIVE_JOBS)
YTDLP_FRAGMENT_CONCURRENCY = max(
    1, min(4, int(os.getenv("YTDLP_FRAGMENT_CONCURRENCY", "4")))
)
YTDLP_RETRIES = max(1, min(10, int(os.getenv("YTDLP_RETRIES", "5"))))

# Optional resolver providers. These are coordinator-side only; workers never
# need the provider secrets. Cobalt is intended for public/authorized media
# URLs. TeraBox Gateway is used only for public share links.
COBALT_API_URL = os.getenv("COBALT_API_URL", "").strip().rstrip("/")
COBALT_API_KEY = os.getenv("COBALT_API_KEY", "").strip()
TERABOX_API_URL = os.getenv("TERABOX_API_URL", "").strip().rstrip("/")
PROVIDER_TIMEOUT_SECONDS = max(5, min(120, int(os.getenv("PROVIDER_TIMEOUT_SECONDS", "30"))))
PROVIDER_RETRIES = max(0, min(3, int(os.getenv("PROVIDER_RETRIES", "2"))))
PROVIDER_FALLBACK_YTDLP = os.getenv("PROVIDER_FALLBACK_YTDLP", "true").strip().lower() in {"1", "true", "yes", "on"}

# Deno is installed into the project by build.sh. yt-dlp uses it for
# JavaScript challenge handling where supported (especially YouTube).
DENO_BIN = os.getenv(
    "DENO_BIN",
    str((Path(__file__).resolve().parent / ".deno" / "bin" / "deno").resolve()),
).strip()

YOUTUBE_HOSTS = {"youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be", "music.youtube.com"}
MEDIA_HOST_SUFFIXES = (
    "youtube.com",
    "youtu.be",
    "tiktok.com",
    "tiktokv.com",
    "instagram.com",
    "facebook.com",
    "fb.watch",
    "twitter.com",
    "x.com",
    "reddit.com",
    "redd.it",
    "vimeo.com",
    "soundcloud.com",
)
TERABOX_HINTS = (
    "terabox", "1024tera", "nephobox", "4funbox", "mirrobox",
    "terafileshare", "terasharefile", "terasharelink",
)

app = FastAPI(title="Fattle Downloader", version="1.7-multi-provider")

mongo = None
jobs = None
if MONGODB_URI:
    mongo = MongoClient(
        MONGODB_URI,
        connect=False,
        tls=True,
        tlsCAFile=certifi.where(),
        server_api=ServerApi("1"),
        serverSelectionTimeoutMS=7000,
        connectTimeoutMS=7000,
        socketTimeoutMS=15000,
    )
    jobs = mongo[MONGODB_DB]["download_jobs"]
    try:
        jobs.create_index("job_id", unique=True)
        jobs.create_index([("user_id", 1), ("created_at", DESCENDING)])
    except Exception:
        log.exception("MongoDB index setup failed; service can still start")


def now():
    return datetime.now(timezone.utc)


def auth_client(value):
    if not API_SECRET or value != API_SECRET:
        raise HTTPException(status_code=401, detail="Invalid downloader secret")


def auth_worker(value):
    if not WORKER_SECRET or value != WORKER_SECRET:
        raise HTTPException(status_code=401, detail="Invalid worker secret")


def require_storage():
    missing = [k for k, v in {
        "STORAGE_ENDPOINT": STORAGE_ENDPOINT,
        "STORAGE_BUCKET": STORAGE_BUCKET,
        "STORAGE_ACCESS_KEY": STORAGE_ACCESS_KEY,
        "STORAGE_SECRET_KEY": STORAGE_SECRET_KEY,
    }.items() if not v]
    if missing:
        raise RuntimeError("Missing storage variables: " + ", ".join(missing))


def s3():
    require_storage()
    return boto3.client(
        "s3",
        endpoint_url=STORAGE_ENDPOINT,
        aws_access_key_id=STORAGE_ACCESS_KEY,
        aws_secret_access_key=STORAGE_SECRET_KEY,
        region_name=STORAGE_REGION,
        config=Config(signature_version="s3v4", retries={"max_attempts": 3, "mode": "standard"}),
    )


def telegram_api(method, *, data=None, files=None, timeout=(15, 180)):
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not configured on the coordinator")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    r = requests.post(url, data=data, files=files, timeout=timeout)
    try:
        payload = r.json()
    except Exception:
        payload = {"ok": False, "description": r.text[:500]}
    if r.status_code >= 400 or not payload.get("ok"):
        raise RuntimeError(f"Telegram {method} failed: {payload.get('description') or r.text[:300]}")
    return payload.get("result")


_status_push_state = {}
_status_push_lock = threading.Lock()


def _progress_bar(percent, width=12):
    percent = max(0, min(100, int(percent or 0)))
    filled = int(round(width * percent / 100))
    return "▓" * filled + "░" * (width - filled)


def _status_message_text(doc):
    status = str(doc.get("status") or "queued").replace("_", " ").upper()
    progress = int(doc.get("progress") or 0)
    filename = str(doc.get("filename") or "Preparing…")
    size = int(doc.get("size") or 0)
    quality = str(doc.get("requested_quality") or "best")
    workers = int(doc.get("worker_count") or 0)
    fragments = int(doc.get("fragment_workers") or 0)
    if size >= 1024**3:
        size_text = f"{size / (1024**3):.2f} GB"
    elif size:
        size_text = f"{size / (1024**2):.1f} MB"
    else:
        size_text = "—"
    worker_text = str(workers or "—")
    if fragments:
        worker_text = f"{workers or 1} media worker • {fragments} fragments"
    lines = [
        "📥 <b>Download Status</b>", "",
        f"📄 <code>{html.escape(filename[:180])}</code>",
        f"🎚 Quality: <b>{html.escape(quality)}</b>",
        f"📊 Status: <b>{html.escape(status)}</b>",
        f"⏳ <code>{_progress_bar(progress)}</code> <b>{progress}%</b>",
        f"📦 Size: <b>{size_text}</b>",
        f"⚡ Workers: <b>{html.escape(worker_text)}</b>",
    ]
    tg = doc.get("telegram_upload_progress")
    if str(doc.get("status") or "") == "uploading_telegram" and tg is not None:
        tg = max(0, min(100, int(tg)))
        lines += ["", "☁️ <b>Uploading to Telegram</b>", f"<code>{_progress_bar(tg)}</code> <b>{tg}%</b>"]
    delivery = str(doc.get("delivery_mode") or "")
    if delivery in {"telegram_archive_copy", "telegram_mtproto_archive_copy"}:
        lines += ["", "✅ <b>File sent in Telegram</b>"]
    elif str(doc.get("delivery_status") or "") == "failed":
        err = str(doc.get("delivery_error") or doc.get("telegram_mtproto_error") or "")
        lines += ["", "❌ <b>Telegram delivery failed</b>"]
        if err:
            lines.append(f"<code>{html.escape(err[:350])}</code>")
    if doc.get("error"):
        lines += ["", f"❌ <code>{html.escape(str(doc.get('error'))[:350])}</code>"]
    return "\
".join(lines)


def maybe_push_status(job_id, force=False):
    if jobs is None or not BOT_TOKEN:
        return
    try:
        doc = jobs.find_one({"job_id": job_id}, {"_id": 0})
    except Exception:
        return
    if not doc:
        return
    chat_id = doc.get("chat_id")
    message_id = doc.get("status_message_id")
    if not chat_id or not message_id:
        return
    progress = int(doc.get("progress") or 0)
    tg = int(doc.get("telegram_upload_progress") or -1)
    status = str(doc.get("status") or "")
    signature = (status, progress // 4, tg // 4, str(doc.get("delivery_status") or ""))
    with _status_push_lock:
        if not force and _status_push_state.get(job_id) == signature:
            return
        _status_push_state[job_id] = signature
    body = _status_message_text(doc)
    def _edit():
        try:
            telegram_api("editMessageText", data={
                "chat_id": str(int(chat_id)), "message_id": str(int(message_id)),
                "text": body, "parse_mode": "HTML", "disable_web_page_preview": "true",
            }, timeout=(10, 30))
        except Exception as exc:
            if "message is not modified" not in str(exc).lower():
                log.warning("Could not update Telegram download status: %s", exc)
    threading.Thread(target=_edit, daemon=True).start()


def telegram_post_form(method, encoder, *, timeout=(15, 300)):
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not configured on the coordinator")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    headers = {"Content-Type": encoder.content_type}
    r = requests.post(url, data=encoder, headers=headers, timeout=timeout)
    try:
        payload = r.json()
    except Exception:
        payload = {"ok": False, "description": r.text[:500]}
    if r.status_code >= 400 or not payload.get("ok"):
        raise RuntimeError(f"Telegram {method} failed: {payload.get('description') or r.text[:300]}")
    return payload.get("result")


def generate_download_url(key, expires=None):
    ttl = int(expires or R2_LINK_TTL_SECONDS)
    return s3().generate_presigned_url(
        "get_object",
        Params={"Bucket": STORAGE_BUCKET, "Key": key},
        ExpiresIn=ttl,
    )



class R2BufferedReader(io.RawIOBase):
    """Seekable buffered reader backed by R2/S3 Range requests."""

    def __init__(self, client, bucket, key, total_size, *, buffer_bytes=8 * 1024 * 1024, name="download.bin"):
        super().__init__()
        self.client = client
        self.bucket = bucket
        self.key = key
        self.total_size = int(total_size)
        self.buffer_bytes = int(buffer_bytes)
        self._pos = 0
        self._buffer = b""
        self._buffer_start = -1
        self.name = str(name or "download.bin")
        self.mode = "rb"

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self._pos

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            new_pos = int(offset)
        elif whence == io.SEEK_CUR:
            new_pos = self._pos + int(offset)
        elif whence == io.SEEK_END:
            new_pos = self.total_size + int(offset)
        else:
            raise ValueError("Invalid whence")
        if new_pos < 0:
            raise ValueError("Negative seek position")
        self._pos = min(new_pos, self.total_size)
        return self._pos

    def _ensure_buffer(self):
        if self._pos >= self.total_size:
            self._buffer = b""
            self._buffer_start = self._pos
            return
        if self._buffer and self._buffer_start <= self._pos < self._buffer_start + len(self._buffer):
            return
        start = self._pos
        end = min(self.total_size - 1, start + self.buffer_bytes - 1)
        obj = self.client.get_object(
            Bucket=self.bucket,
            Key=self.key,
            Range=f"bytes={start}-{end}",
        )
        try:
            data = obj["Body"].read()
        finally:
            try:
                obj["Body"].close()
            except Exception:
                pass
        if not data:
            raise IOError(f"R2 returned no data for bytes {start}-{end}")
        self._buffer = data
        self._buffer_start = start

    def read(self, size=-1):
        if self._pos >= self.total_size:
            return b""
        if size is None or int(size) < 0:
            size = min(self.buffer_bytes, self.total_size - self._pos)
        else:
            size = min(int(size), self.total_size - self._pos)

        out = bytearray()
        while len(out) < size and self._pos < self.total_size:
            self._ensure_buffer()
            offset = self._pos - self._buffer_start
            available = len(self._buffer) - offset
            take = min(size - len(out), available)
            if take <= 0:
                self._buffer = b""
                continue
            out.extend(self._buffer[offset:offset + take])
            self._pos += take
        return bytes(out)


def telegram_archive_missing():
    missing = []
    if not BOT_TOKEN:
        missing.append("BOT_TOKEN")
    if not ARCHIVE_CHAT_ID:
        missing.append("ARCHIVE_CHAT_ID")
    if TELEGRAM_API_ID <= 0:
        missing.append("TELEGRAM_API_ID")
    if not TELEGRAM_API_HASH:
        missing.append("TELEGRAM_API_HASH")
    return missing


def mtproto_configured():
    return not telegram_archive_missing()


async def _resolve_archive_entity(client):
    wanted = int(ARCHIVE_CHAT_ID)
    async for dialog in client.iter_dialogs():
        try:
            if int(telethon_utils.get_peer_id(dialog.entity)) == wanted:
                return dialog.entity
        except Exception:
            continue
    raise RuntimeError(
        "Archive channel was not found. Add the bot as an admin of the private "
        "channel and allow it to post messages."
    )


async def _archive_large_mtproto_async(job_id, key, filename, content_type, size):
    client = TelegramClient(
        StringSession(),
        TELEGRAM_API_ID,
        TELEGRAM_API_HASH,
        connection_retries=5,
        request_retries=5,
    )
    await client.start(bot_token=BOT_TOKEN)
    reader = None
    try:
        archive_entity = await _resolve_archive_entity(client)
        reader = R2BufferedReader(
            s3(),
            STORAGE_BUCKET,
            key,
            int(size),
            buffer_bytes=MTPROTO_R2_BUFFER_BYTES,
            name=filename,
        )

        size_mb = int(size) / (1024 * 1024)
        size_text = f"{size_mb / 1024:.2f} GB" if size_mb >= 1024 else f"{size_mb:.1f} MB"
        caption = (
            "✅ Download complete\n\n"
            f"📄 {filename}\n"
            f"📦 {size_text}"
        )[:1024]

        last_report = {"percent": -1}

        def progress(current, total):
            total = max(1, int(total or size))
            percent = min(100, int(int(current) * 100 / total))
            if percent >= last_report["percent"] + 2 or percent == 100:
                last_report["percent"] = percent
                set_job(
                    job_id,
                    status="uploading_telegram",
                    telegram_upload_progress=percent,
                )

        uploaded = await client.upload_file(
            reader,
            file_size=int(size),
            file_name=filename,
            part_size_kb=512,
            progress_callback=progress,
        )
        message = await client.send_file(
            archive_entity,
            uploaded,
            caption=caption,
            force_document=True,
        )
        return int(message.id)
    finally:
        try:
            if reader is not None:
                reader.close()
        except Exception:
            pass
        await client.disconnect()


def archive_large_mtproto(job_id, chat_id, key, filename, content_type, size):
    if not mtproto_configured():
        raise RuntimeError(
            "MTProto is not configured. Set TELEGRAM_API_ID and TELEGRAM_API_HASH on Render #1."
        )

    message_id = asyncio.run(
        _archive_large_mtproto_async(
            job_id,
            key,
            filename,
            content_type,
            int(size),
        )
    )

    copied = telegram_api(
        "copyMessage",
        data={
            "chat_id": str(int(chat_id)),
            "from_chat_id": str(ARCHIVE_CHAT_ID),
            "message_id": str(message_id),
        },
        timeout=(15, 90),
    )

    if DELETE_R2_AFTER_TELEGRAM_ARCHIVE:
        try:
            s3().delete_object(Bucket=STORAGE_BUCKET, Key=key)
        except Exception:
            log.exception("Could not delete MTProto-archived R2 object %s", key)

    return {
        "delivery_mode": "telegram_mtproto_archive_copy",
        "archive_message_id": message_id,
        "user_message_id": int((copied or {}).get("message_id") or 0),
        "telegram_upload_progress": 100,
    }


def archive_small_file(job_id, chat_id, key, filename, content_type, size):
    if not ARCHIVE_CHAT_ID:
        raise RuntimeError("ARCHIVE_CHAT_ID is not configured")
    client = s3()
    with tempfile.NamedTemporaryFile(prefix=f"archive-{job_id}-", suffix="-" + Path(filename).name, delete=True) as tmp:
        client.download_fileobj(STORAGE_BUCKET, key, tmp)
        tmp.flush()
        tmp.seek(0)
        size_mb = int(size) / (1024 * 1024)
        size_text = f"{size_mb:.1f} MB"
        caption = (
            "✅ Download complete\n\n"
            f"📄 {filename}\n"
            f"📦 {size_text}"
        )
        encoder = MultipartEncoder(fields={
            "chat_id": str(ARCHIVE_CHAT_ID),
            "caption": caption[:1024],
            "document": (filename, tmp, content_type or "application/octet-stream"),
        })
        result = telegram_post_form("sendDocument", encoder, timeout=(15, 600))

    message_id = int(result.get("message_id"))
    document = result.get("document") or {}
    file_id = str(document.get("file_id") or "")
    copy = telegram_api(
        "copyMessage",
        data={
            "chat_id": str(int(chat_id)),
            "from_chat_id": str(ARCHIVE_CHAT_ID),
            "message_id": str(message_id),
        },
        timeout=(15, 60),
    )
    if DELETE_R2_AFTER_TELEGRAM_ARCHIVE:
        try:
            client.delete_object(Bucket=STORAGE_BUCKET, Key=key)
        except Exception:
            log.exception("Could not delete archived R2 object %s", key)
    return {
        "delivery_mode": "telegram_archive_copy",
        "archive_message_id": message_id,
        "telegram_file_id": file_id,
        "user_message_id": int((copy or {}).get("message_id") or 0),
    }


def deliver_large_link(job_id, chat_id, key, filename, content_type, size):
    url = generate_download_url(key)
    size_mb = int(size) / (1024 * 1024)
    size_text = f"{size_mb / 1024:.2f} GB" if size_mb >= 1024 else f"{size_mb:.1f} MB"
    body = (
        "✅ <b>Download complete</b>\n\n"
        f"📄 <code>{html.escape(filename)}</code>\n"
        f"📦 <b>{size_text}</b>\n\n"
        "This file is larger than the normal Telegram Bot API upload limit, "
        "so use the temporary download button below."
    )
    markup = json.dumps({"inline_keyboard": [[{"text": "⬇️ Download File", "url": url}]]})

    archive_message_id = 0
    if ARCHIVE_CHAT_ID and ARCHIVE_LARGE_LINKS:
        try:
            archived = telegram_api(
                "sendMessage",
                data={
                    "chat_id": str(ARCHIVE_CHAT_ID),
                    "text": (
                        "📦 <b>Large Downloader Archive</b>\n\n"
                        f"📄 <code>{html.escape(filename)}</code>\n"
                        f"📦 {size_text}\n"
                        f"🆔 <code>{job_id}</code>\n"
                        f"🗄 <code>{html.escape(key)}</code>"
                    ),
                    "parse_mode": "HTML",
                    "disable_web_page_preview": "true",
                },
                timeout=(15, 60),
            )
            archive_message_id = int((archived or {}).get("message_id") or 0)
        except Exception:
            log.exception("Could not write large-file metadata to archive channel")

    sent = telegram_api(
        "sendMessage",
        data={
            "chat_id": str(int(chat_id)),
            "text": body,
            "parse_mode": "HTML",
            "reply_markup": markup,
            "disable_web_page_preview": "true",
        },
        timeout=(15, 60),
    )
    return {
        "delivery_mode": "r2_temporary_link",
        "archive_message_id": archive_message_id,
        "user_message_id": int((sent or {}).get("message_id") or 0),
        "link_expires_seconds": R2_LINK_TTL_SECONDS,
    }


def deliver_completed_file(job_id, chat_id, key, filename, content_type, size):
    """Archive once in Telegram, then copyMessage to the user.

    R2 is staging/storage. User-facing R2 links are used only when
    R2_FALLBACK_LINKS=true.
    """
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not configured on Render #1")

    size = int(size)

    if size <= TELEGRAM_DIRECT_MAX_BYTES and ARCHIVE_CHAT_ID:
        try:
            result = archive_small_file(job_id, chat_id, key, filename, content_type, size)
            set_job(job_id, delivery_status="sent", **result)
            return result
        except Exception as exc:
            log.exception("Small Telegram archive upload failed")
            set_job(job_id, telegram_archive_error=str(exc)[:1000])
            if not R2_FALLBACK_LINKS:
                raise RuntimeError(f"Telegram archive upload failed: {exc}") from exc

    if size <= TELEGRAM_MTPROTO_MAX_BYTES:
        missing = telegram_archive_missing()
        if missing:
            message = "Telegram large-file archive is not configured. Missing: " + ", ".join(missing)
            set_job(job_id, delivery_status="failed", telegram_mtproto_error=message)
            if not R2_FALLBACK_LINKS:
                raise RuntimeError(message)
        else:
            try:
                set_job(job_id, status="uploading_telegram", progress=94, telegram_upload_progress=0)
                result = archive_large_mtproto(job_id, chat_id, key, filename, content_type, size)
                set_job(job_id, delivery_status="sent", **result)
                return result
            except Exception as exc:
                log.exception("MTProto archive upload failed")
                set_job(job_id, delivery_status="failed", telegram_mtproto_error=str(exc)[:1000])
                if not R2_FALLBACK_LINKS:
                    raise RuntimeError(f"Telegram large-file upload failed: {exc}") from exc

    if R2_FALLBACK_LINKS:
        result = deliver_large_link(job_id, chat_id, key, filename, content_type, size)
        set_job(job_id, delivery_status="sent", **result)
        return result

    raise RuntimeError("Telegram delivery could not be completed and R2_FALLBACK_LINKS is disabled.")


def set_job(job_id, **fields):
    fields["updated_at"] = now()
    if jobs is not None:
        jobs.update_one({"job_id": job_id}, {"$set": fields}, upsert=True)
        if any(k in fields for k in {
            "status", "progress", "filename", "size", "worker_count",
            "fragment_workers", "telegram_upload_progress",
            "delivery_status", "delivery_mode", "error",
        }):
            maybe_push_status(job_id)


def get_job(job_id):
    if jobs is None:
        return None
    doc = jobs.find_one({"job_id": job_id}, {"_id": 0})
    if doc:
        for k in ("created_at", "updated_at", "completed_at"):
            if hasattr(doc.get(k), "isoformat"):
                doc[k] = doc[k].isoformat()
    return doc


def normalize_host(host):
    return (host or "").strip(".").lower()


def validate_public_url(url):
    p = urlparse(url)
    if p.scheme not in {"http", "https"}:
        raise ValueError("Only http:// and https:// URLs are allowed")
    host = normalize_host(p.hostname)
    if not host or host == "localhost" or host.endswith(".local"):
        raise ValueError("Local/private hosts are not allowed")
    try:
        infos = socket.getaddrinfo(host, p.port or (443 if p.scheme == "https" else 80), type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise ValueError("Could not resolve source host") from exc
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if any((ip.is_private, ip.is_loopback, ip.is_link_local, ip.is_multicast, ip.is_reserved, ip.is_unspecified)):
            raise ValueError("Private/reserved network addresses are not allowed")
    return url


def safe_request(method, url, *, headers=None, stream=False, timeout=None):
    current = url
    base_host = normalize_host(urlparse(url).hostname)
    request_headers = dict(headers or {})
    for _ in range(MAX_REDIRECTS + 1):
        validate_public_url(current)
        current_headers = dict(request_headers)
        if normalize_host(urlparse(current).hostname) != base_host:
            # Provider API keys are valid only for the provider origin. Never
            # forward Authorization to an origin reached by redirect.
            current_headers.pop("Authorization", None)
        r = requests.request(
            method,
            current,
            headers=current_headers,
            stream=stream,
            timeout=timeout or (15, DOWNLOAD_TIMEOUT),
            allow_redirects=False,
        )
        if r.status_code in {301, 302, 303, 307, 308}:
            loc = r.headers.get("Location")
            r.close()
            if not loc:
                raise ValueError("Source returned a redirect without a Location header")
            current = urljoin(current, loc)
            continue
        return r, current
    raise ValueError("Too many redirects")


def source_kind(url):
    host = normalize_host(urlparse(url).hostname)

    if any(x in host for x in TERABOX_HINTS):
        return "terabox"

    if host in YOUTUBE_HOSTS or host.endswith(".youtube.com"):
        return "media"

    for suffix in MEDIA_HOST_SUFFIXES:
        if host == suffix or host.endswith("." + suffix):
            return "media"

    return "direct"


def parse_filename(response, final_url):
    cd = response.headers.get("Content-Disposition", "")
    m = re.search(r"filename\*=UTF-8''([^;]+)", cd, re.I)
    if m:
        name = unquote(m.group(1))
    else:
        m = re.search(r'filename="?([^";]+)', cd, re.I)
        name = m.group(1) if m else Path(unquote(urlparse(final_url).path)).name
    name = re.sub(r"[^A-Za-z0-9._()\- ]+", "_", name or "download.bin").strip(" .")
    return (name or "download.bin")[:180]


def probe_direct(url, extra_headers=None):
    request_headers = {"Range": "bytes=0-0", "User-Agent": "FattleDownloader/1.0"}
    request_headers.update(extra_headers or {})
    r, final_url = safe_request("GET", url, headers=request_headers, stream=True)
    try:
        content_type = (r.headers.get("Content-Type") or "application/octet-stream").split(";", 1)[0].strip()
        filename = parse_filename(r, final_url)
        total = None
        ranges = False
        if r.status_code == 206:
            cr = r.headers.get("Content-Range", "")
            m = re.match(r"bytes\s+\d+-\d+/(\d+|\*)", cr, re.I)
            if m and m.group(1).isdigit():
                total = int(m.group(1))
                ranges = True
        elif r.status_code == 200:
            cl = r.headers.get("Content-Length")
            total = int(cl) if cl and cl.isdigit() else None
            ranges = "bytes" in (r.headers.get("Accept-Ranges") or "").lower()
        else:
            raise ValueError(f"Source returned HTTP {r.status_code}")
        if not total:
            raise ValueError("The source did not provide a reliable file size")
        if total > MAX_FILE_BYTES:
            raise ValueError(f"File is too large ({total} bytes). Limit is {MAX_FILE_BYTES} bytes")
        if content_type.startswith("text/html"):
            if source_kind(url) == "terabox":
                raise ValueError(
                    "This TeraBox share page is not a direct public file URL. "
                    "This build does not bypass TeraBox login/share restrictions."
                )
            raise ValueError("MEDIA_PAGE: source is a webpage, not a direct file")
        return {"url": final_url, "size": total, "range": ranges, "filename": filename, "content_type": content_type}
    finally:
        r.close()


def choose_part_count(size, range_supported):
    if not range_supported:
        return 1
    max_parts_by_size = max(1, size // MIN_MULTIPART_PART)
    return max(1, min(MAX_WORKERS_PER_JOB, len(WORKER_URLS) or 1, max_parts_by_size))


def split_ranges(size, count):
    base = size // count
    result = []
    start = 0
    for i in range(count):
        end = size - 1 if i == count - 1 else start + base - 1
        result.append((i + 1, start, end))
        start = end + 1
    return result


class JobCreate(BaseModel):
    user_id: int
    chat_id: int
    url: str = Field(min_length=8, max_length=4096)
    quality: str = "best"
    status_message_id: int | None = None


class WorkerRange(BaseModel):
    job_id: str
    source_url: str
    storage_key: str
    upload_id: str
    part_number: int
    start: int
    end: int
    source_headers: dict[str, str] | None = None


class WorkerMedia(BaseModel):
    job_id: str
    source_url: str
    storage_key_prefix: str
    quality: str = "720p"


# Backward-compatible name for older coordinator calls.
WorkerYoutube = WorkerMedia


class CancelRequest(BaseModel):
    user_id: int


class ProbeRequest(BaseModel):
    url: str = Field(min_length=8, max_length=4096)


def worker_download_range(body: WorkerRange):
    headers = {
        "Range": f"bytes={body.start}-{body.end}",
        "User-Agent": "FattleDownloader/1.0",
        "Accept-Encoding": "identity",
    }
    headers.update(body.source_headers or {})
    response, _ = safe_request("GET", body.source_url, headers=headers, stream=True)
    try:
        if response.status_code != 206:
            raise RuntimeError(f"Range download expected HTTP 206 but received {response.status_code}")
        expected = body.end - body.start + 1
        with tempfile.NamedTemporaryFile(prefix=f"{body.job_id}-p{body.part_number}-", delete=True) as tmp:
            received = 0
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                tmp.write(chunk)
                received += len(chunk)
                if received > expected:
                    raise RuntimeError("Origin returned more bytes than requested")
            if received != expected:
                raise RuntimeError(f"Incomplete range: expected {expected}, got {received}")
            tmp.flush()
            tmp.seek(0)
            result = s3().upload_part(
                Bucket=STORAGE_BUCKET,
                Key=body.storage_key,
                UploadId=body.upload_id,
                PartNumber=body.part_number,
                Body=tmp,
                ContentLength=expected,
            )
        return {"PartNumber": body.part_number, "ETag": result["ETag"], "bytes": expected}
    finally:
        response.close()


def ffmpeg_location():
    """Return a bundled FFmpeg path when imageio-ffmpeg provides one."""
    if imageio_ffmpeg is None:
        return None
    try:
        path = imageio_ffmpeg.get_ffmpeg_exe()
        return path if path and Path(path).exists() else None
    except Exception:
        return None


def deno_location():
    """Return a usable Deno executable path, if installed."""
    configured = (DENO_BIN or "").strip()
    if configured and Path(configured).exists():
        return configured
    found = shutil.which("deno")
    return found or None


def ytdlp_js_runtime_options():
    deno = deno_location()
    if not deno:
        return {}
    return {"deno": {"path": deno}}



class ProviderError(RuntimeError):
    pass


def _provider_request(method, url, *, headers=None, json_body=None, params=None):
    """Small retry wrapper for provider APIs.

    Only network errors and 5xx responses are retried. 4xx responses (notably
    401/403/429) are returned immediately so Fattle does not hammer a provider
    that is rejecting or rate-limiting the request.
    """
    last_error = None
    attempts = PROVIDER_RETRIES + 1
    for attempt in range(attempts):
        try:
            r = requests.request(
                method,
                url,
                headers=headers or {},
                json=json_body,
                params=params,
                timeout=(10, PROVIDER_TIMEOUT_SECONDS),
                allow_redirects=False,
            )
            if r.status_code < 500:
                return r
            last_error = ProviderError(f"Provider returned HTTP {r.status_code}")
        except requests.RequestException as exc:
            last_error = ProviderError(f"Provider request failed: {exc}")
        if attempt + 1 < attempts:
            time.sleep(min(2 ** attempt, 3))
    raise last_error or ProviderError("Provider request failed")


def _cobalt_headers():
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "FattleDownloader/1.7",
    }
    if COBALT_API_KEY:
        headers["Authorization"] = f"Api-Key {COBALT_API_KEY}"
    return headers


def _cobalt_quality(quality):
    q = str(quality or "best").strip().lower()
    if q in {"audio", "audio only", "mp3", "m4a"}:
        return "max", "audio"
    if q in {"best", "max", "highest", "original"}:
        return "max", "auto"
    m = re.search(r"(144|240|360|480|720|1080|1440|2160|4320)", q)
    return (m.group(1) if m else "1080"), "auto"


def cobalt_resolve(source_url, quality):
    if not COBALT_API_URL:
        raise ProviderError("Cobalt provider is not configured")
    video_quality, download_mode = _cobalt_quality(quality)
    payload = {
        "url": source_url,
        "videoQuality": video_quality,
        "downloadMode": download_mode,
        "audioFormat": "best",
        "filenameStyle": "basic",
        "youtubeVideoContainer": "mp4",
        "localProcessing": "disabled",
    }
    r = _provider_request(
        "POST", COBALT_API_URL + "/", headers=_cobalt_headers(), json_body=payload
    )
    if r.status_code == 429:
        raise ProviderError("Cobalt is rate-limiting requests. Please try again later.")
    if r.status_code in {401, 403}:
        raise ProviderError("Cobalt rejected the API credentials or request.")
    if r.status_code >= 400:
        raise ProviderError(f"Cobalt returned HTTP {r.status_code}")
    try:
        data = r.json()
    except Exception as exc:
        raise ProviderError("Cobalt returned an invalid response") from exc

    status = str(data.get("status") or "").lower()
    if status in {"redirect", "tunnel"}:
        direct = str(data.get("url") or "").strip()
        if not direct:
            raise ProviderError("Cobalt did not return a download URL")
        validate_public_url(direct)
        source_headers = {}
        if normalize_host(urlparse(direct).hostname) == normalize_host(urlparse(COBALT_API_URL).hostname):
            source_headers = _cobalt_headers()
            source_headers.pop("Content-Type", None)
            source_headers.pop("Accept", None)
        return {
            "url": direct,
            "filename": str(data.get("filename") or "").strip() or None,
            "headers": source_headers,
            "provider": "cobalt",
            "provider_status": status,
        }

    if status == "picker":
        want_audio = _cobalt_quality(quality)[1] == "audio"
        if want_audio and data.get("audio"):
            direct = str(data.get("audio") or "").strip()
            filename = str(data.get("audioFilename") or "audio.m4a")
        else:
            items = data.get("picker") if isinstance(data.get("picker"), list) else []
            item = next((x for x in items if isinstance(x, dict) and x.get("type") in {"video", "gif"}), None)
            if item is None:
                raise ProviderError("This post contains multiple images/items; multi-item Cobalt picker downloads are not supported yet")
            direct = str(item.get("url") or "").strip()
            filename = None
        validate_public_url(direct)
        return {"url": direct, "filename": filename, "headers": {}, "provider": "cobalt", "provider_status": status}

    if status == "local-processing":
        raise ProviderError("Cobalt requires local media processing for this result")

    if status == "error":
        err = data.get("error") if isinstance(data.get("error"), dict) else {}
        code = str(err.get("code") or "unknown")
        raise ProviderError(f"Cobalt could not resolve this media ({code})")

    raise ProviderError(f"Unsupported Cobalt response: {status or 'unknown'}")


def terabox_resolve(source_url):
    if not TERABOX_API_URL:
        raise ProviderError("TeraBox provider is not configured")
    r = _provider_request(
        "GET",
        TERABOX_API_URL + "/api",
        headers={"Accept": "application/json", "User-Agent": "FattleDownloader/1.7"},
        params={"url": source_url, "resolve": "true"},
    )
    if r.status_code == 429:
        raise ProviderError("TeraBox resolver is rate-limiting requests. Please try again later.")
    if r.status_code >= 400:
        raise ProviderError(f"TeraBox resolver returned HTTP {r.status_code}")
    try:
        data = r.json()
    except Exception as exc:
        raise ProviderError("TeraBox resolver returned an invalid response") from exc
    if str(data.get("status") or "").lower() != "success":
        raise ProviderError(str(data.get("message") or "TeraBox resolver could not resolve this share"))
    files = data.get("files") if isinstance(data.get("files"), list) else []
    downloadable = [x for x in files if isinstance(x, dict) and x.get("download_link")]
    if not downloadable:
        raise ProviderError("TeraBox share contains no downloadable file")
    if len(downloadable) != 1:
        raise ProviderError("TeraBox folders/multi-file shares are not supported yet; send a single-file share")
    item = downloadable[0]
    direct = str(item.get("download_link") or "").strip()
    validate_public_url(direct)
    raw_size = item.get("size")
    size = int(raw_size) if isinstance(raw_size, (int, float)) or (isinstance(raw_size, str) and raw_size.isdigit()) else None
    return {
        "url": direct,
        "filename": str(item.get("filename") or "").strip() or None,
        "size": size,
        "headers": {},
        "provider": "terabox",
    }

def ytdlp_format(quality):
    """Prefer the requested quality, but always keep a general fallback.

    Some extractors (notably Instagram and other social sites) expose formats
    without a normal height field. A strict `height<=720` selector can therefore
    match nothing even though a perfectly downloadable format exists.
    """
    q = (quality or "720p").strip().lower()

    if q in {"audio", "audio only", "mp3", "m4a"}:
        return "bestaudio[ext=m4a]/bestaudio/best"

    if q in {"best", "max", "highest"}:
        height = None
    else:
        height = 720
        m = re.search(r"(144|240|360|480|720|1080)", q)
        if m:
            height = int(m.group(1))

    ffmpeg = ffmpeg_location()

    if ffmpeg:
        if height:
            # Requested height first, then any muxable/best format.
            return (
                f"bv*[height<={height}]+ba/"
                f"b[height<={height}]/"
                f"bv*+ba/"
                f"best"
            )
        return "bv*+ba/best"

    # Without FFmpeg, prefer a single progressive file. The final /best keeps
    # social extractors usable even when they omit width/height metadata.
    if height:
        return f"b[height<={height}]/best"
    return "best"


def friendly_ytdlp_error(message):
    raw = str(message or "")
    low = raw.lower()

    if "sign in to confirm" in low or "not a bot" in low:
        return (
            "YouTube/site verification blocked this cloud server request. "
            "The downloader does not bypass sign-in or anti-bot verification. "
            "Try again later or use another public/authorized source."
        )
    if "private video" in low or "private" in low and "video" in low:
        return "This media is private and is not available to the public downloader."
    if "members-only" in low or "premium" in low:
        return "This media requires account or premium access and is not supported."
    if "copyright" in low and "unavailable" in low:
        return "This media is unavailable from the source."
    if "unexpected response from webpage request" in low and "tiktok" in low:
        return (
            "TikTok did not return a usable public media response to this server. "
            "The current TikTok extractor or the cloud-server request may be rejected. "
            "Try again later or use another public source."
        )
    if "http error 429" in low or "too many requests" in low:
        return (
            "The source rate-limited this server (HTTP 429). "
            "Please wait and try again later."
        )
    if "requested format is not available" in low:
        return (
            "The source did not expose a downloadable format for this media. "
            "Try Best quality or another public media URL."
        )
    if "unsupported url" in low:
        return "This website or URL is not supported by the media extractor."
    if "video unavailable" in low:
        return "The media is unavailable from the source."

    # Avoid sending a huge extractor traceback back to Telegram.
    cleaned = re.sub(r"\s+", " ", raw).strip()
    return cleaned[:600] or "The media extractor could not download this URL."


def worker_download_media(body: WorkerMedia):
    validate_public_url(body.source_url)

    with tempfile.TemporaryDirectory(prefix=f"media-{body.job_id}-") as td:
        outtmpl = str(Path(td) / "%(title).120B-%(id)s.%(ext)s")
        ffmpeg = ffmpeg_location()

        progress_state = {"percent": -1}

        def media_progress(d):
            try:
                state = str(d.get("status") or "")
                if state == "downloading":
                    downloaded = int(d.get("downloaded_bytes") or 0)
                    total = int(d.get("total_bytes") or d.get("total_bytes_estimate") or 0)
                    if total > 0:
                        percent = min(84, 5 + int(79 * downloaded / total))
                    else:
                        percent = max(5, progress_state["percent"])
                    fields = {
                        "status": "downloading",
                        "progress": percent,
                        "fragment_workers": YTDLP_FRAGMENT_CONCURRENCY,
                    }
                    info = d.get("info_dict") or {}
                    candidate_name = d.get("filename") or info.get("_filename")
                    if candidate_name:
                        fields["filename"] = Path(str(candidate_name)).name
                    if total > 0:
                        fields["size"] = total
                    if percent >= progress_state["percent"] + 2:
                        progress_state["percent"] = percent
                        set_job(body.job_id, **fields)
                elif state == "finished":
                    set_job(body.job_id, status="processing", progress=86, fragment_workers=YTDLP_FRAGMENT_CONCURRENCY)
            except Exception:
                pass

        opts = {
            "format": ytdlp_format(body.quality),
            "outtmpl": outtmpl,
            "noplaylist": True,
            "quiet": True,
            "no_warnings": True,
            "restrictfilenames": True,
            "socket_timeout": 30,
            "retries": YTDLP_RETRIES,
            "fragment_retries": YTDLP_RETRIES,
            "extractor_retries": min(3, YTDLP_RETRIES),
            "continuedl": True,
            "concurrent_fragment_downloads": YTDLP_FRAGMENT_CONCURRENCY,
            "overwrites": True,
            "progress_hooks": [media_progress],
        }

        js_runtimes = ytdlp_js_runtime_options()
        if js_runtimes:
            opts["js_runtimes"] = js_runtimes

        if ffmpeg:
            opts["ffmpeg_location"] = ffmpeg
            opts["merge_output_format"] = "mp4"

        try:
            with YoutubeDL(opts) as ydl:
                info = ydl.extract_info(body.source_url, download=True)
                prepared = Path(ydl.prepare_filename(info))
        except DownloadError as exc:
            message = str(exc)
            low = message.lower()

            # Some sites expose only one unusual format or omit normal height
            # metadata. If the requested format is unavailable, retry once with
            # yt-dlp's broadest public format selection.
            if "requested format is not available" in low:
                retry_opts = dict(opts)
                retry_opts["format"] = "best"
                retry_opts.pop("merge_output_format", None)
                try:
                    with YoutubeDL(retry_opts) as ydl:
                        info = ydl.extract_info(body.source_url, download=True)
                        prepared = Path(ydl.prepare_filename(info))
                except DownloadError as retry_exc:
                    raise RuntimeError(friendly_ytdlp_error(retry_exc)) from retry_exc
            else:
                raise RuntimeError(friendly_ytdlp_error(exc)) from exc

        # The final file can differ from prepare_filename after FFmpeg merging.
        candidates = [
            p for p in Path(td).iterdir()
            if p.is_file()
            and not p.name.endswith((".part", ".ytdl"))
        ]
        if prepared.exists():
            path = prepared
        elif candidates:
            path = max(candidates, key=lambda p: p.stat().st_size)
        else:
            raise RuntimeError("The media extractor did not produce a downloadable file.")

        size = path.stat().st_size
        if size > MAX_FILE_BYTES:
            raise RuntimeError(
                f"Media result is too large ({size} bytes). Limit is {MAX_FILE_BYTES} bytes."
            )

        filename = re.sub(r"[^A-Za-z0-9._()\- ]+", "_", path.name)[:180]
        key = f"{body.storage_key_prefix}/{filename}"
        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

        s3().upload_file(
            str(path),
            STORAGE_BUCKET,
            key,
            ExtraArgs={"ContentType": content_type},
        )

        return {
            "storage_key": key,
            "filename": filename,
            "size": size,
            "content_type": content_type,
        }


# Keep old function name for compatibility with older calls.
def worker_download_youtube(body: WorkerYoutube):
    return worker_download_media(body)


def call_worker(url, path, payload):
    r = requests.post(
        url + path,
        json=payload,
        headers={"X-Worker-Secret": WORKER_SECRET},
        timeout=(15, DOWNLOAD_TIMEOUT),
    )
    if r.status_code >= 400:
        detail = r.text[:700]
        try:
            payload = r.json()
            if isinstance(payload, dict) and payload.get("detail"):
                detail = str(payload["detail"])[:700]
        except Exception:
            pass
        raise RuntimeError(f"Worker {url} failed: {detail}")
    return r.json()


def maybe_deliver(job_id, chat_id, key, filename, content_type, size):
    try:
        deliver_completed_file(job_id, chat_id, key, filename, content_type, size)
        return True
    except Exception as exc:
        log.exception("Delivery failed for job %s", job_id)
        set_job(
            job_id, status="delivery_failed", progress=95, delivery_status="failed",
            delivery_error=str(exc)[:1000], error=str(exc)[:1000],
        )
        return False


def run_resolved_direct_job(
    job_id,
    request: JobCreate,
    source_url,
    *,
    source_headers=None,
    source_type="direct",
    requested_quality="original",
    filename_hint=None,
    provider=None,
):
    probe = probe_direct(source_url, extra_headers=source_headers)
    effective_headers = dict(source_headers or {})
    if normalize_host(urlparse(probe["url"]).hostname) != normalize_host(urlparse(source_url).hostname):
        effective_headers.pop("Authorization", None)
    filename = filename_hint or probe["filename"]
    filename = re.sub(r"[^A-Za-z0-9._()\- ]+", "_", filename or probe["filename"]).strip(" .")[:180] or probe["filename"]
    set_job(
        job_id,
        status="preparing",
        source_type=source_type,
        provider=provider,
        filename=filename,
        size=probe["size"],
        content_type=probe["content_type"],
        progress=2,
        requested_quality=requested_quality,
        fragment_workers=0,
    )
    client = s3()
    key = f"jobs/{job_id}/{filename}"
    count = choose_part_count(probe["size"], probe["range"])
    upload = client.create_multipart_upload(Bucket=STORAGE_BUCKET, Key=key, ContentType=probe["content_type"])
    upload_id = upload["UploadId"]
    set_job(job_id, status="downloading", storage_key=key, worker_count=count, progress=5)
    try:
        ranges = split_ranges(probe["size"], count)
        workers = (WORKER_URLS or [os.getenv("SELF_URL", "").strip().rstrip("/")])[:count]
        if len(workers) < count or any(not x for x in workers):
            raise RuntimeError("Not enough worker URLs are configured")
        payloads = []
        for (part_number, start, end), worker in zip(ranges, workers):
            payloads.append((worker, {
                "job_id": job_id,
                "source_url": probe["url"],
                "storage_key": key,
                "upload_id": upload_id,
                "part_number": part_number,
                "start": start,
                "end": end,
                "source_headers": effective_headers,
            }))
        parts = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=count) as ex:
            futures = [ex.submit(call_worker, worker, "/worker/range", payload) for worker, payload in payloads]
            for idx, fut in enumerate(concurrent.futures.as_completed(futures), 1):
                parts.append(fut.result())
                set_job(job_id, progress=min(85, 5 + int(75 * idx / count)))
        parts.sort(key=lambda x: x["PartNumber"])
        client.complete_multipart_upload(
            Bucket=STORAGE_BUCKET,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"PartNumber": p["PartNumber"], "ETag": p["ETag"]} for p in parts]},
        )
    except Exception:
        try:
            client.abort_multipart_upload(Bucket=STORAGE_BUCKET, Key=key, UploadId=upload_id)
        except Exception:
            pass
        raise

    set_job(job_id, status="stored", progress=92, storage_key=key)
    if maybe_deliver(job_id, request.chat_id, key, filename, probe["content_type"], probe["size"]):
        set_job(job_id, status="complete", progress=100, completed_at=now())
        maybe_push_status(job_id, force=True)


def run_direct_job(job_id, request: JobCreate):
    return run_resolved_direct_job(
        job_id,
        request,
        request.url,
        source_type="direct",
        requested_quality="original",
    )


def run_cobalt_job(job_id, request: JobCreate):
    media_quality = "best" if str(request.quality or "").lower() == "original" else request.quality
    set_job(
        job_id,
        status="resolving_provider",
        source_type="media",
        provider="cobalt",
        progress=3,
        requested_quality=media_quality,
        worker_count=0,
        fragment_workers=0,
    )
    resolved = cobalt_resolve(request.url, media_quality)
    return run_resolved_direct_job(
        job_id,
        request,
        resolved["url"],
        source_headers=resolved.get("headers") or {},
        source_type="cobalt",
        requested_quality=media_quality,
        filename_hint=resolved.get("filename"),
        provider="cobalt",
    )


def run_terabox_provider_job(job_id, request: JobCreate):
    set_job(
        job_id,
        status="resolving_provider",
        source_type="terabox",
        provider="terabox",
        progress=3,
        requested_quality="original",
        worker_count=0,
        fragment_workers=0,
    )
    resolved = terabox_resolve(request.url)
    return run_resolved_direct_job(
        job_id,
        request,
        resolved["url"],
        source_headers=resolved.get("headers") or {},
        source_type="terabox",
        requested_quality="original",
        filename_hint=resolved.get("filename"),
        provider="terabox",
    )


def run_ytdlp_media_job(job_id, request: JobCreate):
    if not WORKER_URLS:
        raise RuntimeError("At least one worker URL is required for media extraction")
    media_quality = "best" if str(request.quality or "").lower() == "original" else request.quality
    set_job(
        job_id, status="downloading", source_type="media", provider="yt-dlp", progress=5,
        worker_count=1, fragment_workers=YTDLP_FRAGMENT_CONCURRENCY,
        requested_quality=media_quality,
    )
    result = call_worker(WORKER_URLS[0], "/worker/media", {
        "job_id": job_id,
        "source_url": request.url,
        "storage_key_prefix": f"jobs/{job_id}",
        "quality": media_quality,
    })
    set_job(
        job_id,
        status="stored",
        progress=92,
        storage_key=result["storage_key"],
        filename=result["filename"],
        size=result["size"],
        content_type=result["content_type"],
    )
    if maybe_deliver(
        job_id, request.chat_id, result["storage_key"], result["filename"],
        result["content_type"], result["size"],
    ):
        set_job(job_id, status="complete", progress=100, completed_at=now())
        maybe_push_status(job_id, force=True)


# Backward-compatible function name.
def run_youtube_job(job_id, request: JobCreate):
    return run_ytdlp_media_job(job_id, request)


def run_media_job(job_id, request: JobCreate):
    """Primary media route: Cobalt first when configured, yt-dlp fallback."""
    if COBALT_API_URL:
        try:
            return run_cobalt_job(job_id, request)
        except Exception as exc:
            log.warning("Cobalt provider failed for %s: %s", job_id, exc)
            set_job(job_id, provider_error=str(exc)[:500])
            if not PROVIDER_FALLBACK_YTDLP:
                raise
            set_job(job_id, status="provider_fallback", progress=4, provider="yt-dlp")
    return run_ytdlp_media_job(job_id, request)


def run_job(job_id, request: JobCreate):
    try:
        if jobs is not None:
            doc = jobs.find_one({"job_id": job_id}, {"cancel_requested": 1}) or {}
            if doc.get("cancel_requested"):
                set_job(job_id, status="cancelled")
                return
        kind = source_kind(request.url)
        if kind == "media":
            run_media_job(job_id, request)
        elif kind == "terabox":
            if TERABOX_API_URL:
                try:
                    run_terabox_provider_job(job_id, request)
                except Exception as exc:
                    log.warning("TeraBox provider failed for %s: %s", job_id, exc)
                    set_job(job_id, provider_error=str(exc)[:500])
                    # Preserve support for already-direct public TeraBox links.
                    try:
                        run_direct_job(job_id, request)
                    except Exception:
                        raise ProviderError(str(exc)) from exc
            else:
                run_direct_job(job_id, request)
        else:
            # Prefer the fast 4-worker direct-file path. If the URL is a normal
            # webpage rather than a file, route it through the media providers.
            try:
                run_direct_job(job_id, request)
            except ValueError as exc:
                msg = str(exc)
                if (
                    msg.startswith("MEDIA_PAGE:")
                    or "reliable file size" in msg.lower()
                    or "source returned http 405" in msg.lower()
                ):
                    set_job(job_id, status="preparing", progress=3, source_type="media")
                    run_media_job(job_id, request)
                else:
                    raise
    except Exception as exc:
        log.exception("Download job %s failed", job_id)
        set_job(job_id, status="failed", error=str(exc)[:1000])


def _run_admitted_job(job_id, request):
    try:
        # Status pushes must not delay the job-creation response to Vercel.
        maybe_push_status(job_id, force=True)
        run_job(job_id, request)
    except Exception as exc:
        log.exception("Admitted job failed before completion: %s", job_id)
        set_job(job_id, status="failed", error=str(exc)[:1000])
    finally:
        _job_slots.release()


@app.get("/")
@app.head("/")
def root():
    return {
        "service": "fattle-downloader", "role": ROLE, "coordinator": COORDINATOR_ENABLED,
        "mtproto": mtproto_configured(), "telegram_archive_ready": mtproto_configured(),
        "telegram_missing": telegram_archive_missing(), "r2_fallback_links": R2_FALLBACK_LINKS,
        "deno_ready": bool(deno_location()),
        "ffmpeg_ready": bool(ffmpeg_location()),
        "yt_dlp_version": getattr(getattr(yt_dlp, "version", None), "__version__", "unknown"),
        "providers": {
            "cobalt": bool(COBALT_API_URL),
            "terabox": bool(TERABOX_API_URL),
            "yt_dlp_fallback": PROVIDER_FALLBACK_YTDLP,
        },
    }


@app.get("/health")
@app.head("/health")
def health():
    return {
        "ok": True, "role": ROLE, "coordinator": COORDINATOR_ENABLED,
        "telegram_archive_ready": mtproto_configured(),
        "telegram_missing": telegram_archive_missing(),
        "deno_ready": bool(deno_location()),
        "ffmpeg_ready": bool(ffmpeg_location()),
        "yt_dlp_version": getattr(getattr(yt_dlp, "version", None), "__version__", "unknown"),
        "providers": {
            "cobalt": bool(COBALT_API_URL),
            "terabox": bool(TERABOX_API_URL),
            "yt_dlp_fallback": PROVIDER_FALLBACK_YTDLP,
        },
    }


@app.post("/worker/range")
def worker_range(body: WorkerRange, x_worker_secret: str | None = Header(default=None)):
    auth_worker(x_worker_secret)
    return worker_download_range(body)


@app.post("/worker/media")
def worker_media(body: WorkerMedia, x_worker_secret: str | None = Header(default=None)):
    auth_worker(x_worker_secret)
    try:
        return worker_download_media(body)
    except RuntimeError as exc:
        raise HTTPException(status_code=422, detail=str(exc)[:700])


@app.post("/worker/youtube")
def worker_youtube(body: WorkerYoutube, x_worker_secret: str | None = Header(default=None)):
    # Compatibility endpoint for older coordinator builds.
    auth_worker(x_worker_secret)
    try:
        return worker_download_youtube(body)
    except RuntimeError as exc:
        raise HTTPException(status_code=422, detail=str(exc)[:700])


@app.post("/api/probe")
def probe_url(body: ProbeRequest, x_downloader_secret: str | None = Header(default=None)):
    """Classify a URL before asking the Telegram user for quality."""
    auth_client(x_downloader_secret)
    if not COORDINATOR_ENABLED:
        raise HTTPException(status_code=404, detail="Coordinator API is disabled on this service")
    try:
        validate_public_url(body.url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    kind = source_kind(body.url)
    if kind == "media":
        return {
            "ok": True,
            "kind": "media",
            "source_type": "media",
            "provider": "cobalt" if COBALT_API_URL else "yt-dlp",
        }

    if kind == "terabox" and TERABOX_API_URL:
        try:
            resolved = terabox_resolve(body.url)
            # Probe only the resolved public file. The direct URL itself is not
            # returned to Vercel and will be resolved again when the job starts.
            info = probe_direct(resolved["url"], extra_headers=resolved.get("headers") or {})
            return {
                "ok": True,
                "kind": "direct",
                "source_type": "terabox",
                "provider": "terabox",
                "filename": resolved.get("filename") or info.get("filename"),
                "size": info.get("size"),
                "content_type": info.get("content_type"),
                "range": bool(info.get("range")),
                "worker_count": choose_part_count(int(info.get("size") or 0), bool(info.get("range"))),
            }
        except ProviderError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    try:
        info = probe_direct(body.url)
        return {
            "ok": True,
            "kind": "direct",
            "source_type": "direct",
            "filename": info.get("filename"),
            "size": info.get("size"),
            "content_type": info.get("content_type"),
            "range": bool(info.get("range")),
            "worker_count": choose_part_count(
                int(info.get("size") or 0), bool(info.get("range"))
            ),
        }
    except ValueError as exc:
        message = str(exc)
        low = message.lower()
        if kind == "terabox" and "terabox" in low:
            raise HTTPException(status_code=400, detail=message)
        if (
            message.startswith("MEDIA_PAGE:")
            or "reliable file size" in low
            or "http 405" in low
            or "http 403" in low
            or "http 429" in low
        ):
            return {"ok": True, "kind": "media", "source_type": "media"}
        raise HTTPException(status_code=400, detail=message)


@app.post("/api/jobs")
def create_job(body: JobCreate, x_downloader_secret: str | None = Header(default=None)):
    auth_client(x_downloader_secret)
    if not COORDINATOR_ENABLED:
        raise HTTPException(status_code=404, detail="Coordinator API is disabled on this service")
    if jobs is None:
        raise HTTPException(status_code=503, detail="MONGODB_URI is required on the coordinator")
    try:
        validate_public_url(body.url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not _job_slots.acquire(blocking=False):
        raise HTTPException(429, "Downloader is busy. Try again shortly.", headers={"Retry-After": "15"})
    job_id = uuid.uuid4().hex
    try:
        jobs.insert_one({
            "job_id": job_id,
            "user_id": int(body.user_id),
            "chat_id": int(body.chat_id),
            "source_url": body.url,
            "requested_quality": body.quality,
            "status_message_id": int(body.status_message_id) if body.status_message_id else None,
            "status": "queued",
            "progress": 0,
            "created_at": now(),
            "updated_at": now(),
            "cancel_requested": False,
        })
        response = {"ok": True, "job_id": job_id, "status": "queued", "telegram_archive_ready": mtproto_configured()}
        threading.Thread(target=_run_admitted_job, args=(job_id, body), daemon=True).start()
        return response
    except Exception:
        _job_slots.release()
        try:
            set_job(job_id, status="failed", error="Unable to start download. Please try again.")
        except Exception:
            log.exception("Unable to record job startup failure: %s", job_id)
        raise



@app.get("/api/jobs/{job_id}")
def job_status(job_id: str, x_downloader_secret: str | None = Header(default=None)):
    auth_client(x_downloader_secret)
    doc = get_job(job_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Job not found")
    return doc


@app.get("/api/users/{user_id}/recent")
def recent_jobs(user_id: int, x_downloader_secret: str | None = Header(default=None)):
    auth_client(x_downloader_secret)
    if jobs is None:
        raise HTTPException(status_code=503, detail="MongoDB is not configured")
    result = []
    for doc in jobs.find({"user_id": int(user_id)}, {"_id": 0}).sort("created_at", -1).limit(10):
        for k in ("created_at", "updated_at", "completed_at"):
            if hasattr(doc.get(k), "isoformat"):
                doc[k] = doc[k].isoformat()
        result.append(doc)
    return {"jobs": result}


@app.get("/api/jobs/{job_id}/download-link")
def job_download_link(job_id: str, x_downloader_secret: str | None = Header(default=None)):
    auth_client(x_downloader_secret)
    doc = get_job(job_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Job not found")
    key = str(doc.get("storage_key") or "")
    if not key:
        raise HTTPException(status_code=409, detail="The file is not stored yet")
    if not STORAGE_BUCKET:
        raise HTTPException(status_code=503, detail="Object storage is not configured")
    return {
        "ok": True,
        "url": generate_download_url(key),
        "expires_in": R2_LINK_TTL_SECONDS,
        "filename": doc.get("filename"),
        "size": doc.get("size"),
    }


@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str, body: CancelRequest, x_downloader_secret: str | None = Header(default=None)):
    auth_client(x_downloader_secret)
    if jobs is None:
        raise HTTPException(status_code=503, detail="MongoDB is not configured")
    result = jobs.update_one({"job_id": job_id, "user_id": int(body.user_id)}, {"$set": {"cancel_requested": True, "updated_at": now()}})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Job not found")
    # Cancellation is cooperative. Already-running HTTP range requests may finish their current part.
    return {"ok": True, "cancel_requested": True}
