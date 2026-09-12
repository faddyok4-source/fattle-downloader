import concurrent.futures
import asyncio
import io
import hashlib
import ipaddress
import json
import logging
import math
import queue
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
from telethon import TelegramClient, functions, types, utils as telethon_utils
from telethon.sessions import StringSession
from providers import ProviderError, ProviderRouter, detect_platform, ROUTER_BUILD

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
TELEGRAM_API_RETRIES = max(1, min(5, int(os.getenv("TELEGRAM_API_RETRIES", "3"))))

# Normal Bot API for small-file upload and Telegram server-side copyMessage.
TELEGRAM_DIRECT_MAX_BYTES = int(os.getenv("TELEGRAM_DIRECT_MAX_BYTES", str(50 * 1024 * 1024)))

# MTProto for larger archive uploads.
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0") or "0")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "").strip()
TELEGRAM_MTPROTO_MAX_BYTES = int(os.getenv("TELEGRAM_MTPROTO_MAX_BYTES", "1900000000"))
MTPROTO_R2_BUFFER_BYTES = max(
    1024 * 1024,
    min(32 * 1024 * 1024, int(os.getenv("MTPROTO_R2_BUFFER_BYTES", str(16 * 1024 * 1024))))
)

# True source -> Telegram pipelining for non-range/single-stream downloads.
# Telegram MTProto parts are 512 KiB maximum. The queue provides bounded
# backpressure while still letting source, R2 and Telegram overlap.
LIVE_TELEGRAM_PIPELINE = os.getenv(
    "LIVE_TELEGRAM_PIPELINE", "true"
).strip().lower() not in {"0", "false", "no", "off"}
LIVE_TELEGRAM_PART_BYTES = 512 * 1024
LIVE_TELEGRAM_MIN_BYTES = max(
    1024 * 1024,
    min(64 * 1024 * 1024, int(os.getenv("LIVE_TELEGRAM_MIN_BYTES", str(8 * 1024 * 1024))))
)
LIVE_TELEGRAM_QUEUE_PARTS = max(
    4,
    min(128, int(os.getenv("LIVE_TELEGRAM_QUEUE_PARTS", "48")))
)
LIVE_TELEGRAM_FINISH_TIMEOUT_SECONDS = max(
    60,
    min(3600, int(os.getenv("LIVE_TELEGRAM_FINISH_TIMEOUT_SECONDS", "1200")))
)

R2_LINK_TTL_SECONDS = max(300, min(604800, int(os.getenv("R2_LINK_TTL_SECONDS", "86400"))))
ARCHIVE_LARGE_LINKS = os.getenv("ARCHIVE_LARGE_LINKS", "true").strip().lower() not in {"0", "false", "no", "off"}
DELETE_R2_AFTER_TELEGRAM_ARCHIVE = os.getenv("DELETE_R2_AFTER_TELEGRAM_ARCHIVE", "false").strip().lower() in {"1", "true", "yes", "on"}
R2_FALLBACK_LINKS = os.getenv("R2_FALLBACK_LINKS", "false").strip().lower() in {"1", "true", "yes", "on"}

MAX_FILE_BYTES = int(os.getenv("MAX_FILE_BYTES", "1900000000"))
MAX_REDIRECTS = max(1, min(10, int(os.getenv("MAX_REDIRECTS", "5"))))
DOWNLOAD_TIMEOUT = max(30, min(3600, int(os.getenv("DOWNLOAD_TIMEOUT_SECONDS", "900"))))
MIN_MULTIPART_PART = 5 * 1024 * 1024
R2_STREAM_PART_BYTES = max(
    MIN_MULTIPART_PART,
    min(64 * 1024 * 1024, int(os.getenv("R2_STREAM_PART_BYTES", str(8 * 1024 * 1024))))
)

# Non-range source -> R2 pipeline.
# One source connection is still used, but R2 uploads happen concurrently so
# the source download does not pause for every multipart upload.
R2_STREAM_UPLOAD_WORKERS = max(
    1,
    min(4, int(os.getenv("R2_STREAM_UPLOAD_WORKERS", "3")))
)
R2_STREAM_INFLIGHT_PARTS = max(
    R2_STREAM_UPLOAD_WORKERS,
    min(8, int(os.getenv("R2_STREAM_INFLIGHT_PARTS", "6")))
)
MAX_WORKERS_PER_JOB = 4

# Provider/CDN range downloads can occasionally close a connection early.
# Keep each R2 multipart piece small enough that retries are cheap and reliable.
RANGE_PART_BYTES = max(
    MIN_MULTIPART_PART,
    min(64 * 1024 * 1024, int(os.getenv("RANGE_PART_BYTES", str(8 * 1024 * 1024))))
)
WORKER_RANGE_RETRIES = max(1, min(8, int(os.getenv("WORKER_RANGE_RETRIES", "4"))))
WORKER_RANGE_RETRY_BACKOFF_SECONDS = max(
    0.25,
    min(10.0, float(os.getenv("WORKER_RANGE_RETRY_BACKOFF_SECONDS", "1.0")))
)
STREAM_DOWNLOAD_RETRIES = max(1, min(5, int(os.getenv("STREAM_DOWNLOAD_RETRIES", "3"))))
WORKER_CALL_RETRIES = max(1, min(6, int(os.getenv("WORKER_CALL_RETRIES", "4"))))
WORKER_CALL_BACKOFF_SECONDS = max(0.5, min(10.0, float(os.getenv("WORKER_CALL_BACKOFF_SECONDS", "1.5"))))
MONGO_STATUS_RETRIES = max(1, min(4, int(os.getenv("MONGO_STATUS_RETRIES", "2"))))
DOWNLOADER_BUILD = "3.7-live-telegram-pipeline"

MAX_ACTIVE_JOBS = max(1, min(16, int(os.getenv("MAX_ACTIVE_JOBS", "2"))))
_job_slots = threading.BoundedSemaphore(MAX_ACTIVE_JOBS)
YTDLP_FRAGMENT_CONCURRENCY = max(
    1, min(4, int(os.getenv("YTDLP_FRAGMENT_CONCURRENCY", "4")))
)
YTDLP_RETRIES = max(1, min(10, int(os.getenv("YTDLP_RETRIES", "5"))))

# Resolver-provider settings. Provider secrets are used only by the coordinator.
PROVIDER_TIMEOUT_SECONDS = max(5, min(120, int(os.getenv("PROVIDER_TIMEOUT_SECONDS", "30"))))
PROVIDER_RETRIES = max(0, min(4, int(os.getenv("PROVIDER_RETRIES", "3"))))
PROVIDER_FALLBACK_YTDLP = os.getenv("PROVIDER_FALLBACK_YTDLP", "false").strip().lower() in {"1", "true", "yes", "on"}
PROVIDER_ROUTER = ProviderRouter.from_env(
    timeout_seconds=PROVIDER_TIMEOUT_SECONDS,
    retries=PROVIDER_RETRIES,
)

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

app = FastAPI(title="Fattle Downloader", version=DOWNLOADER_BUILD)

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
    last_error = None

    for attempt in range(1, TELEGRAM_API_RETRIES + 1):
        try:
            r = requests.post(url, data=data, files=files, timeout=timeout)
        except requests.RequestException as exc:
            last_error = RuntimeError(f"Telegram {method} connection failed: {exc}")
            if attempt >= TELEGRAM_API_RETRIES:
                raise last_error from exc
            time.sleep(min(6.0, 1.5 * attempt))
            continue

        try:
            try:
                payload = r.json()
            except Exception:
                payload = {"ok": False, "description": r.text[:500]}

            if r.status_code < 400 and payload.get("ok"):
                return payload.get("result")

            description = payload.get("description") or r.text[:300]
            last_error = RuntimeError(f"Telegram {method} failed: {description}")
            retryable = r.status_code == 429 or r.status_code >= 500
            if not retryable or attempt >= TELEGRAM_API_RETRIES:
                raise last_error

            retry_after = 0
            params = payload.get("parameters") if isinstance(payload, dict) else None
            if isinstance(params, dict):
                try:
                    retry_after = int(params.get("retry_after") or 0)
                except Exception:
                    retry_after = 0
            time.sleep(min(15.0, max(float(retry_after), 1.5 * attempt)))
        finally:
            try:
                r.close()
            except Exception:
                pass

    raise last_error or RuntimeError(f"Telegram {method} failed")


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
    downloaded_bytes = int(doc.get("downloaded_bytes") or 0)
    quality = str(doc.get("requested_quality") or "best")
    workers = int(doc.get("worker_count") or 0)
    fragments = int(doc.get("fragment_workers") or 0)
    if size >= 1024**3:
        size_text = f"{size / (1024**3):.2f} GB"
    elif size:
        size_text = f"{size / (1024**2):.1f} MB"
    elif downloaded_bytes:
        size_text = f"{downloaded_bytes / (1024**2):.1f} MB downloaded"
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
    return "\n".join(lines)


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


TELEGRAM_INLINE_VIDEO_EXTENSIONS = {".mp4", ".m4v"}


def is_telegram_inline_video(filename, content_type=None):
    """Return True for video files that Telegram can normally show inline."""
    ext = Path(str(filename or "")).suffix.lower()
    mime = str(content_type or "").split(";", 1)[0].strip().lower()
    return ext in TELEGRAM_INLINE_VIDEO_EXTENSIONS or mime == "video/mp4"


async def _resolve_archive_entity(client):
    """Resolve the private archive channel without enumerating bot dialogs."""
    if not ARCHIVE_CHAT_ID:
        raise RuntimeError("ARCHIVE_CHAT_ID is not configured")

    wanted = int(ARCHIVE_CHAT_ID)

    try:
        # Telethon special-cases bot accounts here and can resolve a numeric
        # channel ID through channels.GetChannelsRequest without GetDialogs.
        return await client.get_input_entity(wanted)
    except Exception as exc:
        raise RuntimeError(
            "Could not resolve ARCHIVE_CHAT_ID with the bot account. "
            "Make sure this same bot is in the private archive channel, is an "
            "administrator, and can post messages. "
            f"ARCHIVE_CHAT_ID={wanted}. Telegram error: {exc}"
        ) from exc


class LiveTelegramUploader:
    """Upload sequential source bytes to Telegram while R2 is still receiving.

    This uploader is best-effort. A Telegram-side failure must never destroy the
    source/R2 download; callers can disable it and fall back to the normal
    R2 -> Telegram delivery path.
    """

    _STOP = object()

    def __init__(self, job_id, filename, content_type, exact_size):
        self.job_id = str(job_id)
        self.filename = str(filename or "download.bin")
        self.content_type = str(content_type or "application/octet-stream")
        self.size = int(exact_size)
        if self.size <= 0:
            raise ValueError("Live Telegram upload requires an exact positive size")

        self.part_size = LIVE_TELEGRAM_PART_BYTES
        self.total_parts = int(math.ceil(self.size / self.part_size))
        self.is_big = self.size > 10 * 1024 * 1024

        # Positive signed 63-bit random ID is valid for Telegram's `long`.
        self.file_id = int.from_bytes(os.urandom(8), "little") & ((1 << 63) - 1)
        if self.file_id == 0:
            self.file_id = 1

        self._queue = queue.Queue(maxsize=LIVE_TELEGRAM_QUEUE_PARTS)
        self._buffer = bytearray()
        self._source_bytes = 0
        self._queued_parts = 0
        self._abort = threading.Event()
        self._done = threading.Event()
        self._closed = False
        self._error = None
        self._result = None

        self._thread = threading.Thread(
            target=self._thread_main,
            name=f"tg-live-{self.job_id[:8]}",
            daemon=True,
        )

    @property
    def error(self):
        return self._error

    @property
    def result(self):
        return self._result

    @property
    def alive(self):
        return self._thread.is_alive() and not self._done.is_set()

    def start(self):
        self._thread.start()
        set_job(
            self.job_id,
            telegram_live_pipeline=True,
            telegram_live_total_bytes=self.size,
            telegram_upload_progress=0,
        )
        return self

    def _put(self, item):
        while not self._abort.is_set():
            if self._done.is_set():
                return False
            try:
                self._queue.put(item, timeout=0.5)
                return True
            except queue.Full:
                try:
                    raise_if_cancelled(self.job_id)
                except Exception:
                    self.abort()
                    raise
                continue
        return False

    def feed(self, data):
        """Feed sequential bytes. Returns False if Telegram pipeline has failed."""
        if self._closed or self._abort.is_set() or self._done.is_set():
            return False
        if self._error is not None:
            return False
        if not data:
            return True

        data = bytes(data)
        self._source_bytes += len(data)
        if self._source_bytes > self.size:
            self._error = RuntimeError(
                f"Live Telegram source exceeded exact size: {self._source_bytes}>{self.size}"
            )
            self.abort()
            return False

        self._buffer.extend(data)
        while len(self._buffer) >= self.part_size:
            payload = bytes(self._buffer[:self.part_size])
            del self._buffer[:self.part_size]
            index = self._queued_parts
            if index >= self.total_parts:
                self._error = RuntimeError("Too many Telegram file parts were produced")
                self.abort()
                return False
            if not self._put((index, payload)):
                return False
            self._queued_parts += 1

        return self._error is None

    def close_input(self):
        """Signal source EOF without waiting for Telegram finalization."""
        if self._closed:
            return
        self._closed = True

        if self._source_bytes != self.size:
            self._error = RuntimeError(
                f"Live Telegram source size mismatch: expected {self.size}, got {self._source_bytes}"
            )
            self.abort()
            return

        if self._buffer:
            index = self._queued_parts
            if index >= self.total_parts:
                self._error = RuntimeError("Unexpected final Telegram file part")
                self.abort()
                return
            if not self._put((index, bytes(self._buffer))):
                return
            self._queued_parts += 1
            self._buffer.clear()

        if self._queued_parts != self.total_parts:
            self._error = RuntimeError(
                f"Telegram part count mismatch: expected {self.total_parts}, "
                f"queued {self._queued_parts}"
            )
            self.abort()
            return

        self._put(self._STOP)

    def abort(self):
        self._abort.set()
        # Consumer uses a timed get, so it will notice the abort even if the
        # queue is full and no STOP marker can be inserted.
        try:
            self._queue.put_nowait(self._STOP)
        except Exception:
            pass

    def wait(self, timeout=None):
        timeout = (
            LIVE_TELEGRAM_FINISH_TIMEOUT_SECONDS
            if timeout is None
            else float(timeout)
        )
        if not self._done.wait(timeout):
            self.abort()
            raise RuntimeError("Timed out waiting for live Telegram upload to finish")
        if self._error is not None:
            raise RuntimeError(f"Live Telegram upload failed: {self._error}") from self._error
        if not self._result:
            raise RuntimeError("Live Telegram upload ended without an archive message")
        return dict(self._result)

    def _thread_main(self):
        try:
            asyncio.run(self._async_main())
        except Exception as exc:
            self._error = exc
            log.exception("Live Telegram pipeline failed for job %s", self.job_id)
            set_job(
                self.job_id,
                telegram_live_pipeline=False,
                telegram_live_error=str(exc)[:1000],
            )
        finally:
            self._done.set()

    async def _async_main(self):
        client = TelegramClient(
            StringSession(),
            TELEGRAM_API_ID,
            TELEGRAM_API_HASH,
            connection_retries=5,
            request_retries=5,
        )

        uploaded_bytes = 0
        uploaded_parts = 0
        hash_md5 = hashlib.md5()
        archive_message_id = 0

        await client.start(bot_token=BOT_TOKEN)
        try:
            archive_entity = await _resolve_archive_entity(client)

            while not self._abort.is_set():
                try:
                    item = await asyncio.to_thread(self._queue.get, True, 0.5)
                except queue.Empty:
                    continue

                if item is self._STOP:
                    break

                part_index, payload = item
                if part_index != uploaded_parts:
                    raise RuntimeError(
                        f"Telegram parts arrived out of order: expected {uploaded_parts}, "
                        f"got {part_index}"
                    )

                if part_index < self.total_parts - 1 and len(payload) != self.part_size:
                    raise RuntimeError(
                        f"Telegram part {part_index} has invalid size {len(payload)}"
                    )
                if len(payload) <= 0 or len(payload) > self.part_size:
                    raise RuntimeError(
                        f"Telegram part {part_index} has invalid payload size {len(payload)}"
                    )

                if self.is_big:
                    ok = await client(
                        functions.upload.SaveBigFilePartRequest(
                            file_id=self.file_id,
                            file_part=part_index,
                            file_total_parts=self.total_parts,
                            bytes=payload,
                        )
                    )
                else:
                    hash_md5.update(payload)
                    ok = await client(
                        functions.upload.SaveFilePartRequest(
                            file_id=self.file_id,
                            file_part=part_index,
                            bytes=payload,
                        )
                    )

                if not ok:
                    raise RuntimeError(f"Telegram rejected file part {part_index}")

                uploaded_parts += 1
                uploaded_bytes += len(payload)
                percent = min(99, int(uploaded_bytes * 100 / max(1, self.size)))

                # Limit Mongo writes while still showing useful live progress.
                if percent == 99 or percent % 5 == 0:
                    set_job(
                        self.job_id,
                        telegram_live_pipeline=True,
                        telegram_upload_progress=percent,
                        telegram_uploaded_bytes=uploaded_bytes,
                    )

            if self._abort.is_set():
                return

            if uploaded_parts != self.total_parts:
                raise RuntimeError(
                    f"Telegram upload ended early: {uploaded_parts}/{self.total_parts} parts"
                )
            if uploaded_bytes != self.size:
                raise RuntimeError(
                    f"Telegram upload byte mismatch: expected {self.size}, got {uploaded_bytes}"
                )

            if self.is_big:
                uploaded = types.InputFileBig(
                    self.file_id,
                    self.total_parts,
                    self.filename,
                )
            else:
                uploaded = types.InputFile(
                    self.file_id,
                    self.total_parts,
                    self.filename,
                    hash_md5.hexdigest(),
                )

            size_mb = self.size / (1024 * 1024)
            size_text = (
                f"{size_mb / 1024:.2f} GB"
                if size_mb >= 1024
                else f"{size_mb:.1f} MB"
            )
            caption = (
                "✅ Download complete\n\n"
                f"📄 {self.filename}\n"
                f"📦 {size_text}"
            )[:1024]

            inline_video = is_telegram_inline_video(
                self.filename,
                self.content_type,
            )
            message = await client.send_file(
                archive_entity,
                uploaded,
                caption=caption,
                force_document=not inline_video,
                supports_streaming=inline_video,
                mime_type=self.content_type,
            )
            archive_message_id = int(message.id)

            self._result = {
                "archive_message_id": archive_message_id,
                "delivery_media_type": "video" if inline_video else "document",
                "telegram_upload_progress": 100,
                "telegram_live_pipeline": True,
            }
            set_job(
                self.job_id,
                telegram_upload_progress=100,
                telegram_live_archive_message_id=archive_message_id,
            )

        finally:
            await client.disconnect()


def _copy_live_archive_to_user(job_id, chat_id, key, live_result):
    """Finish a successful live archive upload with server-side copyMessage."""
    message_id = int((live_result or {}).get("archive_message_id") or 0)
    if message_id <= 0:
        raise RuntimeError("Live Telegram archive message ID is missing")

    copied = telegram_api(
        "copyMessage",
        data={
            "chat_id": str(int(chat_id)),
            "from_chat_id": str(ARCHIVE_CHAT_ID),
            "message_id": str(message_id),
        },
        timeout=(15, 90),
    )

    result = {
        "delivery_mode": "telegram_live_mtproto_archive_copy",
        "delivery_media_type": str(
            (live_result or {}).get("delivery_media_type") or "document"
        ),
        "archive_message_id": message_id,
        "user_message_id": int((copied or {}).get("message_id") or 0),
        "telegram_upload_progress": 100,
        "telegram_live_pipeline": True,
    }

    set_job(job_id, delivery_status="sent", **result)

    if DELETE_R2_AFTER_TELEGRAM_ARCHIVE:
        try:
            s3().delete_object(Bucket=STORAGE_BUCKET, Key=key)
        except Exception:
            log.exception("Could not delete live-archived R2 object %s", key)

    return result


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
            if percent >= last_report["percent"] + 5 or percent == 100:
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

        inline_video = is_telegram_inline_video(filename, content_type)
        message = await client.send_file(
            archive_entity,
            uploaded,
            caption=caption,
            force_document=not inline_video,
            supports_streaming=inline_video,
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
        "delivery_media_type": "video" if is_telegram_inline_video(filename, content_type) else "document",
        "archive_message_id": message_id,
        "user_message_id": int((copied or {}).get("message_id") or 0),
        "telegram_upload_progress": 100,
    }


def archive_small_file(job_id, chat_id, key, filename, content_type, size):
    if not ARCHIVE_CHAT_ID:
        raise RuntimeError("ARCHIVE_CHAT_ID is not configured")

    client = s3()
    with tempfile.NamedTemporaryFile(
        prefix=f"archive-{job_id}-",
        suffix="-" + Path(filename).name,
        delete=True,
    ) as tmp:
        client.download_fileobj(STORAGE_BUCKET, key, tmp)
        tmp.flush()
        tmp.seek(0)

        size_mb = int(size) / (1024 * 1024)
        size_text = f"{size_mb:.1f} MB"
        caption = (
            "✅ Download complete\n\n"
            f"📄 {filename}\n"
            f"📦 {size_text}"
        )[:1024]

        inline_video = is_telegram_inline_video(filename, content_type)
        result = None
        media_type = "document"

        # sendVideo makes Telegram render an inline video player/thumbnail.
        # If Telegram rejects a particular MP4/codec, fall back safely to a
        # normal document instead of losing the delivery.
        if inline_video:
            encoder = MultipartEncoder(fields={
                "chat_id": str(ARCHIVE_CHAT_ID),
                "caption": caption,
                "supports_streaming": "true",
                "video": (filename, tmp, content_type or "video/mp4"),
            })
            try:
                result = telegram_post_form("sendVideo", encoder, timeout=(15, 600))
                media_type = "video"
            except Exception:
                log.exception("Telegram sendVideo failed; falling back to sendDocument")
                tmp.seek(0)

        if result is None:
            encoder = MultipartEncoder(fields={
                "chat_id": str(ARCHIVE_CHAT_ID),
                "caption": caption,
                "document": (filename, tmp, content_type or "application/octet-stream"),
            })
            result = telegram_post_form("sendDocument", encoder, timeout=(15, 600))

    message_id = int(result.get("message_id"))
    media_obj = result.get("video") if media_type == "video" else result.get("document")
    media_obj = media_obj or {}
    file_id = str(media_obj.get("file_id") or "")

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
        "delivery_media_type": media_type,
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
            log.exception("Small Telegram archive upload failed; trying MTProto next when available")
            set_job(job_id, telegram_archive_error=str(exc)[:1000])
            # Do not fail yet. MTProto is a second Telegram delivery path and
            # works for small files too. This avoids losing delivery because of
            # a transient Bot API upload problem.

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


class JobCancelled(RuntimeError):
    """The user cancelled a coordinator job."""


def set_job(job_id, **fields):
    """Best-effort status persistence.

    A transient MongoDB status-update failure must not destroy a file download
    that is otherwise progressing correctly. Job creation still requires MongoDB;
    updates during the job are retried briefly and then logged.
    """
    fields["updated_at"] = now()
    if jobs is None:
        return False

    updated = False
    for attempt in range(1, MONGO_STATUS_RETRIES + 1):
        try:
            jobs.update_one({"job_id": job_id}, {"$set": fields}, upsert=True)
            updated = True
            break
        except Exception as exc:
            if attempt >= MONGO_STATUS_RETRIES:
                log.warning(
                    "MongoDB status update failed for job %s after %s attempt(s): %s",
                    job_id, attempt, exc,
                )
                break
            time.sleep(0.25 * attempt)

    if updated and any(k in fields for k in {
        "status", "progress", "filename", "size", "worker_count",
        "fragment_workers", "telegram_upload_progress",
        "delivery_status", "delivery_mode", "delivery_media_type", "error",
    }):
        maybe_push_status(job_id)
    return updated


def is_cancel_requested(job_id):
    if jobs is None:
        return False
    try:
        doc = jobs.find_one({"job_id": job_id}, {"cancel_requested": 1}) or {}
        return bool(doc.get("cancel_requested"))
    except Exception as exc:
        log.warning("Could not read cancel state for job %s: %s", job_id, exc)
        return False


def raise_if_cancelled(job_id):
    if is_cancel_requested(job_id):
        raise JobCancelled("Download cancelled")


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
            # Sensitive resolver headers are valid only for the original host.
            # Never forward them to a different redirect target.
            sensitive = {
                "authorization", "cookie", "proxy-authorization",
                "x-api-key", "x-auth-token", "host",
            }
            for header_name in list(current_headers):
                if str(header_name).lower() in sensitive:
                    current_headers.pop(header_name, None)
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


def _header_int(headers, *names):
    for name in names:
        value = str(headers.get(name) or "").strip()
        if value.isdigit():
            return int(value)
    return None


def probe_direct(url, extra_headers=None, allow_unknown_size=False):
    """Probe a direct/resolved URL without consuming the full body.

    Normal direct files still require an exact size by default. Provider
    tunnels can opt into ``allow_unknown_size`` because streaming APIs such as
    Cobalt may omit Content-Length and expose Estimated-Content-Length instead.
    """
    request_headers = {
        "Range": "bytes=0-0",
        "User-Agent": "FattleDownloader/1.0",
        "Accept-Encoding": "identity",
    }
    request_headers.update(extra_headers or {})
    r, final_url = safe_request("GET", url, headers=request_headers, stream=True)
    try:
        content_type = (r.headers.get("Content-Type") or "application/octet-stream").split(";", 1)[0].strip()
        filename = parse_filename(r, final_url)
        total = None
        ranges = False
        if r.status_code == 206:
            cr = r.headers.get("Content-Range", "")
            m = re.match(r"bytes\s+(\d+)-(\d+)/(\d+|\*)", cr, re.I)
            if m and m.group(3).isdigit():
                total = int(m.group(3))

                # The probe requested exactly bytes=0-0. Some provider/CDN
                # endpoints incorrectly reply 206 while sending the entire
                # file (for example: bytes 0-50441526/50441527). Treat those
                # as NON-range sources so Fattle uses one safe stream.
                returned_start = int(m.group(1))
                returned_end = int(m.group(2))
                probe_length = _header_int(r.headers, "Content-Length")
                content_encoding = str(r.headers.get("Content-Encoding") or "").strip().lower()
                ranges = (
                    returned_start == 0
                    and returned_end == 0
                    and probe_length in {None, 1}
                    and content_encoding in {"", "identity"}
                )

                if not ranges:
                    log.info(
                        "Origin advertised/returned HTTP 206 but ignored the "
                        "0-0 range probe (%s); disabling multi-worker ranges",
                        cr[:200],
                    )
        elif r.status_code == 200:
            total = _header_int(r.headers, "Content-Length")

            # Accept-Ranges by itself is not proof. The request we just sent
            # contained Range: bytes=0-0, and HTTP 200 means the server ignored
            # it. Do not launch parallel range workers.
            ranges = False
        else:
            raise ValueError(f"Source returned HTTP {r.status_code}")

        estimated = _header_int(
            r.headers,
            "Estimated-Content-Length",
            "X-Estimated-Content-Length",
        )
        if total and total > MAX_FILE_BYTES:
            raise ValueError(f"File is too large ({total} bytes). Limit is {MAX_FILE_BYTES} bytes")
        if estimated and estimated > MAX_FILE_BYTES:
            raise ValueError(f"Estimated file size is too large ({estimated} bytes). Limit is {MAX_FILE_BYTES} bytes")
        if not total and not allow_unknown_size:
            raise ValueError("The source did not provide a reliable file size")
        if content_type.startswith("text/html"):
            if source_kind(url) == "terabox":
                raise ValueError(
                    "This TeraBox share page is not a direct public file URL. "
                    "This build does not bypass TeraBox login/share restrictions."
                )
            raise ValueError("MEDIA_PAGE: source is a webpage, not a direct file")
        return {
            "url": final_url,
            "size": total,
            "estimated_size": estimated,
            "range": bool(ranges and total),
            "filename": filename,
            "content_type": content_type,
        }
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


def split_ranges_by_chunk(size, chunk_bytes=RANGE_PART_BYTES):
    """Create R2-compatible byte ranges with small retry-friendly pieces."""
    size = int(size)
    chunk_bytes = max(MIN_MULTIPART_PART, int(chunk_bytes))
    result = []
    start = 0
    part_number = 1
    while start < size:
        end = min(size - 1, start + chunk_bytes - 1)
        result.append((part_number, start, end))
        start = end + 1
        part_number += 1
    if len(result) > 10000:
        raise RuntimeError("Too many multipart ranges")
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


class RangeNotHonoredError(RuntimeError):
    """The source ignored or changed the requested byte range."""


def worker_download_range(body: WorkerRange):
    expected = body.end - body.start + 1
    last_error = None

    for attempt in range(1, WORKER_RANGE_RETRIES + 1):
        response = None
        try:
            headers = {
                "Range": f"bytes={body.start}-{body.end}",
                "User-Agent": "FattleDownloader/3.2",
                "Accept-Encoding": "identity",
                "Connection": "close",
            }
            headers.update(body.source_headers or {})

            response, _ = safe_request(
                "GET",
                body.source_url,
                headers=headers,
                stream=True,
            )

            if response.status_code != 206:
                # If the origin stopped honoring Range, retrying the same request
                # will not help. Let the coordinator use its safer fallback.
                raise RangeNotHonoredError(
                    f"Range download expected HTTP 206 but received {response.status_code}"
                )

            content_range = str(response.headers.get("Content-Range") or "")
            if content_range and not content_range.lower().startswith(
                f"bytes {body.start}-{body.end}/".lower()
            ):
                raise RangeNotHonoredError(
                    f"Origin returned unexpected Content-Range: {content_range[:200]}"
                )

            with tempfile.NamedTemporaryFile(
                prefix=f"{body.job_id}-p{body.part_number}-",
                delete=True,
            ) as tmp:
                received = 0
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    tmp.write(chunk)
                    received += len(chunk)
                    if received > expected:
                        raise RuntimeError("Origin returned more bytes than requested")

                if received != expected:
                    raise RuntimeError(
                        f"Incomplete range: expected {expected}, got {received}"
                    )

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

            return {
                "PartNumber": body.part_number,
                "ETag": result["ETag"],
                "bytes": expected,
                "attempts": attempt,
            }

        except Exception as exc:
            last_error = exc

            # HTTP status/content-range errors are normally deterministic.
            deterministic = isinstance(exc, RangeNotHonoredError) or (
                isinstance(exc, RuntimeError)
                and "more bytes than requested" in str(exc)
            )
            if deterministic or attempt >= WORKER_RANGE_RETRIES:
                raise RuntimeError(
                    f"Range {body.part_number} failed after {attempt} attempt(s): {exc}"
                ) from exc

            delay = WORKER_RANGE_RETRY_BACKOFF_SECONDS * attempt
            log.warning(
                "Range part %s interrupted on attempt %s/%s (%s). Retrying in %.1fs",
                body.part_number,
                attempt,
                WORKER_RANGE_RETRIES,
                exc,
                delay,
            )
            time.sleep(delay)

        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:
                    pass

    raise RuntimeError(f"Range download failed: {last_error}")



class StreamIntegrityError(RuntimeError):
    """A full-file stream was partial or internally inconsistent."""


def _stream_source_to_r2_once(job_id, source_url, source_headers, storage_key, content_type, estimated_size=None, live_telegram=None):
    """Pipeline one source stream into concurrent R2 multipart uploads.

    The source may not support Range, so it is read through one safe HTTP
    connection. R2 multipart uploads run concurrently in the background,
    preventing each R2 upload from pausing the source download.
    """
    headers = {
        "User-Agent": "FattleDownloader/3.6",
        "Accept-Encoding": "identity",
    }
    headers.update(source_headers or {})
    response, final_url = safe_request("GET", source_url, headers=headers, stream=True)
    client = s3()
    upload_id = None
    received = 0
    last_reported = -1
    part_number = 1
    buffer = bytearray()
    executor = None
    pending = {}
    completed_parts = []
    live_uploader = None
    live_result = None

    try:
        if response.status_code not in {200, 206}:
            raise RuntimeError(f"Stream download returned HTTP {response.status_code}")

        response_size = _header_int(response.headers, "Content-Length")
        response_estimate = _header_int(
            response.headers,
            "Estimated-Content-Length",
            "X-Estimated-Content-Length",
        )
        content_encoding = str(response.headers.get("Content-Encoding") or "").strip().lower()
        exact_expected = None

        if response.status_code == 206:
            content_range = str(response.headers.get("Content-Range") or "")
            match = re.match(r"bytes\s+(\d+)-(\d+)/(\d+|\*)", content_range, re.I)
            if not match or not match.group(3).isdigit():
                raise StreamIntegrityError(
                    f"Full stream received incomplete HTTP 206 response: "
                    f"{content_range[:200] or 'missing Content-Range'}"
                )
            start_byte = int(match.group(1))
            end_byte = int(match.group(2))
            total_bytes = int(match.group(3))
            if start_byte != 0 or end_byte != total_bytes - 1:
                raise StreamIntegrityError(
                    f"Full stream received only bytes {start_byte}-{end_byte}/{total_bytes}"
                )
            if content_encoding not in {"", "identity"}:
                raise StreamIntegrityError("Encoded HTTP 206 response cannot be verified safely")
            if response_size is not None and response_size != total_bytes:
                raise StreamIntegrityError(
                    f"HTTP 206 length mismatch: Content-Length={response_size}, total={total_bytes}"
                )
            exact_expected = total_bytes
        elif content_encoding in {"", "identity"} and response_size is not None:
            exact_expected = response_size

        expected = exact_expected or response_estimate or estimated_size
        if expected and int(expected) > MAX_FILE_BYTES:
            raise RuntimeError(
                f"File is too large ({expected} bytes). Limit is {MAX_FILE_BYTES} bytes"
            )

        # True download + Telegram upload overlap is enabled only when HTTP
        # gives us an exact byte count. Telegram's big-file API requires the
        # total part count before upload begins. If Telegram setup fails here,
        # the R2 download continues normally and delivery falls back later.
        if (
            LIVE_TELEGRAM_PIPELINE
            and live_telegram
            and exact_expected is not None
            and int(exact_expected) >= LIVE_TELEGRAM_MIN_BYTES
            and int(exact_expected) <= TELEGRAM_MTPROTO_MAX_BYTES
            and mtproto_configured()
        ):
            try:
                live_uploader = LiveTelegramUploader(
                    job_id,
                    live_telegram.get("filename"),
                    live_telegram.get("content_type") or content_type,
                    int(exact_expected),
                ).start()
                set_job(
                    job_id,
                    telegram_live_pipeline=True,
                    telegram_live_exact_size=int(exact_expected),
                )
            except Exception as exc:
                live_uploader = None
                set_job(
                    job_id,
                    telegram_live_pipeline=False,
                    telegram_live_error=str(exc)[:1000],
                )
                log.warning(
                    "Could not start live Telegram pipeline for job %s: %s",
                    job_id,
                    exc,
                )

        upload = client.create_multipart_upload(
            Bucket=STORAGE_BUCKET,
            Key=storage_key,
            ContentType=content_type or "application/octet-stream",
        )
        upload_id = upload["UploadId"]

        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=R2_STREAM_UPLOAD_WORKERS,
            thread_name_prefix="r2-stream",
        )

        def _upload_one(number, payload):
            result = client.upload_part(
                Bucket=STORAGE_BUCKET,
                Key=storage_key,
                UploadId=upload_id,
                PartNumber=number,
                Body=payload,
                ContentLength=len(payload),
            )
            return {
                "PartNumber": number,
                "ETag": result["ETag"],
                "bytes": len(payload),
            }

        def _collect_completed(*, block=False):
            if not pending:
                return

            mode = (
                concurrent.futures.FIRST_COMPLETED
                if block
                else concurrent.futures.FIRST_COMPLETED
            )
            done, _ = concurrent.futures.wait(
                list(pending.keys()),
                timeout=None if block else 0,
                return_when=mode,
            )

            for fut in done:
                number = pending.pop(fut)
                try:
                    completed_parts.append(fut.result())
                except Exception as exc:
                    raise RuntimeError(
                        f"R2 stream part {number} upload failed: {exc}"
                    ) from exc

        def _submit_part(payload):
            nonlocal part_number
            payload = bytes(payload)
            number = part_number
            part_number += 1
            fut = executor.submit(_upload_one, number, payload)
            pending[fut] = number

            # Bound memory: if too many parts are waiting, wait only until one
            # finishes while the remaining R2 uploads continue in parallel.
            while len(pending) >= R2_STREAM_INFLIGHT_PARTS:
                raise_if_cancelled(job_id)
                _collect_completed(block=True)

        # Larger read chunks reduce Python/requests overhead while still keeping
        # responsive cancellation/status updates.
        source_read_chunk = min(4 * 1024 * 1024, R2_STREAM_PART_BYTES)

        for chunk in response.iter_content(chunk_size=source_read_chunk):
            if not chunk:
                continue

            received += len(chunk)
            if received > MAX_FILE_BYTES:
                raise RuntimeError(
                    f"File exceeded the configured limit of {MAX_FILE_BYTES} bytes"
                )

            if live_uploader is not None:
                try:
                    if not live_uploader.feed(chunk):
                        reason = live_uploader.error or "live uploader stopped"
                        log.warning(
                            "Disabling live Telegram pipeline for job %s: %s",
                            job_id,
                            reason,
                        )
                        set_job(
                            job_id,
                            telegram_live_pipeline=False,
                            telegram_live_error=str(reason)[:1000],
                        )
                        live_uploader.abort()
                        live_uploader = None
                except JobCancelled:
                    raise
                except Exception as exc:
                    log.warning(
                        "Live Telegram feed failed for job %s; continuing R2: %s",
                        job_id,
                        exc,
                    )
                    set_job(
                        job_id,
                        telegram_live_pipeline=False,
                        telegram_live_error=str(exc)[:1000],
                    )
                    try:
                        live_uploader.abort()
                    except Exception:
                        pass
                    live_uploader = None

            buffer.extend(chunk)

            while len(buffer) >= R2_STREAM_PART_BYTES:
                payload = buffer[:R2_STREAM_PART_BYTES]
                del buffer[:R2_STREAM_PART_BYTES]
                _submit_part(payload)

            # Harvest completed R2 uploads without waiting.
            _collect_completed(block=False)

            if expected:
                pct = min(
                    85,
                    5 + int(80 * min(received, int(expected)) / max(1, int(expected))),
                )
            else:
                pct = 10

            # Fewer Mongo writes: every 8 MiB instead of every 4 MiB.
            report_bucket = received // (8 * 1024 * 1024)
            if report_bucket != last_reported:
                last_reported = report_bucket
                raise_if_cancelled(job_id)
                set_job(
                    job_id,
                    progress=pct,
                    downloaded_bytes=received,
                    download_mode="single_stream_pipelined",
                    r2_upload_workers=R2_STREAM_UPLOAD_WORKERS,
                )

        if not received:
            raise RuntimeError("The source returned an empty file")

        if exact_expected is not None and received != int(exact_expected):
            raise StreamIntegrityError(
                f"Incomplete full stream: expected {int(exact_expected)} bytes, got {received}"
            )

        # Source EOF is now known-good. Let Telegram upload/send its last part
        # while R2 finishes outstanding multipart uploads.
        if live_uploader is not None:
            try:
                live_uploader.close_input()
            except Exception as exc:
                log.warning(
                    "Could not close live Telegram input for job %s: %s",
                    job_id,
                    exc,
                )
                set_job(
                    job_id,
                    telegram_live_pipeline=False,
                    telegram_live_error=str(exc)[:1000],
                )

        if buffer:
            _submit_part(buffer)

        while pending:
            raise_if_cancelled(job_id)
            _collect_completed(block=True)

        if not completed_parts:
            raise RuntimeError("The stream did not produce any upload parts")

        completed_parts.sort(key=lambda p: p["PartNumber"])

        client.complete_multipart_upload(
            Bucket=STORAGE_BUCKET,
            Key=storage_key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": p["PartNumber"], "ETag": p["ETag"]}
                    for p in completed_parts
                ]
            },
        )
        upload_id = None

        if live_uploader is not None:
            try:
                live_result = live_uploader.wait()
            except Exception as exc:
                log.warning(
                    "Live Telegram finalization failed for job %s; "
                    "normal R2 delivery will be used: %s",
                    job_id,
                    exc,
                )
                set_job(
                    job_id,
                    telegram_live_pipeline=False,
                    telegram_live_error=str(exc)[:1000],
                )
                live_result = None

        return {
            "size": received,
            "url": final_url,
            "r2_upload_workers": R2_STREAM_UPLOAD_WORKERS,
            "download_mode": "single_stream_pipelined",
            "live_telegram": live_result,
        }

    except Exception:
        if live_uploader is not None:
            try:
                live_uploader.abort()
            except Exception:
                pass

        if pending:
            for fut in pending:
                fut.cancel()

        if upload_id:
            try:
                client.abort_multipart_upload(
                    Bucket=STORAGE_BUCKET,
                    Key=storage_key,
                    UploadId=upload_id,
                )
            except Exception:
                pass
        raise

    finally:
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)
        response.close()


def stream_source_to_r2(job_id, source_url, source_headers, storage_key, content_type, estimated_size=None, live_telegram=None):
    """Retry a whole-file stream from scratch when a CDN disconnects.

    This path is used for origins that do not reliably honor byte ranges.
    Each failed attempt aborts its R2 multipart upload inside the one-shot
    helper, then the next attempt starts a fresh source connection.
    """
    last_error = None

    for attempt in range(1, STREAM_DOWNLOAD_RETRIES + 1):
        try:
            if attempt > 1:
                set_job(
                    job_id,
                    status="retrying_stream",
                    stream_attempt=attempt,
                    progress=5,
                    downloaded_bytes=0,
                )
            result = _stream_source_to_r2_once(
                job_id,
                source_url,
                source_headers,
                storage_key,
                content_type,
                estimated_size=estimated_size,
                live_telegram=live_telegram,
            )
            result["attempts"] = attempt
            return result

        except JobCancelled:
            raise
        except Exception as exc:
            last_error = exc
            if attempt >= STREAM_DOWNLOAD_RETRIES:
                break

            delay = min(6.0, 1.5 * attempt)
            log.warning(
                "Whole-file stream failed on attempt %s/%s for job %s: %s. "
                "Retrying from the beginning in %.1fs",
                attempt,
                STREAM_DOWNLOAD_RETRIES,
                job_id,
                exc,
                delay,
            )
            time.sleep(delay)

    raise RuntimeError(
        f"Whole-file stream failed after {STREAM_DOWNLOAD_RETRIES} attempt(s): {last_error}"
    ) from last_error


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
    retry_statuses = {408, 425, 429, 500, 502, 503, 504}
    last_error = None

    for attempt in range(1, WORKER_CALL_RETRIES + 1):
        response = None
        try:
            response = requests.post(
                url + path,
                json=payload,
                headers={"X-Worker-Secret": WORKER_SECRET},
                timeout=(30, DOWNLOAD_TIMEOUT),
            )

            if response.status_code < 400:
                try:
                    data = response.json()
                except Exception as exc:
                    raise RuntimeError("Worker returned a non-JSON success response") from exc
                if not isinstance(data, dict):
                    raise RuntimeError("Worker returned an invalid JSON response")
                return data

            detail = response.text[:700]
            try:
                error_payload = response.json()
                if isinstance(error_payload, dict) and error_payload.get("detail"):
                    detail = str(error_payload["detail"])[:700]
            except Exception:
                pass

            last_error = RuntimeError(f"Worker {url} failed: {detail}")
            if response.status_code not in retry_statuses or attempt >= WORKER_CALL_RETRIES:
                raise last_error

            retry_after = 0.0
            try:
                retry_after = float(response.headers.get("Retry-After") or 0)
            except Exception:
                retry_after = 0.0
            delay = max(retry_after, WORKER_CALL_BACKOFF_SECONDS * attempt)
            log.warning(
                "Worker %s returned HTTP %s on attempt %s/%s. Retrying in %.1fs",
                url, response.status_code, attempt, WORKER_CALL_RETRIES, delay,
            )
            time.sleep(min(15.0, delay))

        except requests.RequestException as exc:
            last_error = RuntimeError(f"Worker {url} connection failed: {exc}")
            if attempt >= WORKER_CALL_RETRIES:
                raise last_error from exc
            delay = WORKER_CALL_BACKOFF_SECONDS * attempt
            log.warning(
                "Worker %s connection error on attempt %s/%s: %s. Retrying in %.1fs",
                url, attempt, WORKER_CALL_RETRIES, exc, delay,
            )
            time.sleep(min(15.0, delay))
        except RuntimeError:
            raise
        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:
                    pass

    raise last_error or RuntimeError(f"Worker {url} failed")


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
    allow_unknown_size=False,
):
    raise_if_cancelled(job_id)
    probe = probe_direct(
        source_url,
        extra_headers=source_headers,
        allow_unknown_size=allow_unknown_size,
    )
    effective_headers = dict(source_headers or {})
    if normalize_host(urlparse(probe["url"]).hostname) != normalize_host(urlparse(source_url).hostname):
        effective_headers.pop("Authorization", None)
    filename = filename_hint or probe["filename"]
    filename = re.sub(r"[^A-Za-z0-9._()\- ]+", "_", filename or probe["filename"]).strip(" .")[:180] or probe["filename"]
    initial_size = int(probe.get("size") or 0)
    set_job(
        job_id,
        status="preparing",
        source_type=source_type,
        provider=provider,
        filename=filename,
        size=initial_size,
        estimated_size=int(probe.get("estimated_size") or 0),
        content_type=probe["content_type"],
        progress=2,
        requested_quality=requested_quality,
        fragment_workers=0,
    )
    client = s3()
    key = f"jobs/{job_id}/{filename}"
    live_delivery = None

    # Use the fast multi-worker path only when the origin provided an exact
    # size and actually supports byte ranges. Each request is intentionally
    # small so a transient CDN disconnect only retries a small piece.
    if probe.get("size") and probe.get("range"):
        worker_count = choose_part_count(probe["size"], True)
        workers = (WORKER_URLS or [os.getenv("SELF_URL", "").strip().rstrip("/")])[:worker_count]
        if len(workers) < worker_count or any(not x for x in workers):
            raise RuntimeError("Not enough worker URLs are configured")

        ranges = split_ranges_by_chunk(probe["size"], RANGE_PART_BYTES)

        def _download_ranges(active_workers, *, fallback_mode=False):
            upload = client.create_multipart_upload(
                Bucket=STORAGE_BUCKET,
                Key=key,
                ContentType=probe["content_type"],
            )
            upload_id = upload["UploadId"]
            concurrency = min(len(active_workers), MAX_WORKERS_PER_JOB)
            set_job(
                job_id,
                status="downloading",
                storage_key=key,
                worker_count=concurrency,
                range_parts=len(ranges),
                range_part_bytes=RANGE_PART_BYTES,
                range_fallback=fallback_mode,
                progress=5,
            )

            try:
                raise_if_cancelled(job_id)
                payloads = []
                for index, (part_number, start, end) in enumerate(ranges):
                    worker = active_workers[index % len(active_workers)]
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
                ex = concurrent.futures.ThreadPoolExecutor(max_workers=concurrency)
                futures = [
                    ex.submit(call_worker, worker, "/worker/range", payload)
                    for worker, payload in payloads
                ]
                try:
                    for idx, fut in enumerate(concurrent.futures.as_completed(futures), 1):
                        parts.append(fut.result())
                        raise_if_cancelled(job_id)
                        set_job(
                            job_id,
                            progress=min(85, 5 + int(75 * idx / max(1, len(ranges)))),
                        )
                except Exception:
                    # Do not wait for every queued range after one part already
                    # proved the strategy is broken or the user cancelled.
                    for pending in futures:
                        pending.cancel()
                    ex.shutdown(wait=False, cancel_futures=True)
                    raise
                else:
                    ex.shutdown(wait=True)

                parts.sort(key=lambda x: x["PartNumber"])
                client.complete_multipart_upload(
                    Bucket=STORAGE_BUCKET,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={
                        "Parts": [
                            {"PartNumber": p["PartNumber"], "ETag": p["ETag"]}
                            for p in parts
                        ]
                    },
                )
                return

            except Exception:
                try:
                    client.abort_multipart_upload(
                        Bucket=STORAGE_BUCKET,
                        Key=key,
                        UploadId=upload_id,
                    )
                except Exception:
                    pass
                raise

        try:
            _download_ranges(workers, fallback_mode=False)
            final_size = int(probe["size"])

        except Exception as first_exc:
            raise_if_cancelled(job_id)
            first_text = str(first_exc).lower()
            range_is_invalid = (
                "unexpected content-range" in first_text
                or "expected http 206" in first_text
                or "range download expected http 206" in first_text
            )

            sequential_error = first_exc
            if not range_is_invalid:
                # A worker may simply be sleeping/down. Try the available workers
                # one at a time before giving up on byte ranges entirely.
                set_job(
                    job_id,
                    status="retrying_download",
                    progress=5,
                    range_parallel_error=str(first_exc)[:1000],
                    worker_count=1,
                )
                for worker in workers:
                    try:
                        log.warning(
                            "Parallel range download failed for job %s. "
                            "Trying sequential ranges via %s",
                            job_id, worker,
                        )
                        _download_ranges([worker], fallback_mode=True)
                        final_size = int(probe["size"])
                        sequential_error = None
                        break
                    except JobCancelled:
                        raise
                    except Exception as exc:
                        sequential_error = exc
                        log.warning("Sequential worker %s failed for job %s: %s", worker, job_id, exc)

            if sequential_error is not None:
                # The source either does not really support Range, every worker
                # is unavailable, or the CDN changed behavior after probing.
                # A normal single GET on the coordinator is the safest fallback.
                log.warning(
                    "Range strategies exhausted for job %s: %s. "
                    "Switching to full-source streaming.",
                    job_id,
                    sequential_error,
                )
                set_job(
                    job_id,
                    status="stream_fallback",
                    progress=5,
                    worker_count=1,
                    range_fallback_error=str(sequential_error)[:1000],
                    downloaded_bytes=0,
                )
                raise_if_cancelled(job_id)
                streamed = stream_source_to_r2(
                    job_id,
                    probe["url"],
                    effective_headers,
                    key,
                    probe["content_type"],
                    estimated_size=probe.get("size") or probe.get("estimated_size"),
                    live_telegram={
                        "filename": filename,
                        "content_type": probe["content_type"],
                    },
                )
                final_size = int(streamed["size"])
                live_delivery = streamed.get("live_telegram")
                set_job(
                    job_id,
                    size=final_size,
                    downloaded_bytes=final_size,
                    progress=85,
                    stream_fallback=True,
                )
    else:
        set_job(
            job_id,
            status="downloading",
            storage_key=key,
            worker_count=1,
            progress=5,
            downloaded_bytes=0,
        )
        streamed = stream_source_to_r2(
            job_id,
            probe["url"],
            effective_headers,
            key,
            probe["content_type"],
            estimated_size=probe.get("estimated_size") or probe.get("size"),
            live_telegram={
                "filename": filename,
                "content_type": probe["content_type"],
            },
        )
        final_size = int(streamed["size"])
        live_delivery = streamed.get("live_telegram")
        set_job(job_id, size=final_size, downloaded_bytes=final_size, progress=85)

    set_job(job_id, status="stored", progress=92, storage_key=key, size=final_size)
    raise_if_cancelled(job_id)

    delivered = False
    if live_delivery:
        try:
            set_job(
                job_id,
                status="uploading_telegram",
                progress=96,
                telegram_upload_progress=100,
            )
            _copy_live_archive_to_user(
                job_id,
                request.chat_id,
                key,
                live_delivery,
            )
            delivered = True
        except Exception as exc:
            # Archive upload succeeded but copyMessage failed. Keep the R2
            # object and use the existing delivery stack as a safe fallback.
            log.exception(
                "Live Telegram archive copy failed for job %s; "
                "falling back to normal R2 delivery",
                job_id,
            )
            set_job(
                job_id,
                telegram_live_copy_error=str(exc)[:1000],
                telegram_live_pipeline=False,
            )

    if not delivered:
        delivered = maybe_deliver(
            job_id,
            request.chat_id,
            key,
            filename,
            probe["content_type"],
            final_size,
        )

    if delivered:
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


def run_provider_media_job(job_id, request: JobCreate):
    media_quality = "best" if str(request.quality or "").lower() == "original" else request.quality
    platform = detect_platform(request.url)
    raise_if_cancelled(job_id)

    if platform == "youtube":
        provider_names = PROVIDER_ROUTER.youtube_provider_names()
        if not provider_names:
            raise ProviderError("No YouTube provider is configured")

        errors = []
        for index, provider_name in enumerate(provider_names, 1):
            raise_if_cancelled(job_id)
            set_job(
                job_id,
                status="resolving_provider",
                source_type="youtube",
                provider=provider_name,
                provider_attempt=index,
                progress=3,
                requested_quality=media_quality,
                worker_count=0,
                fragment_workers=0,
            )
            try:
                resolved = PROVIDER_ROUTER.resolve_youtube_provider(
                    provider_name, request.url, media_quality
                )
                validate_public_url(resolved.url)
                set_job(job_id, provider=resolved.provider)
                # Important: fallback covers BOTH resolver failure and a broken
                # direct URL/CDN. AHM7 can resolve successfully yet its returned
                # CDN URL may later fail; in that case Prexzy gets a full try.
                return run_resolved_direct_job(
                    job_id,
                    request,
                    resolved.url,
                    source_headers=resolved.headers or {},
                    source_type="youtube",
                    requested_quality=media_quality,
                    filename_hint=resolved.filename,
                    provider=resolved.provider,
                    allow_unknown_size=True,
                )
            except JobCancelled:
                raise
            except Exception as exc:
                errors.append(f"{provider_name}: {exc}")
                log.warning(
                    "YouTube provider pipeline %s failed for job %s: %s",
                    provider_name, job_id, exc,
                )
                set_job(
                    job_id,
                    provider_error=str(exc)[:700],
                    provider_attempts=errors[-4:],
                )

        raise ProviderError(" | ".join(errors)[-1800:] or "All YouTube providers failed")

    set_job(
        job_id,
        status="resolving_provider",
        source_type="media",
        provider=f"router:{platform}",
        progress=3,
        requested_quality=media_quality,
        worker_count=0,
        fragment_workers=0,
    )
    resolved = PROVIDER_ROUTER.resolve_media(request.url, media_quality)
    validate_public_url(resolved.url)
    set_job(job_id, provider=resolved.provider)
    return run_resolved_direct_job(
        job_id,
        request,
        resolved.url,
        source_headers=resolved.headers or {},
        source_type=platform,
        requested_quality=media_quality,
        filename_hint=resolved.filename,
        provider=resolved.provider,
        allow_unknown_size=True,
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
    resolved = PROVIDER_ROUTER.resolve_terabox(request.url)
    validate_public_url(resolved.url)
    return run_resolved_direct_job(
        job_id,
        request,
        resolved.url,
        source_headers=resolved.headers or {},
        source_type="terabox",
        requested_quality="original",
        filename_hint=resolved.filename,
        provider=resolved.provider,
        allow_unknown_size=True,
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
    raise_if_cancelled(job_id)
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
    """Resolve through the platform router, then optionally fall back to yt-dlp."""
    try:
        return run_provider_media_job(job_id, request)
    except ProviderError as exc:
        platform = detect_platform(request.url)
        log.warning("Provider router failed for %s (%s): %s", job_id, platform, exc)
        set_job(job_id, provider_error=str(exc)[:700])
        if not PROVIDER_FALLBACK_YTDLP:
            raise
        set_job(job_id, status="provider_fallback", progress=4, provider="yt-dlp")
        return run_ytdlp_media_job(job_id, request)


def run_job(job_id, request: JobCreate):
    try:
        raise_if_cancelled(job_id)
        kind = source_kind(request.url)
        if kind == "media":
            run_media_job(job_id, request)
        elif kind == "terabox":
            if PROVIDER_ROUTER.terabox.configured:
                try:
                    run_terabox_provider_job(job_id, request)
                except JobCancelled:
                    raise
                except Exception as exc:
                    log.warning("TeraBox provider failed for %s: %s", job_id, exc)
                    set_job(job_id, provider_error=str(exc)[:500])
                    # Preserve already-direct public TeraBox links only.
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
    except JobCancelled:
        log.info("Download job %s cancelled", job_id)
        set_job(job_id, status="cancelled", progress=0, error=None)
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


def config_warnings():
    warnings = []
    if not WORKER_SECRET:
        warnings.append("WORKER_SECRET missing")
    if not all((STORAGE_ENDPOINT, STORAGE_BUCKET, STORAGE_ACCESS_KEY, STORAGE_SECRET_KEY)):
        warnings.append("R2 storage configuration incomplete")
    if COORDINATOR_ENABLED:
        if not API_SECRET:
            warnings.append("DOWNLOADER_API_SECRET missing")
        if not MONGODB_URI:
            warnings.append("MONGODB_URI missing")
        if not WORKER_URLS:
            warnings.append("No WORKER_1..WORKER_4 URLs configured")
        if not BOT_TOKEN:
            warnings.append("BOT_TOKEN missing")
        if not ARCHIVE_CHAT_ID:
            warnings.append("ARCHIVE_CHAT_ID missing")
    return warnings


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
        "downloader_build": DOWNLOADER_BUILD,
        "telegram_live_pipeline": LIVE_TELEGRAM_PIPELINE,
        "telegram_live_part_bytes": LIVE_TELEGRAM_PART_BYTES,
        "telegram_live_queue_parts": LIVE_TELEGRAM_QUEUE_PARTS,
        "provider_router_build": ROUTER_BUILD,
        "storage_configured": all((STORAGE_ENDPOINT, STORAGE_BUCKET, STORAGE_ACCESS_KEY, STORAGE_SECRET_KEY)),
        "worker_urls_configured": len(WORKER_URLS),
        "config_warnings": config_warnings(),
        "providers": {
            **PROVIDER_ROUTER.status(),
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
        "downloader_build": DOWNLOADER_BUILD,
        "telegram_live_pipeline": LIVE_TELEGRAM_PIPELINE,
        "telegram_live_part_bytes": LIVE_TELEGRAM_PART_BYTES,
        "telegram_live_queue_parts": LIVE_TELEGRAM_QUEUE_PARTS,
        "provider_router_build": ROUTER_BUILD,
        "storage_configured": all((STORAGE_ENDPOINT, STORAGE_BUCKET, STORAGE_ACCESS_KEY, STORAGE_SECRET_KEY)),
        "worker_urls_configured": len(WORKER_URLS),
        "config_warnings": config_warnings(),
        "providers": {
            **PROVIDER_ROUTER.status(),
            "yt_dlp_fallback": PROVIDER_FALLBACK_YTDLP,
        },
    }


@app.post("/worker/range")
def worker_range(body: WorkerRange, x_worker_secret: str | None = Header(default=None)):
    auth_worker(x_worker_secret)
    try:
        return worker_download_range(body)
    except RuntimeError as exc:
        # Deterministic source/range failures are not Render 500s. Returning a
        # structured 422 lets the coordinator switch strategy immediately.
        raise HTTPException(status_code=422, detail=str(exc)[:900])


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
        platform = detect_platform(body.url)
        provider_status = PROVIDER_ROUTER.status()
        if platform == "youtube":
            youtube_status = provider_status.get("youtube") or {}
            order = youtube_status.get("order") or ["ahm7", "prexzy"]
            configured = [
                name for name in order
                if youtube_status.get(name) is True
            ]
            provider_name = configured[0] if configured else (
                "yt-dlp" if PROVIDER_FALLBACK_YTDLP else "unconfigured"
            )
        else:
            social_status = provider_status.get("social") or {}
            provider_name = "cobalt" if social_status.get("cobalt") is True else (
                "yt-dlp" if PROVIDER_FALLBACK_YTDLP else "unconfigured"
            )
        return {
            "ok": True,
            "kind": "media",
            "source_type": platform,
            "provider": provider_name,
        }

    if kind == "terabox" and PROVIDER_ROUTER.terabox.configured:
        try:
            resolved = PROVIDER_ROUTER.resolve_terabox(body.url)
            validate_public_url(resolved.url)
            # Probe only the resolved public file. The direct URL itself is not
            # returned to Vercel and will be resolved again when the job starts.
            info = probe_direct(
                resolved.url,
                extra_headers=resolved.headers or {},
                allow_unknown_size=True,
            )
            return {
                "ok": True,
                "kind": "direct",
                "source_type": "terabox",
                "provider": resolved.provider,
                "filename": resolved.filename or info.get("filename"),
                "size": info.get("size") or info.get("estimated_size"),
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
