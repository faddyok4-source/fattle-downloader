# Fattle Downloader — 4 Render workers + private Telegram archive (no VPS)

Upload this repository to GitHub, then deploy the **same repo to four Render Web Services**.

## Delivery model

No Local Telegram Bot API server or VPS is required.

- Files up to `TELEGRAM_DIRECT_MAX_BYTES` (default ~49 MB): Render downloads the completed object from R2, uploads it **once** to your private Telegram archive channel, then uses `copyMessage` to deliver it to the user. The copied message does not expose the archive channel as a forwarded source.
- Larger files: the object stays in Cloudflare R2 and the bot sends the user a temporary signed **Download File** button. Large-file metadata can also be recorded in the private archive channel.
- The normal hosted Bot API upload limit still applies. A private channel cannot bypass that initial upload limit.

Use it only for public/authorized content. This project does not bypass DRM, passwords, paywalls, private shares, or access controls.

## Render #1 (coordinator + worker)

Set:

```text
DOWNLOADER_ROLE=coordinator_worker
DOWNLOADER_API_SECRET=...
WORKER_SECRET=...
MONGODB_URI=...
MONGODB_DB=fattle_downloader
WORKER_1=https://...
WORKER_2=https://...
WORKER_3=https://...
WORKER_4=https://...

STORAGE_ENDPOINT=https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com
STORAGE_BUCKET=fattle-downloads
STORAGE_ACCESS_KEY=...
STORAGE_SECRET_KEY=...
STORAGE_REGION=auto
MAX_FILE_BYTES=1900000000

BOT_TOKEN=YOUR_EXISTING_TELEGRAM_BOT_TOKEN
ARCHIVE_CHAT_ID=-1001234567890
TELEGRAM_DIRECT_MAX_BYTES=49000000
R2_LINK_TTL_SECONDS=86400
ARCHIVE_LARGE_LINKS=true
DELETE_R2_AFTER_TELEGRAM_ARCHIVE=false
```

Add the bot as an **administrator** of the private archive channel with permission to post messages.

## Render #2–#4

Set only worker/storage settings:

```text
DOWNLOADER_ROLE=worker
WORKER_SECRET=THE_SAME_WORKER_SECRET
STORAGE_ENDPOINT=...
STORAGE_BUCKET=fattle-downloads
STORAGE_ACCESS_KEY=...
STORAGE_SECRET_KEY=...
STORAGE_REGION=auto
MAX_FILE_BYTES=1900000000
```

They do not need `BOT_TOKEN` or `ARCHIVE_CHAT_ID`.

## Build / start

Build:

```text
pip install -r requirements.txt
```

Start:

```text
uvicorn app:app --host 0.0.0.0 --port $PORT
```

## Supported sources

- Public direct HTTP/HTTPS files with byte ranges: up to four Render workers cooperate on one file.
- Public YouTube videos: `yt-dlp` on one worker.
- TeraBox only when the supplied URL resolves to a public direct downloadable file. Private/login/password-protected shares are not bypassed.

## Private archive behavior

Small file:

```text
R2 -> private channel -> copyMessage -> user
```

Large file:

```text
R2 -> temporary signed link -> user
     -> metadata entry in private channel (optional)
```

The endpoint `GET /api/jobs/{job_id}/download-link` can generate a fresh R2 link for a completed stored job.


## MTProto private archive delivery

Large completed files can now be streamed from R2 into Telegram through MTProto
on Render #1, then delivered to the user with Telegram's server-side
`copyMessage`.

Add on Render #1 only:

```text
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=YOUR_API_HASH
TELEGRAM_MTPROTO_MAX_BYTES=1900000000
MTPROTO_R2_BUFFER_BYTES=8388608
```

Flow:

```text
4 Render workers -> R2 -> Render #1 MTProto
-> private archive channel -> copyMessage -> user
```

The private archive channel is not shown as a forwarded source. The large file
is not re-uploaded for each user. The MTProto uploader uses buffered R2 range
reads instead of writing the complete multi-GB object to Render's local disk.

If MTProto upload fails, the user receives an expiring R2 link as fallback.


## Media extractor upgrade

This build uses an independent server-side implementation inspired by the
feature model of YTDLnis: yt-dlp extraction, FFmpeg-assisted format merging,
retry support, and concurrent fragment downloads.

It does **not** copy the YTDLnis Android source and does not use the YTDLnis
name for this application.

Supported behavior:

- Direct range-capable files: up to 4 Render workers.
- Public media pages supported by yt-dlp: one extraction worker with up to 4
  concurrent media fragments.
- Bundled FFmpeg support through `imageio-ffmpeg` when available.
- Quality values: `360p`, `480p`, `720p`, `1080p`, `best`, `audio`.
- R2 -> MTProto private archive -> Telegram `copyMessage` delivery remains
  unchanged.
- TeraBox remains direct-public-file only.
- No cookies, account sessions, paywall bypass, DRM bypass, or private-access
  bypass are included.

A site can still reject cloud-hosted IP addresses. In particular, if a site
returns an account-verification or anti-bot challenge, the downloader returns a
clear error instead of attempting to bypass it.


## v1.2 format fallback

Social-media extractors such as Instagram do not always provide a normal
`height` field for every downloadable format. The downloader now:

1. Tries the requested quality.
2. Falls back to a general video+audio/best selector.
3. If yt-dlp still reports "Requested format is not available", retries once
   with `best`.

This keeps the requested quality as a preference rather than turning it into a
hard failure.


## v1.3 — progress, quality and Telegram delivery

- Media downloads now write real yt-dlp progress into MongoDB.
- Render #1 edits one Telegram status message with a visual loading bar.
- Media jobs show one extractor worker plus concurrent fragment count.
- `R2_FALLBACK_LINKS=false` is the default: R2 is staging/storage; the finished file is expected to be archived in Telegram and delivered with `copyMessage`.
- `/health` reports `telegram_archive_ready` and a safe `telegram_missing` list.


## v1.4 URL-first auto detection

The bot sends the URL to `/api/probe` first. Direct files start immediately in
original quality. Media webpages return `kind=media`, so quality choices appear
only for media. Classification is based on the actual HTTP response, not `.com`
or a filename extension alone.


## v1.5 — media-host routing

Known media hosts such as TikTok, Instagram and YouTube are now classified as
media before the direct-file Range probe. This prevents the URL-inspection step
from producing a confusing HTTP 429 for sites that rate-limit generic probes.

The build also pins yt-dlp to stable 2026.08.19 so a stale Render dependency
cache cannot leave an older TikTok extractor installed.

This does not bypass login, verification, private access, DRM, or a site's
rate-limits. If TikTok rejects a cloud-server request, the job returns a short
friendly error and can be retried later.


## v1.6 — YouTube + TikTok + Instagram Reels

This backend keeps the existing generic yt-dlp media extractor, so supported
public TikTok videos and Instagram Reels remain supported. YouTube additionally
gets a Deno JavaScript runtime and the `yt-dlp[default]` dependency set.

Media routing:

```text
Direct file -> up to 4 range workers
YouTube -> yt-dlp + Deno + FFmpeg
TikTok -> yt-dlp TikTok extractor
Instagram Reel -> yt-dlp Instagram extractor
Other supported media pages -> yt-dlp
```

Completed files still follow:

```text
R2 staging -> private Telegram archive -> copyMessage -> user
```

The service does not add cookies or bypass login, private-access, DRM,
paywalls, rate limits, or anti-bot verification.

### Render build command

Use:

```text
bash build.sh
```

After deploying, `/health` should include:

```json
{
  "deno_ready": true,
  "ffmpeg_ready": true,
  "yt_dlp_version": "2026.08.19"
}
```


## v1.7 — multi-provider resolver layer

The coordinator can now use optional resolver services before falling back to
its local yt-dlp worker:

```text
YouTube / Instagram / TikTok / supported media
  -> Cobalt (when configured)
  -> resolved public download/tunnel URL
  -> Fattle direct downloader (up to 4 Range workers)
  -> yt-dlp fallback if Cobalt cannot resolve it

TeraBox public single-file share
  -> TeraBox Gateway (when configured)
  -> resolved public direct URL
  -> Fattle direct downloader (up to 4 Range workers)

Normal direct file
  -> Fattle direct downloader immediately
```

Only Render #1 needs `COBALT_API_URL`, `COBALT_API_KEY`, and
`TERABOX_API_URL`. Worker #2-#4 do not need provider credentials.

The coordinator never exposes resolved provider URLs or provider keys to the
Telegram/Vercel client. Authorization headers are stripped if a provider URL
redirects to a different host, preventing an API key from being forwarded to
an origin site.

This integration is for public or authorized URLs. It does not add account
cookies or bypass private content, DRM, paywalls, login checks, site
verification, or rate limits.


## v1.8 — Cobalt tunnel / unknown-size streaming fix

Cobalt `tunnel` responses do not always provide an exact `Content-Length`.
Fattle now accepts `Estimated-Content-Length` when present and falls back to a
single bounded-memory stream into R2 when an exact size or HTTP Range support
is unavailable. Exact range-capable files still use up to four workers.

This also fixes known-size files whose origin does not support Range: they now
use the same one-stream R2 path instead of incorrectly sending a Range worker.
The actual byte count is measured during the stream and `MAX_FILE_BYTES` is
still enforced.

Cobalt 4xx responses now preserve the provider's structured error code in the
job error instead of only showing `Cobalt returned HTTP 400`.
