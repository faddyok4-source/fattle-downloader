from __future__ import annotations

import re
from urllib.parse import urlparse

from .base import ProviderError, ResolvedMedia, HttpClient, json_or_none, error_message


URL_KEYS = {
    "url", "download_url", "downloadurl", "download", "link",
    "video", "video_url", "videourl", "audio", "audio_url", "audiourl",
    "media", "src", "file", "stream",
}


def _safe_filename(value: str | None, audio: bool = False) -> str:
    name = re.sub(r'[\\/:*?"<>|]+', "_", str(value or "").strip()).strip(" .")
    name = (name or "youtube")[:150]
    wanted = ".mp3" if audio else ".mp4"
    if "." not in name.rsplit("/", 1)[-1]:
        name += wanted
    return name


def _walk_urls(value, path=""):
    found = []
    if isinstance(value, dict):
        for key, item in value.items():
            k = str(key).lower()
            child = f"{path}.{k}" if path else k
            if isinstance(item, str) and item.startswith(("http://", "https://")):
                if k in URL_KEYS or any(tok in k for tok in ("url", "link", "download", "video", "audio", "stream")):
                    found.append((child, item))
            elif isinstance(item, (dict, list)):
                found.extend(_walk_urls(item, child))
    elif isinstance(value, list):
        for i, item in enumerate(value):
            found.extend(_walk_urls(item, f"{path}[{i}]"))
    return found


def _title_from(data):
    if isinstance(data, dict):
        for key in ("title", "filename", "name"):
            v = data.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
        for v in data.values():
            if isinstance(v, dict):
                title = _title_from(v)
                if title:
                    return title
    return None


class PrexzyYouTube:
    name = "prexzy"

    def __init__(self, api_url: str, http: HttpClient):
        self.api_url = (api_url or "https://prexzyapis.com").strip().rstrip("/")
        self.http = http

    @property
    def configured(self):
        return bool(self.api_url)

    def resolve(self, source_url: str, quality: str) -> ResolvedMedia:
        q = str(quality or "best").strip().lower()
        audio = q in {"audio", "audio only", "mp3", "m4a"}
        endpoint = "/download/ytmp3" if audio else "/download/ytmp4"

        response = self.http.request(
            "GET",
            self.api_url + endpoint,
            headers={
                "Accept": "application/json",
                "User-Agent": "FattleDownloader/3.0",
            },
            params={"url": source_url},
        )

        if response.status_code == 429:
            raise ProviderError("Prexzy is rate-limiting requests")
        if response.status_code >= 400:
            raise ProviderError(error_message(response, f"Prexzy returned HTTP {response.status_code}"))

        data = json_or_none(response)
        if not data:
            raise ProviderError("Prexzy returned an invalid response")

        # Respect obvious failure markers.
        if data.get("success") is False:
            raise ProviderError(str(data.get("message") or data.get("error") or "Prexzy could not resolve this YouTube URL"))
        status = str(data.get("status") or "").lower()
        if status in {"error", "failed", "fail"}:
            raise ProviderError(str(data.get("message") or data.get("error") or "Prexzy could not resolve this YouTube URL"))

        candidates = _walk_urls(data)
        if not candidates:
            raise ProviderError("Prexzy response did not contain a downloadable URL")

        # Prefer media-looking URLs over thumbnails/artwork.
        scored = []
        for path, url in candidates:
            score = 0
            p = path.lower()
            u = url.lower()
            if audio:
                if "audio" in p or ".mp3" in u or ".m4a" in u:
                    score += 8
                if "video" in p:
                    score -= 3
            else:
                if "video" in p or ".mp4" in u or ".webm" in u:
                    score += 8
                if "audio" in p:
                    score -= 3
            if "download" in p:
                score += 3
            if "thumbnail" in p or "thumb" in p or "image" in p:
                score -= 10
            scored.append((score, url))

        scored.sort(key=lambda x: x[0], reverse=True)
        direct = scored[0][1]

        return ResolvedMedia(
            url=direct,
            provider=self.name,
            filename=_safe_filename(_title_from(data), audio=audio),
            content_type="audio/mpeg" if audio else "video/mp4",
            provider_status="resolved",
            metadata={
                "quality": "best",  # Prexzy's dedicated ytmp4 endpoint documents best direct quality.
                "requested_quality": q,
                "exact_quality_supported": False,
            },
        )
