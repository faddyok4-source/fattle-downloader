from __future__ import annotations

import re
from urllib.parse import urlparse

from .base import ProviderError, ResolvedMedia, HttpClient, json_or_none


def _host(url: str) -> str:
    return str(urlparse(url).hostname or "").lower().rstrip(".")


class CobaltProvider:
    name = "cobalt"

    def __init__(self, api_url: str, api_key: str, http: HttpClient):
        self.api_url = (api_url or "").strip().rstrip("/")
        self.api_key = (api_key or "").strip()
        self.http = http

    @property
    def configured(self):
        return bool(self.api_url)

    def headers(self):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "FattleDownloader/2.0",
        }
        if self.api_key:
            headers["Authorization"] = f"Api-Key {self.api_key}"
        return headers

    @staticmethod
    def quality(quality: str):
        q = str(quality or "best").strip().lower()
        if q in {"audio", "audio only", "mp3", "m4a"}:
            return "max", "audio"
        if q in {"best", "max", "highest", "original"}:
            return "max", "auto"
        match = re.search(r"(144|240|360|480|720|1080|1440|2160|4320)", q)
        return (match.group(1) if match else "1080"), "auto"

    def resolve(self, source_url: str, quality: str) -> ResolvedMedia:
        if not self.configured:
            raise ProviderError("Cobalt provider is not configured")

        video_quality, download_mode = self.quality(quality)
        response = self.http.request(
            "POST",
            self.api_url + "/",
            headers=self.headers(),
            json_body={
                "url": source_url,
                "videoQuality": video_quality,
                "downloadMode": download_mode,
                "audioFormat": "best",
                "filenameStyle": "basic",
                "youtubeVideoContainer": "mp4",
                "localProcessing": "disabled",
            },
        )

        if response.status_code == 429:
            raise ProviderError("Cobalt is rate-limiting requests")
        if response.status_code in {401, 403}:
            raise ProviderError("Cobalt rejected the API credentials or request")
        if response.status_code >= 400:
            data = json_or_none(response) or {}
            err = data.get("error") if isinstance(data.get("error"), dict) else {}
            code = str(err.get("code") or f"HTTP {response.status_code}")
            context = err.get("context") if isinstance(err.get("context"), dict) else {}
            service = str(context.get("service") or "")
            detail = code + (f"; service={service}" if service else "")
            raise ProviderError(f"Cobalt could not resolve this media ({detail})")

        data = json_or_none(response)
        if not data:
            raise ProviderError("Cobalt returned an invalid response")

        status = str(data.get("status") or "").lower()
        if status in {"redirect", "tunnel"}:
            direct = str(data.get("url") or "").strip()
            if not direct:
                raise ProviderError("Cobalt did not return a download URL")
            source_headers = {}
            if _host(direct) == _host(self.api_url):
                source_headers = self.headers()
                source_headers.pop("Content-Type", None)
                source_headers.pop("Accept", None)
            return ResolvedMedia(
                url=direct,
                provider=self.name,
                filename=str(data.get("filename") or "").strip() or None,
                headers=source_headers,
                provider_status=status,
            )

        if status == "picker":
            want_audio = self.quality(quality)[1] == "audio"
            if want_audio and data.get("audio"):
                direct = str(data.get("audio") or "").strip()
                filename = str(data.get("audioFilename") or "audio.m4a")
            else:
                items = data.get("picker") if isinstance(data.get("picker"), list) else []
                item = next(
                    (x for x in items if isinstance(x, dict) and x.get("type") in {"video", "gif"}),
                    None,
                )
                if item is None:
                    raise ProviderError("This post contains multiple items; picker downloads are not supported yet")
                direct = str(item.get("url") or "").strip()
                filename = None
            return ResolvedMedia(
                url=direct,
                provider=self.name,
                filename=filename,
                provider_status=status,
            )

        if status == "local-processing":
            raise ProviderError("Cobalt requires local media processing for this result")
        if status == "error":
            err = data.get("error") if isinstance(data.get("error"), dict) else {}
            raise ProviderError(f"Cobalt could not resolve this media ({err.get('code') or 'unknown'})")

        raise ProviderError(f"Unsupported Cobalt response: {status or 'unknown'}")
