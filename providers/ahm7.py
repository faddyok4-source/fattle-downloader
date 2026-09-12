from __future__ import annotations

import re

from .base import ProviderError, ResolvedMedia, HttpClient, json_or_none, error_message


def _height(value) -> int:
    m = re.search(r"(\d{3,4})", str(value or ""))
    return int(m.group(1)) if m else 0


def _safe_filename(title: str | None, audio: bool = False) -> str:
    title = re.sub(r'[\\/:*?"<>|]+', "_", str(title or "").strip()).strip(" .")
    title = (title or "youtube")[:150]
    ext = ".mp3" if audio else ".mp4"
    if not title.lower().endswith(ext):
        title += ext
    return title


class AHM7YouTube:
    name = "ahm7"

    def __init__(self, api_url: str, http: HttpClient):
        self.api_url = (api_url or "https://ahm7xmakki.com/api/alldl").strip()
        self.http = http

    @property
    def configured(self):
        return bool(self.api_url)

    def resolve(self, source_url: str, quality: str) -> ResolvedMedia:
        response = self.http.request(
            "GET",
            self.api_url,
            headers={
                "Accept": "application/json",
                "User-Agent": "FattleDownloader/3.0",
            },
            params={"url": source_url},
        )

        if response.status_code == 429:
            raise ProviderError("AHM7 fair-use limit was reached; try again later")
        if response.status_code >= 400:
            raise ProviderError(error_message(response, f"AHM7 returned HTTP {response.status_code}"))

        data = json_or_none(response)
        if not data:
            raise ProviderError("AHM7 returned an invalid response")
        if data.get("success") is not True:
            raise ProviderError(str(data.get("message") or data.get("error") or "AHM7 could not resolve this YouTube URL"))

        media = data.get("mediaInfo")
        if not isinstance(media, dict):
            raise ProviderError("AHM7 response did not contain mediaInfo")

        q = str(quality or "best").strip().lower()
        want_audio = q in {"audio", "audio only", "mp3", "m4a"}

        if want_audio:
            direct = str(media.get("audioUrl") or "").strip()
            if not direct:
                raise ProviderError("AHM7 did not return an audio download URL")
            return ResolvedMedia(
                url=direct,
                provider=self.name,
                filename=_safe_filename(media.get("title"), audio=True),
                content_type="audio/mpeg",
                provider_status="resolved",
                metadata={"platform": media.get("platform"), "quality": "audio"},
            )

        qualities = [
            item for item in (media.get("qualities") or [])
            if isinstance(item, dict) and str(item.get("url") or "").startswith(("http://", "https://"))
        ]

        chosen = None
        if qualities:
            if q in {"best", "max", "highest", "original"}:
                chosen = max(qualities, key=lambda x: _height(x.get("quality")))
            else:
                target = _height(q)
                exact = [x for x in qualities if _height(x.get("quality")) == target] if target else []
                if exact:
                    chosen = exact[0]
                elif target:
                    lower = [x for x in qualities if 0 < _height(x.get("quality")) <= target]
                    if lower:
                        chosen = max(lower, key=lambda x: _height(x.get("quality")))
                    else:
                        higher = [x for x in qualities if _height(x.get("quality")) > target]
                        if higher:
                            chosen = min(higher, key=lambda x: _height(x.get("quality")))

        direct = str((chosen or {}).get("url") or media.get("videoUrl") or "").strip()
        if not direct:
            raise ProviderError("AHM7 did not return a video download URL")

        selected_quality = (chosen or {}).get("quality") or ("best" if q == "best" else q)

        return ResolvedMedia(
            url=direct,
            provider=self.name,
            filename=_safe_filename(media.get("title"), audio=False),
            content_type="video/mp4",
            provider_status="resolved",
            metadata={
                "platform": media.get("platform"),
                "quality": selected_quality,
                "thumbnail": media.get("thumbnail"),
            },
        )
