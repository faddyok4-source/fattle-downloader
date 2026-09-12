from __future__ import annotations

import os
from urllib.parse import urlparse

from .base import ProviderError, ResolvedMedia, HttpClient
from .ahm7 import AHM7YouTube
from .prexzy import PrexzyYouTube
from .cobalt import CobaltProvider
from .terabox import TeraBoxProvider


YOUTUBE_HOSTS = {"youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com", "youtu.be"}
INSTAGRAM_HOSTS = {"instagram.com", "www.instagram.com"}
TIKTOK_HOSTS = {"tiktok.com", "www.tiktok.com", "m.tiktok.com", "vm.tiktok.com", "vt.tiktok.com"}
ROUTER_BUILD = "ahm7-prexzy-v3.1"

TERABOX_HINTS = (
    "terabox", "1024tera", "nephobox", "4funbox", "mirrobox",
    "terafileshare", "terasharefile", "terasharelink",
)


def host(url: str) -> str:
    return str(urlparse(url).hostname or "").lower().rstrip(".")


def _matches(hostname: str, roots: set[str]) -> bool:
    return any(hostname == root or hostname.endswith("." + root) for root in roots)


def detect_platform(url: str) -> str:
    hostname = host(url)
    if any(token in hostname for token in TERABOX_HINTS):
        return "terabox"
    if _matches(hostname, YOUTUBE_HOSTS):
        return "youtube"
    if _matches(hostname, INSTAGRAM_HOSTS):
        return "instagram"
    if _matches(hostname, TIKTOK_HOSTS):
        return "tiktok"
    return "generic"


def _order(value: str, default: str) -> list[str]:
    return [x.strip().lower() for x in (value or default).split(",") if x.strip()]


class ProviderRouter:
    def __init__(
        self,
        *,
        timeout_seconds=20,
        retries=1,
        ahm7_api_url="https://ahm7xmakki.com/api/alldl",
        prexzy_api_url="https://prexzyapis.com",
        youtube_order=None,
        cobalt_api_url="",
        cobalt_api_key="",
        terabox_api_url="",
    ):
        http = HttpClient(timeout_seconds=timeout_seconds, retries=retries)
        self.ahm7 = AHM7YouTube(ahm7_api_url, http)
        self.prexzy = PrexzyYouTube(prexzy_api_url, http)
        self.cobalt = CobaltProvider(cobalt_api_url, cobalt_api_key, http)
        self.terabox = TeraBoxProvider(terabox_api_url, http)
        self.youtube_order = youtube_order or ["ahm7", "prexzy"]

    @classmethod
    def from_env(cls, *, timeout_seconds=20, retries=1):
        return cls(
            timeout_seconds=timeout_seconds,
            retries=retries,
            ahm7_api_url=os.getenv("AHM7_API_URL", "https://ahm7xmakki.com/api/alldl").strip(),
            prexzy_api_url=os.getenv("PREXZY_API_URL", "https://prexzyapis.com").strip(),
            youtube_order=_order(os.getenv("YOUTUBE_PROVIDER_ORDER", ""), "ahm7,prexzy"),
            cobalt_api_url=os.getenv("COBALT_API_URL", "").strip(),
            cobalt_api_key=os.getenv("COBALT_API_KEY", "").strip(),
            terabox_api_url=os.getenv("TERABOX_API_URL", "").strip(),
        )

    def status(self):
        return {
            "youtube": {
                "ahm7": self.ahm7.configured,
                "prexzy": self.prexzy.configured,
                "order": self.youtube_order,
                "api_key_required": False,
            },
            "social": {
                "cobalt": self.cobalt.configured,
            },
            "terabox": self.terabox.configured,
        }

    def _resolve_youtube(self, source_url: str, quality: str) -> ResolvedMedia:
        errors = []

        for name in self.youtube_order:
            try:
                if name == "ahm7":
                    if self.ahm7.configured:
                        return self.ahm7.resolve(source_url, quality)
                elif name == "prexzy":
                    if self.prexzy.configured:
                        return self.prexzy.resolve(source_url, quality)
            except ProviderError as exc:
                errors.append(f"{name}: {exc}")

        if errors:
            raise ProviderError(" | ".join(errors))
        raise ProviderError("No YouTube provider is configured")

    def resolve_media(self, source_url: str, quality: str) -> ResolvedMedia:
        platform = detect_platform(source_url)

        if platform == "youtube":
            return self._resolve_youtube(source_url, quality)

        if platform in {"instagram", "tiktok", "generic"}:
            if not self.cobalt.configured:
                raise ProviderError(f"Cobalt is not configured for {platform}")
            return self.cobalt.resolve(source_url, quality)

        raise ProviderError(f"No media provider is configured for {platform}")

    def resolve_terabox(self, source_url: str) -> ResolvedMedia:
        return self.terabox.resolve(source_url)
