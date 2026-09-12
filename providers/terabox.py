from __future__ import annotations

from .base import ProviderError, ResolvedMedia, HttpClient, json_or_none, error_message


class TeraBoxProvider:
    name = "terabox"

    def __init__(self, api_url: str, http: HttpClient):
        self.api_url = (api_url or "").strip().rstrip("/")
        self.http = http

    @property
    def configured(self):
        return bool(self.api_url)

    def resolve(self, source_url: str) -> ResolvedMedia:
        if not self.configured:
            raise ProviderError("TeraBox provider is not configured")

        response = self.http.request(
            "GET",
            self.api_url + "/api",
            headers={"Accept": "application/json", "User-Agent": "FattleDownloader/2.0"},
            params={"url": source_url, "resolve": "true"},
        )

        if response.status_code == 429:
            raise ProviderError("TeraBox resolver is rate-limiting requests")
        if response.status_code >= 400:
            raise ProviderError(error_message(response, f"TeraBox resolver returned HTTP {response.status_code}"))

        data = json_or_none(response)
        if not data:
            raise ProviderError("TeraBox resolver returned an invalid response")
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
        raw_size = item.get("size")
        size = int(raw_size) if isinstance(raw_size, (int, float)) or (
            isinstance(raw_size, str) and raw_size.isdigit()
        ) else None

        return ResolvedMedia(
            url=direct,
            provider=self.name,
            filename=str(item.get("filename") or "").strip() or None,
            size=size,
            provider_status="resolved",
        )
