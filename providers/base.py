from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any

import requests


class ProviderError(RuntimeError):
    """A resolver/provider failed in a way that is safe to show to the user."""


@dataclass
class ResolvedMedia:
    url: str
    provider: str
    filename: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    size: int | None = None
    content_type: str | None = None
    provider_status: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class HttpClient:
    """Small provider HTTP client.

    Retries only network failures and 5xx responses. 4xx responses are
    returned immediately so Fattle does not hammer a provider that is
    rejecting credentials, credits, validation, or rate limits.
    """

    def __init__(self, timeout_seconds: int = 30, retries: int = 2):
        self.timeout_seconds = max(5, min(120, int(timeout_seconds)))
        self.retries = max(0, min(3, int(retries)))

    def request(self, method: str, url: str, *, headers=None, json_body=None, params=None):
        """Make a provider request with conservative retries.

        Network errors and 5xx responses are retried, but the FINAL HTTP
        response is returned to the provider adapter instead of being replaced
        with a generic ProviderError. This lets each provider parse its own
        structured error body (for example OmegaTech's "Response not JSON").
        """
        last_error = None
        last_response = None
        attempts = self.retries + 1

        for attempt in range(attempts):
            try:
                response = requests.request(
                    method,
                    url,
                    headers=headers or {},
                    json=json_body,
                    params=params,
                    timeout=(10, self.timeout_seconds),
                    allow_redirects=False,
                )
                last_response = response

                # 4xx should never be retried here; adapters handle them.
                if response.status_code < 500:
                    return response

                # 5xx may be transient, so retry until the last attempt.
                if attempt + 1 >= attempts:
                    return response

            except requests.RequestException as exc:
                last_error = ProviderError(f"Provider request failed: {exc}")
                if attempt + 1 >= attempts:
                    raise last_error

            if attempt + 1 < attempts:
                time.sleep(min(2 ** attempt, 3))

        if last_response is not None:
            return last_response
        raise last_error or ProviderError("Provider request failed")


def json_or_none(response):
    try:
        value = response.json()
        return value if isinstance(value, dict) else None
    except Exception:
        return None


def error_message(response, default: str) -> str:
    data = json_or_none(response) or {}
    for key in ("message", "msg", "detail", "error"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            msg = value.get("message") or value.get("code")
            if isinstance(msg, str) and msg.strip():
                return msg.strip()
    return default
