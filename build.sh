#!/usr/bin/env bash
set -euo pipefail

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# Provider mode does not require Deno. Install it only when explicitly enabled,
# or automatically when yt-dlp fallback is enabled. A Deno mirror outage must
# not break the whole Render deployment.
DENO_INSTALL="${DENO_INSTALL:-$PWD/.deno}"
DENO_VERSION="${DENO_VERSION:-v2.9.1}"
INSTALL_DENO="${INSTALL_DENO:-auto}"
PROVIDER_FALLBACK_YTDLP="${PROVIDER_FALLBACK_YTDLP:-false}"

want_deno=false
if [ "$INSTALL_DENO" = "true" ]; then
  want_deno=true
elif [ "$INSTALL_DENO" = "auto" ] && [ "${PROVIDER_FALLBACK_YTDLP,,}" = "true" ]; then
  want_deno=true
fi

if [ "$want_deno" = "true" ] && [ ! -x "$DENO_INSTALL/bin/deno" ]; then
  echo "Installing Deno ${DENO_VERSION} (best effort)..."
  if curl -fsSL https://deno.land/install.sh | DENO_INSTALL="$DENO_INSTALL" sh -s "$DENO_VERSION"; then
    echo "Deno installed."
  else
    echo "WARNING: Deno install failed; continuing because provider downloads do not require it."
  fi
fi

if [ -x "$DENO_INSTALL/bin/deno" ]; then
  "$DENO_INSTALL/bin/deno" --version || true
else
  echo "Deno not installed (expected when provider fallback is disabled)."
fi

python - <<'PY'
import yt_dlp
print("yt-dlp:", getattr(getattr(yt_dlp, "version", None), "__version__", "unknown"))
PY
