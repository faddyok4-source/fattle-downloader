#!/usr/bin/env bash
set -euo pipefail

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# yt-dlp recommends a supported JavaScript runtime for full YouTube support.
# Install Deno inside the project so it remains available at runtime.
DENO_INSTALL="${DENO_INSTALL:-$PWD/.deno}"
DENO_VERSION="${DENO_VERSION:-v2.9.1}"

if [ ! -x "$DENO_INSTALL/bin/deno" ]; then
  echo "Installing Deno ${DENO_VERSION}..."
  curl -fsSL https://deno.land/install.sh | DENO_INSTALL="$DENO_INSTALL" sh -s "$DENO_VERSION"
fi

"$DENO_INSTALL/bin/deno" --version
python - <<'PY'
import yt_dlp
print("yt-dlp:", getattr(getattr(yt_dlp, "version", None), "__version__", "unknown"))
PY
