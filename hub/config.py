from __future__ import annotations

# Shared configuration for Flask routes, import scripts, and frontend bootstrap.
# Keeping this outside app.py lets command-line tools reuse the same project
# paths, player metadata, and secrets lookup without importing route code first.
import os
import tomllib
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[1]
FALLBACK_SOURCE_REPO = Path(os.environ.get("SOURCE_REPO", r"C:\\Users\\joeym\\Repos\\high-council-hub-1"))
DATA_DIR = (BASE_DIR / "data") if (BASE_DIR / "data").exists() else (FALLBACK_SOURCE_REPO / "data")
ASSETS_DIR = (BASE_DIR / "assets") if (BASE_DIR / "assets").exists() else (FALLBACK_SOURCE_REPO / "assets")
GODS_ASSETS_DIR = ASSETS_DIR / "gods"
PANTHEONS_DIR = ASSETS_DIR / "pantheons"

LOCAL_SECRETS_PATH = BASE_DIR / "secrets" / "app_secrets.toml"
LOCAL_SECRETS_PATH_TXT = BASE_DIR / "secrets" / "app_secrets.toml.txt"
LEGACY_LOCAL_SECRETS_PATH = Path.home() / "Documents" / "New project" / "secrets" / "app_secrets.toml"
SECRETS_PATH = (
    LOCAL_SECRETS_PATH
    if LOCAL_SECRETS_PATH.exists()
    else (
        LOCAL_SECRETS_PATH_TXT
        if LOCAL_SECRETS_PATH_TXT.exists()
        else (
            LEGACY_LOCAL_SECRETS_PATH
            if LEGACY_LOCAL_SECRETS_PATH.exists()
            else ((BASE_DIR / ".streamlit" / "secrets.toml") if (BASE_DIR / ".streamlit" / "secrets.toml").exists() else (FALLBACK_SOURCE_REPO / ".streamlit" / "secrets.toml"))
        )
    )
)
LOCAL_ACTIVITY_LOG_PATH = BASE_DIR / "local_activity_log.json"
LOCAL_ACTIVITY_LOG_ENABLED = not bool(os.environ.get("VERCEL"))

PLAYERS = ["Joey", "Darian", "Jami", "Jamie", "Mike"]
PLAYER_ABBR = {
    "Joey": "Jo",
    "Darian": "Da",
    "Jami": "Ji",
    "Jamie": "Je",
    "Mike": "Mi",
}

COUNCIL_PLAYER_ALIASES = {
    "Joey": {"names": ["Joey", "littlem0nk"], "ids": ["76561198000048896"]},
    "Darian": {"names": ["Darian", "AntiSocialElf"], "ids": ["76561198881409884"]},
    "Jami": {"names": ["Jami", "crispyplug"], "ids": ["76561198045467382"]},
    "Jamie": {"names": ["Jamie"], "ids": []},
    "Mike": {"names": ["Mike"], "ids": []},
}
COUNCIL_COLORS = {
    "Joey": "#d7a33d",
    "Darian": "#4c8dd8",
    "Jami": "#d46fa2",
    "Jamie": "#4ea885",
    "Mike": "#c44e5e",
}
TIER_THRESHOLDS = [(95, "SS"), (90, "S"), (80, "A"), (70, "B"), (60, "C"), (50, "D"), (1, "F")]
TIER_ORDER = ["SS", "S", "A", "B", "C", "D", "F", "U"]
TIER_COLORS = {
    "SS": "#efd28e",
    "S": "#d7aa58",
    "A": "#bcc4d8",
    "B": "#9fb5ce",
    "C": "#97bea2",
    "D": "#b09d82",
    "F": "#b06a58",
    "U": "#8d877d",
}
HOT_TAKE_THRESHOLD = 30
SMITESOURCE_PROFILE_LINKS = {
    "Joey": "https://smitesource.com/player/f29ca789-74f0-442f-937a-f72fcba045d3",
    "Darian": "https://smitesource.com/player/8005a240-cd89-4f14-bc40-db769319cb43",
    "Jami": "https://smitesource.com/player/8f5f48ca-10d1-4104-ab5d-bb80d4683313",
    "Jamie": "",
    "Mike": "https://smitesource.com/player/f09127e9-676e-498e-b09e-6e20924a91f5",
}
SMITESOURCE_RPC_BASE = "https://smitesource.com/rpc"
SMITESOURCE_CACHE_TTL_SECONDS = int(os.environ.get("SMITESOURCE_CACHE_TTL_SECONDS", "1800"))
SMITESOURCE_MATCH_PAGE_SIZE = int(os.environ.get("SMITESOURCE_MATCH_PAGE_SIZE", "20"))
SMITESOURCE_MATCH_SAMPLE_SIZE = int(os.environ.get("SMITESOURCE_MATCH_SAMPLE_SIZE", "200"))
MATCH_HISTORY_UI_ROW_LIMIT = int(os.environ.get("MATCH_HISTORY_UI_ROW_LIMIT", "5000"))
MATCH_SUMMARY_MIN_ROWS = int(os.environ.get("MATCH_SUMMARY_MIN_ROWS", "1000"))


def load_streamlit_secrets() -> dict[str, Any]:
    """Load local TOML secrets while keeping Vercel environment variables primary."""
    if not SECRETS_PATH.exists():
        return {}
    with SECRETS_PATH.open("rb") as handle:
        return tomllib.load(handle)


def get_secret(name: str, default: str = "") -> str:
    """Read env vars first, then local secrets, so local and Vercel config match."""
    secrets = load_streamlit_secrets()
    return os.environ.get(name, secrets.get(name, default))
