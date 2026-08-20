# =============================================================================
# SMITE 2 | HIGH COUNCIL HUB
# =============================================================================
# Structure:
#   1. Imports & Constants
#   2. Helper Functions (pure utilities)
#   3. Data Helpers (UI-aware, use PLAYERS/COUNCIL_COLORS)
#   4. Data Loading & Persistence
#   5. Page Config & CSS Injection
#   6. Data Load + Validation
#   7. Top Status Bar
#   8. Sidebar (Live Rankings)
#   9. Tabs (Index, Leaderboard, Favorites, Tierlist, Analytics, H2H, Activity, Input)
# =============================================================================


# ── 1. IMPORTS & CONSTANTS ───────────────────────────────────────────────────

import hashlib
import os
import base64
import json
import requests
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import io
from PIL import Image
from datetime import datetime, timezone


# --- Player roster: add/remove names here to scale the app ---
PLAYERS: list[str] = ["Joey", "Darian", "Jami", "Jamie", "Mike"]

# --- Per-player accent colors ---
COUNCIL_COLORS: dict[str, str] = {
    "Joey":   "#f59e0b",
    "Darian": "#3b82f6",
    "Jami":   "#ec4899",
    "Jamie":  "#10b981",
    "Mike":   "#e11d48",
}

# --- 2-char abbreviations for pills (avoids J/J/J ambiguity) ---
PLAYER_ABBR: dict[str, str] = {
    "Joey":   "Jo",
    "Darian": "Da",
    "Jami":   "Ji",
    "Jamie":  "Je",
    "Mike":   "Mi",
}

# --- Tier thresholds & colors (edit tiers here and the whole app updates) ---
TIER_THRESHOLDS: list[tuple[int, str]] = [
    (95, "SS"),
    (90, "S"),
    (80, "A"),
    (70, "B"),
    (60, "C"),
    (50, "D"),
    (1,  "F"),
]

TIER_COLORS: dict[str, str] = {
    "SS": "#ef4444",
    "S":  "#f59e0b",
    "A":  "#8b5cf6",
    "B":  "#3b82f6",
    "C":  "#10b981",
    "D":  "#64748b",
    "F":  "#991b1b",
    "U":  "#475569",
}

# --- File paths (assets only — data now lives in Supabase) ---
BACKGROUND_PATH   = "assets/void_bg.png"
PANTHEONS_DIR     = "assets/pantheons"

# --- Supabase REST helpers (no SDK — plain requests, zero version conflicts) ---
def _sb_headers() -> dict:
    key = st.secrets["SUPABASE_KEY"]
    return {
        "apikey":        key,
        "Authorization": f"Bearer {key}",
        "Content-Type":  "application/json",
        "Prefer":        "return=representation",
    }

def _sb_url(table: str) -> str:
    return f"{st.secrets['SUPABASE_URL']}/rest/v1/{table}"

def sb_select(table: str) -> list[dict]:
    """Fetch all rows from a table. Returns a list of dicts."""
    resp = requests.get(_sb_url(table), headers=_sb_headers(), params={"select": "*"})
    resp.raise_for_status()
    return resp.json()

def sb_upsert(table: str, records: list[dict], on_conflict: str) -> None:
    """Upsert a list of records into a table."""
    headers = {**_sb_headers(), "Prefer": f"resolution=merge-duplicates,return=minimal"}
    
    params = {}
    if on_conflict:
        params["on_conflict"] = on_conflict   # <-- THIS WAS MISSING
    
    resp = requests.post(
        _sb_url(table),
        headers=headers,
        params=params,          # <-- added
        data=json.dumps(records),
    )
    resp.raise_for_status()

def sb_insert(table: str, records: list[dict]) -> None:
    """Insert records (append-only, no conflict resolution)."""
    if not records:
        return
    headers = {**_sb_headers(), "Prefer": "return=minimal"}
    resp = requests.post(
        _sb_url(table),
        headers=headers,
        data=json.dumps(records),
    )
    resp.raise_for_status()

def sb_select_player_rankings(player: str) -> list[dict]:
    """Fetch personal ranking rows for one player."""
    resp = requests.get(
        _sb_url("personal_rankings"),
        headers=_sb_headers(),
        params={"select": "*", "player": f"eq.{player}"},
    )
    resp.raise_for_status()
    return resp.json()

@st.cache_data(ttl=60)
def load_all_rankings() -> dict:
    """
    Returns {player: {god_name: rank}} for all players.
    Used by council_ratings_html to show per-player ranks on god cards.
    """
    try:
        resp = requests.get(
            _sb_url("personal_rankings"),
            headers=_sb_headers(),
            params={"select": "*"},
        )
        resp.raise_for_status()
        rows = resp.json()
        result = {}
        for r in rows:
            p = r.get("player", "")
            g = r.get("god_name", "")
            rk = r.get("rank", 0)
            if p and g and rk:
                result.setdefault(p, {})[g] = rk
        return result
    except Exception:
        return {}

# --- Layout ---
GRID_COLS = 5   # God Index / Input grid columns
TIER_COLS = 6   # Tier list columns
HOT_TAKE_THRESHOLD = 30   # pts from group avg to earn HOT TAKE badge


# ── 2. PURE HELPER FUNCTIONS ─────────────────────────────────────────────────

def get_tier_from_rating(rating: int) -> str:
    """Map a numeric rating to a tier label."""
    for threshold, label in TIER_THRESHOLDS:
        if rating >= threshold:
            return label
    return "U"


def check_pin(player: str, entered: str) -> bool:
    """Compare entered PIN against the plain-text PIN stored in secrets.toml."""
    secret_key = f"PIN_{player.upper()}"
    stored_pin = st.secrets.get(secret_key, "")
    return entered.strip() == stored_pin


def encode_file_base64(filepath: str) -> str:
    """Read a file and return a base64-encoded string, or empty string if missing."""
    if os.path.exists(filepath):
        with open(filepath, "rb") as f:
            return base64.b64encode(f.read()).decode()
    return ""


def _file_to_data_uri(filepath: str) -> str:
    """Convert a local image to a base64 data URI. Returns empty string if file missing."""
    if not os.path.exists(filepath):
        return ""
    ext = os.path.splitext(filepath)[1].lower().lstrip(".")
    mime = {"jpg": "jpeg", "jpeg": "jpeg", "png": "png", "webp": "webp"}.get(ext, "jpeg")
    try:
        with open(filepath, "rb") as f:
            data = base64.b64encode(f.read()).decode()
        return f"data:image/{mime};base64,{data}" if data else ""
    except Exception:
        return ""

def _file_to_data_uri_optimized(filepath: str, max_size: int = 400) -> str:
    """Resize image to max_size (width/height) before encoding to save memory."""
    if not os.path.exists(filepath):
        return ""
    
    try:
        with Image.open(filepath) as img:
            # Convert to RGB if necessary (handles PNG transparency issues in some browsers)
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")
            
            # Resize maintaining aspect ratio
            img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
            
            # Save to buffer
            buffer = io.BytesIO()
            img.save(buffer, format="JPEG", quality=85) # Compress slightly
            buffer.seek(0)
            
            encoded = base64.b64encode(buffer.getvalue()).decode()
            ext = os.path.splitext(filepath)[1].lower().lstrip(".")
            mime = {"jpg": "jpeg", "jpeg": "jpeg", "png": "png", "webp": "webp"}.get(ext, "jpeg")
            
            return f"data:image/{mime};base64,{encoded}"
    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return ""

@st.cache_data
def build_god_image_map() -> dict:
    god_map = {}
    gods_dir = os.path.join("assets", "gods")
    if not os.path.exists(gods_dir):
        return god_map
    
    for fname in os.listdir(gods_dir):
        stem, ext = os.path.splitext(fname)
        if ext.lower() in (".png", ".jpg", ".jpeg", ".webp"):
            # Use the optimized function
            uri = _file_to_data_uri_optimized(os.path.join(gods_dir, fname), max_size=350)
            if uri:
                god_map[stem.lower()] = uri
    return god_map


@st.cache_data
def build_pantheon_image_map() -> dict:
    """Same as build_god_image_map — reads assets/pantheons/ once and caches."""
    pantheon_map = {}
    if not os.path.exists(PANTHEONS_DIR):
        return pantheon_map
    for fname in os.listdir(PANTHEONS_DIR):
        stem, ext = os.path.splitext(fname)
        if ext.lower() in (".png", ".jpg", ".jpeg", ".webp"):
            uri = _file_to_data_uri(os.path.join(PANTHEONS_DIR, fname))
            if uri:
                pantheon_map[stem.lower()] = uri
    return pantheon_map


def get_god_image_url(god_name: str, god_map: dict) -> str:
    """
    Look up a god image data URI from the cached map — no file I/O, no network.
    Tries multiple name formats so Zeus, zeus, SunWukong, Sun_Wukong all resolve.
    """
    fallback = "https://via.placeholder.com/300x450/1e293b/334155?text=No+Art"
    if not god_map:
        return fallback
    for candidate in [
        god_name,
        god_name.replace(" ", ""),
        god_name.replace(" ", "").lower(),
        god_name.replace(" ", "_").lower(),
        god_name.lower(),
    ]:
        if candidate.lower() in god_map:
            return god_map[candidate.lower()]
    return fallback


def get_pantheon_icon_src(pantheon_name: str, pantheon_map: dict = None) -> str:
    """Look up a pantheon icon data URI from the cached map. Falls back to placeholder."""
    if pantheon_map:
        for candidate in [
            pantheon_name.replace(" ", "_").lower(),
            pantheon_name.lower(),
            pantheon_name.replace(" ", "").lower(),
        ]:
            if candidate in pantheon_map:
                return pantheon_map[candidate]
    return "https://via.placeholder.com/30/7c3aed/ffffff?text=?"



# ── 3. UI COMPONENT HELPERS ──────────────────────────────────────────────────

def council_ratings_html(row: pd.Series, all_ranks: dict = None) -> str:
    """
    Render per-player rating pills, optionally with personal rank below each.
    all_ranks: {player: {god_name: rank}} from load_all_rankings().
    """
    god  = row.get("God", "")
    pills = []
    for p in PLAYERS:
        color      = COUNCIL_COLORS.get(p, "#7c3aed")
        has_rating = p in row and pd.notna(row[p]) and row[p] > 0
        rating_val = int(row[p]) if has_rating else None
        rank_val   = (all_ranks or {}).get(p, {}).get(god) if god else None

        score_html = (
            f'<span style="font-size:0.85rem;font-weight:900;color:white;'
            f'background:rgba(255,255,255,0.07);border:1px solid {color}44;'
            f'border-radius:6px;padding:2px 7px;min-width:32px;text-align:center;">'
            f'{rating_val}</span>'
        ) if has_rating else (
            f'<span style="font-size:0.85rem;font-weight:900;color:#475569;'
            f'background:rgba(255,255,255,0.03);border:1px solid #47556933;'
            f'border-radius:6px;padding:2px 7px;min-width:32px;text-align:center;">—</span>'
        )

        rank_html = (
            f'<span style="font-size:0.58rem;color:{color};opacity:0.75;'
            f'font-weight:700;margin-top:1px;">#{rank_val}</span>'
        ) if rank_val else (
            f'<span style="font-size:0.58rem;color:#334155;margin-top:1px;">·</span>'
        )

        name_color = color if has_rating else "#475569"
        pills.append(
            f'<div style="display:flex;flex-direction:column;align-items:center;gap:1px;">'
            f'<span style="font-size:0.6rem;color:{name_color};font-weight:700;'
            f'text-transform:uppercase;">{p}</span>'
            + score_html + rank_html +
            f'</div>'
        )
    return (
        '<div style="display:flex;justify-content:center;gap:8px;margin-top:10px;'
        'padding-top:8px;border-top:1px solid rgba(255,255,255,0.07);">'
        + "".join(pills)
        + "</div>"
    )


def get_hot_take_html(row: pd.Series) -> str:
    """Return a HOT TAKE badge if any player is HOT_TAKE_THRESHOLD+ pts from group avg."""
    rated = [p for p in PLAYERS if p in row and pd.notna(row[p]) and row[p] > 0]
    if len(rated) < 2:
        return ""
    scores = [row[p] for p in rated]
    avg = sum(scores) / len(scores)
    if any(abs(row[p] - avg) >= HOT_TAKE_THRESHOLD for p in rated):
        return (
            '<div style="display:inline-block;background:#f59e0b22;border:1px solid #f59e0b;'
            'border-radius:8px;padding:2px 8px;font-size:0.6rem;font-weight:900;'
            'color:#f59e0b;margin-top:6px;">⚡ HOT TAKE</div>'
        )
    return ""


def movement_indicator_html(mvt: int) -> tuple[str, str]:
    """
    Return (css_class, indicator_html) for a rank-movement value.
    css_class is applied to the container div for glow effects.
    """
    if mvt > 0:
        return (
            "up-shift",
            f'<span style="color:#4ade80;font-size:0.6rem;font-weight:900;">▲{int(mvt)}</span>',
        )
    if mvt < 0:
        return (
            "down-shift",
            f'<span style="color:#f87171;font-size:0.6rem;font-weight:900;">▼{abs(int(mvt))}</span>',
        )
    return ("", '<span style="color:#64748b;font-size:0.6rem;">•</span>')


# ── 4. DATA LOADING & PERSISTENCE ────────────────────────────────────────────

@st.cache_data(ttl=30)
def load_titan_data() -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Fetch metadata + ratings from Supabase via plain REST, merge, compute columns.
    ttl=30 → re-fetches every 30 s so all council members see live updates.
    """
    # ── Fetch ────────────────────────────────────────────────────────────────
    meta_rows    = sb_select("gods_metadata")
    ratings_rows = sb_select("council_ratings")

    if not meta_rows or not ratings_rows:
        return None, None, None

    df_meta    = pd.DataFrame(meta_rows)
    df_ratings = pd.DataFrame(ratings_rows)

    # ── Rename DB snake_case → app display names ─────────────────────────────
    df_meta = df_meta.rename(columns={
        "god_name":    "God",
        "title":       "Title",
        "pantheon":    "Pantheon",
        "role":        "Role",
        "class":       "Class",
        "attack_type": "Attack Type",
        "damage_type": "Damage Type",
        "tier":        "Tier",
        "rank":        "Rank",
        "movement":    "Movement",
    })

    player_col_map = {p.lower(): p for p in PLAYERS}
    df_ratings = df_ratings.rename(columns={"god_name": "God"})
    df_ratings = df_ratings.rename(columns=player_col_map)

    # ── Merge ─────────────────────────────────────────────────────────────────
    full_df = pd.merge(df_meta, df_ratings, on="God", how="left")

    # ── Compute consensus rating (ignore 0s / NaN) ───────────────────────────
    available    = [p for p in PLAYERS if p in full_df.columns]
    ratings_only = full_df[available].apply(pd.to_numeric, errors="coerce").replace(0, pd.NA)
    full_df["Rating"] = ratings_only.mean(axis=1, skipna=True).fillna(0).astype(int)

    if "Movement" not in full_df.columns:
        full_df["Movement"] = 0
    else:
        full_df["Movement"] = full_df["Movement"].fillna(0).astype(int)

    if "Rank" not in full_df.columns:
        full_df = full_df.sort_values(by=["Rating", "God"], ascending=[False, True])
        full_df["Rank"] = range(1, len(full_df) + 1)

    full_df["Tier"]  = full_df["Tier"].astype(str).str.strip().str.upper()
    full_df["Role"]  = full_df["Role"].fillna("Unknown")
    full_df["Class"] = full_df["Class"].fillna("Unknown")

    tiers = full_df.groupby("Tier")["God"].apply(list).to_dict()
    return full_df, df_ratings, tiers

def get_recent_history(limit: int = 20) -> list[dict]:
    """Fetch the latest records from the rating_history table."""
    params = {
        "select": "*",
        "order": "changed_at.desc",
        "limit": str(limit)
    }
    try:
        resp = requests.get(_sb_url("rating_history"), headers=_sb_headers(), params=params)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"Error fetching history: {e}")
    return []

def save_and_sync_data(updated_ratings_df: pd.DataFrame, current_data: pd.DataFrame) -> None:
    """
    Recalculate ratings/ranks/movement, then upsert both Supabase tables
    via plain REST — no SDK required.
    """
    available = [p for p in PLAYERS if p in updated_ratings_df.columns]

    meta_cols = ["God", "Title", "Pantheon", "Role", "Class", "Attack Type", "Damage Type", "Rank"]
    temp_df   = pd.merge(current_data[meta_cols], updated_ratings_df, on="God", how="left")

    # Recalculate consensus rating
    scores_only       = temp_df[available].apply(pd.to_numeric, errors="coerce").replace(0, pd.NA)
    temp_df["Rating"] = scores_only.mean(axis=1, skipna=True).fillna(0).astype(int)
    temp_df["Tier"]   = temp_df["Rating"].apply(get_tier_from_rating)

    # New ranks + movement
    temp_df   = temp_df.sort_values(by=["Rating", "God"], ascending=[False, True])
    new_ranks = list(range(1, len(temp_df) + 1))
    old_ranks = pd.to_numeric(temp_df["Rank"], errors="coerce").fillna(len(temp_df))
    temp_df["Movement"] = old_ranks.astype(int) - new_ranks
    temp_df["Rank"]     = new_ranks

    # ── Upsert gods_metadata ──────────────────────────────────────────────────
    meta_records = []
    for _, row in temp_df.iterrows():
        meta_records.append({
            "god_name":    row["God"],
            "title":       row.get("Title"),
            "pantheon":    row.get("Pantheon"),
            "role":        row.get("Role"),
            "class":       row.get("Class"),
            "attack_type": row.get("Attack Type"),
            "damage_type": row.get("Damage Type"),
            "tier":        row["Tier"],
            "rank":        int(row["Rank"]),
            "movement":    int(row["Movement"]),
        })
    sb_upsert("gods_metadata", meta_records, on_conflict="god_name")

    # ── Upsert council_ratings ────────────────────────────────────────────────
    ratings_records = []
    for _, row in updated_ratings_df.iterrows():
        rec = {"god_name": row["God"]}
        for p in available:
            val = row.get(p)
            rec[p.lower()] = int(val) if pd.notna(val) and val > 0 else None
        ratings_records.append(rec)
    sb_upsert("council_ratings", ratings_records, on_conflict="god_name")

    st.cache_data.clear()


# ── 5. PAGE CONFIG & CSS INJECTION ───────────────────────────────────────────

st.set_page_config(
    page_title="SMITE 2 | High Council Hub",
    page_icon="🔱",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Background image (swap BACKGROUND_PATH to change the bg — no code changes needed)
_bg_b64 = encode_file_base64(BACKGROUND_PATH)
_bg_css = (
    f'url("data:image/png;base64,{_bg_b64}")'
    if _bg_b64
    else "linear-gradient(135deg, #0f172a 0%, #1e1b4b 100%)"
)

st.markdown(
    f"""
    <style>
    /* ── Layout ── */
    /* Keep collapse button visible so mobile users can open sidebar */
    [data-testid="stHeader"]                {{ background: rgba(0,0,0,0); }}
    .stAppViewMain > div:nth-child(1)       {{ padding-top: 0rem; }}
    /* Hide collapse button on desktop only */
    @media (min-width: 769px) {{
        [data-testid="stSidebarCollapseButton"] {{ display: none; }}
        [data-testid="collapsedControl"]        {{ display: none; }}
    }}

    /* ── STICKY TABS ── */
    [data-testid="stTabs"] > div:first-child {{
        position: sticky;
        top: 0;
        z-index: 999;
        background: rgba(10, 10, 20, 0.97);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border-bottom: 2px solid rgba(124,58,237,0.5);
        padding: 6px 0 0 0;
        box-shadow: 0 4px 20px rgba(0,0,0,0.6);
        margin-left: -1rem;
        margin-right: -1rem;
        padding-left: 1rem;
        padding-right: 1rem;
    }}

    /* Tab button styling with flair */
    [data-testid="stTabs"] button[role="tab"] {{
        font-weight: 700 !important;
        font-size: 0.78rem !important;
        letter-spacing: 0.5px !important;
        text-transform: uppercase !important;
        padding: 8px 12px !important;
        color: #94a3b8 !important;
        border-bottom: 2px solid transparent !important;
        transition: all 0.2s ease !important;
    }}
    [data-testid="stTabs"] button[role="tab"]:hover {{
        color: #a78bfa !important;
        border-bottom-color: #7c3aed !important;
    }}
    [data-testid="stTabs"] button[role="tab"][aria-selected="true"] {{
        color: #ffffff !important;
        border-bottom: 2px solid #7c3aed !important;
        background: rgba(124,58,237,0.15) !important;
        border-radius: 6px 6px 0 0 !important;
        box-shadow: 0 0 12px rgba(124,58,237,0.3) !important;
    }}

    /* Scrollable tab bar on mobile */
    [data-testid="stTabs"] > div:first-child > div {{
        overflow-x: auto !important;
        -webkit-overflow-scrolling: touch !important;
        scrollbar-width: none !important;
        flex-wrap: nowrap !important;
    }}
    [data-testid="stTabs"] > div:first-child > div::-webkit-scrollbar {{ display: none; }}

    /* ── MOBILE RESPONSIVE ── */
    @media (max-width: 768px) {{
        /* Reduce main padding */
        .main .block-container {{
            padding-left: 0.75rem !important;
            padding-right: 0.75rem !important;
            padding-top: 0.5rem !important;
            max-width: 100% !important;
        }}

        /* Hide sidebar on mobile by default */
        [data-testid="stSidebar"] {{
            display: none !important;
        }}

        /* Ticker bar — stack vertically on tiny screens */
        .top-ticker {{
            flex-wrap: wrap !important;
            gap: 6px !important;
            padding: 8px 12px !important;
            margin: -16px -12px 20px -12px !important;
        }}
        .ticker-item {{
            font-size: 0.7rem !important;
            letter-spacing: 0.5px !important;
        }}

        /* Title size */
        h1 {{ font-size: 1.4rem !important; letter-spacing: 1px !important; }}
        h2 {{ font-size: 1.1rem !important; }}
        h3 {{ font-size: 1rem !important; }}

        /* Podium — stack vertically on small screens */
        .podium-art {{
            max-width: 130px !important;
        }}

        /* God cards in H2H — 2 cols instead of 4 */
        .god-card {{
            padding: 12px !important;
        }}

        /* Rank rows — compact */
        .rank-row {{
            padding: 10px 12px !important;
            flex-wrap: wrap !important;
            gap: 6px !important;
        }}
        .rank-number {{
            font-size: 1.1rem !important;
            width: 36px !important;
        }}
        .rank-name {{
            margin-left: 10px !important;
        }}

        /* Input grid: 2 cols on mobile */
        .mobile-input-grid {{
            display: grid !important;
            grid-template-columns: 1fr 1fr !important;
            gap: 8px !important;
        }}

        /* Filter expander: single column */
        [data-testid="stExpander"] [data-testid="stHorizontalBlock"] {{
            flex-direction: column !important;
        }}

        /* Tab font smaller on mobile */
        [data-testid="stTabs"] button[role="tab"] {{
            font-size: 0.68rem !important;
            padding: 7px 8px !important;
        }}

        /* Progress bar row — stack */
        .progress-row {{
            flex-direction: column !important;
            align-items: flex-start !important;
            gap: 8px !important;
        }}

        /* Controversy row — 2 cols */
        .controversy-grid {{
            display: grid !important;
            grid-template-columns: 1fr 1fr !important;
            gap: 8px !important;
        }}
    }}

    @media (max-width: 480px) {{
        .top-ticker .ticker-item:nth-child(n+4) {{
            display: none;
        }}
        .podium-art {{
            max-width: 110px !important;
        }}
        [data-testid="stTabs"] button[role="tab"] {{
            font-size: 0.62rem !important;
            padding: 6px 6px !important;
        }}
    }}

    /* ── Mobile-specific multi-column overrides ── */
    @media (max-width: 640px) {{
        /* H2H disagreements: 2 cols */
        .h2h-grid [data-testid="stHorizontalBlock"] > div {{
            min-width: 45% !important;
        }}
        /* Council favorites: 1 col stacked */
        .favorites-grid > div {{
            width: 100% !important;
        }}
        /* Analytics stat cards: 2 col */
        .analytics-stats [data-testid="stHorizontalBlock"] > div {{
            min-width: 45% !important;
        }}
        /* God index & tier list: force 2-column wrapping */
        [data-testid="stHorizontalBlock"] {{
            flex-wrap: wrap !important;
        }}
        [data-testid="stHorizontalBlock"] > div[data-testid="stVerticalBlockBorderWrapper"] {{
            min-width: 48% !important;
            flex: 1 1 48% !important;
        }}
    }}
    @media (max-width: 400px) {{
        [data-testid="stHorizontalBlock"] > div[data-testid="stVerticalBlockBorderWrapper"] {{
            min-width: 100% !important;
            flex: 1 1 100% !important;
        }}
    }}

    /* ── Background ── */
    .stApp {{
        background: linear-gradient(rgba(10,10,20,0.82), rgba(10,10,20,0.82)), {_bg_css};
        background-size: cover;
        background-attachment: fixed;
        color: #f8fafc;
        font-family: 'Inter', sans-serif;
    }}

    /* ── Sidebar ── */
    [data-testid="stSidebar"] {{
        background: rgba(15,23,42,0.98) !important;
        border-right: 2px solid #7c3aed !important;
        box-shadow: 5px 0 20px rgba(124,58,237,0.3);
    }}

    /* ── Rank-movement glows ── */
    .up-shift {{
        box-shadow: 0 0 15px rgba(74,222,128,0.4) !important;
        border-color: #4ade80 !important;
        background: rgba(74,222,128,0.1) !important;
        animation: glow-green 3s infinite;
    }}
    .down-shift {{
        box-shadow: 0 0 15px rgba(248,113,113,0.4) !important;
        border-color: #f87171 !important;
        background: rgba(248,113,113,0.1) !important;
        animation: glow-red 3s infinite;
    }}

    /* ── Podium ── */
    .podium-container {{
        display: flex; justify-content: space-around; align-items: flex-end;
        background: rgba(124,58,237,0.05); border: 1px solid rgba(124,58,237,0.2);
        padding: 20px; border-radius: 16px; margin-bottom: 30px; gap: 10px;
    }}
    .podium-item {{
        text-align: center; flex: 1; padding: 10px; border-radius: 10px;
        background: rgba(30,41,59,0.5); border: 1px solid transparent;
    }}
    .podium-1 {{
        border-color: gold; transform: scale(1.1);
        background: rgba(255,215,0,0.1); z-index: 2;
    }}

    /* ── God card (plain, used in H2H / controversy / favorites) ── */
    .god-card {{
        background: rgba(30,41,59,0.5); border: 1px solid rgba(124,58,237,0.3);
        padding: 20px; border-radius: 16px; text-align: center;
        backdrop-filter: blur(12px); transition: all 0.3s ease-in-out;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3); margin-bottom: 20px;
    }}
    .god-card:hover {{
        transform: translateY(-8px); border-color: #7c3aed;
        box-shadow: 0 0 20px rgba(124,58,237,0.4); background: rgba(30,41,59,0.7);
    }}

    /* ── Updated Art card (God Index) to match Podium ── */
    .art-card {{
        border-radius: 16px; 
        overflow: hidden; 
        position: relative;
        border: 1px solid rgba(255,255,255,0.1);
        box-shadow: 0 4px 20px rgba(0,0,0,0.5);
        margin-bottom: 20px; 
        transition: all 0.3s ease-in-out;
        background: #0f172a;
        /* Scaling down: 5 columns + max-width keeps images sharp */
        max-width: 320px;
        margin-left: auto;
        margin-right: auto;
    }}
    
    .art-card:hover {{
        transform: translateY(-8px);
        border-color: rgba(255,255,255,0.3);
        box-shadow: 0 12px 30px rgba(0,0,0,0.8);
    }}

    /* Natural image flow (like podium) */
    .art-card img {{
        width: 100%; 
        aspect-ratio: 2/3; 
        object-fit: cover; 
        object-position: center 15%; 
        display: block;
        /* Sharpness Fix */
        image-rendering: -webkit-optimize-contrast;
        image-rendering: crisp-edges;
    }}

    /* Rank Tag - Top Right */
    .rank-tag {{
        position: absolute;
        top: 12px;
        left: 12px;
        z-index: 2;
        color: #ffffff;
        font-size: 0.8rem;
        font-weight: 900;
        background: rgba(0,0,0,0.6);
        padding: 2px 10px;
        border-radius: 6px;
        backdrop-filter: blur(4px);
        border: 1px solid rgba(255,255,255,0.2);
    }}

    .art-card-overlay {{
        position: absolute; 
        bottom: 0; left: 0; right: 0;
        /* Deep gradient for text legibility */
        background: linear-gradient(
            to bottom, 
            transparent 0%, 
            rgba(10,10,20,0.4) 20%,
            rgba(10,10,20,0.95) 70%, 
            rgba(10,10,20,1) 100%
        );
        padding: 14px;
        z-index: 1;
    }}

    /* ── Tier badges with Fixed Glow ── */
    .tier-badge {{ 
        font-weight: 900; 
        padding: 3px 10px; 
        border-radius: 8px; 
        font-size: 0.75rem; 
        display: inline-block; 
        text-shadow: 1px 1px 2px rgba(0,0,0,0.5);
    }}
    
    /* Using specific CSS classes for the glows */
    .ss-rank {{ background:#ef4444; box-shadow: 0 0 15px rgba(239,68,68,0.6); }}
    .s-rank  {{ background:#f59e0b; box-shadow: 0 0 15px rgba(245,158,11,0.6); }}
    .a-rank  {{ background:#7c3aed; box-shadow: 0 0 15px rgba(124,58,237,0.6); }}
    .b-rank  {{ background:#3b82f6; box-shadow: 0 0 15px rgba(59,130,246,0.6); }}
    .c-rank  {{ background:#10b981; box-shadow: 0 0 15px rgba(16,185,129,0.6); }}
    .d-rank  {{ background:#64748b; box-shadow: 0 0 15px rgba(100,116,139,0.6); }}
    .f-rank  {{ background:#991b1b; box-shadow: 0 0 15px rgba(153,27,27,0.6); }}
    .u-rank  {{ background:#475569; box-shadow: 0 0 15px rgba(71,85,105,0.6); }}

    /* ── Podium art card ── */
    .podium-art {{
        border-radius: 14px; overflow: hidden; position: relative;
        border: 2px solid transparent;
        box-shadow: 0 4px 20px rgba(0,0,0,0.6);
        flex: 1; transition: all 0.3s ease-in-out;
        background: #0f172a;
        max-width: 220px;
        min-width: 0;
    }}
    .podium-art:hover {{ transform: translateY(-4px); }}
    .podium-art img {{
        width: 100%; aspect-ratio: 2/3; object-fit: cover;
        object-position: center top; display: block;
    }}
    .podium-art-overlay {{
        position: absolute; bottom: 0; left: 0; right: 0;
        background: linear-gradient(to bottom, transparent 0%, rgba(10,10,20,0.75) 35%, rgba(10,10,20,0.98) 100%);
        padding: 12px 14px 14px; text-align: center;
    }}
    .podium-1-art {{ border-color: gold; box-shadow: 0 0 30px rgba(255,215,0,0.35); }}
    .podium-2-art {{ border-color: silver; box-shadow: 0 0 20px rgba(192,192,192,0.2); }}
    .podium-3-art {{ border-color: #cd7f32; box-shadow: 0 0 20px rgba(205,127,50,0.2); }}

    /* ── Podium mobile: stack vertically, gold on top ── */
    @media (max-width: 600px) {{
        .podium-row {{
            flex-direction: column !important;
            align-items: center !important;
            gap: 12px !important;
        }}
        .podium-art {{
            max-width: 82% !important;
            width: 82% !important;
            transform: none !important;
        }}
        .podium-1-art {{
            order: -1 !important;
        }}
        .podium-art img {{
            aspect-ratio: 3/2 !important;
            object-position: center 20% !important;
        }}
        .podium-art-overlay {{
            padding: 8px 10px 10px !important;
        }}
    }}

    /* ── Tier badges ── */
    .tier-badge {{ font-weight:800; padding:5px 15px; border-radius:20px; font-size:0.85rem; display:inline-block; margin-bottom:10px; }}
    .ss-rank {{ background:#ef4444; box-shadow:0 0 10px #ef4444; }}
    .s-rank  {{ background:#f59e0b; box-shadow:0 0 10px #f59e0b; }}
    .a-rank  {{ background:#7c3aed; box-shadow:0 0 10px #7c3aed; }}
    .b-rank  {{ background:#3b82f6; box-shadow:0 0 10px #3b82f6; }}
    .c-rank  {{ background:#10b981; box-shadow:0 0 10px #10b981; }}
    .d-rank  {{ background:#64748b; box-shadow:0 0 10px #64748b; }}
    .f-rank  {{ background:#991b1b; box-shadow:0 0 10px #991b1b; }}
    .u-rank  {{ background:#475569; box-shadow:0 0 10px #475569; }}

    /* ── Power rankings row ── */
    .rank-row {{
        display:flex; align-items:center; background:rgba(30,41,59,0.4);
        border:1px solid rgba(124,58,237,0.2); padding:15px 25px;
        border-radius:12px; margin-bottom:10px; transition: transform 0.2s;
    }}
    .rank-row:hover {{ transform:scale(1.01); border-color:#7c3aed; background:rgba(30,41,59,0.6); }}
    .rank-number {{ font-size:1.5rem; font-weight:900; color:#7c3aed; width:50px; }}
    .rank-name   {{ flex-grow:1; margin-left:20px; }}
    .rank-meta   {{ font-size:0.8rem; color:#94a3b8; text-transform:uppercase; }}
    .pantheon-icon {{ width:28px; height:28px; margin-right:10px; vertical-align:middle; filter:drop-shadow(0 0 5px rgba(124,58,237,0.5)); }}

    /* ── Top ticker ── */
    .top-ticker {{
        background:rgba(15,23,42,0.8); border-bottom:2px solid #7c3aed;
        padding:10px 20px; margin:-50px -50px 30px -50px;
        display:flex; justify-content:space-between; align-items:center;
        box-shadow:0 0 20px rgba(124,58,237,0.4);
    }}
    .ticker-item {{ font-size:0.8rem; font-weight:700; text-transform:uppercase; letter-spacing:1px; }}
    .pulse {{
        height:8px; width:8px; background:#ef4444; border-radius:50%;
        display:inline-block; margin-right:8px; box-shadow:0 0 8px #ef4444;
        animation:pulse-red 2s infinite;
    }}
    @keyframes pulse-red {{
        0%   {{ transform:scale(0.95); box-shadow:0 0 0 0 rgba(239,68,68,0.7); }}
        70%  {{ transform:scale(1);    box-shadow:0 0 0 10px rgba(239,68,68,0); }}
        100% {{ transform:scale(0.95); box-shadow:0 0 0 0 rgba(239,68,68,0); }}
    }}

    /* ── Global headings ── */
    h1, h2, h3 {{ color:#ffffff; text-transform:uppercase; letter-spacing:2px; }}

    /* ── Sidebar label/widget overrides ── */
    [data-testid="stExpander"] summary p {{ color:#8b5cf6 !important; font-weight:800 !important; }}
    [data-testid="stWidgetLabel"] p {{
        color:#8b5cf6 !important; font-weight:700 !important;
        text-transform:uppercase; font-size:0.85rem;
    }}

    /* ── Primary buttons ── */
    button[kind="primary"] {{
        background-color:#7c3aed !important; color:#ffffff !important;
        border:1px solid #a78bfa !important; font-weight:800 !important;
        letter-spacing:1px; transition:all 0.2s ease-in-out;
    }}
    button[kind="primary"]:hover {{
        background-color:#6d28d9 !important; border-color:#ffffff !important;
        box-shadow:0 0 15px rgba(124,58,237,0.6);
    }}

    /* ── Input-tab +/− buttons ── */
    div[data-testid="column"] button[kind="secondary"] {{
        background:rgba(124,58,237,0.2) !important; border:1px solid #7c3aed !important;
        color:#ffffff !important; font-size:1.1rem !important; font-weight:900 !important; padding:2px 0 !important;
    }}
    div[data-testid="column"] button[kind="secondary"]:hover {{
        background:rgba(124,58,237,0.5) !important; border-color:#a78bfa !important;
        box-shadow:0 0 10px rgba(124,58,237,0.5);
    }}
    div[data-testid="column"]:first-child button[kind="secondary"] {{
        background:rgba(239,68,68,0.15) !important; border-color:#ef4444 !important;
    }}
    div[data-testid="column"]:first-child button[kind="secondary"]:hover {{
        background:rgba(239,68,68,0.35) !important;
    }}
    div[data-testid="column"]:last-child button[kind="secondary"] {{
        background:rgba(74,222,128,0.15) !important; border-color:#4ade80 !important;
    }}
    div[data-testid="column"]:last-child button[kind="secondary"]:hover {{
        background:rgba(74,222,128,0.35) !important;
    }}

    /* ── Number input mobile touch-friendly sizing ── */
    @media (max-width: 768px) {{
        /* Bigger tap targets for number input +/- buttons */
        div[data-testid="column"] button[kind="secondary"] {{
            min-height: 40px !important;
            font-size: 1.3rem !important;
        }}
        /* Number input text larger */
        input[type="number"] {{
            font-size: 1rem !important;
            text-align: center !important;
        }}
        /* Expand multiselect tags so they're tappable */
        [data-testid="stMultiSelect"] span {{
            font-size: 0.8rem !important;
            padding: 3px 8px !important;
        }}
        /* Bigger select boxes */
        [data-testid="stSelectbox"] select,
        [data-testid="stSelectbox"] > div {{
            font-size: 1rem !important;
            min-height: 44px !important;
        }}
        /* Bigger text inputs */
        [data-testid="stTextInput"] input {{
            font-size: 1rem !important;
            min-height: 44px !important;
        }}
        /* Expander header bigger tap target */
        [data-testid="stExpander"] summary {{
            padding: 14px 16px !important;
            font-size: 1rem !important;
        }}
        /* All buttons bigger on mobile */
        button {{
            min-height: 44px !important;
        }}
        /* Leaderboard rank rows more compact on mobile */
        .rank-meta {{
            font-size: 0.7rem !important;
            flex-wrap: wrap !important;
        }}
        /* God index overlay text scale down */
        .art-card-overlay {{
            padding: 8px !important;
        }}
        /* Tier list god cells */
        .tier-cell {{
            font-size: 0.8rem !important;
        }}
        /* Section headers */
        .section-header {{
            font-size: 0.85rem !important;
            padding: 8px 12px !important;
        }}
        /* Podium cards — shrink overlay text */
        .podium-art-overlay {{
            padding: 8px 10px 10px !important;
        }}
    }}

    /* ── Smooth scroll for anchor links ── */
    html {{ scroll-behavior: smooth; }}

    /* ── Scrollbar styling (desktop) ── */
    ::-webkit-scrollbar {{ width: 6px; height: 6px; }}
    ::-webkit-scrollbar-track {{ background: rgba(15,23,42,0.5); }}
    ::-webkit-scrollbar-thumb {{ background: #7c3aed; border-radius: 3px; }}
    ::-webkit-scrollbar-thumb:hover {{ background: #a78bfa; }}

    /* ── Mobile list view vs desktop grid view (Rankings tab) ── */
    .mobile-list-view  {{ display: none; }}
    .desktop-grid-view {{ display: block; }}
    @media (max-width: 768px) {{
        .mobile-list-view  {{ display: block !important; }}
        .desktop-grid-view {{ display: none !important; }}
    }}
    </style>
    """,
    unsafe_allow_html=True,
)


# ── 6. DATA LOAD & VALIDATION ────────────────────────────────────────────────

try:
    data, raw_input, tiers = load_titan_data()
except Exception as e:
    st.error(f"⚠️ Data load error: {e}")
    st.stop()

if data is None:
    st.error(
        "⚠️ No data returned from Supabase. "
        "Check that `gods_metadata` and `council_ratings` tables exist and contain rows."
    )
    st.stop()

# Build god image map once — cached, no per-render file I/O
GOD_IMAGE_MAP      = build_god_image_map()
PANTHEON_IMAGE_MAP = build_pantheon_image_map()
ALL_RANKINGS       = load_all_rankings()  # {player: {god: rank}} for pills


# ── 7. TOP STATUS BAR ────────────────────────────────────────────────────────

total_gods  = len(data)
avg_rating  = int(data["Rating"].mean())
ss_count    = int((data["Tier"] == "SS").sum())

st.markdown(
    f"""
    <div class="top-ticker" style="flex-wrap:wrap;gap:8px;">
        <div class="ticker-item"><span class="pulse"></span>LIVE META</div>
        <div class="ticker-item" style="color:#94a3b8;">
            ROSTER: <span style="color:#7c3aed;">{total_gods} GODS</span>
        </div>
        <div class="ticker-item" style="color:#94a3b8;">
            AVG: <span style="color:#7c3aed;">{avg_rating} PTS</span>
        </div>
        <div class="ticker-item" style="color:#94a3b8;">
            SS: <span style="color:#7c3aed;">{ss_count}</span>
        </div>
        <div class="ticker-item" style="color:#ef4444;">v2.0</div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.title("🔱 SMITE 2 HIGH COUNCIL HUB")


# ── 8. PODIUM & CONTROVERSY WIDGETS ─────────────────────────────────────────

with st.expander("🏆 THE PODIUM", expanded=False):
    top3 = data.sort_values(by="Rating", ascending=False).head(3).reset_index(drop=True)
    if len(top3) >= 3:
        p2, p1, p3 = top3.iloc[1], top3.iloc[0], top3.iloc[2]

        p1_img = get_god_image_url(p1['God'], GOD_IMAGE_MAP)
        p2_img = get_god_image_url(p2['God'], GOD_IMAGE_MAP)
        p3_img = get_god_image_url(p3['God'], GOD_IMAGE_MAP)
        p1_tier_color = TIER_COLORS.get(str(p1['Tier']).upper().strip(), '#475569')
        p2_tier_color = TIER_COLORS.get(str(p2['Tier']).upper().strip(), '#475569')
        p3_tier_color = TIER_COLORS.get(str(p3['Tier']).upper().strip(), '#475569')

        st.markdown(
            f"""
            <div class="podium-row" style="display:flex;justify-content:center;align-items:flex-end;gap:16px;
                        padding:20px 10px;margin-bottom:10px;flex-wrap:wrap;">
                <div class="podium-art podium-2-art" style="margin-bottom:0;align-self:flex-end;">
                    <img src="{p2_img}" onerror="this.onerror=null;this.style.background='#1e293b';" />
                    <div class="podium-art-overlay">
                        <div style="font-size:1.4rem;margin-bottom:2px;">🥈</div>
                        <div style="font-weight:800;color:white;font-size:1rem;">{p2['God']}</div>
                        <div style="color:#94a3b8;font-size:0.75rem;">{p2['Title'] if pd.notna(p2['Title']) else ''}</div>
                        <div style="color:silver;font-weight:900;font-size:1.1rem;margin-top:3px;">{int(p2['Rating'])} PTS</div>
                        <div style="margin-top:4px;">
                            <span style="background:{p2_tier_color};color:white;font-size:0.7rem;
                                font-weight:800;padding:1px 8px;border-radius:8px;">{str(p2['Tier']).upper()}</span>
                        </div>
                    </div>
                </div>
                <div class="podium-art podium-1-art" style="transform:scale(1.06);transform-origin:bottom center;">
                    <img src="{p1_img}" onerror="this.onerror=null;this.style.background='#1e293b';" />
                    <div class="podium-art-overlay">
                        <div style="font-size:1.8rem;margin-bottom:2px;">🥇</div>
                        <div style="font-weight:900;color:white;font-size:1.15rem;">{p1['God']}</div>
                        <div style="color:#94a3b8;font-size:0.75rem;">{p1['Title'] if pd.notna(p1['Title']) else ''}</div>
                        <div style="color:gold;font-weight:900;font-size:1.3rem;margin-top:3px;">{int(p1['Rating'])} PTS</div>
                        <div style="margin-top:4px;">
                            <span style="background:{p1_tier_color};color:white;font-size:0.7rem;
                                font-weight:800;padding:1px 8px;border-radius:8px;">{str(p1['Tier']).upper()}</span>
                        </div>
                    </div>
                </div>
                <div class="podium-art podium-3-art" style="margin-bottom:0;align-self:flex-end;">
                    <img src="{p3_img}" onerror="this.onerror=null;this.style.background='#1e293b';" />
                    <div class="podium-art-overlay">
                        <div style="font-size:1.4rem;margin-bottom:2px;">🥉</div>
                        <div style="font-weight:800;color:white;font-size:1rem;">{p3['God']}</div>
                        <div style="color:#94a3b8;font-size:0.75rem;">{p3['Title'] if pd.notna(p3['Title']) else ''}</div>
                        <div style="color:#cd7f32;font-weight:900;font-size:1.1rem;margin-top:3px;">{int(p3['Rating'])} PTS</div>
                        <div style="margin-top:4px;">
                            <span style="background:{p3_tier_color};color:white;font-size:0.7rem;
                                font-weight:800;padding:1px 8px;border-radius:8px;">{str(p3['Tier']).upper()}</span>
                        </div>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

with st.expander("⚡ MOST CONTROVERSIAL GODS", expanded=False):
    available_players = [p for p in PLAYERS if p in data.columns]
    if len(available_players) >= 2:
        cont_df = data[data[available_players].notna().sum(axis=1) >= 2].copy()
        cont_df["MaxDiff"] = (
            cont_df[available_players].max(axis=1) - cont_df[available_players].min(axis=1)
        )
        top_cont = cont_df.sort_values("MaxDiff", ascending=False).head(5)

        cont_cols = st.columns(min(5, len(top_cont)))
        for i, (_, row) in enumerate(top_cont.iterrows()):
            with cont_cols[i]:
                rater_scores = {
                    p: int(row[p])
                    for p in available_players
                    if pd.notna(row[p]) and row[p] > 0
                }
                score_str = " vs ".join(
                    f'<span style="color:{COUNCIL_COLORS.get(p,"#7c3aed")};">{PLAYER_ABBR.get(p, p[:2])}:{v}</span>'
                    for p, v in rater_scores.items()
                )
                st.markdown(
                    f"""
                    <div class="god-card" style="border-color:#f59e0b;box-shadow:0 0 15px rgba(245,158,11,0.3);">
                        <div style="font-size:1.2rem;">⚡</div>
                        <div style="font-weight:800;color:white;font-size:1rem;">{row['God']}</div>
                        <div style="color:#f59e0b;font-size:0.7rem;font-weight:700;margin:4px 0;">
                            SPLIT: {int(row['MaxDiff'])} PTS
                        </div>
                        <div style="font-size:0.75rem;margin-top:6px;">{score_str}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )


# ── 9. GLOBAL FILTERS ────────────────────────────────────────────────────────

# On mobile: auto-collapse the Podium and Controversial expanders so users
# land on the tabs faster. JS clicks any open expander summary on small screens.
st.markdown(
    """
    <script>
    (function() {
        function collapseOnMobile() {
            if (window.innerWidth > 768) return;
            var summaries = window.parent.document.querySelectorAll(
                '[data-testid="stExpander"] summary'
            );
            summaries.forEach(function(s) {
                var label = s.innerText || '';
                if (label.includes('PODIUM') || label.includes('CONTROVERSIAL')) {
                    var details = s.closest('details');
                    if (details && details.open) { s.click(); }
                }
            });
        }
        // Run after Streamlit finishes rendering
        if (document.readyState === 'complete') { collapseOnMobile(); }
        else { window.addEventListener('load', collapseOnMobile); }
    })();
    </script>
    """,
    unsafe_allow_html=True,
)

with st.expander("⚙️ FILTERS", expanded=False):
    f_col1, f_col2 = st.columns(2)
    with f_col1:
        g_search   = st.text_input("🔍 Search God", placeholder="Zeus, Bellona...")
        g_role     = st.multiselect("Role", sorted(data["Role"].dropna().unique()))
        g_class    = st.multiselect("Class", sorted(data["Class"].dropna().unique()))
    with f_col2:
        g_pantheon = st.multiselect("Pantheon", sorted(data["Pantheon"].dropna().unique()))
        g_atk      = st.multiselect("Attack Type", sorted(data["Attack Type"].dropna().unique()))
        g_dmg      = st.multiselect("Damage Type", sorted(data["Damage Type"].dropna().unique()))

filtered_data = data.copy()
if g_search:   filtered_data = filtered_data[filtered_data["God"].str.contains(g_search, case=False, na=False)]
if g_role:     filtered_data = filtered_data[filtered_data["Role"].isin(g_role)]
if g_class:    filtered_data = filtered_data[filtered_data["Class"].isin(g_class)]
if g_pantheon: filtered_data = filtered_data[filtered_data["Pantheon"].isin(g_pantheon)]
if g_atk:      filtered_data = filtered_data[filtered_data["Attack Type"].isin(g_atk)]
if g_dmg:      filtered_data = filtered_data[filtered_data["Damage Type"].isin(g_dmg)]


# ── 10. SIDEBAR — LIVE RANKINGS ──────────────────────────────────────────────

with st.sidebar:
    st.markdown(
        "<h3 style='text-align:center;color:#a78bfa;margin-bottom:15px;'>LIVE RANKINGS</h3>",
        unsafe_allow_html=True,
    )

    sidebar_items = []
    for _, row in filtered_data.sort_values(by=["Rating", "God"], ascending=[False, True]).iterrows():
        tier_val  = str(row["Tier"]).upper().strip()
        t_color   = TIER_COLORS.get(tier_val, "#475569")
        mvt       = row.get("Movement", 0)
        css_class, mvt_html = movement_indicator_html(mvt)

        sidebar_items.append(
            f'<div class="{css_class}" style="display:flex;justify-content:space-between;align-items:center;'
            f'padding:6px 10px;background:rgba(30,41,59,0.5);border-left:3px solid {t_color};'
            f'border-radius:4px;margin-bottom:8px;">'
            f'<div style="display:flex;align-items:center;gap:8px;">'
            f'<div style="display:flex;flex-direction:column;align-items:center;width:25px;">'
            f'<span style="color:{t_color};font-weight:900;font-size:0.9rem;line-height:1;">{row["Rank"]}</span>'
            f'{mvt_html}</div>'
            f'<span style="color:white;font-weight:700;font-size:0.85rem;">{row["God"]}</span>'
            f'</div>'
            f'<div style="text-align:right;line-height:1.1;">'
            f'<div style="color:{t_color};font-weight:800;font-size:0.7rem;">{tier_val}</div>'
            f'<div style="color:#94a3b8;font-size:0.6rem;">{int(row["Rating"])} PTS</div>'
            f'</div></div>'
        )

    st.markdown(
        f'<div style="display:flex;flex-direction:column;">{"".join(sidebar_items)}</div>',
        unsafe_allow_html=True,
    )


# ── 11. TABS ─────────────────────────────────────────────────────────────────

t_index, t_leaderboard, t_indiv, t_tier, t_analytics, t_h2h, t_activity, t_ranker = st.tabs([
    "🎮 INDEX",
    "♛ RANKINGS",
    "👑 FAVORITES",
    "📊 TIERLIST",
    "📈 ANALYTICS",
    "⚔️ H2H",
    "📜 ACTIVITY",
    "⚡ RATE & RANK",
])


# ── TAB: GOD INDEX ───────────────────────────────────────────────────────────

with t_index:
    if filtered_data.empty:
        st.warning("No gods match your current filters.")
    else:
        for i in range(0, len(filtered_data), GRID_COLS):
            cols = st.columns(GRID_COLS)
            for j, (_, row) in enumerate(filtered_data.iloc[i : i + GRID_COLS].iterrows()):
                with cols[j]:
                    t_val   = str(row["Tier"]).upper().strip()
                    t_color = TIER_COLORS.get(t_val, "#475569")
                    t_class = f"{t_val.lower()}-rank"
                    icon_url = get_god_image_url(row["God"], GOD_IMAGE_MAP)

                    st.markdown(
                        f"""
                        <div class="art-card" style="border-color:{t_color}55;">
                            <div class="rank-tag" style="border-color:{t_color}88;">#{row['Rank']}</div>
                            <img src="{icon_url}" onerror="this.onerror=null;this.style.background='#1e293b';" />
                            <div class="art-card-overlay">
                                <div style="display:flex; justify-content:space-between; align-items:flex-end;">
                                    <div style="flex-grow:1;">
                                        <div style="font-size:0.55rem; font-weight:800; text-transform:uppercase;
                                            letter-spacing:1.2px; color:#94a3b8; margin-bottom:1px; opacity:0.97;">
                                            {row['Title']}
                                        </div>
                                        <div style="font-weight:900; font-size:1.2rem; color:white; line-height:1; margin-bottom:4px;">
                                            {row['God']}
                                        </div>
                                        <div style="color:#94a3b8; font-size:0.65rem; font-weight:600; margin-bottom:1px;">
                                            {row['Role']} <span style="color:{t_color}; opacity:0.4;">•</span> {row['Pantheon']}
                                        </div>
                                    </div>
                                    <div style="display:flex; flex-direction:column; align-items:center; margin-left:10px;">
                                        <span class="tier-badge {t_class}" style="margin-bottom:1px; transform:scale(0.9);">{t_val}</span>
                                        <div style="color:{t_color}; font-weight:900; font-size:1.4rem; 
                                            text-shadow:0 0 10px {t_color}44; line-height:1; margin-top:2px;">
                                            {int(row['Rating'])}
                                        </div>
                                    </div>
                                </div>
                                {council_ratings_html(row, ALL_RANKINGS)}
                                <div style="margin-top:4px; align-items:center;">{get_hot_take_html(row)}</div>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )


# ── TAB: POWER RANKINGS ──────────────────────────────────────────────────────

with t_leaderboard:
    st.subheader("High Council Power Rankings")
    sorted_df = filtered_data.sort_values(
        by=["Rating", "God"], ascending=[False, True], na_position="last"
    )

    # Build all rows as HTML once — show compact version on mobile, full on desktop
    full_rows_html = []
    compact_rows_html = []

    for _, row in sorted_df.iterrows():
        raw_rating     = row["Rating"]
        is_unrated     = pd.isna(raw_rating) or raw_rating == 0
        tier           = "U" if is_unrated else str(row["Tier"]).upper().strip()
        rating_display = "UNRATED" if is_unrated else f"{int(raw_rating)} PTS"
        rank_display   = "--" if is_unrated else str(row["Rank"])
        tier_color     = TIER_COLORS.get(tier, "#475569")
        pantheon_img   = get_pantheon_icon_src(row["Pantheon"], PANTHEON_IMAGE_MAP)

        lb_pills = []
        for p in PLAYERS:
            if p in row and pd.notna(row[p]) and row[p] > 0:
                c = COUNCIL_COLORS.get(p, "#7c3aed")
                lb_pills.append(
                    f'<span style="font-size:0.7rem;font-weight:800;color:{c};'
                    f'background:rgba(255,255,255,0.05);border:1px solid {c}55;'
                    f'border-radius:5px;padding:1px 6px;margin-left:4px;">'
                    f'{PLAYER_ABBR.get(p, p[:2])} {int(row[p])}</span>'
                )

        # ── Desktop full row ──
        full_rows_html.append(
            f'<div class="rank-row" style="border-left:4px solid {tier_color};">'
            f'<div class="rank-number" style="color:{tier_color if not is_unrated else "#475569"};">'
            f'{rank_display}</div>'
            f'<div class="rank-name">'
            f'<div style="font-weight:800;font-size:1.2rem;color:white;">{row["God"]}'
            f'<span style="font-size:0.8rem;font-weight:400;color:#94a3b8;margin-left:10px;">'
            f'{row["Title"] if pd.notna(row["Title"]) else ""}</span></div>'
            f'<div class="rank-meta" style="display:flex;align-items:center;flex-wrap:wrap;gap:4px;">'
            f'<img src="{pantheon_img}" class="pantheon-icon">'
            f'{row["Pantheon"]} • {row["Role"]} • {row["Class"]}'
            f'<span style="margin-left:8px;">{"".join(lb_pills)}</span></div>'
            f'</div>'
            f'<div style="text-align:right;">'
            f'<div style="color:{tier_color};font-weight:900;font-size:1.1rem;">{tier} TIER</div>'
            f'<div style="color:white;font-size:0.8rem;opacity:0.6;">{rating_display}</div>'
            f'</div></div>'
        )

        # ── Mobile compact row ──
        compact_rows_html.append(
            f'<div style="display:flex;align-items:center;gap:10px;padding:8px 10px;'
            f'border-bottom:1px solid rgba(255,255,255,0.06);border-left:3px solid {tier_color};">'
            f'<div style="width:28px;text-align:center;flex-shrink:0;">'
            f'<span style="color:{tier_color};font-weight:900;font-size:0.95rem;">{rank_display}</span>'
            f'</div>'
            f'<div style="flex:1;min-width:0;">'
            f'<span style="font-weight:800;color:white;font-size:0.9rem;">{row["God"]}</span>'
            f'<div style="color:#64748b;font-size:0.65rem;">{row["Pantheon"]} · {row["Role"]}</div>'
            f'<div style="margin-top:2px;">{"".join(lb_pills)}</div>'
            f'</div>'
            f'<div style="text-align:right;flex-shrink:0;">'
            f'<div style="background:{tier_color};color:white;font-size:0.65rem;font-weight:900;'
            f'padding:1px 6px;border-radius:5px;display:inline-block;">{tier}</div>'
            f'<div style="color:{tier_color};font-weight:900;font-size:0.95rem;">{int(raw_rating) if not is_unrated else "—"}</div>'
            f'</div></div>'
        )

    st.markdown(
        f'<div class="desktop-grid-view">{"".join(full_rows_html)}</div>'
        f'<div class="mobile-list-view" style="display:none;">'
        f'<div style="background:rgba(15,23,42,0.6);border-radius:12px;'
        f'border:1px solid rgba(124,58,237,0.2);overflow:hidden;">'
        + "".join(compact_rows_html)
        + '</div></div>',
        unsafe_allow_html=True,
    )


# ── TAB: COUNCIL FAVORITES ───────────────────────────────────────────────────

with t_indiv:
    st.subheader("👤 THE HIGH COUNCIL'S TOP 5")
    p_cols = st.columns(len(PLAYERS))   # scales with roster size

    for i, p in enumerate(PLAYERS):
        with p_cols[i]:
            actual_col = next(
                (c for c in data.columns if str(c).strip().lower() == p.lower()), None
            )
            if actual_col:
                p_top = (
                    filtered_data.dropna(subset=[actual_col])
                    .query(f"`{actual_col}` > 0")
                    .sort_values(by=actual_col, ascending=False)
                    .head(5)
                )
                if not p_top.empty:
                    rows_html = "".join(
                        f'<div style="display:flex;justify-content:space-between;padding:8px 0;'
                        f'border-bottom:1px solid rgba(255,255,255,0.05);">'
                        f'<span style="font-weight:500;font-size:0.9em;">{r["God"]}</span>'
                        f'<b style="color:#4ade80;">{int(r[actual_col])}</b></div>'
                        for _, r in p_top.iterrows()
                    )
                    color = COUNCIL_COLORS.get(p, "#7c3aed")
                    st.markdown(
                        f'<div class="god-card" style="border-left:4px solid {color};">'
                        f'<div style="color:{color};font-size:1rem;font-weight:800;'
                        f'text-transform:uppercase;margin-bottom:15px;">{p}</div>'
                        f'{rows_html}</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f'<div class="god-card" style="opacity:0.5;">{p}<br>'
                        f'<small>No ratings match filters</small></div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.markdown(
                    f'<div class="god-card" style="opacity:0.5;border-color:#ef4444;">'
                    f'{p}<br><small>Column not found in data</small></div>',
                    unsafe_allow_html=True,
                )


# ── TAB: TIER LIST ───────────────────────────────────────────────────────────

with t_tier:
    st.subheader("📊 TIERLIST")

    for t_label in [t for _, t in TIER_THRESHOLDS] + ["U"]:
        color      = TIER_COLORS.get(t_label, "#475569")
        tier_gods  = filtered_data[filtered_data["Tier"] == t_label]
        if tier_gods.empty:
            continue

        st.markdown(
            f'<div style="background:linear-gradient(90deg,{color},transparent);'
            f'border-left:5px solid {color};padding:10px 16px;border-radius:4px;'
            f'margin:16px 0 8px 0;display:flex;justify-content:space-between;align-items:center;">'
            f'<span style="font-weight:800;font-size:1.1rem;">{t_label} TIER</span>'
            f'<span style="font-size:0.7em;opacity:0.8;">{len(tier_gods)} GODS</span></div>',
            unsafe_allow_html=True,
        )

        for row_start in range(0, len(tier_gods), TIER_COLS):
            row_cols = st.columns(TIER_COLS)
            for j, (_, row) in enumerate(tier_gods.iloc[row_start : row_start + TIER_COLS].iterrows()):
                with row_cols[j]:
                    pill_bits = [
                        f'<span style="color:{COUNCIL_COLORS.get(p,"#7c3aed")};'
                        f'font-size:0.6rem;font-weight:700;">{PLAYER_ABBR.get(p, p[:2])}:{int(row[p])}</span>'
                        for p in PLAYERS
                        if p in row and pd.notna(row[p]) and row[p] > 0
                    ]
                    pills_str = ' <span style="color:#334155;">|</span> '.join(pill_bits)
                    st.markdown(
                        f"<div style='text-align:center;font-size:0.9em;padding:6px 5px 4px;"
                        f"background:rgba(255,255,255,0.03);border-radius:4px;margin-bottom:4px;'>"
                        f"<div style='font-weight:700;color:white;'>{row['God']}</div>"
                        f"<div style='margin-top:3px;'>{pills_str}</div></div>",
                        unsafe_allow_html=True,
                    )


# ── TAB: ANALYTICS ───────────────────────────────────────────────────────────
with t_analytics:
    # --- 1. Progress Boxes (Moved to Top) ---
    st.markdown("### 📊 COUNCIL COMPLETION PROGRESS")
    progress_cols = st.columns(len(PLAYERS))
    total_gods = len(data)
    
    for i, player in enumerate(PLAYERS):
        # Count how many gods this player has rated (>0)
        count = data[data[player] > 0][player].count() if player in data.columns else 0
        pct = (count / total_gods) * 100
        color = COUNCIL_COLORS.get(player, "#7c3aed")
        
        with progress_cols[i]:
            st.markdown(f"""
                <div style="background:rgba(255,255,255,0.05); padding:15px; border-radius:12px; border-left:4px solid {color}; text-align:center;">
                    <div style="font-size:0.8rem; color:{color}; font-weight:800; text-transform:uppercase;">{player}</div>
                    <div style="font-size:1.8rem; font-weight:900; color:white;">{count}<span style="font-size:0.9rem; color:#64748b;">/{total_gods}</span></div>
                    <div style="font-size:0.75rem; color:#94a3b8;">{pct:.1f}% Complete</div>
                </div>
            """, unsafe_allow_html=True)

    st.divider()

    # --- 2. God Trendlines ---
    st.markdown("### 📈 GOD RATING TRENDS")
    
    col_god, col_filter = st.columns([2, 1])
    with col_god:
        target_god = st.selectbox("Select a God to see history", options=data["God"].tolist())
    
    # Fetch history for this god from Supabase
    history_data = sb_select("rating_history") # You'll need a filter query here or filter in Pandas
    df_hist = pd.DataFrame(history_data)
    
    if not df_hist.empty:
        df_hist = df_hist[df_hist["god_name"] == target_god]
        df_hist["changed_at"] = pd.to_datetime(df_hist["changed_at"])
        
        with col_filter:
            rater_filter = st.multiselect("Filter by Raters", options=PLAYERS, default=PLAYERS)
        
        df_plot = df_hist[df_hist["player"].isin(rater_filter)]
        
        # Plotly Trendline
        fig = go.Figure()
        for p in rater_filter:
            p_data = df_plot[df_plot["player"] == p].sort_values("changed_at")
            if not p_data.empty:
                fig.add_trace(go.Scatter(
                    x=p_data["changed_at"], 
                    y=p_data["new_value"],
                    name=p,
                    line=dict(color=COUNCIL_COLORS.get(p, "#fff"), width=3),
                    mode='lines+markers'
                ))
        
        fig.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color="white"),
            margin=dict(l=0, r=0, t=20, b=0),
            height=300,
            xaxis=dict(showgrid=False),
            yaxis=dict(range=[0, 105])
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No history recorded for this god yet.")


# ── TAB: HEAD-TO-HEAD COMPARISON ─────────────────────────────────────────────

with t_h2h:
    st.subheader("⚔️ COUNCIL HEAD-TO-HEAD")

    cmp_col1, cmp_col2 = st.columns(2)
    with cmp_col1:
        player_a = st.selectbox("Council Member A", PLAYERS, index=0, key="cmp_a")
    with cmp_col2:
        player_b = st.selectbox("Council Member B", PLAYERS, index=min(1, len(PLAYERS) - 1), key="cmp_b")

    if player_a == player_b:
        st.info("Pick two different council members to compare.")
    elif player_a not in data.columns or player_b not in data.columns:
        st.warning("One or both players have no rating data yet.")
    else:
        cmp_df = (
            data[["God", "Tier", player_a, player_b]]
            .dropna(subset=[player_a, player_b])
            .copy()
        )
        cmp_df = cmp_df[(cmp_df[player_a] > 0) & (cmp_df[player_b] > 0)]
        cmp_df["Diff"]    = cmp_df[player_a] - cmp_df[player_b]
        cmp_df["AbsDiff"] = cmp_df["Diff"].abs()

        color_a = COUNCIL_COLORS.get(player_a, "#7c3aed")
        color_b = COUNCIL_COLORS.get(player_b, "#3b82f6")

        agrees   = len(cmp_df[cmp_df["AbsDiff"] <= 5])
        a_higher = len(cmp_df[cmp_df["Diff"] > 5])
        b_higher = len(cmp_df[cmp_df["Diff"] < -5])

        m1, m2, m3 = st.columns(3)
        m1.metric(f"{player_a} rates higher", a_higher, help="Gods where A scored 5+ more than B")
        m2.metric("Agreement (±5 pts)", agrees)
        m3.metric(f"{player_b} rates higher", b_higher)

        abbr_a = PLAYER_ABBR.get(player_a, player_a[:2])
        abbr_b = PLAYER_ABBR.get(player_b, player_b[:2])

        def _h2h_art_card(row, color_a, color_b, abbr_a, abbr_b,
                          player_a, player_b, accent_color,
                          footer_html) -> str:
            """Shared art-card HTML for both H2H sections."""
            tier_val   = str(row.get("Tier", "U")).upper().strip()
            tier_color = TIER_COLORS.get(tier_val, "#475569")
            god_img    = get_god_image_url(row["God"], GOD_IMAGE_MAP)
            return f"""
            <div class="god-card" style="padding:0;overflow:hidden;
                        border-color:{accent_color}33;position:relative;margin-bottom:0;">
                <div style="position:relative;width:100%;aspect-ratio:3/2;overflow:hidden;">
                    <img src="{god_img}"
                         style="width:100%;height:100%;object-fit:cover;
                                object-position:center 15%;display:block;"
                         onerror="this.style.background='#1e293b';" />
                    <div style="position:absolute;inset:0;
                                background:linear-gradient(to bottom,transparent 20%,
                                rgba(5,5,15,0.92) 100%);"></div>
                    <div style="position:absolute;top:6px;right:6px;">
                        <span style="background:{tier_color};color:white;font-size:0.65rem;
                                     font-weight:800;padding:1px 7px;border-radius:6px;">
                            {tier_val}
                        </span>
                    </div>
                    <div style="position:absolute;bottom:6px;left:0;right:0;
                                text-align:center;padding:0 6px;">
                        <div style="font-weight:800;color:white;font-size:0.9rem;
                                    line-height:1.2;">{row['God']}</div>
                    </div>
                </div>
                <div style="padding:8px 10px 10px;">
                    <div style="display:flex;justify-content:space-around;
                                align-items:center;margin-bottom:6px;">
                        <div style="text-align:center;">
                            <div style="color:{color_a};font-size:0.6rem;font-weight:700;">{abbr_a}</div>
                            <div style="font-weight:900;color:white;font-size:1rem;">{int(row[player_a])}</div>
                        </div>
                        <div style="font-size:1rem;color:{accent_color};">⚡</div>
                        <div style="text-align:center;">
                            <div style="color:{color_b};font-size:0.6rem;font-weight:700;">{abbr_b}</div>
                            <div style="font-weight:900;color:white;font-size:1rem;">{int(row[player_b])}</div>
                        </div>
                    </div>
                    <div style="text-align:center;border-top:1px solid {accent_color}22;padding-top:5px;">
                        {footer_html}
                    </div>
                </div>
            </div>"""

        # ── BIGGEST DISAGREEMENTS ─────────────────────────────────────────────
        top_diff = cmp_df.sort_values("AbsDiff", ascending=False).head(12)

        with st.expander(f"⚡ BIGGEST DISAGREEMENTS  ·  top {len(top_diff)}", expanded=True):
            diff_cols = st.columns(4)
            for i, (_, row) in enumerate(top_diff.iterrows()):
                winner       = player_a if row["Diff"] > 0 else player_b
                winner_color = color_a  if row["Diff"] > 0 else color_b
                footer = (
                    f'<span style="color:{winner_color};font-size:0.7rem;font-weight:700;">'
                    f'+{int(row["AbsDiff"])} pts</span>'
                    f'<span style="color:#475569;font-size:0.65rem;margin-left:6px;">'
                    f'{winner} higher</span>'
                )
                with diff_cols[i % 4]:
                    st.markdown(
                        _h2h_art_card(row, color_a, color_b, abbr_a, abbr_b,
                                      player_a, player_b, winner_color, footer),
                        unsafe_allow_html=True,
                    )

        # ── AGREED UPON GODS ─────────────────────────────────────────────────
        agreed_df = cmp_df[
            (cmp_df[player_a] >= 80) &
            (cmp_df[player_b] >= 80) &
            (cmp_df["AbsDiff"] <= 10)
        ].copy()
        agreed_df["AvgScore"] = (agreed_df[player_a] + agreed_df[player_b]) / 2
        agreed_df = agreed_df.sort_values("AvgScore", ascending=False)

        agreed_label = (
            f"🤝 AGREED UPON GODS  ·  {len(agreed_df)} gods"
            if not agreed_df.empty
            else "🤝 AGREED UPON GODS  ·  none yet"
        )

        with st.expander(agreed_label, expanded=True):
            if agreed_df.empty:
                st.markdown(
                    f"""
                    <div style="background:rgba(15,23,42,0.5);border:1px solid rgba(124,58,237,0.2);
                                border-radius:12px;padding:20px;text-align:center;">
                        <div style="color:#94a3b8;font-size:0.9rem;">
                            No gods where both
                            <span style="color:{color_a};font-weight:700;">{player_a}</span> and
                            <span style="color:{color_b};font-weight:700;">{player_b}</span>
                            rated ≥ 80 within 10 pts yet.
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            else:
                agreed_cols = st.columns(4)
                for i, (_, row) in enumerate(agreed_df.iterrows()):
                    avg    = round(row["AvgScore"])
                    footer = (
                        f'<span style="color:#4ade80;font-size:0.7rem;font-weight:700;">'
                        f'AVG {avg}</span>'
                        f'<span style="color:#475569;font-size:0.65rem;margin-left:6px;">'
                        f'Δ{int(row["AbsDiff"])} pts</span>'
                    )
                    with agreed_cols[i % 4]:
                        st.markdown(
                            _h2h_art_card(row, color_a, color_b, abbr_a, abbr_b,
                                          player_a, player_b, "#4ade80", footer),
                            unsafe_allow_html=True,
                        )


# ── TAB: RECENT ACTIVITY ─────────────────────────────────────────────────────

with t_activity:
    st.subheader("📜 RECENT ACTIVITY")
    history = get_recent_history(60)

    if not history:
        st.info("No recent activity found. Make some updates to see them here!")
    else:
        act_fc1, act_fc2 = st.columns([1, 2])
        with act_fc1:
            act_player_filter = st.selectbox("Filter player", ["All"] + PLAYERS,
                                             key="act_player_filter")
        with act_fc2:
            act_type_filter = st.selectbox("Filter type", ["All", "Rating changes", "Rank changes"],
                                           key="act_type_filter")

        filtered_history = history
        if act_player_filter != "All":
            filtered_history = [r for r in filtered_history
                                 if r.get("player") == act_player_filter]
        if act_type_filter == "Rating changes":
            filtered_history = [r for r in filtered_history
                                 if r.get("change_type", "rating") == "rating"]
        elif act_type_filter == "Rank changes":
            filtered_history = [r for r in filtered_history
                                 if r.get("change_type") == "rank"]

        for record in filtered_history:
            p          = record.get("player", "Unknown")
            g          = record.get("god_name", "Unknown")
            old_v      = record.get("old_value")
            new_v      = record.get("new_value")
            dt_raw     = record.get("changed_at", "")
            ctype      = record.get("change_type", "rating")
            god_img_a  = get_god_image_url(g, GOD_IMAGE_MAP)

            try:
                dt_obj = datetime.fromisoformat(dt_raw.replace("Z", "+00:00"))
                dt_str = dt_obj.strftime("%b %d, %H:%M")
            except Exception:
                dt_str = dt_raw

            p_color = COUNCIL_COLORS.get(p, "#7c3aed")
            is_rank = (ctype == "rank")

            if is_rank:
                # Rank change
                if old_v is None:
                    action_str = f"ranked #{new_v}"
                    diff_str   = f"#{new_v}"
                    diff_color = "#a78bfa"
                else:
                    moved = old_v - new_v  # positive = moved up
                    if moved > 0:
                        action_str = f"moved up #{old_v} → #{new_v}"
                        diff_str   = f"▲{moved}"
                        diff_color = "#4ade80"
                    elif moved < 0:
                        action_str = f"moved down #{old_v} → #{new_v}"
                        diff_str   = f"▼{abs(moved)}"
                        diff_color = "#f87171"
                    else:
                        action_str = f"rank unchanged #{new_v}"
                        diff_str   = "•"
                        diff_color = "#64748b"
                type_label = "RANK"
                type_color = "#a78bfa"
            else:
                # Rating change
                old_v = old_v or 0
                new_v = new_v or 0
                diff  = new_v - old_v
                if new_v == 0:
                    action_str = "unrated"
                    diff_str   = "removed"
                    diff_color = "#f87171"
                elif old_v == 0:
                    action_str = f"first rated {new_v}"
                    diff_str   = f"+{new_v}"
                    diff_color = "#4ade80"
                else:
                    action_str = f"{old_v} → {new_v}"
                    diff_str   = f"+{diff}" if diff > 0 else str(diff)
                    diff_color = "#4ade80" if diff > 0 else "#f87171"
                type_label = "RATING"
                type_color = "#7c3aed"

            st.markdown(
                f"""<div style="display:flex;align-items:center;gap:10px;
                        background:rgba(15,23,42,0.5);border-radius:10px;
                        border:1px solid rgba(255,255,255,0.06);
                        padding:9px 12px;margin-bottom:7px;">
                    <img src="{god_img_a}"
                         style="width:38px;height:38px;border-radius:7px;
                                object-fit:cover;object-position:center 15%;
                                flex-shrink:0;border:1px solid {p_color}44;"
                         onerror="this.style.background='#1e293b';" />
                    <div style="flex:1;min-width:0;">
                        <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
                            <span style="color:{p_color};font-weight:800;font-size:0.88rem;">{p}</span>
                            <span style="background:{type_color}22;color:{type_color};font-size:0.58rem;
                                         font-weight:800;padding:1px 6px;border-radius:4px;
                                         letter-spacing:0.5px;">{type_label}</span>
                            <span style="font-weight:700;color:white;font-size:0.88rem;">{g}</span>
                        </div>
                        <div style="color:#475569;font-size:0.72rem;margin-top:2px;">{dt_str}</div>
                    </div>
                    <div style="text-align:right;flex-shrink:0;">
                        <div style="color:#94a3b8;font-size:0.78rem;">{action_str}</div>
                        <div style="color:{diff_color};font-weight:900;font-size:0.85rem;">{diff_str}</div>
                    </div>
                </div>""",
                unsafe_allow_html=True,
            )


# ── TAB: RATE & RANK ─────────────────────────────────────────────────────────

with t_ranker:
    st.markdown(
        """
        <div style="background:rgba(15,23,42,0.7);padding:15px;border-radius:12px;
                    border:1px solid rgba(124,58,237,0.4);margin-bottom:20px;">
            <h2 style="color:#ffffff;margin-top:0;font-size:1.4rem;">⚡ RATE &amp; RANK</h2>
            <p style="color:#a78bfa;font-size:0.9rem;margin-bottom:0;">
                Enter a rating for each god — they auto-sort into your personal ranking.
                Use ▲▼ to break ties. Moving a god above a higher-rated one bumps its score up to match.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Player selector + PIN gate ────────────────────────────────────────────
    rk_col1, _ = st.columns([1, 3])
    with rk_col1:
        rk_player = st.selectbox("👤 Council Member", PLAYERS, key="rk_player")

    rk_color  = COUNCIL_COLORS.get(rk_player, "#7c3aed")
    rk_unlock = f"rr_unlocked_{rk_player}"

    if not st.session_state.get(rk_unlock, False):
        st.markdown(
            f"""<div style="max-width:340px;margin:40px auto;background:rgba(15,23,42,0.8);
                    border:1px solid {rk_color}55;border-radius:14px;padding:28px 24px;
                    text-align:center;">
                <div style="font-size:2rem;margin-bottom:8px;">🔒</div>
                <div style="color:{rk_color};font-weight:800;font-size:1.1rem;
                            text-transform:uppercase;letter-spacing:2px;margin-bottom:4px;">
                    {rk_player}
                </div>
                <div style="color:#94a3b8;font-size:0.85rem;margin-bottom:20px;">
                    Enter your PIN to unlock
                </div>
            </div>""",
            unsafe_allow_html=True,
        )
        pin_col, _ = st.columns([1, 2])
        with pin_col:
            rk_pin = st.text_input(
                "PIN", type="password", placeholder="Enter PIN...",
                key=f"rr_pin_{rk_player}", label_visibility="collapsed",
            )
            if st.button("🔓 UNLOCK", type="primary", use_container_width=True,
                         key=f"rr_pin_btn_{rk_player}"):
                if check_pin(rk_player, rk_pin):
                    st.session_state[rk_unlock] = True
                    st.rerun()
                else:
                    st.error("❌ Wrong PIN.")
        st.stop()

    # ── Lock bar ──────────────────────────────────────────────────────────────
    lk_col, _ = st.columns([2, 3])
    with lk_col:
        st.markdown(
            f"""<div style="background:rgba(30,41,59,0.6);border:1px solid {rk_color}55;
                    border-radius:8px;padding:8px 14px;display:flex;align-items:center;
                    gap:10px;margin-bottom:12px;">
                <span>🔓</span>
                <span style="color:{rk_color};font-weight:800;font-size:0.9rem;
                             text-transform:uppercase;">{rk_player}</span>
                <span style="color:#64748b;font-size:0.75rem;">unlocked</span>
            </div>""",
            unsafe_allow_html=True,
        )
        if st.button("🔒 Lock", key=f"rr_lock_{rk_player}",type="primary"):
            st.session_state[rk_unlock] = False
            st.rerun()

    # ── Load state ────────────────────────────────────────────────────────────
    ratings_key = f"rr_ratings_{rk_player}"
    if ratings_key not in st.session_state or             st.session_state.get("rr_player_last") != rk_player:
        if rk_player in raw_input.columns:
            series = raw_input[rk_player].fillna(0).astype(int)
            st.session_state[ratings_key] = dict(zip(raw_input["God"], series))
        else:
            st.session_state[ratings_key] = {g: 0 for g in raw_input["God"]}
        st.session_state["rr_player_last"] = rk_player

    working_ratings: dict = st.session_state[ratings_key]

    ranks_key = f"rr_ranks_{rk_player}"

    @st.cache_data(ttl=20)
    def _load_rr_ranks(player: str) -> dict:
        try:
            rows = sb_select_player_rankings(player)
            return {r["god_name"]: r["rank"] for r in rows if r.get("rank")}
        except Exception:
            return {}

    if ranks_key not in st.session_state or             st.session_state.get("rr_player_last_rank") != rk_player:
        st.session_state[ranks_key]             = _load_rr_ranks(rk_player)
        st.session_state["rr_player_last_rank"] = rk_player

    working_ranks: dict = st.session_state[ranks_key]

    all_gods   = data["God"].tolist()
    total_gods = len(all_gods)

    # ── Rank helpers ──────────────────────────────────────────────────────────
    def _full_resort(ratings: dict, ranks: dict, gods: list) -> None:
        """Full re-sort by (rating desc, existing-rank tiebreak, alpha).
        Called only when a rating value actually changes."""
        rated = [g for g in gods if ratings.get(g, 0) > 0]
        rated.sort(key=lambda g: (-ratings.get(g, 0), ranks.get(g, 99999), g.lower()))
        ranks.clear()
        for i, g in enumerate(rated, start=1):
            ranks[g] = i

    def _insert_new_only(ratings: dict, ranks: dict, gods: list) -> None:
        """Append any newly-rated gods that have no rank yet.
        Never moves existing entries — manual nudges are preserved."""
        new_entries = sorted(
            [g for g in gods if ratings.get(g, 0) > 0 and g not in ranks],
            key=lambda g: (-ratings.get(g, 0), g.lower()),
        )
        if not new_entries:
            return
        nxt = max(ranks.values(), default=0) + 1
        for g in new_entries:
            ranks[g] = nxt
            nxt += 1

    def _remove_from_ranks(god: str, ranks: dict) -> None:
        """Remove a god and close the gap."""
        old = ranks.pop(god, 0)
        if old:
            for g in list(ranks):
                if ranks[g] > old:
                    ranks[g] -= 1

    def _move_rank(god: str, direction: int, ranks: dict, ratings: dict, player: str) -> None:
        """
        Move god up (direction=-1) or down (direction=+1) by one position.
        Also updates the rating if crossing a rating boundary, and syncs Streamlit state.
        """
        cur_rank = ranks.get(god, 0)
        if cur_rank == 0:
            return
        target_rank = cur_rank + direction
        if target_rank < 1 or target_rank > len(ranks):
            return

        # Find the neighbour we're swapping past BEFORE we move
        neighbour = next(
            (g for g, r in ranks.items() if r == target_rank and g != god),
            None,
        )

        # Swap ranks
        if neighbour:
            ranks[neighbour] = cur_rank
        ranks[god] = target_rank

        # Rating adjustment: only when crossing a rating boundary
        if neighbour and ratings.get(god, 0) > 0:
            nb_rating  = ratings.get(neighbour, 0)
            my_rating  = ratings.get(god, 0)
            
            new_rating = my_rating
            if direction == -1 and nb_rating > my_rating:
                # Moving UP past a higher-rated god → bump our rating up
                new_rating = nb_rating
            elif direction == 1 and nb_rating > 0 and nb_rating < my_rating:
                # Moving DOWN past a lower-rated god → pull our rating down
                new_rating = nb_rating
            
            # CRITICAL FIX: Sync the underlying data AND the Streamlit widget state
            if new_rating != my_rating:
                ratings[god] = new_rating
                safe_g = god.lower().replace(" ","_").replace("-","_").replace("/","_").replace("'","")
                widget_key = f"rr_rate_{player}_{safe_g}"
                
                # Update the widget's memory so it doesn't revert our change on rerun
                if widget_key in st.session_state:
                    st.session_state[widget_key] = new_rating

    _insert_new_only(working_ratings, working_ranks, all_gods)

    # ── Progress ──────────────────────────────────────────────────────────────
    rated_count = sum(1 for v in working_ratings.values() if v > 0)
    rated_pct   = int(rated_count / total_gods * 100) if total_gods else 0

    st.markdown(
        f"""<div style="background:rgba(30,41,59,0.5);border:1px solid rgba(124,58,237,0.3);
                border-radius:10px;padding:12px 20px;margin-bottom:12px;
                display:flex;align-items:center;gap:16px;">
            <div style="color:{rk_color};font-weight:800;font-size:1rem;min-width:50px;">
                {rk_player.upper()}
            </div>
            <div style="flex:1;">
                <div style="display:flex;justify-content:space-between;
                            margin-bottom:3px;font-size:0.68rem;color:#64748b;">
                    <span>Rated</span>
                    <span style="color:{rk_color};">{rated_count}/{total_gods} ({rated_pct}%)</span>
                </div>
                <div style="background:rgba(15,23,42,0.8);border-radius:99px;height:7px;overflow:hidden;">
                    <div style="width:{rated_pct}%;background:{rk_color};height:100%;border-radius:99px;"></div>
                </div>
            </div>
        </div>""",
        unsafe_allow_html=True,
    )

    # ── Controls ──────────────────────────────────────────────────────────────
    cc1, cc2, cc3, cc4, cc5 = st.columns([2, 2, 1, 1, 1])
    with cc1:
        rr_search = st.text_input("🔍 Filter", placeholder="Search god...", key="rr_search")
    with cc2:
        rr_sort = st.selectbox("List order", ["#1 first", "#1 last",
                                               "Show unrated last", "Show unrated first"],
                               key="rr_sort")
    with cc3:
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        list_order_asc = rr_sort in ("#1 first", "Show unrated last", "Show unrated first")
    with cc4:
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        if st.button("💾 SAVE", type="primary", use_container_width=True, key="rr_save"):
            # === 1. Save ratings + history (unchanged) ===
            history_records = []
            new_ratings_df  = raw_input.copy()
            changes_made    = 0
            old_ranks_snap  = _load_rr_ranks(rk_player)

            for god, val in working_ratings.items():
                idx = new_ratings_df[new_ratings_df["God"] == god].index
                if not idx.empty:
                    old_raw        = new_ratings_df.at[idx[0], rk_player]
                    old_val_clean  = 0 if pd.isna(old_raw) else int(old_raw)
                    new_val_clean  = int(val)
                    if old_val_clean != new_val_clean:
                        new_ratings_df.at[idx[0], rk_player] = val if val > 0 else pd.NA
                        changes_made += 1
                        history_records.append({
                            "player":      rk_player,
                            "god_name":    god,
                            "old_value":   old_val_clean,
                            "new_value":   new_val_clean,
                            "changed_at":  datetime.now(timezone.utc).isoformat(),
                        })

            meta_cols_save = ["God", "Title", "Pantheon", "Role", "Class",
                            "Attack Type", "Damage Type", "Tier", "Rank"]
            save_and_sync_data(new_ratings_df, data[meta_cols_save])

            if history_records:
                sb_insert("rating_history", history_records)

            # === 2. NEW: Persist the manual personal ranking order ===
            personal_records = [
                {
                    "player": rk_player,
                    "god_name": god,
                    "rank": rank_val
                }
                for god, rank_val in working_ranks.items()
                if rank_val > 0
            ]
            if personal_records:
                sb_upsert("personal_rankings", personal_records, on_conflict="player,god_name")

            # === 3. Cleanup & refresh ===
            for k in (ratings_key, ranks_key):
                st.session_state.pop(k, None)
            st.cache_data.clear()

            st.success(f"✅ {changes_made} ratings + personal rank order saved!")
            st.rerun()

    with cc5:
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        if st.button("🗑️ RESET", use_container_width=True, key="rr_reset",type="primary"):
            if st.session_state.get(f"rr_confirm_{rk_player}"):
                for g in working_ratings: working_ratings[g] = 0
                working_ranks.clear()
                st.session_state[f"rr_confirm_{rk_player}"] = False
                st.success("Cleared — hit SAVE to persist.")
                st.rerun()
            else:
                st.session_state[f"rr_confirm_{rk_player}"] = True
                st.warning("⚠️ Click again to confirm")

    st.divider()

    # ── Build display list ────────────────────────────────────────────────────
    filtered_gods = [g for g in all_gods
                     if not rr_search or rr_search.lower() in g.lower()]

    # Ranked gods sorted by rank, unranked gods sorted alpha at end
    ranked_part   = sorted(
        [(working_ranks[g], g) for g in filtered_gods if g in working_ranks],
        key=lambda x: x[0],
    )
    unranked_part = sorted(
        [g for g in filtered_gods if g not in working_ranks]
    )

    if rr_sort == "#1 last":
        ranked_part = list(reversed(ranked_part))
    elif rr_sort == "Show unrated first":
        # swap: unrated at top, then ranked
        display_rows = [(0, g, False) for g in unranked_part] +                        [(rn, g, True) for rn, g in ranked_part]
        unranked_part = []
        ranked_part   = []
    else:
        display_rows = None

    if display_rows is None:
        display_rows = [(rn, g, True)  for rn, g in ranked_part] +                        [(0,  g, False) for g in unranked_part]

    # ── Render list ───────────────────────────────────────────────────────────
    for rank_num, god, is_placed in display_rows:
        g_data_r   = data[data["God"] == god]
        tier_val_r = str(g_data_r["Tier"].values[0]).upper().strip()                      if len(g_data_r) else "U"
        tc_r       = TIER_COLORS.get(tier_val_r, "#475569")
        god_img_r  = get_god_image_url(god, GOD_IMAGE_MAP)
        cur_rating = working_ratings.get(god, 0)
        safe_g     = (god.lower().replace(" ","_").replace("-","_")
                               .replace("/","_").replace("'",""))

        row_info, row_input, row_btns = st.columns([4, 1, 1])

        with row_info:
            rank_label = f"#{rank_num}" if is_placed else "—"
            rating_display = (
                f'<span style="color:{tc_r};font-weight:900;font-size:0.82rem;'
                f'margin-left:2px;">{cur_rating}</span>'
            ) if cur_rating > 0 else (
                '<span style="color:#334155;font-size:0.75rem;margin-left:2px;">unrated</span>'
            )
            border_col = rk_color + "44" if is_placed else "rgba(255,255,255,0.04)"
            bg_col     = "rgba(15,23,42,0.5)" if is_placed else "rgba(15,23,42,0.25)"

            st.markdown(
                f"""<div style="display:flex;align-items:center;gap:8px;
                        padding:6px 8px;background:{bg_col};border-radius:8px;
                        border:1px solid {border_col};margin-bottom:2px;">
                    <span style="color:{rk_color if is_placed else '#334155'};
                                 font-weight:900;font-size:0.88rem;width:34px;
                                 flex-shrink:0;text-align:right;">{rank_label}</span>
                    <img src="{god_img_r}"
                         style="width:30px;height:30px;border-radius:5px;
                                object-fit:cover;object-position:center 15%;
                                flex-shrink:0;border:1px solid {tc_r}55;"
                         onerror="this.style.background='#1e293b';" />
                    <span style="font-weight:700;color:white;font-size:0.83rem;
                                 flex:1;min-width:0;white-space:nowrap;
                                 overflow:hidden;text-overflow:ellipsis;">{god}</span>
                    {rating_display}
                    <span style="background:{tc_r};color:white;font-size:0.58rem;
                                 font-weight:800;padding:1px 5px;border-radius:4px;
                                 flex-shrink:0;">{tier_val_r}</span>
                </div>""",
                unsafe_allow_html=True,
            )

        with row_input:
            rt_key = f"rr_rate_{rk_player}_{safe_g}"
            new_rating = st.number_input(
                "Rating", min_value=0, max_value=100,
                value=cur_rating, step=1,
                label_visibility="collapsed",
                key=rt_key
            )
            if new_rating != cur_rating:
                working_ratings[god] = new_rating
                if new_rating == 0:
                    _remove_from_ranks(god, working_ranks)
                else:
                    _full_resort(working_ratings, working_ranks, all_gods)
                st.rerun()

        with row_btns:
            b_up, b_dn = st.columns(2)
            with b_up:
                # We use on_click and args instead of an if statement. 
                # This forces _move_rank to execute BEFORE the next rerun starts.
                st.button("▲", key=f"rr_up_{rk_player}_{safe_g}",type="primary",
                             disabled=(not is_placed or rank_num <= 1),
                             use_container_width=True,
                             on_click=_move_rank,
                             args=(god, -1, working_ranks, working_ratings, rk_player))
            with b_dn:
                st.button("▼", key=f"rr_dn_{rk_player}_{safe_g}",type="primary",
                             disabled=(not is_placed or rank_num >= len(working_ranks)),
                             use_container_width=True,
                             on_click=_move_rank,
                             args=(god, +1, working_ranks, working_ratings, rk_player))