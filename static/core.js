// Shared frontend state, DOM handles, and formatting helpers.
// This file must load before app.js because every tab renderer reads from the
// same state object and utility functions.

// This block stores the full client-side app state so every tab can render
// from one source of truth instead of each feature managing isolated data.
const state = {
    config: null,
    gods: [],
    filteredGods: [],
    allRankings: {},
    recentHistory: [],
    errors: [],
    stats: {},
    itemTaxonomy: {},
    dataHealth: {
        report: null,
        loaded: false,
        loading: false,
        error: "",
    },
    raterStats: {
        profiles: {},
        loaded: false,
        loading: false,
        error: "",
        syncing: false,
        syncMessage: "",
        cacheHydrated: false,
        selectedPlayer: "Joey",
        section: "profile",
        godSort: "played",
    },
    chemistry: {
        section: "trinity",
        pairSection: "overview",
    },
    activeTab: "index",
    filters: {
        search: "",
        role: "",
        className: "",
        pantheon: "",
        attackType: "",
        damageType: "",
    },
    items: {
        search: "",
        category: "Starter + Tier 3",
        filters: [],
        sort: "Most used",
        selected: "",
        section: "overview",
    },
    analytics: {
        god: "",
        players: [],
        rows: [],
        section: "overview",
    },
    h2h: {
        a: "Joey",
        b: "Darian",
        mode: "performance",
    },
    activity: {
        player: "All",
        type: "All",
    },
    ranker: {
        selectedPlayer: "Joey",
        unlocked: {},
        byPlayer: {},
        baselineByPlayer: {},
        serverStateByPlayer: {},
        dirtyPlayers: {},
        draftMetaByPlayer: {},
        lastSavedByPlayer: {},
        search: "",
        sort: "#1 first",
        mode: "all",
        section: "editor",
    },
    godDetail: {
        god: "",
        section: "council",
        editPlayer: "Joey",
        buildAspect: "All",
    },
    councilScroll: {
        section: "players",
        emailing: false,
        emailMessage: "",
    },
    ui: {
        isMobile: false,
    },
};

// This block stores the important DOM nodes so the render functions can update
// them without repeatedly querying the document.
const elements = {};

const RATER_PROFILE_LINKS = {
    Joey: 'https://smitesource.com/player/f29ca789-74f0-442f-937a-f72fcba045d3',
    Darian: 'https://smitesource.com/player/8005a240-cd89-4f14-bc40-db769319cb43',
    Jami: 'https://smitesource.com/player/8f5f48ca-10d1-4104-ab5d-bb80d4683313',
    Jamie: '',
    Mike: 'https://smitesource.com/player/f09127e9-676e-498e-b09e-6e20924a91f5',
};

// This helper safely escapes text before it is inserted into generated HTML.
function escapeHtml(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}

// This helper wraps fetch with JSON parsing and a readable failure message.
async function api(url, options = {}) {
    const response = await fetch(url, {
        headers: { "Content-Type": "application/json", ...(options.headers || {}) },
        ...options,
    });

    let payload = null;
    try {
        payload = await response.json();
    } catch (error) {
        payload = null;
    }

    if (!response.ok) {
        const message = payload?.message || payload?.error || `Request failed: ${response.status}`;
        throw new Error(message);
    }

    return payload;
}

// This helper returns a config-driven tier color with a neutral fallback.
function tierColor(tier) {
    return state.config?.tierColors?.[tier] || "#8d877d";
}

// This helper returns a config-driven player color with a neutral fallback.
function playerColor(player) {
    return state.config?.councilColors?.[player] || "#c89f4e";
}

// This helper returns the short label used on compact player score pills.
function playerAbbr(player) {
    return state.config?.playerAbbr?.[player] || player.slice(0, 2);
}

// This helper formats a timestamp into a readable local date/time string.
function formatDateTime(value) {
    if (!value) return "";
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return value;
    return parsed.toLocaleString(undefined, {
        month: "short",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
    });
}

// This helper formats numeric values for compact dashboard cards and gracefully
// falls back to an em dash when live data is not available.
function formatMetric(value, digits = 0, suffix = "") {
    if (value === null || value === undefined || value === "") return "—";
    const number = Number(value);
    if (!Number.isFinite(number)) return "—";
    return `${number.toLocaleString(undefined, {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
    })}${suffix}`;
}

// This helper formats a wins-losses summary with win rate for the chemistry
// modules so paired records are easy to scan at a glance.
function formatRecord(record) {
    if (!record || !Number.isFinite(Number(record.games)) || Number(record.games) <= 0) return "—";
    return `${record.wins}-${record.losses} • ${formatMetric(record.winRate, 1, "%")}`;
}

// This helper looks up a god's metadata from the already-loaded council
// catalog so chemistry modules can derive classes and roles for combo records.
function godMetaByName(godName) {
    return state.gods.find((god) => god.God === godName) || null;
}

// This helper builds a stable pair label like "Guardian + Mage" by sorting the
// two class names, which keeps mirrored pairings grouped together.
function classPairLabel(a, b) {
    return [a || "Unknown", b || "Unknown"].sort((left, right) => left.localeCompare(right)).join(" + ");
}

// This helper formats "who played what" receipts for chemistry rows without
// forcing the user to mentally cross-reference names and gods.
function formatParticipantAssignments(participantGods = {}, participants = []) {
    return participants
        .map((player) => `${player}: ${participantGods[player] || "—"}`)
        .join(" • ");
}

// This helper renders chemistry leaderboard rows with an optional subline so
// combo cards can stay compact while still showing the important details.
