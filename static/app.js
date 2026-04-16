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
    activeTab: "index",
    filters: {
        search: "",
        role: "",
        className: "",
        pantheon: "",
        attackType: "",
        damageType: "",
    },
    analytics: {
        god: "",
        players: [],
        rows: [],
    },
    h2h: {
        a: "Joey",
        b: "Darian",
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
        dirtyPlayers: {},
        lastSavedByPlayer: {},
        search: "",
        sort: "#1 first",
        mode: "all",
    },
    ui: {
        isMobile: false,
    },
};

// This block stores the important DOM nodes so the render functions can update
// them without repeatedly querying the document.
const elements = {};

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

// This helper formats short relative-style save feedback for the ranker.
function formatSavedLabel(value) {
    if (!value) return "Not saved yet";
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return "Saved";
    return `Saved ${parsed.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" })}`;
}

// This helper builds a stable localStorage key for each player's in-progress draft.
function rankerDraftKey(player) {
    return `high-council-ranker-draft:${player}`;
}

// This helper creates a lightweight signature for a player's current draft so
// the UI can cheaply tell whether there are unsaved changes.
function buildRankerSignature(playerState) {
    return JSON.stringify({
        ratings: playerState.ratings,
        order: playerState.order,
    });
}

// This helper writes one player's local draft to storage so refreshes do not
// wipe in-progress ranking work.
function persistRankerDraft(player) {
    const playerState = state.ranker.byPlayer[player];
    if (!playerState) return;

    const payload = {
        ratings: playerState.ratings,
        order: playerState.order,
        savedAt: new Date().toISOString(),
    };
    localStorage.setItem(rankerDraftKey(player), JSON.stringify(payload));
}

// This helper clears a player's local draft after a successful save.
function clearRankerDraft(player) {
    localStorage.removeItem(rankerDraftKey(player));
}

// This helper updates the dirty flag for a player by comparing current draft
// state against the last known saved baseline.
function refreshDirtyState(player) {
    const current = buildRankerSignature(state.ranker.byPlayer[player]);
    const baseline = state.ranker.baselineByPlayer[player];
    state.ranker.dirtyPlayers[player] = current !== baseline;
}

// This helper counts how many council members have rated a god.
function coverageCount(god) {
    return state.config.players.filter((player) => Number.isFinite(god[player]) && god[player] > 0).length;
}

// This helper measures disagreement size for a god using the spread between
// the highest and lowest submitted scores.
function controversyScore(god) {
    const scores = state.config.players
        .map((player) => god[player])
        .filter((value) => Number.isFinite(value) && value > 0);
    if (scores.length < 2) return 0;
    return Math.max(...scores) - Math.min(...scores);
}

// This helper measures agreement by finding the inverse of score spread.
function agreementScore(god) {
    const scores = state.config.players
        .map((player) => god[player])
        .filter((value) => Number.isFinite(value) && value > 0);
    if (scores.length < 2) return 0;
    return 100 - (Math.max(...scores) - Math.min(...scores));
}

// This helper produces a small filter summary row so users can see why lists
// changed without reopening the filters panel.
function renderFilterSummary() {
    const active = [];
    if (state.filters.search) active.push(`Search: ${state.filters.search}`);
    if (state.filters.role) active.push(`Role: ${state.filters.role}`);
    if (state.filters.className) active.push(`Class: ${state.filters.className}`);
    if (state.filters.pantheon) active.push(`Pantheon: ${state.filters.pantheon}`);
    if (state.filters.attackType) active.push(`Attack: ${state.filters.attackType}`);
    if (state.filters.damageType) active.push(`Damage: ${state.filters.damageType}`);

    if (!active.length) {
        return `<div class="filter-summary"><span class="summary-pill muted">No active filters</span></div>`;
    }

    return `
        <div class="filter-summary">
            ${active.map((label) => `<span class="summary-pill">${escapeHtml(label)}</span>`).join("")}
        </div>
    `;
}

// This helper returns HTML for a simple back-to-top affordance used on long tabs.
function renderBackToTop() {
    return `<button class="back-to-top-btn" type="button" data-back-to-top="true">Back To Top</button>`;
}

// This helper produces the current filtered god list used by most tabs.
function applyFilters() {
    const search = state.filters.search.trim().toLowerCase();

    state.filteredGods = state.gods.filter((god) => {
        const haystack = `${god.God} ${god.Title || ""} ${god.Role || ""} ${god.Pantheon || ""}`.toLowerCase();
        if (search && !haystack.includes(search)) return false;
        if (state.filters.role && god.Role !== state.filters.role) return false;
        if (state.filters.className && god.Class !== state.filters.className) return false;
        if (state.filters.pantheon && god.Pantheon !== state.filters.pantheon) return false;
        if (state.filters.attackType && god["Attack Type"] !== state.filters.attackType) return false;
        if (state.filters.damageType && god["Damage Type"] !== state.filters.damageType) return false;
        return true;
    });
}

// This helper builds unique select options for the global filters.
function optionValues(key) {
    return [...new Set(state.gods.map((god) => god[key]).filter(Boolean))].sort((a, b) => String(a).localeCompare(String(b)));
}

// This helper stores the DOM references needed throughout the app lifecycle.
function cacheElements() {
    elements.heroStats = document.getElementById("hero-stats");
    elements.statusBanner = document.getElementById("status-banner");
    elements.podiumPanel = document.getElementById("podium-panel");
    elements.podiumSummaryPreview = document.getElementById("podium-summary-preview");
    elements.liveRankings = document.getElementById("live-rankings");
    elements.podiumDetails = document.getElementById("podium-details");
    elements.sidebarDetails = document.getElementById("sidebar-details");
    elements.filtersDetails = document.getElementById("filters-details");
    elements.tabButtons = [...document.querySelectorAll(".tab-btn")];
    elements.tabButtons.forEach((button) => {
        button.dataset.fullLabel = button.textContent.trim();
    });
    elements.tabPanels = [...document.querySelectorAll(".tab-panel")];
    elements.filtersForm = document.getElementById("filters-form");
    elements.filterSearch = document.getElementById("filter-search");
    elements.filterRole = document.getElementById("filter-role");
    elements.filterClass = document.getElementById("filter-class");
    elements.filterPantheon = document.getElementById("filter-pantheon");
    elements.filterAttackType = document.getElementById("filter-attack-type");
    elements.filterDamageType = document.getElementById("filter-damage-type");
    elements.filtersReset = document.getElementById("filters-reset");
    elements.tabIndex = document.getElementById("tab-index");
    elements.tabRankings = document.getElementById("tab-rankings");
    elements.tabFavorites = document.getElementById("tab-favorites");
    elements.tabTierlist = document.getElementById("tab-tierlist");
    elements.tabAnalytics = document.getElementById("tab-analytics");
    elements.tabH2h = document.getElementById("tab-h2h");
    elements.tabActivity = document.getElementById("tab-activity");
    elements.tabRanker = document.getElementById("tab-ranker");
}

// This helper applies the requested default open/closed behavior for the
// collapsible overview panels and live sidebar, especially on mobile.
function configureResponsiveDefaults() {
    const isMobile = window.innerWidth <= 860;
    state.ui.isMobile = isMobile;

    if (elements.sidebarDetails) {
        elements.sidebarDetails.open = !isMobile;
    }
    if (elements.podiumDetails) {
        elements.podiumDetails.open = !isMobile;
    }
    if (elements.filtersDetails) {
        elements.filtersDetails.open = !isMobile;
    }

    elements.tabButtons.forEach((button) => {
        button.textContent = isMobile ? (button.dataset.mobileLabel || button.dataset.fullLabel || button.textContent) : (button.dataset.fullLabel || button.textContent);
    });
}

// This helper binds the static event listeners that exist before the app data
// has finished loading.
function bindStaticEvents() {
    elements.tabButtons.forEach((button) => {
        button.addEventListener("click", () => {
            state.activeTab = button.dataset.tab;
            renderTabs();
        });
    });

    elements.filtersForm.addEventListener("submit", (event) => {
        event.preventDefault();
        syncFiltersFromInputs();
        renderAll();
    });

    elements.filtersReset.addEventListener("click", () => {
        state.filters = {
            search: "",
            role: "",
            className: "",
            pantheon: "",
            attackType: "",
            damageType: "",
        };
        syncInputsFromFilters();
        renderAll();
    });

    document.addEventListener("click", (event) => {
        const trigger = event.target.closest("[data-back-to-top='true']");
        if (!trigger) return;
        window.scrollTo({ top: 0, behavior: "smooth" });
    });

    document.addEventListener("keydown", (event) => {
        if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "s") {
            if (state.activeTab === "ranker" && state.ranker.unlocked[state.ranker.selectedPlayer]) {
                event.preventDefault();
                saveRanker();
            }
        }
    });

    window.addEventListener("resize", () => {
        configureResponsiveDefaults();
        renderTabs();
    });
}

// This helper copies the filter form values into the shared state object.
function syncFiltersFromInputs() {
    state.filters.search = elements.filterSearch.value;
    state.filters.role = elements.filterRole.value;
    state.filters.className = elements.filterClass.value;
    state.filters.pantheon = elements.filterPantheon.value;
    state.filters.attackType = elements.filterAttackType.value;
    state.filters.damageType = elements.filterDamageType.value;
}

// This helper pushes the state filter values back into the form controls.
function syncInputsFromFilters() {
    elements.filterSearch.value = state.filters.search;
    elements.filterRole.value = state.filters.role;
    elements.filterClass.value = state.filters.className;
    elements.filterPantheon.value = state.filters.pantheon;
    elements.filterAttackType.value = state.filters.attackType;
    elements.filterDamageType.value = state.filters.damageType;
}

// This helper renders the select options for the shared global filter form.
function renderFilterOptions() {
    const sets = [
        [elements.filterRole, "Role"],
        [elements.filterClass, "Class"],
        [elements.filterPantheon, "Pantheon"],
        [elements.filterAttackType, "Attack Type"],
        [elements.filterDamageType, "Damage Type"],
    ];

    sets.forEach(([select, key]) => {
        const options = optionValues(key)
            .map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`)
            .join("");
        select.innerHTML = `<option value="">All</option>${options}`;
    });

    syncInputsFromFilters();
}

// This helper initializes each player's editable ranker state from the live
// catalog and saved personal ranking data returned by the backend.
function initializeRankerState() {
    state.config.players.forEach((player) => {
        const ratings = {};
        state.gods.forEach((god) => {
            ratings[god.God] = Number(god[player] || 0);
        });

        const savedRanks = state.allRankings[player] || {};
        const ordered = Object.entries(savedRanks)
            .sort((a, b) => a[1] - b[1])
            .map(([godName]) => godName);

        const ratedButMissing = state.gods
            .filter((god) => ratings[god.God] > 0 && !ordered.includes(god.God))
            .sort((a, b) => (ratings[b.God] - ratings[a.God]) || a.God.localeCompare(b.God))
            .map((god) => god.God);

        const baseState = {
            ratings,
            order: [...ordered, ...ratedButMissing],
        };
        const draftRaw = localStorage.getItem(rankerDraftKey(player));
        if (draftRaw) {
            try {
                const draft = JSON.parse(draftRaw);
                if (draft?.ratings && draft?.order) {
                    baseState.ratings = { ...baseState.ratings, ...draft.ratings };
                    baseState.order = Array.isArray(draft.order) ? draft.order : baseState.order;
                }
            } catch (error) {
                // Ignore malformed local drafts and keep the server-backed state.
            }
        }

        state.ranker.byPlayer[player] = baseState;
        state.ranker.baselineByPlayer[player] = buildRankerSignature({
            ratings,
            order: [...ordered, ...ratedButMissing],
        });
        state.ranker.lastSavedByPlayer[player] = state.ranker.lastSavedByPlayer[player] || "";
        refreshDirtyState(player);
        state.ranker.unlocked[player] = false;
    });
}

// This helper loads the entire bootstrap payload from the Flask backend.
async function loadBootstrap() {
    const payload = await api("/api/bootstrap");
    state.config = payload.config;
    state.gods = payload.gods;
    state.filteredGods = payload.gods;
    state.allRankings = payload.allRankings;
    state.recentHistory = payload.recentHistory || [];
    state.errors = payload.errors || [];
    state.stats = payload.stats || {};
    state.analytics.god = payload.gods[0]?.God || "";
    state.analytics.players = [...payload.config.players];
    initializeRankerState();
    renderFilterOptions();
}

// This helper renders the hero statistic cards.
function renderHeroStats() {
    if (!elements.heroStats) {
        return;
    }

    const topThree = [...state.gods]
        .filter((god) => god.Rating > 0)
        .sort((a, b) => b.Rating - a.Rating || a.God.localeCompare(b.God))
        .slice(0, 3);

    const cards = [
        ["Roster", state.stats.total_gods ?? 0],
        ["Average Rating", state.stats.avg_rating ?? 0],
        ["SS Tier", state.stats.ss_count ?? 0],
        ["Council Members", state.config.players.length],
    ];

    const cardsHtml = cards
        .map(([label, value]) => `
            <div class="stat-card">
                <span class="stat-label">${escapeHtml(label)}</span>
                <strong class="stat-value">${escapeHtml(value)}</strong>
            </div>
        `)
        .join("");

    const mobilePodium = topThree.length
        ? `
            <div class="stat-card mobile-podium-card">
                <span class="stat-label">Podium</span>
                <div class="mobile-podium-row">
                    ${topThree.map((god, index) => `
                        <span class="mobile-podium-chip">
                            <span class="mobile-podium-medal">${["🥇", "🥈", "🥉"][index] || "⚜️"}</span>
                            <span class="mobile-podium-name">${escapeHtml(god.God)}</span>
                            <span class="mobile-podium-score" style="color:${tierColor(god.Tier)}">${god.Rating}</span>
                        </span>
                    `).join("")}
                </div>
            </div>
        `
        : "";

    elements.heroStats.innerHTML = `${cardsHtml}${mobilePodium}`;
}

// This helper renders the backend status banner when any live reads fell back
// to snapshot data.
function renderStatusBanner() {
    if (!state.errors.length) {
        elements.statusBanner.innerHTML = "";
        return;
    }

    elements.statusBanner.innerHTML = `
        <div class="status-banner">
            Running with partial fallbacks: ${escapeHtml(state.errors.join(" | "))}
        </div>
    `;
}

// This helper renders the top-three podium section.
function renderPodium() {
    const topThree = [...state.gods]
        .filter((god) => god.Rating > 0)
        .sort((a, b) => b.Rating - a.Rating || a.God.localeCompare(b.God))
        .slice(0, 3);

    if (!topThree.length) {
        if (elements.podiumSummaryPreview) {
            elements.podiumSummaryPreview.innerHTML = "";
        }
        elements.podiumPanel.innerHTML = emptyState("The Podium", "No ranked gods are available right now.");
        return;
    }

    const medals = ["🥇", "🥈", "🥉"];
    if (elements.podiumSummaryPreview) {
        elements.podiumSummaryPreview.innerHTML = topThree.map((god, index) => `
            <span class="podium-peek-chip">
                <span class="podium-peek-medal">${medals[index] || "⚜️"}</span>
                <span class="podium-peek-name">${escapeHtml(god.God)}</span>
                <span class="podium-peek-score" style="color:${tierColor(god.Tier)}">${god.Rating}</span>
            </span>
        `).join("");
    }

    const cards = topThree
        .map((god, index) => `
            <article class="podium-card ${index === 0 ? "gold" : ""}">
                <div class="podium-image-wrap">
                    ${god.ImageUrl ? `<img class="podium-image" src="${god.ImageUrl}" alt="${escapeHtml(god.God)}">` : `<div class="image-fallback">No Art</div>`}
                    <div class="podium-overlay"></div>
                    <div class="podium-content">
                        <div class="podium-medal">${medals[index] || "⚜️"}</div>
                        <div class="podium-name">${escapeHtml(god.God)}</div>
                        <div class="podium-title">${escapeHtml(god.Title || "")}</div>
                        <div class="podium-score" style="color:${tierColor(god.Tier)}">${god.Rating} PTS</div>
                    </div>
                </div>
            </article>
        `)
        .join("");

    elements.podiumPanel.innerHTML = `<div class="podium-grid">${cards}</div>`;
}

// This helper renders the most controversial gods panel.
function renderControversyCards() {
    const controversial = [...state.gods]
        .map((god) => {
            const scores = state.config.players
                .map((player) => god[player])
                .filter((value) => Number.isFinite(value) && value > 0);
            const maxDiff = scores.length >= 2 ? Math.max(...scores) - Math.min(...scores) : 0;
            return { ...god, maxDiff };
        })
        .filter((god) => god.maxDiff > 0)
        .sort((a, b) => b.maxDiff - a.maxDiff)
        .slice(0, 5);

    if (!controversial.length) {
        return emptyState("Most Controversial", "Not enough overlapping ratings yet.");
    }

    const cards = controversial
        .map((god) => {
            const chips = state.config.players
                .filter((player) => Number.isFinite(god[player]) && god[player] > 0)
                .map((player) => `<span class="score-chip" style="color:${playerColor(player)}">${playerAbbr(player)}:${god[player]}</span>`)
                .join("");
            return `
                <article class="feature-card">
                    <div class="feature-title">${escapeHtml(god.God)}</div>
                    <div class="feature-split">Split: ${god.maxDiff} pts</div>
                    <div class="feature-scores">${chips}</div>
                </article>
            `;
        })
        .join("");

    return `<div class="feature-list">${cards}</div>`;
}

// This helper renders the sidebar's live rankings list using the current
// filtered god set.
function renderSidebar() {
    const rows = [...state.filteredGods]
        .sort((a, b) => (b.Rating - a.Rating) || a.God.localeCompare(b.God))
        .map((god) => {
            const movement = Number(god.Movement || 0);
            const movementClass = movement > 0 ? "movement-up" : movement < 0 ? "movement-down" : "movement-flat";
            const movementLabel = movement > 0 ? `▲${movement}` : movement < 0 ? `▼${Math.abs(movement)}` : "•";

            return `
                <div class="sidebar-rank-item">
                    <div class="rank-badge">
                        <span style="color:${tierColor(god.Tier)}">#${god.Rank || "—"}</span>
                        <span class="${movementClass}">${movementLabel}</span>
                    </div>
                    <div>
                        <div class="rank-name">${escapeHtml(god.God)}</div>
                        <div class="rank-meta">${escapeHtml(god.Pantheon || "")} • ${escapeHtml(god.Role || "")}</div>
                    </div>
                    <div class="rank-score-block">
                        <div style="color:${tierColor(god.Tier)};font-weight:900">${escapeHtml(god.Tier)}</div>
                        <div class="rank-meta">${god.Rating} pts</div>
                    </div>
                </div>
            `;
        })
        .join("");

    elements.liveRankings.innerHTML = rows
        ? `<div class="sidebar-rank-list">${rows}</div>`
        : emptyState("Filtered Out", "No gods match the current filters.");
}

// This helper renders a reusable empty-state card.
function emptyState(title, description) {
    return `
        <div class="empty-state">
            <h3>${escapeHtml(title)}</h3>
            <p>${escapeHtml(description)}</p>
        </div>
    `;
}

// This helper renders the God Index cards.
function renderIndexTab() {
    if (!state.filteredGods.length) {
        elements.tabIndex.innerHTML = emptyState("No Gods Found", "Try clearing a filter or broadening the search.");
        return;
    }

    const cards = state.filteredGods
        .map((god) => {
            const coverage = coverageCount(god);
            const split = controversyScore(god);
            const agreed = agreementScore(god);
            return `
            <article class="god-card ${coverage < state.config.players.length ? "partial-coverage" : ""}">
                <div class="god-art-wrap">
                    ${god.ImageUrl ? `<img class="god-art" src="${god.ImageUrl}" alt="${escapeHtml(god.God)}">` : `<div class="image-fallback">No Art</div>`}
                    <div class="god-overlay"></div>
                    <div class="chip-row">
                        <span class="chip">#${god.Rank || "—"}</span>
                        <span class="chip" style="color:${tierColor(god.Tier)}">${escapeHtml(god.Tier)}</span>
                    </div>
                    <div class="god-overlay-content">
                        <div>
                            <p class="god-title">${escapeHtml(god.Title || "")}</p>
                            <h3>${escapeHtml(god.God)}</h3>
                            <div class="god-meta">${escapeHtml(god.Role || "")} • ${escapeHtml(god.Pantheon || "")} • ${escapeHtml(god.Class || "Unknown")} • ${escapeHtml(god["Attack Type"] || "Unknown")}</div>
                        </div>
                        <div class="rank-score-block">
                            <span class="rating-label">Council Rating</span>
                            <div class="rating-value" style="color:${tierColor(god.Tier)}">${god.Rating}</div>
                        </div>
                    </div>
                </div>
                <div class="god-body">
                    <div class="pill-grid">
                        ${god.CouncilPills.map((pill) => `
                            <div class="council-pill">
                                <span class="pill-name" style="color:${pill.color}" title="${escapeHtml(pill.player)}">${escapeHtml(pill.abbr)}</span>
                                <span class="pill-score">${pill.score ?? "—"}</span>
                                <span class="pill-rank">${pill.rank ? `#${pill.rank}` : "·"}</span>
                            </div>
                        `).join("")}
                    </div>
                    <div class="card-meta-rail">
                        <span class="summary-pill">${coverage}/${state.config.players.length} rated</span>
                        ${split >= 20 ? `<span class="summary-pill warm">Split ${split}</span>` : ""}
                        ${agreed >= 92 ? `<span class="summary-pill cool">High agreement</span>` : ""}
                        ${god.HotTake ? `<span class="summary-pill hot">Hot take</span>` : ""}
                    </div>
                </div>
            </article>
        `;
        })
        .join("");

    elements.tabIndex.innerHTML = `
        <div class="panel">
            <div class="panel-heading">
                <p class="eyebrow">Gallery View</p>
                <h2>God Index</h2>
            </div>
            ${renderFilterSummary()}
            <div class="god-grid">${cards}</div>
            ${renderBackToTop()}
        </div>
    `;
}

// This helper renders the rankings tab.
function renderRankingsTab() {
    const risers = [...state.gods].filter((god) => Number(god.Movement || 0) > 0).sort((a, b) => Number(b.Movement || 0) - Number(a.Movement || 0)).slice(0, 3);
    const fallers = [...state.gods].filter((god) => Number(god.Movement || 0) < 0).sort((a, b) => Number(a.Movement || 0) - Number(b.Movement || 0)).slice(0, 3);
    const rows = [...state.filteredGods]
        .sort((a, b) => (b.Rating - a.Rating) || a.God.localeCompare(b.God))
        .map((god) => `
            <article class="rank-row" style="border-left:4px solid ${tierColor(god.Tier)}">
                <div class="rank-number" style="color:${tierColor(god.Tier)}">${god.Rank || "—"}</div>
                <div>
                    <div class="rank-title-line">
                        <h3>${escapeHtml(god.God)}</h3>
                        <span class="title-muted">${escapeHtml(god.Title || "")}</span>
                    </div>
                    <div class="rank-meta">${escapeHtml(god.Pantheon || "")} • ${escapeHtml(god.Role || "")} • ${escapeHtml(god.Class || "")}</div>
                    <div class="rank-pill-row">
                        ${god.CouncilPills.filter((pill) => pill.score).map((pill) => `
                            <span class="tiny-pill" style="color:${pill.color}">${escapeHtml(pill.abbr)} ${pill.score}</span>
                        `).join("")}
                    </div>
                </div>
                <div class="rank-score-block">
                    <div style="color:${tierColor(god.Tier)};font-weight:900">${escapeHtml(god.Tier)} tier</div>
                    <div class="rank-meta">${god.Rating ? `${god.Rating} pts` : "Unrated"}</div>
                    <div class="rank-meta ${Number(god.Movement || 0) > 0 ? "movement-up" : Number(god.Movement || 0) < 0 ? "movement-down" : "movement-flat"}">
                        ${Number(god.Movement || 0) > 0 ? `▲${god.Movement}` : Number(god.Movement || 0) < 0 ? `▼${Math.abs(god.Movement)}` : "• steady"}
                    </div>
                </div>
            </article>
        `)
        .join("");

    elements.tabRankings.innerHTML = `
        <div class="panel">
            <div class="panel-heading">
                <p class="eyebrow">Consensus Ladder</p>
                <h2>Power Rankings</h2>
            </div>
            ${renderFilterSummary()}
            <div class="mini-highlight-grid">
                <article class="mini-highlight-card">
                    <div class="metric-label">Most Improved</div>
                    ${risers.length ? risers.map((god) => `<div class="mini-highlight-row"><span>${escapeHtml(god.God)}</span><strong class="movement-up">▲${god.Movement}</strong></div>`).join("") : `<div class="rank-meta">No risers yet</div>`}
                </article>
                <article class="mini-highlight-card">
                    <div class="metric-label">Biggest Faller</div>
                    ${fallers.length ? fallers.map((god) => `<div class="mini-highlight-row"><span>${escapeHtml(god.God)}</span><strong class="movement-down">▼${Math.abs(god.Movement)}</strong></div>`).join("") : `<div class="rank-meta">No fallers yet</div>`}
                </article>
            </div>
            <div class="rankings-list">${rows || emptyState("No Rankings", "No gods match the current filters.")}</div>
            ${renderBackToTop()}
        </div>
    `;
}

// This helper renders each council member's top-five list.
function renderFavoritesTab() {
    const columns = state.config.players
        .map((player) => {
            const topFive = [...state.filteredGods]
                .filter((god) => Number.isFinite(god[player]) && god[player] > 0)
                .sort((a, b) => (b[player] - a[player]) || a.God.localeCompare(b.God))
                .slice(0, 5);

            const rows = topFive.length
                ? topFive.map((god) => `
                    <div class="favorite-row">
                        <span>${escapeHtml(god.God)}</span>
                        <div style="text-align:right;">
                            <strong style="color:${playerColor(player)}">${god[player]}</strong>
                            <div class="rank-meta ${Number(god[player] - god.Rating) > 0 ? "movement-up" : Number(god[player] - god.Rating) < 0 ? "movement-down" : "movement-flat"}">
                                ${Number(god[player] - god.Rating) > 0 ? "+" : ""}${Number(god[player] - god.Rating) || 0} vs avg
                            </div>
                        </div>
                    </div>
                `).join("")
                : `<p class="rank-meta">No ratings match the current filters.</p>`;

            return `
                <article class="ranking-list-card">
                    <div class="panel-heading">
                        <p class="eyebrow" style="color:${playerColor(player)}">${escapeHtml(player)}</p>
                        <h2>${escapeHtml(player)} Top 5</h2>
                    </div>
                    ${rows}
                </article>
            `;
        })
        .join("");

    elements.tabFavorites.innerHTML = `
        <div class="panel">
            <div class="panel-heading">
                <p class="eyebrow">Individual Taste</p>
                <h2>Council Favorites</h2>
            </div>
            ${renderFilterSummary()}
            <div class="favorites-grid">${columns}</div>
            ${renderBackToTop()}
        </div>
    `;
}

// This helper renders the tier list grouped by tier bucket.
function renderTierlistTab() {
    const groups = state.config.tierOrder
        .map((tier) => {
            const gods = state.filteredGods.filter((god) => god.Tier === tier);
            if (!gods.length) return "";

            return `
                <section class="tier-group">
                    <div class="tier-header">
                        <span class="tier-chip-large" style="background:${tierColor(tier)}">${escapeHtml(tier)}</span>
                        <span class="rank-meta">${gods.length} gods</span>
                    </div>
                    <div class="tier-god-grid">
                        ${gods.map((god) => `
                            <article class="tier-god-card">
                                <strong>${escapeHtml(god.God)}</strong>
                                <div class="rank-meta">${god.CouncilPills.filter((pill) => pill.score).map((pill) => `<span style="color:${pill.color}">${pill.abbr}:${pill.score}</span>`).join(" • ")}</div>
                            </article>
                        `).join("")}
                    </div>
                </section>
            `;
        })
        .join("");

    elements.tabTierlist.innerHTML = `
        <div class="panel">
            <div class="panel-heading">
                <p class="eyebrow">Macro View</p>
                <h2>Tier List</h2>
            </div>
            ${renderFilterSummary()}
            <div class="tier-stack">${groups || emptyState("No Tier Data", "No filtered gods are available.")}</div>
            ${renderBackToTop()}
        </div>
    `;
}

// This helper creates the analytics progress cards for player completion.
function renderProgressCards() {
    const totalGods = state.gods.length;
    return state.config.players.map((player) => {
        const count = state.gods.filter((god) => Number.isFinite(god[player]) && god[player] > 0).length;
        const percent = totalGods ? ((count / totalGods) * 100).toFixed(1) : "0.0";
        return `
            <article class="metric-card">
                <div class="metric-label" style="color:${playerColor(player)}">${escapeHtml(player)}</div>
                <div class="metric-value">${count}<span class="rank-meta">/${totalGods}</span></div>
                <div class="rank-meta">${percent}% complete</div>
            </article>
        `;
    }).join("");
}

// This helper fetches rating history for the currently selected analytics god.
async function loadAnalyticsHistory() {
    if (!state.analytics.god) {
        state.analytics.rows = [];
        return;
    }

    try {
        const payload = await api(`/api/history?god=${encodeURIComponent(state.analytics.god)}&limit=300`);
        state.analytics.rows = payload.rows || [];
    } catch (error) {
        state.analytics.rows = [];
    }
}

// This helper converts history rows into an SVG polyline chart with one line
// per selected player.
function buildTrendChartSvg(rows, players) {
    const grouped = players
        .map((player) => ({
            player,
            rows: rows
                .filter((row) => row.player === player && (row.change_type || "rating") !== "rank")
                .sort((a, b) => new Date(a.changed_at) - new Date(b.changed_at)),
        }))
        .filter((group) => group.rows.length);

    if (!grouped.length) {
        return `<div class="empty-state"><h3>No History</h3><p>No rating history has been recorded for this god yet.</p></div>`;
    }

    const allValues = grouped.flatMap((group) => group.rows.map((row) => Number(row.new_value || 0)));
    const minValue = 0;
    const maxValue = Math.max(100, ...allValues);
    const width = 900;
    const height = 320;
    const padLeft = 36;
    const padBottom = 28;
    const padTop = 16;
    const usableWidth = width - padLeft - 16;
    const usableHeight = height - padTop - padBottom;
    const xCount = Math.max(...grouped.map((group) => group.rows.length), 1);

    const gridLines = [0, 25, 50, 75, 100].map((value) => {
        const y = padTop + usableHeight - ((value - minValue) / (maxValue - minValue || 1)) * usableHeight;
        return `
            <line x1="${padLeft}" y1="${y}" x2="${width - 12}" y2="${y}" stroke="rgba(143,106,42,0.14)" stroke-width="1" />
            <text x="8" y="${y + 4}" fill="#8a7455" font-size="12">${value}</text>
        `;
    }).join("");

    const series = grouped.map((group) => {
        const points = group.rows.map((row, index) => {
            const x = padLeft + (index / Math.max(xCount - 1, 1)) * usableWidth;
            const y = padTop + usableHeight - ((Number(row.new_value || 0) - minValue) / (maxValue - minValue || 1)) * usableHeight;
            return `${x},${y}`;
        }).join(" ");

        const circles = group.rows.map((row, index) => {
            const x = padLeft + (index / Math.max(xCount - 1, 1)) * usableWidth;
            const y = padTop + usableHeight - ((Number(row.new_value || 0) - minValue) / (maxValue - minValue || 1)) * usableHeight;
            return `<circle cx="${x}" cy="${y}" r="4" fill="${playerColor(group.player)}" />`;
        }).join("");

        return `
            <polyline fill="none" stroke="${playerColor(group.player)}" stroke-width="3" points="${points}" />
            ${circles}
        `;
    }).join("");

    return `
        <div class="chart-legend">
            ${grouped.map((group) => `
                <span class="legend-chip">
                    <span class="legend-dot" style="background:${playerColor(group.player)}"></span>
                    ${escapeHtml(group.player)}
                </span>
            `).join("")}
        </div>
        <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Rating trend chart">
            ${gridLines}
            ${series}
        </svg>
    `;
}

// This helper renders the analytics tab.
function renderAnalyticsTab() {
    const mostAgreed = [...state.gods]
        .filter((god) => coverageCount(god) >= 3)
        .sort((a, b) => agreementScore(b) - agreementScore(a))
        .slice(0, 3);
    const mostRated = [...state.gods]
        .sort((a, b) => coverageCount(b) - coverageCount(a))
        .slice(0, 3);
    const playerOptions = state.config.players
        .map((player) => `
            <label class="tiny-pill" style="display:inline-flex;align-items:center;gap:8px;">
                <input type="checkbox" class="analytics-player" value="${escapeHtml(player)}" ${state.analytics.players.includes(player) ? "checked" : ""}>
                ${escapeHtml(player)}
            </label>
        `)
        .join("");

    const godOptions = state.gods
        .map((god) => `<option value="${escapeHtml(god.God)}" ${state.analytics.god === god.God ? "selected" : ""}>${escapeHtml(god.God)}</option>`)
        .join("");

    elements.tabAnalytics.innerHTML = `
        <div class="panel">
            <div class="panel-heading">
                <p class="eyebrow">Completion + Trends</p>
                <h2>Analytics</h2>
            </div>

            <div class="progress-grid">${renderProgressCards()}</div>
            <div class="mini-highlight-grid" style="margin-top:18px;">
                <article class="mini-highlight-card">
                    <div class="metric-label">Most Agreed Upon</div>
                    ${mostAgreed.length ? mostAgreed.map((god) => `<div class="mini-highlight-row"><span>${escapeHtml(god.God)}</span><strong class="movement-up">${agreementScore(god)}</strong></div>`).join("") : `<div class="rank-meta">Not enough overlap yet</div>`}
                </article>
                <article class="mini-highlight-card">
                    <div class="metric-label">Most Rated Gods</div>
                    ${mostRated.length ? mostRated.map((god) => `<div class="mini-highlight-row"><span>${escapeHtml(god.God)}</span><strong>${coverageCount(god)}/${state.config.players.length}</strong></div>`).join("") : `<div class="rank-meta">No ratings yet</div>`}
                </article>
            </div>

            <div class="chart-shell" style="margin-top:18px;">
                <div class="panel-heading">
                    <p class="eyebrow">History View</p>
                    <h2>God Rating Trends</h2>
                </div>
                <div class="analytics-controls">
                    <label class="field">
                        <span>Target God</span>
                        <select id="analytics-god-select">${godOptions}</select>
                    </label>
                    <div class="field">
                        <span>Included Raters</span>
                        <div style="display:flex;flex-wrap:wrap;gap:8px;">${playerOptions}</div>
                    </div>
                </div>
                <div id="analytics-chart">${buildTrendChartSvg(state.analytics.rows, state.analytics.players)}</div>
            </div>

            <div class="panel-heading" style="margin-top:18px;">
                <p class="eyebrow">Council Disputes</p>
                <h2>Most Controversial Gods</h2>
            </div>
            ${renderControversyCards()}
            ${renderBackToTop()}
        </div>
    `;

    document.getElementById("analytics-god-select")?.addEventListener("change", async (event) => {
        state.analytics.god = event.target.value;
        await loadAnalyticsHistory();
        renderAnalyticsTab();
    });

    document.querySelectorAll(".analytics-player").forEach((input) => {
        input.addEventListener("change", () => {
            state.analytics.players = [...document.querySelectorAll(".analytics-player:checked")].map((box) => box.value);
            renderAnalyticsTab();
        });
    });
}

// This helper builds the filtered head-to-head comparison rows.
function buildH2hRows() {
    return state.gods
        .filter((god) => Number.isFinite(god[state.h2h.a]) && god[state.h2h.a] > 0 && Number.isFinite(god[state.h2h.b]) && god[state.h2h.b] > 0)
        .map((god) => ({
            ...god,
            diff: Number(god[state.h2h.a]) - Number(god[state.h2h.b]),
            absDiff: Math.abs(Number(god[state.h2h.a]) - Number(god[state.h2h.b])),
        }));
}

// This helper renders a reusable art card for the H2H tab.
function h2hCard(god, footerHtml) {
    return `
        <article class="h2h-card">
            <div class="h2h-art-wrap">
                ${god.ImageUrl ? `<img class="h2h-art" src="${god.ImageUrl}" alt="${escapeHtml(god.God)}">` : `<div class="image-fallback">No Art</div>`}
                <div class="h2h-overlay"></div>
                <div class="god-overlay-content">
                    <div>
                        <span class="chip" style="color:${tierColor(god.Tier)}">${escapeHtml(god.Tier)}</span>
                        <h3 style="margin-top:10px;color:white">${escapeHtml(god.God)}</h3>
                    </div>
                </div>
            </div>
            <div class="h2h-card-body">
                <div class="h2h-score-row">
                    <div style="text-align:center">
                        <div style="color:${playerColor(state.h2h.a)};font-size:0.72rem;font-weight:900">${escapeHtml(playerAbbr(state.h2h.a))}</div>
                        <div style="font-weight:900">${god[state.h2h.a]}</div>
                    </div>
                    <div class="versus-label">⚡</div>
                    <div style="text-align:center">
                        <div style="color:${playerColor(state.h2h.b)};font-size:0.72rem;font-weight:900">${escapeHtml(playerAbbr(state.h2h.b))}</div>
                        <div style="font-weight:900">${god[state.h2h.b]}</div>
                    </div>
                </div>
                <div class="rank-meta" style="text-align:center">${footerHtml}</div>
            </div>
        </article>
    `;
}

// This helper renders the head-to-head tab.
function renderH2hTab() {
    const options = state.config.players
        .map((player) => `<option value="${escapeHtml(player)}">${escapeHtml(player)}</option>`)
        .join("");

    if (state.h2h.a === state.h2h.b) {
        elements.tabH2h.innerHTML = emptyState("Choose Two Players", "Pick two different council members to compare.");
        return;
    }

    const rows = buildH2hRows();
    const agreement = rows.filter((god) => god.absDiff <= 5).length;
    const aHigher = rows.filter((god) => god.diff > 5).length;
    const bHigher = rows.filter((god) => god.diff < -5).length;
    const topDiff = [...rows].sort((a, b) => b.absDiff - a.absDiff).slice(0, 12);
    const agreed = rows
        .filter((god) => god[state.h2h.a] >= 80 && god[state.h2h.b] >= 80 && god.absDiff <= 10)
        .sort((a, b) => ((b[state.h2h.a] + b[state.h2h.b]) / 2) - ((a[state.h2h.a] + a[state.h2h.b]) / 2));

    elements.tabH2h.innerHTML = `
        <div class="panel">
            <div class="panel-heading">
                <p class="eyebrow">Cross-Council Compare</p>
                <h2>Head To Head</h2>
            </div>

            <div class="h2h-controls">
                <label class="field">
                    <span>Council Member A</span>
                    <select id="h2h-player-a">${options}</select>
                </label>
                <label class="field">
                    <span>Council Member B</span>
                    <select id="h2h-player-b">${options}</select>
                </label>
            </div>

            <div class="metrics-grid" style="margin-top:18px;">
                <article class="metric-card">
                    <div class="metric-label">${escapeHtml(state.h2h.a)} rates higher</div>
                    <div class="metric-value">${aHigher}</div>
                </article>
                <article class="metric-card">
                    <div class="metric-label">Agreement (±5)</div>
                    <div class="metric-value">${agreement}</div>
                </article>
                <article class="metric-card">
                    <div class="metric-label">${escapeHtml(state.h2h.b)} rates higher</div>
                    <div class="metric-value">${bHigher}</div>
                </article>
            </div>

            <div class="panel-heading" style="margin-top:22px;">
                <p class="eyebrow">Biggest Splits</p>
                <h2>Disagreements</h2>
            </div>
            <div class="feature-grid-4">
                ${topDiff.map((god) => {
                    const winner = god.diff > 0 ? state.h2h.a : state.h2h.b;
                    return h2hCard(god, `<span style="color:${playerColor(winner)}">+${god.absDiff} pts</span> • ${escapeHtml(winner)} higher`);
                }).join("") || emptyState("No Overlap", "These players do not share enough rated gods yet.")}
            </div>

            <div class="panel-heading" style="margin-top:22px;">
                <p class="eyebrow">Shared Love</p>
                <h2>Agreed Upon Gods</h2>
            </div>
            <div class="h2h-grid">
                ${agreed.map((god) => {
                    const avg = Math.round((god[state.h2h.a] + god[state.h2h.b]) / 2);
                    return h2hCard(god, `<span style="color:var(--green)">AVG ${avg}</span> • Δ${god.absDiff}`);
                }).join("") || emptyState("No Agreed Gods", "No high-score agreements match the current comparison.")}
            </div>
            ${renderBackToTop()}
        </div>
    `;

    document.getElementById("h2h-player-a").value = state.h2h.a;
    document.getElementById("h2h-player-b").value = state.h2h.b;

    document.getElementById("h2h-player-a")?.addEventListener("change", (event) => {
        state.h2h.a = event.target.value;
        renderH2hTab();
    });
    document.getElementById("h2h-player-b")?.addEventListener("change", (event) => {
        state.h2h.b = event.target.value;
        renderH2hTab();
    });
}

// This helper renders the recent activity feed and its client-side filters.
function renderActivityTab() {
    let history = [...state.recentHistory];

    if (state.activity.player !== "All") {
        history = history.filter((row) => row.player === state.activity.player);
    }
    if (state.activity.type === "Rating changes") {
        history = history.filter((row) => (row.change_type || "rating") === "rating");
    }
    if (state.activity.type === "Rank changes") {
        history = history.filter((row) => row.change_type === "rank");
    }

    const rows = history.map((row) => {
        const type = row.change_type || "rating";
        const typeStyle = type === "rank"
            ? "background:rgba(129,102,186,0.12);color:#8166ba"
            : "background:rgba(204,164,87,0.16);color:#8f6a2a";

        let action = "";
        let diff = "";
        let diffColor = "#8c8378";

        if (type === "rank") {
            const oldValue = row.old_value;
            const newValue = row.new_value;
            if (oldValue == null && newValue != null) {
                action = `ranked #${newValue}`;
                diff = `#${newValue}`;
            } else if (oldValue != null && newValue == null) {
                action = `removed rank #${oldValue}`;
                diff = "removed";
                diffColor = "#c86868";
            } else {
                const move = Number(oldValue) - Number(newValue);
                action = `#${oldValue} → #${newValue}`;
                diff = move > 0 ? `▲${move}` : move < 0 ? `▼${Math.abs(move)}` : "•";
                diffColor = move > 0 ? "#4aa274" : move < 0 ? "#c86868" : "#8c8378";
            }
        } else {
            const oldValue = Number(row.old_value || 0);
            const newValue = Number(row.new_value || 0);
            const delta = newValue - oldValue;
            action = oldValue === 0 ? `first rated ${newValue}` : newValue === 0 ? "unrated" : `${oldValue} → ${newValue}`;
            diff = oldValue === 0 ? `+${newValue}` : newValue === 0 ? "removed" : `${delta > 0 ? "+" : ""}${delta}`;
            diffColor = delta > 0 ? "#4aa274" : delta < 0 ? "#c86868" : "#8c8378";
        }

        const god = state.gods.find((item) => item.God === row.god_name);

        return `
            <article class="activity-item">
                <div class="activity-thumb">
                    ${god?.ImageUrl ? `<img class="god-art" src="${god.ImageUrl}" alt="${escapeHtml(row.god_name)}">` : `<div class="image-fallback">Art</div>`}
                </div>
                <div>
                    <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
                        <strong style="color:${playerColor(row.player || "")}">${escapeHtml(row.player || "Unknown")}</strong>
                        <span class="activity-type" style="${typeStyle}">${escapeHtml(type.toUpperCase())}</span>
                        <span>${escapeHtml(row.god_name || "Unknown")}</span>
                    </div>
                    <div class="rank-meta">${formatDateTime(row.changed_at)}</div>
                </div>
                <div class="activity-side">
                    <div>${escapeHtml(action)}</div>
                    <div style="color:${diffColor};font-weight:900">${escapeHtml(diff)}</div>
                </div>
            </article>
        `;
    }).join("");

    const playerOptions = [`<option value="All">All</option>`, ...state.config.players.map((player) => `<option value="${escapeHtml(player)}">${escapeHtml(player)}</option>`)].join("");

    elements.tabActivity.innerHTML = `
        <div class="panel">
            <div class="panel-heading">
                <p class="eyebrow">Council Log</p>
                <h2>Recent Activity</h2>
            </div>
            <div class="activity-filter-grid">
                <label class="field">
                    <span>Filter Player</span>
                    <select id="activity-player">${playerOptions}</select>
                </label>
                <label class="field">
                    <span>Filter Type</span>
                    <select id="activity-type">
                        <option value="All">All</option>
                        <option value="Rating changes">Rating changes</option>
                        <option value="Rank changes">Rank changes</option>
                    </select>
                </label>
            </div>
            <div class="activity-stream" style="margin-top:16px;">
                ${rows || emptyState("No Activity", "No recent activity matches the selected filters.")}
            </div>
            ${renderBackToTop()}
        </div>
    `;

    document.getElementById("activity-player").value = state.activity.player;
    document.getElementById("activity-type").value = state.activity.type;
    document.getElementById("activity-player")?.addEventListener("change", (event) => {
        state.activity.player = event.target.value;
        renderActivityTab();
    });
    document.getElementById("activity-type")?.addEventListener("change", (event) => {
        state.activity.type = event.target.value;
        renderActivityTab();
    });
}

// This helper re-sorts a player's currently rated gods by rating and by the
// existing manual order as a tiebreaker.
function fullResort(playerState) {
    const currentOrderIndex = new Map(playerState.order.map((god, index) => [god, index]));
    playerState.order = Object.keys(playerState.ratings)
        .filter((god) => Number(playerState.ratings[god] || 0) > 0)
        .sort((a, b) => {
            const scoreDiff = Number(playerState.ratings[b] || 0) - Number(playerState.ratings[a] || 0);
            if (scoreDiff !== 0) return scoreDiff;
            const indexDiff = (currentOrderIndex.get(a) ?? 9999) - (currentOrderIndex.get(b) ?? 9999);
            if (indexDiff !== 0) return indexDiff;
            return a.localeCompare(b);
        });
}

// This helper updates one rating in the local rank editor state and applies
// the same auto-resort behavior as the original Streamlit version.
function updateRating(player, godName, nextValue) {
    const playerState = state.ranker.byPlayer[player];
    const value = Math.max(0, Math.min(100, Number(nextValue || 0)));
    playerState.ratings[godName] = value;

    if (value === 0) {
        playerState.order = playerState.order.filter((god) => god !== godName);
    } else if (!playerState.order.includes(godName)) {
        playerState.order.push(godName);
    }

    fullResort(playerState);
    persistRankerDraft(player);
    refreshDirtyState(player);
    renderRankerTab();
}

// This helper moves a god up or down inside the player's manual order and
// bumps ratings when it crosses a higher or lower-rated neighbor.
function moveRank(player, godName, direction) {
    const playerState = state.ranker.byPlayer[player];
    const index = playerState.order.indexOf(godName);
    const targetIndex = index + direction;
    if (index < 0 || targetIndex < 0 || targetIndex >= playerState.order.length) return;

    const neighbor = playerState.order[targetIndex];
    const myScore = Number(playerState.ratings[godName] || 0);
    const neighborScore = Number(playerState.ratings[neighbor] || 0);

    if (direction === -1 && neighborScore > myScore) {
        playerState.ratings[godName] = neighborScore;
    } else if (direction === 1 && neighborScore > 0 && neighborScore < myScore) {
        playerState.ratings[godName] = neighborScore;
    }

    const nextOrder = [...playerState.order];
    [nextOrder[index], nextOrder[targetIndex]] = [nextOrder[targetIndex], nextOrder[index]];
    playerState.order = nextOrder;
    persistRankerDraft(player);
    refreshDirtyState(player);
    renderRankerTab();
}

// This helper returns the ranker rows after search and sort options are applied.
function buildRankerRows(player) {
    const playerState = state.ranker.byPlayer[player];
    const search = state.ranker.search.trim().toLowerCase();
    let ranked = playerState.order.filter((god) => god.toLowerCase().includes(search));
    const unranked = state.gods
        .map((god) => god.God)
        .filter((god) => !playerState.order.includes(god) && god.toLowerCase().includes(search))
        .sort((a, b) => a.localeCompare(b));

    if (state.ranker.mode === "unrated") {
        ranked = [];
    }
    if (state.ranker.mode === "rated") {
        return ranked.map((god, index) => ({ god, rank: index + 1, placed: true }));
    }
    if (state.ranker.mode === "unrated") {
        return unranked.map((god) => ({ god, rank: 0, placed: false }));
    }

    let orderedRanked = [...ranked];
    if (state.ranker.sort === "#1 last") {
        orderedRanked.reverse();
    }

    if (state.ranker.sort === "Show unrated first") {
        return [
            ...unranked.map((god) => ({ god, rank: 0, placed: false })),
            ...orderedRanked.map((god, index) => ({ god, rank: state.ranker.sort === "#1 last" ? playerState.order.length - index : index + 1, placed: true })),
        ];
    }

    return [
        ...orderedRanked.map((god, index) => ({ god, rank: state.ranker.sort === "#1 last" ? playerState.order.length - index : index + 1, placed: true })),
        ...unranked.map((god) => ({ god, rank: 0, placed: false })),
    ];
}

// This helper checks whether a specific god row differs from the saved baseline.
function isRankerRowChanged(player, godName) {
    const baselineRaw = state.ranker.baselineByPlayer[player];
    if (!baselineRaw) return false;
    let baseline;
    try {
        baseline = JSON.parse(baselineRaw);
    } catch (error) {
        return false;
    }
    const current = state.ranker.byPlayer[player];
    const currentScore = Number(current.ratings[godName] || 0);
    const baselineScore = Number(baseline.ratings?.[godName] || 0);
    const currentRank = current.order.indexOf(godName);
    const baselineRank = Array.isArray(baseline.order) ? baseline.order.indexOf(godName) : -1;
    return currentScore !== baselineScore || currentRank !== baselineRank;
}

// This helper jumps the ranker view to either the first unrated god or the
// top of the current rated order.
function jumpRanker(target) {
    const player = state.ranker.selectedPlayer;
    const playerState = state.ranker.byPlayer[player];
    let godName = "";
    if (target === "unrated") {
        godName = state.gods.find((god) => Number(playerState.ratings[god.God] || 0) === 0)?.God || "";
    } else {
        godName = playerState.order[0] || "";
    }
    if (!godName) return;
    const node = document.querySelector(`[data-ranker-row="${CSS.escape(godName)}"]`);
    node?.scrollIntoView({ behavior: "smooth", block: "center" });
}

// This helper persists the currently selected player's ranker edits.
async function saveRanker() {
    const player = state.ranker.selectedPlayer;
    const playerState = state.ranker.byPlayer[player];

    try {
        const payload = await api("/api/save-rankings", {
            method: "POST",
            body: JSON.stringify({
                player,
                ratings: playerState.ratings,
                order: playerState.order,
            }),
        });

        alert(`${payload.message}\nRating changes: ${payload.ratingChanges}\nRank changes: ${payload.rankChanges}`);
        state.ranker.lastSavedByPlayer[player] = new Date().toISOString();
        state.ranker.baselineByPlayer[player] = buildRankerSignature(playerState);
        clearRankerDraft(player);
        refreshDirtyState(player);
        await refreshData();
    } catch (error) {
        alert(error.message);
    }
}

// This helper clears the selected player's local editor state without saving.
function resetRanker() {
    const confirmed = window.confirm("Clear all local ratings for this player? You will still need to press Save to persist.");
    if (!confirmed) return;

    const player = state.ranker.selectedPlayer;
    const playerState = state.ranker.byPlayer[player];
    Object.keys(playerState.ratings).forEach((god) => {
        playerState.ratings[god] = 0;
    });
    playerState.order = [];
    persistRankerDraft(player);
    refreshDirtyState(player);
    renderRankerTab();
}

// This helper attempts to unlock the selected player's editor with the PIN.
async function unlockRanker(player, pin) {
    try {
        await api("/api/unlock", {
            method: "POST",
            body: JSON.stringify({ player, pin }),
        });
        state.ranker.unlocked[player] = true;
        renderRankerTab();
    } catch (error) {
        alert(error.message);
    }
}

// This helper renders the full rate-and-rank editor.
function renderRankerTab() {
    const player = state.ranker.selectedPlayer;
    const playerState = state.ranker.byPlayer[player];
    const unlocked = !!state.ranker.unlocked[player];
    const playerRows = buildRankerRows(player);
    const ratedCount = Object.values(playerState.ratings).filter((value) => Number(value) > 0).length;
    const ratedPercent = state.gods.length ? Math.round((ratedCount / state.gods.length) * 100) : 0;
    const dirty = !!state.ranker.dirtyPlayers[player];
    const lastSaved = formatSavedLabel(state.ranker.lastSavedByPlayer[player]);
    const playerSelectOptions = state.config.players
        .map((entry) => `<option value="${escapeHtml(entry)}">${escapeHtml(entry)}</option>`)
        .join("");

    const lockedBlock = `
        <div class="unlock-card panel-card">
            <p class="eyebrow" style="color:${playerColor(player)}">${escapeHtml(player)}</p>
            <h3>Enter PIN To Unlock</h3>
            <p class="rank-meta">This keeps the rating editor private to each council member.</p>
            <div class="ranker-controls" style="grid-template-columns: 1fr auto; margin-top:14px;">
                <input id="ranker-pin" type="password" placeholder="Enter PIN">
                <button class="btn-primary" id="ranker-unlock-btn" type="button">Unlock</button>
            </div>
        </div>
    `;

    const listRows = playerRows.map((row) => {
        const god = state.gods.find((item) => item.God === row.god);
        const value = Number(playerState.ratings[row.god] || 0);
        const rankLabel = row.placed ? `#${row.rank}` : "—";
        const disabledUp = !row.placed || row.rank <= 1;
        const disabledDown = !row.placed || row.rank >= playerState.order.length;
        const changed = isRankerRowChanged(player, row.god);

        return `
            <article class="ranker-row ${changed ? "ranker-row-changed" : ""}" data-ranker-row="${escapeHtml(row.god)}">
                <div class="ranker-main">
                    <div class="ranker-rank" style="color:${row.placed ? playerColor(player) : "#9d8c76"}">${rankLabel}</div>
                    <div class="ranker-thumb">
                        ${god?.ImageUrl ? `<img class="god-art" src="${god.ImageUrl}" alt="${escapeHtml(row.god)}">` : `<div class="image-fallback">Art</div>`}
                    </div>
                    <div style="min-width:0;">
                        <div class="ranker-name">${escapeHtml(row.god)}</div>
                        <div class="ranker-submeta">${escapeHtml(god?.Tier || "U")} tier • ${escapeHtml(god?.Role || "Unknown")}</div>
                    </div>
                </div>
                <div class="ranker-score">
                    <input class="ranker-score-input" data-god="${escapeHtml(row.god)}" type="number" min="0" max="100" value="${value}">
                </div>
                <div class="ranker-buttons">
                    <button class="mini-btn ${disabledUp ? "" : "primary"} ranker-move-up" data-god="${escapeHtml(row.god)}" ${disabledUp ? "disabled" : ""}>▲</button>
                    <button class="mini-btn ${disabledDown ? "" : "primary"} ranker-move-down" data-god="${escapeHtml(row.god)}" ${disabledDown ? "disabled" : ""}>▼</button>
                </div>
            </article>
        `;
    }).join("");

    elements.tabRanker.innerHTML = `
        <div class="ranker-header">
            <p class="eyebrow">Private Council Workflow</p>
            <h2>Rate &amp; Rank ${dirty ? `<span class="dirty-badge">Unsaved</span>` : ""}</h2>
            <p class="hero-text" style="margin-top:10px;max-width:72ch;">
                Enter a rating for each god and the list will auto-sort into your personal ranking.
                Use the arrows to break ties manually; moving above a higher-rated god will pull your score up to match.
            </p>
            <div class="ranker-status-row">
                <span class="summary-pill ${dirty ? "warm" : "cool"}">${dirty ? "Unsaved changes" : lastSaved}</span>
                <span class="summary-pill">${ratedCount}/${state.gods.length} rated</span>
                <span class="summary-pill">${playerRows.length} visible</span>
            </div>
        </div>

        <div class="panel">
            <div class="ranker-top-grid">
                <label class="field">
                    <span>Council Member</span>
                    <select id="ranker-player-select">${playerSelectOptions}</select>
                </label>
                <div>
                    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                        <span class="metric-label" style="color:${playerColor(player)}">${escapeHtml(player)} progress</span>
                        <span class="rank-meta">${ratedCount}/${state.gods.length} rated (${ratedPercent}%)</span>
                    </div>
                    <div class="progress-bar-shell">
                        <div class="progress-bar-fill" style="width:${ratedPercent}%;background:${playerColor(player)}"></div>
                    </div>
                </div>
            </div>

            ${unlocked ? `
                <div class="ranker-controls">
                    <label class="field">
                        <span>Filter</span>
                        <input id="ranker-search" type="text" placeholder="Search god..." value="${escapeHtml(state.ranker.search)}">
                    </label>
                    <label class="field">
                        <span>List Order</span>
                        <select id="ranker-sort">
                            ${["#1 first", "#1 last", "Show unrated last", "Show unrated first"].map((option) => `
                                <option value="${escapeHtml(option)}" ${state.ranker.sort === option ? "selected" : ""}>${escapeHtml(option)}</option>
                            `).join("")}
                        </select>
                    </label>
                    <label class="field">
                        <span>View</span>
                        <select id="ranker-mode">
                            ${[
                                ["all", "All gods"],
                                ["rated", "Rated only"],
                                ["unrated", "Unrated only"],
                            ].map(([value, label]) => `<option value="${value}" ${state.ranker.mode === value ? "selected" : ""}>${label}</option>`).join("")}
                        </select>
                    </label>
                    <button class="btn-primary ranker-save-trigger" type="button">Save</button>
                    <button class="btn-secondary" id="ranker-reset-btn" type="button">Reset</button>
                </div>
                <div class="ranker-jump-row">
                    <button class="mini-btn" type="button" id="ranker-jump-unrated">Jump to Unrated</button>
                    <button class="mini-btn" type="button" id="ranker-jump-top">Jump to Top 10</button>
                    <span class="rank-meta">Tip: press Ctrl/Cmd+S to save, Enter to move to the next score, Alt+Up/Down to nudge rank.</span>
                </div>
                <div class="ranker-list">${listRows}</div>
                <div class="sticky-ranker-save">
                    <button class="btn-primary ranker-save-trigger sticky-save-btn" type="button">${dirty ? "Save Changes" : "Saved"}</button>
                </div>
                ${renderBackToTop()}
            ` : lockedBlock}
        </div>
    `;

    document.getElementById("ranker-player-select").value = player;
    document.getElementById("ranker-player-select")?.addEventListener("change", (event) => {
        state.ranker.selectedPlayer = event.target.value;
        renderRankerTab();
    });

    if (!unlocked) {
        document.getElementById("ranker-unlock-btn")?.addEventListener("click", () => {
            const pin = document.getElementById("ranker-pin").value;
            unlockRanker(player, pin);
        });
        return;
    }

    document.getElementById("ranker-search")?.addEventListener("input", (event) => {
        state.ranker.search = event.target.value;
        renderRankerTab();
    });

    document.getElementById("ranker-sort")?.addEventListener("change", (event) => {
        state.ranker.sort = event.target.value;
        renderRankerTab();
    });
    document.getElementById("ranker-mode")?.addEventListener("change", (event) => {
        state.ranker.mode = event.target.value;
        renderRankerTab();
    });
    document.getElementById("ranker-jump-unrated")?.addEventListener("click", () => jumpRanker("unrated"));
    document.getElementById("ranker-jump-top")?.addEventListener("click", () => jumpRanker("top"));

    document.querySelectorAll(".ranker-save-trigger").forEach((button) => {
        button.addEventListener("click", saveRanker);
    });
    document.getElementById("ranker-reset-btn")?.addEventListener("click", resetRanker);

    document.querySelectorAll(".ranker-score-input").forEach((input) => {
        input.addEventListener("change", (event) => {
            updateRating(player, event.target.dataset.god, event.target.value);
        });
        input.addEventListener("keydown", (event) => {
            if (event.key === "Enter") {
                event.preventDefault();
                const inputs = [...document.querySelectorAll(".ranker-score-input")];
                const index = inputs.indexOf(event.currentTarget);
                inputs[index + 1]?.focus();
                return;
            }
            if (event.altKey && (event.key === "ArrowUp" || event.key === "ArrowDown")) {
                event.preventDefault();
                moveRank(player, event.currentTarget.dataset.god, event.key === "ArrowUp" ? -1 : 1);
            }
        });
    });

    document.querySelectorAll(".ranker-move-up").forEach((button) => {
        button.addEventListener("click", () => moveRank(player, button.dataset.god, -1));
    });

    document.querySelectorAll(".ranker-move-down").forEach((button) => {
        button.addEventListener("click", () => moveRank(player, button.dataset.god, 1));
    });
}

// This helper keeps the tab strip and visible panel in sync with state.activeTab.
function renderTabs() {
    elements.tabButtons.forEach((button) => {
        button.classList.toggle("active", button.dataset.tab === state.activeTab);
        const isRanker = button.dataset.tab === "ranker";
        const label = state.ui.isMobile ? (button.dataset.mobileLabel || button.dataset.fullLabel) : button.dataset.fullLabel;
        button.textContent = isRanker && state.ranker.dirtyPlayers[state.ranker.selectedPlayer] ? `${label} *` : label;
    });
    elements.tabPanels.forEach((panel) => {
        panel.classList.toggle("active", panel.id === `tab-${state.activeTab}`);
    });
}

// This helper refreshes every view after data or filters change.
function renderAll() {
    applyFilters();
    renderHeroStats();
    renderStatusBanner();
    renderPodium();
    renderSidebar();
    renderIndexTab();
    renderRankingsTab();
    renderFavoritesTab();
    renderTierlistTab();
    renderAnalyticsTab();
    renderH2hTab();
    renderActivityTab();
    renderRankerTab();
    renderTabs();
}

// This helper reloads the bootstrap payload after a successful save and keeps
// the current tab selection intact.
async function refreshData() {
    const activeTab = state.activeTab;
    const unlockedState = { ...state.ranker.unlocked };
    const selectedPlayer = state.ranker.selectedPlayer;
    const lastSavedByPlayer = { ...state.ranker.lastSavedByPlayer };
    await loadBootstrap();
    await loadAnalyticsHistory();
    state.ranker.unlocked = { ...state.ranker.unlocked, ...unlockedState };
    state.ranker.selectedPlayer = selectedPlayer;
    state.ranker.lastSavedByPlayer = { ...state.ranker.lastSavedByPlayer, ...lastSavedByPlayer };
    state.activeTab = activeTab;
    renderAll();
}

// This block boots the whole frontend: gather elements, load data, fetch the
// initial analytics history, and render every tab.
document.addEventListener("DOMContentLoaded", async () => {
    cacheElements();
    configureResponsiveDefaults();
    bindStaticEvents();

    try {
        await loadBootstrap();
        await loadAnalyticsHistory();
        renderAll();
    } catch (error) {
        document.querySelector(".app-shell").innerHTML = emptyState("App Failed To Load", error.message);
    }
});
