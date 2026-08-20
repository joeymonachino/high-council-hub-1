// Council Scroll recap helpers and rendering.
// Kept separate because this feature has its own recent-performance rules that
// also inform the email recap workflow on the backend.

const COUNCIL_SCROLL_RECENT_LIMIT = 25;

// This helper finds a roster god with lenient name matching so Scroll cards can
// connect match-history names to local artwork, ratings, and rankings.
function findGodByName(name) {
    const target = canonicalGodKey(name);
    return state.gods.find((god) => canonicalGodKey(god.God) === target) || null;
}

// This helper keeps Scroll comparisons resilient to punctuation/casing from
// Tracker, SmiteSource, and hand-loaded rows.
function normalizeGodName(name) {
    return String(name || "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

// This helper applies known god-name aliases after normalization so activity,
// match history, and roster rows point at the same council record.
function canonicalGodKey(name) {
    const normalized = normalizeGodName(name);
    const aliases = {
        morrigan: "themorrigan",
        themorrigan: "themorrigan",
        morgenlefay: "morganlefay",
        morganlefay: "morganlefay",
        daji: "daji",
        change: "change",
        bakekujira: "bakekujira",
    };
    return aliases[normalized] || normalized;
}

// This helper converts win/game counts into the compact record object used by
// existing formatting helpers.
function finishScrollRecord(record) {
    const games = Number(record.games || 0);
    const wins = Number(record.wins || 0);
    const losses = Math.max(games - wins, Number(record.losses || 0));
    return {
        ...record,
        games,
        wins,
        losses,
        winRate: games ? wins / games * 100 : 0,
    };
}

// This helper builds recent-only god stats from the latest 25 Joust matches for
// one player so the Scroll is about current behavior rather than all-time identity.
function buildRecentGodStats(profile, limit = COUNCIL_SCROLL_RECENT_LIMIT) {
    const rows = (profile.recentMatches || []).slice(0, limit);
    const byGod = new Map();
    rows.forEach((match) => {
        const name = match.godName || "Unknown";
        const current = byGod.get(name) || {
            name,
            games: 0,
            gamesPlayed: 0,
            wins: 0,
            losses: 0,
            kills: 0,
            deaths: 0,
            assists: 0,
            lastPlayed: match.startedAt || "",
        };
        current.games += 1;
        current.gamesPlayed += 1;
        current.wins += match.won ? 1 : 0;
        current.losses += match.won ? 0 : 1;
        current.kills += Number(match.kills || 0);
        current.deaths += Number(match.deaths || 0);
        current.assists += Number(match.assists || 0);
        current.lastPlayed = current.lastPlayed || match.startedAt || "";
        byGod.set(name, current);
    });
    return [...byGod.values()].map((record) => {
        const finished = finishScrollRecord(record);
        return {
            ...finished,
            kda: finished.deaths ? (finished.kills + finished.assists) / finished.deaths : finished.kills + finished.assists,
        };
    });
}

// This helper returns the current subjective favorites from ratings/rankings so
// recent performance can be compared against what each player already believes.
function buildCurrentFavorites(player, limit = 5) {
    const rankingMap = state.allRankings?.[player] || {};
    const ranked = state.gods
        .filter((god) => rankingMap[god.God])
        .sort((a, b) => Number(rankingMap[a.God]) - Number(rankingMap[b.God]))
        .slice(0, limit)
        .map((god) => ({ name: god.God, rating: god[player] || null, rank: rankingMap[god.God] || null }));
    if (ranked.length) return ranked;

    return state.gods
        .filter((god) => Number(god[player] || 0) > 0)
        .sort((a, b) => Number(b[player] || 0) - Number(a[player] || 0) || a.God.localeCompare(b.God))
        .slice(0, limit)
        .map((god) => ({ name: god.God, rating: god[player] || null, rank: rankingMap[god.God] || null }));
}

// This helper finds the newest rating/rank touch for one player+god so Scroll
// nudges can call out played-a-lot picks that may deserve a refreshed opinion.
function latestCouncilActivityForGod(player, godName) {
    const target = canonicalGodKey(godName);
    return [...(state.recentHistory || [])]
        .filter((row) => row.player === player && canonicalGodKey(row.god_name) === target && ["rating", "rank"].includes(row.change_type || "rating"))
        .sort((a, b) => new Date(b.changed_at || 0) - new Date(a.changed_at || 0))[0] || null;
}

// This helper counts ranking/rating gaps and lists recent gods that deserve a
// quick update after actual play.
function buildRecentUpdateNudges(player, recentGods, limit = 5) {
    const rankingMap = state.allRankings?.[player] || {};
    const unratedCount = state.gods.filter((god) => !Number(god[player] || 0)).length;
    const unrankedCount = state.gods.filter((god) => !rankingMap[god.God]).length;
    const nudges = [...recentGods]
        .map((god) => {
            const catalogGod = findGodByName(god.name) || {};
            const hasRating = Number(catalogGod[player] || 0) > 0;
            const canonicalName = catalogGod.God || god.name;
            const hasRank = Boolean(rankingMap[canonicalName] || rankingMap[god.name]);
            const recentGames = Number(god.games || god.gamesPlayed || 0);
            const latestActivity = latestCouncilActivityForGod(player, canonicalName);
            const reasons = [];
            if (!hasRating || !hasRank) reasons.push("needs update");
            if (hasRating && hasRank && recentGames >= 3 && !latestActivity) reasons.push(`${recentGames} recent plays, revisit`);
            return { ...god, name: canonicalName, rating: catalogGod[player] || null, rank: rankingMap[canonicalName] || rankingMap[god.name] || null, reasons };
        })
        .filter((god) => god.reasons.length)
        .sort((a, b) => Number(b.games || 0) - Number(a.games || 0) || new Date(b.lastPlayed || 0) - new Date(a.lastPlayed || 0))
        .slice(0, limit);
    return { unratedCount, unrankedCount, nudges };
}

// This helper renders one recent god row with optional art and council context.
function renderRecentGodLine(god, player, { label = "", showReasons = false } = {}) {
    const name = god.name || god.God || god.godName || god.god || "Unknown";
    const catalogGod = findGodByName(name) || {};
    const rating = god.rating || catalogGod[player] || null;
    const rank = god.rank || state.allRankings?.[player]?.[name] || null;
    const imageUrl = catalogGod.ImageUrl || god.imageUrl || "";
    const recordText = Number(god.games || god.gamesPlayed || 0)
        ? `${formatWinLossRecord(god.wins, god.games || god.gamesPlayed)} | ${formatMetric(god.winRate, 1, "%")} WR`
        : "Current favorite";
    const note = showReasons && god.reasons?.length ? god.reasons.join(" + ") : [rating ? `${rating}` : "--", rank ? `#${rank}` : "--"].join(" / ");
    return `
        <article class="scroll-recap-god-line">
            <div class="scroll-recap-thumb">${imageUrl ? `<img src="${imageUrl}" alt="${escapeHtml(name)}" loading="lazy" decoding="async">` : `<span>?</span>`}</div>
            <div>
                ${label ? `<span>${escapeHtml(label)}</span>` : ""}
                <strong>${escapeHtml(name)}</strong>
                <small>${escapeHtml(recordText)}</small>
            </div>
            <em>${escapeHtml(note)}</em>
        </article>
    `;
}

// This helper renders a top-five list for one player card while keeping empty
// states compact enough for email-style reuse later.
function renderRecentGodList(gods, player, emptyText, { reasons = false, limit = 5 } = {}) {
    if (!gods.length) return `<div class="rank-meta">${escapeHtml(emptyText)}</div>`;
    return gods.slice(0, limit).map((god, index) => renderRecentGodLine(god, player, { label: `#${index + 1}`, showReasons: reasons })).join("");
}

// This helper renders recent god rows as dense chips inside a report-table cell,
// which lets the Scroll show more of the last-25 sample without massive cards.
function renderRecentGodChips(gods, player, emptyText, { reasons = false, limit = 5 } = {}) {
    const rows = [...(gods || [])].slice(0, limit);
    if (!rows.length) return `<span class="scroll-recap-empty-chip">${escapeHtml(emptyText)}</span>`;
    return rows.map((god, index) => {
        const name = god.name || god.God || god.godName || god.god || "Unknown";
        const catalogGod = findGodByName(name) || {};
        const rating = god.rating || catalogGod[player] || null;
        const rank = god.rank || state.allRankings?.[player]?.[name] || null;
        const recordText = Number(god.games || god.gamesPlayed || 0)
            ? `${formatWinLossRecord(god.wins, god.games || god.gamesPlayed)} ${formatMetric(god.winRate, 0, "%")}`
            : [rating ? `${rating}` : "--", rank ? `#${rank}` : "--"].join("/");
        const note = reasons && god.reasons?.length ? god.reasons.join(" + ") : recordText;
        return `<span class="scroll-recap-chip"><b>${index + 1}. ${escapeHtml(name)}</b><small>${escapeHtml(note)}</small></span>`;
    }).join("");
}

// This helper builds a compact recent player recap from only the latest 25 rows.
function buildScrollPlayerRecap(profile) {
    const player = profile.player;
    const recentRows = (profile.recentMatches || []).slice(0, COUNCIL_SCROLL_RECENT_LIMIT);
    const wins = recentRows.filter((match) => match.won).length;
    const losses = Math.max(recentRows.length - wins, 0);
    const recentGods = buildRecentGodStats(profile);
    const mostPlayed = [...recentGods]
        .sort((a, b) => Number(b.games || 0) - Number(a.games || 0) || Number(b.winRate || 0) - Number(a.winRate || 0) || a.name.localeCompare(b.name))
        .slice(0, 5);
    const best = [...recentGods]
        .filter((god) => Number(god.wins || 0) > 0)
        .sort((a, b) => Number(b.winRate || 0) - Number(a.winRate || 0) || Number(b.games || 0) - Number(a.games || 0) || a.name.localeCompare(b.name))
        .slice(0, 5);
    const worst = [...recentGods]
        .filter((god) => Number(god.losses || 0) > 0)
        .sort((a, b) => Number(a.winRate || 0) - Number(b.winRate || 0) || Number(b.losses || 0) - Number(a.losses || 0) || Number(b.games || 0) - Number(a.games || 0) || a.name.localeCompare(b.name))
        .slice(0, 5);
    const nudges = buildRecentUpdateNudges(player, recentGods);
    return {
        player,
        recentRows,
        wins,
        losses,
        winRate: recentRows.length ? wins / recentRows.length * 100 : 0,
        favorites: buildCurrentFavorites(player, 5),
        mostPlayed,
        best: best.length ? best : [...recentGods].sort((a, b) => Number(b.winRate || 0) - Number(a.winRate || 0)).slice(0, 5),
        worst,
        nudges,
    };
}

// This helper filters recent shared sessions to the latest 25 exact duo or trio
// samples, depending on the members requested.
function recentSessionsForMembers(sessions, members) {
    const key = chemistryMembersKey(members);
    return [...(sessions || [])]
        .filter((session) => chemistryMembersKey(session.participants || []) === key)
        .slice(0, COUNCIL_SCROLL_RECENT_LIMIT);
}

// This helper aggregates god compositions from recent duo/trio sessions.
function buildRecentCompRecords(sessions, members) {
    const records = new Map();
    sessions.forEach((session) => {
        const participantGods = session.participantGods || {};
        const label = members.map((member) => participantGods[member] || "Unknown").join(" + ");
        const key = `${chemistryMembersKey(members)}|${label}`;
        const current = records.get(key) || { label, members, participantGods, games: 0, wins: 0, losses: 0 };
        current.games += 1;
        current.wins += session.won ? 1 : 0;
        current.losses += session.won ? 0 : 1;
        records.set(key, current);
    });
    return [...records.values()].map(finishScrollRecord);
}

// This helper builds a recent-only summary for one duo/trio group.
function buildRecentGroupRecap(allSessions, members) {
    const sessions = recentSessionsForMembers(allSessions, members);
    const wins = sessions.filter((session) => session.won).length;
    const losses = Math.max(sessions.length - wins, 0);
    const comps = buildRecentCompRecords(sessions, members);
    return {
        members,
        sessions,
        wins,
        losses,
        winRate: sessions.length ? wins / sessions.length * 100 : 0,
        best: [...comps].sort((a, b) => Number(b.winRate || 0) - Number(a.winRate || 0) || Number(b.games || 0) - Number(a.games || 0) || a.label.localeCompare(b.label)).slice(0, 3),
        worst: [...comps].sort((a, b) => Number(a.winRate || 0) - Number(b.winRate || 0) || Number(b.games || 0) - Number(a.games || 0) || a.label.localeCompare(b.label)).slice(0, 3),
        mostPlayed: [...comps].sort((a, b) => Number(b.games || 0) - Number(a.games || 0) || Number(b.winRate || 0) - Number(a.winRate || 0) || a.label.localeCompare(b.label)).slice(0, 3),
    };
}

// This helper renders a compact comp row that emphasizes record first and hides
// player assignments in one readable line.
function renderRecentCompRows(records, emptyText) {
    if (!records.length) return `<div class="rank-meta">${escapeHtml(emptyText)}</div>`;
    return records.map((record) => `
        <article class="scroll-recap-comp-line ${Number(record.winRate || 0) >= 50 ? "good" : "rough"}">
            <div>
                <strong>${escapeHtml(record.label || "Unknown comp")}</strong>
                <small>${escapeHtml(formatParticipantAssignments(record.participantGods || {}, record.members || []) || (record.members || []).join(" + "))}</small>
            </div>
            <span>${escapeHtml(formatRecord(record))}</span>
        </article>
    `).join("");
}

// This helper renders the recent-only group cards for duo and trio snapshots.
function renderRecentGroupCard(recap, title, eyebrow = "Recent Chemistry") {
    return `
        <article class="scroll-recap-group-card">
            <div class="scroll-recap-card-head">
                <div><p class="eyebrow">${escapeHtml(eyebrow)}</p><h3>${escapeHtml(title)}</h3></div>
                <span class="summary-pill">${recap.wins}-${recap.losses} | ${formatMetric(recap.winRate, 1, "%")}</span>
            </div>
            <div class="scroll-recap-group-grid">
                <div><div class="metric-label">Best recent comps</div>${renderRecentCompRows(recap.best, "No recent winning comp sample")}</div>
                <div><div class="metric-label">Worst recent comps</div>${renderRecentCompRows(recap.worst, "No recent shaky comp sample")}</div>
                <div><div class="metric-label">Most repeated</div>${renderRecentCompRows(recap.mostPlayed, "No repeated recent comp yet")}</div>
            </div>
        </article>
    `;
}

// This helper renders the Council Scroll as a compact recent-performance recap.
// Unlike Rater Profile, every stat here is intentionally scoped to last 25.
function renderCouncilScrollTab() {
    if (!elements.tabCouncilScroll) return;

    if (!state.raterStats.loaded) {
        elements.tabCouncilScroll.innerHTML = `
            <div class="panel council-scroll-panel scroll-recap-panel">
                <div class="panel-heading panel-heading-inline">
                    <div><p class="eyebrow">Council Recap</p><h2>Council Scroll</h2></div>
                    <span class="summary-pill">Loading match ledgers</span>
                </div>
                ${emptyState("Gathering Recent Joust Stats", "Loading stored player and chemistry data for the tailored recap.")}
            </div>
        `;
        return;
    }

    const recapPlayers = ["Joey", "Jami", "Darian", "Mike"].filter((player) => state.config.players.includes(player));
    const trinityPlayers = ["Joey", "Jami", "Darian"].filter((player) => state.config.players.includes(player));
    const profiles = recapPlayers.map((player) => buildRaterProfile(player));
    const playerRecaps = profiles.map(buildScrollPlayerRecap);
    const insights = buildChemistryInsights();
    const recentSessions = insights.recentSessions || [];
    const trioRecap = buildRecentGroupRecap(recentSessions, trinityPlayers);
    const duoRecaps = [["Joey", "Jami"], ["Joey", "Darian"], ["Jami", "Darian"]]
        .filter((members) => members.every((member) => trinityPlayers.includes(member)))
        .map((members) => buildRecentGroupRecap(recentSessions, members));
    const totalRecent = playerRecaps.reduce((sum, recap) => sum + recap.recentRows.length, 0);
    const totalWins = playerRecaps.reduce((sum, recap) => sum + recap.wins, 0);
    const totalLosses = playerRecaps.reduce((sum, recap) => sum + recap.losses, 0);
    const updateNudges = playerRecaps.reduce((sum, recap) => sum + recap.nudges.nudges.length, 0);

    const playerCards = `
        <div class="scroll-recap-player-table">
            <div class="scroll-recap-player-row scroll-recap-player-row-head">
                <span>Player</span>
                <span>Update Nudges</span>
                <span>Favorites</span>
                <span>Most Played Last 25</span>
                <span>Best W/L</span>
                <span>Poor W/L</span>
            </div>
            ${playerRecaps.map((recap) => `
                <article class="scroll-recap-player-row">
                    <div class="scroll-recap-player-cell player-summary">
                        <strong>${escapeHtml(recap.player)}</strong>
                        <small>${recap.wins}-${recap.losses} | ${formatMetric(recap.winRate, 1, "%")} | ${formatMetric(recap.recentRows.length)} matches</small>
                        <em>${formatMetric(recap.nudges.unratedCount)} unrated roster gods</em>
                    </div>
                    <div class="scroll-recap-player-cell nudges">${renderRecentGodChips(recap.nudges.nudges, recap.player, "No gaps", { reasons: true, limit: 5 })}</div>
                    <div class="scroll-recap-player-cell">${renderRecentGodChips(recap.favorites, recap.player, "No favorites", { limit: 5 })}</div>
                    <div class="scroll-recap-player-cell">${renderRecentGodChips(recap.mostPlayed, recap.player, "No recent sample", { limit: 5 })}</div>
                    <div class="scroll-recap-player-cell">${renderRecentGodChips(recap.best, recap.player, "No wins", { limit: 5 })}</div>
                    <div class="scroll-recap-player-cell">${renderRecentGodChips(recap.worst, recap.player, "No losses", { limit: 5 })}</div>
                </article>
            `).join("")}
        </div>
    `;

    elements.tabCouncilScroll.innerHTML = `
        <div class="panel council-scroll-panel scroll-recap-panel">
            <section class="scroll-recap-hero">
                <div>
                    <p class="eyebrow">Council Scroll</p>
                    <h2>Last 25 Joust Check-In</h2>
                    <p>Recent play only: what everyone has been favoring, what is winning, what is struggling, and which ratings or ranks may need a fresh look.</p>
                    <p class="scroll-recap-reminder">Want the full archive? Open Rater Profile for all-time player data or Chemistry for all-time duo/trio receipts.</p>
                    <div class="scroll-recap-actions">
                        <button class="btn-primary" type="button" data-send-council-scroll-email="true" ${state.councilScroll.emailing ? "disabled" : ""}>${state.councilScroll.emailing ? "Sending..." : "Email Recap To Joey"}</button>
                        <span>${escapeHtml(state.councilScroll.emailMessage || "Joey-only until a sending domain is verified; forward it from Gmail.")}</span>
                    </div>
                </div>
                <div class="scroll-recap-mini-stats hero-stats">
                    <span><strong>${formatMetric(totalRecent)}</strong><small>Player samples</small></span>
                    <span><strong>${totalWins}-${totalLosses}</strong><small>Player W/L</small></span>
                    <span><strong>${trioRecap.wins}-${trioRecap.losses}</strong><small>Trio W/L</small></span>
                    <span><strong>${formatMetric(updateNudges)}</strong><small>Update nudges</small></span>
                </div>
            </section>

            <section class="scroll-recap-section">
                <div class="section-head"><div><p class="eyebrow">Players</p><h3>Favorites, Form, And Rating Prompts</h3></div><span class="summary-pill">Last ${COUNCIL_SCROLL_RECENT_LIMIT} each</span></div>
                <div class="scroll-recap-player-grid">${playerCards}</div>
            </section>

            <section class="scroll-recap-section">
                <div class="section-head"><div><p class="eyebrow">Trinity</p><h3>Recent Trio Snapshot</h3></div><span class="summary-pill">Exact trio sessions</span></div>
                ${renderRecentGroupCard(trioRecap, trinityPlayers.join(" + "), "Last 25 Trio")}
            </section>

            <section class="scroll-recap-section">
                <div class="section-head"><div><p class="eyebrow">Duos</p><h3>Recent Pairing Snapshot</h3></div><span class="summary-pill">Exact duo sessions</span></div>
                <div class="scroll-recap-duo-grid">${duoRecaps.map((recap) => renderRecentGroupCard(recap, recap.members.join(" + "), "Last 25 Duo")).join("")}</div>
            </section>

            ${renderBackToTop()}
        </div>
    `;
}
// This helper formats a health timestamp as a short date, which keeps admin
// cards dense without losing the important recency signal.
