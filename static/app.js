function renderChemistryRows(records, { emptyText, main, sub, value, valueClass = "" }) {
    if (!records.length) return `<div class="rank-meta">${escapeHtml(emptyText)}</div>`;
    return records.map((record) => `
        <div class="mini-highlight-row chemistry-detail-row">
            <span class="chemistry-row-text">
                <span class="chemistry-row-main">${main(record)}</span>
                ${sub ? `<span class="chemistry-row-sub">${sub(record)}</span>` : ""}
            </span>
            <strong class="${typeof valueClass === "function" ? valueClass(record) : valueClass}">${value ? value(record) : formatRecord(record)}</strong>
        </div>
    `).join("");
}

// This helper renders compact chemistry chips so combos and stacks read more
// like receipts than plain text labels.
function renderChemistryChips(items = [], tone = "neutral") {
    if (!items.length) return "";
    return `
        <div class="chemistry-chip-row">
            ${items.map((item) => `<span class="chemistry-chip chemistry-chip-${tone}">${escapeHtml(item)}</span>`).join("")}
        </div>
    `;
}

// This helper renders the bold spotlight cards at the top of Chemistry so the
// most interesting duo/trio stories are obvious before the detail grids.
function renderChemistrySpotlightCard({ eyebrow, title, record, emptyText, tone = "gold", chips = [], subline = "" }) {
    if (!record) {
        return `
            <article class="chemistry-spotlight chemistry-spotlight-${tone}">
                <p class="eyebrow">${escapeHtml(eyebrow)}</p>
                <h3>${escapeHtml(title)}</h3>
                <div class="chemistry-spotlight-empty">${escapeHtml(emptyText)}</div>
            </article>
        `;
    }

    return `
        <article class="chemistry-spotlight chemistry-spotlight-${tone}">
            <p class="eyebrow">${escapeHtml(eyebrow)}</p>
            <h3>${escapeHtml(title)}</h3>
            <div class="chemistry-spotlight-label">${escapeHtml(record.label || "—")}</div>
            ${chips.length ? renderChemistryChips(chips, tone) : ""}
            ${subline ? `<div class="chemistry-spotlight-subline">${escapeHtml(subline)}</div>` : ""}
            <div class="chemistry-spotlight-record">${formatRecord(record)}</div>
        </article>
    `;
}

// This helper turns the current chemistry leaders into a short narrative so the
// tab feels like a council story instead of just a stat wall.
function buildChemistryNarrative({ heroDuo, heroCombo, heroTrio, heroTrioCombo, heroWorstCombo, heroWorstTrioCombo, heroQueue }) {
    const lines = [];

    if (heroDuo?.label && heroCombo?.label) {
        lines.push(`${heroDuo.label} are the current power duo, with ${heroCombo.label} standing out as their cleanest winning look.`);
    }
    if (heroTrio?.label && heroTrioCombo?.label) {
        lines.push(`${heroTrio.label} form the current centerpiece stack, and ${heroTrioCombo.label} is the comp most worth respecting when all three queue together.`);
    }
    if (heroQueue?.label) {
        lines.push(`${heroQueue.label} is the queue where the council chemistry is hitting hardest right now.`);
    }
    if (heroWorstCombo?.label || heroWorstTrioCombo?.label) {
        lines.push(`Watchlist: ${heroWorstCombo?.label || "one shaky duo look"} and ${heroWorstTrioCombo?.label || "one shaky trio comp"} are the spots where the receipts still look rough.`);
    }

    return lines;
}

const CHEMISTRY_TRINITY = ["Darian", "Jami", "Joey"];

function chemistryMembersKey(members = []) {
    return [...members].sort((left, right) => left.localeCompare(right)).join("|");
}

function trioSessionsFromInsights(insights) {
    return (insights.recentSessions || []).filter((session) => {
        const members = session.participants || [];
        return CHEMISTRY_TRINITY.every((member) => members.includes(member));
    });
}

function duoSessionsFromInsights(insights) {
    return (insights.recentSessions || []).filter((session) => {
        const members = session.participants || [];
        const trinityMembers = members.filter((member) => CHEMISTRY_TRINITY.includes(member));
        return trinityMembers.length >= 2;
    });
}

function aggregateChemistryMemberPicks(records = [], members = []) {
    const counts = new Map();
    records.forEach((record) => {
        const games = Number(record.games || 0);
        members.forEach((member) => {
            const god = record.participantGods?.[member];
            if (!god) return;
            const key = `${member}|${god}`;
            counts.set(key, (counts.get(key) || 0) + games);
        });
    });
    return [...counts.entries()]
        .map(([key, games]) => {
            const [member, god] = key.split("|");
            return { member, god, games };
        })
        .sort((a, b) => b.games - a.games || a.member.localeCompare(b.member) || a.god.localeCompare(b.god));
}


function chemistryClassAssignments(participantGods = {}, members = []) {
    const assignments = members.map((member) => {
        const godName = participantGods?.[member] || "";
        const meta = godMetaByName(godName);
        return {
            member,
            className: meta?.Class || "Unknown",
        };
    });
    return {
        assignments,
        label: assignments.map((entry) => `${entry.member} ${entry.className}`).join(" + "),
        key: assignments.map((entry) => `${entry.member}:${entry.className}`).join("|"),
    };
}

function aggregateChemistryMemberRecords(records = [], members = []) {
    const counts = new Map();
    records.forEach((record) => {
        const games = Number(record.games || 0);
        const wins = Number(record.wins || 0);
        const losses = Number(record.losses || Math.max(0, games - wins));
        members.forEach((member) => {
            const god = record.participantGods?.[member];
            if (!god) return;
            const key = `${member}|${god}`;
            if (!counts.has(key)) {
                counts.set(key, { member, god, games: 0, wins: 0, losses: 0 });
            }
            const entry = counts.get(key);
            entry.games += games;
            entry.wins += wins;
            entry.losses += losses;
        });
    });
    return [...counts.values()]
        .map((entry) => ({
            ...entry,
            winRate: entry.games ? Math.round((entry.wins / entry.games) * 1000) / 10 : 0,
        }))
        .sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.member.localeCompare(b.member) || a.god.localeCompare(b.god));
}

function buildReliableComfortPicks(records = [], members = [], { minGames = 2, limit = 4 } = {}) {
    const aggregated = aggregateChemistryMemberRecords(records, members);
    const reliable = aggregated
        .filter((entry) => entry.games >= minGames && entry.winRate >= 55)
        .sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.member.localeCompare(b.member) || a.god.localeCompare(b.god));
    return (reliable.length ? reliable : aggregated).slice(0, limit);
}

function buildContextTags(primaryRecords = [], secondaryRecords = [], members = [], { primaryLabel, secondaryLabel, limit = 3 } = {}) {
    const primary = aggregateChemistryMemberRecords(primaryRecords, members);
    const secondary = new Map(
        aggregateChemistryMemberRecords(secondaryRecords, members).map((entry) => [`${entry.member}|${entry.god}`, entry]),
    );

    return primary
        .map((entry) => {
            const counterpart = secondary.get(`${entry.member}|${entry.god}`);
            if (!counterpart) {
                if (entry.games < 2) return null;
                return {
                    label: `${entry.member} ${entry.god}`,
                    context: `${entry.member} ${entry.god} has only shown up in ${primaryLabel.toLowerCase()} sets so far`,
                    games: entry.games,
                    strength: entry.winRate,
                };
            }
            const delta = Math.round((entry.winRate - counterpart.winRate) * 10) / 10;
            if (Math.abs(delta) < 15) return null;
            return {
                label: `${entry.member} ${entry.god}`,
                context: delta > 0
                    ? `${entry.member} ${entry.god} performs better in ${primaryLabel.toLowerCase()} than ${secondaryLabel.toLowerCase()}`
                    : `${entry.member} ${entry.god} performs better in ${secondaryLabel.toLowerCase()} than ${primaryLabel.toLowerCase()}`,
                games: entry.games + counterpart.games,
                strength: Math.abs(delta),
            };
        })
        .filter(Boolean)
        .sort((a, b) => b.strength - a.strength || b.games - a.games || a.label.localeCompare(b.label))
        .slice(0, limit);
}

function renderComfortChips(records = [], tone = "neutral", emptyText = "No comfort reads yet") {
    if (!records.length) return `<span class="rank-meta">${escapeHtml(emptyText)}</span>`;
    return records
        .map((record) => `<span class="chemistry-chip chemistry-chip-${tone}">${escapeHtml(record.member)}: ${escapeHtml(record.god)} • ${formatMetric(record.winRate, 1, "%")} (${record.games})</span>`)
        .join("");
}

function renderContextChips(records = [], tone = "neutral", emptyText = "No strong duo/trio split yet") {
    if (!records.length) return `<span class="rank-meta">${escapeHtml(emptyText)}</span>`;
    return records
        .map((record) => `<span class="chemistry-chip chemistry-chip-${tone}">${escapeHtml(record.context)}</span>`)
        .join("");
}

function renderChemistryTrinityShowcase(insights, isMobile) {
    const trinityKey = chemistryMembersKey(CHEMISTRY_TRINITY);
    const trioRecord = (insights.groupRecords || []).find((record) => chemistryMembersKey(record.members || []) === trinityKey) || null;
    const trioCombos = (insights.trioComboRecords || []).filter((record) => chemistryMembersKey(record.members || []) === trinityKey);
    const trinitySessions = trioSessionsFromInsights(insights);
    const mostPlayedCombo = trioCombos.slice().sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.label.localeCompare(b.label))[0] || null;
    const bestCombo = trioCombos.find((record) => record.games >= 2) || mostPlayedCombo;
    const worstCombo = trioCombos.filter((record) => record.games >= 2).slice().sort((a, b) => a.winRate - b.winRate || b.games - a.games || a.label.localeCompare(b.label))[0] || null;
    const queueMap = new Map();
    trinitySessions.forEach((session) => {
        const queueKey = session.queueType || "Unknown Queue";
        if (!queueMap.has(queueKey)) {
            queueMap.set(queueKey, { label: queueKey, games: 0, wins: 0, losses: 0 });
        }
        const queueRecord = queueMap.get(queueKey);
        queueRecord.games += 1;
        queueRecord.wins += session.won ? 1 : 0;
        queueRecord.losses += session.won ? 0 : 1;
    });
    const queueLeaders = [...queueMap.values()]
        .map((record) => ({ ...record, winRate: record.games ? Math.round((record.wins / record.games) * 1000) / 10 : 0 }))
        .sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.label.localeCompare(b.label));
    const comfortPicks = buildReliableComfortPicks(trioCombos, CHEMISTRY_TRINITY, { minGames: 2, limit: isMobile ? 3 : 5 });
    const trioClassCombos = (insights.trioClassComboRecords || []).filter((record) => chemistryMembersKey(record.members || []) === trinityKey);
    const trioClassWinners = trioClassCombos
        .filter((record) => Number(record.wins || 0) > 0)
        .slice()
        .sort((a, b) => b.winRate - a.winRate || b.wins - a.wins || b.games - a.games || a.label.localeCompare(b.label));
    const bestTrioClass = trioClassWinners[0] || trioClassCombos[0] || null;
    const trioClassLosses = trioClassCombos
        .filter((record) => Number(record.losses || 0) > 0)
        .slice()
        .sort((a, b) => b.losses - a.losses || a.winRate - b.winRate || b.games - a.games || a.label.localeCompare(b.label));
    const worstTrioClass = trioClassLosses.find((record) => record.label !== bestTrioClass?.label) || trioClassLosses[0] || null;
    const winningCombos = trioCombos.slice().sort((a, b) => b.winRate - a.winRate || b.games - a.games || a.label.localeCompare(b.label)).slice(0, isMobile ? 2 : 3);
    const shakyCombos = trioCombos
        .filter((record) => Number(record.losses || 0) > 0)
        .slice()
        .sort((a, b) => b.losses - a.losses || a.winRate - b.winRate || b.games - a.games || a.label.localeCompare(b.label))
        .slice(0, isMobile ? 2 : 3);
    const trioSummaryBits = [
        trioRecord ? `Trio ${formatRecord(trioRecord)}` : "",
        queueLeaders[0] ? `Best in ${queueLeaders[0].label}` : "",
        mostPlayedCombo ? `Most used: ${mostPlayedCombo.label}` : "",
    ].filter(Boolean);

    return `
        <section class="chemistry-trinity-shell">
            <article class="chemistry-trinity-hero panel">
                <div class="chemistry-trinity-copy">
                    <p class="eyebrow">Joust Trinity</p>
                    <h3>Joey, Jami, and Darian</h3>
                    <p class="chemistry-trinity-blurb">The core stack in one clean read: trio record, the comp that actually wins, the comp that slips, and the picks each of you keep showing up on.</p>
                    <div class="chemistry-trinity-pillrow">
                        ${renderChemistryChips(CHEMISTRY_TRINITY, "gold")}
                    </div>
                    ${trioSummaryBits.length ? `<div class="chemistry-chip-row">${trioSummaryBits.map((item) => `<span class="chemistry-chip chemistry-chip-neutral">${escapeHtml(item)}</span>`).join("")}</div>` : ""}
                    <div class="chemistry-trinity-metrics">
                        <div class="chemistry-metric-card">
                            <span class="metric-label">Trio Record</span>
                            <strong class="chemistry-metric-value">${trioRecord ? formatRecord(trioRecord) : "—"}</strong>
                        </div>
                        <div class="chemistry-metric-card">
                            <span class="metric-label">Shared Sessions</span>
                            <strong class="chemistry-metric-value">${trioRecord?.games || trinitySessions.length || 0}</strong>
                        </div>
                        <div class="chemistry-metric-card">
                            <span class="metric-label">Best Queue</span>
                            <strong class="chemistry-metric-value">${queueLeaders[0]?.label || "—"}</strong>
                        </div>
                    </div>
                </div>
                <div class="chemistry-trinity-side">
                    <article class="chemistry-trinity-card chemistry-trinity-card-best">
                        <p class="eyebrow">Winning Trio Comp</p>
                        <h4>${escapeHtml(bestCombo?.label || "Still forming")}</h4>
                        <div class="chemistry-trinity-record">${bestCombo ? formatRecord(bestCombo) : "Need more trio games"}</div>
                        ${bestCombo ? `<div class="chemistry-row-sub">${escapeHtml(formatParticipantAssignments(bestCombo.participantGods || {}, bestCombo.members || []))}</div>` : ""}
                    </article>
                    <article class="chemistry-trinity-card chemistry-trinity-card-watch">
                        <p class="eyebrow">Shakiest Trio Comp</p>
                        <h4>${escapeHtml(worstCombo?.label || "No weak spot yet")}</h4>
                        <div class="chemistry-trinity-record">${worstCombo ? formatRecord(worstCombo) : "Need more trio games"}</div>
                        ${worstCombo ? `<div class="chemistry-row-sub">${escapeHtml(formatParticipantAssignments(worstCombo.participantGods || {}, worstCombo.members || []))}</div>` : ""}
                    </article>
                </div>
            </article>
            <div class="chemistry-trinity-footer">
                <article class="chemistry-trinity-strip panel">
                    <p class="eyebrow">Reliable Comfort Picks</p>
                    <div class="chemistry-chip-row">
                        ${renderComfortChips(comfortPicks, "neutral", "No reliable trio comfort picks yet")}
                    </div>
                </article>
                <article class="chemistry-trinity-strip panel">
                    <p class="eyebrow">Winning Trio Comps</p>
                    <div class="chemistry-mini-stack">
                        ${renderChemistryRows(winningCombos, {
                            emptyText: "No trio comp sample yet",
                            main: (record) => escapeHtml(record.label),
                            sub: (record) => escapeHtml(formatParticipantAssignments(record.participantGods || {}, record.members || [])),
                            value: (record) => formatRecord(record),
                        })}
                    </div>
                    <p class="eyebrow chemistry-subeyebrow">Shaky Trio Comps</p>
                    <div class="chemistry-mini-stack">
                        ${renderChemistryRows(shakyCombos, {
                            emptyText: "No shaky trio sample yet",
                            main: (record) => escapeHtml(record.label),
                            sub: (record) => escapeHtml(formatParticipantAssignments(record.participantGods || {}, record.members || [])),
                            value: (record) => formatRecord(record),
                        })}
                    </div>
                </article>
            </div>
        </section>
    `;
}

function renderChemistrySynergyMatrix(insights, isMobile) {
    const pairs = [
        ["Joey", "Jami"],
        ["Joey", "Darian"],
        ["Jami", "Darian"],
    ];
    const pairTabs = pairs.map(([left, right]) => ({
        key: chemistryMembersKey([left, right]),
        label: `${left} + ${right}`,
        left,
        right,
    }));
    if (!pairTabs.some((tab) => tab.key === state.chemistry.pairSection)) {
        state.chemistry.pairSection = pairTabs[0]?.key || "overview";
    }

    const pairCards = pairTabs.filter((tab) => tab.key === state.chemistry.pairSection).map(({ key, label, left, right }) => {
        const pairRecord = (insights.duoRecords || []).find((record) => chemistryMembersKey(record.members || []) === key) || null;
        const combos = (insights.duoComboRecords || []).filter((record) => chemistryMembersKey(record.members || []) === key);
        const bestCombo = combos.find((record) => record.games >= 2) || combos[0] || null;
        const mostPlayed = combos.slice().sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.label.localeCompare(b.label))[0] || null;
        const watchCombo = combos.filter((record) => record.losses > 0).slice().sort((a, b) => b.losses - a.losses || a.winRate - b.winRate || b.games - a.games || a.label.localeCompare(b.label))[0] || null;
        const winningCombos = combos.slice().sort((a, b) => b.winRate - a.winRate || b.games - a.games || a.label.localeCompare(b.label)).slice(0, isMobile ? 4 : 6);
        const shakyCombos = combos
            .filter((record) => Number(record.losses || 0) > 0)
            .slice()
            .sort((a, b) => b.losses - a.losses || a.winRate - b.winRate || b.games - a.games || a.label.localeCompare(b.label))
            .slice(0, isMobile ? 4 : 6);
        const comfortPicks = buildReliableComfortPicks(combos, [left, right], { minGames: 2, limit: isMobile ? 4 : 6 });
        const swingCombos = combos
            .filter((record) => Number(record.games || 0) >= 2)
            .slice()
            .sort((a, b) => Math.abs((b.winRate || 0) - 50) - Math.abs((a.winRate || 0) - 50) || b.games - a.games)
            .slice(0, isMobile ? 3 : 5);

        return `
            <section class="chemistry-pair-focus" data-pair-panel="${escapeHtml(key)}">
                <article class="chemistry-matrix-card chemistry-pair-hero panel">
                    <div class="chemistry-matrix-head">
                        <div>
                            <p class="eyebrow">Focused Pairing</p>
                            <h3>${escapeHtml(label)}</h3>
                        </div>
                        <div class="chemistry-matrix-record">${pairRecord ? formatRecord(pairRecord) : "?"}</div>
                    </div>
                    <div class="chemistry-matrix-grid chemistry-pair-summary-grid">
                        <div class="chemistry-matrix-cell">
                            <span class="metric-label">Best Combo</span>
                            <strong>${escapeHtml(bestCombo?.label || "?")}</strong>
                            ${bestCombo ? `<span class="chemistry-row-sub">${escapeHtml(formatRecord(bestCombo))}</span>` : `<span class="chemistry-row-sub">Need more pair games</span>`}
                        </div>
                        <div class="chemistry-matrix-cell">
                            <span class="metric-label">Most Played</span>
                            <strong>${escapeHtml(mostPlayed?.label || "?")}</strong>
                            ${mostPlayed ? `<span class="chemistry-row-sub">${mostPlayed.games} games | ${formatRecord(mostPlayed)}</span>` : `<span class="chemistry-row-sub">No stored pair history yet</span>`}
                        </div>
                        <div class="chemistry-matrix-cell chemistry-matrix-cell-muted">
                            <span class="metric-label">Watchlist</span>
                            <strong>${escapeHtml(watchCombo?.label || "?")}</strong>
                            ${watchCombo ? `<span class="chemistry-row-sub">${escapeHtml(formatRecord(watchCombo))}</span>` : `<span class="chemistry-row-sub">No weak spot yet</span>`}
                        </div>
                    </div>
                </article>
                <div class="chemistry-pair-detail-grid">
                    <article class="chemistry-matrix-card panel">
                        <span class="metric-label">Winning God Combos</span>
                        <div class="chemistry-mini-stack">
                            ${renderChemistryRows(winningCombos, {
                                emptyText: "No winning pairings yet",
                                main: (record) => escapeHtml(record.label),
                                sub: (record) => escapeHtml(formatParticipantAssignments(record.participantGods || {}, record.members || [])),
                                value: (record) => formatRecord(record),
                            })}
                        </div>
                    </article>
                    <article class="chemistry-matrix-card panel chemistry-matrix-section-muted">
                        <span class="metric-label">Losing God Combos</span>
                        <div class="chemistry-mini-stack">
                            ${renderChemistryRows(shakyCombos, {
                                emptyText: "No losing pairings yet",
                                main: (record) => escapeHtml(record.label),
                                sub: (record) => escapeHtml(formatParticipantAssignments(record.participantGods || {}, record.members || [])),
                                value: (record) => formatRecord(record),
                            })}
                        </div>
                    </article>
                    <article class="chemistry-matrix-card panel">
                        <span class="metric-label">Reliable Comfort Picks</span>
                        <div class="chemistry-chip-row chemistry-chip-row-tight">
                            ${renderComfortChips(comfortPicks, "neutral", "No reliable pair comfort yet")}
                        </div>
                    </article>
                    <article class="chemistry-matrix-card panel">
                        <span class="metric-label">High-Signal Swings</span>
                        <div class="chemistry-mini-stack">
                            ${renderChemistryRows(swingCombos, {
                                emptyText: "Need repeat games to identify swing picks",
                                main: (record) => escapeHtml(record.label),
                                sub: (record) => escapeHtml(`${record.games} games | ${formatParticipantAssignments(record.participantGods || {}, record.members || [])}`),
                                value: (record) => formatRecord(record),
                            })}
                        </div>
                    </article>
                </div>
            </section>
        `;
    }).join("");

    return `
        <section class="panel chemistry-section-card chemistry-pairing-section" style="margin-top:16px;">
            <div class="panel-heading">
                <p class="eyebrow">God Synergy Matrix</p>
                <h2>Pair Strengths and Weaknesses</h2>
            </div>
            <div class="subtab-shell chemistry-pair-subtab-shell">
                <div class="subtab-bar chemistry-pair-tabs" role="tablist" aria-label="Pairing focus">
                    ${pairTabs.map((tab) => `<button class="subtab-btn ${state.chemistry.pairSection === tab.key ? "active" : ""}" type="button" data-chemistry-pair="${escapeHtml(tab.key)}" role="tab" aria-selected="${state.chemistry.pairSection === tab.key ? "true" : "false"}">${escapeHtml(tab.label)}</button>`).join("")}
                </div>
                <div class="subtab-content">
                    ${pairCards}
                </div>
            </div>
        </section>
    `;
}

function renderChemistryReceiptsTimeline(insights, isMobile) {
    const sessions = duoSessionsFromInsights(insights)
        .slice()
        .sort((left, right) => new Date(right.startedAt) - new Date(left.startedAt))
        .slice(0, isMobile ? 6 : 8);
    return `
        <section class="panel chemistry-section-card" style="margin-top:16px;">
            <div class="panel-heading">
                <p class="eyebrow">Receipts Timeline</p>
                <h2>Recent Stack History</h2>
            </div>
            <div class="chemistry-timeline">
                ${sessions.length ? sessions.map((session) => `
                    <article class="chemistry-timeline-item chemistry-timeline-${session.won ? "win" : "loss"}">
                        <div class="chemistry-timeline-rail">
                            <span class="chemistry-timeline-dot"></span>
                        </div>
                        <div class="chemistry-timeline-card">
                            <div class="chemistry-timeline-topline">
                                <span class="chemistry-timeline-result ${session.won ? "movement-up" : "movement-down"}">${session.won ? "Win" : "Loss"}</span>
                                <span class="chemistry-timeline-queue">${escapeHtml(session.queueType || "Unknown Queue")}</span>
                                <span class="chemistry-timeline-date">${escapeHtml(formatDateTime(session.startedAt))}</span>
                            </div>
                            <h3>${escapeHtml(formatParticipantAssignments(session.participantGods || {}, session.participants || []))}</h3>
                            <div class="chemistry-chip-row">
                                ${renderChemistryChips((session.participants || []).map((member) => `${member}`), session.won ? "gold" : "neutral")}
                            </div>
                            <div class="chemistry-timeline-meta">
                                <span>KDA ${escapeHtml(session.kda || "?")}</span>
                                <span>${(session.participants || []).length >= 3 ? "Trio session" : "Duo session"}</span>
                            </div>
                        </div>
                    </article>
                `).join("") : `<div class="rank-meta">No recent duo/trio receipts yet.</div>`}
            </div>
        </section>
    `;
}

// This helper formats short relative-style save feedback for the ranker.
function formatSavedLabel(value) {
    if (!value) return "Not saved yet";
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return "Saved";
    return `Saved ${parsed.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" })}`;
}

// This helper inspects the loaded rater profiles and summarizes whether the tab
// is currently reading from durable Supabase history or a live fallback sample.
function raterStatsSourceSummary() {
    const profiles = Object.values(state.raterStats.profiles || {});
    const sourced = profiles.filter((profile) => profile?.linked);
    const supabaseCount = sourced.filter((profile) => profile?.historySource === "supabase").length;
    const liveCount = sourced.filter((profile) => profile?.historySource === "live-sample").length;
    return {
        total: sourced.length,
        supabaseCount,
        liveCount,
        usingSupabase: supabaseCount > 0 && liveCount === 0,
        mixed: supabaseCount > 0 && liveCount > 0,
    };
}

const RANKER_DRAFT_TTL_MS = 1000 * 60 * 60 * 48;

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

// This helper clears a player's local draft after a successful save or an
// explicit discard action.
function clearRankerDraft(player) {
    localStorage.removeItem(rankerDraftKey(player));
    delete state.ranker.draftMetaByPlayer[player];
}

// This helper checks whether an autosaved ranker draft is still safe to restore.
// Old mobile drafts were the source of stale ratings, so anything older than
// 48 hours gets discarded instead of overriding Supabase.
function usableRankerDraft(player) {
    const draftRaw = localStorage.getItem(rankerDraftKey(player));
    if (!draftRaw) return null;

    try {
        const draft = JSON.parse(draftRaw);
        const savedAt = new Date(draft?.savedAt || 0);
        const ageMs = Date.now() - savedAt.getTime();
        if (!draft?.ratings || !draft?.order || Number.isNaN(savedAt.getTime()) || ageMs > RANKER_DRAFT_TTL_MS) {
            clearRankerDraft(player);
            return null;
        }
        return { ...draft, ageMs };
    } catch (error) {
        clearRankerDraft(player);
        return null;
    }
}

// This helper restores the selected player to the latest Supabase-backed state
// and discards any local draft that was shadowing it.
function discardRankerDraft(player) {
    const serverState = state.ranker.serverStateByPlayer[player];
    if (!serverState) return;
    state.ranker.byPlayer[player] = {
        ratings: { ...serverState.ratings },
        order: [...serverState.order],
    };
    clearRankerDraft(player);
    refreshDirtyState(player);
    renderRankerTab();
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

// This helper returns one council member's average submitted score so the UI
// can compare who tends to score most generously or strictly.
function averagePlayerScore(player) {
    const scores = state.gods
        .map((god) => god[player])
        .filter((value) => Number.isFinite(value) && value > 0);
    if (!scores.length) return 0;
    return Math.round(scores.reduce((sum, value) => sum + value, 0) / scores.length);
}

// This helper computes a simple average for numeric arrays used throughout the
// deeper analytics modules.
function average(values) {
    if (!values.length) return 0;
    return values.reduce((sum, value) => sum + value, 0) / values.length;
}

// This helper measures spread around an average so we can label players as
// more steady or more swingy in their scoring habits.
function standardDeviation(values) {
    if (values.length < 2) return 0;
    const avg = average(values);
    const variance = average(values.map((value) => (value - avg) ** 2));
    return Math.sqrt(variance);
}

// This helper finds the player's most-loved category by average score while
// still requiring enough samples to avoid one-off outliers dominating.
function favoriteDimensionForPlayer(player, key) {
    const bucket = new Map();

    state.gods.forEach((god) => {
        const score = god[player];
        const label = god[key];
        if (!Number.isFinite(score) || score <= 0 || !label) return;
        if (!bucket.has(label)) {
            bucket.set(label, []);
        }
        bucket.get(label).push(score);
    });

    const ranked = [...bucket.entries()]
        .map(([label, scores]) => ({
            label,
            count: scores.length,
            average: scores.reduce((sum, value) => sum + value, 0) / scores.length,
        }))
        .filter((entry) => entry.count >= 2)
        .sort((a, b) => b.average - a.average || b.count - a.count || a.label.localeCompare(b.label));

    return ranked[0] || null;
}

// This helper finds the strongest player bias for a category by comparing
// personal averages against council consensus on the same set of gods.
function strongestBiasForPlayer(player, key) {
    const bucket = new Map();

    state.gods.forEach((god) => {
        const playerScore = god[player];
        const label = god[key];
        if (!Number.isFinite(playerScore) || playerScore <= 0 || !label || !Number.isFinite(god.Rating) || god.Rating <= 0) return;
        if (!bucket.has(label)) {
            bucket.set(label, []);
        }
        bucket.get(label).push(playerScore - god.Rating);
    });

    const ranked = [...bucket.entries()]
        .map(([label, deltas]) => ({
            label,
            count: deltas.length,
            delta: average(deltas),
        }))
        .filter((entry) => entry.count >= 2)
        .sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta) || b.count - a.count);

    return ranked[0] || null;
}

// This helper assigns a simple council archetype based on generosity,
// steadiness, and overlap with consensus.
function playerArchetype(player) {
    const ratedGods = state.gods.filter((god) => Number.isFinite(god[player]) && god[player] > 0 && Number.isFinite(god.Rating) && god.Rating > 0);
    const scores = ratedGods.map((god) => god[player]);
    const deltas = ratedGods.map((god) => god[player] - god.Rating);
    const avgScore = averagePlayerScore(player);
    const volatility = standardDeviation(scores);
    const avgDelta = average(deltas);

    let title = "Balanced Evaluator";
    let note = "Usually tracks close to the room without many dramatic swings.";

    if (avgScore >= 78 && avgDelta > 4) {
        title = "Sunlight Enthusiast";
        note = "Hands out optimistic scores and sees upside faster than the rest of the council.";
    } else if (avgScore <= 68 && avgDelta < -4) {
        title = "Gatekeeper";
        note = "Makes gods earn every point and rarely gives away easy praise.";
    } else if (volatility >= 18) {
        title = "Chaos Theorist";
        note = "Owns the widest swing range and is comfortable with dramatic highs and lows.";
    } else if (Math.abs(avgDelta) <= 2 && volatility <= 12) {
        title = "Consensus Anchor";
        note = "Acts like the room's stabilizer with very little drift from council average.";
    }

    return {
        player,
        avgScore: Math.round(avgScore),
        avgDelta: Math.round(avgDelta * 10) / 10,
        volatility: Math.round(volatility * 10) / 10,
        title,
        note,
    };
}

// This helper summarizes recent volatility from the history feed so analytics
// can call out which gods are moving around most often.
function buildVolatilityLeaders() {
    const bucket = new Map();

    state.recentHistory
        .filter((row) => (row.change_type || "rating") === "rating")
        .forEach((row) => {
            const godName = row.god_name;
            if (!godName) return;
            const oldValue = Number(row.old_value || 0);
            const newValue = Number(row.new_value || 0);
            const delta = Math.abs(newValue - oldValue);
            if (!bucket.has(godName)) {
                bucket.set(godName, []);
            }
            bucket.get(godName).push(delta);
        });

    return [...bucket.entries()]
        .map(([godName, deltas]) => ({
            godName,
            touches: deltas.length,
            swing: Math.round(average(deltas) * 10) / 10,
        }))
        .sort((a, b) => b.touches - a.touches || b.swing - a.swing || a.godName.localeCompare(b.godName))
        .slice(0, 5);
}

// This helper explains the current ownership picture for the selected god:
// biggest believer, biggest skeptic, and whether the room broadly agrees.
function buildOwnershipSnapshot(godName) {
    const god = state.gods.find((entry) => entry.God === godName);
    if (!god) return null;

    const scored = state.config.players
        .map((player) => {
            const profile = buildRaterProfile(player);
            const godStat = profile.godStats?.[godName] || (profile.topGods || []).find((row) => row.name === godName) || null;
            const games = Number(godStat?.gamesPlayed || 0);
            const wins = Number(godStat?.wins || 0);
            return {
                player,
                score: god[player],
                delta: Number.isFinite(god[player]) ? god[player] - god.Rating : null,
                games,
                wins,
                winRate: godStat?.winRate,
            };
        })
        .filter((entry) => Number.isFinite(entry.score) && entry.score > 0);

    if (!scored.length) {
        return { god, owner: null, skeptic: null, spread: 0, coverage: 0 };
    }

    const owner = [...scored].sort((a, b) => b.delta - a.delta || b.score - a.score)[0];
    const skeptic = [...scored].sort((a, b) => a.delta - b.delta || a.score - b.score)[0];

    return {
        god,
        owner,
        skeptic,
        spread: controversyScore(god),
        coverage: scored.length,
    };
}

// This helper computes the role where the selected head-to-head pairing is most
// lopsided, which gives the tab a stronger narrative summary.
function strongestRoleLean(rows) {
    const buckets = new Map();

    rows.forEach((god) => {
        if (!god.Role) return;
        if (!buckets.has(god.Role)) {
            buckets.set(god.Role, []);
        }
        buckets.get(god.Role).push(god.diff);
    });

    const ranked = [...buckets.entries()]
        .map(([role, diffs]) => ({
            role,
            count: diffs.length,
            averageDiff: diffs.reduce((sum, value) => sum + value, 0) / diffs.length,
        }))
        .filter((entry) => entry.count >= 2)
        .sort((a, b) => Math.abs(b.averageDiff) - Math.abs(a.averageDiff));

    return ranked[0] || null;
}

// This helper summarizes how many gods sit in each tier bucket for the
// analytics overview and compact visual distribution cards.
function buildTierDistribution(gods = state.gods) {
    return state.config.tierOrder.map((tier) => ({
        tier,
        count: gods.filter((god) => god.Tier === tier).length,
    }));
}

// This helper produces the headline pulse cards shown in the hero so the app
// opens with stronger narrative takeaways than raw counts alone.
function buildCouncilPulse() {
    const pantheonRows = [...new Set(state.gods.map((god) => god.Pantheon).filter(Boolean))]
        .map((pantheon) => {
            const gods = state.gods.filter((god) => god.Pantheon === pantheon && god.Rating > 0);
            const average = gods.length ? Math.round(gods.reduce((sum, god) => sum + god.Rating, 0) / gods.length) : 0;
            return { pantheon, average, count: gods.length };
        })
        .filter((entry) => entry.count >= 2)
        .sort((a, b) => b.average - a.average || b.count - a.count);

    const mostContested = [...state.gods]
        .filter((god) => coverageCount(god) >= 2)
        .sort((a, b) => controversyScore(b) - controversyScore(a))
        .at(0);

    const incomplete = [...state.gods]
        .filter((god) => coverageCount(god) < state.config.players.length)
        .sort((a, b) => coverageCount(a) - coverageCount(b) || b.Rating - a.Rating)
        .at(0);

    const playerAverages = state.config.players
        .map((player) => ({ player, average: averagePlayerScore(player) }))
        .sort((a, b) => b.average - a.average);

    return {
        topPantheon: pantheonRows[0] || null,
        mostContested: mostContested || null,
        watchlist: incomplete || null,
        generous: playerAverages[0] || null,
        strict: playerAverages.at(-1) || null,
    };
}

// This helper turns tier counts into a compact row of progress bars for the
// analytics tab without requiring a charting library.
function renderTierDistributionBars(gods = state.gods) {
    const total = gods.length || 1;
    const rows = buildTierDistribution(gods).filter((entry) => entry.count > 0);

    return `
        <div class="distribution-list">
            ${rows.map((entry) => `
                <div class="distribution-row">
                    <div class="distribution-label-row">
                        <span class="legend-chip">
                            <span class="legend-dot" style="background:${tierColor(entry.tier)}"></span>
                            ${escapeHtml(entry.tier)} Tier
                        </span>
                        <strong>${entry.count}</strong>
                    </div>
                    <div class="distribution-track">
                        <span class="distribution-fill" style="width:${Math.max((entry.count / total) * 100, 6)}%;background:${tierColor(entry.tier)}"></span>
                    </div>
                </div>
            `).join("")}
        </div>
    `;
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
    elements.tabItems = document.getElementById("tab-items");
    elements.tabRankings = document.getElementById("tab-rankings");
    elements.tabFavorites = document.getElementById("tab-favorites");
    elements.tabTierlist = document.getElementById("tab-tierlist");
    elements.tabAnalytics = document.getElementById("tab-analytics");
    elements.tabRaterStats = document.getElementById("tab-rater-stats");
    elements.tabChemistry = document.getElementById("tab-chemistry");
    elements.tabH2h = document.getElementById("tab-h2h");
    elements.tabCouncilScroll = document.getElementById("tab-council-scroll");
    elements.tabDataHealth = document.getElementById("tab-data-health");
    elements.tabActivity = document.getElementById("tab-activity");
    elements.tabRanker = document.getElementById("tab-ranker");
    elements.godModalBackdrop = document.getElementById("god-modal-backdrop");
    elements.godModalContent = document.getElementById("god-modal-content");
    elements.godModalClose = document.getElementById("god-modal-close");
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
            maybeLoadHeavyTabData();
        });
    });

    elements.filtersForm.addEventListener("submit", (event) => {
        event.preventDefault();
        syncFiltersFromInputs();
        renderAll();
    });

    elements.filterSearch?.addEventListener("input", () => {
        syncFiltersFromInputs();
        applyFilters();
        renderIndexTab();
        renderTabs();
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
        if (trigger) {
            window.scrollTo({ top: 0, behavior: "smooth" });
            return;
        }

        const godTrigger = event.target.closest("[data-god-detail]");
        if (godTrigger) {
            openGodDetail(godTrigger.dataset.godDetail || "");
            return;
        }

        const analyticsSectionTrigger = event.target.closest("[data-analytics-section]");
        if (analyticsSectionTrigger) {
            state.analytics.section = analyticsSectionTrigger.dataset.analyticsSection || "overview";
            renderAnalyticsTab();
            return;
        }

        const raterPlayerTrigger = event.target.closest("[data-rater-player]");
        if (raterPlayerTrigger) {
            state.raterStats.selectedPlayer = raterPlayerTrigger.dataset.raterPlayer || state.config.players[0];
            renderRaterStatsTab();
            return;
        }

        const raterSectionTrigger = event.target.closest("[data-rater-section]");
        if (raterSectionTrigger) {
            state.raterStats.section = raterSectionTrigger.dataset.raterSection || "profile";
            renderRaterStatsTab();
            return;
        }

        const raterGodSortTrigger = event.target.closest("[data-rater-god-sort]");
        if (raterGodSortTrigger) {
            state.raterStats.godSort = raterGodSortTrigger.dataset.raterGodSort || "played";
            renderRaterStatsTab();
            return;
        }

        const h2hModeTrigger = event.target.closest("[data-h2h-mode]");
        if (h2hModeTrigger) {
            state.h2h.mode = h2hModeTrigger.dataset.h2hMode || "performance";
            renderH2hTab();
            return;
        }

        const scrollSectionTrigger = event.target.closest("[data-scroll-section]");
        if (scrollSectionTrigger) {
            state.councilScroll.section = scrollSectionTrigger.dataset.scrollSection || "players";
            renderCouncilScrollTab();
            return;
        }

        const chemistrySectionTrigger = event.target.closest("[data-chemistry-section]");
        if (chemistrySectionTrigger) {
            state.chemistry.section = chemistrySectionTrigger.dataset.chemistrySection || "trinity";
            renderChemistryTab();
            return;
        }

        const chemistryPairTrigger = event.target.closest("[data-chemistry-pair]");
        if (chemistryPairTrigger) {
            state.chemistry.pairSection = chemistryPairTrigger.dataset.chemistryPair || "overview";
            renderChemistryTab();
            return;
        }

        const rankerSectionTrigger = event.target.closest("[data-ranker-section]");
        if (rankerSectionTrigger) {
            state.ranker.section = rankerSectionTrigger.dataset.rankerSection || "editor";
            renderRankerTab();
            return;
        }

        const healthRefreshTrigger = event.target.closest("[data-refresh-health]");
        if (healthRefreshTrigger) {
            loadDataHealth().then(() => renderDataHealthTab());
            return;
        }

        const councilEmailTrigger = event.target.closest("[data-send-council-scroll-email]");
        if (councilEmailTrigger) {
            sendCouncilScrollEmail();
            return;
        }

        const syncTrigger = event.target.closest("[data-sync-rater-stats]");
        if (syncTrigger) {
            syncRaterStats(syncTrigger.dataset.syncRaterStats === "all" ? "" : syncTrigger.dataset.syncRaterStats);
        }

        const discardDraftTrigger = event.target.closest("[data-discard-ranker-draft]");
        if (discardDraftTrigger) {
            discardRankerDraft(discardDraftTrigger.dataset.discardRankerDraft || state.ranker.selectedPlayer);
            return;
        }

    });

    if (elements.godModalClose) {
        elements.godModalClose.addEventListener("click", closeGodDetail);
    }
    if (elements.godModalBackdrop) {
        elements.godModalBackdrop.addEventListener("click", (event) => {
            if (event.target === elements.godModalBackdrop) closeGodDetail();
        });
    }

    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && elements.godModalBackdrop && !elements.godModalBackdrop.classList.contains("hidden")) {
            closeGodDetail();
            return;
        }
        if (event.key === "Enter") {
            const godTrigger = event.target.closest?.("[data-god-detail]");
            if (godTrigger) {
                openGodDetail(godTrigger.dataset.godDetail || "");
                return;
            }
        }
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

        const serverState = {
            ratings,
            order: [...ordered, ...ratedButMissing],
        };
        const baseState = {
            ratings: { ...serverState.ratings },
            order: [...serverState.order],
        };
        const draft = usableRankerDraft(player);
        if (draft) {
            baseState.ratings = { ...baseState.ratings, ...draft.ratings };
            baseState.order = Array.isArray(draft.order) ? draft.order : baseState.order;
            state.ranker.draftMetaByPlayer[player] = { savedAt: draft.savedAt, ageMs: draft.ageMs };
        }

        state.ranker.serverStateByPlayer[player] = {
            ratings: { ...serverState.ratings },
            order: [...serverState.order],
        };
        state.ranker.byPlayer[player] = baseState;
        state.ranker.baselineByPlayer[player] = buildRankerSignature(serverState);
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
    state.itemMetadata = payload.itemMetadata || [];
    state.itemTaxonomy = payload.itemTaxonomy || {};
    state.analytics.god = payload.gods[0]?.God || "";
    state.analytics.players = [...payload.config.players];
    initializeRankerState();
    renderFilterOptions();
}


// This helper fetches the read-only Supabase health report used by the admin
// panel and keeps a friendly error in state if any table cannot be inspected.
async function loadDataHealth() {
    state.dataHealth.loading = true;
    try {
        const payload = await api("/api/data-health");
        state.dataHealth.report = payload;
        state.dataHealth.error = "";
    } catch (error) {
        state.dataHealth.report = null;
        state.dataHealth.error = error.message || "Data Health is unavailable right now.";
    } finally {
        state.dataHealth.loading = false;
        state.dataHealth.loaded = true;
    }
}

// This helper clears older cached match summaries. Those summaries can become
// wrong whenever importer/dedupe logic changes, so live API data is safer.
function clearRaterStatsCache() {
    try {
        localStorage.removeItem("highCouncilRaterStatsCache");
    } catch (error) {
        // Storage can fail in private browsing; the live API remains authoritative.
    }
}

function persistRaterStatsCache() {
    clearRaterStatsCache();
}

// This helper fetches the live SmiteSource-backed profile data used by the
// Rater Stats tab while keeping the rest of the app responsive if it fails.
async function loadRaterStats({ force = false } = {}) {
    if (state.raterStats.loading) return;
    if (state.raterStats.loaded && !force) return;
    state.raterStats.loading = true;
    try {
        const payload = await api("/api/rater-stats");
        state.raterStats.profiles = payload.profiles || payload || {};
        state.raterStats.error = "";
        state.raterStats.cacheHydrated = false;
        persistRaterStatsCache();
    } catch (error) {
        if (!state.raterStats.loaded) {
            state.raterStats.profiles = {};
        }
        state.raterStats.error = error.message || "Rater stats are unavailable right now.";
    } finally {
        state.raterStats.loaded = true;
        state.raterStats.loading = false;
    }
    if (state.godDetail.god && elements.godModalBackdrop && !elements.godModalBackdrop.classList.contains("hidden")) {
        openGodDetail(state.godDetail.god);
    }
}

function maybeLoadHeavyTabData() {
    const heavyTabs = new Set(["items", "rater-stats", "chemistry", "council-scroll"]);
    if (!heavyTabs.has(state.activeTab) || state.raterStats.loaded || state.raterStats.loading) return;
    loadRaterStats().then(() => renderAll());
}


// This helper sends a protected Joey-only recap through the Flask/Resend route
// so the free Resend test sender can still be useful without domain verification.
async function sendCouncilScrollEmail() {
    const savedKey = sessionStorage.getItem("highCouncilRecapKey") || sessionStorage.getItem("highCouncilSyncKey") || "";
    const recapKey = window.prompt("Enter your recap email key", savedKey);
    if (!recapKey) return;

    sessionStorage.setItem("highCouncilRecapKey", recapKey);
    state.councilScroll.emailing = true;
    state.councilScroll.emailMessage = "Preparing Joey-only recap...";
    renderCouncilScrollTab();

    try {
        const payload = await api("/api/council-scroll/email", {
            method: "POST",
            body: JSON.stringify({ recapKey, test: true }),
        });
        state.councilScroll.emailMessage = `${payload.message || "Joey recap sent."} Forward it to the council from Gmail.`;
    } catch (error) {
        state.councilScroll.emailMessage = error.message || "Email recap failed.";
    } finally {
        state.councilScroll.emailing = false;
        renderCouncilScrollTab();
    }
}

// This helper triggers the protected backend history sync and then refreshes
// the in-memory rater stats so the UI immediately reflects stored data.
async function syncRaterStats(player = "") {
    const savedKey = sessionStorage.getItem("highCouncilSyncKey") || "";
    const syncKey = window.prompt("Enter your SmiteSource sync key", savedKey);
    if (!syncKey) return;

    sessionStorage.setItem("highCouncilSyncKey", syncKey);
    state.raterStats.syncing = true;
    state.raterStats.syncMessage = player ? `Syncing ${player}...` : "Syncing all linked profiles...";
    renderAll();

    try {
        const payload = await api("/api/rater-stats/sync", {
            method: "POST",
            body: JSON.stringify({
                syncKey,
                ...(player ? { player } : {}),
            }),
        });
        await loadRaterStats({ force: true });
        const inserted = (payload.results || []).reduce((sum, row) => sum + Number(row.inserted || 0), 0);
        const stored = (payload.results || []).reduce((sum, row) => sum + Number(row.stored || 0), 0);
        if (payload.ok === false) {
            state.raterStats.syncMessage = payload.message || `Sync could not reach SmiteSource. Existing Supabase history still has ${stored} stored rows.`;
        } else {
            state.raterStats.syncMessage = `Sync complete: ${inserted} new matches, ${stored} stored rows across ${(payload.results || []).length} profile(s).`;
        }
    } catch (error) {
        state.raterStats.syncMessage = error.message || "Sync failed.";
    } finally {
        state.raterStats.syncing = false;
        renderAll();
    }
}

// This helper uploads a browser-exported SmiteSource HAR so blocked server-side
// syncs can still be turned into durable Supabase match history.
async function importSmitesourceHar() {
    const savedKey = sessionStorage.getItem("highCouncilSyncKey") || "";
    const syncKey = window.prompt("Enter your SmiteSource sync key", savedKey);
    if (!syncKey) return;

    const input = document.createElement("input");
    input.type = "file";
    input.accept = ".har,.json,application/json";
    input.addEventListener("change", async () => {
        const file = input.files?.[0];
        if (!file) return;

        sessionStorage.setItem("highCouncilSyncKey", syncKey);
        state.raterStats.syncing = true;
        state.raterStats.syncMessage = `Importing ${file.name}...`;
        renderAll();

        try {
            const formData = new FormData();
            formData.append("syncKey", syncKey);
            formData.append("file", file);
            const response = await fetch("/api/rater-stats/import-har", {
                method: "POST",
                body: formData,
            });
            const payload = await response.json().catch(() => null);
            if (!response.ok) {
                throw new Error(payload?.message || `Import failed: ${response.status}`);
            }
            await loadRaterStats({ force: true });
            state.raterStats.syncMessage = payload?.message || "HAR import complete.";
        } catch (error) {
            state.raterStats.syncMessage = error.message || "HAR import failed.";
        } finally {
            state.raterStats.syncing = false;
            renderAll();
        }
    }, { once: true });
    input.click();
}

// This helper renders the hero statistic cards.
function renderHeroStats() {
    if (!elements.heroStats) {
        return;
    }

    const cards = [
        ["Roster", state.stats.total_gods ?? 0],
        ["Average", state.stats.avg_rating ?? 0],
        ["SS Tier", state.stats.ss_count ?? 0],
        ["Council", state.config.players.length],
    ];

    elements.heroStats.innerHTML = cards
        .map(([label, value]) => `
            <div class="stat-card stat-card-compact">
                <span class="stat-label">${escapeHtml(label)}</span>
                <strong class="stat-value">${escapeHtml(value)}</strong>
            </div>
        `)
        .join("");
}

// This helper renders the backend status banner when any live reads fell back
// to snapshot data.
function renderStatusBanner() {
    if (!elements.statusBanner) return;
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
    if (!elements.podiumPanel) return;
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
    if (!elements.liveRankings) return;
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
        .map((god, index) => {
            const coverage = coverageCount(god);
            const imageLoading = index < 12 ? "eager" : "lazy";
            const imagePriority = index < 6 ? "high" : "auto";
            return `
            <article class="god-card ${coverage < state.config.players.length ? "partial-coverage" : ""}" data-god-detail="${escapeHtml(god.God)}" role="button" tabindex="0">
                <div class="god-art-wrap">
                    ${god.ImageUrl ? `<img class="god-art" src="${god.ImageUrl}" alt="${escapeHtml(god.God)}" loading="${imageLoading}" decoding="async" fetchpriority="${imagePriority}">` : `<div class="image-fallback">No Art</div>`}
                    ${god.PantheonImageUrl ? `<img class="pantheon-watermark" src="${god.PantheonImageUrl}" alt="" aria-hidden="true" loading="lazy" decoding="async">` : ""}
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
                </div>
            </article>
        `;
        })
        .join("");

    elements.tabIndex.innerHTML = `
        <div class="index-heading-row index-heading-row-compact">
            <p class="eyebrow">God Index</p>
            <span class="summary-pill">${state.filteredGods.length} shown</span>
        </div>
        ${renderFilterSummary()}
        <div class="god-grid">${cards}</div>
        ${renderBackToTop()}
    `;
}

// This helper finds stored match stats for one god across every linked rater.
function godPlayerHistory(godName) {
    return state.config.players.map((player) => {
        const profile = buildRaterProfile(player);
        const godStat = profile.godStats?.[godName] || (profile.topGods || []).find((row) => row.name === godName) || null;
        return {
            player,
            rating: state.gods.find((god) => god.God === godName)?.[player] || null,
            rank: state.allRankings?.[player]?.[godName] || null,
            games: Number(godStat?.gamesPlayed || 0),
            wins: Number(godStat?.wins || 0),
            winRate: godStat?.winRate,
            kdaRatio: godStat?.kdaRatio,
            damagePerMin: godStat?.damagePerMin,
        };
    });
}

// This helper gathers recent matches and chemistry rows involving one selected god.
function godMatchInsights(godName) {
    const recentRows = [];
    const synergyMap = new Map();

    state.config.players.forEach((player) => {
        const profile = buildRaterProfile(player);
        (profile.recentMatches || []).forEach((match) => {
            if (match.godName === godName) {
                recentRows.push({ ...match, player });
            }
        });

        ((profile.chemistry || {}).groupGodRecords || []).forEach((record) => {
            const participantGods = record.participantGods || {};
            if (!Object.values(participantGods).includes(godName)) return;
            const key = `${chemistryMembersKey(record.members || [])}|${Object.entries(participantGods).sort().map(([member, god]) => `${member}:${god}`).join("|")}`;
            if (synergyMap.has(key)) return;
            synergyMap.set(key, record);
        });
    });

    const synergies = [...synergyMap.values()]
        .sort((a, b) => Number(b.games || 0) - Number(a.games || 0) || Number(b.winRate || 0) - Number(a.winRate || 0))
        .slice(0, 5);

    return { recentRows: recentRows.slice(0, 8), synergies };
}


// This helper renders the small gold-edged stat cards used across detail views.
function dossierStat(label, value, note = "", toneClass = "") {
    return `
        <article class="god-dossier-stat ${toneClass}">
            <span>${escapeHtml(label)}</span>
            <strong>${escapeHtml(value)}</strong>
            ${note ? `<small>${escapeHtml(note)}</small>` : ""}
        </article>
    `;
}
// This helper combines each rater's server-side build receipts into one compact
// god-card summary without shipping full raw match payloads to the browser.
function godCoreItemInsights(godName, aspectFilter = "All") {
    const targetKey = canonicalGodKey(godName);
    const byPlayer = [];
    const itemMap = new Map();
    const starterMap = new Map();
    const pathMap = new Map();
    const aspectMap = new Map();
    let totalGames = 0;
    let totalWins = 0;

    state.config.players.forEach((player) => {
        const profile = buildRaterProfile(player);
        const buildStats = profile.buildStats || {};
        const statKey = Object.keys(buildStats).find((key) => canonicalGodKey(key) === targetKey);
        const stats = statKey ? buildStats[statKey] : null;
        if (!stats || !Number(stats.games || 0)) return;

        (stats.aspects || []).forEach((aspect) => {
            const key = aspect.name || "No Aspect";
            const record = aspectMap.get(key) || { name: key, games: 0, wins: 0 };
            record.games += Number(aspect.games || 0);
            record.wins += Number(aspect.wins || 0);
            aspectMap.set(key, record);
        });

        const selectedStats = aspectFilter && aspectFilter !== "All" ? (stats.aspectStats || {})[aspectFilter] : stats;
        if (!selectedStats || !Number(selectedStats.games || 0)) return;

        const games = Number(selectedStats.games || 0);
        const wins = Number(selectedStats.wins || 0);
        totalGames += games;
        totalWins += wins;
        byPlayer.push({ player, ...selectedStats, games, wins });

        (selectedStats.starterItems || []).forEach((starter) => {
            const key = starter.name || starter.label || "Unknown Starter";
            const record = starterMap.get(key) || { name: key, category: "Starter", games: 0, wins: 0 };
            record.games += Number(starter.games || 0);
            record.wins += Number(starter.wins || 0);
            starterMap.set(key, record);
        });

        (selectedStats.topItems || []).forEach((item) => {
            const key = item.name || item.label || "Unknown Item";
            const record = itemMap.get(key) || { name: key, category: item.category || "Items", games: 0, wins: 0 };
            record.games += Number(item.games || 0);
            record.wins += Number(item.wins || 0);
            itemMap.set(key, record);
        });

        (selectedStats.corePaths || []).forEach((path) => {
            const key = (path.items || []).join("|") || path.label || "Core path";
            const record = pathMap.get(key) || { label: path.label || (path.items || []).join(" -> "), items: path.items || [], games: 0, wins: 0 };
            record.games += Number(path.games || 0);
            record.wins += Number(path.wins || 0);
            pathMap.set(key, record);
        });
    });

    const finish = (record) => {
        const games = Number(record.games || 0);
        const wins = Number(record.wins || 0);
        return {
            ...record,
            games,
            wins,
            pickRate: totalGames ? games / totalGames * 100 : 0,
            winRate: games ? wins / games * 100 : 0,
        };
    };

    const aspects = [...aspectMap.values()].map((aspect) => {
        const games = Number(aspect.games || 0);
        const wins = Number(aspect.wins || 0);
        return {
            ...aspect,
            games,
            wins,
            pickRate: games ? games / Math.max([...aspectMap.values()].reduce((sum, row) => sum + Number(row.games || 0), 0), 1) * 100 : 0,
            winRate: games ? wins / games * 100 : 0,
        };
    }).sort((a, b) => b.games - a.games || a.name.localeCompare(b.name));

    return {
        aspectFilter,
        aspects,
        totalGames,
        totalWins,
        winRate: totalGames ? totalWins / totalGames * 100 : 0,
        starterItems: [...starterMap.values()].map(finish).sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.name.localeCompare(b.name)).slice(0, 6),
        topItems: [...itemMap.values()].map(finish).sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.name.localeCompare(b.name)).slice(0, 8),
        corePaths: [...pathMap.values()].map(finish).sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.label.localeCompare(b.label)).slice(0, 5),
        byPlayer: byPlayer.sort((a, b) => Number(b.games || 0) - Number(a.games || 0) || b.wins - a.wins),
    };
}


// This helper unlocks one council member from inside a god detail modal.
async function unlockGodQuickEdit(godName) {
    const player = state.godDetail.editPlayer || state.config.players[0];
    const pin = document.getElementById("god-edit-pin")?.value || "";
    if (!pin) {
        alert("Enter the PIN first.");
        return;
    }
    try {
        await api("/api/unlock", {
            method: "POST",
            body: JSON.stringify({ player, pin }),
        });
        state.ranker.unlocked[player] = true;
        openGodDetail(godName);
    } catch (error) {
        alert(error.message);
    }
}

// This helper saves a single god rating from the modal while preserving the
// same backend save path and auto-sort behavior used by the full Rate & Rank tab.
async function saveGodQuickEdit(godName) {
    const player = state.godDetail.editPlayer || state.config.players[0];
    if (!state.ranker.unlocked[player]) {
        alert("Unlock this rater first.");
        return;
    }

    const playerState = state.ranker.byPlayer[player];
    const rating = Math.max(0, Math.min(100, Number(document.getElementById("god-edit-rating")?.value || 0)));

    playerState.ratings[godName] = rating;
    if (rating === 0) {
        playerState.order = playerState.order.filter((name) => name !== godName);
    } else if (!playerState.order.includes(godName)) {
        playerState.order.push(godName);
    }
    fullResort(playerState);

    persistRankerDraft(player);
    refreshDirtyState(player);

    const sectionAfterSave = state.godDetail.section || "edit";

    try {
        const payload = await api("/api/save-rankings", {
            method: "POST",
            body: JSON.stringify({
                player,
                ratings: playerState.ratings,
                order: playerState.order,
            }),
        });
        state.ranker.lastSavedByPlayer[player] = new Date().toISOString();
        state.ranker.baselineByPlayer[player] = buildRankerSignature(playerState);
        clearRankerDraft(player);
        refreshDirtyState(player);
        await refreshData();
        state.godDetail.section = sectionAfterSave;
        openGodDetail(godName);
        alert(payload.message || "Saved.");
    } catch (error) {
        alert(error.message);
    }
}

// This helper opens the God Index deep-dive modal with council and match data.
function openGodDetail(godName) {
    const god = state.gods.find((item) => item.God === godName);
    if (!god || !elements.godModalBackdrop || !elements.godModalContent) return;

    if (state.godDetail.god !== godName) {
        state.godDetail.god = godName;
        state.godDetail.section = "council";
        state.godDetail.buildAspect = "All";
    }
    if (!state.config.players.includes(state.godDetail.editPlayer)) {
        state.godDetail.editPlayer = state.config.players[0] || "Joey";
    }

    const historyRows = godPlayerHistory(godName);
    const playedRows = historyRows.filter((row) => row.games > 0);
    const bestPlayer = [...playedRows].sort((a, b) => Number(b.winRate || 0) - Number(a.winRate || 0) || b.games - a.games)[0] || null;
    const { recentRows, synergies } = godMatchInsights(godName);
    const selectedBuildAspect = state.godDetail.buildAspect || "All";
    const buildInsights = godCoreItemInsights(godName, selectedBuildAspect);
    const ownership = buildOwnershipSnapshot(godName);
    const coverage = coverageCount(god);
    const split = controversyScore(god);
    const topPerformance = [...playedRows].sort((a, b) => Number(b.winRate || 0) - Number(a.winRate || 0) || b.wins - a.wins || b.games - a.games)[0] || null;
    const mostPlayedRow = [...playedRows].sort((a, b) => b.games - a.games || Number(b.winRate || 0) - Number(a.winRate || 0))[0] || null;
    const sections = [
        { key: "council", label: "Council" },
        { key: "ownership", label: "Ownership" },
        { key: "performance", label: "Performance" },
        { key: "synergy", label: "Synergy" },
        { key: "recent", label: "Recent" },
        { key: "builds", label: "Aspects & Items" },
        { key: "edit", label: "Rate & Rank" },
    ];
    if (!sections.some((section) => section.key === state.godDetail.section)) {
        state.godDetail.section = "council";
    }

    const ratingOwnershipLine = (label, person, toneClass = "") => {
        if (!person) return "";
        const deltaText = Number(person.delta || 0) > 0 ? `+${person.delta} vs council` : `${person.delta} vs council`;
        return `<div class="mini-row-v2 ownership-row"><span><strong>${escapeHtml(label)}: ${escapeHtml(person.player)}</strong><small>${escapeHtml(deltaText)}</small></span><b class="${toneClass}" style="color:${playerColor(person.player)}">${person.score}</b></div>`;
    };

    const performanceOwnershipLine = (label, person, value) => {
        if (!person) return `<div class="rank-meta">No stored Joust games for this god yet.</div>`;
        return `<div class="mini-row-v2 ownership-row"><span><strong>${escapeHtml(label)}: ${escapeHtml(person.player)}</strong><small>${formatWinLossRecord(person.wins, person.games)} | ${formatMetric(person.games)} games</small></span><b style="color:${playerColor(person.player)}">${escapeHtml(value)}</b></div>`;
    };

    const ratedHistoryRows = historyRows.filter((row) => Number(row.rating || 0) > 0);
    const highestRating = [...ratedHistoryRows].sort((a, b) => Number(b.rating || 0) - Number(a.rating || 0) || (a.rank || 9999) - (b.rank || 9999))[0] || null;
    const topRanked = [...ratedHistoryRows].filter((row) => row.rank).sort((a, b) => Number(a.rank || 9999) - Number(b.rank || 9999))[0] || null;
    const strongestRecord = [...playedRows].sort((a, b) => Number(b.winRate || 0) - Number(a.winRate || 0) || b.games - a.games)[0] || null;
    const recentWins = recentRows.filter((match) => match.won).length;
    const recentLosses = recentRows.filter((match) => !match.won).length;
    const bestSynergy = [...synergies].sort((a, b) => Number(b.winRate || 0) - Number(a.winRate || 0) || Number(b.games || 0) - Number(a.games || 0))[0] || null;
    const mostUsedSynergy = [...synergies].sort((a, b) => Number(b.games || 0) - Number(a.games || 0) || Number(b.winRate || 0) - Number(a.winRate || 0))[0] || null;


    const councilPlayerCards = historyRows.map((row) => `
        <article class="god-player-card">
            <div class="god-player-card-head">
                <strong style="color:${playerColor(row.player)}">${escapeHtml(row.player)}</strong>
                <span>${row.rank ? `#${row.rank}` : "Unranked"}</span>
            </div>
            <div class="god-player-score" style="color:${playerColor(row.player)}">${row.rating || "--"}</div>
            <div class="god-player-meta">
                <span>${row.games ? formatWinLossRecord(row.wins, row.games) : "No matches"}</span>
                <span>${row.games ? `${formatMetric(row.winRate, 1, "%")} WR` : "-- WR"}</span>
                <span>${row.games ? `${formatMetric(row.kdaRatio, 2)} KDA` : "-- KDA"}</span>
            </div>
        </article>
    `).join("");

    const performanceCards = playedRows.length ? playedRows
        .slice()
        .sort((a, b) => Number(b.winRate || 0) - Number(a.winRate || 0) || b.games - a.games)
        .slice(0, 8)
        .map((row) => `
            <article class="god-performance-card">
                <div>
                    <p class="eyebrow" style="color:${playerColor(row.player)}">${escapeHtml(row.player)}</p>
                    <h4>${formatWinLossRecord(row.wins, row.games)}</h4>
                </div>
                <div class="god-performance-meter">
                    <span style="width:${Math.max(2, Math.min(100, Number(row.winRate || 0)))}%"></span>
                </div>
                <div class="god-player-meta">
                    <span>${formatMetric(row.winRate, 1, "%")} WR</span>
                    <span>${formatMetric(row.games)} games</span>
                    <span>${formatMetric(row.damagePerMin)} dmg/min</span>
                </div>
            </article>
        `).join("") : `<p class="rank-meta">No stored Joust match history for this god yet.</p>`;

    const synergyCards = synergies.length ? synergies.map((record) => `
        <article class="god-synergy-card">
            <div class="god-synergy-card-main">
                <strong>${escapeHtml(record.label || "Combo")}</strong>
                <span>${escapeHtml(formatParticipantAssignments(record.participantGods || {}, record.members || []))}</span>
            </div>
            <b>${formatRecord(record)}</b>
        </article>
    `).join("") : `<p class="rank-meta">No stored council combo involving this god yet.</p>`;

    const recentCards = recentRows.length ? recentRows.map((match) => `
        <article class="god-recent-card ${match.won ? "win" : "loss"}">
            <div>
                <strong style="color:${playerColor(match.player)}">${escapeHtml(match.player)}</strong>
                <span class="rank-meta">${escapeHtml(formatDateTime(match.startedAt))}</span>
            </div>
            <div class="god-recent-result ${match.won ? "movement-up" : "movement-down"}">${match.won ? "Win" : "Loss"}</div>
            <div class="god-player-meta">
                <span>${escapeHtml(match.role || "Role")}</span>
                <span>${formatMetric(match.kills)}/${formatMetric(match.deaths)}/${formatMetric(match.assists)}</span>
                <span>${escapeHtml(match.queueType || "Queue")}</span>
            </div>
        </article>
    `).join("") : `<p class="rank-meta">No recent stored matches for this god in the current profile payload.</p>`;


    const aspectToggleRows = [`All`, ...buildInsights.aspects.map((aspect) => aspect.name)].map((aspectName) => {
        const aspect = buildInsights.aspects.find((row) => row.name === aspectName);
        const isActive = selectedBuildAspect === aspectName || (!selectedBuildAspect && aspectName === "All");
        const note = aspect ? `${formatWinLossRecord(aspect.wins, aspect.games)} | ${formatMetric(aspect.pickRate, 1, "%")}` : "All stored loadouts";
        return `<button class="aspect-toggle-btn ${isActive ? "active" : ""}" type="button" data-build-aspect="${escapeHtml(aspectName)}"><strong>${escapeHtml(aspectName)}</strong><span>${escapeHtml(note)}</span></button>`;
    }).join("");

    const starterRows = buildInsights.starterItems.length ? buildInsights.starterItems.map((starter) => `
        <article class="core-item-row starter-item-row">
            <div>
                <strong>${escapeHtml(starter.name)}</strong>
                <small>Starter pick</small>
            </div>
            <div class="core-build-result">
                <b>${formatMetric(starter.pickRate, 1, "%")}</b>
                <span>${formatWinLossRecord(starter.wins, starter.games)} | ${formatMetric(starter.winRate, 1, "%")} WR</span>
            </div>
        </article>
    `).join("") : `<p class="rank-meta">No starter item sample for this aspect/god yet.</p>`;

    const coreItemRows = buildInsights.topItems.length ? buildInsights.topItems.map((item) => `
        <article class="core-item-row">
            <div>
                <strong>${escapeHtml(item.name)}</strong>
                <small>${escapeHtml(item.category || "Items")}</small>
            </div>
            <div class="core-build-result">
                <b>${formatMetric(item.pickRate, 1, "%")}</b>
                <span>${formatWinLossRecord(item.wins, item.games)} | ${formatMetric(item.winRate, 1, "%")} WR</span>
            </div>
        </article>
    `).join("") : `<p class="rank-meta">No stored build item sample for this god yet.</p>`;

    const corePathRows = buildInsights.corePaths.length ? buildInsights.corePaths.map((path) => `
        <article class="core-path-row">
            <div class="core-path-items">${(path.items || []).map((item) => `<span>${escapeHtml(item)}</span>`).join("") || `<span>${escapeHtml(path.label || "Core path")}</span>`}</div>
            <div class="core-build-meter"><span style="width:${Math.max(3, Math.min(100, Number(path.pickRate || 0)))}%"></span></div>
            <div class="core-path-meta"><strong>${formatMetric(path.pickRate, 1, "%")} bought</strong><span>${formatWinLossRecord(path.wins, path.games)} | ${formatMetric(path.winRate, 1, "%")} WR</span></div>
        </article>
    `).join("") : `<p class="rank-meta">No repeated core paths yet. Need more stored loadout samples.</p>`;

    const playerBuildRows = buildInsights.byPlayer.length ? buildInsights.byPlayer.map((row) => {
        const favoritePath = (row.corePaths || [])[0];
        const favoriteItem = (row.topItems || [])[0];
        return `
            <article class="core-player-card">
                <div class="core-player-head"><strong style="color:${playerColor(row.player)}">${escapeHtml(row.player)}</strong><span>${formatWinLossRecord(row.wins, row.games)}</span></div>
                <p>${favoritePath ? escapeHtml(favoritePath.label || (favoritePath.items || []).join(" -> ")) : "No consistent path yet"}</p>
                <small>${favoriteItem ? `Most bought: ${escapeHtml(favoriteItem.name)} (${formatMetric(favoriteItem.pickRate, 1, "%")})` : "No item sample"}</small>
            </article>
        `;
    }).join("") : `<p class="rank-meta">No rater has stored build data for this god yet.</p>`;

    const editPlayer = state.godDetail.editPlayer;
    const editUnlocked = !!state.ranker.unlocked[editPlayer];
    const editState = state.ranker.byPlayer[editPlayer] || { ratings: {}, order: [] };
    const editRating = Number(editState.ratings?.[godName] ?? god[editPlayer] ?? 0);
    const editRank = editState.order?.includes(godName)
        ? editState.order.indexOf(godName) + 1
        : (state.allRankings?.[editPlayer]?.[godName] || "");

    const sectionHtml = {
        council: `
            <section class="god-modal-tab-panel god-dossier-panel">
                <article class="detail-card-v2 wide god-dossier-card">
                    <div class="section-head"><div><p class="eyebrow">Council</p><h3>Ratings And Personal Ranks</h3></div><span class="summary-pill">${bestPlayer ? `Best WR: ${escapeHtml(bestPlayer.player)} ${formatMetric(bestPlayer.winRate, 1, "%")}` : "No match sample"}</span></div>
                    <div class="detail-table-wrap"><table class="table compact-table"><thead><tr><th>Player</th><th>Rating</th><th>Rank</th><th>Record</th><th>WR</th><th>KDA</th></tr></thead><tbody>
                        ${historyRows.map((row) => `<tr><td><strong>${escapeHtml(row.player)}</strong></td><td>${row.rating || "--"}</td><td>${row.rank ? `#${row.rank}` : "--"}</td><td>${row.games ? formatWinLossRecord(row.wins, row.games) : "--"}</td><td class="${Number(row.winRate || 0) >= 55 ? "movement-up" : row.games ? "movement-down" : ""}">${row.games ? formatMetric(row.winRate, 1, "%") : "--"}</td><td>${row.games ? formatMetric(row.kdaRatio, 2) : "--"}</td></tr>`).join("")}
                    </tbody></table></div>
                </article>
            </section>
        `,
        ownership: `
            <section class="god-modal-tab-panel god-dossier-panel">
                <div class="god-ownership-grid">
                    <article class="detail-card-v2">
                        <p class="eyebrow">Ratings Ownership</p>
                        <h3>Council Belief</h3>
                        <div class="mini-list-v2">${ownership?.owner ? `${ratingOwnershipLine("Biggest believer", ownership.owner, "movement-up")}${ratingOwnershipLine("Most skeptical", ownership.skeptic, "movement-down")}<div class="mini-row-v2"><span><strong>Room spread</strong><small>${coverage}/${state.config.players.length} rated</small></span><b>${split} pts</b></div>` : `<p class="rank-meta">No council ownership story yet.</p>`}</div>
                    </article>
                    <article class="detail-card-v2">
                        <p class="eyebrow">Actual Performance</p>
                        <h3>Best Results</h3>
                        <div class="mini-list-v2">${performanceOwnershipLine("Best win rate", topPerformance, topPerformance ? formatMetric(topPerformance.winRate, 1, "%") : "--")}</div>
                    </article>
                    <article class="detail-card-v2 wide">
                        <p class="eyebrow">Most Played</p>
                        <h3>Who Has The Reps</h3>
                        <div class="mini-list-v2">${performanceOwnershipLine("Most played", mostPlayedRow, mostPlayedRow ? `${formatMetric(mostPlayedRow.games)} games` : "--")}</div>
                    </article>
                </div>
            </section>
        `,
        performance: `
            <section class="god-modal-tab-panel god-dossier-panel">
                <div class="god-dossier-grid">
                    ${dossierStat("Best WR", topPerformance ? `${topPerformance.player}` : "--", topPerformance ? `${formatMetric(topPerformance.winRate, 1, "%")} over ${formatMetric(topPerformance.games)} games` : "No games yet")}
                    ${dossierStat("Most Played", mostPlayedRow ? `${mostPlayedRow.player}` : "--", mostPlayedRow ? `${formatMetric(mostPlayedRow.games)} games` : "No games yet")}
                    ${dossierStat("Total Sample", `${playedRows.reduce((sum, row) => sum + Number(row.games || 0), 0)}`, "stored player-games")}
                    ${dossierStat("Council Form", playedRows.length ? `${formatMetric(average(playedRows.map((row) => Number(row.winRate || 0))), 1, "%")}` : "--", playedRows.length ? "avg WR across raters" : "No sample")}
                </div>
                <article class="detail-card-v2 wide god-dossier-card"><p class="eyebrow">Performance</p><h3>Who Actually Plays It</h3><div class="god-performance-grid">${performanceCards}</div></article>
            </section>
        `,
        synergy: `
            <section class="god-modal-tab-panel god-dossier-panel">
                <div class="god-dossier-grid">
                    ${dossierStat("Best Look", bestSynergy ? bestSynergy.label : "--", bestSynergy ? formatRecord(bestSynergy) : "No combo yet")}
                    ${dossierStat("Most Used", mostUsedSynergy ? mostUsedSynergy.label : "--", mostUsedSynergy ? `${formatMetric(mostUsedSynergy.games)} games` : "No combo yet")}
                    ${dossierStat("Combo Count", `${synergies.length}`, "stored council looks")}
                    ${dossierStat("Best Record", bestSynergy ? formatRecord(bestSynergy) : "--", bestSynergy ? escapeHtml(formatParticipantAssignments(bestSynergy.participantGods || {}, bestSynergy.members || [])) : "No assignments")}
                </div>
                <article class="detail-card-v2 wide god-dossier-card"><p class="eyebrow">Synergy</p><h3>Group Looks</h3><div class="god-synergy-grid">${synergyCards}</div></article>
            </section>
        `,
        recent: `
            <section class="god-modal-tab-panel god-dossier-panel">
                <div class="god-dossier-grid">
                    ${dossierStat("Recent Sample", `${recentRows.length}`, "stored matches")}
                    ${dossierStat("Recent Wins", `${recentWins}`, `${recentLosses} losses`)}
                    ${dossierStat("Last Played", recentRows[0] ? formatDateTime(recentRows[0].startedAt) : "--", recentRows[0] ? recentRows[0].player : "No recent match")}
                    ${dossierStat("Latest Pick", recentRows[0] ? recentRows[0].player : "--", recentRows[0] ? `${recentRows[0].won ? "Win" : "Loss"} ${formatMetric(recentRows[0].kills)}/${formatMetric(recentRows[0].deaths)}/${formatMetric(recentRows[0].assists)}` : "No match")}
                </div>
                <article class="detail-card-v2 wide god-dossier-card"><div class="section-head"><div><p class="eyebrow">Recent Games</p><h3>Latest Stored Matches</h3></div><span class="summary-pill">${recentRows.length} found</span></div><div class="god-recent-grid">${recentCards}</div></article>
            </section>
        `,
        builds: `
            <section class="god-modal-tab-panel god-dossier-panel">
                <div class="god-dossier-grid">
                    ${dossierStat("Aspect", selectedBuildAspect, selectedBuildAspect === "All" ? "all stored loadouts" : "filtered loadouts")}
                    ${dossierStat("Build Sample", `${buildInsights.totalGames}`, "stored loadouts")}
                    ${dossierStat("Build WR", buildInsights.totalGames ? formatMetric(buildInsights.winRate, 1, "%") : "--", buildInsights.totalGames ? formatWinLossRecord(buildInsights.totalWins, buildInsights.totalGames) : "No sample")}
                    ${dossierStat("Starter", buildInsights.starterItems[0]?.name || "--", buildInsights.starterItems[0] ? `${formatMetric(buildInsights.starterItems[0].pickRate, 1, "%")} of games` : "No starter sample")}
                    ${dossierStat("Most Bought", buildInsights.topItems[0]?.name || "--", buildInsights.topItems[0] ? `${formatMetric(buildInsights.topItems[0].pickRate, 1, "%")} of builds` : "No items yet")}
                </div>
                <article class="detail-card-v2 wide core-build-card aspect-filter-card"><div class="section-head"><div><p class="eyebrow">Aspect Filter</p><h3>Choose The Lens First</h3></div><span class="summary-pill">${buildInsights.aspects.length} detected</span></div><div class="aspect-toggle-row">${aspectToggleRows}</div></article>
                <div class="core-build-grid">
                    <article class="detail-card-v2 core-build-card"><div class="section-head"><div><p class="eyebrow">Starter Picks</p><h3>Opening Choice</h3></div></div><div class="core-item-list">${starterRows}</div></article>
                    <article class="detail-card-v2 core-build-card"><div class="section-head"><div><p class="eyebrow">Council Items</p><h3>Most Bought</h3></div></div><div class="core-item-list">${coreItemRows}</div></article>
                    <article class="detail-card-v2 core-build-card"><div class="section-head"><div><p class="eyebrow">Core Paths</p><h3>Common Starts</h3></div></div><div class="core-path-list">${corePathRows}</div></article>
                    <article class="detail-card-v2 core-build-card"><div class="section-head"><div><p class="eyebrow">By Rater</p><h3>Who Builds What</h3></div></div><div class="core-player-grid">${playerBuildRows}</div></article>
                </div>
            </section>
        `,
        edit: `
            <section class="god-modal-tab-panel god-dossier-panel">
                <article class="detail-card-v2 wide god-edit-card god-dossier-card">
                    <div class="section-head"><div><p class="eyebrow">Rate & Rank</p><h3>Update This God</h3></div><span class="summary-pill">Private council edit</span></div>
                    <div class="god-edit-grid">
                        <label class="field"><span>Council Member</span><select id="god-edit-player">${state.config.players.map((player) => `<option value="${escapeHtml(player)}" ${editPlayer === player ? "selected" : ""}>${escapeHtml(player)}</option>`).join("")}</select></label>
                        ${editUnlocked ? `
                            <label class="field"><span>Rating</span><input id="god-edit-rating" type="number" min="0" max="100" value="${editRating}"></label>
                            <div class="rank-meta god-edit-auto-rank">Current rank: ${editRank ? `#${escapeHtml(editRank)}` : "Unranked"}. Saving will auto-order this god by rating.</div>
                            <button class="btn-primary" id="god-edit-save" type="button">Save ${escapeHtml(god.God)}</button>
                        ` : `
                            <label class="field"><span>PIN</span><input id="god-edit-pin" type="password" placeholder="Enter ${escapeHtml(editPlayer)} PIN"></label>
                            <button class="btn-primary" id="god-edit-unlock" type="button">Unlock</button>
                        `}
                    </div>
                    <p class="rank-meta">Tip: set rating to 0 to remove this god from that rater's ranked list.</p>
                </article>
            </section>
        `,
    };

    elements.godModalContent.innerHTML = `
        <div class="god-modal-hero" style="${god.ImageUrl ? `background-image:url('${god.ImageUrl}')` : ""}">
            ${god.PantheonImageUrl ? `<img class="modal-pantheon-watermark" src="${god.PantheonImageUrl}" alt="" aria-hidden="true">` : ""}
            <div class="god-modal-shade"></div>
            <div class="modal-badge-stack">
                <div class="modal-rank-chip">#${god.Rank || "--"}</div>
                <div class="modal-tier-chip" style="color:${tierColor(god.Tier)}">${escapeHtml(god.Tier || "U")}</div>
            </div>
            <div class="modal-score-chip" style="color:${tierColor(god.Tier)}">${god.Rating}</div>
            <div class="god-modal-copy"><p class="eyebrow god-pantheon-label">${god.PantheonImageUrl ? `<img class="pantheon-inline-icon" src="${god.PantheonImageUrl}" alt="" aria-hidden="true">` : ""}<span>${escapeHtml(god.Pantheon || "Unknown Pantheon")}</span></p><h2>${escapeHtml(god.God)}</h2><p class="god-modal-title">${escapeHtml(god.Title || "")}</p><div class="modal-meta-line">${escapeHtml(god.Role || "")} | ${escapeHtml(god.Class || "")} | ${escapeHtml(god["Attack Type"] || "")} | ${escapeHtml(god["Damage Type"] || "")}</div></div>
        </div>
        <div class="god-modal-body">
            <div class="subtab-bar god-modal-tabs" role="tablist" aria-label="God detail sections">${sections.map((section) => `<button class="subtab-btn ${state.godDetail.section === section.key ? "active" : ""}" type="button" data-god-modal-section="${section.key}" role="tab" aria-selected="${state.godDetail.section === section.key ? "true" : "false"}">${escapeHtml(section.label)}</button>`).join("")}</div>
            <div class="subtab-content god-modal-tab-content">${sectionHtml[state.godDetail.section]}</div>
        </div>
    `;

    elements.godModalBackdrop.classList.remove("hidden");
    document.body.classList.add("modal-open");

    elements.godModalContent.querySelectorAll("[data-build-aspect]").forEach((button) => {
        button.addEventListener("click", () => {
            state.godDetail.buildAspect = button.dataset.buildAspect || "All";
            openGodDetail(godName);
        });
    });

    elements.godModalContent.querySelectorAll("[data-god-modal-section]").forEach((button) => {
        button.addEventListener("click", () => {
            state.godDetail.section = button.dataset.godModalSection || "council";
            openGodDetail(godName);
        });
    });
    elements.godModalContent.querySelector("#god-edit-player")?.addEventListener("change", (event) => {
        state.godDetail.editPlayer = event.target.value;
        openGodDetail(godName);
    });
    elements.godModalContent.querySelector("#god-edit-unlock")?.addEventListener("click", () => unlockGodQuickEdit(godName));
    elements.godModalContent.querySelector("#god-edit-save")?.addEventListener("click", () => saveGodQuickEdit(godName));
}

// This helper closes the God Index deep-dive modal.
function closeGodDetail() {
    if (!elements.godModalBackdrop) return;
    elements.godModalBackdrop.classList.add("hidden");
    document.body.classList.remove("modal-open");
}

// This helper renders the rankings tab.
// This helper aggregates starter and completed item usage from council loadouts
// so the Items tab can answer what the group actually builds and wins with.
// Item catalog rendering lives in static/items.js.

function renderRankingsTab() {
    const risers = [...state.gods].filter((god) => Number(god.Movement || 0) > 0).sort((a, b) => Number(b.Movement || 0) - Number(a.Movement || 0)).slice(0, 3);
    const fallers = [...state.gods].filter((god) => Number(god.Movement || 0) < 0).sort((a, b) => Number(a.Movement || 0) - Number(b.Movement || 0)).slice(0, 3);
    const fullCouncilFavorites = [...state.gods]
        .filter((god) => coverageCount(god) === state.config.players.length)
        .sort((a, b) => b.Rating - a.Rating || a.God.localeCompare(b.God))
        .slice(0, 3);
    const hiddenGems = [...state.gods]
        .filter((god) => coverageCount(god) < state.config.players.length && coverageCount(god) >= 2 && god.Rating >= 80)
        .sort((a, b) => b.Rating - a.Rating || coverageCount(b) - coverageCount(a))
        .slice(0, 3);
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
            <div class="mini-highlight-grid">
                <article class="mini-highlight-card">
                    <div class="metric-label">Full Council Locks</div>
                    ${fullCouncilFavorites.length ? fullCouncilFavorites.map((god) => `<div class="mini-highlight-row"><span>${escapeHtml(god.God)}</span><strong>${god.Rating}</strong></div>`).join("") : `<div class="rank-meta">No full-coverage locks yet</div>`}
                </article>
                <article class="mini-highlight-card">
                    <div class="metric-label">Hidden Gems</div>
                    ${hiddenGems.length ? hiddenGems.map((god) => `<div class="mini-highlight-row"><span>${escapeHtml(god.God)}</span><strong>${coverageCount(god)}/${state.config.players.length}</strong></div>`).join("") : `<div class="rank-meta">No partial-coverage gems right now</div>`}
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
            const bestRole = favoriteDimensionForPlayer(player, "Role");
            const bestPantheon = favoriteDimensionForPlayer(player, "Pantheon");
            const averageScore = averagePlayerScore(player);
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
                    <div class="profile-chip-row">
                        <span class="summary-pill">Avg ${averageScore}</span>
                        ${bestRole ? `<span class="summary-pill">Role: ${escapeHtml(bestRole.label)}</span>` : ""}
                        ${bestPantheon ? `<span class="summary-pill">Pantheon: ${escapeHtml(bestPantheon.label)}</span>` : ""}
                    </div>
                    <div class="taste-note">
                        ${bestRole ? `${escapeHtml(player)} leans hardest toward ${escapeHtml(bestRole.label.toLowerCase())}` : `${escapeHtml(player)} is still building a clear taste profile`}
                        ${bestPantheon ? ` and especially lights up for ${escapeHtml(bestPantheon.label)} picks.` : "."}
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

// This helper builds a council-and-SmiteSource profile object so the tab can
// merge real match stats with the personality data already in this app.
function buildRaterProfile(player) {
    const ratedGods = state.gods
        .filter((god) => Number.isFinite(god[player]) && god[player] > 0)
        .sort((a, b) => (b[player] - a[player]) || (b.Rating - a.Rating) || a.God.localeCompare(b.God));
    const councilTopGods = ratedGods.slice(0, 3);
    const external = state.raterStats.profiles?.[player] || {};
    const signature = state.gods.find((god) => god.God === external.topGods?.[0]?.name) || councilTopGods[0] || null;
    const favoriteRole = favoriteDimensionForPlayer(player, "Role");
    const favoritePantheon = favoriteDimensionForPlayer(player, "Pantheon");
    const archetype = playerArchetype(player);
    const avgScore = averagePlayerScore(player);
    const ratedCount = ratedGods.length;
    const roleBias = strongestBiasForPlayer(player, "Role");
    const pantheonBias = strongestBiasForPlayer(player, "Pantheon");
    const recentHistory = state.recentHistory
        .filter((row) => row.player === player && (row.change_type || "rating") === "rating")
        .slice(0, 4)
        .map((row) => ({
            god: row.god_name,
            oldValue: Number(row.old_value || 0),
            newValue: Number(row.new_value || 0),
            changedAt: row.changed_at,
        }));

    return {
        player,
        profileLink: external.profileUrl || RATER_PROFILE_LINKS[player] || "",
        linked: external.linked || Boolean(RATER_PROFILE_LINKS[player]),
        available: Boolean(external.available),
        error: external.error || "",
        displayName: external.displayName || player,
        metrics: external.metrics || {},
        topGods: external.topGods || [],
        godStats: external.godStats || {},
        topRoles: external.topRoles || [],
        opponentMatchups: external.opponentMatchups || {},
        buildStats: external.buildStats || {},
        recentMatches: external.recentMatches || [],
        chemistry: external.chemistry || {},
        insights: external.insights || {},
        rankSummary: external.rankSummary || "",
        peakRankSummary: external.peakRankSummary || "",
        historySource: external.historySource || "",
        signature,
        favoriteRole,
        favoritePantheon,
        archetype,
        avgScore,
        ratedCount,
        roleBias,
        pantheonBias,
        recentHistory,
    };
}

// This helper formats a compact wins-losses record for the stat rows.
function formatWinLossRecord(wins, gamesPlayed) {
    const safeGames = Number(gamesPlayed) || 0;
    const safeWins = Number(wins) || 0;
    const losses = Math.max(safeGames - safeWins, 0);
    return `${safeWins}-${losses}`;
}

// This helper surfaces the gods each rater actually queues the most so the tab
// can answer "who do they play?" at a glance.
function mostPlayedGods(topGods, limit = 5) {
    return [...(topGods || [])]
        .sort((left, right) =>
            (Number(right.gamesPlayed) - Number(left.gamesPlayed))
            || (Number(right.winRate) - Number(left.winRate))
            || String(left.name || "").localeCompare(String(right.name || ""))
        )
        .slice(0, limit);
}

// This helper highlights winning gods while filtering out tiny one-off samples.
function bestGodsByWinRate(topGods, limit = 4, minimumGames = 3) {
    return [...(topGods || [])]
        .filter((god) => Number(god.gamesPlayed) >= minimumGames)
        .sort((left, right) =>
            (Number(right.winRate) - Number(left.winRate))
            || (Number(right.wins) - Number(left.wins))
            || (Number(right.gamesPlayed) - Number(left.gamesPlayed))
            || String(left.name || "").localeCompare(String(right.name || ""))
        )
        .slice(0, limit);
}

function renderOpponentRows(records = [], { emptyText = "No opponent sample yet", tone = "" } = {}) {
    if (!records.length) return `<div class="rank-meta">${escapeHtml(emptyText)}</div>`;
    return records.map((record) => {
        const name = record.god || record.enemyGod || record.label || "Unknown";
        const games = Number(record.games || 0);
        const wins = Number(record.wins || 0);
        const losses = Number(record.losses || Math.max(games - wins, 0));
        const wr = Number(record.winRate || 0);
        return `
            <div class="mini-highlight-row opponent-row ${escapeHtml(tone)}">
                <span><strong>${escapeHtml(name)}</strong><small>${formatWinLossRecord(wins, games)} | ${formatMetric(wr, 1, "%")} WR${record.class ? ` | ${escapeHtml(record.class)}` : ""}</small></span>
                <strong class="${wr >= 55 ? "movement-up" : wr <= 45 && games ? "movement-down" : ""}">${wins}-${losses}</strong>
            </div>
        `;
    }).join("");
}

function raterGodSortLabel(sortKey) {
    if (sortKey === "best") return "Best W/L";
    if (sortKey === "worst") return "Lowest W/L";
    return "Most Played";
}

function allRaterGodRows(profile, selectedPlayer, sortKey = "played") {
    const godLookup = new Map(state.gods.map((god, index) => [god.God, { god, index }]));
    const rows = Object.values(profile.godStats || {})
        .filter((record) => Number(record.gamesPlayed || 0) > 0)
        .map((record) => {
            const lookup = godLookup.get(record.name) || {};
            const god = lookup.god || {};
            return {
                ...record,
                rating: Number.isFinite(Number(god[selectedPlayer])) ? Number(god[selectedPlayer]) : null,
                rank: Number.isFinite(lookup.index) ? lookup.index + 1 : null,
                role: record.role || god.Role || "",
                pantheon: record.pantheon || god.Pantheon || "",
            };
        });

    const byName = (left, right) => String(left.name || "").localeCompare(String(right.name || ""));
    if (sortKey === "best") {
        return rows.sort((left, right) =>
            (Number(right.winRate || 0) - Number(left.winRate || 0))
            || (Number(right.wins || 0) - Number(left.wins || 0))
            || (Number(right.gamesPlayed || 0) - Number(left.gamesPlayed || 0))
            || byName(left, right)
        );
    }
    if (sortKey === "worst") {
        return rows.sort((left, right) =>
            (Number(left.winRate || 0) - Number(right.winRate || 0))
            || ((Number(right.gamesPlayed || 0) - Number(right.wins || 0)) - (Number(left.gamesPlayed || 0) - Number(left.wins || 0)))
            || (Number(right.gamesPlayed || 0) - Number(left.gamesPlayed || 0))
            || byName(left, right)
        );
    }
    return rows.sort((left, right) =>
        (Number(right.gamesPlayed || 0) - Number(left.gamesPlayed || 0))
        || (Number(right.winRate || 0) - Number(left.winRate || 0))
        || byName(left, right)
    );
}

function renderAllRaterGodsSection(profile, selectedPlayer) {
    const sortKey = state.raterStats.godSort || "played";
    const sortOptions = [
        { key: "played", label: "Most Played" },
        { key: "best", label: "Best W/L" },
        { key: "worst", label: "Lowest W/L" },
    ];
    const rows = allRaterGodRows(profile, selectedPlayer, sortKey);
    const totalGames = rows.reduce((sum, row) => sum + Number(row.gamesPlayed || 0), 0);
    const tableRows = rows.length
        ? rows.map((row, index) => {
            const games = Number(row.gamesPlayed || 0);
            const wins = Number(row.wins || 0);
            const winRate = Number(row.winRate || 0);
            return `
                <tr class="rater-god-table-row" data-god-detail="${escapeHtml(row.name)}" tabindex="0" role="button">
                    <td><span class="rater-god-rank">#${index + 1}</span></td>
                    <td><strong>${escapeHtml(row.name)}</strong><small>${[row.role, row.pantheon].filter(Boolean).map(escapeHtml).join(" | ") || "God"}</small></td>
                    <td>${formatMetric(games)}</td>
                    <td>${formatWinLossRecord(wins, games)}</td>
                    <td class="${winRate >= 55 ? "movement-up" : winRate <= 45 ? "movement-down" : ""}">${formatMetric(winRate, 1, "%")}</td>
                    <td>${row.rating ? formatMetric(row.rating) : "--"}</td>
                    <td>${row.rank ? `#${row.rank}` : "--"}</td>
                    <td>${row.kdaRatio ? formatMetric(row.kdaRatio, 2) : "--"}</td>
                </tr>
            `;
        }).join("")
        : `<tr><td colspan="8"><div class="rank-meta">No stored Joust games found for ${escapeHtml(selectedPlayer)} yet.</div></td></tr>`;

    return `
        <section class="rater-profile-panel rater-all-gods-panel">
            <div class="section-head">
                <div class="section-kicker"><p class="eyebrow">Full God Ledger</p><h3>${escapeHtml(selectedPlayer)} All Gods</h3></div>
                <span class="summary-pill">${formatMetric(rows.length)} gods | ${formatMetric(totalGames)} matches</span>
            </div>
            <div class="profile-chip-row rater-sort-row" role="tablist" aria-label="Sort all rater gods">
                ${sortOptions.map((option) => `<button class="summary-pill sort-pill ${sortKey === option.key ? "active" : ""}" type="button" data-rater-god-sort="${option.key}" aria-selected="${sortKey === option.key ? "true" : "false"}">${escapeHtml(option.label)}</button>`).join("")}
            </div>
            <p class="rank-meta">Sorted by ${escapeHtml(raterGodSortLabel(sortKey))}. Rows are clickable if you want to open the god card for the deeper per-god view.</p>
            <div class="rater-god-table-wrap">
                <table class="rater-god-table">
                    <thead><tr><th></th><th>God</th><th>Matches</th><th>W/L</th><th>WR</th><th>Rating</th><th>Rank</th><th>KDA</th></tr></thead>
                    <tbody>${tableRows}</tbody>
                </table>
            </div>
        </section>
    `;
}

function renderPlayerMatchupsSection(profile, selectedPlayer) {
    const matchups = profile.opponentMatchups || {};
    return `
        <section class="rater-profile-panel">
            <div class="section-kicker"><p class="eyebrow">Enemy Ledger</p><h3>${escapeHtml(selectedPlayer)} Matchups</h3></div>
            <div class="profile-chip-row">
                <span class="summary-pill">Raw match opponent data</span>
                <span class="summary-pill">Joust only</span>
                <span class="summary-pill">Nemesis = low WR against</span>
            </div>
            <div class="mini-highlight-grid rater-detail-grid matchup-grid">
                <article class="mini-highlight-card nemesis-card"><div class="metric-label">Nemesis Gods</div>${renderOpponentRows(matchups.painGods || [], { emptyText: "No pain gods yet", tone: "rough" })}</article>
                <article class="mini-highlight-card farm-card"><div class="metric-label">Farm Targets</div>${renderOpponentRows(matchups.farmGods || [], { emptyText: "No farm targets yet", tone: "good" })}</article>
                <article class="mini-highlight-card"><div class="metric-label">Most Seen Enemies</div>${renderOpponentRows(matchups.mostSeenGods || [], { emptyText: "No enemy god sample yet" })}</article>
                <article class="mini-highlight-card"><div class="metric-label">Class Pressure</div>
                    ${renderOpponentRows(matchups.painClasses || [], { emptyText: "No class pain yet" })}
                    <div class="metric-label" style="margin-top:12px;">Classes We Beat</div>
                    ${renderOpponentRows(matchups.farmClasses || [], { emptyText: "No class farms yet" })}
                </article>
            </div>
        </section>
    `;
}


function renderSignaturePodium(profile, selectedPlayer, { compactHeader = false } = {}) {
    const contenders = [...(profile.topGods || [])]
        .filter((god) => Number(god.gamesPlayed || 0) > 0)
        .sort((a, b) => Number(b.gamesPlayed || 0) - Number(a.gamesPlayed || 0) || Number(b.winRate || 0) - Number(a.winRate || 0) || String(a.name || "").localeCompare(String(b.name || "")))
        .slice(0, 3);

    if (!contenders.length) {
        return `<article class="signature-podium empty"><p class="eyebrow">Signature Podium</p><h3>No main crowned yet</h3><p class="rank-meta">Stored Joust matches will crown ${escapeHtml(selectedPlayer)}'s top three once enough games are imported.</p></article>`;
    }

    const podiumOrder = [contenders[1], contenders[0], contenders[2]].filter(Boolean);
    const podiumCard = (god, visualIndex) => {
        const trueRank = contenders.indexOf(god) + 1;
        const isMain = trueRank === 1;
        return `
            <button class="signature-podium-card ${isMain ? "main" : ""} place-${trueRank}" type="button" data-god-detail="${escapeHtml(god.name)}">
                <div class="signature-podium-art">${god.imageUrl ? `<img src="${escapeHtml(god.imageUrl)}" alt="${escapeHtml(god.name)}" loading="lazy" decoding="async">` : `<span>${escapeHtml(String(god.name || "?").slice(0, 1))}</span>`}</div>
                <div class="signature-medal">#${trueRank}</div>
                <div class="signature-podium-copy">
                    <small>${isMain ? "Main" : visualIndex === 0 ? "Second" : "Third"}</small>
                    <strong>${escapeHtml(god.name)}</strong>
                    <span>${formatWinLossRecord(god.wins, god.gamesPlayed)} | ${formatMetric(god.gamesPlayed)} games | ${formatMetric(god.winRate, 1, "%")} WR</span>
                </div>
            </button>
        `;
    };

    const metrics = profile.metrics || {};
    const headerStats = `
        <div class="signature-profile-stats">
            <span><strong>${formatMetric(metrics.winRate, 1, "%")}</strong><small>WR</small></span>
            <span><strong>${formatMetric(metrics.kdaRatio, 2)}</strong><small>KDA</small></span>
            <span><strong>${formatMetric(metrics.matches)}</strong><small>Matches</small></span>
            <span><strong>${profile.ratedCount}</strong><small>Rated</small></span>
        </div>
    `;

    return `
        <article class="signature-podium ${compactHeader ? "profile-signature-podium" : ""}">
            <div class="section-head signature-podium-head"><div><p class="eyebrow">Council Profile</p><h3>${escapeHtml(selectedPlayer)}'s Signature Gods</h3></div>${headerStats}</div>
            <div class="signature-podium-stage">${podiumOrder.map(podiumCard).join("")}</div>
        </article>
    `;
}

// This helper renders the live SmiteSource-backed rater dashboard cards with
// graceful fallbacks for unlinked or data-light profiles.
function renderRaterStatsTab() {
    const isMobile = state.ui.isMobile;
    if (!state.config.players.includes(state.raterStats.selectedPlayer)) {
        state.raterStats.selectedPlayer = state.config.players[0] || "Joey";
    }
    const selectedPlayer = state.raterStats.selectedPlayer;
    const sections = [
        { key: "profile", label: "Profile" },
        { key: "favorites", label: "Favorites" },
        { key: "all-gods", label: "All Gods" },
        { key: "matchups", label: "Matchups" },
        { key: "details", label: "Details" },
    ];
    if (!sections.some((section) => section.key === state.raterStats.section)) {
        state.raterStats.section = "profile";
    }

    const profile = buildRaterProfile(selectedPlayer);
    const mostPlayed = mostPlayedGods(profile.topGods, isMobile ? 4 : 6);
    const bestWinRate = bestGodsByWinRate(profile.topGods, isMobile ? 3 : 5, 3);
    const signatureName = profile.topGods[0]?.name || profile.signature?.God || 'Unformed Legend';
    const signatureImage = profile.topGods[0]?.imageUrl || profile.signature?.ImageUrl || '';
    const recentForm = profile.insights.recentForm || (profile.recentHistory.length >= 3 ? 'Actively tuning council scores' : 'Quiet week');
    const buildDna = profile.insights.buildDna || '';
    const favoritePantheon = profile.favoritePantheon?.label || '';
    const availabilityNote = profile.available
        ? 'No recent match sample is available yet'
        : (profile.linked ? 'Profile linked, but no public match sample was returned yet' : 'Profile not linked yet');

    const topFavorites = [...state.gods]
        .filter((god) => Number.isFinite(god[selectedPlayer]) && god[selectedPlayer] > 0)
        .sort((a, b) => (b[selectedPlayer] - a[selectedPlayer]) || a.God.localeCompare(b.God))
        .slice(0, 12);

    const favoriteRows = topFavorites.length
        ? topFavorites.map((god, index) => {
            const godStat = profile.godStats?.[god.God] || null;
            const delta = Number(god[selectedPlayer] - god.Rating) || 0;
            return `
                <article class="favorite-profile-card" data-god-detail="${escapeHtml(god.God)}" role="button" tabindex="0">
                    <div class="favorite-rank-medallion">#${index + 1}</div>
                    <div class="favorite-profile-art">${god.ImageUrl ? `<img class="god-art" src="${god.ImageUrl}" alt="${escapeHtml(god.God)}" loading="lazy" decoding="async">` : `<div class="image-fallback">Art</div>`}</div>
                    <div>
                        <strong>${escapeHtml(god.God)}</strong>
                        <div class="rank-meta">${escapeHtml(god.Role || "")} | ${escapeHtml(god.Pantheon || "")}</div>
                        <div class="profile-chip-row">
                            <span class="summary-pill">Rating ${god[selectedPlayer]}</span>
                            <span class="summary-pill ${delta > 0 ? 'cool' : delta < 0 ? 'warm' : 'muted'}">${delta > 0 ? '+' : ''}${delta} vs avg</span>
                            ${godStat ? `<span class="summary-pill">${formatWinLossRecord(godStat.wins, godStat.gamesPlayed)} | ${formatMetric(godStat.winRate, 1, '%')} WR</span>` : ''}
                        </div>
                    </div>
                </article>
            `;
        }).join("")
        : `<div class="rank-meta">No favorites yet for ${escapeHtml(selectedPlayer)}.</div>`;

    const profileSection = `
        <section class="rater-profile-panel rater-profile-compact">
            ${renderSignaturePodium(profile, selectedPlayer, { compactHeader: true })}
            <div class="mini-highlight-grid rater-quick-grid" style="margin-top:14px;">
                <article class="mini-highlight-card"><div class="metric-label">Most Played</div>${mostPlayed.length ? mostPlayed.map((god) => `<div class="mini-highlight-row"><span><strong>${escapeHtml(god.name)}</strong><small>${formatWinLossRecord(god.wins, god.gamesPlayed)} | ${formatMetric(god.winRate, 1, '%')} WR</small></span><strong>${formatMetric(god.gamesPlayed)} games</strong></div>`).join('') : `<div class="rank-meta">${escapeHtml(availabilityNote)}</div>`}</article>
                <article class="mini-highlight-card"><div class="metric-label">Best Win Rates</div>${bestWinRate.length ? bestWinRate.map((god) => `<div class="mini-highlight-row"><span><strong>${escapeHtml(god.name)}</strong><small>${formatWinLossRecord(god.wins, god.gamesPlayed)} over ${formatMetric(god.gamesPlayed)} games</small></span><strong>${formatMetric(god.winRate, 1, '%')}</strong></div>`).join('') : `<div class="rank-meta">Need at least 3 games on a god to crown a real win-rate pick.</div>`}</article>
            </div>
        </section>
    `;

    const favoritesSection = `
        <section class="rater-profile-panel">
            <div class="section-kicker"><p class="eyebrow">Individual Taste</p><h3>${escapeHtml(selectedPlayer)} Favorites</h3></div>
            <div class="profile-chip-row">
                <span class="summary-pill">Avg ${profile.avgScore}</span>
                ${profile.favoriteRole ? `<span class="summary-pill">Role: ${escapeHtml(profile.favoriteRole.label)}</span>` : ''}
                ${profile.favoritePantheon ? `<span class="summary-pill">Pantheon: ${escapeHtml(profile.favoritePantheon.label)}</span>` : ''}
            </div>
            <div class="favorite-profile-grid">${favoriteRows}</div>
        </section>
    `;

    const allGodsSection = renderAllRaterGodsSection(profile, selectedPlayer);

    const matchupsSection = renderPlayerMatchupsSection(profile, selectedPlayer);

    const detailsSection = `
        <section class="rater-profile-panel">
            <div class="section-kicker"><p class="eyebrow">Deep Cut</p><h3>${escapeHtml(selectedPlayer)} Details</h3></div>
            <div class="mini-highlight-grid rater-detail-grid">
                <article class="mini-highlight-card mini-highlight-card-muted"><div class="metric-label">Player Snapshot</div><div class="mini-highlight-row"><span>Most queued role</span><strong>${escapeHtml(profile.topRoles[0]?.role || profile.favoriteRole?.label || 'Unknown')}</strong></div><div class="mini-highlight-row"><span>Current signature</span><strong>${escapeHtml(signatureName)}</strong></div><div class="mini-highlight-row"><span>Avg council score</span><strong>${formatMetric(profile.avgScore)}</strong></div><div class="mini-highlight-row"><span>Peak rank</span><strong>${escapeHtml(profile.peakRankSummary || 'Unranked / unavailable')}</strong></div></article>
                <article class="mini-highlight-card"><div class="metric-label">Recent Matches</div>${profile.recentMatches.length ? profile.recentMatches.slice(0, 5).map((match) => `<div class="mini-highlight-row"><span>${escapeHtml(match.godName)} | ${escapeHtml(match.role || 'Role')}</span><strong class="${match.won ? 'movement-up' : 'movement-down'}">${match.won ? 'W' : 'L'} ${formatMetric(match.kills)}/${formatMetric(match.deaths)}/${formatMetric(match.assists)}</strong></div>`).join('') : `<div class="rank-meta">${escapeHtml(availabilityNote)}</div>`}</article>
                <article class="mini-highlight-card"><div class="metric-label">Role Snapshot</div>${profile.topRoles.length ? profile.topRoles.map((role) => `<div class="mini-highlight-row"><span><strong>${escapeHtml(role.role)}</strong><small>${formatWinLossRecord(role.wins, role.gamesPlayed)} | ${formatMetric(role.winRate, 1, '%')} WR</small></span><strong>${formatMetric(role.gamesPlayed)} games</strong></div>`).join('') : `<div class="rank-meta">No role sample yet</div>`}</article>
                <article class="mini-highlight-card"><div class="metric-label">Performance Snapshot</div><div class="mini-highlight-row"><span>Damage / min</span><strong>${formatMetric(profile.metrics.damagePerMin)}</strong></div><div class="mini-highlight-row"><span>Gold / min</span><strong>${formatMetric(profile.metrics.goldPerMin)}</strong></div><div class="mini-highlight-row"><span>XP / min</span><strong>${formatMetric(profile.metrics.xpPerMin)}</strong></div><div class="mini-highlight-row"><span>Wards / match</span><strong>${formatMetric(profile.metrics.wardsPerMatch, 1)}</strong></div></article>
            </div>
        </section>
    `;

    const banner = state.raterStats.error
        ? `<div class="status-banner">Rater stats hit an issue: ${escapeHtml(state.raterStats.error)}. Council-derived profile insights are still available below.</div>`
        : "";

    const sectionMap = { profile: profileSection, favorites: favoritesSection, "all-gods": allGodsSection, matchups: matchupsSection, details: detailsSection };

    elements.tabRaterStats.innerHTML = `
        <div class="panel tab-overview-panel">
            <div class="panel-heading panel-heading-inline">
                <div><p class="eyebrow">Council Member Lens</p><h2>Rater Profile</h2></div>
                <span class="summary-pill">One rater at a time</span>
            </div>
            ${banner}
            <div class="subtab-shell rater-profile-shell">
                <div class="subtab-bar rater-player-tabs" role="tablist" aria-label="Rater profiles">
                    ${state.config.players.map((player) => `<button class="subtab-btn ${selectedPlayer === player ? 'active' : ''}" type="button" data-rater-player="${escapeHtml(player)}" role="tab" aria-selected="${selectedPlayer === player ? 'true' : 'false'}">${escapeHtml(player)}</button>`).join('')}
                </div>
                <div class="subtab-bar rater-section-tabs" role="tablist" aria-label="Rater profile sections">
                    ${sections.map((section) => `<button class="subtab-btn ${state.raterStats.section === section.key ? 'active' : ''}" type="button" data-rater-section="${section.key}" role="tab" aria-selected="${state.raterStats.section === section.key ? 'true' : 'false'}">${escapeHtml(section.label)}</button>`).join('')}
                </div>
                <div class="subtab-content">${sectionMap[state.raterStats.section]}</div>
            </div>
            ${renderBackToTop()}
        </div>
    `;
}

// This helper aggregates per-rater chemistry into council-wide duo, combo,
// class, and queue leaderboards for the dedicated Chemistry tab.
function buildChemistryInsights() {
    const duoMap = new Map();
    const duoComboMap = new Map();
    const trioComboMap = new Map();
    const classMap = new Map();
    const duoClassComboMap = new Map();
    const trioClassComboMap = new Map();
    const queueMap = new Map();
    const sessionMap = new Map();
    const groupMap = new Map();
    const trioQueueMap = new Map();
    const opponentGodMap = new Map();
    const opponentCompMap = new Map();

    state.config.players.forEach((player) => {
        const profile = buildRaterProfile(player);
        const chemistry = profile.chemistry || {};

        (chemistry.duoOnlyRecords || []).forEach((record) => {
            const members = [player, record.player].sort((left, right) => left.localeCompare(right));
            const key = members.join("|");
            if (duoMap.has(key)) return;
            duoMap.set(key, {
                label: members.join(" + "),
                members,
                games: record.games,
                wins: record.wins,
                losses: record.losses,
                winRate: record.winRate,
            });
        });

        (chemistry.sharedGroups || []).forEach((record) => {
            const members = [...(record.members || [])].sort((left, right) => left.localeCompare(right));
            if (members.length < 2) return;
            const key = members.join("|");
            if (groupMap.has(key)) return;
            groupMap.set(key, {
                label: members.join(" + "),
                members,
                games: record.games,
                wins: record.wins,
                losses: record.losses,
                winRate: record.winRate,
            });
        });

        (chemistry.groupGodRecords || []).forEach((record) => {
            const members = [...(record.members || [])].sort((left, right) => left.localeCompare(right));
            if (members.length < 2) return;
            const participantGods = members.reduce((accumulator, member) => ({
                ...accumulator,
                [member]: record.participantGods?.[member] || "",
            }), {});
            const gods = members.map((member) => participantGods[member]).filter(Boolean);
            if (gods.length !== members.length) return;

            const comboKey = `${members.join("|")}|${gods.join("|")}`;
            const comboTarget = members.length >= 3 ? trioComboMap : duoComboMap;
            if (!comboTarget.has(comboKey)) {
                comboTarget.set(comboKey, {
                    label: gods.join(" + "),
                    members,
                    participantGods,
                    games: Number(record.games || 0),
                    wins: Number(record.wins || 0),
                    losses: Number(record.losses || 0),
                    winRate: Number(record.winRate || 0),
                });
            }

        });

        (chemistry.opponentGodRecords || []).forEach((record) => {
            const members = [...(record.members || [])].sort((left, right) => left.localeCompare(right));
            if (members.length < 2 || !record.enemyGod) return;
            const key = `${members.join("|")}|${record.enemyGod}`;
            if (!opponentGodMap.has(key)) {
                opponentGodMap.set(key, {
                    enemyGod: record.enemyGod,
                    label: record.enemyGod,
                    members,
                    games: Number(record.games || 0),
                    wins: Number(record.wins || 0),
                    losses: Number(record.losses || 0),
                    winRate: Number(record.winRate || 0),
                });
            }
        });

        (chemistry.opponentCompRecords || []).forEach((record) => {
            const members = [...(record.members || [])].sort((left, right) => left.localeCompare(right));
            const enemies = [...(record.enemyGods || [])].sort((left, right) => left.localeCompare(right));
            if (members.length < 2 || !enemies.length) return;
            const key = `${members.join("|")}|${enemies.join("|")}`;
            if (!opponentCompMap.has(key)) {
                opponentCompMap.set(key, {
                    label: record.label || enemies.join(" + "),
                    enemyGods: enemies,
                    members,
                    games: Number(record.games || 0),
                    wins: Number(record.wins || 0),
                    losses: Number(record.losses || 0),
                    winRate: Number(record.winRate || 0),
                });
            }
        });

        (chemistry.recentSessions || []).forEach((session) => {
            const members = [...(session.participants || [])].sort((left, right) => left.localeCompare(right));
            const participantGods = members.reduce((accumulator, member) => ({
                ...accumulator,
                [member]: session.participantGods?.[member] || "",
            }), {});
            const key = `${session.startedAt}|${members.join("|")}|${members.map((member) => `${member}:${participantGods[member] || ""}`).join("|")}|${session.queueType}`;
            if (sessionMap.has(key)) return;
            sessionMap.set(key, {
                ...session,
                participants: members,
                participantGods,
            });
        });
    });

    [...sessionMap.values()].forEach((session) => {
        if (!queueMap.has(session.queueType)) {
            queueMap.set(session.queueType, {
                label: session.queueType,
                games: 0,
                wins: 0,
                losses: 0,
                winRate: 0,
            });
        }
        const queueRecord = queueMap.get(session.queueType);
        queueRecord.games += 1;
        queueRecord.wins += session.won ? 1 : 0;
        queueRecord.losses += session.won ? 0 : 1;

        if ((session.participants || []).length >= 3) {
            const trioQueueKey = session.queueType;
            if (!trioQueueMap.has(trioQueueKey)) {
                trioQueueMap.set(trioQueueKey, {
                    label: session.queueType,
                    games: 0,
                    wins: 0,
                    losses: 0,
                    winRate: 0,
                });
            }
            const trioQueue = trioQueueMap.get(trioQueueKey);
            trioQueue.games += 1;
            trioQueue.wins += session.won ? 1 : 0;
            trioQueue.losses += session.won ? 0 : 1;
        }
    });

    [...duoComboMap.values(), ...trioComboMap.values()].forEach((record) => {
        const members = record.members || [];
        const { label: assignmentLabel, key: assignmentKey, assignments } = chemistryClassAssignments(record.participantGods || {}, members);
        const assignmentTarget = members.length >= 3 ? trioClassComboMap : duoClassComboMap;
        const assignmentRecordKey = `${chemistryMembersKey(members)}|${assignmentKey}`;
        if (!assignmentTarget.has(assignmentRecordKey)) {
            assignmentTarget.set(assignmentRecordKey, {
                label: assignmentLabel,
                members,
                classAssignments: assignments,
                games: 0,
                wins: 0,
                losses: 0,
                winRate: 0,
            });
        }
        const assignmentRecord = assignmentTarget.get(assignmentRecordKey);
        assignmentRecord.games += Number(record.games || 0);
        assignmentRecord.wins += Number(record.wins || 0);
        assignmentRecord.losses += Number(record.losses || 0);

        const classLabel = members
            .map((member) => godMetaByName(record.participantGods?.[member] || "")?.Class || "Unknown")
            .sort((left, right) => left.localeCompare(right))
            .join(" + ");
        const classKey = `${members.length}|${classLabel}`;
        if (!classMap.has(classKey)) {
            classMap.set(classKey, {
                label: classLabel,
                size: members.length,
                games: 0,
                wins: 0,
                losses: 0,
                winRate: 0,
            });
        }
        const classRecord = classMap.get(classKey);
        classRecord.games += Number(record.games || 0);
        classRecord.wins += Number(record.wins || 0);
        classRecord.losses += Number(record.losses || 0);
    });

    const finish = (record) => ({
        ...record,
        winRate: record.games ? Math.round((record.wins / record.games) * 1000) / 10 : 0,
    });

    const duoRecords = [...duoMap.values()].map(finish).sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.label.localeCompare(b.label));
    const duoComboRecords = [...duoComboMap.values()].map(finish).sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.label.localeCompare(b.label));
    const trioComboRecords = [...trioComboMap.values()].map(finish).sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.label.localeCompare(b.label));
    const classRecords = [...classMap.values()].map(finish).sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.label.localeCompare(b.label));
    const duoClassComboRecords = [...duoClassComboMap.values()].map(finish).sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.label.localeCompare(b.label));
    const trioClassComboRecords = [...trioClassComboMap.values()].map(finish).sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.label.localeCompare(b.label));
    const queueRecords = [...queueMap.values()].map(finish).sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.label.localeCompare(b.label));
    const trioQueueRecords = [...trioQueueMap.values()].map(finish).sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.label.localeCompare(b.label));
    const groupRecords = [...groupMap.values()].map(finish).sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.label.localeCompare(b.label));
    const opponentGodRecords = [...opponentGodMap.values()].map(finish).sort((a, b) => b.games - a.games || a.winRate - b.winRate || a.label.localeCompare(b.label));
    const opponentCompRecords = [...opponentCompMap.values()].map(finish).sort((a, b) => b.games - a.games || a.winRate - b.winRate || a.label.localeCompare(b.label));
    const recentSessions = [...sessionMap.values()].sort((a, b) => new Date(b.startedAt) - new Date(a.startedAt));

    const duoFloor = duoRecords.filter((record) => record.games >= 2);
    const duoComboFloor = duoComboRecords.filter((record) => record.games >= 2);
    const trioFloor = groupRecords.filter((record) => record.members.length >= 3 && record.games >= 2);
    const trioComboFloor = trioComboRecords.filter((record) => record.games >= 2);
    const duoClassFloor = classRecords.filter((record) => record.size === 2 && record.games >= 2);
    const trioClassFloor = classRecords.filter((record) => record.size >= 3 && record.games >= 2);
    const winSessions = recentSessions.filter((session) => session.won);
    const lossSessions = recentSessions.filter((session) => !session.won);

    return {
        duoRecords,
        duoComboRecords,
        duoClassComboRecords,
        classRecords,
        queueRecords,
        trioQueueRecords,
        groupRecords,
        trioComboRecords,
        trioClassComboRecords,
        recentSessions,
        opponentGodRecords,
        opponentCompRecords,
        bestDuo: duoFloor[0] || duoRecords[0] || null,
        worstDuo: [...duoFloor].sort((a, b) => a.winRate - b.winRate || b.games - a.games || a.label.localeCompare(b.label))[0] || null,
        bestCombo: duoComboFloor[0] || duoComboRecords[0] || null,
        worstCombo: [...duoComboFloor].sort((a, b) => a.winRate - b.winRate || b.games - a.games || a.label.localeCompare(b.label))[0] || null,
        bestTrio: trioFloor[0] || null,
        worstTrio: [...trioFloor].sort((a, b) => a.winRate - b.winRate || b.games - a.games || a.label.localeCompare(b.label))[0] || null,
        bestTrioCombo: trioComboFloor[0] || null,
        worstTrioCombo: [...trioComboFloor].sort((a, b) => a.winRate - b.winRate || b.games - a.games || a.label.localeCompare(b.label))[0] || null,
        bestDuoClass: duoClassFloor[0] || null,
        worstDuoClass: [...duoClassFloor].sort((a, b) => a.winRate - b.winRate || b.games - a.games || a.label.localeCompare(b.label))[0] || null,
        bestTrioClass: trioClassFloor[0] || null,
        worstTrioClass: [...trioClassFloor].sort((a, b) => a.winRate - b.winRate || b.games - a.games || a.label.localeCompare(b.label))[0] || null,
        recentWins: winSessions.slice(0, 6),
        recentLosses: lossSessions.slice(0, 6),
    };
}

function renderChemistryNemesisSection(insights) {
    const withSample = (records = []) => records.filter((record) => Number(record.games || 0) >= 2);
    const opponentGods = withSample(insights.opponentGodRecords || []);
    const opponentComps = withSample(insights.opponentCompRecords || []);
    const painGods = [...opponentGods].sort((a, b) => Number(a.winRate || 0) - Number(b.winRate || 0) || Number(b.losses || 0) - Number(a.losses || 0)).slice(0, 8);
    const farmGods = [...opponentGods].sort((a, b) => Number(b.winRate || 0) - Number(a.winRate || 0) || Number(b.wins || 0) - Number(a.wins || 0)).slice(0, 8);
    const painComps = [...opponentComps].sort((a, b) => Number(a.winRate || 0) - Number(b.winRate || 0) || Number(b.losses || 0) - Number(a.losses || 0)).slice(0, 6);
    const farmComps = [...opponentComps].sort((a, b) => Number(b.winRate || 0) - Number(a.winRate || 0) || Number(b.wins || 0) - Number(a.wins || 0)).slice(0, 6);

    const sub = (record) => `${escapeHtml((record.members || []).join(" + "))} | ${formatWinLossRecord(record.wins, record.games)} | ${formatMetric(record.winRate, 1, "%")} WR`;
    return `
        <section class="chemistry-nemesis-panel">
            <div class="section-head"><div><p class="eyebrow">Enemy Ledger</p><h3>Who Ruins The Queue?</h3></div><span class="summary-pill">Exact duo/trio groups</span></div>
            <div class="mini-highlight-grid matchup-grid">
                <article class="mini-highlight-card nemesis-card"><div class="metric-label">Enemy Gods We Struggle Into</div>${renderChemistryRows(painGods, { emptyText: "No repeated pain gods yet", main: (record) => escapeHtml(record.enemyGod || record.label), sub, value: (record) => formatRecord(record), valueClass: "movement-down" })}</article>
                <article class="mini-highlight-card farm-card"><div class="metric-label">Enemy Gods We Beat</div>${renderChemistryRows(farmGods, { emptyText: "No repeated farm gods yet", main: (record) => escapeHtml(record.enemyGod || record.label), sub, value: (record) => formatRecord(record), valueClass: "movement-up" })}</article>
                <article class="mini-highlight-card nemesis-card"><div class="metric-label">Enemy Comps We Struggle Into</div>${renderChemistryRows(painComps, { emptyText: "No repeated enemy comps yet", main: (record) => escapeHtml(record.label), sub, value: (record) => formatRecord(record), valueClass: "movement-down" })}</article>
                <article class="mini-highlight-card farm-card"><div class="metric-label">Enemy Comps We Beat</div>${renderChemistryRows(farmComps, { emptyText: "No repeated enemy comps beaten yet", main: (record) => escapeHtml(record.label), sub, value: (record) => formatRecord(record), valueClass: "movement-up" })}</article>
            </div>
        </section>
    `;
}

// This helper renders the dedicated Chemistry tab so shared-match stats can
// breathe without overloading the Rater Stats profile cards.
function renderChemistryTab() {
    const isMobile = state.ui.isMobile;
    if (!state.raterStats.loaded) {
        elements.tabChemistry.innerHTML = `
            <div class="panel">
                <div class="panel-heading">
                    <p class="eyebrow">Council Synergy</p>
                    <h2>Chemistry</h2>
                </div>
                <div class="status-banner">Loading chemistry records from stored history.</div>
            </div>
        `;
        return;
    }
    const insights = buildChemistryInsights();
    const hasChemistryData = Boolean(
        (insights.duoRecords || []).length
        || (insights.groupRecords || []).length
        || (insights.duoComboRecords || []).length
        || (insights.trioComboRecords || []).length
        || (insights.recentSessions || []).length,
    );
    const chemistryBanner = state.raterStats.error
        ? `<div class="status-banner">Chemistry data hit an issue: ${escapeHtml(state.raterStats.error)}. Showing the last good council data when available.</div>`
        : "";

    if (!hasChemistryData) {
        elements.tabChemistry.innerHTML = `
            <div class="panel">
                <div class="panel-heading">
                    <p class="eyebrow">Council Synergy</p>
                    <h2>Chemistry</h2>
                </div>
                ${chemistryBanner}
                ${emptyState("No Chemistry Data Yet", "No duo, trio, or shared-session chemistry has been loaded into the client yet.")}
            </div>
        `;
        return;
    }

    let chemistryContent = "";
    try {
        const trinityShowcase = renderChemistryTrinityShowcase(insights, isMobile);
        const synergyMatrix = renderChemistrySynergyMatrix(insights, isMobile);
        const receiptsTimeline = renderChemistryReceiptsTimeline(insights, isMobile);
        const nemesisSection = renderChemistryNemesisSection(insights);
        const sections = [
            { key: "trinity", label: "Trinity" },
            { key: "matrix", label: "Pairings" },
            { key: "nemesis", label: "Nemesis" },
            { key: "timeline", label: "Receipts" },
        ];
        if (!sections.some((section) => section.key === state.chemistry.section)) {
            state.chemistry.section = "trinity";
        }
        const sectionHtml = {
            trinity: `<section class="chemistry-headline-section">${trinityShowcase}</section>`,
            matrix: `<section class="chemistry-subtab-panel">${synergyMatrix}</section>`,
            nemesis: `<section class="chemistry-subtab-panel">${nemesisSection}</section>`,
            timeline: `<section class="chemistry-subtab-panel">${receiptsTimeline}</section>`,
        };
        chemistryContent = `
            <div class="subtab-shell chemistry-subtab-shell">
                <div class="subtab-bar" role="tablist" aria-label="Chemistry sections">
                    ${sections.map((section) => `<button class="subtab-btn ${state.chemistry.section === section.key ? "active" : ""}" type="button" data-chemistry-section="${section.key}" role="tab" aria-selected="${state.chemistry.section === section.key ? "true" : "false"}">${escapeHtml(section.label)}</button>`).join("")}
                </div>
                <div class="subtab-content">${sectionHtml[state.chemistry.section]}</div>
            </div>
        `;
    } catch (error) {
        chemistryContent = `
            <div class="status-banner">Chemistry hit a render issue: ${escapeHtml(error.message || "Unknown error")}. Showing a compact fallback instead.</div>
            <div class="mini-highlight-grid" style="margin-top:16px;">
                <article class="mini-highlight-card">
                    <div class="metric-label">Duo Records</div>
                    ${renderChemistryRows((insights.duoRecords || []).slice(0, 6), {
                        emptyText: "No duo records yet",
                        main: (record) => escapeHtml(record.label),
                        value: (record) => formatRecord(record),
                    })}
                </article>
                <article class="mini-highlight-card">
                    <div class="metric-label">Trio Records</div>
                    ${renderChemistryRows((insights.groupRecords || []).filter((record) => (record.members || []).length >= 3).slice(0, 6), {
                        emptyText: "No trio records yet",
                        main: (record) => escapeHtml(record.label),
                        value: (record) => formatRecord(record),
                    })}
                </article>
            </div>
        `;
    }

    elements.tabChemistry.innerHTML = `
        <div class="panel">
            <div class="panel-heading">
                <p class="eyebrow">Council Synergy</p>
                <h2>Chemistry</h2>
            </div>
            ${chemistryBanner}
            ${chemistryContent}
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
    const playerAverages = [...state.config.players]
        .map((player) => ({ player, average: averagePlayerScore(player) }))
        .sort((a, b) => b.average - a.average);
    const strictest = playerAverages.at(-1);
    const mostGenerous = playerAverages[0];
    const archetypes = state.config.players.map((player) => playerArchetype(player));
    const volatilityLeaders = buildVolatilityLeaders();
    const ownership = buildOwnershipSnapshot(state.analytics.god);
    const sections = [
        { key: "overview", label: "Overview" },
        { key: "archetypes", label: "Archetypes" },
        { key: "trends", label: "Trend Lab" },
        { key: "ownership", label: "Ownership" },
        { key: "disputes", label: "Disputes" },
    ];
    if (!sections.some((section) => section.key === state.analytics.section)) {
        state.analytics.section = "overview";
    }

    const playerOptions = state.config.players
        .map((player) => `
            <label class="tiny-pill analytics-check-pill">
                <input type="checkbox" class="analytics-player" value="${escapeHtml(player)}" ${state.analytics.players.includes(player) ? "checked" : ""}>
                ${escapeHtml(player)}
            </label>
        `)
        .join("");

    const godOptions = state.gods
        .map((god) => `<option value="${escapeHtml(god.God)}" ${state.analytics.god === god.God ? "selected" : ""}>${escapeHtml(god.God)}</option>`)
        .join("");

    const archetypeCards = archetypes.map((profile) => {
        const roleBias = strongestBiasForPlayer(profile.player, "Role");
        const pantheonBias = strongestBiasForPlayer(profile.player, "Pantheon");
        return `
            <article class="archetype-card">
                <div class="archetype-topline">
                    <span class="summary-pill" style="color:${playerColor(profile.player)}">${escapeHtml(profile.player)}</span>
                    <span class="summary-pill ${profile.avgDelta > 0 ? "cool" : profile.avgDelta < 0 ? "warm" : ""}">${profile.avgDelta > 0 ? "+" : ""}${profile.avgDelta} vs avg</span>
                </div>
                <h3 class="archetype-title">${escapeHtml(profile.title)}</h3>
                <p class="taste-note">${escapeHtml(profile.note)}</p>
                <div class="profile-chip-row">
                    <span class="summary-pill">Avg ${profile.avgScore}</span>
                    <span class="summary-pill">Volatility ${profile.volatility}</span>
                </div>
                <div class="bias-list">
                    ${roleBias ? `<div class="bias-row"><span>Role bias</span><strong class="${roleBias.delta > 0 ? "movement-up" : "movement-down"}">${escapeHtml(roleBias.label)} ${roleBias.delta > 0 ? "+" : ""}${Math.round(roleBias.delta * 10) / 10}</strong></div>` : ""}
                    ${pantheonBias ? `<div class="bias-row"><span>Pantheon bias</span><strong class="${pantheonBias.delta > 0 ? "movement-up" : "movement-down"}">${escapeHtml(pantheonBias.label)} ${pantheonBias.delta > 0 ? "+" : ""}${Math.round(pantheonBias.delta * 10) / 10}</strong></div>` : ""}
                </div>
            </article>
        `;
    }).join("");

    const ownershipLine = (label, person, toneClass = "") => {
        if (!person) return "";
        const record = person.games ? `${formatWinLossRecord(person.wins, person.games)} | ${formatMetric(person.winRate, 1, "%")} WR | ${formatMetric(person.games)} games` : "No stored Joust games yet";
        return `
            <div class="mini-highlight-row ownership-row">
                <span>
                    <strong>${escapeHtml(label)}</strong>
                    <small>${record}</small>
                </span>
                <strong class="${toneClass}" style="color:${playerColor(person.player)}">${escapeHtml(person.player)} ${person.score}</strong>
            </div>
        `;
    };

    const sectionHtml = {
        overview: `
            <section class="analytics-subtab-panel active">
                <div class="section-kicker"><p class="eyebrow">Completion + Trends</p><h3>Council Snapshot</h3></div>
                <div class="progress-grid">${renderProgressCards()}</div>
                <div class="mini-highlight-grid analytics-snapshot-cards">
                    <article class="mini-highlight-card">
                        <div class="metric-label">Most Agreed Upon</div>
                        ${mostAgreed.length ? mostAgreed.map((god) => `<div class="mini-highlight-row"><span>${escapeHtml(god.God)}</span><strong class="movement-up">${agreementScore(god)}</strong></div>`).join("") : `<div class="rank-meta">Not enough overlap yet</div>`}
                    </article>
                    <article class="mini-highlight-card">
                        <div class="metric-label">Most Rated Gods</div>
                        ${mostRated.length ? mostRated.map((god) => `<div class="mini-highlight-row"><span>${escapeHtml(god.God)}</span><strong>${coverageCount(god)}/${state.config.players.length}</strong></div>`).join("") : `<div class="rank-meta">No ratings yet</div>`}
                    </article>
                    <article class="mini-highlight-card">
                        <div class="metric-label">Council Scoring Style</div>
                        <div class="mini-highlight-row"><span>Most generous</span><strong style="color:${playerColor(mostGenerous?.player)}">${escapeHtml(mostGenerous?.player || "?")} ${mostGenerous?.average || 0}</strong></div>
                        <div class="mini-highlight-row"><span>Most strict</span><strong style="color:${playerColor(strictest?.player)}">${escapeHtml(strictest?.player || "?")} ${strictest?.average || 0}</strong></div>
                    </article>
                    <article class="mini-highlight-card">
                        <div class="metric-label">Tier Distribution</div>
                        ${renderTierDistributionBars()}
                    </article>
                </div>
            </section>
        `,
        archetypes: `
            <section class="analytics-subtab-panel active">
                <div class="section-kicker"><p class="eyebrow">Council Profiles</p><h3>Archetypes</h3></div>
                <div class="archetype-grid">${archetypeCards}</div>
            </section>
        `,
        trends: `
            <section class="analytics-subtab-panel active chart-shell">
                <div class="section-kicker"><p class="eyebrow">History View</p><h3>God Rating Trends</h3></div>
                <div class="analytics-controls">
                    <label class="field">
                        <span>Target God</span>
                        <select id="analytics-god-select">${godOptions}</select>
                    </label>
                    <div class="field">
                        <span>Included Raters</span>
                        <div class="analytics-player-row">${playerOptions}</div>
                    </div>
                </div>
                <div id="analytics-chart">${buildTrendChartSvg(state.analytics.rows, state.analytics.players)}</div>
            </section>
        `,
        ownership: `
            <section class="analytics-subtab-panel active">
                <div class="section-kicker"><p class="eyebrow">God Stories</p><h3>Volatility + Ownership</h3></div>
                <div class="analytics-controls analytics-ownership-controls">
                    <label class="field">
                        <span>Target God</span>
                        <select id="analytics-god-select">${godOptions}</select>
                    </label>
                </div>
                <div class="mini-highlight-grid">
                    <article class="mini-highlight-card">
                        <div class="metric-label">Most Volatile Gods</div>
                        ${volatilityLeaders.length ? volatilityLeaders.map((entry) => `<div class="mini-highlight-row"><span>${escapeHtml(entry.godName)}</span><strong>${entry.touches} edits | ${entry.swing}</strong></div>`).join("") : `<div class="rank-meta">Not enough history yet</div>`}
                    </article>
                    <article class="mini-highlight-card ownership-card">
                        <div class="metric-label">Who Owns ${escapeHtml(state.analytics.god || "This God")}</div>
                        ${ownership?.owner ? `
                            ${ownershipLine("Biggest believer", ownership.owner, "movement-up")}
                            ${ownershipLine("Most skeptical", ownership.skeptic, "movement-down")}
                            <div class="mini-highlight-row"><span>Room spread</span><strong>${ownership.spread} pts</strong></div>
                            <div class="mini-highlight-row"><span>Coverage</span><strong>${ownership.coverage}/${state.config.players.length}</strong></div>
                        ` : `<div class="rank-meta">Select a god with ratings to see the ownership story.</div>`}
                    </article>
                </div>
            </section>
        `,
        disputes: `
            <section class="analytics-subtab-panel active">
                <div class="section-kicker"><p class="eyebrow">Council Disputes</p><h3>Most Controversial Gods</h3></div>
                ${renderControversyCards()}
            </section>
        `,
    };

    elements.tabAnalytics.innerHTML = `
        <div class="panel tab-overview-panel">
            <div class="panel-heading panel-heading-inline">
                <div>
                    <p class="eyebrow">Completion + Trends</p>
                    <h2>Analytics</h2>
                </div>
                <span class="summary-pill">Sub-tabs keep each lab focused</span>
            </div>

            <div class="subtab-shell analytics-subtab-shell">
                <div class="subtab-bar" role="tablist" aria-label="Analytics sections">
                    ${sections.map((section) => `<button class="subtab-btn ${state.analytics.section === section.key ? "active" : ""}" type="button" data-analytics-section="${section.key}" role="tab" aria-selected="${state.analytics.section === section.key ? "true" : "false"}">${escapeHtml(section.label)}</button>`).join("")}
                </div>
                <div class="subtab-content">${sectionHtml[state.analytics.section]}</div>
            </div>
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
        state.h2h.b = state.config.players.find((player) => player !== state.h2h.a) || state.h2h.b;
    }

    const modes = [
        { key: "performance", label: "Performance H2H" },
        { key: "ratings", label: "Ratings H2H" },
    ];
    if (!modes.some((mode) => mode.key === state.h2h.mode)) {
        state.h2h.mode = "performance";
    }

    const rows = buildH2hRows();
    const agreement = rows.filter((god) => god.absDiff <= 5).length;
    const aHigher = rows.filter((god) => god.diff > 5).length;
    const bHigher = rows.filter((god) => god.diff < -5).length;
    const averageDiff = rows.length ? Math.round((rows.reduce((sum, god) => sum + god.diff, 0) / rows.length) * 10) / 10 : 0;
    const topDiff = [...rows].sort((a, b) => b.absDiff - a.absDiff).slice(0, 12);
    const agreed = rows
        .filter((god) => god[state.h2h.a] >= 80 && god[state.h2h.b] >= 80 && god.absDiff <= 10)
        .sort((a, b) => ((b[state.h2h.a] + b[state.h2h.b]) / 2) - ((a[state.h2h.a] + a[state.h2h.b]) / 2));
    const roleLean = strongestRoleLean(rows);
    const verdict = averageDiff > 2
        ? `${state.h2h.a} trends higher overall`
        : averageDiff < -2
            ? `${state.h2h.b} trends higher overall`
            : "These two are surprisingly close overall";

    const profileA = buildRaterProfile(state.h2h.a);
    const profileB = buildRaterProfile(state.h2h.b);
    const performanceRows = state.gods
        .map((god) => {
            const aStat = profileA.godStats?.[god.God] || null;
            const bStat = profileB.godStats?.[god.God] || null;
            const aGames = Number(aStat?.gamesPlayed || 0);
            const bGames = Number(bStat?.gamesPlayed || 0);
            if (!aGames || !bGames) return null;
            const aWr = Number(aStat?.winRate || 0);
            const bWr = Number(bStat?.winRate || 0);
            return { god, aStat, bStat, aGames, bGames, aWr, bWr, wrDiff: aWr - bWr, totalGames: aGames + bGames };
        })
        .filter(Boolean)
        .sort((left, right) => Math.abs(right.wrDiff) - Math.abs(left.wrDiff) || right.totalGames - left.totalGames)
        .slice(0, 16);

    const performanceContent = `
        <section class="h2h-subtab-panel">
            <div class="verdict-banner">
                <div><p class="eyebrow">Performance Verdict</p><h3>Stored Joust records by god</h3></div>
                <div class="verdict-chip-row"><span class="summary-pill">${performanceRows.length} shared-play gods</span></div>
            </div>
            <div class="h2h-performance-grid">
                ${performanceRows.length ? performanceRows.map((row) => {
                    const aRecord = row.aGames ? `${formatWinLossRecord(row.aStat.wins, row.aGames)} | ${formatMetric(row.aWr, 1, '%')} WR | ${row.aGames} games` : 'No stored games';
                    const bRecord = row.bGames ? `${formatWinLossRecord(row.bStat.wins, row.bGames)} | ${formatMetric(row.bWr, 1, '%')} WR | ${row.bGames} games` : 'No stored games';
                    const leader = row.aWr === row.bWr ? 'Even' : row.aWr > row.bWr ? state.h2h.a : state.h2h.b;
                    return `
                        <article class="h2h-performance-card" data-god-detail="${escapeHtml(row.god.God)}" role="button" tabindex="0">
                            <div class="h2h-performance-art">${row.god.ImageUrl ? `<img class="god-art" src="${row.god.ImageUrl}" alt="${escapeHtml(row.god.God)}">` : `<div class="image-fallback">Art</div>`}</div>
                            <div class="h2h-performance-body">
                                <div class="section-kicker"><h3>${escapeHtml(row.god.God)}</h3><span class="summary-pill">${leader === 'Even' ? 'Even' : `${escapeHtml(leader)} leads`}</span></div>
                                <div class="mini-highlight-row"><span><strong style="color:${playerColor(state.h2h.a)}">${escapeHtml(state.h2h.a)}</strong><small>${aRecord}</small></span><strong>${row.aGames ? formatMetric(row.aWr, 1, '%') : '--'}</strong></div>
                                <div class="mini-highlight-row"><span><strong style="color:${playerColor(state.h2h.b)}">${escapeHtml(state.h2h.b)}</strong><small>${bRecord}</small></span><strong>${row.bGames ? formatMetric(row.bWr, 1, '%') : '--'}</strong></div>
                            </div>
                        </article>
                    `;
                }).join('') : emptyState('No Performance Overlap', 'Stored Joust god stats are not available for this pairing yet.')}
            </div>
        </section>
    `;

    const ratingsContent = `
        <section class="h2h-subtab-panel">
            <div class="verdict-banner">
                <div><p class="eyebrow">Match Verdict</p><h3>${escapeHtml(verdict)}</h3></div>
                <div class="verdict-chip-row"><span class="summary-pill">Avg delta ${averageDiff > 0 ? "+" : ""}${averageDiff}</span>${roleLean ? `<span class="summary-pill ${roleLean.averageDiff > 0 ? "cool" : "warm"}">${escapeHtml(roleLean.role)} leans ${roleLean.averageDiff > 0 ? escapeHtml(state.h2h.a) : escapeHtml(state.h2h.b)}</span>` : ""}</div>
            </div>
            <div class="metrics-grid" style="margin-top:18px;"><article class="metric-card"><div class="metric-label">${escapeHtml(state.h2h.a)} rates higher</div><div class="metric-value">${aHigher}</div></article><article class="metric-card"><div class="metric-label">Agreement (+/-5)</div><div class="metric-value">${agreement}</div></article><article class="metric-card"><div class="metric-label">${escapeHtml(state.h2h.b)} rates higher</div><div class="metric-value">${bHigher}</div></article></div>
            <div class="panel-heading" style="margin-top:22px;"><p class="eyebrow">Biggest Splits</p><h2>Disagreements</h2></div>
            <div class="feature-grid-4">${topDiff.map((god) => { const winner = god.diff > 0 ? state.h2h.a : state.h2h.b; return h2hCard(god, `<span style="color:${playerColor(winner)}">+${god.absDiff} pts</span> | ${escapeHtml(winner)} higher`); }).join("") || emptyState("No Overlap", "These players do not share enough rated gods yet.")}</div>
            <div class="panel-heading" style="margin-top:22px;"><p class="eyebrow">Shared Love</p><h2>Agreed Upon Gods</h2></div>
            <div class="h2h-grid">${agreed.map((god) => { const avg = Math.round((god[state.h2h.a] + god[state.h2h.b]) / 2); return h2hCard(god, `<span style="color:var(--green)">AVG ${avg}</span> | D${god.absDiff}`); }).join("") || emptyState("No Agreed Gods", "No high-score agreements match the current comparison.")}</div>
        </section>
    `;

    elements.tabH2h.innerHTML = `
        <div class="panel tab-overview-panel">
            <div class="panel-heading panel-heading-inline"><div><p class="eyebrow">Cross-Council Compare</p><h2>Head To Head</h2></div><span class="summary-pill">Performance or ratings</span></div>
            <div class="h2h-controls"><label class="field"><span>Council Member A</span><select id="h2h-player-a">${options}</select></label><label class="field"><span>Council Member B</span><select id="h2h-player-b">${options}</select></label></div>
            <div class="subtab-bar h2h-mode-tabs" role="tablist" aria-label="Head to head mode">${modes.map((mode) => `<button class="subtab-btn ${state.h2h.mode === mode.key ? 'active' : ''}" type="button" data-h2h-mode="${mode.key}" role="tab" aria-selected="${state.h2h.mode === mode.key ? 'true' : 'false'}">${escapeHtml(mode.label)}</button>`).join('')}</div>
            ${state.h2h.mode === 'performance' ? performanceContent : ratingsContent}
            ${renderBackToTop()}
        </div>
    `;

    document.getElementById("h2h-player-a").value = state.h2h.a;
    document.getElementById("h2h-player-b").value = state.h2h.b;
    document.getElementById("h2h-player-a")?.addEventListener("change", (event) => { state.h2h.a = event.target.value; renderH2hTab(); });
    document.getElementById("h2h-player-b")?.addEventListener("change", (event) => { state.h2h.b = event.target.value; renderH2hTab(); });
}

// This helper renders the recent activity feed and its client-side filters.
function renderActivityPanel() {
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

    return `
        <div class="activity-embedded-panel">
            <div class="panel-heading panel-heading-inline">
                <div><p class="eyebrow">Council Log</p><h2>Recent Activity</h2></div>
                <span class="summary-pill">Ratings + rank receipts</span>
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
        </div>
    `;
}

function bindActivityControls() {
    document.getElementById("activity-player").value = state.activity.player;
    document.getElementById("activity-type").value = state.activity.type;
    document.getElementById("activity-player")?.addEventListener("change", (event) => {
        state.activity.player = event.target.value;
        renderRankerTab();
    });
    document.getElementById("activity-type")?.addEventListener("change", (event) => {
        state.activity.type = event.target.value;
        renderRankerTab();
    });
}

function renderActivityTab() {
    if (!elements.tabActivity) return;
    elements.tabActivity.innerHTML = `<div class="panel">${renderActivityPanel()}${renderBackToTop()}</div>`;
    bindActivityControls();
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

// This helper renders only the ranker row list so search can narrow results as
// the user types without recreating the search input and stealing focus.
function renderRankerRowsHtml(player, playerRows) {
    const playerState = state.ranker.byPlayer[player];
    return playerRows.map((row) => {
        const god = state.gods.find((item) => item.God === row.god);
        const value = Number(playerState.ratings[row.god] || 0);
        const rankLabel = row.placed ? `#${row.rank}` : "-";
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
                        <div class="ranker-submeta">${escapeHtml(god?.Tier || "U")} tier - ${escapeHtml(god?.Role || "Unknown")}</div>
                    </div>
                </div>
                <div class="ranker-score">
                    <input class="ranker-score-input" data-god="${escapeHtml(row.god)}" type="number" min="0" max="100" value="${value}">
                </div>
                <div class="ranker-buttons">
                    <button class="mini-btn ${disabledUp ? "" : "primary"} ranker-move-up" data-god="${escapeHtml(row.god)}" ${disabledUp ? "disabled" : ""}>Up</button>
                    <button class="mini-btn ${disabledDown ? "" : "primary"} ranker-move-down" data-god="${escapeHtml(row.god)}" ${disabledDown ? "disabled" : ""}>Down</button>
                </div>
            </article>
        `;
    }).join("");
}

// This helper rebinds controls inside the ranker rows after a partial list
// refresh from live search.
function bindRankerRowEvents(player) {
    document.querySelectorAll(".ranker-score-input").forEach((input) => {
        input.addEventListener("change", () => updateRating(player, input.dataset.god, input.value));
        input.addEventListener("keydown", (event) => {
            if (event.key === "Enter") {
                event.preventDefault();
                const inputs = [...document.querySelectorAll(".ranker-score-input")];
                const next = inputs[inputs.indexOf(event.currentTarget) + 1];
                next?.focus();
                next?.select();
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

// This helper updates live-search dependent ranker fragments without remounting
// the search input.
function refreshRankerListOnly() {
    const player = state.ranker.selectedPlayer;
    const rows = buildRankerRows(player);
    const list = document.getElementById("ranker-list");
    if (list) {
        list.innerHTML = renderRankerRowsHtml(player, rows);
        bindRankerRowEvents(player);
    }
    const visible = document.getElementById("ranker-visible-count");
    if (visible) {
        visible.textContent = `${rows.length} visible`;
    }
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

        const warningLine = payload.historyWarning ? `\nHistory warning: ${payload.historyWarning}` : "";
        alert(`${payload.message}\nRating changes: ${payload.ratingChanges}\nRank changes: ${payload.rankChanges}${warningLine}`);
        state.ranker.lastSavedByPlayer[player] = new Date().toISOString();
        state.ranker.baselineByPlayer[player] = buildRankerSignature(playerState);
        clearRankerDraft(player);
        refreshDirtyState(player);
        await refreshData();
    } catch (error) {
        alert(error.message);
    }
}

// This helper discards the selected player's local draft and reloads the latest
// Supabase-backed ratings/ranks instead of clearing the whole roster.
function resetRanker() {
    const player = state.ranker.selectedPlayer;
    const confirmed = window.confirm(`Discard local ${player} draft and reload saved Supabase ratings?`);
    if (!confirmed) return;
    discardRankerDraft(player);
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

    const listRows = renderRankerRowsHtml(player, playerRows);
    const draftMeta = state.ranker.draftMetaByPlayer[player];
    const draftWarning = draftMeta ? `
        <div class="status-banner ranker-draft-banner">
            <div>
                <strong>Local draft restored</strong>
                <span>This device has unsaved ${escapeHtml(player)} edits from ${escapeHtml(formatDateTime(draftMeta.savedAt))}. Save them or discard the draft to use Supabase values.</span>
            </div>
            <button class="btn-secondary" type="button" data-discard-ranker-draft="${escapeHtml(player)}">Discard Draft</button>
        </div>
    ` : "";

    const rankerSections = [
        { key: "editor", label: "Editor" },
        { key: "activity", label: "Activity" },
    ];
    if (!rankerSections.some((section) => section.key === state.ranker.section)) {
        state.ranker.section = "editor";
    }
    const editorPanel = `
        <div class="ranker-editor-panel">
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
                <span class="summary-pill" id="ranker-visible-count">${playerRows.length} visible</span>
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
                ${draftWarning}
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
                <div class="ranker-list" id="ranker-list">${listRows}</div>
                <div class="sticky-ranker-save">
                    <button class="btn-primary ranker-save-trigger sticky-save-btn" type="button">${dirty ? "Save Changes" : "Saved"}</button>
                </div>
                ${renderBackToTop()}
            ` : lockedBlock}
        </div>
    </div>`;

    elements.tabRanker.innerHTML = `
        <div class="subtab-shell ranker-subtab-shell">
            <div class="subtab-bar ranker-section-tabs" role="tablist" aria-label="Rate and Rank sections">
                ${rankerSections.map((section) => `<button class="subtab-btn ${state.ranker.section === section.key ? "active" : ""}" type="button" data-ranker-section="${section.key}" role="tab" aria-selected="${state.ranker.section === section.key ? "true" : "false"}">${section.label}</button>`).join("")}
            </div>
            <div class="subtab-content">
                ${state.ranker.section === "activity" ? `<div class="panel">${renderActivityPanel()}${renderBackToTop()}</div>` : editorPanel}
            </div>
        </div>
    `;

    if (state.ranker.section === "activity") {
        bindActivityControls();
        return;
    }

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
        refreshRankerListOnly();
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

    bindRankerRowEvents(player);
}



// Council Scroll recap rendering lives in static/council-scroll.js.

function formatHealthDate(value) {
    if (!value) return "—";
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return "—";
    return parsed.toLocaleDateString(undefined, { month: "short", day: "2-digit", year: "numeric" });
}

// This helper renders small rows for issue samples so the health panel points
// toward the exact data that needs attention without becoming a raw database dump.
function renderHealthSamples(samples = []) {
    if (!samples.length) return "";
    const rows = samples.slice(0, 6).map((sample) => {
        if (typeof sample === "string") return `<span class="health-sample-chip">${escapeHtml(sample)}</span>`;
        const god = sample.god || sample.god_name || sample.godName || sample.player || "Sample";
        const detail = sample.status || sample.asset || sample.match_key || sample.record_key || sample.remote || "Review row";
        return `<span class="health-sample-chip"><strong>${escapeHtml(god)}</strong> ${escapeHtml(detail)}</span>`;
    }).join("");
    return `<div class="health-samples">${rows}</div>`;
}

// This helper renders the Data Health admin panel. It is intentionally read-only:
// the goal is to surface suspicious data before we trust profile/chemistry stats.
function renderDataHealthTab() {
    if (!elements.tabDataHealth) return;

    if (!state.dataHealth.loaded || state.dataHealth.loading) {
        elements.tabDataHealth.innerHTML = `
            <div class="panel data-health-panel">
                <div class="panel-heading panel-heading-inline">
                    <div><p class="eyebrow">Admin Chamber</p><h2>Data Health</h2></div>
                    <span class="summary-pill">Inspecting Supabase</span>
                </div>
                ${emptyState("Checking The Ledgers", "Reading roster, ratings, rankings, activity, match history, and asset health.")}
            </div>
        `;
        return;
    }

    if (state.dataHealth.error) {
        elements.tabDataHealth.innerHTML = `
            <div class="panel data-health-panel">
                <div class="panel-heading panel-heading-inline">
                    <div><p class="eyebrow">Admin Chamber</p><h2>Data Health</h2></div>
                    <button class="btn-secondary" type="button" data-refresh-health="true">Retry</button>
                </div>
                ${emptyState("Health Check Failed", state.dataHealth.error)}
            </div>
        `;
        return;
    }

    const report = state.dataHealth.report || {};
    const overview = report.overview || {};
    const assets = report.assets || { counts: {}, attention: [] };
    const integrity = report.chemistryIntegrity || { sessionSizes: {} };
    const issues = report.issues || [];
    const criticalCount = issues.filter((issue) => issue.severity === "danger").length;
    const warningCount = issues.filter((issue) => issue.severity === "warn").length;
    const healthTone = criticalCount ? "danger" : warningCount ? "warn" : "cool";
    const healthLabel = criticalCount ? "Needs attention" : warningCount ? "Watch list" : "Clean";

    const overviewCards = [
        ["Roster", overview.rosterGods, "gods in metadata"],
        ["Joust Rows", overview.joustRows, "stored match rows"],
        ["Sessions", overview.uniqueJoustSessions, "grouped by match key"],
        ["Newest", formatHealthDate(overview.newestJoustMatch), "latest stored Joust"],
        ["Oldest", formatHealthDate(overview.oldestJoustMatch), "earliest stored Joust"],
        ["Activity", overview.activityRows, "rating/rank receipts"],
    ].map(([label, value, sub]) => `
        <article class="health-metric-card">
            <span>${escapeHtml(label)}</span>
            <strong>${escapeHtml(formatMetric(value))}</strong>
            <small>${escapeHtml(sub)}</small>
        </article>
    `).join("");

    const issueRows = issues.length ? issues.map((issue) => `
        <article class="health-issue-card ${escapeHtml(issue.severity)}">
            <div>
                <p class="eyebrow">${escapeHtml(issue.severity)}</p>
                <h3>${escapeHtml(issue.title)}</h3>
                <p>${escapeHtml(issue.message)}</p>
                ${renderHealthSamples(issue.samples || [])}
            </div>
            <strong>${escapeHtml(formatMetric(issue.count))}</strong>
        </article>
    `).join("") : emptyState("No Issues Flagged", "The current checks did not find missing assets, duplicate rows, incomplete rows, or roster mismatches.");

    const coverageRows = (report.coverage || []).map((row) => `
        <article class="health-coverage-card ${escapeHtml(row.status || "")}">
            <div class="section-kicker"><h3>${escapeHtml(row.player)}</h3><span class="summary-pill">${escapeHtml(row.status || "unknown")}</span></div>
            <div class="health-card-grid compact">
                <span><strong>${escapeHtml(formatMetric(row.joustRows))}</strong><small>Joust rows</small></span>
                <span><strong>${escapeHtml(formatHealthDate(row.newestMatch))}</strong><small>Newest</small></span>
                <span><strong>${escapeHtml(row.mostPlayed?.god || "—")}</strong><small>Most played ${row.mostPlayed ? `${row.mostPlayed.games}-${row.mostPlayed.wins}` : ""}</small></span>
                <span><strong>${escapeHtml(row.bestGod?.god || "—")}</strong><small>Best sample ${row.bestGod ? `${row.bestGod.wins}-${row.bestGod.games - row.bestGod.wins} • ${formatMetric(row.bestGod.winRate, 1, "%")}` : ""}</small></span>
            </div>
        </article>
    `).join("");

    const assetRows = (assets.attention || []).map((row) => `
        <tr>
            <td>${escapeHtml(row.god)}</td>
            <td>${escapeHtml(row.pantheon || "—")}</td>
            <td><span class="health-status-chip ${escapeHtml(row.status)}">${escapeHtml(row.status)}</span></td>
            <td>${escapeHtml(row.asset || row.remote || "—")}</td>
        </tr>
    `).join("");

    elements.tabDataHealth.innerHTML = `
        <div class="panel data-health-panel">
            <div class="panel-heading panel-heading-inline">
                <div>
                    <p class="eyebrow">Admin Chamber</p>
                    <h2>Data Health</h2>
                    <p>Read-only checks for Supabase rows, image assets, Joust coverage, and chemistry fanout.</p>
                </div>
                <div class="health-actions">
                    <span class="summary-pill ${healthTone}">${healthLabel}</span>
                    <button class="btn-secondary" type="button" data-refresh-health="true">Refresh Health</button>
                </div>
            </div>

            <section class="health-metric-grid">${overviewCards}</section>

            <section class="health-section-grid">
                <article class="detail-card-v2">
                    <div class="section-head"><div><p class="eyebrow">Integrity Watch</p><h3>What Needs Attention</h3></div><span class="summary-pill">${issues.length} checks</span></div>
                    <div class="health-issue-list">${issueRows}</div>
                </article>
                <article class="detail-card-v2">
                    <div class="section-head"><div><p class="eyebrow">Chemistry Fanout</p><h3>Stored Session Shape</h3></div><span class="summary-pill">Joust only</span></div>
                    <div class="health-card-grid">
                        <span><strong>${escapeHtml(formatMetric(integrity.sessionSizes?.solo))}</strong><small>Solo sessions</small></span>
                        <span><strong>${escapeHtml(formatMetric(integrity.sessionSizes?.duo))}</strong><small>Duo sessions</small></span>
                        <span><strong>${escapeHtml(formatMetric(integrity.sessionSizes?.trioPlus))}</strong><small>Trio+ sessions</small></span>
                        <span><strong>${escapeHtml(formatMetric(integrity.duplicateMatchPlayerRows))}</strong><small>Duplicate player/match rows</small></span>
                    </div>
                </article>
            </section>

            <section class="detail-card-v2">
                <div class="section-head"><div><p class="eyebrow">Council Coverage</p><h3>Player Data Depth</h3></div><span class="summary-pill">Joust rows only</span></div>
                <div class="health-coverage-grid">${coverageRows}</div>
            </section>

            <section class="detail-card-v2">
                <div class="section-head"><div><p class="eyebrow">Image Assets</p><h3>Artwork Audit</h3></div><span class="summary-pill">${escapeHtml(formatMetric(assets.counts?.local))} local / ${escapeHtml(formatMetric(assets.counts?.remoteFallback || assets.counts?.["remote-fallback"]))} fallback</span></div>
                <div class="health-asset-summary">
                    <span><strong>${escapeHtml(formatMetric(assets.counts?.local))}</strong><small>Local</small></span>
                    <span><strong>${escapeHtml(formatMetric(assets.counts?.placeholder))}</strong><small>Placeholder</small></span>
                    <span><strong>${escapeHtml(formatMetric(assets.counts?.["remote-fallback"]))}</strong><small>Remote fallback</small></span>
                    <span><strong>${escapeHtml(formatMetric(assets.counts?.missing))}</strong><small>Missing</small></span>
                </div>
                <div class="detail-table-wrap">
                    <table class="compact-table health-table">
                        <thead><tr><th>God</th><th>Pantheon</th><th>Status</th><th>Asset / Fallback</th></tr></thead>
                        <tbody>${assetRows || `<tr><td colspan="4">All gods have local production artwork.</td></tr>`}</tbody>
                    </table>
                </div>
            </section>
        </div>
    `;
}
// This helper keeps the tab strip, visible panel, and contextual filter bar
// in sync with state.activeTab so filters only appear where they affect results.
function renderTabs() {
    const visibleTabs = new Set(["index", "items", "rater-stats", "chemistry", "analytics", "h2h", "council-scroll", "data-health", "ranker"]);
    if (!visibleTabs.has(state.activeTab)) {
        state.activeTab = "index";
    }
    document.body.dataset.activeTab = state.activeTab;
    const filterTabs = new Set(["index"]);

    if (elements.filtersDetails) {
        elements.filtersDetails.classList.toggle("hidden", !filterTabs.has(state.activeTab));
        elements.filtersDetails.setAttribute("aria-hidden", filterTabs.has(state.activeTab) ? "false" : "true");
    }

    elements.tabButtons.forEach((button) => {
        button.classList.toggle("active", button.dataset.tab === state.activeTab);
        const isRanker = button.dataset.tab === "ranker";
        const label = state.ui.isMobile ? (button.dataset.mobileLabel || button.dataset.fullLabel) : button.dataset.fullLabel;
        button.textContent = isRanker && state.ranker.dirtyPlayers[state.ranker.selectedPlayer] ? `${label} *` : label;
    });
    elements.tabPanels.forEach((panel) => {
        panel.classList.toggle("active", panel.id === `tab-${state.activeTab}`);
    });

    if (state.activeTab === "data-health" && !state.dataHealth.loaded && !state.dataHealth.loading) {
        renderDataHealthTab();
        loadDataHealth().then(() => renderDataHealthTab());
    }
}

// This helper refreshes every view after data or filters change.
function renderAll() {
    applyFilters();
    renderHeroStats();
    renderStatusBanner();
    renderPodium();
    renderSidebar();
    renderIndexTab();
    renderItemsTab();
    renderRankingsTab();
    renderFavoritesTab();
    renderRaterStatsTab();
    renderChemistryTab();
    renderTierlistTab();
    renderAnalyticsTab();
    renderH2hTab();
    renderCouncilScrollTab();
    renderDataHealthTab();
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


// This helper handles email deep links such as
// `?god=Skadi&section=edit&player=Joey` by opening the god card directly to
// Rate & Rank after the initial roster has loaded.
function handleInitialGodDeepLink() {
    const params = new URLSearchParams(window.location.search);
    const requestedGod = params.get("god") || params.get("openGod");
    if (!requestedGod) return false;

    const god = findGodByName(requestedGod) || state.gods.find((row) => canonicalGodKey(row.God) === canonicalGodKey(requestedGod));
    if (!god) return false;

    const requestedPlayer = params.get("player") || "";
    const requestedSection = params.get("section") || "council";
    const sectionAliases = {
        rate: "edit",
        rank: "edit",
        rating: "edit",
        ratings: "edit",
        edit: "edit",
        rateandrank: "edit",
    };
    const sectionKey = sectionAliases[normalizeGodName(requestedSection)] || requestedSection;
    const validSections = ["council", "ownership", "performance", "synergy", "recent", "edit"];

    state.activeTab = "index";
    state.godDetail.god = god.God;
    state.godDetail.section = validSections.includes(sectionKey) ? sectionKey : "council";
    if (state.config.players.includes(requestedPlayer)) {
        state.godDetail.editPlayer = requestedPlayer;
    }

    openGodDetail(god.God);
    renderTabs();
    setTimeout(() => elements.godModalBackdrop?.scrollTo?.({ top: 0 }), 0);
    return true;
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
        clearRaterStatsCache();
        renderAll();
        handleInitialGodDeepLink();
        maybeLoadHeavyTabData();
    } catch (error) {
        document.querySelector(".app-shell").innerHTML = emptyState("App Failed To Load", error.message);
    }
});


































