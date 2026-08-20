// Item catalog, item taxonomy filters, and item detail modal rendering.
// Loaded after app.js; functions are available before DOMContentLoaded fires.

function buildCouncilItemCatalog() {
    const itemMap = new Map();

    const addItem = ({ item, player, godName, category }) => {
        if (!item || !item.name) return;
        const key = `${category}|${item.name}`;
        const games = Number(item.games || 0);
        const wins = Number(item.wins || 0);
        if (!games) return;

        const record = itemMap.get(key) || {
            name: item.name,
            category,
            games: 0,
            wins: 0,
            gods: new Map(),
            players: new Map(),
        };

        record.games += games;
        record.wins += wins;
        if (!record.imageUrl && item.imageUrl) record.imageUrl = item.imageUrl;

        const godRecord = record.gods.get(godName) || { name: godName, games: 0, wins: 0 };
        godRecord.games += games;
        godRecord.wins += wins;
        record.gods.set(godName, godRecord);

        const playerRecord = record.players.get(player) || { name: player, games: 0, wins: 0 };
        playerRecord.games += games;
        playerRecord.wins += wins;
        record.players.set(player, playerRecord);

        itemMap.set(key, record);
    };

    (state.config?.players || []).forEach((player) => {
        const profile = buildRaterProfile(player);
        Object.entries(profile.buildStats || {}).forEach(([godName, stats]) => {
            (stats.starterItems || []).forEach((item) => addItem({ item, player, godName, category: "Starter" }));
            (stats.topItems || []).forEach((item) => addItem({ item, player, godName, category: item.category || "Tier 3" }));
        });
    });

    const finish = (record) => {
        const gods = [...record.gods.values()].map((god) => ({
            ...god,
            winRate: god.games ? god.wins / god.games * 100 : 0,
        }));
        const players = [...record.players.values()].map((player) => ({
            ...player,
            winRate: player.games ? player.wins / player.games * 100 : 0,
        }));
        const bestGod = [...gods]
            .filter((god) => god.games >= 2)
            .sort((a, b) => b.winRate - a.winRate || b.games - a.games || a.name.localeCompare(b.name))[0] || [...gods].sort((a, b) => b.winRate - a.winRate || b.games - a.games || a.name.localeCompare(b.name))[0];
        const mostUsedGod = [...gods].sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.name.localeCompare(b.name))[0];
        const topPlayer = [...players].sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.name.localeCompare(b.name))[0];
        return {
            ...record,
            gods,
            players,
            bestGod,
            mostUsedGod,
            topPlayer,
            losses: Math.max(record.games - record.wins, 0),
            winRate: record.games ? record.wins / record.games * 100 : 0,
        };
    };

    return [...itemMap.values()].map(finish);
}

// This helper joins council usage data with the optional local item metadata
// snapshot so the Items tab can behave like a browsable armory catalog.
function itemMetadataScore(row) {
    if (!row) return 0;
    return [row.imageUrl, row.summary, row.passive, ...(Array.isArray(row.stats) ? row.stats : [])]
        .filter(Boolean)
        .length;
}

function itemCatalogKey(value) {
    return String(value || "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

function itemTaxonomyMap() {
    const rows = state.itemTaxonomy?.items || {};
    const map = new Map();
    Object.values(rows).forEach((row) => {
        const key = itemCatalogKey(row.name || "");
        if (key) map.set(key, row);
        if (row.aliasOf) {
            const aliasKey = itemCatalogKey(row.aliasOf || "");
            if (aliasKey && !map.has(aliasKey)) map.set(aliasKey, row);
        }
    });
    return map;
}

function itemTaxonomyFor(item) {
    const taxonomy = itemTaxonomyMap();
    return taxonomy.get(itemCatalogKey(itemDisplayName(item))) || taxonomy.get(itemCatalogKey(item?.name || "")) || null;
}

function normalizedItemCategory(value) {
    const raw = String(value || "").trim();
    const key = raw.toLowerCase();
    if (["starter", "starters"].includes(key)) return "Starter";
    if (["tier 3", "t3", "final item", "final items"].includes(key)) return "Tier 3";
    if (["tier 2", "t2"].includes(key)) return "Tier 2";
    if (["tier 1", "t1"].includes(key)) return "Tier 1";
    if (["items", "item", "item-passive", "item-active"].includes(key)) return "Tier 3";
    return raw || "Catalog";
}

function itemMetadataMap() {
    const rows = Array.isArray(state.itemMetadata) ? state.itemMetadata : [];
    const map = new Map();
    rows.forEach((row) => {
        const key = itemCatalogKey(row.name || row.displayName || "");
        if (!key) return;
        const existing = map.get(key);
        if (!existing || itemMetadataScore(row) >= itemMetadataScore(existing)) {
            map.set(key, row);
        }
    });
    return map;
}

function enrichedItemCatalog() {
    const metadata = itemMetadataMap();
    const usageRows = buildCouncilItemCatalog().map((item) => ({
        ...item,
        metadata: metadata.get(itemCatalogKey(item.name || "")) || {},
    }));
    const seen = new Set(usageRows.map((item) => itemCatalogKey(item.name || "")));
    const metadataOnlyRows = [...metadata.values()]
        .filter((row) => {
            const name = String(row.name || row.displayName || "");
            return name && !seen.has(itemCatalogKey(name));
        })
        .map((row) => ({
            name: row.name || row.displayName,
            category: normalizedItemCategory(row.itemType || (Array.isArray(row.categoriesSeen) && row.categoriesSeen[0]) || "Catalog"),
            games: 0,
            wins: 0,
            losses: 0,
            winRate: 0,
            imageUrl: row.imageUrl || "",
            gods: [],
            players: [],
            bestGod: null,
            mostUsedGod: null,
            topPlayer: null,
            metadata: row,
        }));
    return [...usageRows, ...metadataOnlyRows];
}

function statChipList(stats, limit = 5) {
    const rows = Array.isArray(stats) ? stats.filter(Boolean).slice(0, limit) : [];
    return rows.length ? rows.map((stat) => `<span class="item-stat-chip">${escapeHtml(stat)}</span>`).join("") : `<span class="item-stat-chip muted">Stats pending</span>`;
}

function itemInlineStats(stats, limit = 4) {
    const rows = Array.isArray(stats) ? stats.filter(Boolean).slice(0, limit) : [];
    return rows.length ? rows.map((stat) => `<span>${escapeHtml(stat)}</span>`).join("") : `<span class="muted">Stats pending</span>`;
}


const ITEM_FILTER_GROUPS = [
    { key: "beginner", label: "Beginner Type", options: ["Offense", "Defense", "Hybrid", "Utility", "Starter"] },
    { key: "purpose", label: "Purpose", options: ["Anti-Tank", "Anti-Heal", "Anti-Shield", "Cooldown", "Sustain", "Mobility", "Crowd Control", "Teamfight", "Economy"] },
    { key: "antiTank", label: "Anti-Tank Tools", options: ["Anti-Health", "Anti-Protections", "% Health Damage", "% Penetration", "Flat Penetration", "Protection Reduction"] },
    { key: "damage", label: "Damage Shape", options: ["Physical Damage", "Magical Damage", "Basic Attack", "Ability Damage", "Attack Speed", "Crit", "Burst"] },
    { key: "defense", label: "Defense Shape", options: ["Physical Protection", "Magical Protection", "Health", "Mitigation", "Shielding", "Anti-Burst"] },
    { key: "utility", label: "Utility + Sustain", options: ["Aura", "Tenacity", "Slow", "Mana", "Lifesteal", "Healing", "Movement Speed"] },
    { key: "tier", label: "Tier", options: ["Starter", "Tier 3", "Tier 2", "Tier 1"] },
];

const ITEM_TAG_OVERRIDES = {
    "sanguine lash": ["Hybrid", "Anti-Tank", "Anti-Health", "% Health Damage", "Bruiser", "Lifesteal", "Sustain", "Physical Damage"],
    "eros bow": ["Utility", "Healing", "Sustain", "Basic Attack"],
    "eros' bow": ["Utility", "Healing", "Sustain", "Basic Attack"],
    "erosbow": ["Utility", "Healing", "Sustain", "Basic Attack"],
    "eros's bow": ["Utility", "Healing", "Sustain", "Basic Attack"],
    "lernaean bow": ["Offense", "Anti-Shield", "Physical Damage", "Basic Attack", "Attack Speed"],
    "erosion": ["Defense", "Anti-Shield", "Physical Protection", "Magical Protection", "Health", "Anti-Burst"],
    "mystical mail": ["Defense", "Anti-Shield", "Physical Protection", "Health", "Aura", "Teamfight"],
    "pharaoh's curse": ["Defense", "Anti-Shield", "Magical Protection", "Health", "Debuff", "Teamfight"],
    "void shield": ["Hybrid", "Anti-Tank", "Anti-Protections", "Protection Reduction", "Physical Protection", "Physical Damage", "Bruiser", "Aura"],
    "titan's bane": ["Offense", "Anti-Tank", "Anti-Protections", "% Penetration", "Physical Damage", "Burst"],
    "the executioner": ["Offense", "Anti-Tank", "Anti-Protections", "Protection Reduction", "Basic Attack", "Attack Speed", "Physical Damage"],
    "executioner": ["Offense", "Anti-Tank", "Anti-Protections", "Protection Reduction", "Basic Attack", "Attack Speed", "Physical Damage"],
    "soul reaver": ["Offense", "Anti-Tank", "Anti-Health", "% Health Damage", "Magical Damage", "Ability Damage"],
    "qin's blade": ["Offense", "Anti-Tank", "Anti-Health", "% Health Damage", "Basic Attack", "Attack Speed", "Physical Damage"],
    "divine ruin": ["Offense", "Anti-Heal", "Magical Damage", "Ability Damage", "Flat Penetration"],
    "brawler's beat stick": ["Offense", "Anti-Heal", "Physical Damage", "Ability Damage", "Flat Penetration"],
    "pestilence": ["Defense", "Anti-Heal", "Magical Protection", "Health", "Aura", "Teamfight"],
    "contagion": ["Defense", "Anti-Heal", "Physical Protection", "Health", "Aura", "Teamfight"],
    "shogun's kusari": ["Hybrid", "Magical Protection", "Attack Speed", "Aura", "Teamfight", "Basic Attack"],
    "stone of binding": ["Hybrid", "Anti-Tank", "Anti-Protections", "Protection Reduction", "Crowd Control", "Aura", "Teamfight"],
    "gem of isolation": ["Utility", "Slow", "Crowd Control", "Magical Damage", "Ability Damage"],
    "breastplate of valor": ["Defense", "Physical Protection", "Cooldown", "Mana", "Anti-Burst"],
    "genji's guard": ["Defense", "Magical Protection", "Cooldown", "Mana", "Anti-Burst"],
    "rod of tahuti": ["Offense", "Magical Damage", "Burst", "Ability Damage"],
    "spear of desolation": ["Offense", "Magical Damage", "Flat Penetration", "Cooldown", "Burst", "Ability Damage"],
    "demon blade": ["Offense", "Crit", "Attack Speed", "Basic Attack", "Physical Damage"],
    "deathbringer": ["Offense", "Crit", "Basic Attack", "Physical Damage", "Burst"],
};

function uniqueItemTags(tags) {
    const cleaned = tags
        .filter(Boolean)
        .map((tag) => String(tag).replace(/\s+/g, " ").trim())
        .filter(Boolean);
    return cleaned.filter((tag, index) => cleaned.findIndex((other) => other.toLowerCase() === tag.toLowerCase()) === index);
}

function itemTextBlob(item) {
    const meta = item?.metadata || {};
    return [item?.name, itemDisplayName(item), item?.category, meta.itemType, meta.summary, meta.passive, ...(Array.isArray(meta.tags) ? meta.tags : []), ...(Array.isArray(meta.stats) ? meta.stats : [])]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
}

function derivedItemTags(item) {
    const meta = item?.metadata || {};
    const text = itemTextBlob(item);
    const statsText = Array.isArray(meta.stats) ? meta.stats.join(" ").toLowerCase() : "";
    const passiveText = String(meta.passive || "").toLowerCase();
    const tags = [item?.category, ...(Array.isArray(meta.tags) ? meta.tags : [])];
    const has = (pattern) => pattern.test(text);
    const hasStat = (pattern) => pattern.test(statsText);
    const hasPassive = (pattern) => pattern.test(passiveText);

    if (String(item?.category || "").toLowerCase() === "starter") tags.push("Starter");
    if (/tier\s*3/i.test(String(item?.category || ""))) tags.push("Tier 3");
    if (/tier\s*2/i.test(String(item?.category || ""))) tags.push("Tier 2");
    if (/tier\s*1/i.test(String(item?.category || ""))) tags.push("Tier 1");

    if (hasStat(/strength|physical power|physical damage|physical penetration/) || has(/physical/)) tags.push("Physical Damage");
    if (hasStat(/intelligence|magical power|magical damage|magical penetration/) || has(/magical/)) tags.push("Magical Damage");
    if (hasStat(/protection|armor|physical protection|magical protection/)) tags.push("Defense");
    if (hasStat(/physical protection|physical armor/)) tags.push("Physical Protection");
    if (hasStat(/magical protection|magical armor/)) tags.push("Magical Protection");
    if (hasStat(/health|hp5/)) tags.push("Health");
    if (hasStat(/cooldown|cdr/)) tags.push("Cooldown");
    if (hasStat(/mana|mp5/)) tags.push("Mana");
    if (hasStat(/attack speed/)) tags.push("Attack Speed", "Basic Attack");
    if (hasStat(/critical|crit/)) tags.push("Crit", "Basic Attack");
    if (hasStat(/lifesteal|life steal/)) tags.push("Lifesteal", "Sustain");
    if (hasStat(/movement speed|move speed/)) tags.push("Movement Speed", "Mobility");
    if (hasStat(/tenacity|crowd control reduction|ccr/)) tags.push("Tenacity", "Crowd Control");
    if (hasStat(/penetration/)) tags.push("Anti-Tank", "Anti-Protections", "Flat Penetration");
    if (hasStat(/%.*penetration|penetration.*%/)) tags.push("% Penetration");

    const enemyHealthDamagePassive = hasPassive(/bonus (physical|magical)? ?damage\s*(=|equal to|equals).*?%.*?(target|enemy|god|gods|their).*?(base|item|max|current|missing)? ?health/)
        || hasPassive(/(physical|magical) damage\s*(=|equal to|equals).*?%.*?(target|enemy|god|gods|their).*?(base|item|max|current|missing)? ?health/)
        || hasPassive(/damage\s*(=|equal to|equals).*?%.*?(target|enemy|god|gods|their).*?(base|item|max|current|missing)? ?health/)
        || hasPassive(/on enemy god hit.*bonus damage\s*(=|equal to|equals).*?[0-9.]+% of (their )?(current|max|maximum|missing) health/)
        || hasPassive(/gods take additional damage\s*=\s*[0-9.]+% of their (current|max|maximum|missing) health/)
        || hasPassive(/%\s*health (physical|magical) damage/);
    const antiShieldPassive = hasPassive(/anti[- ]?shield|shield break|breaks? shields?|remove(?:s)? shields?|shields? are destroyed|shields? applied to enemy|-[0-9.]+% shields?|lose[s]? [0-9.]+% .*shield|health shield per|bonus damage.*shield|damage.*shielded|shielded enemies|against shields?/);

    if (hasPassive(/anti[- ]?heal|healing.*reduc|reduc(?:es|ing)? healing|less healing|decreased healing/)) tags.push("Anti-Heal");
    if (enemyHealthDamagePassive) tags.push("Anti-Tank", "Anti-Health", "% Health Damage");
    if (hasPassive(/reduc(?:e|es|ing).*protection|protection.*reduc|shred|steal.*protection|ignore.*protection/)) tags.push("Anti-Tank", "Anti-Protections", "Protection Reduction");
    if (hasPassive(/penetration|ignore.*armor|ignore.*defense/)) tags.push("Anti-Tank", "Anti-Protections");
    if (antiShieldPassive) tags.push("Anti-Shield");
    if (hasPassive(/shield|barrier/) && !antiShieldPassive) tags.push("Shielding", "Defense");
    if (hasPassive(/mitigation|mitigate|damage reduction|reduced damage|take.*less damage/)) tags.push("Mitigation", "Anti-Burst", "Defense");
    if (hasPassive(/slow|root|stun|silence|knock|cripple|fear|taunt|mesmerize|crowd control/)) tags.push("Crowd Control");
    if (hasPassive(/aura|allied gods|nearby allies|nearby enemy|nearby enemies/)) tags.push("Aura", "Teamfight");
    const selfSustainPassive = hasPassive(/heal yourself|heal all allies|heal the marked ally|you and allies.*heal|allied gods.*heal|restore.*health|health heal|health regen|regenerate|gain.*lifesteal|\+.*lifesteal|while shielded.*lifesteal/)
        && !hasPassive(/enemy lifesteals|enemy gods? lifesteal|afflicted enemy/);
    if (selfSustainPassive) tags.push("Sustain", "Healing");
    if (hasPassive(/basic attack|basic attacks|on hit|successful hit/)) tags.push("Basic Attack");
    if (hasPassive(/ability|abilities|when you hit an enemy god with an ability/)) tags.push("Ability Damage");
    if (hasPassive(/bonus damage|additional damage|deal.*damage|burst/)) tags.push("Burst");
    if (hasPassive(/gold|stack|evolve|evolves|quest|bounty/)) tags.push("Economy");

    const overrideKey = String(itemDisplayName(item) || item?.name || "").toLowerCase();
    tags.push(...(ITEM_TAG_OVERRIDES[overrideKey] || ITEM_TAG_OVERRIDES[String(item?.name || "").toLowerCase()] || []));

    const offensiveSignals = ["Physical Damage", "Magical Damage", "Attack Speed", "Crit", "Burst", "Anti-Tank", "Basic Attack", "Ability Damage"].some((tag) => tags.some((itemTag) => itemTag.toLowerCase() === tag.toLowerCase()));
    const defensiveSignals = ["Defense", "Physical Protection", "Magical Protection", "Health", "Mitigation", "Shielding", "Anti-Burst"].some((tag) => tags.some((itemTag) => itemTag.toLowerCase() === tag.toLowerCase()));
    const utilitySignals = ["Utility", "Aura", "Crowd Control", "Teamfight", "Mobility", "Tenacity", "Slow", "Sustain"].some((tag) => tags.some((itemTag) => itemTag.toLowerCase() === tag.toLowerCase()));
    if (String(item?.category || "").toLowerCase() === "starter") tags.push("Starter");
    if (offensiveSignals && defensiveSignals) tags.push("Hybrid");
    else if (defensiveSignals) tags.push("Defense");
    else if (offensiveSignals) tags.push("Offense");
    else if (utilitySignals) tags.push("Utility");

    return uniqueItemTags(tags);
}

function itemStrategicTags(item) {
    const taxonomy = itemTaxonomyFor(item);
    if (taxonomy?.tags?.length) return uniqueItemTags(taxonomy.tags);
    return uniqueItemTags([...itemTagValues(item), ...derivedItemTags(item)]);
}

function itemFilterOptionSet() {
    return new Set(ITEM_FILTER_GROUPS.flatMap((group) => group.options).map((option) => option.toLowerCase()));
}

function itemFilterTags(item) {
    const allowed = itemFilterOptionSet();
    return itemStrategicTags(item).filter((tag) => allowed.has(tag.toLowerCase()));
}

function itemMatchesSelectedFilters(item, selectedFilters) {
    if (!selectedFilters.length) return true;
    const itemTags = itemFilterTags(item).map((tag) => tag.toLowerCase());
    return ITEM_FILTER_GROUPS.every((group) => {
        const selectedInGroup = selectedFilters.filter((tag) => group.options.some((option) => option.toLowerCase() === tag.toLowerCase()));
        return !selectedInGroup.length || selectedInGroup.some((tag) => itemTags.includes(tag.toLowerCase()));
    });
}

function renderItemFilterGroup(group, activeFilters, catalog) {
    const catalogTags = new Set(catalog.flatMap((item) => itemFilterTags(item)).map((tag) => tag.toLowerCase()));
    const options = group.options.filter((option) => catalogTags.has(option.toLowerCase()) || activeFilters.includes(option));
    if (!options.length) return "";
    return `
        <fieldset class="item-filter-group">
            <legend>${escapeHtml(group.label)}</legend>
            <div class="item-filter-options">
                ${options.map((option) => `
                    <label class="item-filter-check ${activeFilters.includes(option) ? "active" : ""}">
                        <input type="checkbox" value="${escapeHtml(option)}" ${activeFilters.includes(option) ? "checked" : ""} data-item-filter>
                        <span>${escapeHtml(option)}</span>
                    </label>
                `).join("")}
            </div>
        </fieldset>
    `;
}

function itemTagValues(item) {
    const meta = item?.metadata || {};
    const rawValues = [item?.category, ...(Array.isArray(meta.tags) ? meta.tags : [])];
    if (meta.itemType) rawValues.push(meta.itemType);
    const values = rawValues
        .filter(Boolean)
        .flatMap((value) => {
            const text = String(value).replace(/\s+/g, " ").trim();
            const tagMatches = text.match(/Tier\s*\d+|Starter|Offensive|Defensive|Hybrid|Utility|Magical|Physical|God-Specific/gi);
            return tagMatches && text.split(/\s+/).length > 2 ? tagMatches : [text];
        })
        .map((value) => String(value).replace(/\s+/g, " ").trim())
        .filter((value) => value && !/^tier$/i.test(value) && !/^\d+$/.test(value));
    return values.filter((value, index) => values.findIndex((other) => other.toLowerCase() === value.toLowerCase()) === index);
}

function itemTypeSubtitle(item) {
    const taxonomy = itemTaxonomyFor(item);
    if (taxonomy?.beginnerType) {
        const tags = itemStrategicTags(item).filter((tag) => tag !== taxonomy.beginnerType && !/^tier\s*\d+$/i.test(tag));
        return tags.length ? `${taxonomy.beginnerType} | ${tags.slice(0, 2).join(" + ")}` : taxonomy.beginnerType;
    }
    const category = String(item?.category || "").toLowerCase();
    const tags = itemStrategicTags(item).filter((tag) => tag.toLowerCase() !== category && !/^tier\s*\d+$/i.test(tag));
    return tags.length ? tags.slice(0, 3).join(" + ") : "Council loadout";
}

function itemPillMarkup(item, limit = 7) {
    const meta = item?.metadata || {};
    const category = String(item?.category || "").toLowerCase();
    const pills = [item?.category, ...(meta.cost ? [`${formatMetric(meta.cost)}g`] : []), ...itemStrategicTags(item).filter((tag) => tag.toLowerCase() !== category)];
    return pills
        .filter(Boolean)
        .filter((value, index, values) => values.findIndex((other) => String(other).toLowerCase() === String(value).toLowerCase()) === index)
        .slice(0, limit)
        .map((value) => `<span>${escapeHtml(value)}</span>`)
        .join("");
}

function itemPerformanceLine(item) {
    if (!item || !item.games) return "No council sample yet";
    return `${formatWinLossRecord(item.wins, item.games)} | ${formatMetric(item.winRate, 1, "%")} WR | ${formatMetric(item.games)} games`;
}

function itemDisplayImageUrl(item) {
    const meta = item?.metadata || {};
    return meta.imageUrl || item?.imageUrl || "";
}

function itemDisplayName(item) {
    const meta = item?.metadata || {};
    return meta.displayName || meta.display_name || item?.name || "Item";
}

function itemImageMarkup(item, className = "item-card-icon") {
    const imageUrl = itemDisplayImageUrl(item);
    const label = itemDisplayName(item);
    return `<div class="${className} ${imageUrl ? "" : "empty"}">${imageUrl ? `<img src="${escapeHtml(imageUrl)}" alt="${escapeHtml(label)}" loading="lazy" decoding="async" onerror="this.parentElement.classList.add('empty');this.remove();">` : "?"}</div>`;
}

function openItemDetail(itemName) {
    const catalog = enrichedItemCatalog();
    const item = catalog.find((row) => row.name === itemName);
    if (!item || !elements.godModalBackdrop || !elements.godModalContent) return;

    if (state.items.selected !== itemName) {
        state.items.selected = itemName;
        state.items.section = "overview";
    }

    const meta = item.metadata || {};
    const imageUrl = itemDisplayImageUrl(item);
    const displayName = itemDisplayName(item);
    const sections = [
        { key: "overview", label: "Overview" },
        { key: "stats", label: "Stats" },
        { key: "council", label: "Council" },
        { key: "gods", label: "Gods" },
    ];
    if (!sections.some((section) => section.key === state.items.section)) {
        state.items.section = "overview";
    }

    const topGods = [...(item.gods || [])]
        .sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.name.localeCompare(b.name))
        .slice(0, 10);
    const bestGods = [...(item.gods || [])]
        .filter((god) => Number(god.games || 0) >= 2)
        .sort((a, b) => b.winRate - a.winRate || b.games - a.games || a.name.localeCompare(b.name))
        .slice(0, 8);
    const topPlayers = [...(item.players || [])]
        .sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.name.localeCompare(b.name))
        .slice(0, 8);

    const itemRows = (rows, emptyText) => rows.length ? rows.map((row) => `
        <div class="mini-row-v2 item-modal-row">
            <span><strong>${escapeHtml(row.name)}</strong><small>${formatWinLossRecord(row.wins, row.games)} | ${formatMetric(row.games)} games</small></span>
            <b class="${Number(row.winRate || 0) >= 55 ? "movement-up" : Number(row.winRate || 0) <= 45 ? "movement-down" : ""}">${formatMetric(row.winRate, 1, "%")}</b>
        </div>
    `).join("") : `<p class="rank-meta">${escapeHtml(emptyText)}</p>`;

    const sectionHtml = {
        overview: `
            <section class="god-modal-tab-panel item-modal-panel">
                <div class="god-dossier-grid">
                    ${dossierStat("Record", formatWinLossRecord(item.wins, item.games), `${formatMetric(item.winRate, 1, "%")} WR`)}
                    ${dossierStat("Category", item.category || "--", itemTypeSubtitle(item))}
                    ${dossierStat("Main User", item.topPlayer?.name || "--", item.topPlayer ? itemPerformanceLine(item.topPlayer) : "No sample")}
                    ${dossierStat("Most Used On", item.mostUsedGod?.name || "--", item.mostUsedGod ? itemPerformanceLine(item.mostUsedGod) : "No sample")}
                </div>
                <div class="core-build-grid item-overview-grid">
                    <article class="detail-card-v2 core-build-card">
                        <p class="eyebrow">Attributes</p>
                        <h3>Stats</h3>
                        <div class="item-card-stats overview-stats">${itemInlineStats(meta.stats, 10)}</div>
                    </article>
                    <article class="detail-card-v2 core-build-card">
                        <p class="eyebrow">Passive</p>
                        <h3>Effect</h3>
                        <p class="item-passive-copy">${escapeHtml(meta.passive || "No passive text captured yet.")}</p>
                    </article>
                </div>
                <article class="detail-card-v2 wide item-metadata-card">
                    <p class="eyebrow">Metadata</p>
                    <h3>${escapeHtml(displayName)}</h3>
                    <div class="item-tag-row">${itemPillMarkup(item, 8)}</div>
                </article>
            </section>
        `,
        stats: `
            <section class="god-modal-tab-panel item-modal-panel">
                <div class="core-build-grid">
                    <article class="detail-card-v2 core-build-card"><p class="eyebrow">Stats</p><h3>Item Sheet</h3><div class="item-stat-list">${statChipList(meta.stats, 12)}</div></article>
                    <article class="detail-card-v2 core-build-card"><p class="eyebrow">Passive</p><h3>Effect Text</h3><p class="item-passive-copy">${escapeHtml(meta.passive || "No passive text captured yet.")}</p></article>
                </div>
                ${meta.sourceUrl ? `<a class="source-link" href="${escapeHtml(meta.sourceUrl)}" target="_blank" rel="noopener">Open metadata source</a>` : ""}
            </section>
        `,
        council: `
            <section class="god-modal-tab-panel item-modal-panel">
                <article class="detail-card-v2 wide"><div class="section-head"><div><p class="eyebrow">Council Usage</p><h3>Who Builds It</h3></div><span class="summary-pill">${itemPerformanceLine(item)}</span></div><div class="mini-list-v2">${itemRows(topPlayers, "No player sample yet.")}</div></article>
            </section>
        `,
        gods: `
            <section class="god-modal-tab-panel item-modal-panel">
                <div class="core-build-grid">
                    <article class="detail-card-v2 core-build-card"><div class="section-head"><div><p class="eyebrow">Most Used On</p><h3>God Samples</h3></div></div><div class="mini-list-v2">${itemRows(topGods, "No god sample yet.")}</div></article>
                    <article class="detail-card-v2 core-build-card"><div class="section-head"><div><p class="eyebrow">Best Results</p><h3>Winning Looks</h3></div></div><div class="mini-list-v2">${itemRows(bestGods, "Need at least two games on a god.")}</div></article>
                </div>
            </section>
        `,
    };

    elements.godModalContent.innerHTML = `
        <div class="item-modal-hero">
            <div class="item-modal-hero-glow"></div>
            ${itemImageMarkup(item, "item-modal-icon")}
            <div class="item-modal-copy">
                <p class="eyebrow">Council Armory</p>
                <h2>${escapeHtml(displayName)}</h2>
                <p>${escapeHtml(meta.summary || `${item.category} from stored council loadouts.`)}</p>
                <div class="item-tag-row"><span>${escapeHtml(item.category)}</span><span>${itemPerformanceLine(item)}</span>${meta.cost ? `<span>${formatMetric(meta.cost)} gold</span>` : ""}</div>
            </div>
            <div class="item-modal-score ${Number(item.winRate || 0) >= 55 ? "movement-up" : Number(item.winRate || 0) <= 45 ? "movement-down" : ""}">${formatMetric(item.winRate, 0, "%")}</div>
        </div>
        <div class="god-modal-body item-modal-body">
            <div class="subtab-bar god-modal-tabs" role="tablist" aria-label="Item detail sections">${sections.map((section) => `<button class="subtab-btn ${state.items.section === section.key ? "active" : ""}" type="button" data-item-modal-section="${section.key}" role="tab" aria-selected="${state.items.section === section.key ? "true" : "false"}">${escapeHtml(section.label)}</button>`).join("")}</div>
            <div class="subtab-content god-modal-tab-content">${sectionHtml[state.items.section]}</div>
        </div>
    `;

    elements.godModalBackdrop.classList.remove("hidden");
    document.body.classList.add("modal-open");

    elements.godModalContent.querySelectorAll("[data-item-modal-section]").forEach((button) => {
        button.addEventListener("click", () => {
            state.items.section = button.dataset.itemModalSection || "overview";
            openItemDetail(item.name);
        });
    });
}

// This helper renders the dedicated Items tab as a compact catalog: scan cards
// first, click one item, then inspect stats/passives and council performance.
function renderItemsTab() {
    if (!elements.tabItems) return;

    if (!state.raterStats.loaded && !Object.keys(state.raterStats.profiles || {}).length) {
        elements.tabItems.innerHTML = emptyState("Loading Items", "Council loadouts are still being gathered.");
        return;
    }

    const catalog = enrichedItemCatalog();
    const search = (state.items.search || "").trim().toLowerCase();
    const activeFilters = Array.isArray(state.items.filters) ? state.items.filters : [];
    const sort = state.items.sort || "Most used";
    let rows = catalog.filter((item) => {
        const meta = item.metadata || {};
        const strategicTags = itemStrategicTags(item);
        const matchesSearch = !search || [item.name, itemDisplayName(item), item.category, meta.itemType, item.bestGod?.name, item.mostUsedGod?.name, item.topPlayer?.name, meta.summary, meta.passive, ...strategicTags, ...(meta.stats || [])]
            .filter(Boolean)
            .some((value) => String(value).toLowerCase().includes(search));
        return matchesSearch && itemMatchesSelectedFilters(item, activeFilters);
    });

    if (sort === "Best WR") {
        rows = rows.sort((a, b) => b.winRate - a.winRate || b.games - a.games || a.name.localeCompare(b.name));
    } else if (sort === "Name") {
        rows = rows.sort((a, b) => a.name.localeCompare(b.name));
    } else {
        rows = rows.sort((a, b) => b.games - a.games || b.winRate - a.winRate || a.name.localeCompare(b.name));
    }

    const filterGroups = ITEM_FILTER_GROUPS.map((group) => renderItemFilterGroup(group, activeFilters, catalog)).join("");
    const activeFilterPills = activeFilters.length ? activeFilters.map((filter) => `<button class="filter-chip active removable" type="button" data-remove-item-filter="${escapeHtml(filter)}">${escapeHtml(filter)} x</button>`).join("") : `<span class="summary-pill muted">No item filters</span>`;
    const itemCards = rows.map((item) => {
        const meta = item.metadata || {};
        const tagValues = itemStrategicTags(item).filter((tag) => tag.toLowerCase() !== String(item.category || "").toLowerCase()).slice(0, 5);
        return `
            <button class="item-catalog-card condensed" type="button" data-item-detail="${escapeHtml(item.name)}">
                <div class="item-card-topline"><span>${escapeHtml(item.category)}</span><strong>${itemPerformanceLine(item)}</strong></div>
                <div class="item-card-main">
                    ${itemImageMarkup(item)}
                    <div class="item-card-copy">
                        <h3>${escapeHtml(itemDisplayName(item))}</h3>
                        <div class="item-card-stats">${itemInlineStats(meta.stats, 4)}</div>
                        ${meta.passive ? `<p class="item-card-passive">${escapeHtml(meta.passive)}</p>` : `<p class="item-card-passive muted">Passive pending.</p>`}
                    </div>
                </div>
                <div class="item-card-tags">
                    ${meta.cost ? `<span>${formatMetric(meta.cost)}g</span>` : ""}
                    ${tagValues.map((tag) => `<span>${escapeHtml(tag)}</span>`).join("")}
                </div>
            </button>
        `;
    }).join("");

    elements.tabItems.innerHTML = `
        <div class="panel items-panel">
            <div class="panel-heading split-heading">
                <div>
                    <p class="eyebrow">Armory</p>
                    <h2>Items</h2>
                    <p class="hero-text compact-copy">A searchable catalog of what each item does and how the council performs with it.</p>
                </div>
                <span class="summary-pill">${formatMetric(rows.length)} shown</span>
            </div>

            <section class="detail-card-v2 item-controls-card">
                <div class="items-control-grid">
                    <label class="field">
                        <span>Search Item</span>
                        <input id="item-search" type="text" value="${escapeHtml(state.items.search)}" placeholder="Soul Reaver, Bluestone, Joey..." autocomplete="off">
                    </label>
                    <label class="field">
                        <span>Sort</span>
                        <select id="item-sort">
                            ${["Most used", "Best WR", "Name"].map((option) => `<option value="${option}" ${sort === option ? "selected" : ""}>${option}</option>`).join("")}
                        </select>
                    </label>
                </div>
                <div class="item-filter-toolbar">
                    <div class="item-active-filters">${activeFilterPills}</div>
                    <button class="ghost-btn small" type="button" id="item-clear-filters" ${activeFilters.length ? "" : "disabled"}>Clear Filters</button>
                </div>
                <div class="item-filter-groups">${filterGroups}</div>
            </section>

            <section class="item-catalog-grid">${itemCards || emptyState("No Items Found", "No item usage matched the current filter.")}</section>
            ${renderBackToTop()}
        </div>
    `;

    const searchInput = document.getElementById("item-search");
    searchInput?.addEventListener("input", (event) => {
        const cursor = event.target.selectionStart || 0;
        state.items.search = event.target.value;
        renderItemsTab();
        requestAnimationFrame(() => {
            const nextInput = document.getElementById("item-search");
            nextInput?.focus();
            nextInput?.setSelectionRange(cursor, cursor);
        });
    });
    elements.tabItems.querySelectorAll("[data-item-filter]").forEach((input) => {
        input.addEventListener("change", () => {
            const nextFilters = new Set(Array.isArray(state.items.filters) ? state.items.filters : []);
            if (input.checked) nextFilters.add(input.value);
            else nextFilters.delete(input.value);
            state.items.filters = [...nextFilters];
            renderItemsTab();
        });
    });
    elements.tabItems.querySelectorAll("[data-remove-item-filter]").forEach((button) => {
        button.addEventListener("click", () => {
            state.items.filters = (state.items.filters || []).filter((filter) => filter !== button.dataset.removeItemFilter);
            renderItemsTab();
        });
    });
    document.getElementById("item-clear-filters")?.addEventListener("click", () => {
        state.items.filters = [];
        renderItemsTab();
    });
    document.getElementById("item-sort")?.addEventListener("change", (event) => {
        state.items.sort = event.target.value;
        renderItemsTab();
    });
    elements.tabItems.querySelectorAll("[data-item-detail]").forEach((button) => {
        button.addEventListener("click", () => {
            openItemDetail(button.dataset.itemDetail || "");
        });
    });
}
