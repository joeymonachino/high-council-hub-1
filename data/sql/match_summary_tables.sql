-- Lightweight match summary layer for High Council Hub.
-- Keep smitesource_match_history.raw_match as the archive/source of truth.
-- These tables power normal UI reads without selecting the full raw_match blob.

create table if not exists public.match_player_summary (
    record_key text primary key,
    player text not null,
    profile_player_uuid text,
    hirez_player_uuid text,
    match_key text,
    match_id text,
    source text,
    source_match_id text,
    canonical_match_key text,
    god_name text,
    queue_type text,
    won boolean,
    party_size integer,
    party_label text,
    team_id integer,
    role text,
    kills integer not null default 0,
    deaths integer not null default 0,
    assists integer not null default 0,
    total_damage numeric not null default 0,
    total_gold numeric not null default 0,
    total_xp numeric not null default 0,
    wards integer not null default 0,
    duration_seconds numeric not null default 0,
    aspect_name text,
    team_players jsonb not null default '[]'::jsonb,
    enemy_players jsonb not null default '[]'::jsonb,
    participant_gods jsonb not null default '{}'::jsonb,
    started_at timestamptz,
    synced_at timestamptz,
    summary_updated_at timestamptz not null default now()
);

create index if not exists match_player_summary_player_started_idx on public.match_player_summary (player, started_at desc);
create index if not exists match_player_summary_match_key_idx on public.match_player_summary (match_key);
create index if not exists match_player_summary_canonical_match_key_idx on public.match_player_summary (canonical_match_key);
create index if not exists match_player_summary_god_idx on public.match_player_summary (god_name);
create index if not exists match_player_summary_queue_idx on public.match_player_summary (queue_type);

create table if not exists public.match_item_summary (
    item_key text primary key,
    record_key text not null references public.match_player_summary(record_key) on delete cascade,
    player text not null,
    match_key text,
    god_name text,
    queue_type text,
    won boolean,
    started_at timestamptz,
    item_name text not null,
    category text not null,
    slot_index integer,
    item_master_id text,
    item_hex_id text,
    image_url text,
    summary_updated_at timestamptz not null default now()
);

create index if not exists match_item_summary_record_idx on public.match_item_summary (record_key);
create index if not exists match_item_summary_player_started_idx on public.match_item_summary (player, started_at desc);
create index if not exists match_item_summary_item_idx on public.match_item_summary (item_name, category);
create index if not exists match_item_summary_god_idx on public.match_item_summary (god_name);
