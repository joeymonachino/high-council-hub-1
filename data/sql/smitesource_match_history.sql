create table if not exists public.smitesource_match_history (
    record_key text primary key,
    player text not null,
    profile_player_uuid text,
    hirez_player_uuid text,
    match_key text not null,
    match_id text,
    god_name text,
    queue_type text,
    won boolean,
    party_size integer,
    party_label text,
    team_id integer,
    started_at timestamptz,
    raw_match jsonb not null,
    synced_at timestamptz not null default timezone('utc', now())
);

create index if not exists smitesource_match_history_player_idx
    on public.smitesource_match_history (player);

create index if not exists smitesource_match_history_started_at_idx
    on public.smitesource_match_history (started_at desc);

create index if not exists smitesource_match_history_player_started_at_idx
    on public.smitesource_match_history (player, started_at desc);

create index if not exists smitesource_match_history_match_key_idx
    on public.smitesource_match_history (match_key);
