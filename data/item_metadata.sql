-- Optional item catalog metadata for High Council Hub.
-- The app silently falls back to data/item_metadata.json if this table does not exist.
create table if not exists public.item_metadata (
    name text primary key,
    display_name text,
    slug text,
    source text,
    source_url text,
    image_url text,
    summary text,
    cost integer,
    item_type text,
    tags jsonb not null default '[]'::jsonb,
    stats jsonb not null default '[]'::jsonb,
    passive text,
    categories_seen jsonb not null default '[]'::jsonb,
    sample_count integer not null default 0,
    updated_at timestamptz not null default now()
);

create index if not exists item_metadata_slug_idx on public.item_metadata (slug);
create index if not exists item_metadata_item_type_idx on public.item_metadata (item_type);

-- Example upsert shape:
-- insert into public.item_metadata (name, display_name, slug, source, source_url, image_url, summary, cost, item_type, tags, stats, passive)
-- values (
--   'Gladiator Shield',
--   'Gladiator Shield',
--   'gladiator-shield',
--   'manual',
--   '',
--   '',
--   'Defensive item metadata goes here.',
--   2450,
--   'Tier 3 Defensive',
--   '["Physical Protection", "Health", "Cooldown Reduction"]'::jsonb,
--   '["+200 Health", "+25 Physical Protection", "+10 Cooldown Reduction"]'::jsonb,
--   'Passive text goes here.'
-- )
-- on conflict (name) do update set
--   display_name = excluded.display_name,
--   slug = excluded.slug,
--   source = excluded.source,
--   source_url = excluded.source_url,
--   image_url = excluded.image_url,
--   summary = excluded.summary,
--   cost = excluded.cost,
--   item_type = excluded.item_type,
--   tags = excluded.tags,
--   stats = excluded.stats,
--   passive = excluded.passive,
--   updated_at = now();
