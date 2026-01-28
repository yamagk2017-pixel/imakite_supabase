alter table if exists ihc.daily_rankings
add column if not exists artist_name text;
