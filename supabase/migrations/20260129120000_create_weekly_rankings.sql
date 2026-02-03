create table if not exists ihc.weekly_rankings (
  week_end_date date not null,
  rank int not null,
  prev_rank int,
  group_id uuid not null references imd.groups(id),
  artist_name text,
  total_score numeric,
  artist_popularity int,
  created_at timestamptz default now(),
  constraint weekly_rankings_pkey primary key (week_end_date, group_id)
);
