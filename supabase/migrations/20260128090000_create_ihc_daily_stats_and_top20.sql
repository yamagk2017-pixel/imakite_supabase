create table if not exists ihc.daily_stats (
  snapshot_date date not null,
  avg_score numeric,
  count_pop_zero int,
  count_tpsr_zero int,
  count_both_zero int,
  avg_score_prev numeric,
  count_pop_zero_prev int,
  count_tpsr_zero_prev int,
  count_both_zero_prev int,
  avg_score_diff numeric,
  count_pop_zero_diff int,
  count_tpsr_zero_diff int,
  count_both_zero_diff int,
  avg_score_ratio text,
  count_pop_zero_ratio text,
  count_tpsr_zero_ratio text,
  count_both_zero_ratio text,
  created_at timestamptz default now(),
  constraint daily_stats_pkey primary key (snapshot_date)
);

create table if not exists ihc.daily_top20 (
  snapshot_date date not null,
  rank int not null,
  group_id uuid not null references imd.groups(id),
  artist_name text,
  score numeric,
  latest_track_name text,
  latest_track_embed_link text,
  artist_image_url text,
  created_at timestamptz default now(),
  constraint daily_top20_pkey primary key (snapshot_date, group_id)
);

create table if not exists ihc.cumulative_rankings (
  snapshot_date date not null,
  rank int not null,
  group_id uuid not null references imd.groups(id),
  artist_name text,
  cumulative_score numeric,
  score numeric,
  artist_popularity int,
  created_at timestamptz default now(),
  constraint cumulative_rankings_pkey primary key (snapshot_date, group_id)
);
