alter table if exists ihc.daily_rankings
  add column if not exists base_score numeric,
  add column if not exists conversion_bonus_awarded numeric not null default 0,
  add column if not exists conversion_momentum numeric not null default 0,
  add column if not exists algorithm_version text not null default 'v1';

update ihc.daily_rankings
set base_score = coalesce(score, 0)
where base_score is null;

alter table if exists ihc.daily_rankings
  alter column base_score set default 0,
  alter column base_score set not null;

alter table if exists ihc.weekly_rankings
  add column if not exists base_total_score numeric,
  add column if not exists conversion_momentum numeric not null default 0,
  add column if not exists v2_candidate_total_score numeric,
  add column if not exists v1_candidate_rank integer,
  add column if not exists v2_candidate_rank integer,
  add column if not exists algorithm_version text not null default 'v1';

update ihc.weekly_rankings
set base_total_score = coalesce(base_total_score, total_score, 0),
    v2_candidate_total_score = coalesce(
      v2_candidate_total_score,
      total_score,
      0
    ),
    v1_candidate_rank = coalesce(v1_candidate_rank, rank),
    v2_candidate_rank = coalesce(v2_candidate_rank, rank)
where base_total_score is null
   or v2_candidate_total_score is null
   or v1_candidate_rank is null
   or v2_candidate_rank is null;

alter table if exists ihc.weekly_rankings
  alter column base_total_score set default 0,
  alter column base_total_score set not null,
  alter column v2_candidate_total_score set default 0,
  alter column v2_candidate_total_score set not null;
