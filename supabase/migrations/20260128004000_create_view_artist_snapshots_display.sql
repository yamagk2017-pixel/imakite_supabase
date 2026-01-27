create or replace view ihc.v_artist_snapshots_display as
select
  s.snapshot_date,
  s.group_id,
  g.name_ja as display_name,
  s.name as spotify_name,
  s.artist_popularity,
  s.followers,
  s.track_popularity_sum,
  s.new_release_count,
  s.created_at
from ihc.artist_snapshots s
join imd.groups g on g.id = s.group_id;
