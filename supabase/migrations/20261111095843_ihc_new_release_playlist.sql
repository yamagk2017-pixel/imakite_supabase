-- Dedicated state for the new release playlist. Existing rankings stay untouched.
create table ihc.release_playlist_projects (
  slug text primary key,
  name text not null,
  spotify_playlist_id text unique,
  current_tracks jsonb not null default '[]'::jsonb,
  revision bigint not null default 0,
  last_cutoff date,
  lock_token uuid,
  locked_until timestamptz,
  updated_at timestamptz not null default now(),
  check (jsonb_typeof(current_tracks) = 'array'),
  check (jsonb_array_length(current_tracks) in (0,50))
);
create table ihc.release_playlist_runs (
  id uuid primary key default gen_random_uuid(),
  project_slug text not null references ihc.release_playlist_projects(slug),
  cutoff date not null,
  mode text not null check (mode in ('bootstrap','weekly')),
  status text not null default 'prepared' check (status in ('prepared','committed')),
  previous_revision bigint not null,
  before_tracks jsonb not null,
  target_tracks jsonb not null,
  added_tracks jsonb not null,
  evidence jsonb not null,
  spotify_snapshot_id text,
  cover jsonb,
  created_at timestamptz not null default now(),
  committed_at timestamptz,
  unique (project_slug,cutoff),
  check (jsonb_array_length(target_tracks)=50),
  check (jsonb_array_length(added_tracks) <= case when mode='bootstrap' then 50 else 25 end)
);
create unique index release_playlist_one_pending on ihc.release_playlist_runs(project_slug) where status='prepared';
create table ihc.release_playlist_history (
  project_slug text not null references ihc.release_playlist_projects(slug),
  track_id text not null,
  group_id uuid not null references imd.groups(id),
  first_run_id uuid not null references ihc.release_playlist_runs(id),
  first_published_at timestamptz not null default now(),
  primary key (project_slug,track_id)
);
create index release_playlist_history_run on ihc.release_playlist_history(first_run_id);
create index release_playlist_history_group on ihc.release_playlist_history(group_id);
alter table ihc.release_playlist_projects enable row level security;
alter table ihc.release_playlist_runs enable row level security;
alter table ihc.release_playlist_history enable row level security;
revoke all on ihc.release_playlist_projects,ihc.release_playlist_runs,ihc.release_playlist_history from public,anon,authenticated;
grant select,insert,update,delete on ihc.release_playlist_projects,ihc.release_playlist_runs,ihc.release_playlist_history to service_role;

insert into ihc.release_playlist_projects(slug,name) values ('imakite-idol-japan','イマキテアイドル：ジャパン');

-- Freeze a target before any Spotify mutation. Retry returns the same target.
create function ihc.prepare_release_playlist_run(
  p_slug text,p_cutoff date,p_mode text,p_expected_revision bigint,
  p_target jsonb,p_added jsonb,p_evidence jsonb,p_lock_token uuid
) returns jsonb language plpgsql security invoker set search_path='' as $$
declare p ihc.release_playlist_projects; r ihc.release_playlist_runs; n integer;
begin
  select * into strict p from ihc.release_playlist_projects where slug=p_slug for update;
  if p.locked_until>now() and p.lock_token is distinct from p_lock_token then
    raise exception 'Playlist publisher is locked';
  end if;
  select * into r from ihc.release_playlist_runs where project_slug=p_slug and status='prepared';
  if not found then
    select * into r from ihc.release_playlist_runs where project_slug=p_slug and cutoff=p_cutoff;
    if found then return to_jsonb(r); end if;
    if p.revision<>p_expected_revision then raise exception 'Playlist revision changed'; end if;
    if p_mode='bootstrap' and (p.revision<>0 or jsonb_array_length(p.current_tracks)<>0) then
      raise exception 'Already initialized';
    end if;
    if p_mode='weekly' and (p.last_cutoff is null or p_cutoff<=p.last_cutoff or extract(dow from p_cutoff)<>6) then
      raise exception 'Weekly cutoff must be a later Saturday';
    end if;
    if jsonb_typeof(p_target)<>'array' or jsonb_array_length(p_target)<>50 then raise exception 'Exactly 50 tracks required'; end if;
    select count(distinct value->>'track_id') into n from jsonb_array_elements(p_target);
    if n<>50 then raise exception 'Duplicate or missing target Track ID'; end if;
    if jsonb_array_length(p_added)>(case when p_mode='bootstrap' then 50 else 25 end) then raise exception 'Too many additions'; end if;
    select count(distinct value->>'group_id') into n from jsonb_array_elements(p_added);
    if n<>jsonb_array_length(p_added) then raise exception 'Only one added track per group'; end if;
    if exists(select 1 from jsonb_array_elements(p_added) a join ihc.release_playlist_history h
       on h.project_slug=p_slug and h.track_id=a->>'track_id') then raise exception 'Previously published Track ID'; end if;
    if p_target <> (select jsonb_agg(x.value order by x.ordinality) from jsonb_array_elements(p_added || p.current_tracks) with ordinality x where x.ordinality<=50) then
      raise exception 'Target must prepend additions and preserve retained order';
    end if;
    insert into ihc.release_playlist_runs(project_slug,cutoff,mode,previous_revision,before_tracks,target_tracks,added_tracks,evidence)
    values(p_slug,p_cutoff,p_mode,p.revision,p.current_tracks,p_target,p_added,p_evidence) returning * into r;
  end if;
  update ihc.release_playlist_projects set lock_token=p_lock_token,locked_until=now()+interval '30 minutes' where slug=p_slug;
  return to_jsonb(r);
end; $$;

create function ihc.bind_release_playlist(p_run_id uuid,p_lock_token uuid,p_playlist_id text)
returns void language plpgsql security invoker set search_path='' as $$
declare p ihc.release_playlist_projects; r ihc.release_playlist_runs;
begin
  select * into strict r from ihc.release_playlist_runs where id=p_run_id;
  select * into strict p from ihc.release_playlist_projects where slug=r.project_slug for update;
  if p.lock_token is distinct from p_lock_token or p.locked_until<=now() or r.status<>'prepared' then raise exception 'Publisher lease expired'; end if;
  if p.spotify_playlist_id is not null and p.spotify_playlist_id<>p_playlist_id then raise exception 'Playlist ID cannot change'; end if;
  update ihc.release_playlist_projects set spotify_playlist_id=p_playlist_id where slug=p.slug;
end; $$;

create function ihc.finish_release_playlist_run(p_run_id uuid,p_lock_token uuid,p_snapshot_id text,p_cover jsonb)
returns jsonb language plpgsql security invoker set search_path='' as $$
declare p ihc.release_playlist_projects; r ihc.release_playlist_runs;
begin
  select * into strict r from ihc.release_playlist_runs where id=p_run_id;
  select * into strict p from ihc.release_playlist_projects where slug=r.project_slug for update;
  if r.status='committed' then return to_jsonb(r); end if;
  if p.lock_token is distinct from p_lock_token or p.locked_until<=now() or p.revision<>r.previous_revision then raise exception 'Publisher lease or revision changed'; end if;
  if p.spotify_playlist_id is null or p_snapshot_id is null then raise exception 'Spotify verification required'; end if;
  if p_cover->>'group_id' is distinct from r.target_tracks->0->>'group_id' or p_cover->>'sha256' is null then raise exception 'Cover must belong to the first artist'; end if;
  insert into ihc.release_playlist_history(project_slug,track_id,group_id,first_run_id)
    select r.project_slug,value->>'track_id',(value->>'group_id')::uuid,r.id from jsonb_array_elements(r.added_tracks);
  update ihc.release_playlist_runs set status='committed',spotify_snapshot_id=p_snapshot_id,cover=p_cover,committed_at=now() where id=r.id returning * into r;
  update ihc.release_playlist_projects set current_tracks=r.target_tracks,revision=revision+1,last_cutoff=r.cutoff,
    lock_token=null,locked_until=null,updated_at=now() where slug=p.slug;
  return to_jsonb(r);
end; $$;

create function ihc.unlock_release_playlist(p_slug text,p_lock_token uuid)
returns void language sql security invoker set search_path='' as $$
  update ihc.release_playlist_projects set lock_token=null,locked_until=null where slug=p_slug and lock_token=p_lock_token;
$$;
revoke execute on function ihc.prepare_release_playlist_run(text,date,text,bigint,jsonb,jsonb,jsonb,uuid) from public,anon,authenticated;
revoke execute on function ihc.bind_release_playlist(uuid,uuid,text) from public,anon,authenticated;
revoke execute on function ihc.finish_release_playlist_run(uuid,uuid,text,jsonb) from public,anon,authenticated;
revoke execute on function ihc.unlock_release_playlist(text,uuid) from public,anon,authenticated;
grant execute on function ihc.prepare_release_playlist_run(text,date,text,bigint,jsonb,jsonb,jsonb,uuid) to service_role;
grant execute on function ihc.bind_release_playlist(uuid,uuid,text) to service_role;
grant execute on function ihc.finish_release_playlist_run(uuid,uuid,text,jsonb) to service_role;
grant execute on function ihc.unlock_release_playlist(text,uuid) to service_role;
notify pgrst,'reload schema';
