-- One group per playlist: a new selection replaces that group's old track.
create or replace function ihc.prepare_release_playlist_run(
  p_slug text,p_cutoff date,p_mode text,p_expected_revision bigint,
  p_target jsonb,p_added jsonb,p_evidence jsonb,p_lock_token uuid
) returns jsonb language plpgsql security invoker set search_path='' as $$
declare p ihc.release_playlist_projects; r ihc.release_playlist_runs; n integer; expected jsonb;
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
    select count(distinct value->>'group_id') into n from jsonb_array_elements(p_target);
    if n<>50 then raise exception 'Target requires 50 unique groups'; end if;
    if jsonb_array_length(p_added)>(case when p_mode='bootstrap' then 50 else 25 end) then raise exception 'Too many additions'; end if;
    select count(distinct value->>'group_id') into n from jsonb_array_elements(p_added);
    if n<>jsonb_array_length(p_added) then raise exception 'Only one added track per group'; end if;
    if exists(select 1 from jsonb_array_elements(p_added) a join ihc.release_playlist_history h
       on h.project_slug=p_slug and h.track_id=a->>'track_id') then raise exception 'Previously published Track ID'; end if;
    -- Replace an existing group's track before truncating; retain all other
    -- groups in their current order, including the tail when only replacing.
    with ordered as (
      select a.value,a.ordinality from jsonb_array_elements(p_added) with ordinality a
      union all
      select t.value,jsonb_array_length(p_added)+t.ordinality
      from jsonb_array_elements(p.current_tracks) with ordinality t
      where not exists (
        select 1 from jsonb_array_elements(p_added) a
        where a->>'group_id'=t.value->>'group_id'
      )
    ), first_fifty as (
      select value,ordinality from ordered order by ordinality limit 50
    )
    select coalesce(jsonb_agg(value order by ordinality),'[]'::jsonb) into expected from first_fifty;
    if p_target is distinct from expected then
      raise exception 'Target must replace added groups and preserve retained order';
    end if;
    insert into ihc.release_playlist_runs(project_slug,cutoff,mode,previous_revision,before_tracks,target_tracks,added_tracks,evidence)
    values(p_slug,p_cutoff,p_mode,p.revision,p.current_tracks,p_target,p_added,p_evidence) returning * into r;
  end if;
  update ihc.release_playlist_projects set lock_token=p_lock_token,locked_until=now()+interval '30 minutes' where slug=p_slug;
  return to_jsonb(r);
end; $$;

notify pgrst,'reload schema';
