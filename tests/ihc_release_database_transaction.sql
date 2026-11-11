-- Integration checks are rolled back: no test playlist/history remains.
begin;
set local role service_role;
do $$
declare
  seed jsonb; target jsonb; added jsonb; r jsonb; again jsonb; cover jsonb;
  lease uuid:=gen_random_uuid(); other uuid:=gen_random_uuid(); n integer;
  slug text:='test-release-playlist-'||gen_random_uuid()::text;
begin
  select jsonb_agg(jsonb_build_object('track_id','test-track-'||rn,'group_id',id,'group_name','Test') order by rn)
    into seed from (select id,row_number() over(order by id) rn from imd.groups where deleted_at is null order by id limit 50) g;
  insert into ihc.release_playlist_projects(slug,name) values (slug,'transaction-only test');
  r:=ihc.prepare_release_playlist_run(slug,'2026-11-10','bootstrap',0,seed,seed,'{}',lease);
  if r->>'status'<>'prepared' then raise exception 'Prepare failed'; end if;
  perform ihc.bind_release_playlist((r->>'id')::uuid,lease,'test-playlist-'||lease::text);
  cover:=jsonb_build_object('group_id',seed->0->>'group_id','sha256','testhash');
  perform ihc.finish_release_playlist_run((r->>'id')::uuid,lease,'snapshot',cover);
  again:=ihc.prepare_release_playlist_run(slug,'2026-11-10','bootstrap',0,seed,seed,'{}',other);
  if again->>'status'<>'committed' then raise exception 'Committed rerun was not idempotent'; end if;
  select count(*) into n from ihc.release_playlist_history where project_slug=slug;
  if n<>50 then raise exception 'Initial history expected 50, got %',n; end if;
  -- A past Track ID cannot return, even if placed ahead of the retained 49.
  added:=jsonb_build_array(seed->49);
  select added||jsonb_agg(value order by ordinality) into target from jsonb_array_elements(seed) with ordinality where ordinality<50;
  begin
    perform ihc.prepare_release_playlist_run(slug,'2026-11-14','weekly',1,target,added,'{}',lease);
    raise exception 'Expected history rejection';
  exception when others then
    if sqlerrm<>'Previously published Track ID' then raise; end if;
  end;
  -- Same group, different Track ID is allowed; retained older track stays.
  added:=jsonb_build_array(jsonb_set(seed->0,'{track_id}','"new-test-track"'));
  select added||jsonb_agg(value order by ordinality) into target from jsonb_array_elements(seed) with ordinality where ordinality<50;
  r:=ihc.prepare_release_playlist_run(slug,'2026-11-14','weekly',1,target,added,'{}',lease);
  begin
    perform ihc.prepare_release_playlist_run(slug,'2026-11-14','weekly',1,target,added,'{}',other);
    raise exception 'Expected concurrent publisher rejection';
  exception when others then
    if sqlerrm<>'Playlist publisher is locked' then raise; end if;
  end;
  begin
    perform ihc.finish_release_playlist_run((r->>'id')::uuid,other,'snapshot',cover);
    raise exception 'Expected wrong lease rejection';
  exception when others then
    if sqlerrm<>'Publisher lease or revision changed' then raise; end if;
  end;
  perform ihc.unlock_release_playlist(slug,lease);
  again:=ihc.prepare_release_playlist_run(slug,'2026-11-21','weekly',1,seed,'[]','{}',other);
  if again->>'id'<>r->>'id' or again->'target_tracks'<>target then raise exception 'Frozen pending run was not resumed'; end if;
  perform ihc.finish_release_playlist_run((r->>'id')::uuid,other,'snapshot-2',cover);
  select count(*) into n from ihc.release_playlist_history where project_slug=slug;
  if n<>51 then raise exception 'Weekly history expected 51, got %',n; end if;
  select jsonb_array_length(current_tracks) into n from ihc.release_playlist_projects where release_playlist_projects.slug=(r->>'project_slug');
  if n<>50 then raise exception 'Final playlist must have 50 tracks'; end if;
  if has_table_privilege('anon','ihc.release_playlist_history','select') or
     has_function_privilege('authenticated','ihc.finish_release_playlist_run(uuid,uuid,text,jsonb)','execute') then
    raise exception 'Publication internals must not be publicly accessible';
  end if;
end; $$;
rollback;
select 'passed (all test state rolled back)' as release_playlist_transaction_tests;
