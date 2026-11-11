-- Supabase project: supabase-buzzttara (oslpycadjyhnldloxvyl)
-- Applied migration version: 20261111135226 (2026-11-11 UTC).
-- Security Advisor: function_search_path_mutable.
-- All relation references are already schema-qualified; now() is in pg_catalog.
-- Preserve function bodies, signatures, grants, and SECURITY DEFINER/INVOKER modes.
-- No table data, RLS policies, or Auth settings are changed.
begin;
set local lock_timeout = '2s';
set local statement_timeout = '15s';

-- Abort if a concurrent deployment changed the reviewed functions.
do $$
begin
  if (select count(*) from pg_proc p where
      (p.oid = 'ihc.set_weekly_playlists_updated_at()'::regprocedure
        and md5(p.prosrc) = '9b1889f56258bf9d6554213c05019c76' and not p.prosecdef and p.proconfig is null)
      or (p.oid = 'imd.set_iam_updated_at()'::regprocedure
        and md5(p.prosrc) = '9b1889f56258bf9d6554213c05019c76' and not p.prosecdef and p.proconfig is null)
      or (p.oid = 'public.increment_view_count(uuid)'::regprocedure
        and md5(p.prosrc) = '2bf7bd73047cf23694e77431b77c3765' and p.prosecdef and p.proconfig is null)
     ) <> 3 then
    raise exception 'Reviewed function definitions or settings changed; stop and re-review';
  end if;
end
$$;

alter function ihc.set_weekly_playlists_updated_at() set search_path = '';
alter function imd.set_iam_updated_at() set search_path = '';
alter function public.increment_view_count(uuid) set search_path = '';
commit;
