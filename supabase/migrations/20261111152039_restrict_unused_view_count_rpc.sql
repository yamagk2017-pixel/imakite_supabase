-- Approved security hardening, 2026-11-12 JST.
-- Applied migration version: 20261111152039.
-- Project: supabase-buzzttara (oslpycadjyhnldloxvyl).
-- No application caller or database dependency was found for this legacy RPC.
-- No direct RPC requests were recorded in the inspected last 24 hours.
-- The current tweet page updates views directly; that separate access model is unchanged.
-- Preserve the function, its body, and administrative/server use.
begin;
set local lock_timeout = '2s';
set local statement_timeout = '15s';

do $$
begin
  if not exists (select 1 from pg_catalog.pg_proc
       where oid='public.increment_view_count(uuid)'::regprocedure
         and prosecdef
         and md5(prosrc)='2bf7bd73047cf23694e77431b77c3765') then
    raise exception 'Reviewed view-counter function changed; stop and re-review';
  end if;
  if not pg_catalog.has_function_privilege('service_role','public.increment_view_count(uuid)','EXECUTE') then
    raise exception 'Expected server execution grant is missing';
  end if;
end
$$;

revoke execute on function public.increment_view_count(uuid) from public, anon, authenticated;

do $$
begin
  if pg_catalog.has_function_privilege('anon','public.increment_view_count(uuid)','EXECUTE')
     or pg_catalog.has_function_privilege('authenticated','public.increment_view_count(uuid)','EXECUTE')
     or not pg_catalog.has_function_privilege('service_role','public.increment_view_count(uuid)','EXECUTE') then
    raise exception 'Unexpected effective view-counter function privileges';
  end if;
end
$$;
commit;
