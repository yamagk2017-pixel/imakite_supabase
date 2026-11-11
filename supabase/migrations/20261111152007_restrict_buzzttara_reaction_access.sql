-- Approved security hardening, 2026-11-12 JST.
-- Applied migration version: 20261111152007.
-- Project: supabase-buzzttara (oslpycadjyhnldloxvyl).
-- Keep reactions server-only; keep all reaction rows and counters.
-- Production and Preview service credentials and duplicate handling were verified.
-- The application endpoint requires service-role credentials and never falls back.
begin;
set local lock_timeout = '2s';
set local statement_timeout = '15s';

do $$
begin
  if (select relrowsecurity from pg_catalog.pg_class where oid='public.tweet_reactions'::regclass)
     or exists (select 1 from pg_catalog.pg_policy where polrelid='public.tweet_reactions'::regclass) then
    raise exception 'Reviewed reaction RLS state changed; stop and re-review';
  end if;
  if not (select rolbypassrls from pg_catalog.pg_roles where rolname='service_role') then
    raise exception 'Expected server role RLS bypass is missing';
  end if;
  if pg_catalog.pg_get_serial_sequence('public.tweet_reactions','id') <> 'public.tweet_reactions_id_seq' then
    raise exception 'Reviewed reaction identity sequence changed';
  end if;
end
$$;

alter table public.tweet_reactions enable row level security;
revoke all privileges on table public.tweet_reactions from public, anon, authenticated;
revoke all privileges on sequence public.tweet_reactions_id_seq from public, anon, authenticated;

do $$
declare client_role text;
begin
  foreach client_role in array array['anon','authenticated']
  loop
    if pg_catalog.has_table_privilege(client_role,'public.tweet_reactions',
         'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
       or pg_catalog.has_sequence_privilege(client_role,'public.tweet_reactions_id_seq',
         'USAGE,SELECT,UPDATE') then
      raise exception 'Client reaction access remains for %',client_role;
    end if;
  end loop;
  if not pg_catalog.has_table_privilege('service_role','public.tweet_reactions','SELECT')
     or not pg_catalog.has_table_privilege('service_role','public.tweet_reactions','INSERT')
     or not pg_catalog.has_sequence_privilege('service_role','public.tweet_reactions_id_seq','USAGE') then
    raise exception 'Required server reaction access is missing';
  end if;
end
$$;
commit;
-- RLS without client policies is intentional: only the server handles reactions.
