-- Approved by user on 2026-11-11; restrict only the two reviewed migration tables.
-- Applied migration version: 20261111145229.
-- Project: oslpycadjyhnldloxvyl (shared production database).
-- These tables are migration backup/reconciliation outputs, not IHC ranking tables.
-- Observed rows on 2026-11-11: backup=17; unmatched=0.
-- Usage found in musicite_ai/docs/BUZZTTARA_IMDB_MIGRATION.sql.
-- No runtime reference found in the inspected musicite_ai/IMDB/IHC_DB code.
-- A search cannot exclude unknown external consumers; confirm no public client needs them.
-- Deny all anon/authenticated access; preserve service_role/owner access and all data.
-- REVOKE ALL also removes TRUNCATE, which RLS alone does not protect.
begin;
set local lock_timeout = '2s';
set local statement_timeout = '15s';

do $$
declare
  target record;
  relation_oid oid;
begin
  for target in select * from (values
    ('public', 'groups_ticketdive_backup'),
    ('public', 'groups_ticketdive_unmatched')
  ) as targets(schema_name, table_name)
  loop
    relation_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', target.schema_name, target.table_name));
    if relation_oid is null then
      raise exception 'Missing reviewed table %.%', target.schema_name, target.table_name;
    end if;
    if (select c.relrowsecurity from pg_catalog.pg_class c where c.oid = relation_oid)
       or exists (select 1 from pg_catalog.pg_policy p where p.polrelid = relation_oid) then
      raise exception 'RLS state changed on %.%; stop and re-review', target.schema_name, target.table_name;
    end if;
  end loop;
  if not (select rolbypassrls from pg_catalog.pg_roles where rolname='service_role') then
    raise exception 'Expected service_role RLS bypass is absent; stop and re-review';
  end if;
end
$$;

alter table public.groups_ticketdive_backup enable row level security;
revoke all privileges on table public.groups_ticketdive_backup from anon, authenticated;

alter table public.groups_ticketdive_unmatched enable row level security;
revoke all privileges on table public.groups_ticketdive_unmatched from anon, authenticated;

-- Roll back rather than leave inherited client privileges or break service access.
do $$
declare
  relation_name text;
  role_name text;
begin
  foreach relation_name in array array['public.groups_ticketdive_backup','public.groups_ticketdive_unmatched']
  loop
    foreach role_name in array array['anon','authenticated']
    loop
      if pg_catalog.has_table_privilege(role_name, relation_name,
           'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN') then
        raise exception 'Client privileges remain on % for %', relation_name, role_name;
      end if;
    end loop;
    if not pg_catalog.has_table_privilege('service_role', relation_name, 'SELECT')
       or not pg_catalog.has_table_privilege('service_role', relation_name, 'INSERT')
       or not pg_catalog.has_table_privilege('service_role', relation_name, 'UPDATE')
       or not pg_catalog.has_table_privilege('service_role', relation_name, 'DELETE') then
      raise exception 'Expected service_role access missing on %', relation_name;
    end if;
  end loop;
end
$$;

commit;
-- After applying: expect two fewer rls_disabled errors.
-- No RLS policies is intentional for these internal-only tables.
-- Verify anon/authenticated SELECT denied and no DML/TRUNCATE grants remain.
-- Verify service_role access and row counts (17/0) unchanged.
-- tweet_reactions is intentionally excluded: deployed service-role configuration
-- and the musicite reaction route must be verified before changing its access.
