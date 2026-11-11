-- Approved by user on 2026-11-11; preserve the reviewed public read access.
-- Applied migration version: 20261111145318.
-- Project: oslpycadjyhnldloxvyl (shared production database).
-- RLS warning: https://supabase.com/docs/guides/database/database-linter?lint=0013_rls_disabled_in_public
-- Preserve existing unrestricted SELECT for public ranking/master data.
-- No INSERT/UPDATE/DELETE policies or new GRANTs; service_role continues to bypass RLS.
-- Five tables have anon/authenticated SELECT grants.
-- daily_top20 also has a pre-existing PUBLIC SELECT grant, so preserve it.
-- Does not make existing publicly readable data private.
-- Existing security_invoker views in ICES/ICNS retain their SELECT behavior.
-- Expected impact: short DDL locks; abort on lock contention. Confirm public visibility
-- of master metadata/profile content is intentional before approving.
begin;
set local lock_timeout = '2s';
set local statement_timeout = '15s';

do $$
declare
  target record;
  relation_oid oid;
begin
  for target in select * from (values
    ('ihc', 'daily_rankings'),
    ('ihc', 'daily_top20'),
    ('imd', 'groups'),
    ('imd', 'external_ids'),
    ('imd', 'group_attributes'),
    ('imd', 'group_profiles')
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

create policy security_review_public_read on ihc.daily_rankings
  for select to anon, authenticated using (true);
alter table ihc.daily_rankings enable row level security;

create policy security_review_public_read on ihc.daily_top20
  for select to public using (true);
alter table ihc.daily_top20 enable row level security;

create policy security_review_public_read on imd.groups
  for select to anon, authenticated using (true);
alter table imd.groups enable row level security;

create policy security_review_public_read on imd.external_ids
  for select to anon, authenticated using (true);
alter table imd.external_ids enable row level security;

create policy security_review_public_read on imd.group_attributes
  for select to anon, authenticated using (true);
alter table imd.group_attributes enable row level security;

create policy security_review_public_read on imd.group_profiles
  for select to anon, authenticated using (true);
alter table imd.group_profiles enable row level security;

-- Assert the effective client privileges still match the approved read-only model.
do $$
declare
  relation_name text;
  role_name text;
begin
  foreach relation_name in array array['ihc.daily_rankings','ihc.daily_top20','imd.groups','imd.external_ids','imd.group_attributes','imd.group_profiles']
  loop
    foreach role_name in array array['anon','authenticated']
    loop
      if not pg_catalog.has_table_privilege(role_name, relation_name, 'SELECT')
         or pg_catalog.has_table_privilege(role_name, relation_name,
           'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN') then
        raise exception 'Unexpected client privileges on % for %', relation_name, role_name;
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
-- Applied after backup restriction: rerun advisors; expect rls_disabled count 7 -> 1.
-- Compare anonymous and authenticated SELECT results before/after.
-- Verify daily/weekly ranking pages, ICES/ICNS views, and service-role job access.
-- Do not run ranking jobs as a smoke test: they can recalculate production data.
