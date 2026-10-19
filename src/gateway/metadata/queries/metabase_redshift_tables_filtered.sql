select
  s.name,
  s.schema,
  s.type,
  s.description,
  pg_catalog.has_table_privilege(s.oid, 'SELECT')
    or pg_catalog.has_any_column_privilege(s.oid, 'SELECT') as selectable
from (
  select
    c.oid,
    c.relname as name,
    n.nspname as schema,
    case c.relkind
      when 'r' then 'table'
      when 'p' then 'partitioned table'
      when 'v' then 'view'
      when 'f' then 'foreign table'
      when 'm' then 'materialized view'
      end as type,
    d.description
  from pg_catalog.pg_namespace n, pg_catalog.pg_class c
  left join pg_catalog.pg_description d on c.oid = d.objoid and d.objsubid = 0
  left join pg_catalog.pg_class dc on d.classoid=dc.oid and dc.relname='pg_class'
  left join pg_catalog.pg_namespace dn on dn.oid=dc.relnamespace and dn.nspname='pg_catalog'
  where c.relnamespace = n.oid
    and n.nspname !~ '^information_schema|catalog_history|pg_|metabase_cache_'
    and c.relkind in ('r', 'p', 'v', 'f', 'm')
    and n.nspname in ('__YASP_SCHEMAS__')
    and pg_catalog.has_schema_privilege(n.oid, 'USAGE')
) s
union all
select
  tablename as name,
  schemaname as schema,
  'EXTERNAL TABLE' as type,
  null as description,
  true as selectable
from svv_external_tables t
where schemaname !~ '^information_schema|catalog_history|pg_|metabase_cache_'
  and t.schemaname in ('__YASP_SCHEMAS__')
  and pg_catalog.has_schema_privilege(t.schemaname, 'USAGE')
