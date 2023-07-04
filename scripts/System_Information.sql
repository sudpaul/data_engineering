/* Query to check object dependencies in */
SELECT * FROM sys.sql_expression_dependencies  
WHERE referencing_id = OBJECT_ID(--reference_object);

/* Rename a table */
EXEC sp_rename --Old_name, 'new_Name';

/* Spance Used by a Table */

EXEC SP_SPACEUSED '@SchemaName.@TableName'

/*Return all the objects that have been modified in the last N days */

SELECT name AS object_name
  ,SCHEMA_NAME(schema_id) AS schema_name
  ,type_desc
  ,create_date
  ,modify_date
FROM sys.objects
WHERE modify_date > GETDATE() - 3
ORDER BY modify_date;

/* Return all the user-defined functions in a database */

SELECT name AS function_name
  ,SCHEMA_NAME(schema_id) AS schema_name
  ,type_desc
  ,create_date
  ,modify_date
FROM sys.objects
WHERE type_desc LIKE '%FUNCTION%' ORDER BY create_date DESC;

/* . Return the parameters for a specified stored procedure or function */

SELECT SCHEMA_NAME(schema_id) AS schema_name
    ,o.name AS object_name
    ,o.type_desc
    ,p.parameter_id
    ,p.name AS parameter_name
    ,TYPE_NAME(p.user_type_id) AS parameter_type
    ,p.max_length
    ,p.precision
    ,p.scale
    ,p.is_output
FROM sys.objects AS o
INNER JOIN sys.parameters AS p ON o.object_id = p.object_id
WHERE o.object_id = OBJECT_ID('<schema_name.object_name>')
ORDER BY schema_name, object_name, p.parameter_id;

/* Reture the objects count in the Datawarehouse */
select
  db_name() as DBName,
  s.name as SchemaName,
  o.type_desc,
  count(1) as ObjectCount,
  db_name() + '_' + s.name as row_key
from
  sys.objects o
  inner join sys.schemas s ON o.schema_id = s.schema_id
group by
  s.name,
  o.type_desc
UNION ALL
select
  db_name() as DBName,
  s.name as SchemaName,
  'STAT' as type_desc,
  count(1) as ObjectCount,
  db_name() + '_' + s.name as row_key
from
  sys.objects o
  inner join sys.schemas s ON o.schema_id = s.schema_id
  inner join sys.stats ss ON o.object_id = ss.object_id
group by
  s.name
ORDER BY
  db_name(),
  s.name;