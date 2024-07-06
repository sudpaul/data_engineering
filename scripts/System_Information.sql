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

/* Reture the objects count in the Datawarehouse 
-- Select database name, schema name, object type description, object count, and row key from sys.objects */
SELECT 
    db_name() AS DBName,                     -- Database name
    s.name AS SchemaName,                    -- Schema name
    o.type_desc,                             -- Object type description
    COUNT(1) AS ObjectCount,                 -- Count of objects
    db_name() + '_' + s.name AS row_key      -- Row key (concatenation of database name and schema name)
FROM 
    sys.objects o
    INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
GROUP BY 
    s.name, 
    o.type_desc

UNION ALL

-- Select database name, schema name, 'STAT' as type description, object count, and row key from sys.stats
SELECT 
    db_name() AS DBName,                     -- Database name
    s.name AS SchemaName,                    -- Schema name
    'STAT' AS type_desc,                     -- 'STAT' type description for statistics
    COUNT(1) AS ObjectCount,                 -- Count of statistics
    db_name() + '_' + s.name AS row_key      -- Row key (concatenation of database name and schema name)
FROM 
    sys.objects o
    INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
    INNER JOIN sys.stats ss ON o.object_id = ss.object_id
GROUP BY 
    s.name

ORDER BY 
    db_name(),                               -- Order by database name
    s.name;                                  -- Order by schema name
