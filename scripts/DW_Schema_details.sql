--select top 10 * from [INFORMATION_SCHEMA].[COLUMNS];
--select top 10 * from [INFORMATION_SCHEMA].[TABLES];
--WITH CTE_TABLE
--AS
--(
--select t.table_schema, t.table_name, c.column_name from [INFORMATION_SCHEMA].[TABLES] t
--join [INFORMATION_SCHEMA].[COLUMNS] c on t.table_name = c.table_name
--and t.table_schema = c.table_schema
--where t.table_type = 'BASE TABLE'
--and t.table_schema not in ('dbo','dta','hst','stg','xpt','ins')
--and c.column_name not in ('MetaRunStepLogId','MetaRowId','MetaLoadDt')
--)
--select a.table_schema,a.table_name,a.column_name,b.table_name,b.column_name
--from CTE_TABLE a
--left join CTE_TABLE b on a.table_schema = b.table_schema 
--and a.column_name = b.column_name
--and a.table_name <> b.table_name
--order by a.table_schema, a.table_name, a.column_name

SELECT
    sm.[name] AS [schema_name],
    tb.[name] AS [table_name],
    co.[name] AS [stats_column_name],
    st.[name] AS [stats_name],
    STATS_DATE(st.[object_id],st.[stats_id]) AS [stats_last_updated_date]
FROM
    sys.objects ob
    JOIN sys.stats st
        ON  ob.[object_id] = st.[object_id]
    JOIN sys.stats_columns sc
        ON  st.[stats_id] = sc.[stats_id]
        AND st.[object_id] = sc.[object_id]
    JOIN sys.columns co
        ON  sc.[column_id] = co.[column_id]
        AND sc.[object_id] = co.[object_id]
    JOIN sys.types  ty
        ON  co.[user_type_id] = ty.[user_type_id]
    JOIN sys.tables tb
        ON  co.[object_id] = tb.[object_id]
    JOIN sys.schemas sm
        ON  tb.[schema_id] = sm.[schema_id]
WHERE
    st.[user_created] = 1 AND tb.[name] LIKE 'EBS%';