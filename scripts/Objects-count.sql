SELECT db_name() AS DBName, s.name AS SchemaName 
, o.type_desc, count(1) AS ObjectCount, db_name() + '_' + s.name AS row_key
    FROM sys.objects o
     INNER JOIN sys.schemas s 
        ON o.schema_id = s.schema_id
            GROUP BY s.name, o.type_desc
UNION ALL
SELECT db_name() as DBName, s.name as SchemaName 
, 'STAT' as type_desc, count(1) as ObjectCount, db_name() + '_' + s.name as row_key
    FROM sys.objects o
     INNER JOIN sys.schemas s 
      ON o.schema_id = s.schema_id
       INNER JOIN sys.stats  ss
         ON o.object_id = ss.object_id
            GROUP BY  s.name
                ORDER BY db_name(), s.name;