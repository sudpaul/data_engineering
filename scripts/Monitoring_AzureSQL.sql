-- Calculating database size for Azure SQL Database
SELECT SUM(reserved_page_count)*8.0/1024 AS DBSize
FROM sys.dm_db_partition_stats;

-- Get the size of individual objects (in megabytes) in Azure database:

SELECT sys.objects.name AS Name, SUM(reserved_page_count) * 8.0 / 1024 SIZE_MB
FROM sys.dm_db_partition_stats, sys.objects
WHERE sys.dm_db_partition_stats.object_id = sys.objects.object_id
GROUP BY sys.objects.name

ORDER BY SIZE_MB DESC;


--Monitor database connections in Azure SQL Database

SELECT
   c.session_id, c.net_transport, c.encrypt_option,
   c.auth_scheme, s.host_name, s.program_name,
   s.client_interface_name, s.login_name, s.nt_domain,
   s.nt_user_name, s.original_login_name, c.connect_time,
   s.login_time
FROM sys.dm_exec_connections AS c
JOIN sys.dm_exec_sessions AS s
   ON c.session_id = s.session_id

-- Top queries ranked by average CPU time  

SELECT TOP 10 query_stats.query_hash AS "Query Hash",
   SUM(query_stats.total_worker_time) / SUM(query_stats.execution_count) AS "Avg CPU Time",
   MIN(query_stats.statement_text) AS "Statement Text"
FROM
   (SELECT QS.*,
   SUBSTRING(ST.text, (QS.statement_start_offset/2) + 1,
   ((CASE statement_end_offset
        WHEN -1 THEN DATALENGTH(ST.text)
        ELSE QS.statement_end_offset END
            - QS.statement_start_offset)/2) + 1) AS statement_text
    FROM sys.dm_exec_query_stats AS QS
    CROSS APPLY sys.dm_exec_sql_text(QS.sql_handle) as ST) as query_stats
GROUP BY query_stats.query_hash
ORDER BY 2 DESC;


--Monitoring query plans in Azure SQL Database

SELECT
   highest_cpu_queries.plan_handle,
   highest_cpu_queries.total_worker_time,
   q.dbid,
   q.objectid,
   q.number,
   q.encrypted,
   q.[text]
FROM
   (SELECT TOP 50
        qs.plan_handle,
        qs.total_worker_time
   FROM
        sys.dm_exec_query_stats qs
   ORDER BY qs.total_worker_time desc) AS highest_cpu_queries
   CROSS APPLY sys.dm_exec_sql_text(plan_handle) AS q
ORDER BY highest_cpu_queries.total_worker_time DESC;