DECLARE @TableName NVARCHAR(128) = --'Location'; -- Replace with the actual table name

SELECT 
    OBJECT_NAME(object_id) AS TableName,
    SUM(CASE WHEN index_id IN (0, 1) THEN row_count END) AS TotalRows,
    SUM(CASE WHEN index_id = 0 THEN row_count END) AS InsertedRows,
    SUM(CASE WHEN index_id > 1 THEN row_count END) AS UpdatedRows,
    SUM(CASE WHEN index_id = 1 THEN row_count END) AS DeletedRows
FROM 
    sys.dm_db_partition_stats
WHERE 
    OBJECT_NAME(object_id) = @TableName
GROUP BY 
    object_id;
