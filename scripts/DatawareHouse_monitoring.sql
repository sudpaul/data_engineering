/* Monitor active queries */
--SELECT *
--FROM sys.dm_pdw_exec_requests
--WHERE status not in ('Completed','Failed','Cancelled')
--  AND session_id <> session_id()
--ORDER BY submit_time DESC;

/* Find top 10 queries longest running queries */

--SELECT TOP 10 *
--FROM sys.dm_pdw_exec_requests
--ORDER BY total_elapsed_time DESC;

/* To see the active and recently completed queries associated with a session, use the sys.dm_pdw_exec_sessions and sys.dm_pdw_exec_requests views.
This query returns a list of all active or idle sessions, plus any active or recent queries associated with each session ID.*/

--SELECT es.session_id, es.login_name, es.status AS sessionStatus,   
--er.request_id, er.status AS requestStatus, er.command   
--FROM sys.dm_pdw_exec_sessions es   
--LEFT OUTER JOIN sys.dm_pdw_exec_requests er   
--ON (es.session_id=er.session_id)   
--WHERE (es.status='Active' OR es.status='Idle') AND   
--(er.status!= 'Completed' AND er.status!= 'Failed' AND er.status!= 'Cancelled');  

/* To find a session based on the login name. */

--SELECT session_id, login_name, status FROM sys.dm_pdw_exec_sessions;  

-- Find the distributed query plan steps for a specific query.
-- Replace request_id with value of QID from request_d.

--SELECT * FROM sys.dm_pdw_request_steps
--WHERE request_id = 'QID804239290'
--ORDER BY step_index;

-- Memory consumption

--SELECT
--  pc1.cntr_value as Curr_Mem_KB,
--  pc1.cntr_value/1024.0 as Curr_Mem_MB,
--  (pc1.cntr_value/1048576.0) as Curr_Mem_GB,
--  pc2.cntr_value as Max_Mem_KB,
--  pc2.cntr_value/1024.0 as Max_Mem_MB,
--  (pc2.cntr_value/1048576.0) as Max_Mem_GB,
--  pc1.cntr_value * 100.0/pc2.cntr_value AS Memory_Utilization_Percentage,
--  pc1.pdw_node_id
--FROM
---- pc1: current memory
--sys.dm_pdw_nodes_os_performance_counters AS pc1
---- pc2: total memory allowed for this SQL instance
--JOIN sys.dm_pdw_nodes_os_performance_counters AS pc2
--ON pc1.object_name = pc2.object_name AND pc1.pdw_node_id = pc2.pdw_node_id
--WHERE
--pc1.counter_name = 'Total Server Memory (KB)'
--AND pc2.counter_name = 'Target Server Memory (KB)'

-- Transaction log size

--SELECT
--  instance_name as distribution_db,
--  cntr_value*1.0/1048576 as log_file_size_used_GB,
--  pdw_node_id
--FROM sys.dm_pdw_nodes_os_performance_counters
--WHERE
--instance_name like 'Distribution_%'
--AND counter_name = 'Log File(s) Used Size (KB)'

/*Monitoring the Polybase Load.
To track bytes and files */

--SELECT
--    r.command,
--    s.request_id,
--    r.status,
--    count(distinct input_name) as nbr_files,
--    sum(s.bytes_processed)/1024/1024/1024 as gb_processed
--FROM
--    sys.dm_pdw_exec_requests r
--    inner join sys.dm_pdw_dms_external_work s
--        on r.request_id = s.request_id
--GROUP BY
--    r.command,
--    s.request_id,
--    r.status
--ORDER BY
--    nbr_files desc,
--    gb_processed desc;

/* This query runs against the user database and returns all information regarding WorkloadGroups provisioned on the 
					database.
*/
select 
	effective_min_percentage_resource, 
	effective_Cap_Percentage_resource, 
	effective_request_min_resource_grant_percent, 
	effective_request_max_resource_grant_percent, * 
from sys.dm_workload_management_workload_groups_stats
order by group_id;