SELECT session_id,
       host_name,
       program_name,
       client_interface_name,
       login_name,
       status,
       login_time,
       last_request_start_time,
       last_request_end_time
FROM sys.dm_exec_sessions AS s
INNER JOIN sys.dm_resource_governor_workload_groups AS wg
ON s.group_id = wg.group_id
WHERE s.session_id <> @@SPID
      AND
      (
      (
      wg.name like 'UserPrimaryGroup.DB%'
      AND
      TRY_CAST(RIGHT(wg.name, LEN(wg.name) - LEN('UserPrimaryGroup.DB') - 2) AS int) = DB_ID()
      )
      OR
      wg.name = 'DACGroup'
      );