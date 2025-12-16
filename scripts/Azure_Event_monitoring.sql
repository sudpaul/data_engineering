DROP EVENT SESSION [monerror] ON DATABASE
go
CREATE EVENT SESSION [monerror] ON DATABASE
ADD EVENT sqlserver.error_reported(
    ACTION(package0.callstack_rva,sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.client_pid,sqlserver.database_id,sqlserver.database_name,sqlserver.session_id,sqlserver.sql_text,sqlserver.transaction_id,sqlserver.transaction_sequence,sqlserver.username)),
ADD EVENT sqlserver.rpc_completed(
    ACTION(package0.callstack_rva,sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.client_pid,sqlserver.database_id,sqlserver.database_name,sqlserver.session_id,sqlserver.sql_text,sqlserver.transaction_id,sqlserver.transaction_sequence,sqlserver.username)),
ADD EVENT sqlserver.rpc_starting(
    ACTION(package0.callstack_rva,sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.client_pid,sqlserver.database_id,sqlserver.database_name,sqlserver.session_id,sqlserver.sql_text,sqlserver.transaction_id,sqlserver.transaction_sequence,sqlserver.username)),
ADD EVENT sqlserver.sp_statement_completed(
    ACTION(package0.callstack_rva,sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.client_pid,sqlserver.database_id,sqlserver.database_name,sqlserver.session_id,sqlserver.sql_text,sqlserver.transaction_id,sqlserver.transaction_sequence,sqlserver.username)),
ADD EVENT sqlserver.sp_statement_starting(
    ACTION(package0.callstack_rva,sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.client_pid,sqlserver.database_id,sqlserver.database_name,sqlserver.session_id,sqlserver.sql_text,sqlserver.transaction_id,sqlserver.transaction_sequence,sqlserver.username)),
ADD EVENT sqlserver.sql_batch_completed(
    ACTION(package0.callstack_rva,sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.client_pid,sqlserver.database_id,sqlserver.database_name,sqlserver.session_id,sqlserver.sql_text,sqlserver.transaction_id,sqlserver.transaction_sequence,sqlserver.username)),
ADD EVENT sqlserver.sql_batch_starting(
    ACTION(package0.callstack_rva,sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.client_pid,sqlserver.database_id,sqlserver.database_name,sqlserver.session_id,sqlserver.sql_text,sqlserver.transaction_id,sqlserver.transaction_sequence,sqlserver.username)),
ADD EVENT sqlserver.sql_statement_completed(
    ACTION(package0.callstack_rva,sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.client_pid,sqlserver.database_id,sqlserver.database_name,sqlserver.session_id,sqlserver.sql_text,sqlserver.transaction_id,sqlserver.transaction_sequence,sqlserver.username)),
ADD EVENT sqlserver.sql_statement_starting(
    ACTION(package0.callstack_rva,sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.client_pid,sqlserver.database_id,sqlserver.database_name,sqlserver.session_id,sqlserver.sql_text,sqlserver.transaction_id,sqlserver.transaction_sequence,sqlserver.username))
ADD TARGET package0.ring_buffer(SET max_memory=(204800))
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=OFF)
GO
 