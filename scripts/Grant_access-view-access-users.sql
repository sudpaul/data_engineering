
--CREATE USER [user_login_email] FROM  EXTERNAL PROVIDER  WITH DEFAULT_SCHEMA=[dbo]
--GO


--GRANT SELECT ON OBJECT::[xpt].[STATS_NAPLAN_PARTICIPATION] TO [user_login_email]
--GRANT SELECT ON OBJECT::[xpt].[STATS_NAPLAN_SCORE_DoE]  TO [user_login_email]

--GRANT SELECT ON OBJECT::[xpt].[STATS_NAPLAN_PARTICIPATION] TO [user_login_details]
--GRANT SELECT ON OBJECT::[xpt].[STATS_NAPLAN_SCORE_DoE]  TO [user_login_details]

/* View the user access to the database objects */

SELECT pr.principal_id, 
       pr.name, 
       pr.type_desc, 
       pr.authentication_type_desc, 
	   pe.state_desc, 
	   pe.permission_name, 
	   OBJECT_NAME(major_id) objectName
FROM sys.database_principals AS pr
     JOIN sys.database_permissions AS pe ON pe.grantee_principal_id = pr.principal_id
--INNER JOIN sys.schemas AS s ON s.principal_id =  sys.database_role_members.role_principal_id 
     where pr.name in ('user_login_email','user_login_details') 