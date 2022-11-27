
SELECT DP1.name AS DatabaseRoleName
    ,isnull (DP2.name, 'No members') AS DatabaseUserName
    FROM sys.database_role_members AS DRM
      RIGHT OUTER JOIN sys.database_principals AS DP1
        ON DRM.role_principal_id = DP1.principal_id
      LEFT OUTER JOIN sys.database_principals AS DP2
        ON DRM.member_principal_id = DP2.principal_id
        WHERE DP1.type = 'R'
        ORDER BY DP1.name;

SELECT l.name as grantee
    ,l.type_desc
    ,p.permission_name
    ,p.state_desc
    ,GrantCmd = 'GRANT '+p.permission_name+' TO [];'
    FROM sys.database_permissions AS p
    INNER JOIN sys.database_principals AS l 
    ON p.grantee_principal_id = l.principal_id;

WITH    perms_cte AS
(
        SELECT USER_NAME(p.grantee_principal_id) AS principal_name
               ,dp.principal_id
               ,dp.type_desc AS principal_type_desc
               ,p.class_desc
               ,OBJECT_NAME(p.major_id) AS object_name
               ,p.permission_name,
                p.state_desc AS permission_state_desc
            FROM sys.database_permissions p
            INNER   JOIN sys.database_principals dp
            ON     p.grantee_principal_id = dp.principal_id
)
--role members
SELECT rm.member_principal_name
       ,rm.principal_type_desc
       ,p.class_desc
       ,p.object_name
       ,p.permission_name
       ,p.permission_state_desc
       ,rm.role_name
    FROM    perms_cte p
    RIGHT OUTER JOIN (
    SELECT role_principal_id
    ,dp.type_desc as principal_type_desc
    ,member_principal_id,user_name(member_principal_id) AS member_principal_name
    ,user_name(role_principal_id) as role_name--,*
        FROM  sys.database_role_members rm
         INNER   JOIN sys.database_principals dp
         ON     rm.member_principal_id = dp.principal_id
) rm
ON     rm.role_principal_id = p.principal_id
ORDER BY 1;

