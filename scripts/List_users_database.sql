-- SELECT * FROM sys.database_principals; 
/* View members list of a group in SQL Server Database

username - UserName

create_date - Date the account was added
modify_date - Date the account was last updated

type_desc - Principal type:
- CERTIFICATE_MAPPED_USER - User mapped to a certificate
- EXTERNAL_USER - External user from Azure Active Directory
- ASYMMETRIC_KEY_MAPPED_USER - User mapped to an asymmetric key
- SQL_USER - SQL user
- WINDOWS_USER - Windows user

authentication_type - type of user authentication
- NONE : No authentication
- INSTANCE : Instance authentication
- DATABASE : Database authentication
- WINDOWS : Windows Authentication

*/
SELECT name AS Username,
    create_date AS CreateDate,
    modify_date AS ModifyDate,
    type_desc AS Type,
    authentication_type_desc AS Authentication_Type
FROM sys.database_principals
WHERE type NOT IN ('A', 'G','R')
    AND sid IS NOT NUll
    AND name != 'guest'
ORDER BY Username, ModifyDate DESC;