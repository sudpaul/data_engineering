ALTER AUTHORIZATION ON SCHEMA::[ xpt] TO dbo
GO 

SELECT IS_MEMBER ('db_owner');  

select * from sys.schemas where principal_id in (select principal_id from sys.database_principals where name = /* schema_owner */ )
select * from sys.database_role_members
select * from sys.database_principals where name = /* schema_owner */

SELECT CAST(owner_sid as uniqueidentifier) AS Owner_SID   
FROM sys.databases   
WHERE name = --databasename;  


CREATE SCHEMA xpt
AUTHORIZATION dbo

select 'ALTER SCHEMA xpt TRANSFER xpt_temp.['+name+']'+char(13)+'GO'
from sys.objects where schema_id = (select schema_id from sys.schemas where name = 'xpt')

from information_schema.tables
where table_schema = 'xpt'

select *
from information_schema.tables
where table_schema = 'xpt'

drop schema xpt

alter schema xpt
set 



select * from sys.objects where schema_id = (select schema_id from sys.schemas where name = 'xpt')