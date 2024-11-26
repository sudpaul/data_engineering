-- Create a stored procedure to grant access to a schema
CREATE OR ALTER PROCEDURE dbo.USP_GrantAccessToSchema
    @IdentityName NVARCHAR(50),
    @SchemaName NVARCHAR(30)
AS
BEGIN
    DECLARE @SqlCommand NVARCHAR(MAX);

    -- Drop the existing user (if needed)
    -- SET @SqlCommand = 'DROP USER ' + QUOTENAME(@IdentityName);
    -- EXEC sp_executesql @SqlCommand;

    -- Create a contained user for the managed identity
    SET @SqlCommand = 'CREATE USER ' + QUOTENAME(@IdentityName) + ' FROM EXTERNAL PROVIDER';
    EXEC sp_executesql @SqlCommand;
    SET @SqlCommand = 'ALTER ROLE db_datareader ADD MEMBER ' + QUOTENAME(@IdentityName);
    EXEC sp_executesql @SqlCommand;
    SET @SqlCommand = 'ALTER ROLE db_datawriter ADD MEMBER ' + QUOTENAME(@IdentityName);
    EXEC sp_executesql @SqlCommand;

    -- Grant permissions on the schema
    SET @SqlCommand = 'GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::' + QUOTENAME(@SchemaName) + ' TO ' + QUOTENAME(@IdentityName);
    EXEC sp_executesql @SqlCommand;
    -- You can add more permissions as needed

    -- Optionally, grant EXECUTE permission on stored procedures within the schema
    SET @SqlCommand = 'GRANT EXECUTE ON SCHEMA::' + QUOTENAME(@SchemaName) + ' TO ' + QUOTENAME(@IdentityName);
    EXEC sp_executesql @SqlCommand;
END;