
CREATE PROCEDURE [dbo].[prc_TruncateTable]
	@TableName sysname
AS

/******************************************************************************
** File:	prc_TruncateTable.sql
** Name:	dbo.prc_TruncateTable
** Desc:	This SP truncates table in the same way TRUNCATE TABLE command does.
**			The difference is that truncate table also supports truncation of sysnonyms.
**			The procedure throws and logs the folowing errors:
**			- Invalid argument (NULL value passed, @TableName is not a table or synonym)
**			- Synonym is not pointing to a table
** Params:	@@TableName	sysname -- The name of the table or synonym to truncate
** Returns: Nothing
** Author:	nazaran
** Date:	05/31/2012
** ****************************************************************************
** CHANGE HISTORY
** ****************************************************************************
**	Date		Author		version	#bug	Description 
** ----------------------------------------------------------------------------------------------------------
** ----------------------------------------------------------------------------------------------------------
*/

BEGIN
	SET NOCOUNT ON

	DECLARE @ObjectID int, @BaseObjectName nvarchar(1035), @SQL nvarchar(2048)
	DECLARE @ExtMessage nvarchar(2048) -- Custom error text

	IF (@TableName IS NULL)
	BEGIN
		SET @ExtMessage = N'Invalid @TableName parameter value'
		RAISERROR(@ExtMessage, 15, 1) -- @TableName is null
	END

	SET @ObjectID = OBJECT_ID(@TableName, N'U') -- first check if @TableName is a table

	IF (NOT @ObjectID IS NULL)
	BEGIN
		SET @SQL = N'TRUNCATE TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(@ObjectID)) + N'.' + QUOTENAME(OBJECT_NAME(@ObjectID)) -- @TableName is an ordinary table, just truncate it
		EXEC(@SQL)
	END
	ELSE
	BEGIN
		SET @ObjectID = OBJECT_ID(@TableName, N'SN') -- now check if @TableName is a synonym
		IF (NOT @ObjectID IS NULL)
		BEGIN
			SELECT
				@BaseObjectName = s.[base_object_name]
				FROM sys.synonyms s
				WHERE (s.[object_id] = @ObjectID) -- find out the target where @TableName points

			IF (NOT @BaseObjectName IS NULL)
			BEGIN
				SET @ObjectID = OBJECT_ID(@BaseObjectName, N'U') -- check if @TableName points to a table
				IF (NOT @ObjectID IS NULL)
				BEGIN
					SET @SQL = N'TRUNCATE TABLE ' + @BaseObjectName
					EXEC(@SQL)
				END
				ELSE
				BEGIN
					SET @ExtMessage = N'Synonym @TableName = ' + ISNULL(@TableName, N'(null)') + N' is not pointing to a table'
					RAISERROR(@ExtMessage, 15, 1) -- @TableName is neither table, nor synonym
				END
			END
			ELSE
			BEGIN
				SET @ExtMessage = N'Synonym @TableName = ' + ISNULL(@TableName, N'(null)') + N' is not pointing anywhere'
				RAISERROR(@ExtMessage, 15, 1) -- @TableName is neither table, nor synonym
			END
		END
		ELSE
		BEGIN
			SET @ExtMessage = N'@TableName = ' + ISNULL(@TableName, N'(null)') + N' is neither table, nor synonym'
			RAISERROR(@ExtMessage, 15, 1) -- @TableName is neither table, nor synonym
		END
	END
END
