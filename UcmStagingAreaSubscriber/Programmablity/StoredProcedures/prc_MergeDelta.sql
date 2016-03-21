
CREATE PROCEDURE dbo.prc_MergeDelta 
	@TargetTableName sysname,
	@BatchSize int OUTPUT,
	@CountInserts int OUTPUT,
	@CountUpdates int OUTPUT,
	@CountDeletes int OUTPUT,
	@CountSkippedByMerge int OUTPUT,
	@CountSkippedByDeDupe int OUTPUT,
	@NumGenerations int OUTPUT
AS

/******************************************************************************
** File:	prc_MergeDelta.sql
** Name:	dbo.prc_MergeDelta
** Desc:	This SP performs a delta merge between stage and target table.
**			The procedure generates SQL code for the merge process and executes it on fly
**			The merging process runs in a transaction
**			In case both staging and target table have ModifiedDtim column, only newer changes applied (i.e. staging record is newer than one in target table)
**			Some notes on the merge behavior:
**			- If the record does not exist in the target table and stage table has ActionFlag = 1 or >= 3 (insert or update), the record is inserted into the target table
**			- If the record exists in the target table and stage table has ActionFlag = 1 or >= 3 (insert or update), the record is updated in the target table
**			- If the record exists in the target table and stage table has ActionFlag = 2 (delete), the record is deleted from the target table
**			- All the other combinations between target and staging tables are ignored
**			The procedure throws and logs the folowing errors:
**			- Invalid argument
**			- Lack of staging and target table in the schema
**			- Incorrect name of primary keys in the MetaMerge table
** Params:	@TargetTableName	sysname -- The name of the target table in the MetaMerge table
** Returns: Statistics as OUTPUT parameters:
**				@BatchSize - record count in the queue table
**				@CountInserts - number of records inserted
**				@CountUpdates - number of records updated
**				@CountDeletes - number of records deleted
**				@CountSkippedByMerge - number of records skipped by the MERGE instruction (because of ModifiedDtim mistmatch, attempt to delete nonexisting record, etc)
**				@CountSkippedByDeDupe - number of records skipped by dedupe process (or NULL if dedupe is not performed)
**				@NumGenerations - number of generations into which the staging table input is divided (or NULL if dedupe is performed)
** Author:	nazaran
** Date:	03/23/2012
** ****************************************************************************
** CHANGE HISTORY
** ****************************************************************************
**	Date		Author		version	#bug	Description 
** ----------------------------------------------------------------------------------------------------------
** 04/27/2012	nazaran		1.1		N/A		Added statistical information output
** ----------------------------------------------------------------------------------------------------------
*/

BEGIN
	SET NOCOUNT ON
	SET XACT_ABORT ON
		
	DECLARE @SQL nvarchar(max) -- Generated SP SQL code
	
	BEGIN TRY
		EXEC dbo.prc_MergeDeltaGenCode @TargetTableName, @SQL OUTPUT -- Generate merging code to execute

		EXEC sys.sp_executesql
			@SQL,
			N'@BatchSize int OUTPUT, @CountInserts int OUTPUT, @CountUpdates int OUTPUT, @CountDeletes int OUTPUT, @CountSkippedByMerge int OUTPUT, @CountSkippedByDeDupe int OUTPUT, @NumGenerations int OUTPUT',
			@BatchSize = @BatchSize OUTPUT,
			@CountInserts = @CountInserts OUTPUT,
			@CountUpdates = @CountUpdates OUTPUT,
			@CountDeletes = @CountDeletes OUTPUT,
			@CountSkippedByMerge = @CountSkippedByMerge OUTPUT,
			@CountSkippedByDeDupe = @CountSkippedByDeDupe OUTPUT,
			@NumGenerations = @NumGenerations OUTPUT
	END TRY
	BEGIN CATCH
		IF (@@TRANCOUNT > 0)
			ROLLBACK TRAN

		DECLARE @ErrorMessage nvarchar(2048)
		SET @ErrorMessage = ERROR_MESSAGE()		
		RAISERROR(@ErrorMessage, 16, 1)
	END CATCH
END
