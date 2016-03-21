
CREATE PROCEDURE [dbo].[prc_MergeDeltaGenCode] 
	@TargetTableName sysname,
	@SQL nvarchar(max) OUTPUT
AS

/******************************************************************************
** File:	prc_MergeDeltaGenCode.sql
** Name:	dbo.prc_MergeDeltaGenCode
** Desc:	This SP generates a code that performs a delta merge between stage and target table.
**			The procedure is used by prc_MergeDelta internally
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
**			- Incorrect name of primary keys in the AppStagingTable table
** Params:	@TargetTableName	sysname -- The name of the target table in the AppStagingTable table
			@SQL	nvarchar(max) OUTPUT -- The code generated
** Returns: Nothing. The generated payload is in the @SQL parameter
** TODO: should we determine DeDupe column type or bigint is appropriate?
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
	
	DECLARE @ExtMessage nvarchar(2048) -- Custom error text
	
	DECLARE @TableID int -- ID of the table in the AppStagingTable
	DECLARE @StagingTableName sysname -- Name of the staging table in the AppStagingTable
	DECLARE @MergeKeyColumnList nvarchar(max) -- List of primary key columns of the target table in the AppStagingTable
	DECLARE @DeDupe bit -- Flag if DeDupe required
	DECLARE @DeDupeColumnName sysname -- Name of the column to perform dedupe over
	DECLARE @TruncateOnCompletion bit -- Flag if truncation of the staging table is required
		
	DECLARE @TargetTableID int -- OBJECT_ID of the target table
	DECLARE @StagingTableID int -- OBJECT_ID of the staging table

	DECLARE @TargetQualifiedTableName nvarchar(512) -- Target table name in the [schema].[table] format
	DECLARE @StagingQualifiedTableName nvarchar(512) -- Target table name in the [schema].[table] format
	
	DECLARE @QuotedDeDupeColumnName nvarchar(258) -- @DeDupeColumnName quoted with QUOTENAME function

	DECLARE @PrimaryKeyColumns nvarchar(max) = N'' -- Primary key columns, qualified and separated by comma
	DECLARE @OtherColumns nvarchar(max) = N'' -- Non primary key columns, qualified and separated by comma

	DECLARE @QPrimaryKeyColumns nvarchar(max) = N'' -- Primary key columns, qualified and separated by comma with q prefix (i.e. q.[Column 1], q.[Column 2] etc)
	DECLARE @QOtherColumns nvarchar(max) = N'' -- Non primary key columns, qualified and separated by comma with q prefix (i.e. q.[Column 1], q.[Column 2] etc)

	DECLARE @SourcePrimaryKeyColumns nvarchar(max) = N'' -- Primary key columns, qualified and separated by comma with source prefix (i.e. source.[Column 1], source.[Column 2] etc)
	DECLARE @SourceOtherColumns nvarchar(max) = N'' -- Non primary key columns, qualified and separated by comma with source prefix (i.e. source.[Column 1], source.[Column 2] etc)

	DECLARE @PrimaryKeyJoinExpr nvarchar(max) = N'' -- Expression to joing target and staging tables
	DECLARE @AssignmentExpression nvarchar(max) = N'' -- Expression to UPDATE values in the target and staging tables
	DECLARE @OnePKAssignmentExpression nvarchar(max) = N'' -- Expression to UPDATE values in case only PKs are updated (updating the first PK column to invoke triggers)
	
	DECLARE @ColumnName sysname -- Current column name (used during cursor iterations)
	DECLARE @IsPrimaryKey bit -- Flag if the column is primary key (used during cursor iterations)
	DECLARE @IsBadColumnName bit -- Flag if the column name incorrect
	DECLARE @QuotedColumnName nvarchar(258) -- Column name quoted with QUOTENAME function
	
	DECLARE @HasModifiedTimestamp bit = 0 -- Flag if both tables have a ModifiedDtim column
	DECLARE @TimestampCheckExpression nvarchar(256) = N'' -- Expression to check if the staging record is newer than one in target table
	
	BEGIN TRY
	
		-- Try to get merge information from AppStagingTable
		SELECT
			@TableID = mt.TableId,
			@StagingTableName = mt.StagingTable,
			@MergeKeyColumnList = mt.MergeKeyColumnList,
			@DeDupe = mt.DeDupe,
			@DeDupeColumnName = mt.DedupeKeyColumn,
			@TruncateOnCompletion = mt.TruncateOnCompletion
		FROM
			dbo.AppStagingTable mt
		WHERE
			(mt.TargetTable = @TargetTableName)

		IF (@TableID IS NULL)
		BEGIN
			SET @ExtMessage = N'Invalid @TargetTableName parameter value'
			RAISERROR(@ExtMessage, 15, 1) -- Invalid parameter, @TargetTableName not set or not found in the AppStagingTable
		END
			
		-- Get IDs of the tables
		SET @TargetTableID = OBJECT_ID(@TargetTableName)
		IF (@TargetTableID IS NULL)
		BEGIN
			SET @ExtMessage = 'Target table ' + ISNULL(@TargetTableName, N'(NULL)') + ' not found'
			RAISERROR(@ExtMessage, 15, 1)
		END

		SET @StagingTableID = OBJECT_ID(@StagingTableName)
		IF (@StagingTableID IS NULL)
		BEGIN
			SET @ExtMessage = 'Staging table ' + ISNULL(@StagingTableName, N'(NULL)') + ' not found'
			RAISERROR(@ExtMessage, 15, 1)
		END

		-- Qualify table names
		SET @TargetQualifiedTableName = QUOTENAME(OBJECT_SCHEMA_NAME(@TargetTableID)) + N'.' + QUOTENAME(OBJECT_NAME(@TargetTableID))
		SET @StagingQualifiedTableName = QUOTENAME(OBJECT_SCHEMA_NAME(@StagingTableID)) + N'.' + QUOTENAME(OBJECT_NAME(@StagingTableID))
		
		-- Check and qualify dedupe columns
		IF (@DeDupe = 1)
		BEGIN
			IF (@DeDupeColumnName IS NULL)
			BEGIN
				SET @ExtMessage = N'DeDupe column missing'
				RAISERROR(@ExtMessage, 15, 1)
			END
			
		END

		SET @QuotedDeDupeColumnName = QUOTENAME(@DeDupeColumnName)
		
		DECLARE ColumnCursor CURSOR
			LOCAL FORWARD_ONLY FAST_FORWARD READ_ONLY
			FOR
				SELECT
					ISNULL(c.name, pk.string) AS ColumnName,
					CAST(CASE WHEN NOT pk.id IS NULL THEN 1 ELSE 0 END AS bit) AS IsPrimaryKey,
					CAST(CASE WHEN c.name IS NULL THEN 1 ELSE 0 END AS bit) AS IsBadColumnName
				FROM
						dbo.fn_BreakString(@MergeKeyColumnList, ',') pk
					FULL OUTER JOIN -- FULL outer join is required to provide a logging capabilities in case PK name are incorrect
						(
							SELECT
								ssc.name
							FROM
									sys.syscolumns tsc
								INNER JOIN
									sys.syscolumns ssc
								ON
										(tsc.name = ssc.name)
									AND
										(tsc.id = @TargetTableID)
									AND
										(ssc.id = @StagingTableID)
						) c
					ON
						(pk.string = c.name)

		OPEN ColumnCursor

		FETCH NEXT FROM ColumnCursor INTO @ColumnName, @IsPrimaryKey, @IsBadColumnName
		WHILE (@@FETCH_STATUS = 0)
		BEGIN
			IF (@IsBadColumnName = 1) -- PK key is incorrect
			BEGIN
				SET @ExtMessage = 'Key column ' + ISNULL(@ColumnName, N'(NULL)') + ' not found in the target or staging table'
				RAISERROR(@ExtMessage, 15, 1)
			END
		
			SET @QuotedColumnName = QUOTENAME(@ColumnName)

			-- Prepare strings with column names required to build a procedure
			IF (@IsPrimaryKey = 1)
			BEGIN
				SET @PrimaryKeyColumns = @PrimaryKeyColumns + CASE WHEN (LEN(@PrimaryKeyColumns) > 0) THEN N', ' ELSE N'' END + @QuotedColumnName
				SET @QPrimaryKeyColumns = @QPrimaryKeyColumns + CASE WHEN (LEN(@QPrimaryKeyColumns) > 0) THEN N', ' ELSE N'' END + N'q.' + @QuotedColumnName
				SET @SourcePrimaryKeyColumns = @SourcePrimaryKeyColumns + CASE WHEN (LEN(@SourcePrimaryKeyColumns) > 0) THEN N', ' ELSE N'' END + N'source.' + @QuotedColumnName
				
				SET @PrimaryKeyJoinExpr = @PrimaryKeyJoinExpr + CASE WHEN (LEN(@PrimaryKeyJoinExpr) > 0) THEN N' AND ' ELSE N'' END + N'(target.' + @QuotedColumnName + N' = source.' + @QuotedColumnName + N')'
				
				IF (LEN(@OnePKAssignmentExpression) = 0)
					SET @OnePKAssignmentExpression = @QuotedColumnName + N' = source.' + @QuotedColumnName
			END
			ELSE
			BEGIN
				SET @OtherColumns = @OtherColumns + CASE WHEN (LEN(@OtherColumns) > 0) THEN N', ' ELSE N'' END + @QuotedColumnName
				SET @QOtherColumns = @QOtherColumns + CASE WHEN (LEN(@QOtherColumns) > 0) THEN N', ' ELSE N'' END + N'q.' + @QuotedColumnName
				SET @SourceOtherColumns = @SourceOtherColumns + CASE WHEN (LEN(@SourceOtherColumns) > 0) THEN N', ' ELSE N'' END + N'source.' + @QuotedColumnName
				
				IF (@QuotedColumnName = N'[CustomerId]')
					SET @AssignmentExpression = @AssignmentExpression + CASE WHEN (LEN(@AssignmentExpression) > 0) THEN N', ' ELSE N'' END + N'[CustomerId] = ISNULL(NULLIF(source.[CustomerId], -1), target.[CustomerId])'
				ELSE
					SET @AssignmentExpression = @AssignmentExpression + CASE WHEN (LEN(@AssignmentExpression) > 0) THEN N', ' ELSE N'' END + @QuotedColumnName + N' = source.' + @QuotedColumnName
			END
			
			-- Check if it is a ModifiedDtim coulumn
			IF (@QuotedColumnName = N'[ModifiedDtim]')
				SET @HasModifiedTimestamp = 1

			FETCH NEXT FROM ColumnCursor INTO @ColumnName, @IsPrimaryKey, @IsBadColumnName
		END

		CLOSE ColumnCursor
		DEALLOCATE ColumnCursor
		
		-- Generate modified check code in case the appropriate columns exist
		IF (@HasModifiedTimestamp = 1)
			SET @TimestampCheckExpression = N'(source.[ModifiedDtim] >= target.[ModifiedDtim]) AND '

		-- Generate stored procedure code
		SET @SQL = N'			
				DECLARE @MergeResults table
					(
						act nvarchar(10)
					)
				
				SELECT
					@BatchSize = COUNT(*),
					@CountInserts = 0,
					@CountUpdates = 0,
					@CountDeletes = 0
				FROM ' +
					@StagingQualifiedTableName + N' q
				'

		IF (@DeDupe = 1)
		BEGIN
			-- Simply choose the latest one to process for each unique row in target table
			SET @SQL = @SQL + N'
				DECLARE @RowsToProcess table ' + -- Rows from the staging table which are needed to be processed with the corrected flag
					N'(
						PublisherId smallint NOT NULL,
						PubProcessId bigint NOT NULL,
						RowId bigint NOT NULL
					) 

				INSERT
					INTO @RowsToProcess
					(
						PublisherId,
						PubProcessId,
						RowId
					)
					SELECT
						x.PublisherId,
						x.PubProcessId,
						x.RowId
					FROM
						(
							SELECT
								q.PublisherId,
								q.PubProcessId,
								q.RowId,
								ROW_NUMBER() OVER (PARTITION BY ' + @QPrimaryKeyColumns + N' ORDER BY q.' + @QuotedDeDupeColumnName + N' DESC) AS RowNum
							FROM ' +
								@StagingQualifiedTableName + N' q
						) x
					WHERE
						(x.RowNum = 1)
				
				SET @CountSkippedByDeDupe = @BatchSize - @@ROWCOUNT
				
				BEGIN TRAN'
		END
		ELSE
		BEGIN
			SET @SQL = @SQL + N'
				DECLARE @RowsWithGenerationMarks table ' + -- Table variable which will hold "Generation" (sequence number when the given RowId can be merged in the target table)
					N'(
						PublisherId smallint NOT NULL,
						PubProcessId bigint NOT NULL,
						RowId bigint NOT NULL,
						Generation int NOT NULL
					)
				
				' +

				-- Compute "Generations" by getting their sequential id when partitioned by primary key	
				N'INSERT
					INTO @RowsWithGenerationMarks
					(
						PublisherId,
						PubProcessId,
						RowId,
						Generation
					)
					SELECT
						q.PublisherId,
						q.PubProcessId,
						q.RowId,
						ROW_NUMBER() OVER (PARTITION BY ' + @QPrimaryKeyColumns + N' ORDER BY ' + ISNULL(@QuotedDeDupeColumnName, 'q.[RowId]') + ') AS Generation
					FROM ' +
						@StagingQualifiedTableName + N' q
							
				DECLARE @Generation int
				
				' + 
					
				-- Determine number of "Generations"
				N'SELECT
					@Generation = 1,
					@NumGenerations = ISNULL(MAX(g.Generation), 0)
				FROM
					@RowsWithGenerationMarks g ' +
					
				-- It is safe to perform MERGE statement within the same "Generation" - so loop over all "Generations"
				N'
				
				BEGIN TRAN
				
				WHILE (@Generation <= @NumGenerations)
				BEGIN
					DELETE @MergeResults'
		END
		SET @SQL = @SQL + N'
				; WITH SourceData([ActionFlag], ' + @PrimaryKeyColumns + CASE WHEN (LEN(@OtherColumns) > 0) THEN N', ' + @OtherColumns ELSE N'' END + N')
				AS
				('
		IF (@DeDupe = 1)
		BEGIN
			SET @SQL = @SQL + N'
					SELECT
						q.[ActionFlag], ' + 
						@QPrimaryKeyColumns +
						CASE WHEN (LEN(@QOtherColumns) > 0) THEN N', ' + @QOtherColumns ELSE N'' END + N'
					FROM
							@RowsToProcess r
						INNER JOIN ' +
							@StagingQualifiedTableName + N' q
						ON
								(r.PublisherId = q.PublisherId)
							AND
								(r.PubProcessId = q.PubProcessId)
							AND
								(r.RowId = q.RowId)'		
		END
		ELSE
		BEGIN
			SET @SQL = @SQL + N'
					SELECT
						q.[ActionFlag], ' + 
						@QPrimaryKeyColumns +
						CASE WHEN (LEN(@QOtherColumns) > 0) THEN N', ' + @QOtherColumns ELSE N'' END + N'
					FROM
							@RowsWithGenerationMarks g
						INNER JOIN ' +
							@StagingQualifiedTableName + N' q
						ON
								(g.PublisherId = q.PublisherId)
							AND
								(g.PubProcessId = q.PubProcessId)
							AND
								(g.RowId = q.RowId)
							AND
								(g.Generation = @Generation)'				
		END
		SET @SQL = @SQL + N'
				)
				MERGE ' + 
						@TargetQualifiedTableName + N' AS target
					USING
						SourceData AS source
					ON ' +
						@PrimaryKeyJoinExpr + N'
				WHEN MATCHED AND (' + @TimestampCheckExpression + N'((source.ActionFlag = 1) OR (source.ActionFlag >= 3))) THEN
					UPDATE
					SET ' +
						CASE WHEN (LEN(@AssignmentExpression) > 0) THEN @AssignmentExpression ELSE @OnePKAssignmentExpression END + N'
				WHEN NOT MATCHED AND ((source.ActionFlag = 1) OR (source.ActionFlag >= 3)) THEN
					INSERT
						( ' +
							@PrimaryKeyColumns + 
							CASE WHEN (LEN(@OtherColumns) > 0) THEN N', ' + @OtherColumns ELSE N'' END + N'
						)
						VALUES
						( ' +
							@SourcePrimaryKeyColumns + 
							CASE WHEN (LEN(@SourceOtherColumns) > 0) THEN N', ' + @SourceOtherColumns ELSE N'' END + N'
						)
				WHEN MATCHED AND (' + @TimestampCheckExpression + N'(source.ActionFlag = 2)) THEN
					DELETE
				OUTPUT
					$action INTO @MergeResults;
				
				SELECT
					@CountInserts = @CountInserts + ISNULL(SUM(CASE WHEN (mr.act = N''INSERT'') THEN 1 ELSE 0 END), 0),
					@CountUpdates = @CountUpdates + ISNULL(SUM(CASE WHEN (mr.act = N''UPDATE'') THEN 1 ELSE 0 END), 0),
					@CountDeletes = @CountDeletes + ISNULL(SUM(CASE WHEN (mr.act = N''DELETE'') THEN 1 ELSE 0 END), 0)
				FROM
					@MergeResults mr'
			
		IF (@DeDupe = 0)
		BEGIN
			SET @SQL = @SQL + N'
				SET @Generation = @Generation + 1
			END'
		END
			
		IF (@TruncateOnCompletion = 1)
			SET @SQL = @SQL + N'		
				EXEC dbo.prc_TruncateTable ''' + @StagingQualifiedTableName + N''''
		
		SET @SQL = @SQL + N'
			COMMIT TRAN
			
			SET @CountSkippedByMerge = @BatchSize - @CountInserts - @CountUpdates - @CountDeletes - ISNULL(@CountSkippedByDeDupe, 0)'
	END TRY
	BEGIN CATCH
		IF (@@TRANCOUNT > 0)
			ROLLBACK TRAN

		DECLARE @ErrorMessage nvarchar(2048)
		SET @ErrorMessage = ERROR_MESSAGE()		
		RAISERROR(@ErrorMessage, 16, 1)
	END CATCH
END
