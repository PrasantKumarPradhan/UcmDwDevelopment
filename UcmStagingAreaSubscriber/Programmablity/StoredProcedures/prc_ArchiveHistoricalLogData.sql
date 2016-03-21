
CREATE PROC dbo.prc_ArchiveHistoricalLogData
AS 
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	SET DEADLOCK_PRIORITY 'LOW'

	BEGIN TRY

		--** Prepare for delete processing	
		DECLARE
			@varArchivalPeriod INT
			, @varBatchSize INT
			, @varColumnName SYSNAME
			, @varDeleteCount BIGINT
			, @varDT DATETIME
			, @varObjectName SYSNAME
			, @varObjectSchema SYSNAME
			, @varQuery NVARCHAR(MAX)
			, @varRowCount BIGINT
			, @varRowID INT
			, @varVariables NVARCHAR(MAX)
			, @ErrorMsg varchar(max)	
			, @varParentObjectName SYSNAME
			, @ClosingParanthesisCount INT
			, @varCurrentObjectSchema SYSNAME
			, @varCurrentObjectName SYSNAME
		
		DECLARE @Table TABLE
		(
			  [RowId] INT PRIMARY KEY NOT NULL
			, [ObjectSchema] SYSNAME NOT NULL
			, [ObjectName] SYSNAME NOT NULL
			, [ColumnName] SYSNAME	NOT NULL
			, [ParentObjectName] SYSNAME NULL
		)

		--** Gather configuration values
		SELECT
			@varArchivalPeriod = CAST(ISNULL([dbo].[fn_SystemParameter]('Staging Area Settings', 'ArchivalPeriod'), 365) AS INT)
			, @varBatchSize = CAST(ISNULL([dbo].[fn_SystemParameter]('Staging Area Settings', 'CleanupBatchSize'), 10000) AS INT)
			, @varDeleteCount = 0
			, @varDT = GETUTCDATE()
			, @varVariables = N'@BatchSize INT, @DT DATETIME, @ArchivalPeriod INT'
			
		--** The sequence of the following INSERT statements is critical in order to enforce
		--** parent-child relationships when deleting data.
		--** If you add tables to this collection where a parent-child relationship exists,
		--** ensure that you list the child tables BEFORE the parent tables.
		--** and use ParentObjectName column 
				
		INSERT INTO @Table 
		(
			  [RowId]
			, [ObjectSchema]
			, [ObjectName]
			, [ColumnName]
			, [ParentObjectName]
		)
		
		SELECT 1,  N'dbo', N'APPStagingProcessQueue', N'ProcessId', N'APPStagingProcess' UNION
		SELECT 2,  N'dbo', N'APPStagingProcess', N'ProcessStartedDTim', NULL 		

		--** Remove old data from all tables
		DECLARE CleanupCursor CURSOR
			LOCAL
			FAST_FORWARD
		FOR
			SELECT
				  [RowID]
				, [ObjectSchema]
				, [ObjectName]
				, [ColumnName]
				, [ParentObjectName]
			FROM @Table
			ORDER BY 1 ASC -- ORDER BY is used to enforce the delete sequence

		OPEN CleanupCursor

		--** Get the next row for processing
		FETCH NEXT FROM CleanupCursor
		INTO
			  @varRowID
			, @varObjectSchema
			, @varObjectName
			, @varColumnName
			, @varParentObjectName

		WHILE @@FETCH_STATUS = 0
		BEGIN
			SELECT 
				@varCurrentObjectName = @varObjectName,
				@varCurrentObjectSchema =  @varObjectSchema

			--** Delete in batches
			SET @varDeleteCount = 0
				
			IF @varParentObjectName IS NULL
			BEGIN
				SET @varQuery = N'DELETE TOP (@BatchSize) FROM [' + @varObjectSchema + N'].[' + @varObjectName + N'] WHERE DATEDIFF(dd, [' + @varColumnName + N'], @DT) >= @ArchivalPeriod'	
			END
			ELSE
			BEGIN		
				
				SET @varQuery = N'DELETE TOP (@BatchSize) FROM [' + @varObjectSchema + N'].[' + @varObjectName + N'] WHERE ' + @varColumnName + N' IN(SELECT ' + @varColumnName + 'ClosingParanthesis'
				WHILE @varParentObjectName IS NOT NULL
				BEGIN		
					
					--Get Parent Record
					SELECT 
						  @varObjectSchema = ObjectSchema
						, @varObjectName   = ObjectName
						, @varColumnName   = ColumnName
						, @varParentObjectName = ParentObjectName						
					FROM @Table 
					WHERE ObjectName = @varParentObjectName
					
					IF @varParentObjectName IS NOT NULL
						SET @varQuery = @varQuery + N' FROM [' + @varObjectSchema + N'].[' + @varObjectName + N'] WHERE ' + @varColumnName + N' IN(SELECT ' + @varColumnName + 'ClosingParanthesis'
					ELSE			
						SET @varQuery = @varQuery + N' FROM [' + @varObjectSchema + N'].[' + @varObjectName + N'] WHERE DATEDIFF(dd, [' + @varColumnName + N'], @DT) >= @ArchivalPeriod'
				END --END WHILE
			END --END IF-ELSE
						
			--Set the clsoing parenthesis properly
			SET @ClosingParanthesisCount = 0
			SELECT @ClosingParanthesisCount = (LEN(@varQuery) - LEN(REPLACE(@varQuery, 'ClosingParanthesis', '')))/LEN('ClosingParanthesis') 
			SELECT @varQuery = REPLACE(@varQuery,'ClosingParanthesis','') + REPLICATE (')',@ClosingParanthesisCount) 
			
			--PRINT @varQuery
				
			BEGIN TRY
			
				EXEC sys.sp_executesql
					@varQuery
					, @varVariables
					, @BatchSize = @varBatchSize
					, @DT = @varDT
					, @ArchivalPeriod = @varArchivalPeriod
				
				SET @varRowCount = @@ROWCOUNT
					
			END TRY
				
			BEGIN CATCH
				SET @varRowCount = 0
				IF ERROR_NUMBER() <> 547 -- ignore FK constraint violation
				BEGIN 
					SET @ErrorMsg = ERROR_MESSAGE()
					RAISERROR(@ErrorMsg, 16, 1)
				END
			END CATCH

			SET @varDeleteCount = @varDeleteCount + @varRowCount
			
			WHILE @varRowCount > 0
			BEGIN
				BEGIN TRY
					EXEC sys.sp_executesql
						@varQuery
						, @varVariables
						, @BatchSize = @varBatchSize
						, @DT = @varDT
						, @ArchivalPeriod = @varArchivalPeriod
					SET @varRowCount = @@ROWCOUNT
				END TRY
				BEGIN CATCH
					SET @varRowCount = 0
					IF ERROR_NUMBER() <> 547 -- ignore FK constraint violation
					BEGIN 
						SET @ErrorMsg = error_message()
						RAISERROR(@ErrorMsg, 16, 1)
					END
				END CATCH				

				SET @varDeleteCount = @varDeleteCount + @varRowCount
			END
			
			PRINT @varCurrentObjectSchema + '.' + @varCurrentObjectName + ':' + convert(varchar,@varDeleteCount) + ' Records Deleted.'

			--** Get the next row for processing
			FETCH NEXT FROM CleanupCursor
			INTO
				  @varRowID
				, @varObjectSchema
				, @varObjectName
				, @varColumnName
				, @varParentObjectName
		END

		CLOSE CleanupCursor
		DEALLOCATE CleanupCursor

	END TRY

	BEGIN CATCH
	
		SELECT @ErrorMsg = ERROR_MESSAGE()
		
		IF @@TRANCOUNT <> 1
			ROLLBACK TRAN
		
		DECLARE @CursorStatus INT

		SELECT @CursorStatus = CURSOR_STATUS('global','CleanupCursor')		
		IF @CursorStatus = 1 --Defined and Opened
		BEGIN
			CLOSE CleanupCursor
			DEALLOCATE CleanupCursor
		END
		ELSE IF @CursorStatus = -1 --Defined but not opened
			DEALLOCATE CleanupCursor

		RAISERROR(@ErrorMsg, 16, 1)

	END CATCH

END