
CREATE PROCEDURE dbo.prc_APPStagingProcessQueueEnd
   @ProcessId bigint,
   @QueueName sysname,
   @ErrMsg varchar(4000) = NULL
AS
BEGIN
	declare @TableId int, @ExistingPubProcessId bigint, @PubProcessId bigint, @ProcessQueueId bigint, @PublisherId tinyint, @LastRowId bigint, @LastArchivedDtim datetime

	declare @TargetTable sysname, @StagingTable sysname
	declare @AutoMerge bit, @MergeProcedure sysname
	declare @BatchSize int, @CountInserts int, @CountUpdates int, @CountDeletes int, @CountSkippedByMerge int, @CountSkippedByDeDupe int, @NumGenerations int

	declare @sql nvarchar(400)
	
	begin try

		select
			@ExistingPubProcessId = p.PubProcessId
		from
			dbo.APPStagingProcess p with (nolock)
		where
			p.ProcessId = @ProcessId

		if @@RowCount = 0
			raiserror(N'Invalid value for @ProcessId parameter. Either NULL or parent process does not exist', 15, 1)

		select 
			@TableId = ast.TableId,
			@StagingTable = ast.StagingTable, 
			@TargetTable = ast.TargetTable,
			@AutoMerge = ast.AutoMerge,
			@MergeProcedure = ast.MergeProcedure
		from dbo.AppStagingTable ast with (nolock)
		where (ast.StagingTable = @QueueName)

		if (@TableId IS NULL)
			raiserror(N'Invalid @QueueName parameter value', 15, 1)

		if ((@AutoMerge = 0) AND (@MergeProcedure IS NULL))
			raiserror(N'@AutoMerge = 0, but @MergeProcedure IS NULL', 15, 1)

		select @ProcessQueueId = ProcessQueueId 
		from dbo.AppStagingProcessQueue with (nolock)
		where ProcessId = @ProcessId and TableId = @TableId

		if (@ProcessQueueId IS NULL)
			raiserror(N'No ProcessQueue exists', 15, 1)		

		if (@ErrMsg is null)
		begin

			If Exists (select 1 from sys.synonyms where name = @StagingTable)
			begin
				DECLARE @DbName SYSNAME,
						@TableName SYSNAME,
						@ArchivedCol SYSNAME,
						@SQLQuery NVARCHAR(1024)

				SELECT @DbName = PARSENAME(base_object_name,3),@TableName = PARSENAME(base_object_name,1)
				FROM sys.synonyms WHERE name = @StagingTable
				
				SET @SQLQuery = N'Use ' + @DbName + ' select @ArchivedColOUT = c.Name from sys.objects o join sys.columns c on o.object_id = c.object_id where o.name = ''' + @TableName
							+ ''' and c.name=''ArchivedDTim'''
								
				exec sys.sp_executesql @SQLQuery, N'@ArchivedColOUT sysname OUTPUT',@ArchivedColOUT=@ArchivedCol OUTPUT
			end
			
					
			if exists (select 1 from sys.objects o join sys.columns c on o.object_id=c.object_id and o.Name = @StagingTable and c.name='ArchivedDtim')
			or (@ArchivedCol is not null)
			begin
				set @sql = N'select top (1) @PubProcessId = q.PubProcessId, @PublisherId = q.PublisherId, @LastRowId = q.RowId, @LastArchivedDtim = q.ArchivedDtim from [dbo].' + QUOTENAME(@StagingTable) + N' q order by PublisherId desc, PubProcessId desc, RowId desc' 
			end
			else
			begin
				set @sql = N'select top (1) @PubProcessId = q.PubProcessId, @PublisherId = q.PublisherId, @LastRowId = q.RowId from [dbo].' + QUOTENAME(@StagingTable) + N' q order by PublisherId desc, PubProcessId desc, RowId desc' 			
			end

			exec sys.sp_executesql @sql, N'@PubProcessId bigint OUTPUT, @PublisherId tinyint OUTPUT, @LastRowId bigint OUTPUT, @LastArchivedDtim datetime OUTPUT', @PubProcessId = @PubProcessId OUTPUT,  @PublisherId = @PublisherId OUTPUT,  @LastRowId = @LastRowId OUTPUT, @LastArchivedDtim = @LastArchivedDtim OUTPUT
			
			if @ExistingPubProcessId is null and @PubProcessId is not null
				update dbo.APPStagingProcess set PublisherId = @PublisherId, PubProcessId = @PubProcessId, UpdatedDTim = GETUTCDATE() where ProcessId = @ProcessId
			
			--Extract Completed, Merge Started	
			update dbo.APPStagingProcessQueue
			set [StatusId] = 2, ExtractCompletedDTim = GETUTCDATE(), LastModifiedDTim = GETUTCDATE(), LastRowId = @LastRowId, LastArchivedDtim = @LastArchivedDtim 
			where (ProcessQueueId = @ProcessQueueId)

			begin try				

				if (@AutoMerge = 1)
				begin
					exec dbo.prc_MergeDelta @TargetTableName = @TargetTable, @BatchSize = @BatchSize OUTPUT, @CountInserts = @CountInserts OUTPUT, @CountUpdates = @CountUpdates OUTPUT, @CountDeletes = @CountDeletes OUTPUT, @CountSkippedByMerge = @CountSkippedByMerge OUTPUT, @CountSkippedByDeDupe = @CountSkippedByDeDupe OUTPUT, @NumGenerations = @NumGenerations OUTPUT
				end
				else
				begin
					set @sql = N'EXEC ' + @MergeProcedure + N' @BatchSize = @BatchSize OUTPUT, @CountInserts = @CountInserts OUTPUT, @CountUpdates = @CountUpdates OUTPUT, @CountDeletes = @CountDeletes OUTPUT, @CountSkippedByMerge = @CountSkippedByMerge OUTPUT, @CountSkippedByDeDupe = @CountSkippedByDeDupe OUTPUT, @NumGenerations = @NumGenerations OUTPUT'
					exec sys.sp_executesql @sql,
						N'@BatchSize int OUTPUT, @CountInserts int OUTPUT, @CountUpdates int OUTPUT, @CountDeletes int OUTPUT, @CountSkippedByMerge int OUTPUT, @CountSkippedByDeDupe int OUTPUT, @NumGenerations int OUTPUT',
						@BatchSize = @BatchSize OUTPUT,
						@CountInserts = @CountInserts OUTPUT,
						@CountUpdates = @CountUpdates OUTPUT,
						@CountDeletes = @CountDeletes OUTPUT,
						@CountSkippedByMerge = @CountSkippedByMerge OUTPUT,
						@CountSkippedByDeDupe = @CountSkippedByDeDupe OUTPUT,
						@NumGenerations = @NumGenerations OUTPUT
				end
			end try
			begin catch
				select @ErrMsg = error_message()

				update dbo.APPStagingProcessQueue
				set [StatusId] = 4, ErrMsg = @ErrMsg, LastModifiedDTim = GETUTCDATE() --Merge Failed
				where (ProcessQueueId = @ProcessQueueId) and (([StatusId] != 4) or (ErrMsg is null))

				select @ErrMsg = 'Table: ' + @TargetTable + ':: Merge Failed.' + @ErrMsg
				exec dbo.prc_APPStagingProcessEnd @ProcessId = @ProcessId, @ErrMsg = @ErrMsg

				raiserror(@ErrMsg, 16, 1)
			end catch
		
			update dbo.APPStagingProcessQueue
			set [StatusId] = 3, MergeCompletedDTim = GETUTCDATE(), LastModifiedDTim = GETUTCDATE(), --Merge Succeeded
				Imported = @BatchSize, Inserts = @CountInserts, Updates = @CountUpdates, Deletes = @CountDeletes, SkippedByMerge = @CountSkippedByMerge, SkippedByDeDupe = @CountSkippedByDeDupe, Generations = @NumGenerations
			where (ProcessQueueId = @ProcessQueueId)
		end	
		else
		begin
			update dbo.APPStagingProcessQueue
			set [StatusId] = 4, ErrMsg = @ErrMsg, LastModifiedDTim = GETUTCDATE()
			where (ProcessQueueId = @ProcessQueueId) and (([StatusId] != 4) or (ErrMsg is null))

			select @ErrMsg = 'Table: ' + @TargetTable + ':: Extract Failed.' + @ErrMsg
			exec dbo.prc_APPStagingProcessEnd @ProcessId = @ProcessId, @ErrMsg = @ErrMsg --Extract Failed
		end
	end try
	begin catch
		IF (@@TRANCOUNT > 0)
			ROLLBACK TRAN

		declare @error_message nvarchar(4000)
		select @error_message = error_message()		
		raiserror(@error_message, 16, 1)
	end catch
END
