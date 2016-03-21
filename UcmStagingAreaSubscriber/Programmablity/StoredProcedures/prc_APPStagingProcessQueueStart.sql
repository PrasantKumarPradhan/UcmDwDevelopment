
CREATE PROCEDURE dbo.prc_APPStagingProcessQueueStart
   @ProcessId bigint,
   @QueueName sysname
AS
BEGIN
	declare @TableId int
	declare @TruncateOnCompletion bit
	declare @sql nvarchar(200)
	begin try
		if (@ProcessId is null)
			raiserror(N'@ProcessId parameter cannot be null', 15, 1)

		if (@QueueName is null)
			raiserror(N'@QueueName parameter cannot be null', 15, 1)

		select @TableId = ast.TableId, @TruncateOnCompletion = ast.TruncateOnCompletion
		from dbo.AppStagingTable ast with (nolock)
		where (ast.StagingTable = @QueueName)

		if (@TableId IS NULL)
			raiserror(N'Invalid @QueueName parameter value', 15, 1)

		insert into dbo.APPStagingProcessQueue (ProcessId, TableId, [StatusId])
		select @ProcessId, @TableId, 1		

		if (@TruncateOnCompletion = 1)
		begin
			exec dbo.prc_TruncateTable @QueueName
		end

		return 0

	end try
	begin catch
		IF (@@TRANCOUNT > 0)
			ROLLBACK TRAN

		declare @error_message nvarchar(4000)
		select @error_message = error_message()		
		raiserror(@error_message, 16, 1)
	end catch
END
