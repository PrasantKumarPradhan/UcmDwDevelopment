
CREATE PROCEDURE dbo.prc_APPStagingProcessEnd
   @ProcessId bigint,
   @ErrMsg varchar(4000) = NULL
AS
BEGIN

	begin try
		if (@ProcessId is null)
			raiserror(N'@ProcessId parameter cannot be null', 15, 1)

		if (@ErrMsg is null)
		begin
			update dbo.APPStagingProcess
			set [StatusId] = 3, UpdatedDTim = GETUTCDATE() --All Merge Completed
			where ProcessId = @ProcessId
		end
		else
		begin
			update dbo.APPStagingProcess
			set [StatusId] = 4, UpdatedDTim = GETUTCDATE(), ErrMsg = @ErrMsg --Failed	
			where (ProcessId = @ProcessId) and (([StatusId] != 4) or (ErrMsg is null))

			raiserror(@ErrMsg, 15, 1)
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
