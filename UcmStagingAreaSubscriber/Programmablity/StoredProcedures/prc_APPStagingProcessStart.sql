

CREATE PROCEDURE dbo.prc_APPStagingProcessStart
   @SASubProcessID bigint,
   @ProcessId bigint OUTPUT
AS
BEGIN

	begin try
		if (@SASubProcessID is null)
			raiserror(N'@SASubProcessID parameter cannot be null', 15, 1)

		insert into dbo.APPStagingProcess (PubProcessId,SASubProcessId,StatusId,UpdatedDTim)
		select NULL,@SASubProcessID, 1, GETUTCDATE()

		set @ProcessId = SCOPE_IDENTITY()

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
