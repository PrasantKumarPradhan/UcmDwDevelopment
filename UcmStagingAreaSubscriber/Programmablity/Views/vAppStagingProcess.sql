
CREATE VIEW dbo.vAppStagingProcess as
SELECT
	ProcessId,
	PubProcessId,
	SASubProcessId,
	StatusId,
	DATEDIFF(SS,ProcessStartedDTim, UpdatedDTim) AS ProcessingTimeInSec,
	UpdatedDTim AS LastModifiedDateTime, 
	ErrMsg
FROM APPStagingProcess WITH (NOLOCK)

