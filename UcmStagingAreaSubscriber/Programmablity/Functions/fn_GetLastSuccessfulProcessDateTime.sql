
CREATE FUNCTION dbo.fn_GetLastSuccessfulProcessDateTime()
RETURNS DATETIME
AS
BEGIN
	DECLARE @LastProcessDate DATETIME 
	SELECT TOP 1 @LastProcessDate = P.UpdatedDTim FROM dbo.APPStagingProcess P WITH (NOLOCK) 
	WHERE 
		0 < (SELECT SUM(Imported) FROM dbo.APPStagingProcessQueue d WITH (NOLOCK) WHERE p.ProcessId = d.ProcessId) 
		AND StatusId = 3 
	ORDER BY 
		ProcessId DESC

	RETURN isnull(@LastProcessDate, '1900-01-01')
END
