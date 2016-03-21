

DECLARE  @APPStagingProcessStatus TABLE(StatusId TINYINT, StatusName VARCHAR(50), UpdatedDTim DATETIME)

INSERT INTO @APPStagingProcessStatus (StatusId,StatusName,UpdatedDTim)


SELECT 1, 'Extract Started', getdate() UNION
SELECT 2, 'Extract Completed, Merge Started', getdate() UNION
SELECT 3, 'Merge Completed', getdate() UNION
SELECT 4, 'Failed', getdate()


INSERT INTO dbo.APPStagingProcessStatus 
(
	StatusId,
	StatusName,
	UpdatedDTim
)
SELECT 
	t.StatusId,
	t.StatusName,
	t.UpdatedDTim
FROM @APPStagingProcessStatus t LEFT OUTER JOIN dbo.APPStagingProcessStatus m ON t.StatusId = m.StatusId
WHERE m.StatusId is null

UPDATE m SET 
	m.StatusId = t.StatusId,
	m.StatusName = t.StatusName,
	m.UpdatedDTim = t.UpdatedDTim
FROM @APPStagingProcessStatus t INNER JOIN dbo.APPStagingProcessStatus m ON t.StatusId = m.StatusId
WHERE 
	m.StatusName <> t.StatusName OR
	m.UpdatedDTim <> t.UpdatedDTim 

	