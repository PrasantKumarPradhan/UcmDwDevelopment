
DECLARE  @SystemParameters TABLE(ParameterType VARCHAR(32), ParameterName VARCHAR(57),ParameterValue_Current VARCHAR(57),ParameterValue_New VARCHAR(1024),ParameterValue_Default NVARCHAR(1024),ParameterDesc NVARCHAR(1024),LastModifiedBy NVARCHAR(128), LastModifiedDtim DATETIME)



INSERT INTO @SystemParameters (ParameterType,ParameterName,ParameterValue_Current,ParameterValue_New,ParameterValue_Default,ParameterDesc,LastModifiedBy,LastModifiedDtim)


SELECT N'Staging Area Settings', N'ArchivalPeriod', N'365', N'365', N'365', N'Number of days to keep the historical logging data', N'bactest', CAST(0x0000A5B50183F8B4 AS DATETIME) UNION
SELECT N'Staging Area Settings', N'CleanupBatchSize', N'10000', N'10000', N'10000',N'batch size for archiving log data', N'bactest', CAST(0x0000A5B50183F8BC AS DATETIME)



INSERT INTO dbo.SystemParameters 
(
	ParameterType,
	ParameterName,
	ParameterValue_Current,
	ParameterValue_New,
	ParameterValue_Default,
	ParameterDesc,
	LastModifiedBy,
	LastModifiedDtim
)
SELECT 
	t.ParameterType,
	t.ParameterName,
	t.ParameterValue_Current,
	t.ParameterValue_New,
	t.ParameterValue_Default,
	t.ParameterDesc,
	t.LastModifiedBy,
	t.LastModifiedDtim
FROM @SystemParameters t LEFT OUTER JOIN  dbo.SystemParameters m ON t.ParameterType = m.ParameterType
WHERE m.ParameterType IS NULL

UPDATE m SET 
	m.ParameterType = t.ParameterType,
	m.ParameterName = t.ParameterName,
	m.ParameterValue_Current = t.ParameterValue_Current,
	m.ParameterValue_New = t.ParameterValue_New,
	m.ParameterValue_Default = t.ParameterValue_Default,
	m.ParameterDesc = t.ParameterDesc,
	m.LastModifiedBy = t.LastModifiedBy,
	m.LastModifiedDtim = t.LastModifiedDtim

FROM @SystemParameters t INNER JOIN dbo.SystemParameters m ON t.ParameterType = m.ParameterType

WHERE 
	m.ParameterName <> t.ParameterName OR
	m.ParameterValue_Current <> t.ParameterValue_Current OR
	m.ParameterValue_New <> t.ParameterValue_New OR
	m.ParameterValue_Default <> t.ParameterValue_Default OR
	m.ParameterDesc <> t.ParameterDesc OR
	m.LastModifiedBy <> t.LastModifiedBy OR
	m.LastModifiedDtim <> t.LastModifiedDtim



	


	