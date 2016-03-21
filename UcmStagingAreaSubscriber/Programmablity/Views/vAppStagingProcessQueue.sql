
CREATE VIEW dbo.vAppStagingProcessQueue as
SELECT
	q.ProcessId,
	q.ProcessQueueId,	
	T.TableId,
	t.TargetTable as TableName,
	q.StatusId,
	DATEDIFF(SS,Q.ExtractStartedDTim, ISNULL(Q.ExtractCompletedDTim, 
				CASE 
					WHEN StatusId = 1 AND LastModifiedDTim < DATEADD(HH, -1, GETUTCDATE()) THEN NULL -- terminated during Extract
					WHEN StatusId = 4 THEN LastModifiedDTim --Failed
					ELSE GETUTCDATE()
				END)
			 ) AS ExtractTimeInSec,
	DATEDIFF(SS,Q.ExtractCompletedDTim,ISNULL(Q.MergeCompletedDTim, 
				CASE 
					WHEN StatusId = 1 THEN NULL -- terminated during Extract
					WHEN StatusId = 2 and LastModifiedDTim < DATEADD(HH, -1, GETUTCDATE()) THEN NULL -- terminated during Merge
					WHEN StatusId = 4 THEN LastModifiedDTim --Failed
					ELSE GETUTCDATE()
				END)
			) AS MergeTimeInSec,
	Q.Imported, Q.Inserts, Q.Updates, Q.Deletes, Q.SkippedByMerge, Q.SkippedByDeDupe, Q.Generations, 
	Q.LastModifiedDTim, ErrMsg
FROM APPStagingProcessQueue Q WITH (NOLOCK) 
INNER JOIN AppStagingTable T WITH (NOLOCK) 
	ON Q.TableId = T.TableId
