CREATE TABLE [dbo].[AccountRelationQueue] (
    [PublisherId]      TINYINT       NULL,
    [PubProcessId]     BIGINT        NULL,
    [RowId]            BIGINT        NULL,
    [RelationShipID]   TINYINT       NULL,
    [RelationShipName] NVARCHAR (50) NULL,
    [ArchiveDTim]      DATETIME      NULL,
    [ActionFlag]       TINYINT       NULL,
    [IsSoftDeleted]    BIT           NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_AccountRelationQueue]
    ON [dbo].[AccountRelationQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

