CREATE TABLE [dbo].[AccountAccountRelationQueue] (
    [PublisherId]        TINYINT  NULL,
    [PubProcessId]       BIGINT   NULL,
    [RowId]              BIGINT   NULL,
    [CustomerId]         INT      NULL,
    [AccountId]          INT      NULL,
    [RelationShipID]     TINYINT  NULL,
    [RelatedToAccountId] INT      NULL,
    [CreatedDtim]        DATETIME NULL,
    [ArchivedDtim]       DATETIME NULL,
    [ActionFlag]         TINYINT  NULL,
    [ModifiedDTim]       DATETIME NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_AccountAccountRelationQueue]
    ON [dbo].[AccountAccountRelationQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

