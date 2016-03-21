CREATE TABLE [dbo].[PartnerHierarchyQueue] (
    [PublisherId]         TINYINT  NULL,
    [PubProcessId]        BIGINT   NULL,
    [CustomerId]          INT      NULL,
    [RelatedToCustomerId] INT      NULL,
    [PartnerTypeId]       TINYINT  NULL,
    [CreatedDtim]         DATETIME NULL,
    [RowId]               BIGINT   NULL,
    [ArchiveDTim]         DATETIME NULL,
    [ActionFlag]          TINYINT  NULL,
    [IsSoftDeleted]       BIT      NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_PartnerHierarchyQueue]
    ON [dbo].[PartnerHierarchyQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

