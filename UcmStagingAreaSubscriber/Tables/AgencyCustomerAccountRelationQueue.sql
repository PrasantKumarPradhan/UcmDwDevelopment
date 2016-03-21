CREATE TABLE [dbo].[AgencyCustomerAccountRelationQueue] (
    [PublisherId]                     TINYINT  NULL,
    [PubProcessId]                    BIGINT   NULL,
    [CustomerId]                      INT      NULL,
    [AgencyCustomerAccountRelationId] INT      NULL,
    [AgencyCustomerId]                INT      NULL,
    [AccountId]                       INT      NULL,
    [ModifiedByUserId]                INT      NULL,
    [CreatedDtim]                     DATETIME NULL,
    [ModifiedDtim]                    DATETIME NULL,
    [ArchiveDTim]                     DATETIME NULL,
    [ActionFlag]                      TINYINT  NULL,
    [RowId]                           BIGINT   NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_AgencyCustomerAccountRelationQueue]
    ON [dbo].[AgencyCustomerAccountRelationQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

