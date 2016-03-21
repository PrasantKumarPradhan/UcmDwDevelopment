CREATE TABLE [dbo].[PubAccountQueue] (
    [PublisherId]          TINYINT        NULL,
    [PubProcessId]         BIGINT         NULL,
    [RowId]                BIGINT         NULL,
    [AccountId]            INT            NULL,
    [TaxDocumentId]        INT            NULL,
    [EarningRatio]         DECIMAL (5, 4) NULL,
    [PayeeFirstName]       NVARCHAR (100) NULL,
    [PayeeLastName]        NVARCHAR (100) NULL,
    [BusinessName]         NVARCHAR (100) NULL,
    [ModifiedByUserId]     INT            NULL,
    [CreatedDtim]          DATETIME       NULL,
    [ArchiveDTim]          DATETIME       NULL,
    [ActionFlag]           TINYINT        NULL,
    [OperationCostRatio]   DECIMAL (5, 4) NULL,
    [IsPersistData]        BIT            NULL,
    [IsShareData]          BIT            NULL,
    [IsUsedAccrossNetwork] BIT            NULL,
    [PayeeName]            NVARCHAR (50)  NULL,
    [ModifiedDtim]         DATETIME       NULL,
    [AccountTimestamp]     VARBINARY (8)  NULL,
    [CustomerId]           INT            NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_PubAccountQueue]
    ON [dbo].[PubAccountQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

