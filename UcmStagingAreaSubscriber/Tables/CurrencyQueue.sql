CREATE TABLE [dbo].[CurrencyQueue] (
    [PublisherId]        TINYINT       NULL,
    [PubProcessId]       BIGINT        NULL,
    [RowId]              BIGINT        NULL,
    [CurrencyID]         SMALLINT      NULL,
    [CurrencyName]       NVARCHAR (35) NULL,
    [CurrencyCode]       NCHAR (3)     NULL,
    [CurrentMonthlyRate] FLOAT (53)    NULL,
    [FractionalPart]     TINYINT       NULL,
    [CreatedDTim]        DATETIME      NULL,
    [ModifiedDTim]       DATETIME      NULL,
    [FeatureId]          INT           NULL,
    [ActionFlag]         TINYINT       NULL,
    [ArchiveDtim]        DATETIME      NULL,
    [IsSoftDeleted]      BIT           NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_CurrencyQueue]
    ON [dbo].[CurrencyQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

