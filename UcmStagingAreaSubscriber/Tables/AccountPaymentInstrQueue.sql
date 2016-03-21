CREATE TABLE [dbo].[AccountPaymentInstrQueue] (
    [PublisherId]    TINYINT  NULL,
    [PubProcessId]   BIGINT   NULL,
    [CustomerId]     INT      NULL,
    [RowId]          BIGINT   NULL,
    [PaymentInstrId] INT      NULL,
    [AccountId]      INT      NULL,
    [CreatedDtim]    DATETIME NULL,
    [ArchiveDTim]    DATETIME NULL,
    [ActionFlag]     TINYINT  NULL,
    [ModifiedDTim]   DATETIME NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_AccountPaymentInstrQueue]
    ON [dbo].[AccountPaymentInstrQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

