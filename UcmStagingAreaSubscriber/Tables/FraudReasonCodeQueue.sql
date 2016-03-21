CREATE TABLE [dbo].[FraudReasonCodeQueue] (
    [PublisherId]         TINYINT        NULL,
    [PubProcessId]        BIGINT         NULL,
    [RowId]               BIGINT         NULL,
    [FraudReasonCodeId]   TINYINT        NULL,
    [FraudReasonCodeDesc] NVARCHAR (100) NULL,
    [CreatedDTim]         DATETIME       NULL,
    [ModifiedDTim]        DATETIME       NULL,
    [ArchivedDTim]        DATETIME       NULL,
    [ActionFlag]          TINYINT        NULL
);

