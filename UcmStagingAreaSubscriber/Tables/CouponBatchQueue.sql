CREATE TABLE [dbo].[CouponBatchQueue] (
    [PublisherId]        TINYINT       NULL,
    [PubProcessId]       BIGINT        NULL,
    [RowId]              BIGINT        NULL,
    [CouponClassId]      INT           NULL,
    [MintDate]           SMALLDATETIME NULL,
    [RequesterAlias]     VARCHAR (50)  NULL,
    [MinterAlias]        VARCHAR (50)  NULL,
    [CouponCount]        INT           NULL,
    [MaxRedemptionCount] INT           NULL,
    [StartDate]          SMALLDATETIME NULL,
    [EndDate]            SMALLDATETIME NULL,
    [ExpiryDate]         SMALLDATETIME NULL,
    [StatusId]           TINYINT       NULL,
    [CouponBatchId]      INT           NULL,
    [ArchiveDTim]        DATETIME      NULL,
    [ActionFlag]         TINYINT       NULL,
    [ModifiedDTim]       DATETIME      NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_CouponBatchQueue]
    ON [dbo].[CouponBatchQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

