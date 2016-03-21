CREATE TABLE [dbo].[CouponRedemptionQueue] (
    [PublisherId]        TINYINT       NULL,
    [PubProcessId]       BIGINT        NULL,
    [CustomerId]         INT           NULL,
    [CouponRedemptionId] INT           NULL,
    [CouponId]           INT           NULL,
    [AccountId]          INT           NULL,
    [RedemptionDate]     SMALLDATETIME NULL,
    [ExpiryDate]         SMALLDATETIME NULL,
    [ArchiveDTim]        DATETIME      NULL,
    [ActionFlag]         TINYINT       NULL,
    [RowId]              BIGINT        NULL,
    [CouponValue]        MONEY         NULL,
    [ModifiedDTim]       DATETIME      NULL,
    [CouponClassId]      INT           NULL,
    [CouponBatchId]      INT           NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_CouponRedemptionQueue]
    ON [dbo].[CouponRedemptionQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

