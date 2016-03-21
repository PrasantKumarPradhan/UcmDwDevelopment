CREATE TABLE [dbo].[CouponQueue] (
    [PublisherId]     TINYINT   NULL,
    [PubProcessId]    BIGINT    NULL,
    [RowId]           BIGINT    NULL,
    [CouponId]        INT       NULL,
    [CouponCode]      CHAR (16) NULL,
    [RedemptionCount] INT       NULL,
    [CouponBatchId]   INT       NULL,
    [ArchiveDTim]     DATETIME  NULL,
    [ActionFlag]      TINYINT   NULL,
    [ModifiedDTim]    DATETIME  NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_CouponQueue]
    ON [dbo].[CouponQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

