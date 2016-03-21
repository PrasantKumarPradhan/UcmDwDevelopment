CREATE TABLE [dbo].[CouponRedemption] (
    [CouponRedemptionId] INT           NOT NULL,
    [CouponId]           INT           NOT NULL,
    [AccountId]          INT           NOT NULL,
    [RedemptionDate]     SMALLDATETIME NOT NULL,
    [ExpiryDate]         SMALLDATETIME NOT NULL,
    [CouponValue]        MONEY         NULL,
    [CouponClassId]      INT           NULL,
    [CouponBatchId]      INT           NULL,
    [ModifiedDtim]       DATETIME      NOT NULL,
    [ModifiedByUserId]   INT           NULL
);

