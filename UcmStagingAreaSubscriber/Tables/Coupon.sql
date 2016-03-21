CREATE TABLE [dbo].[Coupon] (
    [CouponId]        INT       NOT NULL,
    [CouponCode]      CHAR (16) NULL,
    [RedemptionCount] INT       NOT NULL,
    [CouponBatchId]   INT       NOT NULL
);

