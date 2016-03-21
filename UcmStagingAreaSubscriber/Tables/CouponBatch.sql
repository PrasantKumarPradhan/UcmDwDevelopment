CREATE TABLE [dbo].[CouponBatch] (
    [CouponBatchId]      INT           NOT NULL,
    [CouponClassId]      INT           NOT NULL,
    [MintDate]           SMALLDATETIME NOT NULL,
    [RequesterAlias]     VARCHAR (50)  NULL,
    [MinterAlias]        VARCHAR (50)  NULL,
    [CouponCount]        INT           NOT NULL,
    [MaxRedemptionCount] INT           NOT NULL,
    [StartDate]          SMALLDATETIME NOT NULL,
    [EndDate]            SMALLDATETIME NOT NULL,
    [ExpiryDate]         SMALLDATETIME NULL,
    [StatusId]           TINYINT       NOT NULL
);

