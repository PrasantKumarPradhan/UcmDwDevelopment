CREATE TABLE [dbo].[CouponClass] (
    [CouponClassId]                         INT           NOT NULL,
    [ClassCode]                             CHAR (10)     NULL,
    [ClassName]                             VARCHAR (50)  NOT NULL,
    [ClassDescription]                      VARCHAR (100) NULL,
    [CouponValue]                           MONEY         NOT NULL,
    [ConsumptionDuration]                   INT           NULL,
    [IsMultiusable]                         BIT           NOT NULL,
    [CouponMarketingChannelId]              TINYINT       NOT NULL,
    [CouponPurposeId]                       TINYINT       NOT NULL,
    [ProductId]                             INT           NOT NULL,
    [CurrencyId]                            SMALLINT      NOT NULL,
    [CountryId]                             SMALLINT      NOT NULL,
    [MaxRedemptionPerAccountCount]          INT           NOT NULL,
    [RequesterAlias]                        VARCHAR (50)  NULL,
    [TOULink]                               VARCHAR (50)  NULL,
    [PartnerCustomerID]                     INT           NULL,
    [ConsumptionPct]                        DECIMAL (18)  NULL,
    [ConsumptionClickValue]                 MONEY         NULL,
    [SignupGracePeriod]                     INT           NULL,
    [LimitForDaysToUseFromCustomerCreation] TINYINT       NULL
);

