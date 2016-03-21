CREATE TABLE [dbo].[CouponClassQueue] (
    [PublisherId]                  TINYINT       NULL,
    [PubProcessId]                 BIGINT        NULL,
    [RowId]                        BIGINT        NULL,
    [ClassCode]                    CHAR (10)     NULL,
    [ClassName]                    VARCHAR (50)  NULL,
    [ClassDescription]             VARCHAR (100) NULL,
    [CouponValue]                  MONEY         NULL,
    [ConsumptionDuration]          INT           NULL,
    [IsMultiusable]                BIT           NULL,
    [CouponMarketingChannelId]     TINYINT       NULL,
    [CouponPurposeId]              TINYINT       NULL,
    [ProductId]                    INT           NULL,
    [CurrencyId]                   SMALLINT      NULL,
    [CountryId]                    SMALLINT      NULL,
    [MaxRedemptionPerAccountCount] INT           NULL,
    [RequesterAlias]               VARCHAR (50)  NULL,
    [TOULink]                      VARCHAR (50)  NULL,
    [PartnerCustomerID]            INT           NULL,
    [ConsumptionPct]               DECIMAL (18)  NULL,
    [ConsumptionClickValue]        MONEY         NULL,
    [SignupGracePeriod]            INT           NULL,
    [CouponClassId]                INT           NULL,
    [ArchiveDTim]                  DATETIME      NULL,
    [ActionFlag]                   TINYINT       NULL,
    [ModifiedDTim]                 DATETIME      NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_CouponClassQueue]
    ON [dbo].[CouponClassQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

