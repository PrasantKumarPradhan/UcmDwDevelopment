CREATE TABLE [dbo].[CouponMarketingChannelQueue] (
    [PublisherId]              TINYINT       NULL,
    [PubProcessId]             BIGINT        NULL,
    [RowId]                    BIGINT        NULL,
    [CouponMarketingChannelId] TINYINT       NULL,
    [ChannelName]              VARCHAR (30)  NULL,
    [ChannelDescription]       VARCHAR (100) NULL,
    [ModifiedDtim]             DATETIME      NULL,
    [ActionFlag]               TINYINT       NULL,
    [ArchiveDtim]              DATETIME      NULL,
    [IsSoftDeleted]            BIT           NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_CouponMarketingChannelQueue]
    ON [dbo].[CouponMarketingChannelQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

