CREATE TABLE [dbo].[DistributionChannelQueue] (
    [PublisherId]             TINYINT      NULL,
    [PubProcessId]            BIGINT       NULL,
    [RowId]                   BIGINT       NULL,
    [DistributionChannelId]   SMALLINT     NULL,
    [DistributionChannelName] VARCHAR (50) NULL,
    [LanguageLocaleId]        INT          NULL,
    [ProductSiteId]           INT          NULL,
    [AdMetrixCountryId]       INT          NULL,
    [CreatedDTim]             DATETIME     NULL,
    [PilotFlag]               TINYINT      NULL,
    [ArchiveDTim]             DATETIME     NULL,
    [ActionFlag]              TINYINT      NULL,
    [IsSoftDeleted]           BIT          NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_DistributionChannelQueue]
    ON [dbo].[DistributionChannelQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

