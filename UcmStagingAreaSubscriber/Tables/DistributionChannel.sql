CREATE TABLE [dbo].[DistributionChannel] (
    [DistributionChannelId]   SMALLINT     NOT NULL,
    [DistributionChannelName] VARCHAR (50) NOT NULL,
    [LanguageLocaleId]        INT          NULL,
    [ProductSiteId]           INT          NOT NULL,
    [AdMetrixCountryId]       INT          NOT NULL,
    [CreatedDTim]             DATETIME     NOT NULL,
    [PilotFlag]               TINYINT      NOT NULL
);

