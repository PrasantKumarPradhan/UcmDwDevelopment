CREATE TABLE [dbo].[LanguageLocale] (
    [LanguageLocaleID]   INT           NOT NULL,
    [LCID]               INT           NOT NULL,
    [LanguageLocaleName] NVARCHAR (50) NOT NULL,
    [LanguageCommonName] NVARCHAR (50) NULL,
    [WordBreakerLCID]    INT           NULL,
    [PrimaryUse]         BIT           NOT NULL,
    [LanguageID]         SMALLINT      NULL,
    [CountryID]          SMALLINT      NULL,
    [CreatedDtim]        DATETIME      NOT NULL,
    [ModifiedDtim]       DATETIME      NOT NULL
);

