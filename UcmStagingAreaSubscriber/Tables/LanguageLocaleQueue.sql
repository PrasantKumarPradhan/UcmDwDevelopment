CREATE TABLE [dbo].[LanguageLocaleQueue] (
    [PublisherId]        TINYINT       NULL,
    [PubProcessId]       BIGINT        NULL,
    [RowId]              BIGINT        NULL,
    [LanguageLocaleID]   INT           NULL,
    [LCID]               INT           NULL,
    [LanguageLocaleName] NVARCHAR (50) NULL,
    [LanguageCommonName] NVARCHAR (50) NULL,
    [WordBreakerLCID]    INT           NULL,
    [PrimaryUse]         BIT           NULL,
    [LanguageID]         SMALLINT      NULL,
    [CountryID]          SMALLINT      NULL,
    [CreatedDtim]        DATETIME      NULL,
    [ModifiedDtim]       DATETIME      NULL,
    [ActionFlag]         TINYINT       NULL,
    [ArchiveDtim]        DATETIME      NULL,
    [IsSoftDeleted]      BIT           NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_LanguageLocaleQueue]
    ON [dbo].[LanguageLocaleQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

