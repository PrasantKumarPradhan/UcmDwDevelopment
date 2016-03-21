CREATE TABLE [dbo].[LanguageQueue] (
    [PublisherId]   TINYINT       NULL,
    [PubProcessId]  BIGINT        NULL,
    [RowId]         BIGINT        NULL,
    [LanguageID]    SMALLINT      NULL,
    [LanguageCode]  VARCHAR (7)   NULL,
    [LanguageName]  VARCHAR (255) NULL,
    [CreatedDtim]   DATETIME      NULL,
    [ModifiedDtim]  DATETIME      NULL,
    [ActionFlag]    TINYINT       NULL,
    [ArchiveDtim]   DATETIME      NULL,
    [IsSoftDeleted] BIT           NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_LanguageQueue]
    ON [dbo].[LanguageQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

