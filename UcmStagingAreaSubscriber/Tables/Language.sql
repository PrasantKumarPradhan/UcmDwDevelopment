CREATE TABLE [dbo].[Language] (
    [LanguageID]   SMALLINT      NOT NULL,
    [LanguageCode] VARCHAR (7)   NOT NULL,
    [LanguageName] VARCHAR (255) NOT NULL,
    [CreatedDtim]  DATETIME      NOT NULL,
    [ModifiedDtim] DATETIME      NOT NULL
);

