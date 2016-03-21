CREATE TABLE [dbo].[CountryQueue] (
    [PublisherId]              TINYINT       NULL,
    [PubProcessId]             BIGINT        NULL,
    [RowId]                    BIGINT        NULL,
    [CountryID]                SMALLINT      NULL,
    [CountryNumber]            CHAR (3)      NULL,
    [CountryCode]              CHAR (2)      NULL,
    [CountryCode3]             CHAR (3)      NULL,
    [CountryName]              VARCHAR (75)  NULL,
    [CountryLongName]          VARCHAR (100) NULL,
    [MSAlexandriaGeoEntityNbr] INT           NULL,
    [MSRegionKeyTxt]           VARCHAR (50)  NULL,
    [MSSalesRegionName]        VARCHAR (35)  NULL,
    [CreatedDtim]              DATETIME      NULL,
    [ModifiedDtim]             DATETIME      NULL,
    [IsActive]                 BIT           NULL,
    [TargetLevel]              TINYINT       NULL,
    [ActionFlag]               TINYINT       NULL,
    [ArchiveDtim]              DATETIME      NULL,
    [IsSoftDeleted]            BIT           NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_CountryQueue]
    ON [dbo].[CountryQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

