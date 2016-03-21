CREATE TABLE [dbo].[Country] (
    [CountryID]                SMALLINT      NOT NULL,
    [CountryNumber]            CHAR (3)      NOT NULL,
    [CountryCode]              CHAR (2)      NOT NULL,
    [CountryCode3]             CHAR (3)      NOT NULL,
    [CountryName]              VARCHAR (75)  NOT NULL,
    [CountryLongName]          VARCHAR (100) NOT NULL,
    [MSAlexandriaGeoEntityNbr] INT           NULL,
    [MSRegionKeyTxt]           VARCHAR (50)  NOT NULL,
    [MSSalesRegionName]        VARCHAR (35)  NOT NULL,
    [CreatedDtim]              DATETIME      NOT NULL,
    [ModifiedDtim]             DATETIME      NOT NULL,
    [IsActive]                 BIT           CONSTRAINT [DF_Country_IsActive] DEFAULT ((1)) NOT NULL
);

