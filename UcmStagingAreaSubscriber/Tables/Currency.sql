CREATE TABLE [dbo].[Currency] (
    [CurrencyID]         SMALLINT      NOT NULL,
    [CurrencyName]       NVARCHAR (35) NOT NULL,
    [CurrencyCode]       NCHAR (3)     NOT NULL,
    [CurrentMonthlyRate] FLOAT (53)    NOT NULL,
    [FractionalPart]     TINYINT       NOT NULL,
    [CreatedDTim]        DATETIME      NOT NULL,
    [ModifiedDTim]       DATETIME      NOT NULL,
    [FeatureId]          INT           NULL
);

