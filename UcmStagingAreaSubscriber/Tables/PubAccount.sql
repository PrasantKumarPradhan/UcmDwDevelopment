CREATE TABLE [dbo].[PubAccount] (
    [AccountId]        INT            NOT NULL,
    [TaxDocumentId]    INT            NULL,
    [PayeeFirstName]   NVARCHAR (100) NULL,
    [PayeeLastName]    NVARCHAR (100) NULL,
    [ModifiedByUserId] INT            NOT NULL,
    [CreatedDtim]      DATETIME       NOT NULL,
    [ModifiedDtim]     DATETIME       NOT NULL,
    [PayeeName]        NVARCHAR (50)  NULL
);

