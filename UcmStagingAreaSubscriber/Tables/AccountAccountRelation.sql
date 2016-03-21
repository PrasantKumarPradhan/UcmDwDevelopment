CREATE TABLE [dbo].[AccountAccountRelation] (
    [AccountId]          INT      NOT NULL,
    [RelationShipID]     TINYINT  NOT NULL,
    [RelatedToAccountId] INT      NOT NULL,
    [CreatedDtim]        DATETIME NULL,
    [CustomerId]         INT      NULL
);

