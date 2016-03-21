CREATE TABLE [dbo].[PartnerHierarchy] (
    [CustomerId]          INT      NOT NULL,
    [RelatedToCustomerId] INT      NULL,
    [PartnerTypeId]       TINYINT  NOT NULL,
    [CreatedDtim]         DATETIME NOT NULL
);

