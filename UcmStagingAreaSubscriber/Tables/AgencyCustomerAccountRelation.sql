CREATE TABLE [dbo].[AgencyCustomerAccountRelation] (
    [AgencyCustomerAccountRelationId] INT      NOT NULL,
    [AgencyCustomerId]                INT      NOT NULL,
    [AccountId]                       INT      NOT NULL,
    [ModifiedByUserId]                INT      NOT NULL,
    [CreatedDtim]                     DATETIME NOT NULL,
    [ModifiedDtim]                    DATETIME NOT NULL,
    [CustomerId]                      INT      NULL
);

