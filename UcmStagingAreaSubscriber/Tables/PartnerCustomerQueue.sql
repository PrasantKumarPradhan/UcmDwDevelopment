CREATE TABLE [dbo].[PartnerCustomerQueue] (
    [PublisherId]             TINYINT        NULL,
    [PubProcessId]            BIGINT         NULL,
    [CustomerId]              INT            NULL,
    [PartnerCustomerId]       INT            NULL,
    [IsAgency]                BIT            NULL,
    [IsSalesHouse]            BIT            NULL,
    [UserRoleId]              TINYINT        NULL,
    [ChargeSignupFee]         BIT            NULL,
    [AllowManageCustomer]     BIT            NULL,
    [AgencyCommissionPct]     DECIMAL (5, 2) NULL,
    [SalesHouseCommissionPct] DECIMAL (5, 2) NULL,
    [PaymentInstrId]          INT            NULL,
    [ServiceLevelId]          TINYINT        NULL,
    [SystemLimitLevelId]      SMALLINT       NULL,
    [RowId]                   BIGINT         NULL,
    [ArchiveDTim]             DATETIME       NULL,
    [ActionFlag]              TINYINT        NULL,
    [IsSoftDeleted]           BIT            NULL,
    [ModifiedDTim]            DATETIME       NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_PartnerCustomerQueue]
    ON [dbo].[PartnerCustomerQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

