CREATE TABLE [dbo].[PartnerCustomer] (
    [PartnerCustomerId]       INT            NOT NULL,
    [IsAgency]                BIT            NOT NULL,
    [IsSalesHouse]            BIT            NOT NULL,
    [UserRoleId]              TINYINT        NOT NULL,
    [ChargeSignupFee]         BIT            NOT NULL,
    [AllowManageCustomer]     BIT            NOT NULL,
    [AgencyCommissionPct]     DECIMAL (5, 2) NOT NULL,
    [SalesHouseCommissionPct] DECIMAL (5, 2) NOT NULL,
    [PaymentInstrId]          INT            NULL,
    [ServiceLevelId]          TINYINT        NULL,
    [SystemLimitLevelId]      SMALLINT       NULL,
    [ModifiedDtim]            DATETIME       NOT NULL
);

