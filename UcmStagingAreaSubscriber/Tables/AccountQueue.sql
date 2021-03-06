﻿CREATE TABLE [dbo].[AccountQueue] (
    [PublisherId]                   TINYINT         NULL,
    [PubProcessId]                  BIGINT          NULL,
    [CustomerId]                    INT             NULL,
    [AccountId]                     INT             NULL,
    [AccountNumber]                 CHAR (8)        NULL,
    [AccountName]                   NVARCHAR (100)  NULL,
    [AccountTypeId]                 TINYINT         NULL,
    [ProductCategoryId]             TINYINT         NULL,
    [AdvertiserCustomerId]          INT             NULL,
    [AgencyCustomerId]              INT             NULL,
    [SalesHouseCustomerId]          INT             NULL,
    [BillToCustomerId]              INT             NULL,
    [PreferredBillToPaymentInstrId] INT             NULL,
    [CountryCode]                   CHAR (2)        NULL,
    [FinancialStatusId]             TINYINT         NULL,
    [LifeCycleStatusId]             TINYINT         NULL,
    [LockStatusId]                  TINYINT         NULL,
    [PreferredUserId]               INT             NULL,
    [PreferredCurrencyId]           SMALLINT        NULL,
    [PreferredLanguageId]           SMALLINT        NULL,
    [AgencyContactName]             NVARCHAR (100)  NULL,
    [SoldToPaymentInstrId]          INT             NULL,
    [OtherAddressId]                INT             NULL,
    [BillingComment]                NVARCHAR (1000) NULL,
    [PaymentOptionId]               TINYINT         NULL,
    [NetTerm]                       TINYINT         NULL,
    [CompressBillingFlag]           BIT             NULL,
    [SpendLimitId]                  SMALLINT        NULL,
    [CreatedDTim]                   DATETIME        NULL,
    [ModifiedDTim]                  DATETIME        NULL,
    [ModifiedByUserId]              INT             NULL,
    [ArchiveDTim]                   DATETIME        NULL,
    [ActionFlag]                    TINYINT         NULL,
    [BillingRetryID]                SMALLINT        NULL,
    [BillingRetryDtim]              DATETIME        NULL,
    [BillingSuccessCount]           INT             NULL,
    [AgencyCommissionPct]           DECIMAL (5, 2)  NULL,
    [SalesHouseCommissionPct]       DECIMAL (5, 2)  NULL,
    [CompressedBilling]             TINYINT         NULL,
    [PauseStatus]                   TINYINT         NULL,
    [PUID]                          BIGINT          NULL,
    [TimeZoneId]                    TINYINT         NULL,
    [RowId]                         BIGINT          NULL,
    [CashbackTypeId]                TINYINT         NULL,
    [NotificationFlagBitmask]       SMALLINT        NULL,
    [CreatedUTCDateTime]            DATETIME        NULL,
    [AnalyticsOptInId]              TINYINT         NULL,
    [Timestamp2]                    BINARY (8)      NULL,
    [BackupPaymentInstrId]          INT             NULL,
    [LandedDateTime]                DATETIME        DEFAULT (getutcdate()) NOT NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_AccountQueue]
    ON [dbo].[AccountQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

