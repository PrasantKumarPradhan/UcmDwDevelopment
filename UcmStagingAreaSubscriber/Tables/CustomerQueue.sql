CREATE TABLE [dbo].[CustomerQueue] (
    [RowId]                   BIGINT         NULL,
    [PublisherId]             TINYINT        NULL,
    [PubProcessId]            BIGINT         NULL,
    [CustomerId]              INT            NULL,
    [Name]                    NVARCHAR (100) NULL,
    [CustomerTypeId]          TINYINT        NULL,
    [ServiceLevelId]          TINYINT        NULL,
    [IndustryId]              SMALLINT       NULL,
    [IsLocal]                 BIT            NULL,
    [FinancialStatusId]       TINYINT        NULL,
    [LifeCycleStatusId]       TINYINT        NULL,
    [LockStatusId]            TINYINT        NULL,
    [ModifiedByUserId]        INT            NULL,
    [CreatedDTim]             DATETIME       NULL,
    [ModifiedDTim]            DATETIME       NULL,
    [ArchivedDTim]            DATETIME       NULL,
    [ActionFlag]              TINYINT        NULL,
    [TrustRatingId]           INT            NULL,
    [MonthlyAnniversaryDay]   TINYINT        NULL,
    [MarketId]                TINYINT        NULL,
    [SystemLimitLevelID]      SMALLINT       NULL,
    [ReportServiceLevelId]    TINYINT        NULL,
    [NotificationFlagBitmask] SMALLINT       NULL,
    [IsTPAN]                  BIT            NULL,
    [CustomerNumber]          CHAR (10)      NULL,
    [FraudStatusId]           TINYINT        NULL,
    [Timestamp2]              BINARY (8)     NULL,
    [LandedDateTime]          DATETIME       DEFAULT (getutcdate()) NOT NULL,
    [FraudReasonCodeId]       TINYINT        NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_CustomerQueue]
    ON [dbo].[CustomerQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

