CREATE TABLE [dbo].[CampaignQueue] (
    [PublisherId]             TINYINT          NOT NULL,
    [PubProcessId]            BIGINT           NOT NULL,
    [CustomerId]              INT              NOT NULL,
    [CampaignName]            NVARCHAR (128)   NOT NULL,
    [CampaignDesc]            NVARCHAR (1000)  NOT NULL,
    [AccountId]               INT              NOT NULL,
    [TimeZoneId]              TINYINT          NOT NULL,
    [DaylightSavingFlag]      TINYINT          NOT NULL,
    [StartDate]               DATETIME         NOT NULL,
    [EndDate]                 DATETIME         NULL,
    [CurrencyId]              SMALLINT         NOT NULL,
    [IncrementalBudgetAmt]    MONEY            NULL,
    [BudgetAmt]               MONEY            NULL,
    [LifeCycleStatusId]       TINYINT          NOT NULL,
    [PauseStatusId]           TINYINT          NOT NULL,
    [ModifiedByUserId]        INT              NOT NULL,
    [CreatedDtim]             DATETIME         NOT NULL,
    [ModifiedDtim]            DATETIME         NOT NULL,
    [Timestamp]               BINARY (8)       NOT NULL,
    [BudgetTypeId]            TINYINT          NOT NULL,
    [TrackConversion]         TINYINT          NOT NULL,
    [msrepl_tran_version]     UNIQUEIDENTIFIER DEFAULT (newid()) NOT NULL,
    [CashbackTypeId]          TINYINT          NULL,
    [BudgetPauseTypeId]       TINYINT          NULL,
    [BPTimeStamp]             DATETIME         NULL,
    [ExcludeSyndicatedSearch] TINYINT          NULL,
    [SourceID]                INT              NULL,
    [PartitionKeyId]          INT              NULL,
    [CampaignId]              INT              NULL,
    [RowId]                   BIGINT           NOT NULL,
    [ArchivedDtim]            DATETIME         NOT NULL,
    [ActionFlag]              TINYINT          NOT NULL,
    [IsSoftDeleted]           BIT              NULL,
    [ExtensionBitMask]        SMALLINT         NULL,
    [PhoneExtensionCountry]   CHAR (2)         NULL,
    [PhoneExtensionNumber]    VARCHAR (20)     NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_CampaignQueue]
    ON [dbo].[CampaignQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

