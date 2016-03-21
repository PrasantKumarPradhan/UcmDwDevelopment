CREATE TABLE [dbo].[PaymentInstrQueue] (
    [PublisherId]        TINYINT       NULL,
    [PubProcessId]       BIGINT        NULL,
    [PaymentInstrId]     INT           NULL,
    [PaymentInstrName]   NVARCHAR (50) NULL,
    [PaymentInstrTypeId] TINYINT       NULL,
    [CustomerId]         INT           NULL,
    [LifecycleStatusId]  TINYINT       NULL,
    [AddressId]          INT           NULL,
    [CreatedDTim]        DATETIME      NULL,
    [ModifiedDTim]       DATETIME      NULL,
    [ModifiedByUserId]   INT           NULL,
    [ArchiveDTim]        DATETIME      NULL,
    [ActionFlag]         TINYINT       NULL,
    [RowId]              BIGINT        NULL,
    [PaymentInstrNumber] CHAR (10)     NULL,
    [ValidationCount]    TINYINT       NULL,
    [PIN]                INT           NULL,
    [LastValidationDTim] DATETIME      NULL,
    [PINResentCount]     TINYINT       NULL,
    [RPMID]              BIGINT        NULL,
    [CTPAccountID]       VARCHAR (32)  NULL,
    [CTPPaymentMethodID] VARCHAR (20)  NULL,
    [AccountHolderName]  NVARCHAR (64) NULL,
    [FriendlyName]       NVARCHAR (64) NULL,
    [SystemCreated]      BIT           NULL,
    [Timestamp]          BINARY (8)    NULL,
    [LandedDateTime]     DATETIME      DEFAULT (getutcdate()) NOT NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_PaymentInstrQueue]
    ON [dbo].[PaymentInstrQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

