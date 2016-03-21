CREATE TABLE [dbo].[CouponPurposeQueue] (
    [PublisherId]        TINYINT       NULL,
    [PubProcessId]       BIGINT        NULL,
    [RowId]              BIGINT        NULL,
    [CouponPurposeId]    TINYINT       NULL,
    [PurposeName]        VARCHAR (30)  NULL,
    [PurposeDescription] VARCHAR (100) NULL,
    [ActionFlag]         TINYINT       NULL,
    [ArchiveDtim]        DATETIME      NULL,
    [ModifiedDtim]       DATETIME      NULL,
    [IsSoftDeleted]      BIT           NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_CouponPurposeQueue]
    ON [dbo].[CouponPurposeQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

