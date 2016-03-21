CREATE TABLE [dbo].[CouponClassMediumQueue] (
    [PublisherId]   TINYINT  NULL,
    [PubProcessId]  BIGINT   NULL,
    [RowId]         BIGINT   NULL,
    [CouponClassId] INT      NULL,
    [MediumId]      SMALLINT NULL,
    [ModifiedDtim]  DATETIME NULL,
    [ActionFlag]    TINYINT  NULL,
    [ArchiveDtim]   DATETIME NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_CouponClassMediumQueue]
    ON [dbo].[CouponClassMediumQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

