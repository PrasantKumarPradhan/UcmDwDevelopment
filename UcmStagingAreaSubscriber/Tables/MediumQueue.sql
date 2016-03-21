CREATE TABLE [dbo].[MediumQueue] (
    [PublisherId]   TINYINT       NULL,
    [PubProcessId]  BIGINT        NULL,
    [RowId]         BIGINT        NULL,
    [MediumId]      SMALLINT      NULL,
    [MediumName]    NVARCHAR (50) NULL,
    [CreatedDtim]   DATETIME      NULL,
    [ArchiveDTim]   DATETIME      NULL,
    [ActionFlag]    TINYINT       NULL,
    [IsSoftDeleted] BIT           NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_MediumQueue]
    ON [dbo].[MediumQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

