CREATE TABLE [dbo].[AdTypeQueue] (
    [PublisherId]   TINYINT      NULL,
    [PubProcessId]  BIGINT       NULL,
    [AdTypeId]      SMALLINT     NULL,
    [AdTypeName]    VARCHAR (50) NULL,
    [CreatedDTim]   DATETIME     NULL,
    [RowId]         BIGINT       NULL,
    [ArchiveDTim]   DATETIME     NULL,
    [ActionFlag]    TINYINT      NULL,
    [IsSoftDeleted] BIT          NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [PKC_AdTypeQueue]
    ON [dbo].[AdTypeQueue]([PublisherId] ASC, [PubProcessId] ASC, [RowId] ASC);

