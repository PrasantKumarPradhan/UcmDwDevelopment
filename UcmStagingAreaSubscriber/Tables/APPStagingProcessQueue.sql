CREATE TABLE [dbo].[APPStagingProcessQueue] (
    [ProcessQueueId]       BIGINT         IDENTITY (1, 1) NOT NULL,
    [ProcessId]            BIGINT         NOT NULL,
    [TableId]              INT            NOT NULL,
    [StatusId]             TINYINT        NOT NULL,
    [ErrMsg]               VARCHAR (4000) NULL,
    [ExtractStartedDTim]   DATETIME       DEFAULT (getutcdate()) NOT NULL,
    [ExtractCompletedDTim] DATETIME       NULL,
    [MergeCompletedDTim]   DATETIME       NULL,
    [LastModifiedDTim]     DATETIME       DEFAULT (getutcdate()) NOT NULL,
    [Imported]             INT            NULL,
    [Inserts]              INT            NULL,
    [Updates]              INT            NULL,
    [Deletes]              INT            NULL,
    [SkippedByMerge]       INT            NULL,
    [SkippedByDeDupe]      INT            NULL,
    [Generations]          INT            NULL,
    [LastRowId]            BIGINT         NULL,
    [LastArchivedDtim]     DATETIME       NULL,
    PRIMARY KEY CLUSTERED ([ProcessQueueId] ASC),
    CONSTRAINT [APPStagingProcessQueue_ProcessId_FK] FOREIGN KEY ([ProcessId]) REFERENCES [dbo].[APPStagingProcess] ([ProcessId]),
    CONSTRAINT [APPStagingProcessQueue_StatusId_FK] FOREIGN KEY ([StatusId]) REFERENCES [dbo].[APPStagingProcessStatus] ([StatusId]),
    CONSTRAINT [APPStagingProcessQueue_TableId_FK] FOREIGN KEY ([TableId]) REFERENCES [dbo].[AppStagingTable] ([TableId])
);


GO
CREATE NONCLUSTERED INDEX [APPStagingProcessQueue_ProcessId_IX]
    ON [dbo].[APPStagingProcessQueue]([ProcessId] ASC);


GO
CREATE NONCLUSTERED INDEX [APPStagingProcessQueue_TableId_IX]
    ON [dbo].[APPStagingProcessQueue]([TableId] ASC);

