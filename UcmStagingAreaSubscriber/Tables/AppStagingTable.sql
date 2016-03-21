CREATE TABLE [dbo].[AppStagingTable] (
    [TableId]              INT           IDENTITY (1, 1) NOT NULL,
    [StagingQueueId]       INT           NOT NULL,
    [StagingTable]         [sysname]     NOT NULL,
    [TargetTable]          [sysname]     NOT NULL,
    [MergeKeyColumnList]   VARCHAR (400) NOT NULL,
    [DeDupe]               BIT           NOT NULL,
    [DedupeKeyColumn]      VARCHAR (100) NULL,
    [TruncateOnCompletion] BIT           NOT NULL,
    [AutoMerge]            BIT           CONSTRAINT [DF_AppStagingTable_AutoMerge] DEFAULT ((1)) NOT NULL,
    [MergeProcedure]       [sysname]     NULL,
    PRIMARY KEY CLUSTERED ([TableId] ASC)
);

