CREATE TABLE [dbo].[APPStagingProcess] (
    [ProcessId]          BIGINT         IDENTITY (1, 1) NOT NULL,
    [PublisherId]        TINYINT        NULL,
    [PubProcessId]       BIGINT         NULL,
    [SASubProcessId]     BIGINT         NULL,
    [StatusId]           TINYINT        NOT NULL,
    [ErrMsg]             VARCHAR (4000) NULL,
    [ProcessStartedDTim] DATETIME       DEFAULT (getutcdate()) NOT NULL,
    [UpdatedDTim]        DATETIME       NOT NULL,
    PRIMARY KEY CLUSTERED ([ProcessId] ASC),
    CONSTRAINT [APPStagingProcess_StatusId_FK] FOREIGN KEY ([StatusId]) REFERENCES [dbo].[APPStagingProcessStatus] ([StatusId])
);


GO
CREATE NONCLUSTERED INDEX [APPStagingProcess_PubProcessId_IX]
    ON [dbo].[APPStagingProcess]([PubProcessId] ASC);


GO
CREATE NONCLUSTERED INDEX [APPStagingProcess_SASubProcessId_IX]
    ON [dbo].[APPStagingProcess]([SASubProcessId] ASC);

