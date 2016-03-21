CREATE TABLE [dbo].[APPStagingProcessStatus] (
    [StatusId]    TINYINT      NOT NULL,
    [StatusName]  VARCHAR (50) NOT NULL,
    [UpdatedDTim] DATETIME     DEFAULT (getutcdate()) NOT NULL,
    PRIMARY KEY CLUSTERED ([StatusId] ASC)
);

