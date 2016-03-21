CREATE TABLE [dbo].[SystemParameters] (
    [ParameterType]          VARCHAR (32)    NOT NULL,
    [ParameterName]          VARCHAR (57)    NOT NULL,
    [ParameterValue_Current] VARCHAR (1024)  NULL,
    [ParameterValue_New]     VARCHAR (1024)  NULL,
    [ParameterValue_Default] VARCHAR (1024)  NULL,
    [ParameterDesc]          NVARCHAR (1024) NOT NULL,
    [LastModifiedBy]         [sysname]       NOT NULL,
    [LastModifiedDtim]       DATETIME        NOT NULL
);

