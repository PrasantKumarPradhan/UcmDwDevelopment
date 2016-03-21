CREATE PROCEDURE [dbo].[prc_SystemParameterSet] (
        @ParameterType varchar(32) = NULL  -- The system parameter category.
      , @ParameterName varchar(57) = NULL  -- The system parameter name.
    ) AS

    SET NOCOUNT ON

    /*
    ** Declarations.
    */

    DECLARE @FAIL smallint                      -- Failure code for RETURN.
    DECLARE @SUCCEED int                        -- Success code for RETURN.

    DECLARE @ProcedureName sysname              -- This procedure.

    SET @ProcedureName = OBJECT_NAME(@@PROCID)

    SET @FAIL = 1
    SET @SUCCEED = 0

    IF @ParameterName IS NOT NULL
        IF NOT EXISTS (SELECT *
                         FROM [dbo].[SystemParameters]
                        WHERE [ParameterType] = COALESCE(@ParameterType, '')
                          AND [ParameterName] = @ParameterName)
            BEGIN
                RAISERROR (50009, 16, -1, @ProcedureName, @ParameterName)
                RETURN @FAIL
            END

    IF @ParameterType IS NOT NULL
        IF @ParameterName IS NULL
            UPDATE [dbo].[SystemParameters]
               SET [ParameterValue_Current] = ParameterValue_New
             WHERE [ParameterType] = @ParameterType
        ELSE
            UPDATE [dbo].[SystemParameters]
               SET [ParameterValue_Current] = ParameterValue_New
             WHERE [ParameterName] = @ParameterName
               AND [ParameterType] = @ParameterType
    ELSE
        UPDATE [dbo].[SystemParameters]
           SET [ParameterValue_Current] = ParameterValue_New

    IF @@ERROR <> 0
        BEGIN
            RAISERROR (50001, 16, -1, @ProcedureName, '[dbo].[SystemParameters]')
            RETURN @FAIL
        END

    RETURN @SUCCEED
