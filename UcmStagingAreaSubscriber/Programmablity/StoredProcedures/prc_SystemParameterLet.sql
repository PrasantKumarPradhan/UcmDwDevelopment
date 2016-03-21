CREATE PROCEDURE [dbo].[prc_SystemParameterLet] (
        @ParameterType varchar(32) = NULL  -- The system parameter category.
      , @ParameterName varchar(57) = NULL  -- The system parameter name.
      , @ParameterValue varchar(1024)       -- The new setting.
      , @ParameterDefault varchar(1024) = NULL -- The default setting.
      , @ParameterDesc varchar(2048) = NULL    -- Parameter description
      , @LastModifiedDtim datetime = NULL -- Last updated date/time.
      , @AffectiveImmediately bit = 0            -- 0= update later, 1= update now.
    ) AS

    SET NOCOUNT ON

    DECLARE @FAIL smallint                      -- Failure code for RETURN.
    DECLARE @SUCCEED int                        -- Success code for RETURN.

    DECLARE @ProcedureName sysname              -- This procedure.
    DECLARE @RetCode int                        -- Procedure return code.

    SET @ProcedureName = OBJECT_NAME(@@PROCID)

    SET @FAIL = 1
    SET @SUCCEED = 0

    IF @ParameterName IS NULL
        BEGIN
            RAISERROR (50008, 16, -1, @ProcedureName, '@ParameterName')
            RETURN @FAIL
        END

    /*
    ** Parameter Check:  @ParameterType
    ** Make sure that the parameter category exists and is not NULL.
    */

    IF @ParameterType IS NULL
        BEGIN
            RAISERROR (50008, 16, -1, @ProcedureName, '@ParameterType')
            RETURN @FAIL
        END

    /*
    ** If this is a new parameter, let's create it in the table.
    */

    IF NOT EXISTS (SELECT *
                     FROM [dbo].[SystemParameters]
                    WHERE [ParameterType] = @ParameterType
                      AND [ParameterName] = @ParameterName)
        BEGIN

            INSERT INTO [dbo].[SystemParameters] (
                [ParameterType]
              , [ParameterName]
              , [ParameterValue_Current]
              , [ParameterValue_New]
              , [ParameterValue_Default]
              , [ParameterDesc]
              , [LastModifiedBy]
              , [LastModifiedDtim]
            ) VALUES (
                @ParameterType
              , @ParameterName
              , NULL
              , NULL
              , @ParameterDefault
              , isnull(@ParameterDesc,'')
              , SUSER_SNAME()
              , CURRENT_TIMESTAMP
            )

            IF @@ERROR <> 0
                BEGIN
                    RAISERROR (50003, 16, -1, @ProcedureName, '[dbo].[SystemParameters]')
                    RETURN @FAIL
                END
        END

    /*
    ** Before we update, let's make sure someone else hasn't already updated!
    */

    IF @LastModifiedDtim IS NOT NULL
        BEGIN
            IF NOT EXISTS (
                SELECT *
                  FROM [dbo].[SystemParameters]
                 WHERE [ParameterName] = @ParameterName
                   AND [ParameterType] = @ParameterType
                   AND CONVERT(varchar, [LastModifiedDtim], 121) = CONVERT(varchar, @LastModifiedDtim, 121)
                )
                BEGIN
                    RAISERROR (50030, 16, -1, @ProcedureName)
                    RETURN @FAIL
                END
        END

    /*
    ** Set the parameter value.
    */

    UPDATE [dbo].[SystemParameters]
      WITH (HOLDLOCK)
       SET [ParameterValue_New] = @ParameterValue
         , [ParameterDesc] = isnull(@ParameterDesc, [ParameterDesc])
         , [LastModifiedBy] = SUSER_SNAME()
         , [LastModifiedDtim] = GETUTCDATE()
     WHERE [ParameterName] = @ParameterName
       AND [ParameterType] = @ParameterType

    IF @@ERROR <> 0
        BEGIN
            RAISERROR (50001, 16, -1, @ProcedureName, '[dbo].[SystemParameters]')
            RETURN @FAIL
        END

    IF @AffectiveImmediately = 1
        BEGIN
            EXECUTE @RetCode = [dbo].[prc_SystemParameterSet]
                @ParameterType = @ParameterType
              , @ParameterName = @ParameterName

            IF @@ERROR <> 0
                BEGIN
                    RAISERROR (50017, 16, -1, @ProcedureName, '[dbo].[prc_SystemParameterSet]')
                    RETURN @FAIL
                END

            IF @RetCode <> 0 RETURN @FAIL

        END
    RETURN @SUCCEED
