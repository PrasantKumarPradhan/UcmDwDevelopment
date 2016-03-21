#region - function calls

        #region INSERT Statement for TestLogEvent
         function InsertIntoTestLogEvent($TestLogID,$TestLogEventName,$LogEventStartTime,$LogEventEndTime)
         {
            $con = New-Object System.Data.SqlClient.SqlConnection
            $con.ConnectionString = $UcmValidationDBCnn
            $con.Credential = $creds
            $con.Open()
            $cmd = $con.CreateCommand()
            $Log_Insert_Statement = "INSERT INTO TestLogEvent(TestLogID,EventName,EventStartTime,EventEndTime) VALUES('{0}','{1}','{2}','{3}')" -f
                       $TestLogID,$TestLogEventName,$LogEventStartTime,$LogEventEndTime
            $cmd.CommandText = $Log_Insert_Statement                              
            $cmd.ExecuteNonQuery()
            }
         #endregion

         #region INSERT Statement for TestLog Table
         function InsertIntoTestLog($BatchId,$TestId,$TestLogStartTime)
         {
                $con = New-Object System.Data.SqlClient.SqlConnection
                $con.ConnectionString = $UcmValidationDBCnn
                $con.Credential = $creds
                $con.Open()
                $cmd = $con.CreateCommand()
                $Log_Insert_Statement = "INSERT INTO TestLog(BatchID,TestID,EventStartTime) VALUES('{0}','{1}','{2}')" -f
                              $BatchId,$TestId,$TestLogStartTime
                $cmd.CommandText = $Log_Insert_Statement                              
                $cmd.ExecuteNonQuery()
            }
         #endregion

        #region Insert Into TestLogRowCount table
            function InsertIntoTestLogRowCount($TestLogID,$SourceRowCount)
            {
                    $con = New-Object System.Data.SqlClient.SqlConnection
                    $con.ConnectionString = $UcmValidationDBCnn
                    $con.Credential = $creds
                    $con.Open()
                    $cmd = $con.CreateCommand()
                    $Log_Insert_Statement = "INSERT INTO TestLogRowCount(TestLogID,SourceCount) VALUES('{0}','{1}')" -f
                              $TestLogID,$SourceRowCount
                    $cmd.CommandText = $Log_Insert_Statement                              
                    $cmd.ExecuteNonQuery()
            }
         #endregion

         #region Update TestLogRowCount table
           function UpdateTestLogRowCount($DestinationRowCount)
           {
                $con = New-Object System.Data.SqlClient.SqlConnection
                $con.ConnectionString = $UcmValidationDBCnn
                $con.Credential = $creds
                $con.Open()
                $cmd = $con.CreateCommand()
                $Last_Insert_LogId = $("SELECT IDENT_CURRENT('TestLogRowCount')")
                $cmd = $con.CreateCommand()
                $cmd.CommandText = $Last_Insert_LogId
                $ID = $cmd.ExecuteScalar()
                $Log_Update_Statement = "Update TestLogRowCount set DestinationCount='$DestinationRowCount' WHERE ID=$ID"             
                $cmd.CommandText = $Log_Update_Statement                              
                $cmd.ExecuteNonQuery()
           }
         #endregion

        
        #region UPDATE Statement for TestLog Table
         function UpdateIntoTestLog($result,$LogEventEndTime,$TestLogID,$ResultTypeId)
         {
                $con = New-Object System.Data.SqlClient.SqlConnection
                $con.ConnectionString = $UcmValidationDBCnn
                $con.Credential = $creds
                $con.Open()
                $cmd = $con.CreateCommand()
                $Log_Update_Statement = "Update testlog set result='$result',ResultType='$ResultTypeId',EventEndTime='$LogEventEndTime' WHERE logid=$TestLogID"             
                $cmd.CommandText = $Log_Update_Statement                              
                $cmd.ExecuteNonQuery()
            }
         #endregion

        
         #region Submit U-SQL job and Loop till it completes
         function SubmitUsqlJob($SubscriptionName,$dataLakeAnalyticName,$jobName,$UsqlScriptFilePath,$adlAnalyticsAccountName,$Path)
         {
                Get-AzureRmSubscription –SubscriptionName $SubscriptionName | Select-AzureRmSubscription
                Submit-AzureRmDataLakeAnalyticsJob -AccountName $dataLakeAnalyticName `
                  -Name $jobName `
                  -ScriptPath $UsqlScriptFilePath `
                  -DegreeOfParallelism 20
            

                 while(!(Test-AzureRmDataLakeStoreItem  -Account $adlAnalyticsAccountName -Path $Path))
                 {
               
                     Start-Sleep -Seconds 5
                     Write-Host "Job is either compiling or is in running state"
               
                 }
         }

         #region Catch block
         function CatchBlock()
         {
            Write-Verbose -Message "Exception" -Verbose
            Write-Verbose -Message $_.Exception.GetType().FullName -Verbose 
            Write-Verbose -Message $_.Exception.Message -Verbose 
         }
         #endregion
#endregion
