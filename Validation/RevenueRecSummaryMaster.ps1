
#Login-AzureRmAccount
#Common Variables 
$UcmSQLDatawarehouseDBConnection = "Server=ucmdatawarehousesqldev.database.windows.net;Database=UcmSqlDataWarehouse;Integrated Security = False;User ID = ucmdwsuper@ucmdatawarehousesqldev; Password = Microsoft!1;"
$UcmValidationDBConnection = "Server=ucmdatawarehousesqldev.database.windows.net;Database=UcmDWValidation;User ID=ucmdwsuper@ucmdatawarehousesqldev; Password = Microsoft!1;"
$TabularCubeConnection="Provider=MSOLAP;Data Source=104.210.4.250;Initial Catalog=UcmCube;"
#endregion

#region Global Flags
#$BatchID = -1;
$ExitOnFirstError = 0;
#endregion

#region Stage Flags
#If one state has to be skipped, make that state as false- by default all are true
$SourcesStageCheck=0;
$StagingStageCheck=0;
$CleansingStageCheck=0;
$EnrichmentStageCheck=0;
$TransformationStageCheck=0;
$DatawarehouseStageCheck=0;
$BusinessLogicStageCheck=0;
$SQLStagingStageCheck=1;
$TabularCubeStageCheck=0;
$PresentationStageCheck=0;
$EmailStageCheck=0;
$ErrorsStageCheck=0;

#STATIC Values pre-defined for convenience and entry into Log DB
$SourceStageID=1
$StagingStageID=2
$CleansingStageID=3
$EnrichmentStageID=4
$TransformationStageID=5
$DatawarehouseStageID=6
$BusinessLogicStageID=7
$SQLStagingStageID=8
$TabularCubeStageID=9
$PresentationStageID=10
$EmailStageID=11
$ErrorsStageID=12
#endregion

#region Error Logging in a text file
function Verbose([String] $Message, [String] $LogFile)
{
    Try
    {
        Write-Output "VERBOSE $(Get-Date -format HH:mm:ss)`t: $Message"  | Tee-Object -Append $LogFile
    }
    Catch
    {
        Write-Verbose -Message "Exception" -Verbose
        Write-Verbose -Message $_.Exception.GetType().FullName -Verbose 
        Write-Verbose -Message $_.Exception.Message -Verbose 
    }
}
#endregion


#region Create a New Test Batch
    #region Connect with Databases 
    $DBconn = New-Object System.Data.SqlClient.SqlConnection
    $DBconn.ConnectionString = $UcmValidationDBConnection
    $DBconn.Credential = $creds
    $DBconn.Open()
    #endregion


    $BatchName = "Batch " + (Get-Date).ToString().Replace("/","-").Replace(":","-")
    $BatchDateKey = (Get-Date).ToShortDateString().ToString().Replace("/","")

    $cmd = $con.CreateCommand()
    $insert_stmt = "INSERT INTO Batch(BatchName) VALUES('{0}')" -f
                            $BatchName
    $cmd.CommandText = $insert_stmt                              
    $cmd.ExecuteNonQuery()

    $Last_Insert_id = $("SELECT IDENT_CURRENT('Batch')")
    $cmd = $con.CreateCommand()
    $cmd.CommandText = $Last_Insert_id
    $BatchId = $cmd.ExecuteScalar()

    if ($BatchId -eq -1) {return;}
#endregion



        
#region - function calls

        #region INSERT Statement for TestLogEvent
         function InsertIntoTestLogEvent($TestLogID,$TestLogEventName,$LogEventStartTime,$LogEventEndTime)
         {
            $con = New-Object System.Data.SqlClient.SqlConnection
            $con.ConnectionString = $UcmValidationDBConnection
            $con.Credential = $creds
            $con.Open()
            $cmd = $con.CreateCommand()
            $Log_Insert_Statement = "INSERT INTO TestLogEvent(TestLogID,EventName,EventStartTime,EventEndTime) VALUES('{0}','{1}','{2}','{3}')" -f
                       $TestLogID,$TestLogEventName,$LogEventStartTime,$LogEventEndTime
            $cmd.CommandText = $Log_Insert_Statement                              
            $cmd.ExecuteNonQuery()
            $con.Close()
            }
         #endregion

         #region INSERT Statement for TestLog Table
         function InsertIntoTestLog($BatchId,$TestId,$TestLogStartTime)
         {
                $con = New-Object System.Data.SqlClient.SqlConnection
                $con.ConnectionString = $UcmValidationDBConnection
                $con.Credential = $creds
                $con.Open()
                $cmd = $con.CreateCommand()
                $Log_Insert_Statement = "INSERT INTO TestLog(BatchID,TestID,EventStartTime) VALUES('{0}','{1}','{2}')" -f
                              $BatchId,$TestId,$TestLogStartTime
                $cmd.CommandText = $Log_Insert_Statement                              
                $cmd.ExecuteNonQuery()
                $con.Close()
            }
         #endregion

        #region Insert Into TestLogRowCount table
            function InsertIntoTestLogRowCount($TestLogID,$SourceRowCount)
            {
                    $con = New-Object System.Data.SqlClient.SqlConnection
                    $con.ConnectionString = $UcmValidationDBConnection
                    $con.Credential = $creds
                    $con.Open()
                    $cmd = $con.CreateCommand()
                    $Log_Insert_Statement = "INSERT INTO TestLogRowCount(TestLogID,SourceCount) VALUES('{0}','{1}')" -f
                              $TestLogID,$SourceRowCount
                    $cmd.CommandText = $Log_Insert_Statement                              
                    $cmd.ExecuteNonQuery()
                    $con.Close()
            }
         #endregion

         #region Update TestLogRowCount table
           function UpdateTestLogRowCount($DestinationRowCount)
           {
                $con = New-Object System.Data.SqlClient.SqlConnection
                $con.ConnectionString = $UcmValidationDBConnection
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
                $con.Close()
           }
         #endregion

        
        #region UPDATE Statement for TestLog Table
         function UpdateIntoTestLog($result,$LogEventEndTime,$TestLogID,$ResultTypeId)
         {
                $con = New-Object System.Data.SqlClient.SqlConnection
                $con.ConnectionString = $UcmValidationDBConnection
                $con.Credential = $creds
                $con.Open()
                $cmd = $con.CreateCommand()
                $Log_Update_Statement = "Update testlog set result='$result',ResultType='$ResultTypeId',EventEndTime='$LogEventEndTime' WHERE logid=$TestLogID"             
                $cmd.CommandText = $Log_Update_Statement                              
                $cmd.ExecuteNonQuery()
                $con.Close()
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

         
if($SourcesStageCheck -eq 1)
{
        
        #region TESTCASE - 1 (RowCount_TestCase:Source->Staging - RevenueRecSummary)
        $ScriptPath = Split-Path $MyInvocation.InvocationName
        .$ScriptPath\Source2Staging_RowCount.ps1
        if ((CompareSourceFileStagingFileCountCheck 1 $BatchId  "RevenueRecSummary" "D:\PS-scripts\RevenueRecSummary_Validations\Source2Staging\ScriptSource2StagingTemplate.txt" "D:\PS-scripts\RevenueRecSummary_Validations\Source2Staging\SourceFiles\RevenueRecSummary\RevenueRecSummary_20160316_20160316.csv" "/UCMDW/STAGING/REVENUERECSUMMARY/DataCleansed20160319.tsv" 36) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion 

}


if($StagingStageCheck -eq 1)
{
        
               
}



#EMPTY For now
if($CleansingStageCheck -eq 1)
{

}


#EMPTY For now
if($EnrichmentStageCheck -eq 1)
{

}

#EMPTY For now
if($TransformationStageCheck -eq 1)
{


}

# PK, FK, Duplicates etc.
if($DatawarehouseStageCheck -eq 1 )
{   

        
        #region TESTCASE 7 (PrimaryKey_TestCase:Staging->Catalog - FactRevenueRecSummary)
        $ScriptPath = Split-Path $MyInvocation.InvocationName
        .$ScriptPath\Staging2CatalogPK.ps1
        .$ScriptPath\Staging2CatalogFK.ps1
        if((CatalogTablePrimaryKeyCheck $BatchId 7 "FactRevenueRecSummary" "D:\PS-scripts\RevenueRecSummary_Validations\Staging2Catalog\ScriptStaging2Catalog_PrimaryKey_Template.txt" "RevRecFactKey") -eq -1)
        {
           #Something failed badly and put a log and return.
           #Put a STOP Execution in DB Error Log and return from the Master Script
           $logtimenow = Get-Date -format dd-MM-yy-HHmm
           $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
           Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion 

        #region TESTCASE 8 (PrimaryKey_TestCase:Staging->Catalog - DimPublisher)
        if((CatalogTablePrimaryKeyCheck $BatchId 8 "DimPublisher" "D:\PS-scripts\RevenueRecSummary_Validations\Staging2Catalog\ScriptStaging2Catalog_PrimaryKey_Template.txt" "PublisherKey") -eq -1)
        {
           #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        
        }
        #endregion 

        #region TESTCASE 9 (PrimaryKey_TestCase:Staging->Catalog - DimSoldBy)
        if((CatalogTablePrimaryKeyCheck $BatchId 9 "DimSoldBy" "D:\PS-scripts\RevenueRecSummary_Validations\Staging2Catalog\ScriptStaging2Catalog_PrimaryKey_Template.txt" "SoldByKey") -eq -1)
        {
                #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"

        }
        #endregion
        
        #region TESTCASE 10 (PrimaryKey_TestCase:Staging->Catalog - DimSKU)
        if((CatalogTablePrimaryKeyCheck $BatchId 10 "DimSku" "D:\PS-scripts\RevenueRecSummary_Validations\Staging2Catalog\ScriptStaging2Catalog_PrimaryKey_Template.txt" "SkuKey") -eq -1)
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"

        }
        #endregion

        #region TESTCASE 11 (PrimaryKey_TestCase:Staging->Catalog - DimSKU)
        if((CatalogTablePrimaryKeyCheck $BatchId 11 "DimDate" "D:\PS-scripts\RevenueRecSummary_Validations\Staging2Catalog\ScriptStaging2Catalog_PrimaryKey_Template.txt" "DateKey") -eq -1)
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"

        }
        #endregion
        
        
        #region TESTCASE 17 (Foreign_TestCase:Staging->Catalog - DimPublisher)
        if(CatalogTableForeignKeyCheck $BatchId 17 "FactRevenueRecSummary" "DimPublisher" "D:\PS-scripts\RevenueRecSummary_Validations\Staging2Catalog\ScriptStaging2Catalog_ForeignKey_Template.txt" "PublisherKey" "PublisherKey")
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE 18 (Foreign_TestCase:Staging->Catalog - DimSoldBy)
        if(CatalogTableForeignKeyCheck $BatchId 18 "FactRevenueRecSummary" "DimSoldBy" "D:\PS-scripts\RevenueRecSummary_Validations\Staging2Catalog\ScriptStaging2Catalog_ForeignKey_Template.txt" "SoldByKey" "SoldByKey")
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE 19 (Foreign_TestCase:Staging->Catalog - DimSKU)
            if(CatalogTableForeignKeyCheck $BatchId 19 "FactRevenueRecSummary" "DimSku" "D:\PS-scripts\RevenueRecSummary_Validations\Staging2Catalog\ScriptStaging2Catalog_ForeignKey_Template.txt" "SkuKey" "SkuKey" )
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE 20 (Foreign_TestCase:Staging->Catalog - DimDate)
            if(CatalogTableForeignKeyCheck $BatchId 20 "FactRevenueRecSummary" "DimDate" "D:\PS-scripts\RevenueRecSummary_Validations\Staging2Catalog\ScriptStaging2Catalog_ForeignKey_Template.txt" "DateKey" "RevenueRecDateKey" )
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }#>
        #endregion

}



#EMPTY For now
if($BusinessLogicStageCheck -eq 1)
{


}




if($SQLStagingStageCheck -eq 1 )
{
        #region TESTCASE 21 (RowCount_TestCase:Catalog->AzureSQL - FactRevenueRecSummary)
        $ScriptPath = Split-Path $MyInvocation.InvocationName
        .$ScriptPath\Catalog2AzureSQL_RowCount.ps1
        if((CompareCatalogTableSQLTableCountCheck  21 $BatchId  "FactRevenueRecSummary" "D:\PS-scripts\RevenueRecSummary_Validations\Catalog2AzureSQL\RowCountScript.txt") -eq -1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE 22 (RowCount_TestCase:Catalog->AzureSQL - DimPublisher)
        if((CompareCatalogTableSQLTableCountCheck  22 $BatchId "DimPublisher" "D:\PS-scripts\RevenueRecSummary_Validations\Catalog2AzureSQL\RowCountScript.txt") -eq -1)
        {
            #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE 23 (RowCount_TestCase:Catalog->AzureSQL - DimSoldBy)
        if((CompareCatalogTableSQLTableCountCheck  23 $BatchId "DimSoldBy" "D:\PS-scripts\RevenueRecSummary_Validations\Catalog2AzureSQL\RowCountScript.txt") -eq -1)
        {
        #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE 24 (RowCount_TestCase:Catalog->AzureSQL - DimSKU)
        if((CompareCatalogTableSQLTableCountCheck  24 $BatchId "DimSku" "D:\PS-scripts\RevenueRecSummary_Validations\Catalog2AzureSQL\RowCountScript.txt") -eq -1)
        {
            #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE 25 (RowCount_TestCase:Catalog->AzureSQL - DimDate)
        if((CompareCatalogTableSQLTableCountCheck  25 $BatchId "DimDate" "D:\PS-scripts\RevenueRecSummary_Validations\Catalog2AzureSQL\RowCountScript.txt") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion
}

if($TabularCubeStageCheck -eq 1 )
{
        $ScriptPath = Split-Path $MyInvocation.InvocationName
        .$ScriptPath\AzureSQL2Tabular_RowCount.ps1
        #region TESTCASE 26 (RowCount_TestCase:AzureSQL->Tabular - FactRevenueRecSummary)
         if((CompareDBTableAndTabularCubeTableRowCountCheck 26  $BatchId  "FactRevenueRecSummary"  "FactRevenueRecSummary" "RevRecFactKey") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        
        #region TESTCASE 27 (RowCount_TestCase:AzureSQL->Tabular - DimPublisher)
         if((CompareDBTableAndTabularCubeTableRowCountCheck 27  $BatchId  "DimPublisher"  "DimPublisher" "PublisherSK") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE 28 (RowCount_TestCase:AzureSQL->Tabular - DimSoldBy)
         if((CompareDBTableAndTabularCubeTableRowCountCheck 28  $BatchId  "DimSoldBy"  "DimSoldBy" "SoldBySk") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE 29 (RowCount_TestCase:AzureSQL->Tabular - DimSKU)
         if((CompareDBTableAndTabularCubeTableRowCountCheck 29  $BatchId  "DimSKU"  "DimSKU" "SkuSK") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE 30 (RowCount_TestCase:AzureSQL->Tabular - DimDate)
         if((CompareDBTableAndTabularCubeTableRowCountCheck 30  $BatchId  "DimDate"  "DimDate" "DateKey") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"



                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion


}

