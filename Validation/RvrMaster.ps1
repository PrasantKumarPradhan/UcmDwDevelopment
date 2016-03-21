#Login-AzureRmAccount
#Common Variables 
#$UcmSqlDataWarehouseConnectionString = "Server=ldtevtephi.database.windows.net;Database=UcmStagingAreaSubscriber;Integrated Security = False;User ID = bactest; Password = Microsoft!1;"
$UcmSqlDataWarehouseConnectionString = "Server=ucmdatawarehousesqldev.database.windows.net;Database=UcmSqlDataWarehouse;Integrated Security = False;User ID = ucmdwsuper@ucmdatawarehousesqldev; Password = Microsoft!1;"
$UcmValidationDbConnectionString = "Server=ucmdatawarehousesqldev.database.windows.net;Database=UcmDWValidation;User ID=ucmdwsuper@ucmdatawarehousesqldev; Password = Microsoft!1;"
$TextFilePath_Staging2Catalog_PK = "D:\UCM_Validation\Staging2Catalog\PKscript.txt"
$TextFilePath_Source2Staging="D:\PS-scripts\BINGADS\Source2Staging\ScriptSource2StagingTemplate.txt"
$TextFilePath_Staging2Catalog="D:\PS-scripts\BINGADS\Staging2Catalog\ScriptStaging2Catalog_DimRowCount_Template.txt"
$TextFilePath_Catalog2AzureSql="D:\PS-scripts\BingAds\Catalog2AzureSQL\RowCountScript.txt"
$TextFilePath_AzureSqlToTabularCube="D:\PS-scripts\RvR\AzureSQLToTabularCube\RowCount.csv"
$TextFilePath_AzureSqlToTabularCubeFinal="D:\PS-scripts\RvR\AzureSQLToTabularCube\FinalRowCount.csv"

#$TabularCubeCnn="Provider=MSOLAP;Data Source=104.210.4.250;Initial Catalog=UcmCube;"
#endregion

#region Global Flags
#$BatchID = -1;
$ExitOnFirstError = 0;
#endregion

#region Stage Flags
#If one state has to be skipped, make that state as false- by default all are true
$SourcesStageCheck=0;
$StagingStageCheck=1;
$CleansingStageCheck=1;
$EnrichmentStageCheck=1;
$TransformationStageCheck=1;
$DatawarehouseStageCheck=1;
$BusinessLogicStageCheck=1;
$SQLStagingStageCheck=1;
$TabularCubeStageCheck=1;
$PresentationStageCheck=1;
$EmailStageCheck=1;
$ErrorsStageCheck=1;

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
    $DBconn.ConnectionString = $UcmValidationDbConnectionString
    $DBconn.Credential = $creds
    $DBconn.Open()
    #endregion

    

    $BatchName = "Batch " + (Get-Date).ToString().Replace("/","-").Replace(":","-")
    $BatchDateKey = (Get-Date).ToShortDateString().ToString().Replace("/","")

    $cmd = $DBconn.CreateCommand()
    $insert_stmt = "INSERT INTO Batch(BatchName) VALUES('{0}')" -f
                            $BatchName
    $cmd.CommandText = $insert_stmt                              
    $cmd.ExecuteNonQuery()

    $Last_Insert_id = $("SELECT IDENT_CURRENT('Batch')")
    $cmd = $DBconn.CreateCommand()
    $cmd.CommandText = $Last_Insert_id
    $BatchId = $cmd.ExecuteScalar()

    if ($BatchId -eq -1) {return;}
#endregion


#region - Common Functions

        #region INSERT Statement for TestLogEvent
         function InsertIntoTestLogEvent($TestLogID,$TestLogEventName,$LogEventStartTime,$LogEventEndTime)
         {
            $con = New-Object System.Data.SqlClient.SqlConnection
            $con.ConnectionString = $UcmValidationDbConnectionString
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
                $con.ConnectionString = $UcmValidationDbConnectionString
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
                    $con.ConnectionString = $UcmValidationDbConnectionString
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
                $con.ConnectionString = $UcmValidationDbConnectionString
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
                $con.ConnectionString = $UcmValidationDbConnectionString
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
#endregion

         
if($SourcesStageCheck -eq 1)
{
        #region Calling a PowerShell Script
         . "D:\PS-scripts\RvR\Source2StagingRowcount.ps1"
        #endregion

        #region TESTCASE - 1 (RowCount_TestCase:Source->Staging - DeviceType)
          if ((CompareSourceFileStagingFileCountCheck 61 $BatchId "DeviceType" $TextFilePath_Source2Staging "D:\PS-scripts\RvR\Source2Staging\SourceFiles\DeviceType.tsv" "/UCMDW/STAGING/RVR/DeviceType.tsv" 2 ) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion 

        #region TESTCASE - 2 (RowCount_TestCase:Source->Staging - PricingLevel)
         if ((CompareSourceFileStagingFileCountCheck 62 $BatchId  "PricingLevel" $TextFilePath_Source2Staging "D:\PS-scripts\RvR\Source2Staging\SourceFiles\PricingLevel.tsv" "/UCMDW/STAGING/RVR/PricingLevel.tsv" 2 ) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 3 (RowCount_TestCase:Source->Staging - ProductFamily)
          
        if ((CompareSourceFileStagingFileCountCheck 63 $BatchId  "ProductFamily" $TextFilePath_Source2Staging "D:\PS-scripts\RvR\Source2Staging\SourceFiles\ProductFamily.tsv" "/UCMDW/STAGING/RVR/ProductFamily.tsv" 3) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 4 (RowCount_TestCase:Source->Staging - RevSumCategory)
                if ((CompareSourceFileStagingFileCountCheck 64 $BatchId  "RevSumCategory" $TextFilePath_Source2Staging "D:\PS-scripts\RvR\Source2Staging\SourceFiles\RevSumCategory.tsv" "/UCMDW/STAGING/RVR/RevSumCategory.tsv" 3 ) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 5 (RowCount_TestCase:Source->Staging - RevSumDivision)
         if ((CompareSourceFileStagingFileCountCheck 65 $BatchId  "RevSumDivision" $TextFilePath_Source2Staging "D:\PS-scripts\RvR\Source2Staging\SourceFiles\RevSumDivision.tsv" "/UCMDW/STAGING/RVR/RevSumDivision.tsv" 3 ) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 6(RowCount_TestCase:Source->Staging - RevenueType)
         
        if ((CompareSourceFileStagingFileCountCheck 66 $BatchId  "RevenueType" $TextFilePath_Source2Staging "D:\PS-scripts\RvR\Source2Staging\SourceFiles\RevenueType.tsv" "/UCMDW/STAGING/RVR/RevenueType.tsv" 4 ) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

         #region TESTCASE - 7(RowCount_TestCase:Source->Staging - RvrData)
         
        if ((CompareSourceFileStagingFileCountCheck 67 $BatchId  "RvrData" $TextFilePath_Source2Staging  "D:\PS-scripts\RvR\Source2Staging\SourceFiles\RvrData.tsv" "/UCMDW/STAGING/RVR/RvrData.tsv" 13 ) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

         #region TESTCASE - 8(RowCount_TestCase:Source->Staging - SrpvData)
         
        if ((CompareSourceFileStagingFileCountCheck 68 $BatchId  "SrpvData" $TextFilePath_Source2Staging  "D:\PS-scripts\RvR\Source2Staging\SourceFiles\SrpvData.tsv" "/UCMDW/STAGING/RVR/SrpvData.tsv" 11 ) -eq - 1)
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
        #region Calling a PowerShell Script
         . "D:\PS-scripts\RvR\Staging2Catalog_Rowcount.ps1"
        #endregion

          #region TESTCASE - 9 (RowCount_TestCase:Staging->Catalog - DimDeviceType)
	 if ((CompareStagingFileCatalogTableCountCheck $BatchId 69 "DimDeviceType" $TextFilePath_Staging2Catalog "/UCMDW/STAGING/RVR/DeviceType.tsv" 2 ) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion 

        #region TESTCASE - 10 (RowCount_TestCase:Staging->Catalog - DimPricingLevel)
         if ((CompareStagingFileCatalogTableCountCheck $BatchId  70 "DimPricingLevel" $TextFilePath_Staging2Catalog "/UCMDW/STAGING/RVR/PricingLevel.tsv" 2) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 11 (RowCount_TestCase:Staging->Catalog - DimProductFamily)
         if ((CompareStagingFileCatalogTableCountCheck $BatchId  71 "DimProductFamily" $TextFilePath_Staging2Catalog "/UCMDW/STAGING/RVR/ProductFamily.tsv" 3) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 12 (RowCount_TestCase:Staging->Catalog - DimRevSumCategory)
         if ((CompareStagingFileCatalogTableCountCheck $BatchId  72 "DimRevSumCategory" $TextFilePath_Staging2Catalog "/UCMDW/STAGING/RVR/RevSumCategory.tsv" 3) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 13 (RowCount_TestCase:Staging->Catalog - DimRevSumDevision)
         if ((CompareStagingFileCatalogTableCountCheck $BatchId  73 "DimRevSumDevision" $TextFilePath_Staging2Catalog "/UCMDW/STAGING/RVR/RevSumDivision.tsv" 3 ) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 14 (RowCount_TestCase:Staging->Catalog - DimRevenueType)
         if ((CompareStagingFileCatalogTableCountCheck $BatchId  74 "DimRevenueType" $TextFilePath_Staging2Catalog "/UCMDW/STAGING/RVR/RevenueType.tsv" 4 ) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 15 (RowCount_TestCase:Staging->Catalog - FactRvR)
         if ((CompareStagingFileCatalogTableCountCheck $BatchId  74 "FactRvR" $TextFilePath_Staging2Catalog "/UCMDW/STAGING/RVR/RvrData.tsv" 13 ) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

           #region TESTCASE - 16 (RowCount_TestCase:Staging->Catalog - FactSearchResultPageViews)
         if ((CompareStagingFileCatalogTableCountCheck $BatchId  74 "FactSearchResultPageViews" $TextFilePath_Staging2Catalog "/UCMDW/STAGING/RVR/SrpvData.tsv" 11) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        
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

        #region Calling a PowerShell Script
         . "D:\PS-scripts\RvR\Staging2Catalog_PrimaryKey.ps1"
        #endregion

        #region TESTCASE - 17 (PrimaryKey_TestCase:Staging->Catalog - DimDeviceType)
         if (( CatalogTablePrimaryKeyCheck $BatchId 75 DimDeviceType $TextFilePath_Staging2Catalog_PK  DeviceTypeKey)-eq -1)
           {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion 

        #region TESTCASE - 18 (PrimaryKey_TestCase:Staging->Catalog - DimPricingLevel)
       if (( CatalogTablePrimaryKeyCheck $BatchId 76 DimPricingLevel $TextFilePath_Staging2Catalog_PK  PricingLevelKey)-eq -1)
           {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 19 (PrimaryKey_TestCase:Staging->Catalog - DimProductFamily)
       if (( CatalogTablePrimaryKeyCheck $BatchId 77 DimProductFamily $TextFilePath_Staging2Catalog_PK  ProductFamilyKey)-eq -1)
           {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 20 (PrimaryKey_TestCase:Staging->Catalog - DimRevSumCategory)
        if (( CatalogTablePrimaryKeyCheck $BatchId 78 DimRevSumCategory $TextFilePath_Staging2Catalog_PK  RevSumCategoryKey)-eq -1)
           {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 21 (PrimaryKey_TestCase:Staging->Catalog - DimRevSumDivision)
         if (( CatalogTablePrimaryKeyCheck $BatchId 79 DimRevSumDivision $TextFilePath_Staging2Catalog_PK  RevSumDivisionKey)-eq -1)
           {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 22 (PrimaryKey_TestCase:Staging->Catalog - DimRevenueType)
        if (( CatalogTablePrimaryKeyCheck $BatchId 75 DimRevenueType $TextFilePath_Staging2Catalog_PK  RevenueTypeKey)-eq -1)
           {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

         #region TESTCASE - 23 (PrimaryKey_TestCase:Staging->Catalog - FactRvR)
        if (( CatalogTablePrimaryKeyCheck $BatchId 76 FactRvR $TextFilePath_Staging2Catalog_PK  RvRFactKey)-eq -1)
           {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

         #region TESTCASE - 24 (PrimaryKey_TestCase:Staging->Catalog - FactSearchResultPageViews)
        if (( CatalogTablePrimaryKeyCheck $BatchId 77 FactSearchResultPageViews $TextFilePath_Staging2Catalog_PK  SrpvFactKey)-eq -1)
           {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

          #region TESTCASE 25 (Foreign_TestCase:Staging->Catalog - DimDeviceType)
        if(CatalogTableForeignKeyCheck $BatchId 78 "FactRvR" "DimDeviceType" "D:\PS-scripts\RvR\Staging2Catalog\ScriptStaging2Catalog_ForeignKey_Template.txt" "DeviceTypeKey" "DeviceTypeKey")
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion
        
         #region TESTCASE 26 (Foreign_TestCase:Staging->Catalog - DimPricingLevel)
        if(CatalogTableForeignKeyCheck $BatchId 78 "FactRvR" "DimPricingLevel" "D:\PS-scripts\RvR\Staging2Catalog\ScriptStaging2Catalog_ForeignKey_Template.txt" "PricingLevelKey" "PricingLevelKey")
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE 27 (Foreign_TestCase:Staging->Catalog - DimProductFamily )
        if(CatalogTableForeignKeyCheck $BatchId 78 "FactRvR" "DimProductFamily" "D:\PS-scripts\RvR\Staging2Catalog\ScriptStaging2Catalog_ForeignKey_Template.txt" "ProductFamilyKey" "ProductFamilyKey")
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE 28 (Foreign_TestCase:Staging->Catalog - DimRevSumCategory)
        if(CatalogTableForeignKeyCheck $BatchId 78 "FactRvR" "DimRevSumCategory" "D:\PS-scripts\RvR\Staging2Catalog\ScriptStaging2Catalog_ForeignKey_Template.txt" "RevSumCategoryKey" "RevSumCategoryKey")
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

         #region TESTCASE 29 (Foreign_TestCase:Staging->Catalog - DimRevSumDevision)
        if(CatalogTableForeignKeyCheck $BatchId 78 "FactRvR" "DimRevSumDevision" "D:\PS-scripts\RvR\Staging2Catalog\ScriptStaging2Catalog_ForeignKey_Template.txt" "RevSumDivisionKey" "RevSumDivisionKey")
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE 30 (Foreign_TestCase:Staging->Catalog - DimRevenueType)
        if(CatalogTableForeignKeyCheck $BatchId 78 "FactRvR" "DimRevenueType" "D:\PS-scripts\RvR\Staging2Catalog\ScriptStaging2Catalog_ForeignKey_Template.txt" "RevenueTypeKey" "RevenueTypeKey")
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE 31 (Foreign_TestCase:Staging->Catalog - DimDeviceType)
        if(CatalogTableForeignKeyCheck $BatchId 78 "FactSearchResultPageViews" "DimDeviceType" "D:\PS-scripts\RvR\Staging2Catalog\ScriptStaging2Catalog_ForeignKey_Template.txt" "DeviceTypeKey" "DeviceTypeKey")
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion
        
         #region TESTCASE 32 (Foreign_TestCase:Staging->Catalog - DimPricingLevel)
        if(CatalogTableForeignKeyCheck $BatchId 78 "FactSearchResultPageViews" "DimPricingLevel" "D:\PS-scripts\RvR\Staging2Catalog\ScriptStaging2Catalog_ForeignKey_Template.txt" "PricingLevelKey" "PricingLevelKey")
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE 33 (Foreign_TestCase:Staging->Catalog - DimProductFamily )
        if(CatalogTableForeignKeyCheck $BatchId 78 "FactSearchResultPageViews" "DimProductFamily" "D:\PS-scripts\RvR\Staging2Catalog\ScriptStaging2Catalog_ForeignKey_Template.txt" "ProductFamilyKey" "ProductFamilyKey")
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE 34 (Foreign_TestCase:Staging->Catalog - DimRevSumCategory)
        if(CatalogTableForeignKeyCheck $BatchId 78 "FactSearchResultPageViews" "DimRevSumCategory" "D:\PS-scripts\RvR\Staging2Catalog\ScriptStaging2Catalog_ForeignKey_Template.txt" "RevSumCategoryKey" "RevSumCategoryKey")
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

         #region TESTCASE 35 (Foreign_TestCase:Staging->Catalog - DimRevSumDevision)
        if(CatalogTableForeignKeyCheck $BatchId 78 "FactSearchResultPageViews" "DimRevSumDevision" "D:\PS-scripts\RvR\Staging2Catalog\ScriptStaging2Catalog_ForeignKey_Template.txt" "RevSumDivisionKey" "RevSumDivisionKey")
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE 36 (Foreign_TestCase:Staging->Catalog - DimRevenueType)
        if(CatalogTableForeignKeyCheck $BatchId 78 "FactSearchResultPageViews" "DimRevenueType" "D:\PS-scripts\RvR\Staging2Catalog\ScriptStaging2Catalog_ForeignKey_Template.txt" "RevenueTypeKey" "RevenueTypeKey")
        {
            #Something failed badly and put a log and return.
                #Put a STOP Execution in DB Error Log and return from the Master Script
                $logtimenow = Get-Date -format dd-MM-yy-HHmm
                $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion
}


#EMPTY For now
if($BusinessLogicStageCheck -eq 1)
{


}

if($SQLStagingStageCheck -eq 1 )
{
        
        #region TESTCASE - 37 (RowCount_TestCase:Catalog->AzureSQL - DimDeviceType)
        if((CompareCatalogTableSQLTableCountCheck 80 $BatchId "DimDeviceType" $TextFilePath_Catalog2AzureSql) -eq -1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion
        

        
        #region TESTCASE - 38 (RowCount_TestCase:Catalog->AzureSQL - DimPricingLevel )
        if((CompareCatalogTableSQLTableCountCheck 50 $BatchId "DimPricingLevel" $TextFilePath_Catalog2AzureSql) -eq -1)
        {
            #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 39 (RowCount_TestCase:Catalog->AzureSQL - DimProductFamily )
        if((CompareCatalogTableSQLTableCountCheck 51 $BatchId "DimProductFamily" $TextFilePath_Catalog2AzureSql) -eq -1)
        {
        #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 40 (RowCount_TestCase:Catalog->AzureSQL - DimRevSumCategory )
        if((CompareCatalogTableSQLTableCountCheck 52 $BatchId "DimRevSumCategory " $TextFilePath_Catalog2AzureSql) -eq -1)
        {
            #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 41 (RowCount_TestCase:Catalog->AzureSQL - DimRevSumDevision)
        if((CompareCatalogTableSQLTableCountCheck 53 $BatchId "DimRevSumDevision " $TextFilePath_Catalog2AzureSql) -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 42 (RowCount_TestCase:Catalog->AzureSQL - DimRevenueType)
        if((CompareCatalogTableSQLTableCountCheck 54 $BatchId "DimRevenueType" $TextFilePath_Catalog2AzureSql) -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

         #region TESTCASE - 43 (RowCount_TestCase:Catalog->AzureSQL - FactRvR)
        if((CompareCatalogTableSQLTableCountCheck 54 $BatchId "FactRvR" $TextFilePath_Catalog2AzureSql) -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

         #region TESTCASE - 44 (RowCount_TestCase:Catalog->AzureSQL - FactSearchResultPageViews)
        if((CompareCatalogTableSQLTableCountCheck 54 $BatchId "FactSearchResultPageViews" $TextFilePath_Catalog2AzureSql) -eq -1)
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
        
       
  
        #region TESTCASE 45 (RowCount_TestCase:AzureSQL->Tabular - DimDeviceType)
         if((CompareDBTableAndTabularCubeTableRowCountCheck 55  $BatchId  "DimDeviceType"  "DimDeviceType" "DeviceTypeKey") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 46 (RowCount_TestCase:AzureSQL->Tabular - DimPricingLevel)
           if((CompareDBTableAndTabularCubeTableRowCountCheck 56  $BatchId  "DimPricingLevel"  "DimPricingLevel" "PricingLevelKey") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 47 (RowCount_TestCase:AzureSQL->Tabular - DimProductFamily)
             if((CompareDBTableAndTabularCubeTableRowCountCheck 57  $BatchId  "DimProductFamily"  "DimProductFamily" "ProductFamilyKey") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 48 (RowCount_TestCase:AzureSQL->Tabular - DimRevSumCategory)
             if((CompareDBTableAndTabularCubeTableRowCountCheck 58  $BatchId  "DimRevSumCategory"  "DimRevSumCategory" "RevSumCategoryKey") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 49 (RowCount_TestCase:AzureSQL->Tabular - DimRevSumDevision)
              if((CompareDBTableAndTabularCubeTableRowCountCheck 59  $BatchId  "DimRevSumDevision"  "DimRevSumDevision" "RevSumDivisionKey") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 50 (RowCount_TestCase:AzureSQL->Tabular - DimRevenueType)
              if((CompareDBTableAndTabularCubeTableRowCountCheck 60  $BatchId  "DimRevenueType"  "DimRevenueType" "RevenueTypeKey") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

         #region TESTCASE - 51 (RowCount_TestCase:AzureSQL->Tabular - FactRvR)
              if((CompareDBTableAndTabularCubeTableRowCountCheck 60  $BatchId  "FactRvR"  "FactRvR" "RvRFactKey") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

         #region TESTCASE - 52 (RowCount_TestCase:AzureSQL->Tabular - FactSearchResultPageViews)
              if((CompareDBTableAndTabularCubeTableRowCountCheck 60  $BatchId  "FactSearchResultPageViews"  "FactSearchResultPageViews" "SrpvFactKey") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion
        

}
