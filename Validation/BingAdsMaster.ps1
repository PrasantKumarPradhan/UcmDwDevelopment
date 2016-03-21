#Login-AzureRmAccount
#Common Variables 
#$UcmSQLDatawarehouseDBCnn = "Server=ldtevtephi.database.windows.net;Database=UcmStagingAreaSubscriber;Integrated Security = False;User ID = bactest; Password = Microsoft!1;"
$UcmSQLDatawarehouseDBCnn = "Server=ucmdatawarehousesqldev.database.windows.net;Database=UcmSqlDataWarehouse;Integrated Security = False;User ID = ucmdwsuper@ucmdatawarehousesqldev; Password = Microsoft!1;"
$UcmValidationDBCnn = "Server=ucmdatawarehousesqldev.database.windows.net;Database=UcmDWValidation;User ID=ucmdwsuper@ucmdatawarehousesqldev; Password = Microsoft!1;"
$TextFilePath_Staging2Catalog_PK = "D:\UCM_Validation\Staging2Catalog\PKscript.txt"
$TextFilePath_Source2Staging="D:\PS-scripts\BINGADS\Source2Staging\ScriptSource2StagingTemplate.txt"
$TextFilePath_Staging2Catalog="D:\PS-scripts\BINGADS\Staging2Catalog\ScriptStaging2Catalog_DimRowCount_Template.txt"
$TextFilePath_Catalog2AzureSql="D:\PS-scripts\BingAds\Catalog2AzureSQL\RowCountScript.txt"
#$TabularCubeCnn="Provider=MSOLAP;Data Source=104.210.4.250;Initial Catalog=UcmCube;"
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
    $DBconn.ConnectionString = $UcmValidationDBCnn
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



$ScriptPath = Split-Path $MyInvocation.InvocationName
.$ScriptPath\CommonFunctions.ps1
         
if($SourcesStageCheck -eq 1)
{
        
        #region TESTCASE - 1 (RowCount_TestCase:Source->Staging - Account)
        if ((CompareSourceFileStagingFileCountCheck 2 $BatchId  "ACCOUNT" 45 $TextFilePath_Source2Staging  "/UCMDW/STAGING/BINGADS/Account.tsv" ) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion 

        #region TESTCASE - 2 (RowCount_TestCase:Source->Staging - AgencyCustomerAccountRelation)
         if ((CompareSourceFileStagingFileCountCheck 2 $BatchId  "AgencyCustomerAccountRelation" 7 $TextFilePath_Source2Staging  "/UCMDW/STAGING/BINGADS/AgencyCustomerAccountRelation.tsv" ) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 3 (RowCount_TestCase:Source->Staging - Coupon)
          
        if ((CompareSourceFileStagingFileCountCheck 2 $BatchId  "Coupon" 4 $TextFilePath_Source2Staging  "/UCMDW/STAGING/BINGADS/Coupon.tsv" ) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 4 (RowCount_TestCase:Source->Staging - CouponBatch)
                if ((CompareSourceFileStagingFileCountCheck 2 $BatchId  "CouponBatch" 11 $TextFilePath_Source2Staging  "/UCMDW/STAGING/BINGADS/CouponBatch.tsv" ) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 5 (RowCount_TestCase:Source->Staging - CouponRedemption)
         if ((CompareSourceFileStagingFileCountCheck 2 $BatchId  "CouponRedemption" 10 $TextFilePath_Source2Staging  "/UCMDW/STAGING/BINGADS/CouponRedemption.tsv" ) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 6(RowCount_TestCase:Source->Staging - Customer)
         
        if ((CompareSourceFileStagingFileCountCheck 2 $BatchId  "Customer" 28 $TextFilePath_Source2Staging  "/UCMDW/STAGING/BINGADS/Customer.tsv" ) -eq - 1)
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

          #region TESTCASE - 7 (RowCount_TestCase:Staging->Catalog - DimAccount)
	 if ((CompareStagingFileCatalogTableCountCheck $BatchId  37 "DimACCOUNT" $TextFilePath_Staging2Catalog "/UCMDW/STAGING/BINGADS/Account.tsv" 45) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion 

        #region TESTCASE - 8(RowCount_TestCase:Staging->Catalog - DimAgencyCustomerAccountRelation)
         if ((CompareStagingFileCatalogTableCountCheck $BatchId  38 "DimAgencyCustomerAccountRelation" $TextFilePath_Staging2Catalog "/UCMDW/STAGING/BINGADS/AgencyCustomerAccountRelation.tsv" 7) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 9(RowCount_TestCase:Staging->Catalog - DimCoupon)
         if ((CompareStagingFileCatalogTableCountCheck $BatchId  39 "DimCoupon" $TextFilePath_Staging2Catalog "/UCMDW/STAGING/BINGADS/Coupon.tsv" 4) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 10(RowCount_TestCase:Staging->Catalog - DimCouponBatch)
         if ((CompareStagingFileCatalogTableCountCheck $BatchId  40 "DimCouponBatch" $TextFilePath_Staging2Catalog "/UCMDW/STAGING/BINGADS/CouponBatch.tsv" 11) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 11(RowCount_TestCase:Staging->Catalog - DimCouponRedemption)
         if ((CompareStagingFileCatalogTableCountCheck $BatchId  41 "DimCouponRedemption" $TextFilePath_Staging2Catalog "/UCMDW/STAGING/BINGADS/CouponRedemption.tsv" 10) -eq - 1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 12(RowCount_TestCase:Staging->Catalog - DimCustomer)
         if ((CompareStagingFileCatalogTableCountCheck $BatchId  42 "DimCustomer" $TextFilePath_Staging2Catalog "/UCMDW/STAGING/BINGADS/Customer.tsv" 28) -eq - 1)
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


        #region TESTCASE - 13 (PrimaryKey_TestCase:Staging->Catalog - DimAccount)
         if (( CatalogTablePrimaryKeyCheck $StageId $BatchId $TextFilePath_Staging2Catalog_PK $UcmValidationDBCnn DimAccount AccountKey)-eq -1)
           {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion 

        #region TESTCASE - 14(PrimaryKey_TestCase:Staging->Catalog - DimAgencyCustomerAccountRelation)
        if (( CatalogTablePrimaryKeyCheck $StageId $BatchId $TextFilePath_Staging2Catalog_PK $UcmValidationDBCnn DimAgencyCustomerAccountRelation AgencyCustomerAccountRelationKey)-eq -1)
           {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 15(PrimaryKey_TestCase:Staging->Catalog - DimCoupon)
        if (( CatalogTablePrimaryKeyCheck $StageId $BatchId $TextFilePath_Staging2Catalog_PK $UcmValidationDBCnn DimCoupon CouponKey)-eq -1)
           {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 16(PrimaryKey_TestCase:Staging->Catalog - DimCouponBatch)
        if (( CatalogTablePrimaryKeyCheck $StageId $BatchId $TextFilePath_Staging2Catalog_PK $UcmValidationDBCnn DimCouponBatch CouponBatchKey)-eq -1)
           {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 17(PrimaryKey_TestCase:Staging->Catalog - DimCouponRedemption)
         if ((CatalogTablePrimaryKeyCheck $StageId $BatchId $TextFilePath_Staging2Catalog_PK $UcmValidationDBCnn DimCouponRedemption CouponRedemptionKey)-eq -1)
           {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 18(PrimaryKey_TestCase:Staging->Catalog - DimCustomer)
        if (( CatalogTablePrimaryKeyCheck $StageId $BatchId $TextFilePath_Staging2Catalog_PK $UcmValidationDBCnn DimCustomer CustomerKey)-eq -1)
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
        
        #region TESTCASE - 19 (RowCount_TestCase:Catalog->AzureSQL - DimAccount)
        $ScriptPath = Split-Path $MyInvocation.InvocationName
        .$ScriptPath\Catalog2AzureSql_RowcountCheck.ps1
        CompareCatalogTableSQLTableCountCheck 49 $BatchId 'DimAccount' $TextFilePath_Catalog2AzureSql
        #Write-Host $ReturnValue
        
        if((CompareCatalogTableSQLTableCountCheck 49 $BatchId "DimAccount" $TextFilePath_Catalog2AzureSql) -eq -1)
        {
            #Something failed badly and put a log and return.
            #Put a STOP Execution in DB Error Log and return from the Master Script
            $logtimenow = Get-Date -format dd-MM-yy-HHmm
            $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
            Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion
        

        
        #region TESTCASE - 20 (RowCount_TestCase:Catalog->AzureSQL - DimAgencyCustomerAccountRelation)
        $ScriptPath = Split-Path $MyInvocation.InvocationName
        .$ScriptPath\Catalog2AzureSql_RowcountCheck.ps1
        CompareCatalogTableSQLTableCountCheck 50 $BatchId "DimAgencyCustomerAccountRelation" $TextFilePath_Catalog2AzureSql
        if((CompareCatalogTableSQLTableCountCheck 50 $BatchId "DimAgencyCustomerAccountRelation" $TextFilePath_Catalog2AzureSql) -eq -1)
        {
            #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 21 (RowCount_TestCase:Catalog->AzureSQL - DimCoupon)
        $ScriptPath = Split-Path $MyInvocation.InvocationName
        .$ScriptPath\Catalog2AzureSql_RowcountCheck.ps1
        CompareCatalogTableSQLTableCountCheck 51 $BatchId "DimCoupon" $TextFilePath_Catalog2AzureSql
        if((CompareCatalogTableSQLTableCountCheck 51 $BatchId "DimCoupon" $TextFilePath_Catalog2AzureSql) -eq -1)
        {
        #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 22 (RowCount_TestCase:Catalog->AzureSQL - DimCouponBatch)
        $ScriptPath = Split-Path $MyInvocation.InvocationName
        .$ScriptPath\Catalog2AzureSql_RowcountCheck.ps1
        CompareCatalogTableSQLTableCountCheck 52 $BatchId "DimCouponBatch" $TextFilePath_Catalog2AzureSql
        if((CompareCatalogTableSQLTableCountCheck 52 $BatchId "DimCouponBatch" $TextFilePath_Catalog2AzureSql) -eq -1)
        {
            #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 23 (RowCount_TestCase:Catalog->AzureSQL - DimCouponRedemption)
        $ScriptPath = Split-Path $MyInvocation.InvocationName
        .$ScriptPath\Catalog2AzureSql_RowcountCheck.ps1
        CompareCatalogTableSQLTableCountCheck 53 $BatchId "DimCouponRedemption" $TextFilePath_Catalog2AzureSql
        if((CompareCatalogTableSQLTableCountCheck 53 $BatchId "DimCouponRedemption" $TextFilePath_Catalog2AzureSql) -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 24 (RowCount_TestCase:Catalog->AzureSQL - DimCustomer)
        $ScriptPath = Split-Path $MyInvocation.InvocationName
        .$ScriptPath\Catalog2AzureSql_RowcountCheck.ps1
        CompareCatalogTableSQLTableCountCheck  54 $BatchId "DimCustomer" $TextFilePath_Catalog2AzureSql
        if((CompareCatalogTableSQLTableCountCheck 54 $BatchId "DimCustomer" $TextFilePath_Catalog2AzureSql) -eq -1)
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
        
       
  
        #region TESTCASE 25 (RowCount_TestCase:AzureSQL->Tabular - DimAccount)
         if((CompareDBTableAndTabularCubeTableRowCountCheck 55  $BatchId  "DimAccount"  "DimAccount" "AccountKey") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 26(RowCount_TestCase:AzureSQL->Tabular - DimAgencyCustomerAccountRelation)
           if((CompareDBTableAndTabularCubeTableRowCountCheck 56  $BatchId  "DimAgencyCustomerAccountRelation"  "DimAgencyCustomerAccountRelation" "AgencyKey") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 27(RowCount_TestCase:AzureSQL->Tabular - DimCoupon)
             if((CompareDBTableAndTabularCubeTableRowCountCheck 57  $BatchId  "DimCoupon"  "DimCoupon" "CouponKey") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 28(RowCount_TestCase:AzureSQL->Tabular - DimCouponBatch)
             if((CompareDBTableAndTabularCubeTableRowCountCheck 58  $BatchId  "DimCouponBatch"  "DimCouponBatch" "CouponBatchKey") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 29(RowCount_TestCase:AzureSQL->Tabular - DimCouponRedemption)
              if((CompareDBTableAndTabularCubeTableRowCountCheck 59  $BatchId  "DimCouponRedemption"  "DimCouponRedemption" "CouponRedemptionKey") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion

        #region TESTCASE - 30(RowCount_TestCase:AzureSQL->Tabular - DimCustomer)
              if((CompareDBTableAndTabularCubeTableRowCountCheck 60  $BatchId  "DimCustomer"  "DimCustomer" "CustomerKey") -eq -1)
        {
                #Something failed badly and put a log and return.
                 #Put a STOP Execution in DB Error Log and return from the Master Script
                 $logtimenow = Get-Date -format dd-MM-yy-HHmm
                 $logFile = 'D:\UCM_Validation' + '\' + "ErrorLog_"+"$logtimenow"+".txt"
                 Verbose -LogFile $logFile -Message "Unexpected Error occurred"
        }
        #endregion


        

}