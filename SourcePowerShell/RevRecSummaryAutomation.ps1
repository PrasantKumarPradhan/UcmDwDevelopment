#Region RevenueRecSummary copy from shared path to CURRENT Folder
<#$temporaryFilePath=$args[0]
$revenueRecSummaryFile=$args[1]
$adlsPath=$args[2]
$subscriptionId = $args[3]
$dataLakeStoreName = $args[4]#>

while ($true){
    start-sleep -seconds 1
    $dateTimeCheck=Get-Date
    Write-Host $dateTimeCheck -foregroundcolor Green
    if((get-date -format "dd-MMM-yyyy HH:mm") -eq (get-date "08:10:00 AM" -format "dd-MMM-yyyy HH:mm"))
    {
        Write-Host "Executing the process"-foregroundcolor DarkRed
        $temporaryFilePath="G:\UCM_STAGING\REVENUERECSUMMARY\CURRENT\"
        $revenueRecSummaryFile="\\kpibillingcubeshare\RevenueRecSummaryFeedShare\"
        $adlsPath="/UCMDW/STAGING/REVENUERECSUMMARY/"
        $subscriptionId = "4167be12-8ddc-4e8f-9b2b-d77b90bb4c22"
        $dataLakeStoreName = "ucmdatalakestorecorp"

        #Login-AzureRMAccount
        #Select a subscription 
        Set-AzureRmContext -SubscriptionId $subscriptionId   
    
        function returnDate($date){ 
            $Month= $date.Month
            $day=$date.Day
            if($Month -lt 10)    
            {$Month="0"+$Month} 
            if($day -lt 10)
            {$day="0"+$day}      
            $dateTime=$date.Year.ToString()+$Month.ToString()+$day.ToString()
            return $dateTime 
        }

        #Copy past 5 day file revenueRecSummary file to current folder.
        $allRevRecFileName=get-childitem -Name $revenueRecSummaryFile
   
        foreach($fileName in $allRevRecFileName)
        { 
            $day1=returnDate -date (get-date).AddDays(-1)
            $day2=returnDate -date (get-date).AddDays(-2)
            $day3=returnDate -date (get-date).AddDays(-3)
            $day4=returnDate -date (get-date).AddDays(-4)
            $day5=returnDate -date (get-date).AddDays(-5)
            $days=$day1,$day2,$day3,$day4,$day5
            foreach($eachday in $days)
            {
              if($fileName.Contains($eachday))
              {     
                $revenueRecSummaryActual=$revenueRecSummaryFile+$fileName
                if((Test-Path $temporaryFilePath) -eq $False){
                    New-Item -ItemType File -Path $temporaryFilePath -Force
                }     
                If ((Test-Path $revenueRecSummaryActual) -eq $true) {
                   Get-ChildItem -Path $temporaryFilePath -File -Recurse | `
                    foreach {
                        if(($_.Name.Contains($eachday)))
                        {    
                            $_.Delete()
                            Write-Host "File Deleted "$_.Name -foregroundcolor DarkRed
                        }
                    }
                    Write-Host "Current File Name " $revenueRecSummaryActual -foregroundcolor DarkYellow
                    Copy-Item -Path $revenueRecSummaryActual -Destination $temporaryFilePath -Force;  
                }
              }   
            }       
        }
        Get-ChildItem -Path $temporaryFilePath|rename-item -NewName {$_.name -replace ".csv",".tsv"} -Force

        #Compare file between current folder and ADLS and delete all the matching file from current folder
        Get-AzureRmDataLakeStoreChildItem `
            -AccountName $dataLakeStoreName `
            -Path $adlsPath | ForEach-Object {
                if($_.Type -eq "File")
                {
                    $adlsFileName=$_.PathSuffix
                    Write-Host "ADLS File Name "$_.PathSuffix -foregroundcolor Gray
                    Get-ChildItem -Path $temporaryFilePath -File -Recurse | `
                    foreach { 
                        Write-Host "Current File Name "$_.Name -foregroundcolor DarkYellow
                        if($adlsFileName -eq $_.Name)
                        {    
                            $_.Delete()
                            Write-Host "File Deleted "$_.Name -foregroundcolor DarkRed
                        }
                    }
                }
            }
    }
}



# FullLoad script to copy all file
 <#foreach($fileName in $allFileName)
    { 
        $revenueRecSummaryActual=$revenueRecSummaryFile+$fileName 

        if((Test-Path $currentRootPath) -eq $False){
            New-Item -ItemType File -Path $currentRootPath -Force
        }     
        If ((Test-Path $revenueRecSummaryActual) -eq $true) {
            Copy-Item -Path $revenueRecSummaryActual -Destination $currentRootPath -Force;  
        }
        else{ 
             Write-Host "File Not Found " -foregroundcolor Red
        }
    }#>

