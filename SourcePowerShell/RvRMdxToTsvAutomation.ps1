#PROCESS THE CUBE REFRESH AND CONVERTING TO TSV FILES

#$connectionString=$args[0]
#$xmlPath=$args[1]
#$tsvFilePath=$args[2]

$connectionString="Provider=MSOLAP.6;Integrated Security=SSPI;Persist Security Info=True;Data Source=OSD-FIN-NA;Initial Catalog=FY16_RVR_Search"
$xmlPath="G:\UCM_STAGING\RVR\MDXQuery.xml"
$tsvFilePath="G:\UCM_STAGING\RVR\CURRENT\"

function InvokeMdxQueryAndGenerateTsv {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)] [string]$connectionString
        ,[Parameter(Mandatory=$false)] [string]$MdxQueryName        
        ,[Parameter(Mandatory=$true)] [string]$MdxQuery
        ,[Parameter(Mandatory=$true)] [string]$csvFileFullName
    )
    Process
    {  
        $todaysDay= get-date -format "yyyyMMdd"
        [System.Reflection.Assembly]::LoadWithPartialName("Microsoft.AnalysisServices.AdomdClient") | Out-Null;
        write-host "InvokeMdxQueryAndGenerateTsv $MdxQueryName"
        $con = new-object Microsoft.AnalysisServices.AdomdClient.AdomdConnection($connectionString) 
        $con.Open() 
        $command = new-object Microsoft.AnalysisServices.AdomdClient.AdomdCommand($MdxQuery, $con) 
        $dataAdapter = new-object Microsoft.AnalysisServices.AdomdClient.AdomdDataAdapter($command) 
        $ds = new-object System.Data.DataSet 
        $dataAdapter.Fill($ds) 
        $con.Close();

        $ds.Tables[0] | Export-Csv -path $csvFileFullName$MdxQueryName"Temp.tsv" -Delimiter `t  -NoTypeInformation
        Get-Content $csvFileFullName$MdxQueryName"Temp.tsv" | select -Skip 1 | Set-Content $csvFileFullName$MdxQueryName$todaysDay".tsv"

    }
};

[xml]$xmlDocument = Get-Content -Path $xmlPath
foreach($data in $xmlDocument.MDXQueries.QueryName) 
{
    InvokeMdxQueryAndGenerateTsv -connectionString $connectionString -MdxQueryName $data.id -MdxQuery $data.Query.Replace('@@@','&') -CsvFileFullName $tsvFilePath 
}

$tempFile=get-childitem $tsvFilePath"\*.*" -Include *.tsv
foreach($file in $tempFile)
{
 if($file.Name.Contains("Temp."))
    {
       Remove-Item $file.FullName
    }
}