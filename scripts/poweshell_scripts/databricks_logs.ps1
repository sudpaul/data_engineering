# Requires the AzTable module.
############################
# Databricks configuration
$DATABRICKS_HOST = "https://northeurope.azuredatabricks.net"
$DATABRICKS_TOKEN = "dapi00000000000000000000000000000000"
$latencyMs = 10 * 60 * 1000
############################

############################
# Storage table configuration
$storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=aaaaaaaaaaaa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa==;EndpointSuffix=core.windows.net"
$watermarkTableName = "watermark"
$idrepTableName = "idempotentRepository"
############################

############################
# Log Analytics configuration
$customerId = "00000000-0000-0000-0000-000000000000"
$sharedKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa=="
$logType = "ClusterEvent"
$timeStampField = "timestamp"
############################

$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = 'tls12'

Function Build-Signature ($customerId, $sharedKey, $date, $contentLength, $method, $contentType, $resource) {
    $xHeaders = "x-ms-date:" + $date
    $stringToHash = $method + "`n" + $contentLength + "`n" + $contentType + "`n" + $xHeaders + "`n" + $resource
    $bytesToHash = [Text.Encoding]::UTF8.GetBytes($stringToHash)
    $keyBytes = [Convert]::FromBase64String($sharedKey)
    $sha256 = New-Object System.Security.Cryptography.HMACSHA256
    $sha256.Key = $keyBytes
    $calculatedHash = $sha256.ComputeHash($bytesToHash)
    $encodedHash = [Convert]::ToBase64String($calculatedHash)
    return 'SharedKey {0}:{1}' -f $customerId, $encodedHash
}

Function Post-LogAnalyticsData($customerId, $sharedKey, $body, $logType) {
    $method = "POST"
    $contentType = "application/json"
    $resource = "/api/logs"
    $rfc1123date = [DateTime]::UtcNow.ToString("r")
    $contentLength = $body.Length
    $signature = Build-Signature -customerId $customerId -sharedKey $sharedKey -date $rfc1123date -contentLength $contentLength -method $method -contentType $contentType -resource $resource
    $uri = "https://" + $customerId + ".ods.opinsights.azure.com" + $resource + "?api-version=2016-04-01"
    $headers = @{
        "Authorization" = $signature
        "Log-Type" = $logType
        "x-ms-date" = $rfc1123date
        "time-generated-field" = $timeStampField
    }
    $response = Invoke-WebRequest -Uri $uri -Method $method -ContentType $contentType -Headers $headers -Body $body -UseBasicParsing
    return $response.StatusCode
}

$password = $DATABRICKS_TOKEN | ConvertTo-SecureString -asPlainText -Force
$credential = New-Object System.Management.Automation.PSCredential("token", $password)
$ctx = New-AzStorageContext -ConnectionString $storageConnectionString
$partitionKey = "partitionKey"
$watermark_table = (Get-AzStorageTable –Name $watermarkTableName -Context $ctx).CloudTable
$idrep_table = (Get-AzStorageTable –Name $idrepTableName -Context $ctx).CloudTable

$watermark = Get-AzTableRow -Table $watermark_table -PartitionKey $partitionKey -RowKey "watermark"
if (!$watermark) {
    Add-AzTableRow -Table $watermark_table -PartitionKey $partitionKey -RowKey "watermark" -property @{"start_time"=0}
    $watermark = Get-AzTableRow -Table $watermark_table -PartitionKey $partitionKey -RowKey "watermark"
}

Write-Host "Starting at time watermark $($watermark.start_time)"
$end_time = int64 - (get-date "1/1/1970")).TotalMilliseconds

$clusters = Invoke-RestMethod -Uri "$DATABRICKS_HOST/api/2.0/clusters/list" -Method GET -Headers @{Authorization = "Bearer $DATABRICKS_TOKEN"}
$clusters.clusters.ForEach({
    Write-Host $_.cluster_id
    $next_page = @{
        "cluster_id" = $_.cluster_id
        "order" = "ASC"
        "start_time" = ($watermark.start_time - $latencyMs)
        "end_time" = $end_time
        "limit" = 100
    }
    while ($next_page) {
        $query = ConvertTo-Json $next_page -Depth 100
        $ret = Invoke-RestMethod -Uri "$DATABRICKS_HOST/api/2.0/clusters/events" -Method POST -Body $query -Headers @{Authorization = "Bearer $DATABRICKS_TOKEN"}
        $next_page = $ret.next_page
        Write-Host "Got $($ret.events.Count) events for cluster '$($_.cluster_id)'"
        $ret.events.ForEach({
            $eventId = $_.cluster_id + "/" + $_.timestamp + "/" + $_.type
            $rowKey = [uri]::EscapeDataString($eventId)
            $seenRow = Get-AzTableRow -Table $idrep_table -PartitionKey $partitionKey -RowKey $rowKey
            if ($seenRow) {
                Write-Host "Ignoring already seen event"
                return
            }
            $epoch = New-Object -Type DateTime -ArgumentList 1970, 1, 1, 0, 0, 0, 0
            $eventTime = $epoch.AddMilliseconds($_.timestamp)
            $_.timestamp = Get-Date -Date $eventTime -Format "yyyy-MM-ddThh:mm:ssZ"
            $json = ConvertTo-Json $_ -Depth 100
            Post-LogAnalyticsData -customerId $customerId -sharedKey $sharedKey -body ([System.Text.Encoding]::UTF8.GetBytes($json)) -logType $logType -TimeStampField $timeStampField
            Add-AzTableRow -Table $idrep_table -PartitionKey $partitionKey -RowKey $rowKey
        })
    }
})

Write-Host "Updating time watermark to $end_time"
$watermark.start_time = $end_time
Update-AzTableRow -Table $watermark_table -entity $watermark