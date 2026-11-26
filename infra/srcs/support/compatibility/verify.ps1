$ErrorActionPreference = 'Continue'
Set-Location -LiteralPath $PSScriptRoot
$project = 'stack-check-' + [guid]::NewGuid().ToString('N')
$composeArgs = @('compose', '-p', $project, '-f', 'compose.integration.yaml')
$log = Join-Path $PSScriptRoot "$project.log"
try {
    & docker @composeArgs up --abort-on-container-exit --exit-code-from verify 2>&1 |
        ForEach-Object { "$_" } | Tee-Object -FilePath $log
    $result = $LASTEXITCODE
    if ($result -ne 0 -or !(Select-String -LiteralPath $log -SimpleMatch 'PASS Kafka -> Spark -> Delta -> Parquet -> Feast' -Quiet)) {
        throw "Integration test failed. Log: $log"
    }
    Write-Host "PASS. Log: $log"
} finally {
    & docker @composeArgs down --volumes
}
