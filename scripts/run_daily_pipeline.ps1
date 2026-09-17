[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PipelineArguments
)

$ErrorActionPreference = "Continue"
$ProjectDirectory = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $ProjectDirectory
New-Item -ItemType Directory -Path "logs" -Force | Out-Null

# Keep Instagram discovery and bulk media traffic in separate phases.
& docker compose stop media-worker *> $null

& docker compose run --rm app python -m pipeline `
    run-scheduled --skip-jobs --export @PipelineArguments
$CollectionExit = $LASTEXITCODE

& docker compose up -d media-worker
$WorkerExit = $LASTEXITCODE
if ($WorkerExit -ne 0) {
    Write-Error "Failed to start media-worker after collection."
    exit $WorkerExit
}

exit $CollectionExit
