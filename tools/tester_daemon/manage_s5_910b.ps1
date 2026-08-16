param(
    [ValidateSet("start", "stop", "restart", "status", "ensure-resident")]
    [string]$Action = "status",
    [double]$IntervalSeconds = 1,
    [double]$WaitSeconds = 30,
    [switch]$Force
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = (Resolve-Path (Join-Path $ScriptDir "..\..")).Path
$Python = (Get-Command python).Source
$Launcher = Join-Path $Root "tools\tester_daemon\launch_s5_910b.py"

$commandArgs = @(
    $Launcher,
    $Action,
    "--interval-seconds", "$IntervalSeconds",
    "--wait-seconds", "$WaitSeconds"
)
if ($Force) {
    $commandArgs += "--force"
}

Push-Location $Root
try {
    & $Python @commandArgs
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
