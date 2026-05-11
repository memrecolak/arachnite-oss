# Compile the Jason baseline into baselines/jason/build/
# Requires:
#   - JDK 11+ (javac on PATH)
#   - Jason 3.x installed; $env:JASON_HOME must point at the unpacked
#     jason-bin distribution (the directory containing libs/jason.jar).

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

if (-not $env:JASON_HOME) {
    Write-Error "JASON_HOME is not set. Download Jason 3.x from https://github.com/jason-lang/jason/releases, unzip, then `$env:JASON_HOME = '<path>'`."
}

$jasonJar = Join-Path $env:JASON_HOME "libs\jason.jar"
if (-not (Test-Path $jasonJar)) {
    Write-Error "Cannot find $jasonJar — JASON_HOME must point to the unpacked jason-bin distribution."
}

if (-not (Test-Path "build")) { New-Item -ItemType Directory -Path "build" | Out-Null }

$cp = Join-Path $env:JASON_HOME "libs\*"
& javac -cp $cp -d build ArmState.java RobotArmEnv.java JasonBench.java
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Output "Built. Run via:"
Write-Output "  java -cp `"build;$cp`" JasonBench --out latencies.jsonl --cycles 10000 --warmup 1000"
