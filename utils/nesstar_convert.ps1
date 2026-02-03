param(
  [string]$NesstarExe,
  [string]$InputStudy,
  [string]$OutputDir,
  [int]$StepTimeoutSec = 900,
  [string]$AutoItExe = "",
  [string]$AutoItScript = "",
  [int]$MaxAttempts = 1,
  [string]$JobId = "",
  [string]$ExportFromDir = "",
  [bool]$MoveFiles = $false,
  [string]$Schema = ""
)

function EmitLog([string]$msg) { Write-Output "LOG $msg" }
function EmitStage([string]$stg) { Write-Output "STAGE $stg" }
function CloseNesstarProcess($proc) {
  if (-not $proc) { return }
  try {
    if (-not $proc.HasExited) {
      $null = $proc.CloseMainWindow()
      Start-Sleep -Seconds 2
    }
  } catch { }
  try {
    if (-not $proc.HasExited) {
      Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    }
  } catch { }
}

$NesstarExe = if ($null -ne $NesstarExe) { $NesstarExe.Trim() } else { "" }
$InputStudy = if ($null -ne $InputStudy) { $InputStudy.Trim() } else { "" }
$OutputDir = if ($null -ne $OutputDir) { $OutputDir.Trim() } else { "" }
$AutoItExe = if ($null -ne $AutoItExe) { $AutoItExe.Trim() } else { "" }
$AutoItScript = if ($null -ne $AutoItScript) { $AutoItScript.Trim() } else { "" }
$JobId = if ($null -ne $JobId) { $JobId.Trim() } else { "" }
$ExportFromDir = if ($null -ne $ExportFromDir) { $ExportFromDir.Trim() } else { "" }

EmitLog "SCRIPT_STARTED"
EmitLog "NesstarExe=$NesstarExe"
EmitLog "InputStudy=$InputStudy"
EmitLog "OutputDir=$OutputDir"
EmitLog "JobId=$JobId"
EmitLog "ExportFromDir=$ExportFromDir"

if (!(Test-Path -LiteralPath $NesstarExe)) { Write-Error "Missing NesstarExe"; exit 1 }
if (!(Test-Path -LiteralPath $InputStudy)) { Write-Error "Missing InputStudy"; exit 2 }

$projectRoot = Split-Path -Parent $PSScriptRoot
$uploadsRoot = Join-Path $projectRoot "uploads"
if (!(Test-Path -LiteralPath $uploadsRoot)) { New-Item -ItemType Directory -Path $uploadsRoot | Out-Null }

$destDir = $OutputDir
if (-not $destDir) { $destDir = $uploadsRoot }
if (!(Test-Path -LiteralPath $destDir)) { New-Item -ItemType Directory -Path $destDir | Out-Null }
EmitLog "OutputDirResolved=$destDir"
try {
  $probe = Join-Path $destDir ".write_probe.tmp"
  Set-Content -LiteralPath $probe -Value "probe" -Encoding Ascii -Force
  Remove-Item -LiteralPath $probe -Force -ErrorAction SilentlyContinue
} catch {
  Write-Error "Cannot write to OutputDir (permissions issue): $destDir"
  exit 3
}

if (-not $ExportFromDir) { $ExportFromDir = $destDir }
if (!(Test-Path -LiteralPath $ExportFromDir)) { New-Item -ItemType Directory -Path $ExportFromDir | Out-Null }
EmitLog "ExportFromDirResolved=$ExportFromDir"

EmitStage "CONVERTING_WITH_NESSTAR"

$titleHint = $env:NESSTAR_WINDOW_TITLE_HINT
if ($titleHint) { $titleHint = $titleHint.Trim() }
if (-not $titleHint) { $titleHint = "Nesstar Explorer" }

$autoCandidates = @()
if ($AutoItExe) { $autoCandidates += $AutoItExe }
$envAutoItExe = $env:NESSTAR_AUTOIT_EXE
if ($envAutoItExe) { $autoCandidates += $envAutoItExe.Trim() }
$autoCandidates += @(
  "C:\Program Files (x86)\AutoIt3\AutoIt3.exe",
  "C:\Program Files\AutoIt3\AutoIt3.exe"
)
$autoItExeResolved = ($autoCandidates | Where-Object { $_ -and (Test-Path -LiteralPath $_) } | Select-Object -First 1)
if (-not $autoItExeResolved) { Write-Error "AutoIt executable not found"; exit 4 }

$scriptCandidates = @()
if ($AutoItScript) { $scriptCandidates += $AutoItScript }
$envAutoItScript = $env:NESSTAR_AUTOIT_SCRIPT
if ($envAutoItScript) { $scriptCandidates += $envAutoItScript.Trim() }
$scriptCandidates += @((Join-Path $PSScriptRoot "nesstar_export.au3"))
$autoItScriptResolved = ($scriptCandidates | Where-Object { $_ -and (Test-Path -LiteralPath $_) } | Select-Object -First 1)
if (-not $autoItScriptResolved) { Write-Error "AutoIt script not found"; exit 5 }

EmitLog "AutoItExe=$autoItExeResolved"
EmitLog "AutoItScript=$autoItScriptResolved"

$env:NESSTAR_EXPORT_FORMAT = "SAV"

$attempt = 1
while ($attempt -le $MaxAttempts) {
  EmitLog "Attempt=$attempt MaxAttempts=$MaxAttempts"
  $exportStartUtc = (Get-Date).ToUniversalTime()

  $p = $null
  try {
    $p = Start-Process -FilePath $NesstarExe -WorkingDirectory $ExportFromDir -ArgumentList @("`"$InputStudy`"") -PassThru
  } catch {
    Write-Error "Failed to launch Nesstar Explorer: $($_.Exception.Message)"
    exit 10
  }

  EmitLog "NESSTAR_LAUNCHED pid=$($p.Id)"
  Start-Sleep -Seconds 2

  $code = 20
  try {
    & $autoItExeResolved /ErrorStdOut $autoItScriptResolved $InputStudy $ExportFromDir $titleHint $Schema 2>&1 | ForEach-Object { Write-Output $_ }
    $code = $LASTEXITCODE
  } catch {
    Write-Error "AutoIt invocation failed: $($_.Exception.Message)"
    $code = 20
  }

  if ($code -eq 0) {
    CloseNesstarProcess $p
    break
  }

  Write-Error "AutoIt export failed (exit=$code)"
  CloseNesstarProcess $p
  if ($code -eq 20 -and $attempt -lt $MaxAttempts) {
    Start-Sleep -Seconds 2
    $attempt += 1
    continue
  }
  exit $code
}

EmitStage "VALIDATING_EXPORTED_FILES"

$minDataBytes = 1024

$t = Get-Date
$stableSince = Get-Date
$lastSig = ""
$picked = @()
while (((Get-Date) - $t).TotalSeconds -lt $StepTimeoutSec) {
  $candidates = @(Get-ChildItem -LiteralPath $ExportFromDir -Recurse -File -ErrorAction SilentlyContinue | Where-Object {
      $_.LastWriteTimeUtc -ge $exportStartUtc.AddSeconds(-2) -and @(".sav", ".csv", ".xml") -contains $_.Extension.ToLowerInvariant()
    })

  $dataCandidates = @($candidates | Where-Object { @(".sav", ".csv") -contains $_.Extension.ToLowerInvariant() })
  $ddiCandidates = @($candidates | Where-Object { $_.Extension.ToLowerInvariant() -eq ".xml" -and $_.Name.ToLowerInvariant().Contains("ddi") })

  $allOk = $true
  foreach ($f in $dataCandidates) {
    if ($f.Length -lt $minDataBytes) { $allOk = $false; break }
  }

  $sigParts = @()
  foreach ($f in $candidates | Sort-Object FullName) {
    $sigParts += "$($f.Name)|$($f.Length)|$($f.LastWriteTimeUtc.Ticks)"
  }
  $sig = ($sigParts -join ";")

  if ($sig -ne $lastSig) {
    $lastSig = $sig
    $stableSince = Get-Date
  }

  if ($dataCandidates.Count -gt 0 -and $allOk) {
    if (((Get-Date) - $stableSince).TotalSeconds -ge 5) {
      $picked = @($dataCandidates + $ddiCandidates)
      break
    }
  }

  Start-Sleep -Milliseconds 750
}

if (-not $picked -or $picked.Count -le 0) {
  Write-Error "Nesstar export did not generate dataset"
  exit 21
}

foreach ($f in $picked) {
  $dst = Join-Path $destDir $f.Name
  $srcResolved = ""
  $dstResolved = ""
  try { $srcResolved = (Resolve-Path -LiteralPath $f.FullName).Path } catch { $srcResolved = $f.FullName }
  try { $dstResolved = (Resolve-Path -LiteralPath $dst).Path } catch { $dstResolved = $dst }
  if ($srcResolved.ToLowerInvariant() -eq $dstResolved.ToLowerInvariant()) {
    continue
  }
  try {
    if ($MoveFiles) {
      Move-Item -LiteralPath $f.FullName -Destination $dst -Force
    } else {
      Copy-Item -LiteralPath $f.FullName -Destination $dst -Force
    }
  } catch {
    Write-Error "Failed to copy exported file: $($f.FullName) -> $dst"
    exit 21
  }
}

$minSavBytes = 1048577
$copiedSav = @()
foreach ($f in $picked | Where-Object { $_.Extension.ToLowerInvariant() -eq ".sav" }) {
  $dst = Join-Path $destDir $f.Name
  if (Test-Path -LiteralPath $dst) {
    $copiedSav += Get-Item -LiteralPath $dst
  }
}

$savCount = ($copiedSav | Measure-Object).Count
EmitLog "ExportedSavCount=$savCount"

$allDataOk = $savCount -gt 0
foreach ($f in $copiedSav) {
  if ($f.Length -lt $minSavBytes) { $allDataOk = $false; break }
}

if ($savCount -gt 0) {
  $largest = $copiedSav | Sort-Object Length -Descending | Select-Object -First 1
  if ($largest) { EmitLog "LargestData=$($largest.Name) bytes=$($largest.Length)" }
}

if ($allDataOk) {
  Write-Output "NESSTAR_REAL_EXPORT_SUCCESS"
  exit 0
}

if ($savCount -le 0) { Write-Error "Nesstar export did not generate dataset"; CloseNesstarProcess $p; exit 21 }
if (-not $allDataOk) { Write-Error "Nesstar export did not generate dataset"; CloseNesstarProcess $p; exit 21 }

Write-Error "Nesstar export validation failed"
exit 20
