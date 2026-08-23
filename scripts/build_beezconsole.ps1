[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
    throw "BeezConsole packaging is supported only on Windows."
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$python = Join-Path $repoRoot ".venv312\Scripts\python.exe"
$launcherSource = Join-Path $repoRoot "beez_console.py"
$iconPng = Join-Path $repoRoot "assets\beezconsole-icon.png"
$uiRoot = Join-Path $repoRoot "control-center-ui"

foreach ($required in @($python, $launcherSource, $iconPng, (Join-Path $repoRoot "main.py"))) {
    if (-not (Test-Path -LiteralPath $required -PathType Leaf)) {
        throw "Required BeezConsole build input is missing: $required"
    }
}

# This directory is exclusively owned by this script.  Do not clean generic
# project build/dist directories or the root-level executable.
$buildRoot = [IO.Path]::GetFullPath((Join-Path $repoRoot ".build\beezconsole"))
$repoPrefix = [IO.Path]::GetFullPath($repoRoot).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
if (-not $buildRoot.StartsWith($repoPrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to clean a BeezConsole build directory outside the repository."
}
if (Test-Path -LiteralPath $buildRoot) {
    Remove-Item -LiteralPath $buildRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $buildRoot -Force | Out-Null

& $python -m pip install --disable-pip-version-check -r (Join-Path $repoRoot "requirements-build.txt")
if ($LASTEXITCODE -ne 0) { throw "Unable to install BeezConsole build dependencies." }

$npmCommand = Get-Command npm.cmd -ErrorAction SilentlyContinue
if ($null -eq $npmCommand) { throw "npm.cmd is required to build control-center-ui." }
Push-Location $uiRoot
try {
    & $npmCommand.Source ci
    if ($LASTEXITCODE -ne 0) { throw "Control-center UI dependency installation failed." }
    & $npmCommand.Source run build
    if ($LASTEXITCODE -ne 0) { throw "Control-center UI build failed." }
}
finally {
    Pop-Location
}

$iconIco = Join-Path $buildRoot "beezconsole-icon.ico"
$iconConverter = @'
from pathlib import Path
import sys
from PIL import Image

source = Path(sys.argv[1])
destination = Path(sys.argv[2])
with Image.open(source) as image:
    image.convert('RGBA').save(destination, format='ICO', sizes=[(16, 16), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)])
'@
& $python -c $iconConverter $iconPng $iconIco
if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $iconIco -PathType Leaf)) {
    throw "Unable to create the Windows icon from $iconPng."
}

$distPath = Join-Path $buildRoot "dist"
& $python -m PyInstaller --noconfirm --clean --onefile --windowed --name BeezConsole --icon $iconIco `
    --distpath $distPath --workpath (Join-Path $buildRoot "work") --specpath (Join-Path $buildRoot "spec") $launcherSource
if ($LASTEXITCODE -ne 0) { throw "PyInstaller failed to build BeezConsole." }

$builtExe = Join-Path $distPath "BeezConsole.exe"
if (-not (Test-Path -LiteralPath $builtExe -PathType Leaf)) {
    throw "PyInstaller completed without producing $builtExe."
}
Copy-Item -LiteralPath $builtExe -Destination (Join-Path $repoRoot "BeezConsole.exe") -Force
Write-Host "BeezConsole built: $(Join-Path $repoRoot 'BeezConsole.exe')"
