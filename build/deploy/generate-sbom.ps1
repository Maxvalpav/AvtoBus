# PowerShell-скрипт генерации SBOM (idea 479). Требует dotnet-sbom tool:
#   dotnet tool install --global Microsoft.SBOM.DotNetTool
#
# Использование:
#   ./generate-sbom.ps1 -ArtifactPath src/AvtoBus.Core/bin/Release/net10.0 -OutputPath build/out/sbom

param(
    [Parameter(Mandatory = $true)]
    [string]$ArtifactPath,

    [Parameter(Mandatory = $false)]
    [string]$OutputPath = "build/out/sbom"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command dotnet-sbom -ErrorAction SilentlyContinue)) {
    throw "dotnet-sbom не установлен. Выполните: dotnet tool install --global Microsoft.SBOM.DotNetTool"
}

New-Item -ItemType Directory -Path $OutputPath -Force | Out-Null

dotnet-sbom generate `
    -b $ArtifactPath `
    -o $OutputPath `
    -n "AvtoBus" `
    -v (git describe --tags --always 2>$null)

Write-Host "SBOM записан в $OutputPath"