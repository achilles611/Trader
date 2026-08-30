[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)] [string] $ArchivePath,
    [Parameter(Mandatory = $false)] [string] $ToolchainRoot,
    [switch] $AllowNetworkRead
)

$ErrorActionPreference = 'Stop'
if ($AllowNetworkRead -and [string]::IsNullOrWhiteSpace($ArchivePath)) {
    throw 'Network provisioning is deliberately not implicit. Download the documented official v1.8.1 archive, verify its hash, then supply -ArchivePath.'
}
if ([string]::IsNullOrWhiteSpace($ArchivePath)) {
    throw 'Supply -ArchivePath for the official Foundry v1.8.1 Windows archive.'
}
$arguments = @('-m', 'src.governance.toolchains', '--provision', '--archive', $ArchivePath)
if (-not [string]::IsNullOrWhiteSpace($ToolchainRoot)) { $arguments += @('--root', $ToolchainRoot) }
& python @arguments
exit $LASTEXITCODE
