param(
    [string] $RepositoryRoot = (Split-Path $PSScriptRoot -Parent),
    [string] $NinjaTraderInstallRoot = (Join-Path $env:ProgramFiles 'NinjaTrader 8'),
    [string] $NinjaTraderDocumentsRoot = (Join-Path $env:USERPROFILE 'Documents\NinjaTrader 8')
)

$ErrorActionPreference = 'Stop'
$bin = Join-Path $NinjaTraderInstallRoot 'bin'
$custom = Join-Path $NinjaTraderDocumentsRoot 'bin\Custom'
$sources = @(
    (Join-Path $RepositoryRoot 'ninjatrader\NinjaScript\AddOns\BeelzebubReadOnlyAddOn.cs'),
    (Join-Path $RepositoryRoot 'ninjatrader\NinjaScript\Indicators\BeelzebubReadOnlyMarketObserver.cs')
)
$referencePaths = @(
    'C:\Windows\Microsoft.NET\Framework64\v4.0.30319\mscorlib.dll',
    'C:\Windows\Microsoft.NET\Framework64\v4.0.30319\System.dll',
    'C:\Windows\Microsoft.NET\Framework64\v4.0.30319\System.Core.dll',
    'C:\Windows\Microsoft.NET\Framework64\v4.0.30319\System.Xml.dll',
    'C:\Windows\Microsoft.NET\Framework64\v4.0.30319\System.Xaml.dll',
    'C:\Windows\Microsoft.NET\Framework\v4.0.30319\WPF\WindowsBase.dll',
    'C:\Windows\Microsoft.NET\Framework\v4.0.30319\WPF\PresentationCore.dll',
    'C:\Windows\Microsoft.NET\Framework\v4.0.30319\WPF\PresentationFramework.dll',
    (Join-Path $bin 'NinjaTrader.Core.dll'),
    (Join-Path $bin 'NinjaTrader.Gui.dll'),
    (Join-Path $custom 'NinjaTrader.Custom.dll')
)
$required = @(
    $sources + $referencePaths + @(
        (Join-Path $bin 'Microsoft.CodeAnalysis.dll'),
        (Join-Path $bin 'Microsoft.CodeAnalysis.CSharp.dll')
    )
)
$missing = @($required | Where-Object { -not [IO.File]::Exists($_) })
if ($missing.Count -gt 0) {
    [ordered]@{
        schema = 'beelzebub-ninjatrader-observer-compile-v1'
        success = $false
        failure = 'REQUIRED_COMPILER_INPUT_MISSING'
        missing = $missing
    } | ConvertTo-Json -Compress -Depth 4
    exit 1
}

Add-Type -Path (Join-Path $bin 'Microsoft.CodeAnalysis.dll')
Add-Type -Path (Join-Path $bin 'Microsoft.CodeAnalysis.CSharp.dll')
[Microsoft.CodeAnalysis.SyntaxTree[]] $trees = @(
    $sources | ForEach-Object {
        [Microsoft.CodeAnalysis.CSharp.CSharpSyntaxTree]::ParseText([IO.File]::ReadAllText($_))
    }
)
[Microsoft.CodeAnalysis.MetadataReference[]] $references = @(
    $referencePaths | ForEach-Object {
        [Microsoft.CodeAnalysis.MetadataReference]::CreateFromFile($_)
    }
)
$options = [Microsoft.CodeAnalysis.CSharp.CSharpCompilationOptions]::new(
    [Microsoft.CodeAnalysis.OutputKind]::DynamicallyLinkedLibrary
)
$compilation = [Microsoft.CodeAnalysis.CSharp.CSharpCompilation]::Create(
    'BeelzebubReadOnlyObserver.Validation', $trees, $references, $options
)
$stream = [IO.MemoryStream]::new()
try {
    $result = $compilation.Emit($stream)
    $errors = @($result.Diagnostics | Where-Object {
        $_.Severity -eq [Microsoft.CodeAnalysis.DiagnosticSeverity]::Error
    })
    $warnings = @($result.Diagnostics | Where-Object {
        $_.Severity -eq [Microsoft.CodeAnalysis.DiagnosticSeverity]::Warning
    })
    [ordered]@{
        schema = 'beelzebub-ninjatrader-observer-compile-v1'
        success = $result.Success
        compiler = 'NINJATRADER_BUNDLED_ROSLYN_IN_MEMORY'
        errors = $errors.Count
        warnings = $warnings.Count
        error_details = @($errors | ForEach-Object { $_.ToString() })
        sources = @($sources | ForEach-Object {
            [ordered]@{
                path = $_
                sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $_).Hash.ToLowerInvariant()
            }
        })
    } | ConvertTo-Json -Compress -Depth 5
    if (-not $result.Success) { exit 1 }
}
finally {
    $stream.Dispose()
}
