param(
    [string] $IndicatorFixture = (Join-Path $env:USERPROFILE 'Documents\NinjaTrader 8\bin\Custom\Indicators\BeelzebubReadOnlyMarketObserver.cs'),
    [string] $NinjaTraderInstallRoot = (Join-Path $env:ProgramFiles 'NinjaTrader 8')
)
$ErrorActionPreference = 'Stop'
# TEST ONLY: no deployment, sockets, market history, or trading authority.
$commission = Join-Path $PSScriptRoot 'commission_ninjatrader_observer.ps1'
$tokens = $null; $parseErrors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($commission, [ref]$tokens, [ref]$parseErrors)
if ($parseErrors.Count) { throw "Installer syntax errors: $parseErrors" }
$function = $ast.Find({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Get-ObserverInstallText' }, $true)
if ($null -eq $function) { throw 'Actual installer formatter not found.' }
. ([scriptblock]::Create($function.Extent.Text))
$source = [IO.File]::ReadAllText((Join-Path $PSScriptRoot '..\ninjatrader\NinjaScript\Indicators\BeelzebubReadOnlyMarketObserver.cs'))
$installed = [IO.File]::ReadAllText($IndicatorFixture).Replace("`r`n", "`n")
$marker = '#region NinjaScript generated code. Neither change nor remove.'
$index = $installed.IndexOf($marker, [StringComparison]::Ordinal)
if ($index -lt 0) { throw 'Regression requires an actual native-generated wrapper fixture.' }
$fixed = Get-ObserverInstallText -SourceText $source -CurrentText $installed -PreserveGeneratedRegion
$fixedLf = $fixed.Replace("`r`n", "`n")
$wrapper = $fixedLf.Substring($fixedLf.IndexOf($marker)).TrimEnd()
$duplicate = $source.TrimEnd() + "`n`n" + $wrapper + "`n`n" + $wrapper + "`n"
$repaired = Get-ObserverInstallText -SourceText $source -CurrentText $duplicate -PreserveGeneratedRegion
if ($repaired -cne $fixed) { throw 'Duplicate repair differs from single-wrapper result.' }
if ((Get-ObserverInstallText -SourceText $source -CurrentText $repaired -PreserveGeneratedRegion) -cne $repaired) { throw 'Installer is not idempotent.' }
if ([regex]::IsMatch($repaired, '(?<!\r)\n')) { throw 'Deployed line endings are not CRLF.' }
if ($fixedLf.Substring(0, $fixedLf.IndexOf($marker)).TrimEnd() -cne $source.Replace("`r`n", "`n").TrimEnd()) { throw 'Observer body changed.' }
$refused = 0
foreach ($bad in @(
    ($duplicate + 'class UnexpectedSource {}'),
    ($duplicate.Substring(0, $duplicate.LastIndexOf('#endregion'))),
    ($source + "`n" + $wrapper + "`n" + $wrapper.Replace('cacheBeelzebub', 'differentCacheBeelzebub'))
)) {
    try { $null = Get-ObserverInstallText -SourceText $source -CurrentText $bad -PreserveGeneratedRegion }
    catch { $refused++; continue }
    throw 'Unsafe wrapper suffix was not refused.'
}
$without = Get-ObserverInstallText -SourceText $source -CurrentText '' -PreserveGeneratedRegion
if ($without.Contains($marker)) { throw 'Invented a native wrapper.' }

$identityFunction = $ast.Find({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Test-CompiledIdentity' }, $true)
. ([scriptblock]::Create($identityFunction.Extent.Text))
$temporaryAssembly = New-TemporaryFile
$compiledAssembly = $temporaryAssembly.FullName
$exclusive = [IO.File]::Open($compiledAssembly, [IO.FileMode]::Open, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
try {
    if (Test-CompiledIdentity ('a' * 64)) { throw 'Exclusively locked assembly was reported verified.' }
} finally {
    $exclusive.Dispose()
    Remove-Item -LiteralPath $temporaryAssembly.FullName
}

Add-Type -Path (Join-Path $NinjaTraderInstallRoot 'bin\Microsoft.CodeAnalysis.dll')
Add-Type -Path (Join-Path $NinjaTraderInstallRoot 'bin\Microsoft.CodeAnalysis.CSharp.dll')
# Compile the actual generated methods with minimal host type definitions.
# This tests duplicate declarations, NOT native integration or observer behavior.
$hostTypes = @'
namespace NinjaTrader.NinjaScript { public interface ISeries<T> {} }
namespace NinjaTrader.Gui.NinjaScript {
    public class IndicatorRenderBase {
        protected NinjaTrader.NinjaScript.ISeries<double> Input;
        protected T CacheIndicator<T>(T item, NinjaTrader.NinjaScript.ISeries<double> input, ref T[] cache) { return item; }
    }
    public class StrategyRenderBase {
        protected NinjaTrader.NinjaScript.ISeries<double> Input;
        protected NinjaTrader.NinjaScript.Indicators.Indicator indicator;
    }
}
namespace NinjaTrader.NinjaScript.Indicators {
    public class BeelzebubReadOnlyMarketObserver { public bool EqualsInput(ISeries<double> input) { return true; } }
}
namespace NinjaTrader.NinjaScript.MarketAnalyzerColumns {
    public class MarketAnalyzerColumnBase {
        protected ISeries<double> Input;
        protected NinjaTrader.NinjaScript.Indicators.Indicator indicator;
    }
}
'@
[Microsoft.CodeAnalysis.MetadataReference[]] $references = @(
    [Microsoft.CodeAnalysis.MetadataReference]::CreateFromFile('C:\Windows\Microsoft.NET\Framework64\v4.0.30319\mscorlib.dll')
)
function Compile-Wrapper([string] $Suffix) {
    [Microsoft.CodeAnalysis.SyntaxTree[]] $trees = @(
        [Microsoft.CodeAnalysis.CSharp.CSharpSyntaxTree]::ParseText($hostTypes + "`n" + $Suffix)
    )
    $options = [Microsoft.CodeAnalysis.CSharp.CSharpCompilationOptions]::new([Microsoft.CodeAnalysis.OutputKind]::DynamicallyLinkedLibrary)
    $compilation = [Microsoft.CodeAnalysis.CSharp.CSharpCompilation]::Create('GeneratedWrapperRegression', $trees, $references, $options)
    $stream = [IO.MemoryStream]::new()
    try {
        $result = $compilation.Emit($stream)
        return ,@($result.Diagnostics | Where-Object { $_.Severity -eq [Microsoft.CodeAnalysis.DiagnosticSeverity]::Error })
    } finally { $stream.Dispose() }
}
$before = Compile-Wrapper ($wrapper + "`n" + $wrapper)
$after = Compile-Wrapper $wrapper
if (@($before | Where-Object Id -eq 'CS0111').Count -ne 6 -or $after.Count -ne 0) {
    throw "Unexpected wrapper compiler regression result. Before=$before After=$after"
}
[ordered]@{
    test_only = $true
    actual_installer_function = $true
    actual_native_generated_wrapper = $true
    legacy_duplicate_errors = $before.Count
    legacy_error_codes = @($before.Id | Sort-Object -Unique)
    repaired_errors = $after.Count
    identical_duplicates_removed = $true
    idempotent = $true
    unsafe_suffix_cases_refused = $refused
    transient_compiler_lock_not_verified = $true
    observer_body_unchanged = $true
    native_loaded_proof = $false
} | ConvertTo-Json
