# Exports the three Hive validation queries as BOM-free UTF-8 TSV files.
# PowerShell 5.1's `>` adds a BOM/UTF-16, which breaks compare_results.py.

param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
)

$ErrorActionPreference = 'Stop'

$env:JAVA_HOME = 'C:\Java\java-11'
$env:HADOOP_HOME = 'C:\hadoop'
$env:HIVE_HOME = 'C:\apache-hive-3.1.3-bin'
$env:PATH = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:HIVE_HOME\bin;$env:PATH"

$hive = Join-Path $env:HIVE_HOME 'bin\hive.cmd'
$outDir = Join-Path $ProjectRoot 'Hive_Output'
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

if (-not (Test-Path $hive)) {
    throw "Hive wrapper not found: $hive"
}
New-Item -ItemType Directory -Force $outDir | Out-Null

function Export-HiveTsv {
    param(
        [string]$Sql,
        [string]$FileName
    )

    # PowerShell 5.1 turns native stderr into ErrorRecords. Temporarily relax
    # ErrorActionPreference so SLF4J warnings do not abort the export.
    $oldPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $raw = & $hive -e $Sql 2>&1
    $hiveExit = $LASTEXITCODE
    $ErrorActionPreference = $oldPreference

    if ($hiveExit -ne 0) {
        $raw | Out-String | Write-Error
        throw "Hive query failed while exporting $FileName"
    }

    # Keep only real result rows (key TAB value). Audit/log lines can also
    # contain tabs, so explicitly exclude them.
    $lines = @($raw | ForEach-Object { "$_" } | Where-Object {
        $_ -match "`t" -and
        $_ -notmatch '\b(INFO|WARN|ERROR|DEBUG)\b' -and
        $_ -notmatch 'audit:'
    })
    if ($lines.Count -eq 0) {
        throw "Hive returned no TAB-separated rows for $FileName"
    }

    $target = Join-Path $outDir $FileName
    [System.IO.File]::WriteAllLines($target, $lines, $utf8NoBom)
    Write-Host "WROTE $target ($($lines.Count) rows)"
}

Export-HiveTsv -FileName 'sales_by_category.tsv' -Sql 'SELECT `Product_Category` AS category, ROUND(SUM(`Gross_Amount`), 2) AS value FROM orders GROUP BY `Product_Category` ORDER BY category;'

Export-HiveTsv -FileName 'category_count.tsv' -Sql 'SELECT `Product_Category` AS category, COUNT(DISTINCT `Order_ID`) AS value FROM orders GROUP BY `Product_Category` ORDER BY category;'

Export-HiveTsv -FileName 'payment_count.tsv' -Sql 'SELECT `Payment_Method` AS category, COUNT(DISTINCT `Order_ID`) AS value FROM orders GROUP BY `Payment_Method` ORDER BY category;'

Get-ChildItem $outDir -File | Select-Object Name, Length
