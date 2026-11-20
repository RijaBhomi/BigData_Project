# Hive 3.1.3 setup on this Windows machine

These are the real setup steps performed locally so `hive` can run with the
existing Hadoop 3.4.3 installation and Java 11.

## Installed location

- Archive: `C:\Users\Admin\Downloads\apache-hive-3.1.3-bin.tar.gz`
- Extracted to: `C:\apache-hive-3.1.3-bin`

The archive contains only the Unix `bin/hive` script, so Windows wrappers were added.

## Files created in the Hive installation

| File | Purpose |
|---|---|
| `bin\hive.cmd` | Windows launcher for the Hive CLI. Sets `HIVE_HOME`, `HADOOP_HOME`, `JAVA_HOME`, builds the Hadoop + Hive classpath, and starts `HiveLauncher`. |
| `bin\schematool.cmd` | Same classpath, but starts `org.apache.hive.beeline.HiveSchemaTool` for metastore initialization. |
| `bin\hiveserver2.cmd` | Starts HiveServer2 on `127.0.0.1:10000` with the same Java 11 launcher. Keep this window open. |
| `bin\beeline.cmd` | Starts the Beeline JDBC client, which prints the dotted `+---+` table format wanted for screenshots. |
| `bin\launcher\HiveLauncher.java` / `.class` | Loads Hive through a `URLClassLoader`, which Hive 3.1.x requires on Java 9+. |
| `bin\launcher\PatchHiveSubList.java` / `.class` | One-time ASM patcher used for the Java 11 compatibility fix below. |
| `conf\hive-site.xml` | Embedded Derby metastore at `C:\apache-hive-3.1.3-bin\metastore_db`, warehouse `/user/hive/warehouse`, scratch `/tmp/hive`, no result headers, local-task/map-side-join optimization disabled, HiveServer2 bound to `127.0.0.1:10000` with `NONE` authentication. |

## Compatibility fixes applied

1. **HDFS directories**
   - Created `/user/hive/warehouse` and `/tmp/hive`.
   - Chowned `/user/hive/warehouse` to `Admin:supergroup`.
   - Charged `/tmp/hive` to mode `777` because Hive rejected `rwxr-xr-x` as not writable.

2. **Metastore schema**
   - Initialized Derby with `schematool.cmd -dbType derby -initSchema`.
   - A first partial `metastore_db` was removed and reinitialized cleanly.

3. **Missing legacy dependency**
   - Copied `commons-collections-3.2.2.jar` from the local Maven repository into `C:\apache-hive-3.1.3-bin\lib`.
   - Hadoop 3.4.x no longer supplies `org.apache.commons.collections.CollectionUtils`, which Hive 3.1.3's metastore still calls.

4. **Java 11 reflection fix**
   - Hive 3.1.3 reflects on `java.util.ArrayList$SubList.parentOffset`; Java 11 renamed that field to `offset`.
   - Patched one string constant in `lib\hive-exec-3.1.3.jar` from `parentOffset` to `offset`.
   - Original jar preserved as `lib\hive-exec-3.1.3.jar.before-jdk11-patch`.

5. **Local-task setting**
   - During the first successful full run, one map-side-join local task failed to launch `C:\hadoop\bin\hadoop` as a Win32 executable.
   - Hive automatically retried that stage with `MapRedTask` and the full SQL file still exited 0.
   - `hive.exec.mode.local.auto=false` and `hive.auto.convert.join=false` were added to `hive-site.xml` so future runs avoid that confusing recovered error.

## Expected harmless warnings

The CLI prints SLF4J “multiple bindings” and illegal reflective-access warnings.
These are warnings only; the smoke query `select 1 as ok` returned `1` with exit code 0.

## Running Hive from the project

```powershell
cd D:\Ecommerce_BigData_Project\Ecommerce_BigData_Project
& 'C:\apache-hive-3.1.3-bin\bin\hive.cmd' -f 'Hive\hive_queries.sql'
.\Hive\export_hive_results.ps1
python Validation\compare_results.py
```

## Dotted-table output with Beeline (for screenshots)

The `hive.cmd` CLI prints long `INFO` logs and tab-separated rows. Beeline gives
the boxed `+---+` table format instead.

1. In **window 1**, start the server and leave it open:

   ```powershell
   & 'C:\apache-hive-3.1.3-bin\bin\hiveserver2.cmd'
   ```

2. In **window 2**, run a query (the `2>` redirect hides SLF4J warnings):

   ```powershell
   & 'C:\apache-hive-3.1.3-bin\bin\beeline.cmd' `
     -u 'jdbc:hive2://127.0.0.1:10000/default' -n Admin -p password --header=true `
     -e 'SELECT `Product_Category` AS category, ROUND(SUM(`Gross_Amount`), 2) AS total_sales FROM orders GROUP BY `Product_Category` ORDER BY total_sales DESC;' `
     2> "$env:TEMP\beeline_stderr.log"
   ```

The password value is a placeholder: `hive.server2.authentication=NONE`, so it
is ignored. An empty password (`-p ''`) triggers a Beeline 3.1.3 bug, so always
pass a non-empty dummy value.

Observed Beeline output:

```text
+----------------+--------------+
|    category    | total_sales  |
+----------------+--------------+
| Electronics    | 11050074.20  |
| Home & Garden  | 4250493.56   |
| Sports         | 3382393.38   |
| Fashion        | 1659902.99   |
| Toys           | 1067731.89   |
| Beauty         | 733728.67    |
| Food           | 446053.72    |
| Books          | 378492.10    |
+----------------+--------------+
```

Stop the server with `Ctrl+C` in window 1 when finished.
