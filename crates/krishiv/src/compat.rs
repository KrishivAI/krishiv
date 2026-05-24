//! PySpark migration analyzer (`krishiv compat analyze`).

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

/// Compatibility verdict for a single API call site.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CompatVerdict {
    Supported,
    PartiallySupported { caveats: String },
    Unsupported { reason: String },
}

/// One analyzed call site.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CompatCallSite {
    pub line: usize,
    pub api: String,
    pub verdict: CompatVerdict,
}

/// Full analysis report.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CompatReport {
    pub file: String,
    pub call_sites: Vec<CompatCallSite>,
    pub supported: usize,
    pub partially_supported: usize,
    pub unsupported: usize,
    pub confidence_score: f64,
}

const ANALYZER: &str = r#"
import ast, json, sys
path = sys.argv[1]
with open(path, encoding="utf-8") as f:
    tree = ast.parse(f.read(), filename=path)
calls = []
class Visitor(ast.NodeVisitor):
    def visit_Call(self, node):
        fn = node.func
        if isinstance(fn, ast.Attribute):
            parts = []
            cur = fn
            while isinstance(cur, ast.Attribute):
                parts.append(cur.attr)
                cur = cur.value
            if isinstance(cur, ast.Name):
                parts.append(cur.id)
            api = ".".join(reversed(parts))
            calls.append({"line": node.lineno, "api": api})
        elif isinstance(fn, ast.Name):
            calls.append({"line": node.lineno, "api": fn.id})
        self.generic_visit(node)
Visitor().visit(tree)
print(json.dumps(calls))
"#;

fn classify_api(api: &str) -> CompatVerdict {
    static SUPPORTED: &[&str] = &[
        "SparkSession.builder",
        "SparkSession.builder.remote",
        "DataFrame.filter",
        "DataFrame.where",
        "DataFrame.select",
        "DataFrame.selectExpr",
        "DataFrame.groupBy",
        "DataFrame.agg",
        "DataFrame.join",
        "DataFrame.orderBy",
        "DataFrame.sort",
        "DataFrame.limit",
        "DataFrame.count",
        "DataFrame.collect",
        "DataFrame.show",
        "DataFrame.union",
        "DataFrame.unionAll",
        "DataFrame.distinct",
        "DataFrame.drop",
        "DataFrame.withColumn",
        "col",
        "lit",
        "avg",
        "sum",
        "count",
        "min",
        "max",
        "explode",
        "when",
        "date_add",
        "datediff",
        "from_unixtime",
        "to_date",
        "to_timestamp",
        "read.parquet",
        "read.table",
    ];
    static PARTIAL: &[(&str, &str)] = &[
        ("DataFrame.toPandas", "requires pyarrow installed on client"),
        ("read_stream", "structured streaming deferred to R16"),
        ("writeStream", "structured streaming deferred to R16"),
    ];
    static UNSUPPORTED: &[(&str, &str)] = &[
        ("SparkContext", "RDD API not supported in Spark Connect"),
        ("RDD", "RDD API not supported"),
        ("read.jdbc", "JDBC connector not in R15 scope"),
        ("ml.", "MLlib not in R15 scope"),
    ];
    if SUPPORTED
        .iter()
        .any(|s| api == *s || api.ends_with(s) || api.contains(s))
    {
        return CompatVerdict::Supported;
    }
    if api.starts_with("SparkSession")
        || api.contains(".builder")
        || api.ends_with(".sql")
        || api == "getOrCreate"
        || api == "appName"
        || api == "remote"
        || matches!(
            api,
            "avg" | "sum" | "count" | "min" | "max" | "col" | "lit" | "explode" | "agg"
        )
        || api.ends_with(".filter")
        || api.ends_with(".where")
        || api.ends_with(".groupBy")
        || api.ends_with(".agg")
        || api.ends_with(".collect")
        || api.ends_with(".show")
        || api.ends_with(".join")
        || api.ends_with(".orderBy")
    {
        return CompatVerdict::Supported;
    }
    for (prefix, reason) in UNSUPPORTED {
        if api.contains(prefix) {
            return CompatVerdict::Unsupported {
                reason: (*reason).into(),
            };
        }
    }
    for (prefix, caveats) in PARTIAL {
        if api.contains(prefix) {
            return CompatVerdict::PartiallySupported {
                caveats: (*caveats).into(),
            };
        }
    }
    if api.starts_with("F.") || api.contains("functions.") {
        return CompatVerdict::Supported;
    }
    CompatVerdict::PartiallySupported {
        caveats: "not listed in compatibility matrix; verify manually".into(),
    }
}

/// Analyze a Python file for PySpark API usage.
pub fn analyze_file(path: &Path) -> Result<CompatReport, String> {
    let output = Command::new("python3")
        .arg("-c")
        .arg(ANALYZER)
        .arg(path)
        .output()
        .map_err(|e| format!("failed to run python3 ast analyzer: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "ast analyzer failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let raw: Vec<BTreeMap<String, serde_json::Value>> = serde_json::from_slice(&output.stdout)
        .map_err(|e| format!("invalid analyzer json: {e}"))?;
    let mut call_sites = Vec::new();
    let mut supported = 0usize;
    let mut partially_supported = 0usize;
    let mut unsupported = 0usize;
    for entry in raw {
        let line = entry.get("line").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        let api = entry
            .get("api")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let verdict = classify_api(&api);
        match &verdict {
            CompatVerdict::Supported => supported += 1,
            CompatVerdict::PartiallySupported { .. } => partially_supported += 1,
            CompatVerdict::Unsupported { .. } => unsupported += 1,
        }
        call_sites.push(CompatCallSite { line, api, verdict });
    }
    let total = call_sites.len().max(1);
    let confidence_score = supported as f64 / total as f64;
    Ok(CompatReport {
        file: path.display().to_string(),
        call_sites,
        supported,
        partially_supported,
        unsupported,
        confidence_score,
    })
}

pub fn format_report_text(report: &CompatReport) -> String {
    let mut out = format!("File: {}\n", report.file);
    out.push_str(&format!(
        "Summary: {} supported, {} partial, {} unsupported (confidence {:.0}%)\n\n",
        report.supported,
        report.partially_supported,
        report.unsupported,
        report.confidence_score * 100.0
    ));
    for site in &report.call_sites {
        out.push_str(&format!(
            "  L{} {} {:?}\n",
            site.line, site.api, site.verdict
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn analyze_simple_pyspark_script() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("job.py");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(
            f,
            "from pyspark.sql import SparkSession\nfrom pyspark.sql.functions import col, avg\nspark = SparkSession.builder.remote('sc://localhost:7070').getOrCreate()\ndf = spark.sql('SELECT 1').filter(col('x') > 0).groupBy('k').agg(avg('v'))\ndf.collect()\n"
        )
        .unwrap();
        let report = analyze_file(&path).expect("analyze");
        assert!(report.supported >= 3);
        assert!(report.supported >= 1, "supported={}", report.supported);
    }
}

#[cfg(test)]
mod tpch_analyzer_tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn analyze_tpch_reference_script() {
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/reference/tpch_pyspark.py");
        let report = analyze_file(&path).expect("analyze");
        assert!(
            report.unsupported == 0,
            "unsupported: {:?}",
            report.call_sites
        );
        assert!(report.supported >= 5);
        assert!(report.confidence_score >= 0.9);
    }
}
