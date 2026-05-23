//! Time travel SQL preprocessing (R18 S4, ADR-18.3).

use regex::Regex;
use std::sync::LazyLock;

use krishiv_lakehouse::AsOfSpec;

/// Parsed `AS OF` qualifier attached to a table reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsOfTableRef {
    pub table: String,
    pub spec: AsOfSpec,
}

static VERSION_AS_OF: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(FROM|JOIN)\s+([`\w.]+)\s+VERSION\s+AS\s+OF\s+(-?\d+)").unwrap()
});

static TIMESTAMP_AS_OF: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)\b(FROM|JOIN)\s+([`\w.]+)\s+TIMESTAMP\s+AS\s+OF\s+'([^']+)'"#).unwrap()
});

static SYSTEM_TIME_AS_OF: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)\b(FROM|JOIN)\s+([`\w.]+)\s+FOR\s+SYSTEM_TIME\s+AS\s+OF\s+TIMESTAMP\s+'([^']+)'"#,
    )
    .unwrap()
});

/// Strip `AS OF` clauses and return rewritten SQL plus qualifiers.
pub fn preprocess_as_of_sql(sql: &str) -> Result<(String, Vec<AsOfTableRef>), String> {
    let mut out = sql.to_string();
    let mut refs = Vec::new();

    for cap in VERSION_AS_OF.captures_iter(sql) {
        let table = cap[2].trim_matches('`').to_string();
        let version: i64 = cap[3].parse::<i64>().map_err(|e| e.to_string())?;
        refs.push(AsOfTableRef {
            table: table.clone(),
            spec: AsOfSpec::Version(version),
        });
    }
    for cap in TIMESTAMP_AS_OF.captures_iter(sql) {
        let table = cap[2].trim_matches('`').to_string();
        let spec = AsOfSpec::parse(&cap[3])?;
        refs.push(AsOfTableRef {
            table: table.clone(),
            spec,
        });
    }
    for cap in SYSTEM_TIME_AS_OF.captures_iter(sql) {
        let table = cap[2].trim_matches('`').to_string();
        let spec = AsOfSpec::parse(&cap[3])?;
        refs.push(AsOfTableRef {
            table: table.clone(),
            spec,
        });
    }

    out = VERSION_AS_OF.replace_all(&out, "${1} ${2}").to_string();
    out = TIMESTAMP_AS_OF.replace_all(&out, "${1} ${2}").to_string();
    out = SYSTEM_TIME_AS_OF.replace_all(&out, "${1} ${2}").to_string();
    Ok((out, refs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_version_as_of() {
        let (sql, refs) =
            preprocess_as_of_sql("SELECT * FROM orders VERSION AS OF 3").unwrap();
        assert!(sql.contains("FROM orders"));
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].spec, AsOfSpec::Version(3));
    }
}
