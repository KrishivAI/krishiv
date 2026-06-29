//! `krishiv pipeline` — run declarative SQL pipeline projects (Spark SDP-style).
//!
//! A project is a directory (or single file) of `.sql` files containing
//! `CREATE SOURCE` / `CREATE INCREMENTAL VIEW` / `CREATE SINK` / `START PIPELINE`
//! statements. The CLI loads them, runs the DDL on an embedded session, and
//! starts the declared pipelines.
//!
//! ```text
//! krishiv pipeline init [--name N] [--dir D]   # scaffold a project
//! krishiv pipeline dry-run <path>              # load + validate, no execution
//! krishiv pipeline run <path> [--full-refresh] # load + execute
//! ```

use std::fs;
use std::path::{Path, PathBuf};

use krishiv_api::SessionBuilder;
use krishiv_common::sql_util::split_sql_statements;

use crate::cli::CliResponse;

pub fn run_pipeline(args: &[&str]) -> CliResponse {
    match args {
        [] | ["--help"] | ["-h"] | ["help"] => CliResponse::ok(pipeline_help()),
        ["init", rest @ ..] => wrap(cmd_init(rest)),
        ["dry-run", rest @ ..] => wrap(cmd_run(rest, true)),
        ["run", rest @ ..] => wrap(cmd_run(rest, false)),
        [unknown, ..] => CliResponse::err(
            format!(
                "unknown pipeline subcommand '{unknown}'\n\n{}",
                pipeline_help()
            ),
            2,
        ),
    }
}

fn wrap(r: Result<String, String>) -> CliResponse {
    match r {
        Ok(s) => CliResponse::ok(s),
        Err(e) => CliResponse::err(format!("{e}\n"), 1),
    }
}

pub fn pipeline_help() -> String {
    "\
krishiv pipeline — run declarative SQL pipeline projects

USAGE:
    krishiv pipeline init [--name <NAME>] [--dir <DIR>]
    krishiv pipeline dry-run <PATH>
    krishiv pipeline run <PATH> [--full-refresh]

A project is a directory (or file) of .sql files with CREATE SOURCE / CREATE
INCREMENTAL VIEW / CREATE SINK / START PIPELINE statements.

    init       Scaffold a new project with an example pipeline.
    dry-run    Load and validate the project without executing it.
    run        Load and execute the project; --full-refresh resets state first.
"
    .to_string()
}

// ── init ──────────────────────────────────────────────────────────────────────

fn cmd_init(args: &[&str]) -> Result<String, String> {
    let name = flag_value(args, "--name").unwrap_or_else(|| "my_pipeline".to_string());
    let dir = flag_value(args, "--dir").unwrap_or_else(|| ".".to_string());
    let pdir = Path::new(&dir).join("pipelines");
    fs::create_dir_all(&pdir).map_err(|e| e.to_string())?;
    let example = format!(
        "-- {name}: example declarative pipeline\n\
         CREATE SOURCE orders AS SELECT 1 AS id, 100 AS amount UNION ALL SELECT 2 AS id, 50 AS amount;\n\
         CREATE INCREMENTAL VIEW revenue AS SELECT SUM(amount) AS total FROM orders;\n\
         CREATE SINK out FROM revenue;\n\
         START PIPELINE out;\n"
    );
    let file = pdir.join("pipeline.sql");
    fs::write(&file, example).map_err(|e| e.to_string())?;
    Ok(format!(
        "Initialized pipeline project '{name}' at {}\n",
        file.display()
    ))
}

// ── run / dry-run ─────────────────────────────────────────────────────────────

fn cmd_run(args: &[&str], dry: bool) -> Result<String, String> {
    let path = positional(args).ok_or("missing project path (a .sql file or directory)")?;
    let full_refresh = args.contains(&"--full-refresh");
    let statements = load_sql_files(&path)?;
    if statements.is_empty() {
        return Err(format!("no SQL statements found in {path}"));
    }

    let session = SessionBuilder::new().build().map_err(|e| e.to_string())?;

    let mut ddl_count = 0usize;
    let mut pipelines: Vec<String> = Vec::new();
    let mut output = String::new();

    for stmt in &statements {
        let upper = stmt.to_uppercase();
        if upper.starts_with("START PIPELINE") {
            let sink = upper
                .strip_prefix("START PIPELINE")
                .map(|s| stmt[stmt.len() - s.len()..].trim().to_string())
                .unwrap_or_default();
            if dry {
                session
                    .validate_pipeline(&sink)
                    .map_err(|e| e.to_string())?;
                pipelines.push(format!("{sink} (valid)"));
            } else {
                let run_stmt = if full_refresh {
                    format!("REFRESH PIPELINE {sink} FULL")
                } else {
                    stmt.clone()
                };
                // Execute and capture the sink's output (memory sinks return a
                // result set; connector sinks return an empty ack).
                let df = session.sql(&run_stmt).map_err(|e| e.to_string())?;
                let result = df.collect().map_err(|e| e.to_string())?;
                if result.row_count() > 0 {
                    let pretty = result.pretty().map_err(|e| e.to_string())?;
                    output.push_str(&format!("\n── pipeline '{sink}' output ──\n{pretty}\n"));
                }
                pipelines.push(sink);
            }
        } else {
            // CREATE / DROP SOURCE / SINK / INCREMENTAL VIEW.
            session.sql(stmt).map_err(|e| e.to_string())?;
            ddl_count += 1;
        }
    }

    let verb = if dry { "validated" } else { "ran" };
    output.push_str(&format!(
        "pipeline {verb}: {ddl_count} DDL statement(s); pipeline(s): {}\n",
        if pipelines.is_empty() {
            "none".to_string()
        } else {
            pipelines.join(", ")
        }
    ));
    Ok(output)
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn flag_value(args: &[&str], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| *a == flag)
        .and_then(|i| args.get(i + 1))
        .map(|s| s.to_string())
}

fn positional(args: &[&str]) -> Option<String> {
    let mut i = 0;
    while i < args.len() {
        let Some(&a) = args.get(i) else {
            break;
        };
        if a == "--name" || a == "--dir" {
            i += 2;
            continue;
        }
        if a.starts_with("--") {
            i += 1;
            continue;
        }
        return Some(a.to_string());
    }
    None
}

fn load_sql_files(path: &str) -> Result<Vec<String>, String> {
    let p = Path::new(path);
    let files: Vec<PathBuf> = if p.is_file() {
        vec![p.to_path_buf()]
    } else if p.is_dir() {
        let mut entries: Vec<PathBuf> = fs::read_dir(p)
            .map_err(|e| e.to_string())?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|f| f.extension().map(|x| x == "sql").unwrap_or(false))
            .collect();
        entries.sort();
        entries
    } else {
        return Err(format!("path not found: {path}"));
    };

    let mut statements = Vec::new();
    for f in files {
        let content = fs::read_to_string(&f).map_err(|e| e.to_string())?;
        statements.extend(split_sql_statements(&content));
    }
    Ok(statements)
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_project(dir: &Path) {
        let pdir = dir.join("pipelines");
        fs::create_dir_all(&pdir).unwrap();
        let mut f = fs::File::create(pdir.join("p.sql")).unwrap();
        writeln!(
            f,
            "-- demo\n\
             CREATE SOURCE orders AS SELECT 1 AS id, 100 AS amount UNION ALL SELECT 2 AS id, 50 AS amount;\n\
             CREATE INCREMENTAL VIEW revenue AS SELECT SUM(amount) AS total FROM orders;\n\
             CREATE SINK out FROM revenue;\n\
             START PIPELINE out;"
        )
        .unwrap();
    }

    #[test]
    fn pipeline_run_and_dry_run_a_sql_project() {
        let tmp = tempfile::tempdir().unwrap();
        write_project(tmp.path());
        let path = tmp.path().join("pipelines");
        let path_str = path.to_str().unwrap();

        // dry-run validates without executing.
        let dry = run_pipeline(&["dry-run", path_str]);
        assert_eq!(dry.exit_code, 0, "dry-run failed: {}", dry.stderr);
        assert!(dry.stdout.contains("out (valid)"), "{}", dry.stdout);

        // run executes the project end to end.
        let run = run_pipeline(&["run", path_str]);
        assert_eq!(run.exit_code, 0, "run failed: {}", run.stderr);
        assert!(run.stdout.contains("ran: 3 DDL"), "{}", run.stdout);
    }

    #[test]
    fn pipeline_init_scaffolds_project() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let resp = run_pipeline(&["init", "--name", "demo", "--dir", dir]);
        assert_eq!(resp.exit_code, 0);
        assert!(tmp.path().join("pipelines/pipeline.sql").exists());
    }
}
