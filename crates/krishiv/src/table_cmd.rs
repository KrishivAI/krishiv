//! `krishiv table read` — lakehouse table scans.

use std::path::PathBuf;

use krishiv_api::{QueryResult, Session, SessionBuilder};
use krishiv_common::async_util::block_on;
use krishiv_connectors::lakehouse::HudiQueryType;

use crate::cli::CliResponse;

pub fn table_help() -> String {
    String::from(
        "Read a table into the query engine and print results.\n\
         \n\
         Usage:\n\
           krishiv table read --path <PATH> --format <parquet|delta|hudi> [OPTIONS]\n\
         \n\
         Options:\n\
           --path <PATH>              Table path (required)\n\
           --format <FORMAT>          parquet | delta | hudi (required)\n\
                                      Note: delta and hudi read local filesystem paths only.\n\
                                      S3 and remote catalog support is planned for a future release.\n\
           --version <N>              Delta table version (optional)\n\
           --hudi-query <snapshot|incremental>  Hudi query type (default: snapshot)\n\
           --hudi-begin <INSTANT>     Hudi incremental begin instant (optional)\n\
           --limit <N>                Max rows to print (default: all)\n\
           -h, --help                 Show help\n\
         \n\
         Examples:\n\
           krishiv table read --path ./t.parquet --format parquet\n\
           krishiv table read --path ./delta_tbl --format delta\n\
           krishiv table read --path ./hudi_tbl --format hudi --hudi-query incremental\n",
    )
}

pub fn run_table(args: &[&str]) -> CliResponse {
    match args {
        [] | ["--help"] | ["-h"] => CliResponse::ok(format!("{}\n", table_help())),
        ["read", "--help"] | ["read", "-h"] => CliResponse::ok(format!("{}\n", table_help())),
        ["read", rest @ ..] => run_table_read(rest),
        [unknown, ..] => CliResponse::err(
            format!("unknown table subcommand: {unknown}\n\n{}", table_help()),
            2,
        ),
    }
}

fn run_table_read(args: &[&str]) -> CliResponse {
    let cmd = match parse_table_read(args) {
        Ok(c) => c,
        Err(e) => return CliResponse::err(format!("{e}\n\n{}", table_help()), 2),
    };
    let session = match SessionBuilder::new().build() {
        Ok(s) => s,
        Err(e) => return CliResponse::err(format!("{e}\n"), 1),
    };
    match block_on(read_and_pretty(&session, &cmd)) {
        Ok(output) => CliResponse::ok(output),
        Err(e) => CliResponse::err(format!("{e}\n"), 1),
    }
}

struct TableReadCommand {
    path: PathBuf,
    format: TableFormat,
    delta_version: Option<i64>,
    hudi_query: HudiQueryType,
    hudi_begin: Option<String>,
    limit: Option<usize>,
}

enum TableFormat {
    Parquet,
    Delta,
    Hudi,
}

fn parse_table_read(args: &[&str]) -> Result<TableReadCommand, String> {
    let mut path = None;
    let mut format = None;
    let mut delta_version = None;
    let mut hudi_query = HudiQueryType::Snapshot;
    let mut hudi_begin = None;
    let mut limit = None;
    let mut idx = 0;
    while idx < args.len() {
        let Some(&arg) = args.get(idx) else { break; };
        match arg {
            "--path" => {
                idx += 1;
                path = Some(PathBuf::from(
                    args.get(idx)
                        .ok_or_else(|| String::from("missing value for --path"))?,
                ));
            }
            "--format" => {
                idx += 1;
                format = Some(
                    args.get(idx)
                        .ok_or_else(|| String::from("missing value for --format"))?
                        .to_string(),
                );
            }
            "--version" => {
                idx += 1;
                let v = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --version"))?;
                delta_version = Some(
                    v.parse::<i64>()
                        .map_err(|_| String::from("--version must be an integer"))?,
                );
            }
            "--hudi-query" => {
                idx += 1;
                let v = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --hudi-query"))?;
                hudi_query = match *v {
                    "snapshot" => HudiQueryType::Snapshot,
                    "incremental" => HudiQueryType::Incremental,
                    other => return Err(format!("unsupported --hudi-query: {other}")),
                };
            }
            "--hudi-begin" => {
                idx += 1;
                hudi_begin = Some(
                    args.get(idx)
                        .ok_or_else(|| String::from("missing value for --hudi-begin"))?
                        .to_string(),
                );
            }
            "--limit" => {
                idx += 1;
                let v = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --limit"))?;
                limit = Some(
                    v.parse::<usize>()
                        .map_err(|_| String::from("--limit must be a positive integer"))?,
                );
            }
            "--help" | "-h" => return Err(String::from("help requested")),
            unknown => return Err(format!("unknown option: {unknown}")),
        }
        idx += 1;
    }
    let path = path.ok_or_else(|| String::from("missing required --path"))?;
    let format = match format
        .ok_or_else(|| String::from("missing required --format"))?
        .as_str()
    {
        "parquet" => TableFormat::Parquet,
        "delta" => TableFormat::Delta,
        "hudi" => TableFormat::Hudi,
        other => return Err(format!("unsupported --format: {other}")),
    };
    Ok(TableReadCommand {
        path,
        format,
        delta_version,
        hudi_query,
        hudi_begin,
        limit,
    })
}

async fn read_and_pretty(
    session: &Session,
    cmd: &TableReadCommand,
) -> Result<String, krishiv_api::KrishivError> {
    let df = match &cmd.format {
        TableFormat::Parquet => session.read_parquet_async(&cmd.path).await?,
        TableFormat::Delta => {
            session
                .read_delta_async(cmd.path.to_string_lossy().as_ref(), cmd.delta_version)
                .await?
        }
        TableFormat::Hudi => {
            session
                .read_hudi_async(
                    cmd.path.to_string_lossy().as_ref(),
                    cmd.hudi_query,
                    cmd.hudi_begin.as_deref(),
                )
                .await?
        }
    };
    let mut result = df.collect_async().await?;
    if let Some(limit) = cmd.limit {
        let batches = result.batches().to_vec();
        let mut rows = 0usize;
        let mut kept = Vec::new();
        for batch in batches {
            if rows >= limit {
                break;
            }
            let take = (limit - rows).min(batch.num_rows());
            if take == batch.num_rows() {
                rows += take;
                kept.push(batch);
            } else if take > 0 {
                kept.push(batch.slice(0, take));
                rows += take;
            }
        }
        result = QueryResult::new(kept);
    }
    let table = result.pretty()?;
    Ok(format!("{table}\n"))
}
