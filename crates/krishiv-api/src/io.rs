//! Generic DataFrame reader and writer builders.

use std::collections::BTreeMap;
use std::path::Path;

pub use krishiv_common::write_commit::WriteMode;

use crate::{DataFrame, KrishivError, Result, Session};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataFormat {
    Parquet,
    Csv,
    Json,
}

impl DataFormat {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "parquet" => Ok(Self::Parquet),
            "csv" => Ok(Self::Csv),
            "json" | "ndjson" => Ok(Self::Json),
            other => Err(KrishivError::unsupported(format!(
                "unsupported data format '{other}'; expected parquet, csv, or json"
            ))),
        }
    }
}

#[derive(Clone)]
pub struct DataFrameReader {
    session: Session,
    format: Option<DataFormat>,
    options: BTreeMap<String, String>,
}

impl DataFrameReader {
    pub(crate) fn new(session: Session) -> Self {
        Self {
            session,
            format: None,
            options: BTreeMap::new(),
        }
    }

    pub fn format(mut self, format: &str) -> Result<Self> {
        self.format = Some(DataFormat::parse(format)?);
        Ok(self)
    }

    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    pub fn load(self, path: impl AsRef<Path>) -> Result<DataFrame> {
        let format = self.format.ok_or_else(|| KrishivError::InvalidConfig {
            message: "reader format must be selected before load".into(),
        })?;
        match format {
            DataFormat::Parquet => {
                let mut opts = krishiv_sql::ParquetReaderOptions::default();
                for (key, value) in &self.options {
                    match key.as_str() {
                        "batch_size" | "batchSize" => {
                            opts.batch_size = Some(value.parse::<usize>().map_err(|_| {
                                KrishivError::InvalidConfig {
                                    message: format!("batch_size must be a positive integer, got '{value}'"),
                                }
                            })?);
                        }
                        other => {
                            return Err(KrishivError::unsupported(format!(
                                "unsupported parquet reader option '{other}'; supported: batch_size"
                            )));
                        }
                    }
                }
                self.session.read_parquet_with_options(path, opts)
            }
            DataFormat::Csv => {
                let mut opts = krishiv_sql::CsvReaderOptions::default();
                for (key, value) in &self.options {
                    match key.as_str() {
                        "delimiter" | "sep" => {
                            let mut chars = value.chars();
                            let c = chars.next().ok_or_else(|| KrishivError::InvalidConfig {
                                message: "delimiter must be a non-empty string".into(),
                            })?;
                            if chars.next().is_some() {
                                return Err(KrishivError::InvalidConfig {
                                    message: "delimiter must be a single character".into(),
                                });
                            }
                            opts.delimiter = Some(c);
                        }
                        "has_header" | "hasHeader" | "header" => {
                            opts.has_header = Some(match value.to_ascii_lowercase().as_str() {
                                "true" | "1" | "yes" => true,
                                "false" | "0" | "no" => false,
                                other => return Err(KrishivError::InvalidConfig {
                                    message: format!("has_header must be true or false, got '{other}'"),
                                }),
                            });
                        }
                        other => {
                            return Err(KrishivError::unsupported(format!(
                                "unsupported csv reader option '{other}'; supported: delimiter, has_header"
                            )));
                        }
                    }
                }
                self.session.read_csv_with_options(path, opts)
            }
            DataFormat::Json => {
                if !self.options.is_empty() {
                    return Err(KrishivError::unsupported(format!(
                        "json reader does not support options: {:?}",
                        self.options.keys().collect::<Vec<_>>()
                    )));
                }
                self.session.read_json(path)
            }
        }
    }
}

#[derive(Clone)]
pub struct DataFrameWriter {
    dataframe: DataFrame,
    format: Option<DataFormat>,
    mode: Option<WriteMode>,
    partition_by: Vec<String>,
    options: BTreeMap<String, String>,
}

impl DataFrameWriter {
    pub(crate) fn new(dataframe: DataFrame) -> Self {
        Self {
            dataframe,
            format: None,
            mode: None,
            partition_by: Vec::new(),
            options: BTreeMap::new(),
        }
    }

    pub fn format(mut self, format: &str) -> Result<Self> {
        self.format = Some(DataFormat::parse(format)?);
        Ok(self)
    }

    /// Set the write mode: `append`, `overwrite`, `error_if_exists`, or `ignore`.
    ///
    /// Setting a mode routes Parquet saves through the distributed sink stage
    /// (Phase 2.3 staged commit protocol): `path` becomes a directory of
    /// `part-*.parquet` files that is committed atomically on job success.
    pub fn mode(mut self, mode: &str) -> Result<Self> {
        let parsed = WriteMode::parse(mode).map_err(|e| KrishivError::InvalidConfig {
            message: e.to_string(),
        })?;
        self.mode = Some(parsed);
        Ok(self)
    }

    /// Partition output by the given columns (Hive `col=value` directory layout).
    ///
    /// Implies the distributed sink write path, like [`Self::mode`].
    pub fn partition_by(mut self, columns: &[&str]) -> Self {
        self.partition_by = columns.iter().map(|c| (*c).to_owned()).collect();
        self
    }

    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    pub fn save(self, path: &str) -> Result<()> {
        let format = self.format.ok_or_else(|| KrishivError::InvalidConfig {
            message: "writer format must be selected before save".into(),
        })?;

        let mut mode = self.mode;
        let mut partition_by = self.partition_by;
        let mut parquet_opts = krishiv_sql::ParquetWriterOptions::default();
        let mut csv_opts = krishiv_sql::CsvWriterOptions::default();

        for (key, value) in &self.options {
            match key.as_str() {
                "mode" => {
                    mode = Some(WriteMode::parse(value).map_err(|e| {
                        KrishivError::InvalidConfig {
                            message: e.to_string(),
                        }
                    })?);
                }
                "partition_by" | "partitionBy" => {
                    partition_by = value
                        .split(',')
                        .map(str::trim)
                        .filter(|c| !c.is_empty())
                        .map(str::to_owned)
                        .collect();
                    if partition_by.is_empty() {
                        return Err(KrishivError::InvalidConfig {
                            message: format!(
                                "writer option '{key}' must list at least one column"
                            ),
                        });
                    }
                }
                "compression" => {
                    parquet_opts.compression = Some(value.clone());
                }
                "max_row_group_size" | "maxRowGroupSize" => {
                    parquet_opts.max_row_group_size =
                        Some(value.parse::<usize>().map_err(|_| {
                            KrishivError::InvalidConfig {
                                message: format!(
                                    "max_row_group_size must be a positive integer, got '{value}'"
                                ),
                            }
                        })?);
                }
                "delimiter" | "sep" => {
                    let mut chars = value.chars();
                    let c = chars.next().ok_or_else(|| KrishivError::InvalidConfig {
                        message: "delimiter must be a non-empty string".into(),
                    })?;
                    if chars.next().is_some() {
                        return Err(KrishivError::InvalidConfig {
                            message: "delimiter must be a single character".into(),
                        });
                    }
                    csv_opts.delimiter = Some(c);
                }
                "has_header" | "hasHeader" | "header" => {
                    csv_opts.has_header = Some(match value.to_ascii_lowercase().as_str() {
                        "true" | "1" | "yes" => true,
                        "false" | "0" | "no" => false,
                        other => {
                            return Err(KrishivError::InvalidConfig {
                                message: format!(
                                    "has_header must be true or false, got '{other}'"
                                ),
                            })
                        }
                    });
                }
                other => {
                    return Err(KrishivError::unsupported(format!(
                        "unsupported writer option '{other}'; supported options: \
                         mode, partition_by, compression, max_row_group_size, \
                         delimiter, has_header"
                    )));
                }
            }
        }

        let wants_sink = mode.is_some() || !partition_by.is_empty();
        let has_parquet_opts =
            parquet_opts.compression.is_some() || parquet_opts.max_row_group_size.is_some();
        let has_csv_opts = csv_opts.delimiter.is_some() || csv_opts.has_header.is_some();

        match format {
            DataFormat::Parquet if wants_sink => self.dataframe.write_parquet_sink(
                path,
                mode.unwrap_or_default(),
                &partition_by,
            ),
            DataFormat::Parquet if has_parquet_opts => {
                self.dataframe.write_parquet_with_options(path, &parquet_opts)
            }
            DataFormat::Parquet => self.dataframe.write_parquet(path),
            DataFormat::Csv | DataFormat::Json if wants_sink => {
                Err(KrishivError::unsupported(
                    "write mode / partition_by are only supported for parquet output; \
                     csv and json writes collect client-side",
                ))
            }
            DataFormat::Csv if has_csv_opts => {
                self.dataframe.write_csv_with_options(path, &csv_opts)
            }
            DataFormat::Csv => self.dataframe.write_csv(path),
            DataFormat::Json => self.dataframe.write_json(path),
        }
    }
}
