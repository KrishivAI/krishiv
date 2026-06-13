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
        if !self.options.is_empty() {
            return Err(KrishivError::unsupported(format!(
                "reader options are reserved for future format-specific configuration: {:?}",
                self.options.keys().collect::<Vec<_>>()
            )));
        }
        match self.format.ok_or_else(|| KrishivError::InvalidConfig {
            message: "reader format must be selected before load".into(),
        })? {
            DataFormat::Parquet => self.session.read_parquet(path),
            DataFormat::Csv => self.session.read_csv(path),
            DataFormat::Json => self.session.read_json(path),
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
        let mut mode = self.mode;
        let mut partition_by = self.partition_by;
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
                other => {
                    return Err(KrishivError::unsupported(format!(
                        "unsupported writer option '{other}'; supported options: \
                         mode, partition_by"
                    )));
                }
            }
        }

        let format = self.format.ok_or_else(|| KrishivError::InvalidConfig {
            message: "writer format must be selected before save".into(),
        })?;
        let wants_sink = mode.is_some() || !partition_by.is_empty();
        match format {
            DataFormat::Parquet if wants_sink => self.dataframe.write_parquet_sink(
                path,
                mode.unwrap_or_default(),
                &partition_by,
            ),
            DataFormat::Parquet => self.dataframe.write_parquet(path),
            DataFormat::Csv | DataFormat::Json if wants_sink => {
                Err(KrishivError::unsupported(
                    "write mode / partition_by are only supported for parquet output; \
                     csv and json writes collect client-side",
                ))
            }
            DataFormat::Csv => self.dataframe.write_csv(path),
            DataFormat::Json => self.dataframe.write_json(path),
        }
    }
}
