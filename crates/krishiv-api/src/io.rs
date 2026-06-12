//! Generic DataFrame reader and writer builders.

use std::collections::BTreeMap;
use std::path::Path;

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
    options: BTreeMap<String, String>,
}

impl DataFrameWriter {
    pub(crate) fn new(dataframe: DataFrame) -> Self {
        Self {
            dataframe,
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

    pub fn save(self, path: &str) -> Result<()> {
        if !self.options.is_empty() {
            return Err(KrishivError::unsupported(format!(
                "writer options are reserved for future distributed sink configuration: {:?}",
                self.options.keys().collect::<Vec<_>>()
            )));
        }
        match self.format.ok_or_else(|| KrishivError::InvalidConfig {
            message: "writer format must be selected before save".into(),
        })? {
            DataFormat::Parquet => self.dataframe.write_parquet(path),
            DataFormat::Csv => self.dataframe.write_csv(path),
            DataFormat::Json => self.dataframe.write_json(path),
        }
    }
}
