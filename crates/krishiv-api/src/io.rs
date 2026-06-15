//! Typed, lazy reader and writer builders.

use std::collections::{BTreeMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::UInt32Array;
use arrow::compute::take;
use arrow::record_batch::RecordBatch;
use krishiv_connectors::lakehouse::{IcebergScanOptions, LakehouseTable};
pub use krishiv_connectors::{
    DatabaseIoOptions, FileFormat as DataFormat, FileLayout, KafkaIoOptions, SchemaEvolutionMode,
    SortField, WriteDistribution, WriteMode,
};

use crate::{DataFrame, Expr, KrishivError, Result, Session};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MalformedRecordPolicy {
    #[default]
    FailFast,
    DropMalformed,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ParquetReadOptions {
    pub projection: Vec<String>,
    pub filter: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvReadOptions {
    pub has_header: bool,
    pub delimiter: u8,
    pub projection: Vec<String>,
    pub filter: Option<Expr>,
    pub malformed_records: MalformedRecordPolicy,
}

impl Default for CsvReadOptions {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            projection: Vec::new(),
            filter: None,
            malformed_records: MalformedRecordPolicy::FailFast,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JsonReadOptions {
    pub projection: Vec<String>,
    pub filter: Option<Expr>,
    pub malformed_records: MalformedRecordPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileReadOptions {
    Parquet(ParquetReadOptions),
    Csv(CsvReadOptions),
    Json(JsonReadOptions),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileWriteOptions {
    pub format: DataFormat,
    pub mode: WriteMode,
    pub layout: FileLayout,
    pub schema_evolution: SchemaEvolutionMode,
}

impl FileWriteOptions {
    pub fn new(format: DataFormat) -> Self {
        Self {
            format,
            mode: WriteMode::ErrorIfExists,
            layout: FileLayout::default(),
            schema_evolution: SchemaEvolutionMode::Strict,
        }
    }
}

#[derive(Clone)]
enum ReadSource {
    Files(Vec<PathBuf>),
    Iceberg(Arc<dyn LakehouseTable>, IcebergScanOptions),
    Kafka(KafkaIoOptions),
    Database(DatabaseIoOptions),
}

#[derive(Clone)]
pub struct DataFrameReader {
    session: Session,
    source: Option<ReadSource>,
    file_options: Option<FileReadOptions>,
}

impl DataFrameReader {
    pub(crate) fn new(session: Session) -> Self {
        Self {
            session,
            source: None,
            file_options: None,
        }
    }

    pub fn parquet(mut self, options: ParquetReadOptions) -> Self {
        self.file_options = Some(FileReadOptions::Parquet(options));
        self
    }

    pub fn csv(mut self, options: CsvReadOptions) -> Self {
        self.file_options = Some(FileReadOptions::Csv(options));
        self
    }

    pub fn json(mut self, options: JsonReadOptions) -> Self {
        self.file_options = Some(FileReadOptions::Json(options));
        self
    }

    pub fn files(mut self, paths: impl IntoIterator<Item = impl Into<PathBuf>>) -> Self {
        self.source = Some(ReadSource::Files(
            paths.into_iter().map(Into::into).collect(),
        ));
        self
    }

    pub fn iceberg(mut self, table: Arc<dyn LakehouseTable>, options: IcebergScanOptions) -> Self {
        self.source = Some(ReadSource::Iceberg(table, options));
        self
    }

    pub fn kafka(mut self, options: KafkaIoOptions) -> Result<Self> {
        options.validate().map_err(connector_error)?;
        self.source = Some(ReadSource::Kafka(options));
        Ok(self)
    }

    pub fn database(mut self, options: DatabaseIoOptions) -> Result<Self> {
        options.validate().map_err(connector_error)?;
        self.source = Some(ReadSource::Database(options));
        Ok(self)
    }

    /// Compatibility selector. Prefer typed `parquet`, `csv`, or `json`.
    pub fn format(self, format: &str) -> Result<Self> {
        match format.trim().to_ascii_lowercase().as_str() {
            "parquet" => Ok(self.parquet(ParquetReadOptions::default())),
            "csv" => Ok(self.csv(CsvReadOptions::default())),
            "json" | "ndjson" => Ok(self.json(JsonReadOptions::default())),
            other => Err(KrishivError::unsupported(format!(
                "unsupported data format '{other}'"
            ))),
        }
    }

    /// Deprecated string compatibility option. Known keys are converted into
    /// typed options; unknown keys fail immediately.
    #[deprecated(note = "use typed format option structs")]
    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Result<Self> {
        let key = key.into();
        let value = value.into();
        match (&mut self.file_options, key.as_str()) {
            (Some(FileReadOptions::Csv(options)), "header") => {
                options.has_header = parse_bool(&key, &value)?;
            }
            (Some(FileReadOptions::Csv(options)), "delimiter") => {
                let bytes = value.as_bytes();
                if bytes.len() != 1 {
                    return Err(invalid("CSV delimiter must be one byte"));
                }
                options.delimiter = bytes[0];
            }
            (Some(FileReadOptions::Csv(options)), "malformedRecords" | "malformed_records") => {
                options.malformed_records = parse_malformed_policy(&key, &value)?;
            }
            (Some(FileReadOptions::Parquet(options)), "columns" | "projection") => {
                options.projection = parse_csv_list(&value);
            }
            (Some(FileReadOptions::Json(options)), "columns" | "projection") => {
                options.projection = parse_csv_list(&value);
            }
            (Some(FileReadOptions::Json(options)), "malformedRecords" | "malformed_records") => {
                options.malformed_records = parse_malformed_policy(&key, &value)?;
            }
            (None, "header" | "delimiter") => {
                let mut options = CsvReadOptions::default();
                if key == "header" {
                    options.has_header = parse_bool(&key, &value)?;
                } else {
                    let bytes = value.as_bytes();
                    if bytes.len() != 1 {
                        return Err(invalid("CSV delimiter must be one byte"));
                    }
                    options.delimiter = bytes[0];
                }
                self.file_options = Some(FileReadOptions::Csv(options));
            }
            _ => return Err(invalid(format!("unknown typed reader option '{key}'"))),
        }
        Ok(self)
    }

    pub fn load(self, path: impl AsRef<Path>) -> Result<DataFrame> {
        krishiv_common::async_util::block_on(self.load_async(path))
    }

    pub async fn load_async(mut self, path: impl AsRef<Path>) -> Result<DataFrame> {
        if self.source.is_none() {
            self.source = Some(ReadSource::Files(vec![path.as_ref().to_path_buf()]));
        }
        self.load_source_async().await
    }

    pub async fn load_source_async(mut self) -> Result<DataFrame> {
        match self
            .source
            .take()
            .ok_or_else(|| invalid("reader source is required"))?
        {
            ReadSource::Files(paths) => self.load_files_async(paths).await,
            ReadSource::Iceberg(table, options) => {
                let batches = table.scan(&options).await.map_err(lakehouse_error)?;
                Ok(self.session.dataframe_from_batches(batches))
            }
            ReadSource::Kafka(options) => Err(KrishivError::unsupported(format!(
                "bounded Kafka load for topic '{}' belongs to structured streaming Phase F",
                options.topic
            ))),
            ReadSource::Database(options) => Err(KrishivError::unsupported(format!(
                "database source '{}' requires a registered database driver",
                options.table
            ))),
        }
    }

    async fn load_files_async(self, paths: Vec<PathBuf>) -> Result<DataFrame> {
        if paths.is_empty() {
            return Err(invalid("at least one file path is required"));
        }
        let options = self
            .file_options
            .ok_or_else(|| invalid("typed file options are required"))?;
        let (projection, filter) = match &options {
            FileReadOptions::Parquet(options) => (&options.projection, &options.filter),
            FileReadOptions::Csv(options) => (&options.projection, &options.filter),
            FileReadOptions::Json(options) => (&options.projection, &options.filter),
        };
        let mut batches = Vec::new();
        for path in paths {
            let mut dataframe = match &options {
                FileReadOptions::Parquet(_) => self.session.read_parquet_async(&path).await?,
                FileReadOptions::Csv(options) => {
                    self.session
                        .read_csv_with_options_async(&path, options.has_header, options.delimiter)
                        .await?
                }
                FileReadOptions::Json(_) => self.session.read_json_async(&path).await?,
            };
            if !projection.is_empty() {
                let columns = projection.iter().map(String::as_str).collect::<Vec<_>>();
                dataframe = dataframe.select(&columns)?;
            }
            if let Some(filter) = filter {
                dataframe = dataframe.filter_expr(filter.clone())?;
            }
            batches.extend(dataframe.collect_async().await?.into_batches());
        }
        Ok(self.session.dataframe_from_batches(batches))
    }
}

#[derive(Clone)]
enum WriteTarget {
    Files(PathBuf),
    Iceberg(Arc<dyn LakehouseTable>),
    Kafka(KafkaIoOptions),
    Database(DatabaseIoOptions),
}

#[derive(Clone)]
pub struct DataFrameWriter {
    dataframe: DataFrame,
    target: Option<WriteTarget>,
    options: Option<FileWriteOptions>,
}

impl DataFrameWriter {
    pub(crate) fn new(dataframe: DataFrame) -> Self {
        Self {
            dataframe,
            target: None,
            options: None,
        }
    }

    pub fn file_options(mut self, options: FileWriteOptions) -> Result<Self> {
        options.layout.validate().map_err(connector_error)?;
        self.options = Some(options);
        Ok(self)
    }

    pub fn mode(mut self, mode: WriteMode) -> Self {
        self.options
            .get_or_insert_with(|| FileWriteOptions::new(DataFormat::Parquet))
            .mode = mode;
        self
    }

    pub fn layout(mut self, layout: FileLayout) -> Result<Self> {
        layout.validate().map_err(connector_error)?;
        self.options
            .get_or_insert_with(|| FileWriteOptions::new(DataFormat::Parquet))
            .layout = layout;
        Ok(self)
    }

    pub fn schema_evolution(mut self, mode: SchemaEvolutionMode) -> Self {
        self.options
            .get_or_insert_with(|| FileWriteOptions::new(DataFormat::Parquet))
            .schema_evolution = mode;
        self
    }

    pub fn iceberg(mut self, table: Arc<dyn LakehouseTable>) -> Self {
        self.target = Some(WriteTarget::Iceberg(table));
        self
    }

    pub fn kafka(mut self, options: KafkaIoOptions) -> Result<Self> {
        options.validate().map_err(connector_error)?;
        self.target = Some(WriteTarget::Kafka(options));
        Ok(self)
    }

    pub fn database(mut self, options: DatabaseIoOptions) -> Result<Self> {
        options.validate().map_err(connector_error)?;
        self.target = Some(WriteTarget::Database(options));
        Ok(self)
    }

    pub fn format(mut self, format: &str) -> Result<Self> {
        let format = match format.trim().to_ascii_lowercase().as_str() {
            "parquet" => DataFormat::Parquet,
            "csv" => DataFormat::Csv,
            "json" | "ndjson" => DataFormat::Json,
            other => {
                return Err(KrishivError::unsupported(format!(
                    "unsupported data format '{other}'"
                )));
            }
        };
        self.options = Some(FileWriteOptions::new(format));
        Ok(self)
    }

    #[deprecated(note = "use typed FileWriteOptions")]
    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Result<Self> {
        let key = key.into();
        let value = value.into();
        let options = self
            .options
            .get_or_insert_with(|| FileWriteOptions::new(DataFormat::Parquet));
        match key.as_str() {
            "mode" => {
                options.mode = parse_write_mode(&key, &value)?;
            }
            "partitionBy" | "partition_by" => {
                options.layout.partition_by = parse_csv_list(&value);
            }
            "format" => {
                options.format = match value.trim().to_ascii_lowercase().as_str() {
                    "parquet" => DataFormat::Parquet,
                    "csv" => DataFormat::Csv,
                    "json" | "ndjson" => DataFormat::Json,
                    other => {
                        return Err(invalid(format!("unsupported data format '{other}'")));
                    }
                };
            }
            "maxRecordsPerFile" | "max_records_per_file" => {
                options.layout.max_rows_per_file = Some(
                    value
                        .parse()
                        .map_err(|_| invalid(format!("option '{key}' must be a positive integer")))?,
                );
            }
            "targetFileSize" | "target_file_size_bytes" => {
                options.layout.target_file_size_bytes = Some(
                    value
                        .parse()
                        .map_err(|_| invalid(format!("option '{key}' must be a positive integer")))?,
                );
            }
            _ => {
                return Err(invalid(format!(
                    "unknown generic writer option '{key}'; use FileWriteOptions"
                )));
            }
        }
        Ok(self)
    }

    pub fn save(self, path: &str) -> Result<()> {
        krishiv_common::async_util::block_on(self.save_async(path))
    }

    pub async fn save_async(mut self, path: impl AsRef<Path>) -> Result<()> {
        if self.target.is_none() {
            self.target = Some(WriteTarget::Files(path.as_ref().to_path_buf()));
        }
        self.save_target_async().await
    }

    pub async fn save_target_async(mut self) -> Result<()> {
        match self
            .target
            .take()
            .ok_or_else(|| invalid("writer target is required"))?
        {
            WriteTarget::Files(path) => self.save_files_async(path).await,
            WriteTarget::Iceberg(table) => {
                let batches = self.dataframe.collect_async().await?.into_batches();
                match self
                    .options
                    .as_ref()
                    .map(|options| options.mode)
                    .unwrap_or(WriteMode::Append)
                {
                    WriteMode::Append => table.append(batches).await.map_err(lakehouse_error),
                    WriteMode::ErrorIfExists => {
                        if table
                            .current_snapshot_id()
                            .await
                            .map_err(lakehouse_error)?
                            .is_some()
                        {
                            Err(invalid(format!(
                                "Iceberg table '{}' already exists",
                                table.table_ref().full_name()
                            )))
                        } else {
                            table.append(batches).await.map_err(lakehouse_error)
                        }
                    }
                    WriteMode::Overwrite | WriteMode::DynamicOverwrite => {
                        table.overwrite(batches).await.map_err(lakehouse_error)
                    }
                    WriteMode::Ignore => {
                        if table
                            .current_snapshot_id()
                            .await
                            .map_err(lakehouse_error)?
                            .is_none()
                        {
                            table.append(batches).await.map_err(lakehouse_error)?;
                        }
                        Ok(())
                    }
                }
            }
            WriteTarget::Kafka(options) => Err(KrishivError::unsupported(format!(
                "bounded Kafka write for topic '{}' belongs to structured streaming Phase F",
                options.topic
            ))),
            WriteTarget::Database(options) => Err(KrishivError::unsupported(format!(
                "database sink '{}' requires a registered database driver",
                options.table
            ))),
        }
    }

    async fn save_files_async(self, path: PathBuf) -> Result<()> {
        let options = self
            .options
            .ok_or_else(|| invalid("typed file write options are required"))?;
        options.layout.validate().map_err(connector_error)?;
        let dataframe = apply_sort(self.dataframe, &options.layout)?;

        if options.format == DataFormat::Parquet {
            if let Some(sink_mode) = to_sink_write_mode(options.mode) {
                let partition_by = options.layout.partition_by.clone();
                let path_str = path.to_string_lossy().into_owned();
                if dataframe
                    .try_distributed_parquet_sink(&path_str, sink_mode, &partition_by)?
                    .is_some()
                {
                    return Ok(());
                }
            }
        }

        let batches = dataframe.collect_async().await?.into_batches();
        tokio::task::spawn_blocking(move || write_files(path, batches, options))
            .await
            .map_err(|error| KrishivError::Runtime {
                message: format!("file writer task failed: {error}"),
            })?
    }
}

fn apply_sort(mut dataframe: DataFrame, layout: &FileLayout) -> Result<DataFrame> {
    if !layout.sort_by.is_empty() {
        let columns = layout
            .sort_by
            .iter()
            .map(|field| field.column.as_str())
            .collect::<Vec<_>>();
        let descending = layout
            .sort_by
            .iter()
            .map(|field| {
                matches!(
                    field.direction,
                    krishiv_connectors::FileSortDirection::Descending
                )
            })
            .collect::<Vec<_>>();
        dataframe = dataframe.sort(&columns, &descending)?;
    }
    Ok(dataframe)
}

fn write_files(path: PathBuf, batches: Vec<RecordBatch>, options: FileWriteOptions) -> Result<()> {
    prepare_target(&path, options.mode)?;
    if options.mode == WriteMode::Ignore && path.exists() {
        return Ok(());
    }
    let groups = distribute_batches(batches, &options.layout)?;
    let directory_layout = groups.len() > 1
        || !options.layout.partition_by.is_empty()
        || options.layout.max_rows_per_file.is_some()
        || options.layout.target_file_size_bytes.is_some()
        || matches!(
            options.mode,
            WriteMode::Append | WriteMode::DynamicOverwrite
        );
    if directory_layout {
        std::fs::create_dir_all(&path).map_err(io_error)?;
        for (group, batches) in groups {
            let directory = if group.is_empty() {
                path.clone()
            } else {
                path.join(group)
            };
            if options.mode == WriteMode::DynamicOverwrite
                && directory.exists()
                && directory != path
            {
                std::fs::remove_dir_all(&directory).map_err(io_error)?;
            }
            std::fs::create_dir_all(&directory).map_err(io_error)?;
            write_group_files(&directory, &batches, &options)?;
        }
    } else if let Some((_, batches)) = groups.into_iter().next() {
        write_atomic_file(&path, &batches, options.format)?;
    }
    Ok(())
}

fn prepare_target(path: &Path, mode: WriteMode) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    match mode {
        WriteMode::ErrorIfExists => Err(invalid(format!(
            "target '{}' already exists",
            path.display()
        ))),
        WriteMode::Ignore | WriteMode::Append | WriteMode::DynamicOverwrite => Ok(()),
        WriteMode::Overwrite => {
            if path.is_dir() {
                std::fs::remove_dir_all(path).map_err(io_error)?;
            } else {
                std::fs::remove_file(path).map_err(io_error)?;
            }
            Ok(())
        }
    }
}

fn distribute_batches(
    batches: Vec<RecordBatch>,
    layout: &FileLayout,
) -> Result<BTreeMap<String, Vec<RecordBatch>>> {
    let mut groups: BTreeMap<String, Vec<RecordBatch>> = BTreeMap::new();
    for batch in batches {
        if layout.partition_by.is_empty()
            && !matches!(layout.distribution, WriteDistribution::Hash { .. })
        {
            groups.entry(String::new()).or_default().push(batch);
            continue;
        }
        let mut indices: BTreeMap<String, Vec<u32>> = BTreeMap::new();
        for row in 0..batch.num_rows() {
            let mut parts = Vec::new();
            for column in &layout.partition_by {
                let index = batch
                    .schema()
                    .index_of(column)
                    .map_err(|_| invalid(format!("unknown partition column '{column}'")))?;
                let value = arrow::util::display::array_value_to_string(batch.column(index), row)
                    .map_err(|error| invalid(error.to_string()))?;
                parts.push(format!(
                    "{}={}",
                    sanitize_path(column),
                    sanitize_path(&value)
                ));
            }
            if let WriteDistribution::Hash {
                columns,
                partitions,
            } = &layout.distribution
            {
                let mut hasher = DefaultHasher::new();
                for column in columns {
                    let index = batch
                        .schema()
                        .index_of(column)
                        .map_err(|_| invalid(format!("unknown distribution column '{column}'")))?;
                    arrow::util::display::array_value_to_string(batch.column(index), row)
                        .map_err(|error| invalid(error.to_string()))?
                        .hash(&mut hasher);
                }
                parts.push(format!("bucket={}", hasher.finish() % *partitions as u64));
            }
            indices.entry(parts.join("/")).or_default().push(row as u32);
        }
        for (key, rows) in indices {
            let indices = UInt32Array::from(rows);
            let columns = batch
                .columns()
                .iter()
                .map(|column| {
                    take(column.as_ref(), &indices, None)
                        .map_err(|error| invalid(error.to_string()))
                })
                .collect::<Result<Vec<_>>>()?;
            groups.entry(key).or_default().push(
                RecordBatch::try_new(batch.schema(), columns)
                    .map_err(|error| invalid(error.to_string()))?,
            );
        }
    }
    Ok(groups)
}

fn write_group_files(
    directory: &Path,
    batches: &[RecordBatch],
    options: &FileWriteOptions,
) -> Result<()> {
    let max_rows = effective_max_rows(batches, &options.layout);
    let mut parts = Vec::new();
    for batch in batches {
        if let Some(max_rows) = max_rows {
            let mut offset = 0;
            while offset < batch.num_rows() {
                let length = max_rows.min(batch.num_rows() - offset);
                parts.push(batch.slice(offset, length));
                offset += length;
            }
        } else {
            parts.push(batch.clone());
        }
    }
    let start = std::fs::read_dir(directory).map_err(io_error)?.count();
    for (index, batch) in parts.iter().enumerate() {
        let path = directory.join(format!(
            "part-{:05}.{}",
            start + index,
            extension(options.format)
        ));
        write_atomic_file(&path, std::slice::from_ref(batch), options.format)?;
    }
    Ok(())
}

fn effective_max_rows(batches: &[RecordBatch], layout: &FileLayout) -> Option<usize> {
    let by_size = layout.target_file_size_bytes.and_then(|target| {
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
        (rows > 0).then(|| ((target as usize * rows) / bytes.max(1)).max(1))
    });
    match (layout.max_rows_per_file, by_size) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    }
}

fn write_atomic_file(path: &Path, batches: &[RecordBatch], format: DataFormat) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(io_error)?;
    }
    let temporary = path.with_extension(format!("{}.tmp", extension(format)));
    let file = std::fs::File::create(&temporary).map_err(io_error)?;
    match format {
        DataFormat::Parquet => {
            if let Some(first) = batches.first() {
                let mut writer = parquet::arrow::ArrowWriter::try_new(file, first.schema(), None)
                    .map_err(|error| invalid(error.to_string()))?;
                for batch in batches {
                    writer
                        .write(batch)
                        .map_err(|error| invalid(error.to_string()))?;
                }
                writer.close().map_err(|error| invalid(error.to_string()))?;
            }
        }
        DataFormat::Csv => {
            let mut writer = arrow::csv::Writer::new(file);
            for batch in batches {
                writer
                    .write(batch)
                    .map_err(|error| invalid(error.to_string()))?;
            }
        }
        DataFormat::Json => {
            let mut writer = arrow::json::LineDelimitedWriter::new(file);
            for batch in batches {
                writer
                    .write(batch)
                    .map_err(|error| invalid(error.to_string()))?;
            }
            writer
                .finish()
                .map_err(|error| invalid(error.to_string()))?;
        }
    }
    std::fs::rename(&temporary, path).map_err(io_error)
}

fn extension(format: DataFormat) -> &'static str {
    match format {
        DataFormat::Parquet => "parquet",
        DataFormat::Csv => "csv",
        DataFormat::Json => "json",
    }
}

fn sanitize_path(value: &str) -> String {
    value.replace(['/', '\\', '='], "_")
}

fn parse_bool(key: &str, value: &str) -> Result<bool> {
    value
        .parse()
        .map_err(|_| invalid(format!("option '{key}' must be true or false")))
}

fn parse_csv_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect()
}

fn parse_malformed_policy(key: &str, value: &str) -> Result<MalformedRecordPolicy> {
    match value.trim().to_ascii_lowercase().as_str() {
        "failfast" | "fail_fast" | "fail" => Ok(MalformedRecordPolicy::FailFast),
        "drop" | "dropmalformed" | "drop_malformed" => Ok(MalformedRecordPolicy::DropMalformed),
        other => Err(invalid(format!(
            "option '{key}' must be failfast or drop; got '{other}'"
        ))),
    }
}

fn parse_write_mode(key: &str, value: &str) -> Result<WriteMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "append" => Ok(WriteMode::Append),
        "overwrite" => Ok(WriteMode::Overwrite),
        "errorifexists" | "error_if_exists" | "error-if-exists" => Ok(WriteMode::ErrorIfExists),
        "ignore" => Ok(WriteMode::Ignore),
        "dynamicoverwrite" | "dynamic_overwrite" | "dynamic-overwrite" => {
            Ok(WriteMode::DynamicOverwrite)
        }
        other => Err(invalid(format!("unknown write mode '{other}' for option '{key}'"))),
    }
}

fn to_sink_write_mode(mode: WriteMode) -> Option<krishiv_common::write_commit::WriteMode> {
    use krishiv_common::write_commit::WriteMode as SinkMode;
    match mode {
        WriteMode::Append => Some(SinkMode::Append),
        WriteMode::Overwrite => Some(SinkMode::Overwrite),
        WriteMode::ErrorIfExists => Some(SinkMode::ErrorIfExists),
        WriteMode::Ignore => Some(SinkMode::Ignore),
        WriteMode::DynamicOverwrite => None,
    }
}

fn connector_error(error: krishiv_connectors::ConnectorError) -> KrishivError {
    invalid(error.to_string())
}

fn lakehouse_error(error: krishiv_connectors::lakehouse::LakehouseError) -> KrishivError {
    KrishivError::Runtime {
        message: error.to_string(),
    }
}

fn io_error(error: std::io::Error) -> KrishivError {
    KrishivError::Runtime {
        message: error.to_string(),
    }
}

fn invalid(message: impl Into<String>) -> KrishivError {
    KrishivError::InvalidConfig {
        message: message.into(),
    }
}
