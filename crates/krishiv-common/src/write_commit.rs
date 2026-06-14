//! Distributed write commit protocol (Phase 2.3).
//!
//! Sink tasks never write directly to their final destination. Instead they
//! stage output under `<dest>/_staging/<job_id>/` and the coordinator (or the
//! client that drove the job, for direct-runner tests) publishes the staged
//! files into the destination once the whole job has succeeded:
//!
//! ```text
//! staged : <base_dir>/<dest>/_staging/<job_id>/[<hive_path>/]<task_id>-<attempt>.parquet
//! final  : <base_dir>/<dest>/[<hive_path>/]part-<task_index>-<job_id>.parquet
//! ```
//!
//! Publication renames staged files into place (falling back to copy+delete
//! when the underlying store cannot rename across boundaries) and then removes
//! the staging directory. Both publish and cleanup are idempotent so a crashed
//! or retried publish converges: files already present at their final name are
//! skipped, already-removed staging directories are tolerated.
//!
//! The sink contract string format is an extension of the legacy
//! `object-parquet-sink:<base_dir>:<object_path>` payload:
//!
//! ```text
//! <base_dir>:<dest_dir>[:mode=<append|overwrite|error_if_exists|ignore>][:partition_by=col1,col2]
//! ```
//!
//! A payload without any trailing token keeps the legacy semantics (direct
//! single-object write, no staging) for backwards compatibility. A payload
//! with at least one token engages the staged commit protocol; a missing
//! `mode=` token defaults to [`WriteMode::Append`].

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use arrow::array::{Array, StringArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

/// Directory name used for staged (uncommitted) sink output.
pub const SINK_STAGING_DIR_NAME: &str = "_staging";

/// Hive convention for null partition values.
pub const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// Errors raised by the write commit protocol.
#[derive(Debug, thiserror::Error)]
pub enum WriteCommitError {
    #[error("invalid sink contract: {message}")]
    InvalidContract { message: String },
    #[error("destination '{dest}' is not empty and write mode is error_if_exists")]
    DestinationNotEmpty { dest: String },
    #[error("partition column '{column}' not found in sink output schema")]
    MissingPartitionColumn { column: String },
    #[error("failed to partition sink output: {message}")]
    PartitionSplit { message: String },
    #[error("staged file name '{name}' is not a valid <task_id>-<attempt>.parquet name")]
    InvalidStagedFileName { name: String },
    #[error(
        "staged tasks '{first}' and '{second}' both map to final part index {index}; \
         sink task ids must carry distinct numeric suffixes"
    )]
    FinalNameCollision {
        first: String,
        second: String,
        index: usize,
    },
    #[error("write commit io error at '{path}': {source}")]
    Io {
        path: String,
        #[source]
        source: io::Error,
    },
}

impl WriteCommitError {
    fn io(path: &Path, source: io::Error) -> Self {
        Self::Io {
            path: path.display().to_string(),
            source,
        }
    }
}

/// Result of a publish pass.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PublishOutcome {
    /// Final destination paths published in this pass.
    pub published: Vec<PathBuf>,
    /// Staged groups whose final file already existed (idempotent re-publish).
    pub skipped_existing: usize,
    /// True when `WriteMode::Ignore` suppressed publication because the
    /// destination already contained data.
    pub ignored: bool,
}

/// Write disposition applied when staged sink output is published.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteMode {
    /// Add new part files next to whatever already exists (default).
    #[default]
    Append,
    /// Remove pre-existing foreign files in the destination before publishing.
    Overwrite,
    /// Fail publication if the destination already contains foreign files.
    ErrorIfExists,
    /// Silently skip publication if the destination already contains foreign files.
    Ignore,
}

impl WriteMode {
    /// Parse a mode token (case-insensitive; accepts `errorifexists` and
    /// `error_if_exists` spellings).
    pub fn parse(token: &str) -> Result<Self, WriteCommitError> {
        match token.trim().to_ascii_lowercase().as_str() {
            "append" => Ok(Self::Append),
            "overwrite" => Ok(Self::Overwrite),
            "error_if_exists" | "errorifexists" | "error-if-exists" => Ok(Self::ErrorIfExists),
            "ignore" => Ok(Self::Ignore),
            other => Err(WriteCommitError::InvalidContract {
                message: format!(
                    "unknown write mode '{other}'; expected append, overwrite, \
                     error_if_exists, or ignore"
                ),
            }),
        }
    }

    /// Canonical token used in sink contract strings.
    pub fn as_token(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Overwrite => "overwrite",
            Self::ErrorIfExists => "error_if_exists",
            Self::Ignore => "ignore",
        }
    }
}

impl fmt::Display for WriteMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_token())
    }
}

/// Parsed `object-parquet-sink` contract payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkWriteSpec {
    /// Local object-store root (the `LocalFileSystem::new_with_prefix` prefix).
    pub base_dir: String,
    /// Destination path relative to `base_dir`. A directory for staged writes;
    /// a single object path for legacy direct writes.
    pub dest_path: String,
    /// Publish-time write disposition.
    pub mode: WriteMode,
    /// Hive partition columns (empty = unpartitioned).
    pub partition_by: Vec<String>,
    /// True when the staged commit protocol applies. False only for legacy
    /// token-less `<base_dir>:<object_path>` payloads which keep direct-write
    /// semantics.
    pub staged: bool,
}

impl SinkWriteSpec {
    /// Build a staged sink spec (the form emitted by the DataFrame write API).
    pub fn staged(
        base_dir: impl Into<String>,
        dest_path: impl Into<String>,
        mode: WriteMode,
        partition_by: Vec<String>,
    ) -> Result<Self, WriteCommitError> {
        let spec = Self {
            base_dir: base_dir.into(),
            dest_path: dest_path.into(),
            mode,
            partition_by,
            staged: true,
        };
        spec.validate()?;
        // The contract payload separates base_dir from the destination with
        // ':' and only the destination may legally contain further colons.
        if spec.base_dir.contains(':') {
            return Err(WriteCommitError::InvalidContract {
                message: format!(
                    "sink base directory '{}' must not contain ':'",
                    spec.base_dir
                ),
            });
        }
        Ok(spec)
    }

    fn validate(&self) -> Result<(), WriteCommitError> {
        if self.base_dir.trim().is_empty() || self.dest_path.trim().is_empty() {
            return Err(WriteCommitError::InvalidContract {
                message: String::from("sink base_dir and destination path cannot be empty"),
            });
        }
        for column in &self.partition_by {
            if column.trim().is_empty() {
                return Err(WriteCommitError::InvalidContract {
                    message: String::from("partition_by columns cannot be empty"),
                });
            }
        }
        Ok(())
    }

    /// Parse a sink contract payload (the part after `object-parquet-sink:`).
    ///
    /// Recognized trailing tokens: `mode=<m>` and `partition_by=a,b`.
    /// Tokens may appear in any order at the tail of the payload. A payload
    /// without tokens parses as a legacy direct-write spec (`staged == false`,
    /// mode defaults to Append).
    pub fn parse(payload: &str) -> Result<Self, WriteCommitError> {
        let payload = payload.trim();
        let mut segments: Vec<&str> = payload.split(':').collect();

        let mut mode: Option<WriteMode> = None;
        let mut partition_by: Option<Vec<String>> = None;
        let mut staged = false;
        while let Some(last) = segments.last() {
            let last = last.trim();
            if let Some(value) = last.strip_prefix("mode=") {
                if mode.is_some() {
                    return Err(WriteCommitError::InvalidContract {
                        message: String::from("duplicate mode= token in sink contract"),
                    });
                }
                mode = Some(WriteMode::parse(value)?);
                staged = true;
                segments.pop();
            } else if let Some(value) = last.strip_prefix("partition_by=") {
                if partition_by.is_some() {
                    return Err(WriteCommitError::InvalidContract {
                        message: String::from("duplicate partition_by= token in sink contract"),
                    });
                }
                let columns: Vec<String> = value
                    .split(',')
                    .map(str::trim)
                    .filter(|c| !c.is_empty())
                    .map(str::to_owned)
                    .collect();
                if columns.is_empty() {
                    return Err(WriteCommitError::InvalidContract {
                        message: String::from("partition_by= token must list at least one column"),
                    });
                }
                partition_by = Some(columns);
                staged = true;
                segments.pop();
            } else {
                break;
            }
        }

        let head = segments.join(":");
        let (base_dir, dest_path) =
            head.split_once(':')
                .ok_or_else(|| WriteCommitError::InvalidContract {
                    message: format!(
                        "sink contract payload must be \
                         '<base_dir>:<dest>[:mode=...][:partition_by=...]', got '{payload}'"
                    ),
                })?;
        let spec = Self {
            base_dir: base_dir.trim().to_owned(),
            dest_path: dest_path.trim().to_owned(),
            mode: mode.unwrap_or_default(),
            partition_by: partition_by.unwrap_or_default(),
            staged,
        };
        spec.validate()?;
        Ok(spec)
    }

    /// Render the contract payload (inverse of [`Self::parse`] for staged specs).
    pub fn contract_payload(&self) -> String {
        let mut payload = format!("{}:{}", self.base_dir, self.dest_path);
        if self.staged {
            payload.push_str(":mode=");
            payload.push_str(self.mode.as_token());
            if !self.partition_by.is_empty() {
                payload.push_str(":partition_by=");
                payload.push_str(&self.partition_by.join(","));
            }
        }
        payload
    }

    /// Staging directory for `job_id`, relative to `base_dir`.
    pub fn staging_dir_rel(&self, job_id: &str) -> String {
        format!("{}/{SINK_STAGING_DIR_NAME}/{job_id}", self.dest_path)
    }

    /// Relative staged file path for one task attempt and partition directory.
    ///
    /// `hive_path` is empty for unpartitioned writes. Re-running the same task
    /// attempt produces the same path, so retries overwrite their own staging
    /// file instead of duplicating output.
    pub fn staged_file_rel(
        &self,
        job_id: &str,
        hive_path: &str,
        task_id: &str,
        attempt: u32,
    ) -> String {
        let name = staged_file_name(task_id, attempt);
        if hive_path.is_empty() {
            format!("{}/{name}", self.staging_dir_rel(job_id))
        } else {
            format!("{}/{hive_path}/{name}", self.staging_dir_rel(job_id))
        }
    }
}

/// Deterministic staged file name: `<task_id>-<attempt>.parquet`.
pub fn staged_file_name(task_id: &str, attempt: u32) -> String {
    format!("{task_id}-{attempt}.parquet")
}

/// Deterministic final part file name: `part-<task_index>-<job_id>.parquet`.
pub fn final_part_file_name(task_index: usize, job_id: &str) -> String {
    format!("part-{task_index}-{job_id}.parquet")
}

/// Parse a staged file name back into `(task_id, attempt)`.
fn parse_staged_file_name(name: &str) -> Result<(String, u32), WriteCommitError> {
    let stem =
        name.strip_suffix(".parquet")
            .ok_or_else(|| WriteCommitError::InvalidStagedFileName {
                name: name.to_owned(),
            })?;
    let (task, attempt) =
        stem.rsplit_once('-')
            .ok_or_else(|| WriteCommitError::InvalidStagedFileName {
                name: name.to_owned(),
            })?;
    let attempt = attempt
        .parse::<u32>()
        .map_err(|_| WriteCommitError::InvalidStagedFileName {
            name: name.to_owned(),
        })?;
    if task.is_empty() {
        return Err(WriteCommitError::InvalidStagedFileName {
            name: name.to_owned(),
        });
    }
    Ok((task.to_owned(), attempt))
}

/// Derive the final part index from a task id.
///
/// Sink tasks are named `task-<index>` by the job submitters; the trailing
/// decimal run is the part index. Task ids without a numeric suffix map to
/// index 0 (valid only for single-task sink stages — the publisher errors on
/// collisions).
fn task_index_from_task_id(task_id: &str) -> usize {
    let digits: String = task_id
        .chars()
        .rev()
        .take_while(char::is_ascii_digit)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    digits.parse::<usize>().unwrap_or(0)
}

/// Hive-escape a partition value for use as a path component.
///
/// Keeps `[A-Za-z0-9._-]` and percent-encodes everything else byte-wise, so
/// values containing `/`, `=`, `:` or non-ASCII text cannot escape the
/// partition directory.
pub fn hive_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        let c = byte as char;
        if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
            out.push(c);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// One Hive partition slice of the sink output.
#[derive(Debug, Clone)]
pub struct HivePartitionSlice {
    /// Relative directory like `country=US/year=2024` (empty for unpartitioned).
    pub hive_path: String,
    /// Record batches whose rows all share this partition value combination.
    pub batches: Vec<RecordBatch>,
}

/// Split record batches by the distinct values of `columns` (Hive layout).
///
/// Values are string-formatted by casting each partition column to Utf8;
/// nulls map to [`HIVE_DEFAULT_PARTITION`]. Partition columns are retained in
/// the data files (lossless). With empty `columns` the input is returned as a
/// single slice with an empty `hive_path`.
pub fn split_batches_by_partition_columns(
    batches: &[RecordBatch],
    columns: &[String],
) -> Result<Vec<HivePartitionSlice>, WriteCommitError> {
    if columns.is_empty() {
        return Ok(vec![HivePartitionSlice {
            hive_path: String::new(),
            batches: batches.to_vec(),
        }]);
    }

    // BTreeMap keeps deterministic partition ordering for stable output.
    let mut slices: BTreeMap<String, Vec<RecordBatch>> = BTreeMap::new();

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        // Cast each partition column to Utf8 once per batch for formatting.
        let mut formatted: Vec<StringArray> = Vec::with_capacity(columns.len());
        for column in columns {
            let index = batch.schema().index_of(column).map_err(|_| {
                WriteCommitError::MissingPartitionColumn {
                    column: column.clone(),
                }
            })?;
            let array = batch.column(index);
            let utf8 = if array.data_type() == &DataType::Utf8 {
                array.clone()
            } else {
                arrow::compute::cast(array, &DataType::Utf8).map_err(|e| {
                    WriteCommitError::PartitionSplit {
                        message: format!("cast partition column '{column}' to utf8: {e}"),
                    }
                })?
            };
            let strings = utf8
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| WriteCommitError::PartitionSplit {
                    message: format!("partition column '{column}' did not cast to StringArray"),
                })?
                .clone();
            formatted.push(strings);
        }

        // Group row indices by their hive path.
        let mut row_groups: BTreeMap<String, Vec<bool>> = BTreeMap::new();
        for row in 0..batch.num_rows() {
            let mut parts: Vec<String> = Vec::with_capacity(columns.len());
            for (column, values) in columns.iter().zip(&formatted) {
                let value = if values.is_valid(row) {
                    hive_escape(values.value(row))
                } else {
                    HIVE_DEFAULT_PARTITION.to_owned()
                };
                parts.push(format!("{column}={value}"));
            }
            let key = parts.join("/");
            let mask = row_groups
                .entry(key)
                .or_insert_with(|| vec![false; batch.num_rows()]);
            mask[row] = true;
        }

        for (key, mask) in row_groups {
            let predicate = arrow::array::BooleanArray::from(mask);
            let filtered = filter_record_batch(batch, &predicate).map_err(|e| {
                WriteCommitError::PartitionSplit {
                    message: format!("filter rows for partition '{key}': {e}"),
                }
            })?;
            if filtered.num_rows() > 0 {
                slices.entry(key).or_default().push(filtered);
            }
        }
    }

    Ok(slices
        .into_iter()
        .map(|(hive_path, batches)| HivePartitionSlice { hive_path, batches })
        .collect())
}

/// One staged file discovered under the job's staging directory.
#[derive(Debug, Clone)]
struct StagedEntry {
    /// Partition directory relative to the destination (empty if none).
    hive_path: String,
    task_id: String,
    attempt: u32,
    /// Absolute path of the staged file.
    path: PathBuf,
}

fn collect_staged_entries(
    staging_root: &Path,
    rel: &str,
    out: &mut Vec<StagedEntry>,
) -> Result<(), WriteCommitError> {
    let dir = if rel.is_empty() {
        staging_root.to_path_buf()
    } else {
        staging_root.join(rel)
    };
    let entries = std::fs::read_dir(&dir).map_err(|e| WriteCommitError::io(&dir, e))?;
    for entry in entries {
        let entry = entry.map_err(|e| WriteCommitError::io(&dir, e))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|e| WriteCommitError::io(&path, e))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if file_type.is_dir() {
            let child_rel = if rel.is_empty() {
                name
            } else {
                format!("{rel}/{name}")
            };
            collect_staged_entries(staging_root, &child_rel, out)?;
        } else {
            let (task_id, attempt) = parse_staged_file_name(&name)?;
            out.push(StagedEntry {
                hive_path: rel.to_owned(),
                task_id,
                attempt,
                path,
            });
        }
    }
    Ok(())
}

/// True if `name` is one of this job's published part files.
fn is_own_part_file(name: &str, job_id: &str) -> bool {
    name.starts_with("part-") && name.ends_with(&format!("-{job_id}.parquet"))
}

/// Recursively list destination entries that do not belong to this job's
/// publish (`_staging` is excluded). Files are returned; directories
/// containing only our part files are not considered foreign.
fn foreign_entries(
    dest_dir: &Path,
    job_id: &str,
    rel: &str,
    out: &mut Vec<PathBuf>,
) -> Result<(), WriteCommitError> {
    let dir = if rel.is_empty() {
        dest_dir.to_path_buf()
    } else {
        dest_dir.join(rel)
    };
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(WriteCommitError::io(&dir, e)),
    };
    for entry in entries {
        let entry = entry.map_err(|e| WriteCommitError::io(&dir, e))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if rel.is_empty() && name == SINK_STAGING_DIR_NAME {
            continue;
        }
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|e| WriteCommitError::io(&path, e))?;
        if file_type.is_dir() {
            let child_rel = if rel.is_empty() {
                name
            } else {
                format!("{rel}/{name}")
            };
            foreign_entries(dest_dir, job_id, &child_rel, out)?;
        } else if !is_own_part_file(&name, job_id) {
            out.push(path);
        }
    }
    Ok(())
}

/// Move a staged file to its final destination, falling back to copy+delete
/// when rename is not supported (e.g. cross-device moves; object stores
/// without native rename use the same copy-then-delete semantics).
fn move_file(from: &Path, to: &Path) -> Result<(), WriteCommitError> {
    if let Some(parent) = to.parent() {
        std::fs::create_dir_all(parent).map_err(|e| WriteCommitError::io(parent, e))?;
    }
    match std::fs::rename(from, to) {
        Ok(()) => Ok(()),
        Err(_) => {
            std::fs::copy(from, to).map_err(|e| WriteCommitError::io(to, e))?;
            std::fs::remove_file(from).map_err(|e| WriteCommitError::io(from, e))?;
            Ok(())
        }
    }
}

/// Remove `path` recursively, tolerating an already-missing directory.
fn remove_dir_all_tolerant(path: &Path) -> Result<(), WriteCommitError> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(WriteCommitError::io(path, e)),
    }
}

/// Publish all staged outputs for `job_id` into the destination directory.
///
/// Idempotent and convergent:
/// - a missing staging directory is a no-op (the publish already completed);
/// - final files that already exist are skipped;
/// - when multiple attempts of the same task staged files, only the highest
///   attempt is published;
/// - mode enforcement ignores this job's own part files so a re-publish after
///   a partial publish never errors (`ErrorIfExists`), skips (`Ignore`), or
///   deletes its own output (`Overwrite`).
///
/// On success the staging directory for the job is removed.
pub fn publish_staged_outputs(
    spec: &SinkWriteSpec,
    job_id: &str,
) -> Result<PublishOutcome, WriteCommitError> {
    let dest_dir = Path::new(&spec.base_dir).join(&spec.dest_path);
    let staging_dir = Path::new(&spec.base_dir).join(spec.staging_dir_rel(job_id));

    if !staging_dir.exists() {
        // Nothing staged: either the job produced no rows or a previous
        // publish already completed and removed the staging directory.
        return Ok(PublishOutcome::default());
    }

    // Mode enforcement against foreign (not-our-own) destination content.
    let mut foreign = Vec::new();
    foreign_entries(&dest_dir, job_id, "", &mut foreign)?;
    match spec.mode {
        WriteMode::Append => {}
        WriteMode::ErrorIfExists => {
            if !foreign.is_empty() {
                return Err(WriteCommitError::DestinationNotEmpty {
                    dest: dest_dir.display().to_string(),
                });
            }
        }
        WriteMode::Ignore => {
            if !foreign.is_empty() {
                cleanup_staged_outputs(spec, job_id)?;
                return Ok(PublishOutcome {
                    published: Vec::new(),
                    skipped_existing: 0,
                    ignored: true,
                });
            }
        }
        WriteMode::Overwrite => {
            for path in &foreign {
                std::fs::remove_file(path).map_err(|e| WriteCommitError::io(path, e))?;
            }
        }
    }

    // Collect staged files and keep only the highest attempt per task+partition.
    let mut entries = Vec::new();
    collect_staged_entries(&staging_dir, "", &mut entries)?;
    let mut winners: BTreeMap<(String, String), StagedEntry> = BTreeMap::new();
    for entry in entries {
        let key = (entry.hive_path.clone(), entry.task_id.clone());
        match winners.get(&key) {
            Some(existing) if existing.attempt >= entry.attempt => {}
            _ => {
                winners.insert(key, entry);
            }
        }
    }

    // Detect final-name collisions between distinct task ids up front.
    let mut index_owner: BTreeMap<(String, usize), String> = BTreeMap::new();
    for (hive_path, task_id) in winners.keys() {
        let index = task_index_from_task_id(task_id);
        if let Some(owner) = index_owner.get(&(hive_path.clone(), index))
            && owner != task_id
        {
            return Err(WriteCommitError::FinalNameCollision {
                first: owner.clone(),
                second: task_id.clone(),
                index,
            });
        }
        index_owner.insert((hive_path.clone(), index), task_id.clone());
    }

    let mut outcome = PublishOutcome::default();
    for ((hive_path, task_id), entry) in winners {
        let index = task_index_from_task_id(&task_id);
        let final_name = final_part_file_name(index, job_id);
        let final_path = if hive_path.is_empty() {
            dest_dir.join(&final_name)
        } else {
            dest_dir.join(&hive_path).join(&final_name)
        };
        if final_path.exists() {
            outcome.skipped_existing += 1;
            continue;
        }
        move_file(&entry.path, &final_path)?;
        outcome.published.push(final_path);
    }

    // All winners are in place: remove the staging directory for this job and
    // prune the shared `_staging` parent if it is now empty.
    remove_dir_all_tolerant(&staging_dir)?;
    let staging_parent = dest_dir.join(SINK_STAGING_DIR_NAME);
    let _ = std::fs::remove_dir(&staging_parent); // best-effort: only removes empty dirs
    Ok(outcome)
}

/// Remove staged outputs for an aborted/failed job.
///
/// Tolerates already-missing staging directories so cleanup can run multiple
/// times (job failure followed by GC, for example).
pub fn cleanup_staged_outputs(spec: &SinkWriteSpec, job_id: &str) -> Result<(), WriteCommitError> {
    let dest_dir = Path::new(&spec.base_dir).join(&spec.dest_path);
    let staging_dir = Path::new(&spec.base_dir).join(spec.staging_dir_rel(job_id));
    remove_dir_all_tolerant(&staging_dir)?;
    let staging_parent = dest_dir.join(SINK_STAGING_DIR_NAME);
    let _ = std::fs::remove_dir(&staging_parent); // best-effort: only removes empty dirs
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_mode_parse_round_trip() {
        for mode in [
            WriteMode::Append,
            WriteMode::Overwrite,
            WriteMode::ErrorIfExists,
            WriteMode::Ignore,
        ] {
            assert_eq!(WriteMode::parse(mode.as_token()).unwrap(), mode);
        }
        assert_eq!(
            WriteMode::parse("ErrorIfExists").unwrap(),
            WriteMode::ErrorIfExists
        );
        assert!(WriteMode::parse("truncate").is_err());
    }

    #[test]
    fn spec_parse_legacy_payload_is_unstaged() {
        let spec = SinkWriteSpec::parse("/data:out/file.parquet").unwrap();
        assert!(!spec.staged);
        assert_eq!(spec.mode, WriteMode::Append);
        assert_eq!(spec.base_dir, "/data");
        assert_eq!(spec.dest_path, "out/file.parquet");
        assert!(spec.partition_by.is_empty());
    }

    #[test]
    fn spec_parse_tokens_and_round_trip() {
        let spec =
            SinkWriteSpec::parse("/data:out:mode=overwrite:partition_by=country,year").unwrap();
        assert!(spec.staged);
        assert_eq!(spec.mode, WriteMode::Overwrite);
        assert_eq!(spec.partition_by, vec!["country", "year"]);
        assert_eq!(
            SinkWriteSpec::parse(&spec.contract_payload()).unwrap(),
            spec
        );
    }

    #[test]
    fn spec_parse_partition_only_defaults_to_append() {
        let spec = SinkWriteSpec::parse("/data:out:partition_by=c").unwrap();
        assert!(spec.staged);
        assert_eq!(spec.mode, WriteMode::Append);
    }

    #[test]
    fn spec_parse_rejects_garbage() {
        assert!(SinkWriteSpec::parse("no-colon-here").is_err());
        assert!(SinkWriteSpec::parse("/data:out:mode=bogus").is_err());
        assert!(SinkWriteSpec::parse("/data:out:mode=append:mode=ignore").is_err());
        assert!(SinkWriteSpec::parse("/data:out:partition_by=").is_err());
        assert!(SinkWriteSpec::parse(":out:mode=append").is_err());
    }

    #[test]
    fn staging_paths_are_deterministic() {
        let spec = SinkWriteSpec::staged("/base", "out", WriteMode::Append, vec![]).unwrap();
        assert_eq!(spec.staging_dir_rel("job-7"), "out/_staging/job-7");
        assert_eq!(
            spec.staged_file_rel("job-7", "", "task-3", 2),
            "out/_staging/job-7/task-3-2.parquet"
        );
        assert_eq!(
            spec.staged_file_rel("job-7", "c=US", "task-3", 2),
            "out/_staging/job-7/c=US/task-3-2.parquet"
        );
    }

    #[test]
    fn staged_name_parse_and_task_index() {
        assert_eq!(
            parse_staged_file_name("task-3-2.parquet").unwrap(),
            (String::from("task-3"), 2)
        );
        assert!(parse_staged_file_name("nope.txt").is_err());
        assert!(parse_staged_file_name("noattempt.parquet").is_err());
        assert_eq!(task_index_from_task_id("task-12"), 12);
        assert_eq!(task_index_from_task_id("task-sql"), 0);
    }

    #[test]
    fn hive_escape_keeps_safe_chars() {
        assert_eq!(hive_escape("US-east_1.zone"), "US-east_1.zone");
        assert_eq!(hive_escape("a/b=c"), "a%2Fb%3Dc");
    }
}
