//! Small-file scan planner.

/// Per-file metadata used by [`SmallFilePlanner`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStats {
    pub path: String,
    pub size_bytes: u64,
}

/// Advice produced by [`SmallFilePlanner`]: a list of scan groups where each
/// group of file paths should be handled by a single executor task.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitPlanAdvice {
    /// Each inner `Vec` is one task's worth of files.
    pub task_groups: Vec<Vec<String>>,
}

/// Plans scan parallelism for a set of files.
///
/// When individual files are smaller than `target_bytes`, multiple files are
/// grouped into a single task so each task processes roughly `target_bytes` of
/// data. Files larger than `target_bytes` each get their own task (splitting
/// within a file is not yet supported).
pub struct SmallFilePlanner {
    target_bytes: u64,
}

impl SmallFilePlanner {
    /// Create a planner with the given target bytes per task.
    pub fn new(target_bytes: u64) -> Self {
        Self { target_bytes }
    }

    /// Produce a scan plan for the given file list.
    ///
    /// Files are grouped greedily: accumulate until the next file would push the
    /// group over `target_bytes`, then start a new group. This ensures each
    /// group is at most `target_bytes + max_single_file_bytes`.
    pub fn plan(&self, files: &[FileStats]) -> SplitPlanAdvice {
        if files.is_empty() {
            return SplitPlanAdvice {
                task_groups: Vec::new(),
            };
        }

        let mut groups: Vec<Vec<String>> = Vec::new();
        let mut current: Vec<String> = Vec::new();
        let mut current_bytes = 0u128;
        let target_bytes = u128::from(self.target_bytes);

        for file in files {
            let file_bytes = u128::from(file.size_bytes);
            if !current.is_empty() && current_bytes + file_bytes > target_bytes {
                groups.push(std::mem::take(&mut current));
                current_bytes = 0;
            }
            current.push(file.path.clone());
            current_bytes += file_bytes;
        }
        if !current.is_empty() {
            groups.push(current);
        }

        SplitPlanAdvice {
            task_groups: groups,
        }
    }
}
