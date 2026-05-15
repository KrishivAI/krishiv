#![forbid(unsafe_code)]

//! Command-line shell for Krishiv.
//!
//! R1 bootstrap provides help and command boundaries only. Command execution
//! will be wired to the API/runtime in later R1 slices.

/// CLI response used by `main` and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliResponse {
    /// Standard output.
    pub stdout: String,
    /// Standard error.
    pub stderr: String,
    /// Process exit code.
    pub exit_code: i32,
}

impl CliResponse {
    fn ok(stdout: impl Into<String>) -> Self {
        Self {
            stdout: stdout.into(),
            stderr: String::new(),
            exit_code: 0,
        }
    }

    fn err(stderr: impl Into<String>, exit_code: i32) -> Self {
        Self {
            stdout: String::new(),
            stderr: stderr.into(),
            exit_code,
        }
    }
}

/// Dispatch CLI arguments after the binary name.
pub fn dispatch(args: &[&str]) -> CliResponse {
    match args {
        [] | ["--help"] | ["-h"] | ["help"] => CliResponse::ok(main_help()),
        ["sql"] | ["sql", "--help"] | ["sql", "-h"] => CliResponse::ok(sql_help()),
        ["explain"] | ["explain", "--help"] | ["explain", "-h"] => CliResponse::ok(explain_help()),
        ["jobs"] | ["jobs", "--help"] | ["jobs", "-h"] => CliResponse::ok(jobs_help()),
        ["help", "sql"] => CliResponse::ok(sql_help()),
        ["help", "explain"] => CliResponse::ok(explain_help()),
        ["help", "jobs"] => CliResponse::ok(jobs_help()),
        [unknown, ..] => {
            CliResponse::err(format!("unknown command: {unknown}\n\n{}", main_help()), 2)
        }
    }
}

/// Top-level help text.
pub fn main_help() -> String {
    String::from(
        "Krishiv hybrid compute framework\n\
         \n\
         Usage:\n\
           krishiv <COMMAND>\n\
         \n\
         Commands:\n\
           sql       Run a local SQL query (R1 stub)\n\
           explain   Show logical/physical plan information (R1 stub)\n\
           jobs      List local jobs (R1 stub)\n\
           help      Show help for a command\n\
         \n\
         Options:\n\
           -h, --help  Show help\n",
    )
}

/// Help text for `krishiv sql`.
pub fn sql_help() -> String {
    String::from(
        "Run a local SQL query.\n\
         \n\
         Usage:\n\
           krishiv sql --query <SQL>\n\
         \n\
         R1 bootstrap note:\n\
           SQL execution is not implemented yet. This command exists to lock\n\
           the CLI shape before DataFusion integration.\n",
    )
}

/// Help text for `krishiv explain`.
pub fn explain_help() -> String {
    String::from(
        "Show logical and physical plan information.\n\
         \n\
         Usage:\n\
           krishiv explain --query <SQL>\n\
         \n\
         R1 bootstrap note:\n\
           Explain output is represented by stubs until DataFusion integration.\n",
    )
}

/// Help text for `krishiv jobs`.
pub fn jobs_help() -> String {
    String::from(
        "List local jobs.\n\
         \n\
         Usage:\n\
           krishiv jobs\n\
         \n\
         R1 bootstrap note:\n\
           Job listing is local-only until the scheduler is introduced.\n",
    )
}

#[cfg(test)]
mod tests {
    use super::dispatch;

    #[test]
    fn top_level_help_lists_commands() {
        let response = dispatch(&["--help"]);

        assert_eq!(response.exit_code, 0);
        assert!(response.stdout.contains("Commands:"));
        assert!(response.stdout.contains("sql"));
        assert!(response.stdout.contains("explain"));
        assert!(response.stdout.contains("jobs"));
    }

    #[test]
    fn subcommand_help_is_available() {
        let response = dispatch(&["help", "explain"]);

        assert_eq!(response.exit_code, 0);
        assert!(response.stdout.contains("krishiv explain"));
    }

    #[test]
    fn unknown_command_fails() {
        let response = dispatch(&["bogus"]);

        assert_eq!(response.exit_code, 2);
        assert!(response.stderr.contains("unknown command: bogus"));
    }
}
