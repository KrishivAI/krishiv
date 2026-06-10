#![forbid(unsafe_code)]

use std::net::SocketAddr;

use krishiv_ui::{demo_state, empty_state, serve};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServerConfig::parse(std::env::args().skip(1))?;
    if config.help {
        print!("{}", ServerConfig::help());
        return Ok(());
    }

    let state = if config.demo {
        demo_state()?
    } else {
        empty_state()?
    };

    // Attach a local SQL engine for the query editor.
    let state = if config.with_sql || config.demo {
        let engine = krishiv_sql::SqlEngine::new();
        state.with_sql_engine(engine)
    } else {
        state
    };

    let listener = tokio::net::TcpListener::bind(config.addr).await?;
    let local_addr = listener.local_addr()?;

    println!("Krishiv R2 status UI listening on http://{local_addr}/ui");
    if config.with_sql || config.demo {
        println!("  SQL editor:    http://{local_addr}/ui/submit");
    }
    println!("  Health:        http://{local_addr}/ui/health");
    serve(listener, state).await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ServerConfig {
    addr: SocketAddr,
    demo: bool,
    with_sql: bool,
    help: bool,
}

impl ServerConfig {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, Box<dyn std::error::Error>> {
        let mut addr = "127.0.0.1:8080"
            .parse::<SocketAddr>()
            .expect("default UI address is valid");
        let mut demo = false;
        let mut with_sql = false;
        let mut help = false;
        let mut args = args.into_iter();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--addr" => {
                    let value = args
                        .next()
                        .ok_or_else(|| String::from("missing value for --addr"))?;
                    addr = value
                        .parse()
                        .map_err(|_| format!("invalid socket address: {value}"))?;
                }
                "--demo" => demo = true,
                "--with-sql" => with_sql = true,
                "--help" | "-h" => help = true,
                unknown => {
                    return Err(format!("unknown option: {unknown}\n\n{}", Self::help()).into());
                }
            }
        }

        Ok(Self {
            addr,
            demo,
            with_sql,
            help,
        })
    }

    fn help() -> &'static str {
        "Run the Krishiv R2 status UI.\n\
         \n\
         Usage:\n\
           krishiv-ui [--addr <HOST:PORT>] [--demo] [--with-sql]\n\
         \n\
         Options:\n\
           --addr <HOST:PORT>  Address to bind, defaults to 127.0.0.1:8080\n\
           --demo              Seed one local coordinator, executor, and running job\n\
           --with-sql          Enable the SQL query editor (uses embedded DataFusion)\n\
           -h, --help          Show help\n"
    }
}

#[cfg(test)]
mod tests {
    use super::ServerConfig;

    #[test]
    fn parses_defaults() {
        let config = ServerConfig::parse([]).unwrap();

        assert_eq!(config.addr.to_string(), "127.0.0.1:8080");
        assert!(!config.demo);
        assert!(!config.with_sql);
    }

    #[test]
    fn parses_demo_and_addr() {
        let config = ServerConfig::parse([
            String::from("--demo"),
            String::from("--addr"),
            String::from("127.0.0.1:0"),
        ])
        .unwrap();

        assert!(config.demo);
        assert_eq!(config.addr.to_string(), "127.0.0.1:0");
    }

    #[test]
    fn parses_with_sql() {
        let config = ServerConfig::parse([String::from("--with-sql")]).unwrap();
        assert!(config.with_sql);
    }
}
