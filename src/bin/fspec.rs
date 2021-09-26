use simplelog::{Config, SimpleLogger};
use clap::{App, Arg,ArgMatches,SubCommand};
use flow_spec::fspec_config::SpecEnvironmentConfiguration;
use log::{LevelFilter, debug, trace, info, warn, error};


fn main() -> anyhow::Result<()> {
    let matches = App::new("fspec")
        .args_from_usage(
        "-v... 'Sets the verbosity level (-v, -vv, -vvv correspond to info, debug, and trace respectively)'")
        .subcommand(SubCommand::with_name("init")
                        .about("initializes the fspec database at a location"))
        .subcommand(SubCommand::with_name("list")
                        .about("list the configured parameter types"))
        .get_matches();

    let log_level = match matches.occurrences_of("v") {
        0 => LevelFilter::Warn,
        1 => LevelFilter::Info,
        2 => LevelFilter::Debug,
        _ => LevelFilter::Trace
    };

    let _ = SimpleLogger::init(
        log_level,
        Config::default()

    )?;

    let command = match matches.subcommand() {
        ("init", _) => flow_spec::FspecCommand::Initialize,
        ("list", _) => flow_spec::FspecCommand::ListAvailableTypes,
        _ => {
            error!("unexpected command");
            return Err(anyhow::Error::from(std::io::Error::from(std::io::ErrorKind::InvalidInput)));
        }
    };

    flow_spec::execute_fspec_command(command, SpecEnvironmentConfiguration::default())?;
    Ok(())
}

