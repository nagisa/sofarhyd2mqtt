use clap::Parser as _;
use sofarhyd2mqtt::commands;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

#[derive(clap::Parser)]
#[clap(version, about, author)]
enum Commands {
    Mqtt(commands::mqtt::Args),
    Dump(commands::dump::Args),
}

fn end<E: std::error::Error>(r: Result<(), E>, to_journal: bool) {
    std::process::exit(match r {
        Ok(_) => 0,
        Err(e) if to_journal => {
            // One line, which journald stores at error priority because of the `<3>`, whatever
            // the log filter.
            eprintln!("<3>error: {}", sofarhyd2mqtt::Chain(&e));
            1
        }
        Err(e) => {
            eprintln!("error: {e}");
            let mut cause = e.source();
            while let Some(e) = cause {
                eprintln!("  because: {e}");
                cause = e.source();
            }
            1
        }
    });
}

/// The journald layer, if stderr is the journal stream systemd connected. Logging through it gives
/// the lines their real priorities and their fields as journal fields.
fn journald() -> Option<tracing_journald::Layer> {
    use std::os::{fd::AsFd as _, unix::fs::MetadataExt as _};
    // Children inherit `JOURNAL_STREAM`, so it only counts if it is stderr's device and inode.
    let stream = std::env::var("JOURNAL_STREAM").ok()?;
    let stderr = std::fs::File::from(std::io::stderr().as_fd().try_clone_to_owned().ok()?);
    let stderr = stderr.metadata().ok()?;
    if stream != format!("{}:{}", stderr.dev(), stderr.ino()) {
        return None;
    }
    let priorities = tracing_journald::PriorityMappings {
        info: tracing_journald::Priority::Informational,
        debug: tracing_journald::Priority::Debug,
        ..tracing_journald::PriorityMappings::new()
    };
    Some(tracing_journald::layer().ok()?.with_priority_mappings(priorities))
}

fn main() {
    let filter_description = std::env::var("SOFARHYD2MQTT_LOG");
    let filter = filter_description
        .as_deref()
        .unwrap_or("info")
        .parse::<tracing_subscriber::filter::targets::Targets>();
    let journald = journald();
    let to_journal = journald.is_some();
    let stderr =
        (!to_journal).then(|| tracing_subscriber::fmt::layer().with_writer(std::io::stderr));
    match filter {
        Ok(f) => tracing_subscriber::registry().with(journald).with(stderr).with(f).init(),
        Err(e) => end(Err(e), to_journal),
    }
    match Commands::parse() {
        Commands::Mqtt(args) => end(commands::mqtt::run(args), to_journal),
        Commands::Dump(args) => end(commands::dump::run(args), to_journal),
    }
}
