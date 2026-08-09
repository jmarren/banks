mod agent;
mod provider;

use agent::Agent;
use clap::Parser;
use provider::claude::ClaudeProvider;
use std::io::{self, BufRead, Write};

#[derive(Parser)]
#[command(name = "banks", about = "A minimal CLI AI agent")]
struct Args {
    #[arg(long, default_value = "claude-sonnet-5")]
    model: String,

    #[arg(long)]
    system: Option<String>,

    #[arg(long, default_value_t = 4096)]
    max_tokens: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // load .env if present; missing file is fine, other errors are not
    match dotenvy::dotenv() {
        Ok(_) | Err(dotenvy::Error::Io(_)) => {}
        Err(e) => return Err(e.into()),
    }

    // setup tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // parse args passed
    let args = Args::parse();

    // get api key
    let api_key = std::env::var("ANTHROPIC_API_KEY")
        .map_err(|_| anyhow::anyhow!("ANTHROPIC_API_KEY environment variable is not set"))?;

    // instantiate the claude provider
    let provider = ClaudeProvider::new(api_key, args.model);

    // use the provider to instantiate the agent
    let mut agent = Agent::new(provider, args.system, args.max_tokens);

    // lock lines
    let mut lines = io::stdin().lock().lines();

    loop {
        // print command prompt
        print!("> ");

        // flush
        io::stdout().flush()?;

        // get line or break;
        let Some(line) = lines.next() else { break };
        let line = line?;
        let trimmed = line.trim();

        match trimmed {
            "" => continue,
            "/exit" | "/quit" => break,
            _ => {
                if let Err(e) = agent.turn(trimmed).await {
                    eprintln!("error: {e}");
                }
            }
        }
    }

    Ok(())
}
