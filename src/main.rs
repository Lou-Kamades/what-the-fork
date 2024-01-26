use log::{error, warn};
use crate::grpc_plugin::{subscribe_geyser, GrpcConfig};
use std::{
    fs::File,
    io::Read,
};

mod grpc_plugin;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // let exit: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

    // load config
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Please enter a config file path argument");
        return Ok(());
    }

    let config: GrpcConfig = {
        let mut file = File::open(&args[1])?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        toml::from_str(&contents).unwrap()
    };

    // Continuously reconnect on failure
    loop {
        let out = subscribe_geyser(config.clone());
        match out.await {
            // happy case!
            Err(err) => {
                warn!(
                    "error during communication with the geyser plugin - retrying: {:?}",
                    err
                );
            }
            // this should never happen
            Ok(_) => {
                error!("feed_data must return an error, not OK - continue");
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(
            config.retry_connection_sleep_secs,
        ))
        .await;
    }
}
