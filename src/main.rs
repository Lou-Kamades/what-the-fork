use crate::{
    chain_data::SlotData,
    grpc_plugin::{subscribe_geyser, GrpcConfig},
};
use chain_data::ChainData;
use std::{fs::File, io::Read};
use tokio::{pin, sync::mpsc};

mod chain_data;
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

    let (slot_sender, slot_receiver) = mpsc::channel(100);
    let mut chain_data = ChainData::new();

    tokio::spawn(async move {
        pin!(slot_receiver);
        loop {
            let update = slot_receiver.recv().await.unwrap();
            chain_data.update_slot(SlotData::from_update(update));
            chain_data.print();
            println!("\n\n");
        }
    });

    subscribe_geyser(config, slot_sender).await
}
