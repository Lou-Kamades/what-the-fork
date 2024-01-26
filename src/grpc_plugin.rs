use futures::{stream::once, StreamExt};
use log::*;
use serde_derive::Deserialize;
use std::{collections::HashMap, env, time::Duration};
use yellowstone_grpc_proto::{
    geyser::{
        geyser_client::GeyserClient, subscribe_update, SubscribeRequest,
        SubscribeRequestFilterSlots,
    },
    tonic::{
        metadata::MetadataValue,
        transport::{Channel, ClientTlsConfig},
        Request,
    },
};

#[derive(Clone, Debug, Deserialize)]
pub struct GrpcConfig {
    pub grpc_url: String,
    pub grpc_token: Option<String>,
    pub retry_connection_sleep_secs: u64,
}

pub async fn subscribe_geyser(config: GrpcConfig) -> anyhow::Result<()> {
    info!("connecting {}", &config.grpc_url);
    let tls_config = if config.grpc_url.starts_with("https") {
        Some(ClientTlsConfig::new())
    } else {
        None
    };
    let endpoint = Channel::from_shared(config.grpc_url)?;
    let channel = if let Some(tls) = tls_config {
        endpoint.tls_config(tls)?
    } else {
        endpoint
    }
    .connect()
    .await?;

    let token: Option<MetadataValue<_>> = match &config.grpc_token {
        Some(token) => match token.chars().next().unwrap() {
            '$' => Some(
                env::var(&token[1..])
                    .expect("reading token from env")
                    .parse()?,
            ),
            _ => Some(token.clone().parse()?),
        },
        None => None,
    };

    let mut client = GeyserClient::with_interceptor(channel, move |mut req: Request<()>| {
        if let Some(token) = &token {
            req.metadata_mut().insert("x-token", token.clone());
        }
        Ok(req)
    });

    let accounts = HashMap::new();
    let mut slots = HashMap::new();
    let blocks = HashMap::new();
    let transactions = HashMap::new();
    let blocks_meta = HashMap::new();

    slots.insert(
        "client".to_owned(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: None,
        },
    );

    let request = SubscribeRequest {
        accounts,
        blocks,
        blocks_meta,
        entry: Default::default(),
        commitment: None,
        slots,
        transactions,
        accounts_data_slice: vec![],
        ping: None,
    };
    info!("sending subscribe request: {:?}", request);

    let response = client.subscribe(once(async move { request })).await?;
    let mut update_stream = response.into_inner();
    let fatal_idle_timeout = Duration::from_secs(60);

    loop {
        tokio::select! {
            update = update_stream.next() => {
                use subscribe_update::UpdateOneof;
                let mut update = update.ok_or(anyhow::anyhow!("geyser plugin has closed the stream"))??;
                match update.update_oneof.as_mut().expect("invalid grpc") {
                    UpdateOneof::Slot(slot_update) => {
                        println!("{:?}", slot_update);
                    },
                    _ => {}
                }
            }
            _ = tokio::time::sleep(fatal_idle_timeout) => {
                anyhow::bail!("geyser plugin hasn't sent a message in too long");
            }
        }
    }
}
