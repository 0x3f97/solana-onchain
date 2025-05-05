use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::Local;
use dotenvy::dotenv;
use futures_util::SinkExt;
use grpc_client::{AppError, TransactionFormat, YellowstoneGrpc};
use log::{error, info};
use solana_client::rpc_client::RpcClient;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeRequestPing,
    subscribe_update::UpdateOneof,
};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    dotenv().ok();
    pretty_env_logger::init();

    // Initialize RPC client for benchmark
    let rpc_url = std::env::var("SOLANA_RPC_URL").expect("SOLANA_RPC_URL must be set");
    let rpc_client = RpcClient::new(rpc_url);

    // Initialize gRPC client
    let url = std::env::var("YELLOWSTONE_GRPC_URL").expect("YELLOWSTONE_GRPC_URL must be set");
    let grpc = YellowstoneGrpc::new(url, None);
    let client = grpc.build_client().await?;

    // Start HTTP RPC slot monitoring in a separate task
    let mut last_http_slot = 0;
    tokio::spawn(async move {
        loop {
            match rpc_client.get_slot() {
                Ok(current_http_slot) => {
                    if current_http_slot > last_http_slot {
                        info!("HTTP RPC Slot: {}", current_http_slot);
                        last_http_slot = current_http_slot;
                    }
                }
                Err(e) => error!("HTTP RPC Error: {}", e),
            }
            sleep(Duration::from_millis(400)).await;
        }
    });

    // Original transaction subscription logic
    let addrs = vec!["6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P".to_string()];
    let subscribe_request = SubscribeRequest {
        transactions: HashMap::from([(
            "client".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                signature: None,
                account_include: addrs,
                account_exclude: vec![],
                account_required: vec![],
            },
        )]),
        commitment: Some(CommitmentLevel::Processed.into()),
        ..Default::default()
    };

    let (mut subscribe_tx, mut stream) = client
        .lock()
        .await
        .subscribe_with_request(Some(subscribe_request))
        .await?;

    let mut last_grpc_time = Instant::now();
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                match msg.update_oneof {
                    Some(UpdateOneof::Transaction(sut)) => {
                        let transaction_pretty: TransactionFormat = sut.into();
                        info!("transaction: {:#?}", transaction_pretty);
                        let latency = last_grpc_time.elapsed();
                        info!("gRPC Latency: {}ms", latency.as_millis());
                        last_grpc_time = Instant::now();
                    }
                    Some(UpdateOneof::Ping(_)) => {
                        let _ = subscribe_tx
                            .send(SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: 1 }),
                                ..Default::default()
                            })
                            .await;
                        info!("service is ping: {:#?}", Local::now());
                    }
                    Some(UpdateOneof::Pong(_)) => {
                        info!("service is pong: {:#?}", Local::now());
                    }
                    _ => {}
                }
                continue;
            }
            Err(error) => {
                error!("error: {error:?}");
                break;
            }
        }
    }
    Ok(())
}
