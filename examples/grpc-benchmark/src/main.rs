use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::Local;
use dotenvy::dotenv;
use futures_util::SinkExt;
use grpc_client::{AppError, YellowstoneGrpc};
use log::{error, info};
use solana_client::rpc_client::RpcClient;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest,
    SubscribeRequestPing, subscribe_update::UpdateOneof,
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

    // 创建 Subscribe 请求，订阅 blocks
    let subscribe_request = SubscribeRequest {
        blocks: HashMap::from([(
            "benchmark".to_string(),
            yellowstone_grpc_proto::geyser::SubscribeRequestFilterBlocks {
                account_include: vec![],
                include_transactions: Some(true),
                include_accounts: Some(false),
                include_entries: Some(false),
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

    // 用于记录和比较延迟的变量
    let mut last_http_slot = 0;
    let mut last_grpc_slot = 0;

    // 启动一个任务每 400ms 检查一次 HTTP RPC 的 slot
    let http_task = tokio::spawn(async move {
        loop {
            match rpc_client.get_slot() {
                Ok(slot) => {
                    if slot > last_http_slot {
                        info!("[{}] HTTP RPC Slot: {}", Local::now().format("%H:%M:%S%.3f"), slot);
                        last_http_slot = slot;
                    }
                }
                Err(e) => error!("HTTP RPC Error: {}", e),
            }
            sleep(Duration::from_millis(400)).await;
        }
    });

    // 处理 gRPC 流
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                match msg.update_oneof {
                    Some(UpdateOneof::Block(block)) => {
                        let now = Instant::now();
                        last_grpc_slot = block.slot;
                        info!("[{}] gRPC Block slot: {}", Local::now().format("%H:%M:%S%.3f"), block.slot);
                        info!("[{}] http slot: {}", Local::now().format("%H:%M:%S%.3f"), last_http_slot);

                        // 比较 HTTP 和 gRPC 的延迟
                        if last_grpc_slot == last_http_slot {
                            info!("[{}] gRPC and HTTP slots are in sync at slot {}", Local::now().format("%H:%M:%S%.3f"), last_grpc_slot);
                        } else if last_grpc_slot > last_http_slot {
                            info!("[{}] gRPC is ahead of HTTP by {} slots", Local::now().format("%H:%M:%S%.3f"), last_grpc_slot - last_http_slot);
                        } else {
                            info!("[{}] HTTP is ahead of gRPC by {} slots", Local::now().format("%H:%M:%S%.3f"), last_http_slot - last_grpc_slot);
                        }
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
