use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::Local;
use dotenvy::dotenv;
use futures_util::SinkExt;
use grpc_client::{AppError, YellowstoneGrpc};
use log::{error, info};
use solana_client::rpc_client::RpcClient;
use tokio::sync::watch;
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

    // 使用 tokio 的 watch 通道来共享 HTTP slot 信息
    let (http_slot_tx, http_slot_rx) = watch::channel(0u64);
    
    // Initialize RPC client for benchmark
    let rpc_url = std::env::var("SOLANA_RPC_URL").expect("SOLANA_RPC_URL must be set");
    info!("连接到 Solana RPC: {}", rpc_url);
    
    // 先测试 RPC 连接是否正常
    let test_client = RpcClient::new(rpc_url.clone());
    match test_client.get_slot() {
        Ok(slot) => info!("RPC 连接正常，当前 slot: {}", slot),
        Err(e) => {
            error!("RPC 连接测试失败: {}", e);
            panic!("无法连接到 Solana RPC，请检查 SOLANA_RPC_URL 环境变量");
        }
    }
    
    let rpc_client = RpcClient::new(rpc_url.clone());
    
    // 启动独立的 HTTP RPC 监控任务
    tokio::spawn(async move {
        info!("HTTP RPC 监控任务启动，URL: {}", rpc_url);
        loop {
            info!("[{}] 尝试获取 HTTP RPC slot...", Local::now().format("%H:%M:%S%.3f"));
            match rpc_client.get_slot() {
                Ok(slot) => {
                    let slot = slot as u64;
                    let current_slot = *http_slot_tx.borrow();
                    info!("[{}] 获取到 HTTP RPC slot: {}, 当前存储值: {}", 
                        Local::now().format("%H:%M:%S%.3f"), slot, current_slot);
                    if slot > current_slot {
                        let _ = http_slot_tx.send(slot); // 发送新的 slot 值
                        info!("[{}] HTTP RPC Slot 更新: {}", Local::now().format("%H:%M:%S%.3f"), slot);
                    }
                }
                Err(e) => error!("[{}] HTTP RPC Error: {}", Local::now().format("%H:%M:%S%.3f"), e),
            }
            sleep(Duration::from_millis(400)).await;
        }
    });

    // Initialize gRPC client
    let url = std::env::var("YELLOWSTONE_GRPC_URL").expect("YELLOWSTONE_GRPC_URL must be set");
    let grpc = YellowstoneGrpc::new(url, None);
    let client = grpc.build_client().await?;

    // 创建 Subscribe 请求，订阅 blocks - 减少数据量以避免消息太大
    let subscribe_request = SubscribeRequest {
        blocks: HashMap::from([(
            "benchmark".to_string(),
            yellowstone_grpc_proto::geyser::SubscribeRequestFilterBlocks {
                account_include: vec![],
                include_transactions: Some(true),
                include_accounts: Some(true),
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

    // 处理 gRPC 流
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                match msg.update_oneof {
                    Some(UpdateOneof::Block(block)) => {
                        let grpc_slot = block.slot;
                        let current_http_slot = *http_slot_rx.borrow();
                        
                        info!("[{}] gRPC Block slot: {}", Local::now().format("%H:%M:%S%.3f"), grpc_slot);
                        info!("[{}] 当前 HTTP slot: {}", Local::now().format("%H:%M:%S%.3f"), current_http_slot);
                        
                        // 比较 HTTP 和 gRPC 的延迟
                        if grpc_slot == current_http_slot {
                            info!("[{}] gRPC 和 HTTP slots 同步在 slot {}", Local::now().format("%H:%M:%S%.3f"), grpc_slot);
                        } else if grpc_slot > current_http_slot {
                            info!("[{}] gRPC 领先 HTTP {} slots", Local::now().format("%H:%M:%S%.3f"), grpc_slot - current_http_slot);
                        } else {
                            info!("[{}] HTTP 领先 gRPC {} slots", Local::now().format("%H:%M:%S%.3f"), current_http_slot - grpc_slot);
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
