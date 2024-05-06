use std::collections::HashMap;

use events_api_redis_to_db::redis_reader::{create_connection, stream_events};
use events_api_redis_to_db::{
    events::{NftBurnEvent, NftEventContext, NftMintEvent, NftTransferEvent},
    redis_reader::EventHandler,
};
use redis::{FromRedisValue, Value};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let redis_connection = create_connection(
        &std::env::var("REDIS_URL").expect("REDIS_URL enviroment variable not set"),
    )
    .await;
    let pg_pool = sqlx::PgPool::connect(
        &std::env::var("DATABASE_URL").expect("DATABASE_URL enviroment variable not set"),
    )
    .await?;

    let nft_mint_task = stream_events(
        "nft_mint",
        NftMintHandler,
        redis_connection.clone(),
        pg_pool.clone(),
    );
    let nft_transfer_task = stream_events(
        "nft_transfer",
        NftTransferHandler,
        redis_connection.clone(),
        pg_pool.clone(),
    );
    let nft_burn_task = stream_events(
        "nft_burn",
        NftBurnHandler,
        redis_connection.clone(),
        pg_pool.clone(),
    );

    tokio::join!(nft_mint_task, nft_transfer_task, nft_burn_task);
    Ok(())
}

struct NftTransferHandler;

#[async_trait::async_trait]
impl EventHandler for NftTransferHandler {
    async fn handle(
        &self,
        values: HashMap<String, Value>,
        pg_pool: &sqlx::PgPool,
    ) -> anyhow::Result<()> {
        if let (Ok(context), Ok(event)) = (
            serde_json::from_str::<NftEventContext>(&String::from_redis_value(
                values.get("context").unwrap(),
            )?),
            serde_json::from_str::<NftTransferEvent>(&String::from_redis_value(
                values.get("transfer").unwrap(),
            )?),
        ) {
            sqlx::query!(
                r#"
                INSERT INTO nft_transfer (timestamp, transaction_id, receipt_id, block_height, contract_id, old_owner_id, new_owner_id, token_ids, memo, token_prices_near)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                "#,
                chrono::DateTime::from_timestamp((context.block_timestamp_nanosec / 1_000_000_000) as i64, (context.block_timestamp_nanosec % 1_000_000_000) as u32),
                context.transaction_id,
                context.receipt_id,
                context.block_height as i64,
                context.contract_id,
                event.old_owner_id,
                event.new_owner_id,
                &event.token_ids,
                event.memo,
                &event.token_prices_near.iter().map(|price| price.unwrap_or_default()).collect::<Vec<_>>()
            )
            .execute(pg_pool)
            .await?;
        } else {
            log::error!("Failed to parse event");
        }
        Ok(())
    }
}

struct NftMintHandler;

#[async_trait::async_trait]
impl EventHandler for NftMintHandler {
    async fn handle(
        &self,
        values: HashMap<String, Value>,
        pg_pool: &sqlx::PgPool,
    ) -> anyhow::Result<()> {
        if let (Ok(context), Ok(event)) = (
            serde_json::from_str::<NftEventContext>(&String::from_redis_value(
                values.get("context").unwrap(),
            )?),
            serde_json::from_str::<NftMintEvent>(&String::from_redis_value(
                values.get("mint").unwrap(),
            )?),
        ) {
            sqlx::query!(
                r#"
                INSERT INTO nft_mint (timestamp, transaction_id, receipt_id, block_height, contract_id, owner_id, token_ids, memo)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                "#,
                chrono::DateTime::from_timestamp((context.block_timestamp_nanosec / 1_000_000_000) as i64, (context.block_timestamp_nanosec % 1_000_000_000) as u32),
                context.transaction_id,
                context.receipt_id,
                context.block_height as i64,
                context.contract_id,
                event.owner_id,
                &event.token_ids,
                event.memo
            )
            .execute(pg_pool)
            .await?;
        } else {
            log::error!("Failed to parse event");
        }
        Ok(())
    }
}

struct NftBurnHandler;

#[async_trait::async_trait]
impl EventHandler for NftBurnHandler {
    async fn handle(
        &self,
        values: HashMap<String, Value>,
        pg_pool: &sqlx::PgPool,
    ) -> anyhow::Result<()> {
        if let (Ok(context), Ok(event)) = (
            serde_json::from_str::<NftEventContext>(&String::from_redis_value(
                values.get("context").unwrap(),
            )?),
            serde_json::from_str::<NftBurnEvent>(&String::from_redis_value(
                values.get("burn").unwrap(),
            )?),
        ) {
            sqlx::query!(
                r#"
                INSERT INTO nft_burn (timestamp, transaction_id, receipt_id, block_height, contract_id, owner_id, token_ids, memo)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                "#,
                chrono::DateTime::from_timestamp((context.block_timestamp_nanosec / 1_000_000_000) as i64, (context.block_timestamp_nanosec % 1_000_000_000) as u32),
                context.transaction_id,
                context.receipt_id,
                context.block_height as i64,
                context.contract_id,
                event.owner_id,
                &event.token_ids,
                event.memo
            )
            .execute(pg_pool)
            .await?;
        } else {
            log::error!("Failed to parse event");
        }
        Ok(())
    }
}
