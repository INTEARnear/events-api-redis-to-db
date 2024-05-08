use std::collections::HashMap;
use std::str::FromStr;

use events_api_redis_to_db::redis_reader::{create_connection, stream_events};
use events_api_redis_to_db::{
    events::{
        NftBurnEvent, NftEventContext, NftMintEvent, NftTransferEvent, PotlockDonationEvent,
        PotlockEventContext, PotlockPotDonationEvent, PotlockPotProjectDonationEvent,
        TradeBalanceChangeSwapEvent, TradeContext, TradePoolChangeEvent, TradeRawPoolSwapEvent,
    },
    redis_reader::EventHandler,
};
use redis::{FromRedisValue, Value};
use sqlx::types::BigDecimal;

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
    let potlock_donation_task = stream_events(
        "potlock_donation",
        PotlockDonationHandler,
        redis_connection.clone(),
        pg_pool.clone(),
    );
    let potlock_pot_project_donation_task = stream_events(
        "potlock_pot_project_donation",
        PotlockPotProjectDonationHandler,
        redis_connection.clone(),
        pg_pool.clone(),
    );
    let potlock_pot_donation_task = stream_events(
        "potlock_pot_donation",
        PotlockPotDonationHandler,
        redis_connection.clone(),
        pg_pool.clone(),
    );
    let trade_raw_pool_swap_task = stream_events(
        "trade_pool",
        TradeRawPoolSwapHandler,
        redis_connection.clone(),
        pg_pool.clone(),
    );
    let trade_balance_change_swap_task = stream_events(
        "trade_swap",
        TradeBalanceChangeSwapHandler,
        redis_connection.clone(),
        pg_pool.clone(),
    );
    let trade_pool_change_task = stream_events(
        "trade_pool_change",
        TradePoolChangeHandler,
        redis_connection.clone(),
        pg_pool.clone(),
    );

    tokio::join!(
        nft_mint_task,
        nft_transfer_task,
        nft_burn_task,
        potlock_donation_task,
        potlock_pot_project_donation_task,
        potlock_pot_donation_task,
        trade_raw_pool_swap_task,
        trade_balance_change_swap_task,
        trade_pool_change_task,
    );
    Ok(())
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
            log::error!("Failed to parse nft mint event");
        }
        Ok(())
    }
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
                &event.token_prices_near.iter().map(|price| price.unwrap_or_default()).map(|price| BigDecimal::from_str(&price.to_string()).unwrap()).collect::<Vec<_>>()
            )
            .execute(pg_pool)
            .await?;
        } else {
            log::error!("Failed to parse nft transfer event");
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
            log::error!("Failed to parse nft burn event");
        }
        Ok(())
    }
}

struct PotlockDonationHandler;

#[async_trait::async_trait]
impl EventHandler for PotlockDonationHandler {
    async fn handle(
        &self,
        values: HashMap<String, Value>,
        pg_pool: &sqlx::PgPool,
    ) -> anyhow::Result<()> {
        if let (Ok(context), Ok(event)) = (
            serde_json::from_str::<PotlockEventContext>(&String::from_redis_value(
                values.get("context").unwrap(),
            )?),
            serde_json::from_str::<PotlockDonationEvent>(&String::from_redis_value(
                values.get("donation").unwrap(),
            )?),
        ) {
            sqlx::query!(
                r#"
                INSERT INTO potlock_donation (timestamp, transaction_id, receipt_id, block_height, donation_id, donor_id, total_amount, message, donated_at, project_id, protocol_fee, referrer_id, referrer_fee)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                "#,
                chrono::DateTime::from_timestamp((context.block_timestamp_nanosec / 1_000_000_000) as i64, (context.block_timestamp_nanosec % 1_000_000_000) as u32),
                context.transaction_id,
                context.receipt_id,
                context.block_height as i64,
                event.donation_id as i64,
                event.donor_id,
                BigDecimal::from_str(&event.total_amount.to_string()).unwrap(),
                event.message,
                event.donated_at,
                event.project_id,
                BigDecimal::from_str(&event.protocol_fee.to_string()).unwrap(),
                event.referrer_id,
                event.referrer_fee.map(|fee| BigDecimal::from_str(&fee.to_string()).unwrap())
            )
            .execute(pg_pool)
            .await?;
        } else {
            log::error!("Failed to parse potlock donation event");
        }
        Ok(())
    }
}

struct PotlockPotProjectDonationHandler;

#[async_trait::async_trait]
impl EventHandler for PotlockPotProjectDonationHandler {
    async fn handle(
        &self,
        values: HashMap<String, Value>,
        pg_pool: &sqlx::PgPool,
    ) -> anyhow::Result<()> {
        if let (Ok(context), Ok(event)) = (
            serde_json::from_str::<PotlockEventContext>(&String::from_redis_value(
                values.get("context").unwrap(),
            )?),
            serde_json::from_str::<PotlockPotProjectDonationEvent>(&String::from_redis_value(
                values.get("pot_project_donation").unwrap(),
            )?),
        ) {
            sqlx::query!(
                r#"
                INSERT INTO potlock_pot_project_donation (timestamp, transaction_id, receipt_id, block_height, donation_id, pot_id, donor_id, total_amount, net_amount, message, donated_at, project_id, referrer_id, referrer_fee, protocol_fee, chef_id, chef_fee)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                "#,
                chrono::DateTime::from_timestamp((context.block_timestamp_nanosec / 1_000_000_000) as i64, (context.block_timestamp_nanosec % 1_000_000_000) as u32),
                context.transaction_id,
                context.receipt_id,
                context.block_height as i64,
                event.donation_id as i64,
                event.pot_id,
                event.donor_id,
                BigDecimal::from_str(&event.total_amount.to_string()).unwrap(),
                BigDecimal::from_str(&event.net_amount.to_string()).unwrap(),
                event.message,
                event.donated_at,
                event.project_id,
                event.referrer_id,
                event.referrer_fee.map(|fee| BigDecimal::from_str(&fee.to_string()).unwrap()),
                BigDecimal::from_str(&event.protocol_fee.to_string()).unwrap(),
                event.chef_id,
                event.chef_fee.map(|fee| BigDecimal::from_str(&fee.to_string()).unwrap())
            )
            .execute(pg_pool)
            .await?;
        } else {
            log::error!("Failed to parse potlock pot project donation event");
        }
        Ok(())
    }
}

struct PotlockPotDonationHandler;

#[async_trait::async_trait]
impl EventHandler for PotlockPotDonationHandler {
    async fn handle(
        &self,
        values: HashMap<String, Value>,
        pg_pool: &sqlx::PgPool,
    ) -> anyhow::Result<()> {
        if let (Ok(context), Ok(event)) = (
            serde_json::from_str::<PotlockEventContext>(&String::from_redis_value(
                values.get("context").unwrap(),
            )?),
            serde_json::from_str::<PotlockPotDonationEvent>(&String::from_redis_value(
                values.get("pot_donation").unwrap(),
            )?),
        ) {
            sqlx::query!(
                r#"
                INSERT INTO potlock_pot_donation (timestamp, transaction_id, receipt_id, block_height, donation_id, pot_id, donor_id, total_amount, net_amount, message, donated_at, referrer_id, referrer_fee, protocol_fee, chef_id, chef_fee)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                "#,
                chrono::DateTime::from_timestamp((context.block_timestamp_nanosec / 1_000_000_000) as i64, (context.block_timestamp_nanosec % 1_000_000_000) as u32),
                context.transaction_id,
                context.receipt_id,
                context.block_height as i64,
                event.donation_id as i64,
                event.pot_id,
                event.donor_id,
                BigDecimal::from_str(&event.total_amount.to_string()).unwrap(),
                BigDecimal::from_str(&event.net_amount.to_string()).unwrap(),
                event.message,
                event.donated_at,
                event.referrer_id,
                event.referrer_fee.map(|fee| BigDecimal::from_str(&fee.to_string()).unwrap()),
                BigDecimal::from_str(&event.protocol_fee.to_string()).unwrap(),
                event.chef_id,
                event.chef_fee.map(|fee| BigDecimal::from_str(&fee.to_string()).unwrap())
            )
            .execute(pg_pool)
            .await?;
        } else {
            log::error!("Failed to parse potlock pot donation event");
        }
        Ok(())
    }
}

struct TradeRawPoolSwapHandler;

#[async_trait::async_trait]
impl EventHandler for TradeRawPoolSwapHandler {
    async fn handle(
        &self,
        values: HashMap<String, Value>,
        pg_pool: &sqlx::PgPool,
    ) -> anyhow::Result<()> {
        if let (Ok(context), Ok(event)) = (
            serde_json::from_str::<TradeContext>(&String::from_redis_value(
                values.get("context").unwrap(),
            )?),
            serde_json::from_str::<TradeRawPoolSwapEvent>(&String::from_redis_value(
                values.get("swap").unwrap(),
            )?),
        ) {
            sqlx::query!(
                r#"
                INSERT INTO trade_pool (timestamp, trader, transaction_id, receipt_id, block_height, pool, token_in, token_out, amount_in, amount_out)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                "#,
                chrono::DateTime::from_timestamp((context.block_timestamp_nanosec / 1_000_000_000) as i64, (context.block_timestamp_nanosec % 1_000_000_000) as u32),
                context.trader,
                context.transaction_id,
                context.receipt_id,
                context.block_height as i64,
                event.0.pool,
                event.0.token_in,
                event.0.token_out,
                BigDecimal::from_str(&event.0.amount_in.to_string()).unwrap(),
                BigDecimal::from_str(&event.0.amount_out.to_string()).unwrap(),
            )
            .execute(pg_pool)
            .await?;
        } else {
            log::error!("Failed to parse raw pool swap event");
        }
        Ok(())
    }
}

struct TradeBalanceChangeSwapHandler;

#[async_trait::async_trait]
impl EventHandler for TradeBalanceChangeSwapHandler {
    async fn handle(
        &self,
        values: HashMap<String, Value>,
        pg_pool: &sqlx::PgPool,
    ) -> anyhow::Result<()> {
        if let (Ok(context), Ok(event)) = (
            serde_json::from_str::<TradeContext>(&String::from_redis_value(
                values.get("context").unwrap(),
            )?),
            serde_json::from_str::<TradeBalanceChangeSwapEvent>(&String::from_redis_value(
                values.get("balance_change").unwrap(),
            )?),
        ) {
            sqlx::query!(
                r#"
                INSERT INTO trade_swap (timestamp, trader, transaction_id, receipt_id, block_height, balance_changes)
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
                chrono::DateTime::from_timestamp((context.block_timestamp_nanosec / 1_000_000_000) as i64, (context.block_timestamp_nanosec % 1_000_000_000) as u32),
                context.trader,
                context.transaction_id,
                context.receipt_id,
                context.block_height as i64,
                serde_json::Value::Object(event.balance_changes.into_iter().map(|(k, v)| (k, serde_json::Value::String(v.to_string()))).collect())
            )
            .execute(pg_pool)
            .await?;
        } else {
            log::error!("Failed to parse balance change swap event");
        }
        Ok(())
    }
}

struct TradePoolChangeHandler;

#[async_trait::async_trait]
impl EventHandler for TradePoolChangeHandler {
    async fn handle(
        &self,
        values: HashMap<String, Value>,
        pg_pool: &sqlx::PgPool,
    ) -> anyhow::Result<()> {
        if let Ok(event) = serde_json::from_str::<TradePoolChangeEvent>(&String::from_redis_value(
            values.get("pool_change").unwrap(),
        )?) {
            sqlx::query!(
                r#"
                INSERT INTO trade_pool_change (timestamp, receipt_id, block_height, pool_id, pool)
                VALUES ($1, $2, $3, $4, $5)
                "#,
                chrono::DateTime::from_timestamp(
                    (event.block_timestamp_nanosec / 1_000_000_000) as i64,
                    (event.block_timestamp_nanosec % 1_000_000_000) as u32
                ),
                event.receipt_id,
                event.block_height as i64,
                event.pool_id,
                serde_json::to_value(event.pool)?,
            )
            .execute(pg_pool)
            .await?;
        } else {
            log::error!("Failed to parse pool change event");
        }
        Ok(())
    }
}
