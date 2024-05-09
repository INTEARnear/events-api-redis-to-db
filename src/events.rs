use std::{collections::HashMap, num::ParseIntError};

use chrono::prelude::{DateTime, Utc};
use inindexer::near_utils::{dec_format, dec_format_vec};
use serde::{Deserialize, Serialize};

type TransactionId = String;
type ReceiptId = String;
type AccountId = String;
type NftTokenId = String;
type BlockHeight = u64;
type Balance = u128;
type DonationId = u64;
type ProjectId = AccountId;
type PoolId = String;

#[derive(Debug, Serialize, Deserialize)]
pub struct NftEventContext {
    pub transaction_id: TransactionId,
    pub receipt_id: ReceiptId,
    pub block_height: BlockHeight,
    #[serde(with = "dec_format")]
    pub block_timestamp_nanosec: u128,
    pub contract_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NftMintEvent {
    pub owner_id: AccountId,
    pub token_ids: Vec<NftTokenId>,
    pub memo: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NftTransferEvent {
    pub old_owner_id: AccountId,
    pub new_owner_id: AccountId,
    pub token_ids: Vec<NftTokenId>,
    pub memo: Option<String>,
    #[serde(with = "dec_format_vec")]
    pub token_prices_near: Vec<Option<Balance>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NftBurnEvent {
    pub owner_id: AccountId,
    pub token_ids: Vec<NftTokenId>,
    pub memo: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PotlockEventContext {
    pub transaction_id: TransactionId,
    pub receipt_id: ReceiptId,
    pub block_height: BlockHeight,
    #[serde(with = "dec_format")]
    pub block_timestamp_nanosec: u128,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PotlockDonationEvent {
    pub donation_id: DonationId,
    pub donor_id: AccountId,
    #[serde(with = "dec_format")]
    pub total_amount: Balance,
    pub ft_id: AccountId,
    pub message: Option<String>,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub donated_at: DateTime<Utc>,
    pub project_id: ProjectId,
    #[serde(with = "dec_format")]
    pub protocol_fee: Balance,
    pub referrer_id: Option<AccountId>,
    pub referrer_fee: Option<Balance>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PotlockPotProjectDonationEvent {
    pub donation_id: DonationId,
    pub pot_id: AccountId,
    pub donor_id: AccountId,
    #[serde(with = "dec_format")]
    pub total_amount: Balance,
    #[serde(with = "dec_format")]
    pub net_amount: Balance,
    pub message: Option<String>,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub donated_at: DateTime<Utc>,
    pub project_id: ProjectId,
    pub referrer_id: Option<AccountId>,
    pub referrer_fee: Option<Balance>,
    #[serde(with = "dec_format")]
    pub protocol_fee: Balance,
    pub chef_id: Option<AccountId>,
    pub chef_fee: Option<Balance>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PotlockPotDonationEvent {
    pub donation_id: DonationId,
    pub pot_id: AccountId,
    pub donor_id: AccountId,
    #[serde(with = "dec_format")]
    pub total_amount: Balance,
    #[serde(with = "dec_format")]
    pub net_amount: Balance,
    pub message: Option<String>,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub donated_at: DateTime<Utc>,
    pub referrer_id: Option<AccountId>,
    pub referrer_fee: Option<Balance>,
    #[serde(with = "dec_format")]
    pub protocol_fee: Balance,
    pub chef_id: Option<AccountId>,
    pub chef_fee: Option<Balance>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeContext {
    pub trader: AccountId,
    pub block_height: BlockHeight,
    #[serde(with = "dec_format")]
    pub block_timestamp_nanosec: u128,
    pub transaction_id: TransactionId,
    pub receipt_id: ReceiptId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RawPoolSwap {
    pub pool: PoolId,
    pub token_in: AccountId,
    pub token_out: AccountId,
    #[serde(with = "dec_format")]
    pub amount_in: Balance,
    #[serde(with = "dec_format")]
    pub amount_out: Balance,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TradeRawPoolSwapEvent(pub RawPoolSwap);

#[derive(Debug, Deserialize)]
pub struct TradeBalanceChangeSwapEvent {
    #[serde(deserialize_with = "deserialize_balance_changes")]
    pub balance_changes: HashMap<AccountId, i128>,
    pub pool_swaps: Vec<RawPoolSwap>,
}

fn deserialize_balance_changes<'de, D>(
    deserializer: D,
) -> Result<HashMap<AccountId, i128>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let balance_changes: HashMap<AccountId, String> = HashMap::deserialize(deserializer)?;
    balance_changes
        .into_iter()
        .map(|(k, v)| Ok((k, v.parse()?)))
        .collect::<Result<_, _>>()
        .map_err(|_: ParseIntError| serde::de::Error::custom("Failed to parse i128"))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TradePoolChangeEvent {
    pub pool_id: String,
    pub receipt_id: String,
    #[serde(with = "dec_format")]
    pub block_timestamp_nanosec: u128,
    pub block_height: u64,
    pub pool: serde_json::Value,
}
