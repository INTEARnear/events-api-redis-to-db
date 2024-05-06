use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

type TransactionId = String;
type ReceiptId = String;
type AccountId = String;
type NftTokenId = String;
type BlockHeight = u64;
type Balance = Decimal;

#[derive(Debug, Serialize, Deserialize)]
pub struct NftEventContext {
    pub transaction_id: TransactionId,
    pub receipt_id: ReceiptId,
    pub block_height: BlockHeight,
    #[serde(with = "inindexer::near_utils::dec_format")]
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
    pub token_prices_near: Vec<Option<Balance>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NftBurnEvent {
    pub owner_id: AccountId,
    pub token_ids: Vec<NftTokenId>,
    pub memo: Option<String>,
}
