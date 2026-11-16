//! The deliveries a signer has sent that have not settled.
//!
//! Each owns the Moonlight nonce its transactions are signed with, and
//! names the last transaction sent, once the helper has. A Mailbox
//! reconciles them with the node before it sends a delivery again, so that
//! no message's transactions with different nonces could both be in a
//! block. They live in the event store, when the client keeps one, so
//! that they survive restarts, and otherwise in the endpoint, for the
//! process.

use super::*;

/// The prefix of the sent deliveries in the event store.
const SENT_DELIVERY_PREFIX: &[u8] = b"dusk-sent-delivery-v1:";
/// Bytes of an encoded sent delivery: its nonce, when it was sent, whether
/// it names a transaction, and the transaction.
const SENT_DELIVERY_BYTES: usize = 8 + 8 + 1 + 32;

/// One delivery a signer sent that has not settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SentDelivery {
    /// The Moonlight nonce every transaction of the delivery is signed
    /// with.
    pub nonce: u64,
    /// The last transaction sent, once the helper has named it.
    pub tx_id: Option<[u8; 32]>,
    /// When the last transaction was sent, in milliseconds since the Unix
    /// epoch.
    pub sent_at: u64,
}

impl SentDelivery {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(SENT_DELIVERY_BYTES);
        bytes.extend_from_slice(&self.nonce.to_be_bytes());
        bytes.extend_from_slice(&self.sent_at.to_be_bytes());
        bytes.push(u8::from(self.tx_id.is_some()));
        bytes.extend_from_slice(&self.tx_id.unwrap_or_default());
        bytes
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != SENT_DELIVERY_BYTES {
            return None;
        }
        let number = |offset: usize| -> Option<u64> {
            Some(u64::from_be_bytes(
                bytes.get(offset..offset + 8)?.try_into().ok()?,
            ))
        };
        let tx_id: [u8; 32] = bytes.get(17..49)?.try_into().ok()?;
        Some(Self {
            nonce: number(0)?,
            sent_at: number(8)?,
            tx_id: match bytes.get(16)? {
                0 => None,
                1 => Some(tx_id),
                _ => return None,
            },
        })
    }
}

/// Where a transaction is, as the node sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TransactionStatus {
    /// In a block of the node's chain at `block_height`, with its receipt.
    Included {
        block_height: u64,
        receipt: ConfirmedTransaction,
    },
    /// In the node's mempool.
    Pending,
    /// In neither.
    Unknown,
}

fn sent_delivery_prefix(signer: &[u8; 32]) -> Vec<u8> {
    let mut key = SENT_DELIVERY_PREFIX.to_vec();
    key.extend_from_slice(signer);
    key
}

fn sent_delivery_key(signer: &[u8; 32], message: H256) -> Vec<u8> {
    let mut key = sent_delivery_prefix(signer);
    key.extend_from_slice(message.as_bytes());
    key
}

fn sent_delivery_error(error: impl fmt::Display) -> HyperlaneDuskError {
    HyperlaneDuskError::Other(format!(
        "Failed to use the Dusk deliveries in flight in eventCursorDir: {error}"
    ))
}

impl RuesClient {
    /// The lock the submissions of the signer `signer` (its address) hold
    /// while they use its nonces, shared by every client of the endpoint in
    /// this process.
    pub(crate) fn submission_lock(&self, signer: [u8; 32]) -> Arc<tokio::sync::Mutex<()>> {
        self.endpoint.submission_lock(signer)
    }

    /// The deliveries `signer` sent that have not settled, by message.
    /// Fails on one the event store holds that does not decode: its nonce
    /// is unknown, and nothing may be sent until it is looked at.
    pub(crate) fn sent_deliveries(
        &self,
        signer: &[u8; 32],
    ) -> Result<Vec<(H256, SentDelivery)>, HyperlaneDuskError> {
        let Some(store) = &self.event_store else {
            return Ok(self.endpoint.sent_deliveries(signer));
        };
        let prefix = sent_delivery_prefix(signer);
        let mut sent = Vec::new();
        for item in store.iterator(IteratorMode::From(&prefix, Direction::Forward)) {
            let (key, value) = item.map_err(sent_delivery_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let message = key
                .get(prefix.len()..)
                .filter(|message| message.len() == 32)
                .map(H256::from_slice);
            match (message, SentDelivery::decode(&value)) {
                (Some(message), Some(delivery)) => sent.push((message, delivery)),
                _ => {
                    return Err(sent_delivery_error(
                        "a delivery in flight there is corrupt; remove eventCursorDir only once the signer's transactions have settled",
                    ))
                }
            }
        }
        Ok(sent)
    }

    /// Keep that `signer` sent `message` as `delivery`, durably.
    pub(crate) fn record_sent_delivery(
        &self,
        signer: &[u8; 32],
        message: H256,
        delivery: &SentDelivery,
    ) -> Result<(), HyperlaneDuskError> {
        let Some(store) = &self.event_store else {
            self.endpoint
                .record_sent_delivery(signer, message, *delivery);
            return Ok(());
        };
        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        store
            .put_opt(
                sent_delivery_key(signer, message),
                delivery.encode(),
                &write_options,
            )
            .map_err(sent_delivery_error)
    }

    /// Forget `signer`'s delivery of `message`: it settled.
    pub(crate) fn forget_sent_delivery(
        &self,
        signer: &[u8; 32],
        message: H256,
    ) -> Result<(), HyperlaneDuskError> {
        let Some(store) = &self.event_store else {
            self.endpoint.forget_sent_delivery(signer, message);
            return Ok(());
        };
        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        store
            .delete_opt(sent_delivery_key(signer, message), &write_options)
            .map_err(sent_delivery_error)
    }

    /// Where transaction `tx_id` is: in a block of the node's chain, with
    /// its receipt, in its mempool, or in neither.
    pub(crate) async fn transaction_status(
        &self,
        tx_id: &str,
    ) -> Result<TransactionStatus, HyperlaneDuskError> {
        validate_transaction_id(tx_id)?;
        let query = format!(
            r#"query {{ mempoolTx(hash: "{tx_id}") {{ id }} tx(hash: "{tx_id}") {{ blockHeight gasSpent err }} }}"#
        );
        let data = self.graphql_query(&query).await?;
        if let Some(transaction) = data.get("tx").filter(|value| !value.is_null()) {
            let block_height = transaction
                .get("blockHeight")
                .and_then(JsonValue::as_u64)
                .ok_or_else(|| {
                    HyperlaneDuskError::Other(format!(
                        "Ledger transaction {tx_id} is missing numeric blockHeight: {}",
                        bounded_json(transaction)
                    ))
                })?;
            return Ok(TransactionStatus::Included {
                block_height,
                receipt: parse_confirmed_transaction(tx_id, transaction)?,
            });
        }
        if data.get("mempoolTx").is_some_and(|value| !value.is_null()) {
            return Ok(TransactionStatus::Pending);
        }
        Ok(TransactionStatus::Unknown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sent_deliveries_round_trip_and_reject_corruption() {
        for delivery in [
            SentDelivery {
                nonce: 7,
                tx_id: None,
                sent_at: 1_700_000_000_000,
            },
            SentDelivery {
                nonce: u64::MAX,
                tx_id: Some([9; 32]),
                sent_at: 0,
            },
        ] {
            assert_eq!(SentDelivery::decode(&delivery.encode()), Some(delivery));
        }
        let mut corrupt = SentDelivery {
            nonce: 1,
            tx_id: Some([1; 32]),
            sent_at: 2,
        }
        .encode();
        corrupt[16] = 2;
        assert!(SentDelivery::decode(&corrupt).is_none());
        assert!(SentDelivery::decode(&corrupt[1..]).is_none());
    }

    #[test]
    fn sent_deliveries_survive_a_restart_and_stay_with_their_signer() {
        let directory = tempfile::tempdir().unwrap();
        let url = Url::parse("http://127.0.0.1:9/sent-deliveries").unwrap();
        let delivery = SentDelivery {
            nonce: 3,
            tx_id: Some([4; 32]),
            sent_at: 5,
        };
        {
            let client =
                super::super::tests::test_client_with_event_store(url.clone(), directory.path());
            client
                .record_sent_delivery(&[1; 32], H256::repeat_byte(2), &delivery)
                .unwrap();
            client
                .record_sent_delivery(&[6; 32], H256::repeat_byte(2), &delivery)
                .unwrap();
        }
        let client = super::super::tests::test_client_with_event_store(url, directory.path());
        assert_eq!(
            client.sent_deliveries(&[1; 32]).unwrap(),
            [(H256::repeat_byte(2), delivery)]
        );
        client
            .forget_sent_delivery(&[1; 32], H256::repeat_byte(2))
            .unwrap();
        assert!(client.sent_deliveries(&[1; 32]).unwrap().is_empty());
        assert_eq!(client.sent_deliveries(&[6; 32]).unwrap().len(), 1);
    }
}
