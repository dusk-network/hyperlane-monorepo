//! What a signer that pipelines deliveries has in flight, and how the agent
//! sends the transactions it signed.
//!
//! Every pipelined transaction is signed with a nonce of the signer's
//! account that a reservation holds: for a delivery, kept with its
//! arguments, or for a filler, a call that only uses the nonce so that the
//! later ones can land. A reservation lists every transaction signed with
//! its nonce that may have reached the node, each kept before it is sent,
//! and is released only once one of them is in a final block, or when it
//! never sent one. Reservations are keyed by their nonce, so no two can
//! hold one. They live in the event store, when the client keeps one, so
//! that they survive restarts, and otherwise in the endpoint, for the
//! process.
//!
//! The previous version kept one record per message instead
//! (`dusk-sent-delivery-v1:`): its nonce and its last transaction. Such
//! records are only read, to wait them out, and forgotten once nothing of
//! them can be admitted any more.
//!
//! A delivery sent one at a time keeps the transaction `dusk-tx` prepared
//! for it (`dusk-prepared-delivery-v1:`), by the hash the helper prints
//! before it propagates it, so that the delivery is not sent again while
//! that transaction may still land.

use super::*;

/// The prefix of the transactions prepared for deliveries sent one at a
/// time.
const PREPARED_DELIVERY_PREFIX: &[u8] = b"dusk-prepared-delivery-v1:";
/// Bytes of an encoded prepared delivery: when it was prepared, and its
/// transaction.
const PREPARED_DELIVERY_BYTES: usize = 8 + 32;
/// Most deliveries sent one at a time whose transaction a signer keeps:
/// past it, the one prepared longest ago is forgotten.
pub(crate) const MAX_PREPARED_DELIVERIES: usize = 1_024;

/// The transaction `dusk-tx` prepared for the last submission of a
/// delivery sent one at a time: the hash it printed before it propagated
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PreparedDelivery {
    pub tx_id: [u8; 32],
    /// When the helper named it, in milliseconds since the Unix epoch.
    pub prepared_at: u64,
}

impl PreparedDelivery {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(PREPARED_DELIVERY_BYTES);
        bytes.extend_from_slice(&self.prepared_at.to_be_bytes());
        bytes.extend_from_slice(&self.tx_id);
        bytes
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != PREPARED_DELIVERY_BYTES {
            return None;
        }
        let mut fields = Fields(bytes);
        Some(Self {
            prepared_at: fields.u64()?,
            tx_id: fields.hash()?,
        })
    }
}

/// The prefix of the previous version's deliveries in flight.
const SENT_DELIVERY_PREFIX: &[u8] = b"dusk-sent-delivery-v1:";
/// Bytes of an encoded sent delivery: its nonce, when it was sent, whether
/// it names a transaction, and the transaction.
const SENT_DELIVERY_BYTES: usize = 8 + 8 + 1 + 32;
/// The prefix of the nonce reservations in the event store.
const RESERVATION_PREFIX: &[u8] = b"dusk-nonce-reservation-v2:";
/// Most transactions a reservation lists. Each one past the first was
/// signed after every earlier one was dropped by the node.
pub(crate) const MAX_RESERVATION_TXS: usize = 8;
/// Most bytes of arguments a reservation keeps: a delivery's are at most
/// the helper's 60 KiB.
const MAX_RESERVATION_ARGS: usize = 64 * 1024;

/// One delivery the previous version sent that has not settled.
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

/// What holds a nonce of the signer's account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Holder {
    /// A delivery of `message`, with the `process` arguments and gas limit
    /// it was last signed with, so that it can go again on its own.
    Delivery {
        message: H256,
        gas_limit: u64,
        args: Vec<u8>,
    },
    /// A call that only uses the nonce, so that the later ones can land.
    Filler,
}

impl Holder {
    /// The message it delivers, if it is a delivery.
    pub(crate) fn message(&self) -> Option<H256> {
        match self {
            Self::Delivery { message, .. } => Some(*message),
            Self::Filler => None,
        }
    }
}

/// A nonce of the signer's account, held for a delivery or a filler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Reservation {
    pub nonce: u64,
    pub holder: Holder,
    /// Every transaction signed with the nonce that may have reached the
    /// node, oldest first: a transaction's hash is kept before it is sent.
    pub txs: Vec<[u8; 32]>,
    /// When the last of them was sent, in milliseconds since the Unix
    /// epoch.
    pub sent_at: u64,
}

/// Reads an encoded value field by field.
struct Fields<'a>(&'a [u8]);

impl<'a> Fields<'a> {
    fn take(&mut self, length: usize) -> Option<&'a [u8]> {
        if self.0.len() < length {
            return None;
        }
        let (taken, rest) = self.0.split_at(length);
        self.0 = rest;
        Some(taken)
    }

    fn byte(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }

    fn hash(&mut self) -> Option<[u8; 32]> {
        self.take(32)?.try_into().ok()
    }
}

impl Reservation {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.sent_at.to_be_bytes());
        bytes.push(u8::try_from(self.txs.len()).unwrap_or(u8::MAX));
        for tx in &self.txs {
            bytes.extend_from_slice(tx);
        }
        match &self.holder {
            Holder::Filler => bytes.push(0),
            Holder::Delivery {
                message,
                gas_limit,
                args,
            } => {
                bytes.push(1);
                bytes.extend_from_slice(message.as_bytes());
                bytes.extend_from_slice(&gas_limit.to_be_bytes());
                bytes.extend_from_slice(
                    &u32::try_from(args.len()).unwrap_or(u32::MAX).to_be_bytes(),
                );
                bytes.extend_from_slice(args);
            }
        }
        bytes
    }

    fn decode(nonce: u64, bytes: &[u8]) -> Option<Self> {
        let mut fields = Fields(bytes);
        let sent_at = fields.u64()?;
        let count = usize::from(fields.byte()?);
        if count > MAX_RESERVATION_TXS {
            return None;
        }
        let txs = (0..count)
            .map(|_| fields.hash())
            .collect::<Option<Vec<_>>>()?;
        let holder = match fields.byte()? {
            0 => Holder::Filler,
            1 => {
                let message = H256::from(fields.hash()?);
                let gas_limit = fields.u64()?;
                let length = usize::try_from(fields.u32()?).ok()?;
                if length > MAX_RESERVATION_ARGS {
                    return None;
                }
                Holder::Delivery {
                    message,
                    gas_limit,
                    args: fields.take(length)?.to_vec(),
                }
            }
            _ => return None,
        };
        fields.0.is_empty().then_some(Self {
            nonce,
            holder,
            txs,
            sent_at,
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

/// What became of a signed transaction the agent sent the node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Submission {
    /// The node took it at propagation.
    Propagated,
    /// It was not propagated: the node refused it at preverification, or
    /// preverification could not be asked; why.
    Refused(String),
    /// It was propagated, but whether the node took it is unknown; why.
    Unknown(String),
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

fn prepared_delivery_prefix(signer: &[u8; 32]) -> Vec<u8> {
    let mut key = PREPARED_DELIVERY_PREFIX.to_vec();
    key.extend_from_slice(signer);
    key
}

fn prepared_delivery_key(signer: &[u8; 32], message: H256) -> Vec<u8> {
    let mut key = prepared_delivery_prefix(signer);
    key.extend_from_slice(message.as_bytes());
    key
}

fn reservation_prefix(signer: &[u8; 32]) -> Vec<u8> {
    let mut key = RESERVATION_PREFIX.to_vec();
    key.extend_from_slice(signer);
    key
}

fn reservation_key(signer: &[u8; 32], nonce: u64) -> Vec<u8> {
    let mut key = reservation_prefix(signer);
    key.extend_from_slice(&nonce.to_be_bytes());
    key
}

fn in_flight_error(error: impl fmt::Display) -> HyperlaneDuskError {
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

    /// Stop pipelining the deliveries of `signer` in this process: another
    /// sender uses its nonces.
    pub(crate) fn stop_pipelining(&self, signer: [u8; 32]) {
        self.endpoint.stop_pipelining(signer);
    }

    /// Whether pipelining stopped for `signer` in this process.
    pub(crate) fn pipelining_stopped(&self, signer: &[u8; 32]) -> bool {
        self.endpoint.pipelining_stopped(signer)
    }

    /// The deliveries the previous version kept in flight for `signer`, by
    /// message. Fails on one the event store holds that does not decode:
    /// its nonce is unknown, and nothing may be sent until it is looked at.
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
            let (key, value) = item.map_err(in_flight_error)?;
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
                    return Err(in_flight_error(
                        "a delivery in flight there is corrupt; remove eventCursorDir only once the signer's transactions have settled",
                    ))
                }
            }
        }
        Ok(sent)
    }

    /// Keep that `signer` sent `message` as `delivery`, as the previous
    /// version did, for tests of what it left.
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
        store
            .put_opt(
                sent_delivery_key(signer, message),
                delivery.encode(),
                &synced(),
            )
            .map_err(in_flight_error)
    }

    /// Forget the previous version's delivery of `message` by `signer`.
    pub(crate) fn forget_sent_delivery(
        &self,
        signer: &[u8; 32],
        message: H256,
    ) -> Result<(), HyperlaneDuskError> {
        let Some(store) = &self.event_store else {
            self.endpoint.forget_sent_delivery(signer, message);
            return Ok(());
        };
        store
            .delete_opt(sent_delivery_key(signer, message), &synced())
            .map_err(in_flight_error)
    }

    /// The transaction `signer` prepared for the last submission of
    /// `message` sent one at a time, if one is kept. Fails on one the event
    /// store holds that does not decode: the delivery may not be sent again
    /// until it is looked at.
    pub(crate) fn prepared_delivery(
        &self,
        signer: &[u8; 32],
        message: H256,
    ) -> Result<Option<PreparedDelivery>, HyperlaneDuskError> {
        let Some(store) = &self.event_store else {
            return Ok(self.endpoint.prepared_delivery(signer, message));
        };
        let Some(bytes) = store
            .get(prepared_delivery_key(signer, message))
            .map_err(in_flight_error)?
        else {
            return Ok(None);
        };
        PreparedDelivery::decode(&bytes).map(Some).ok_or_else(|| {
            in_flight_error(format!(
                "the transaction kept for the delivery of message {message:?} is corrupt; remove it only once the signer's transactions have settled"
            ))
        })
    }

    /// Keep that `signer` prepared `delivery` for `message`, durably,
    /// instead of the one kept before. A signer keeps at most
    /// `MAX_PREPARED_DELIVERIES`: past it, the one prepared longest ago,
    /// or one that does not decode, is forgotten.
    pub(crate) fn keep_prepared_delivery(
        &self,
        signer: &[u8; 32],
        message: H256,
        delivery: &PreparedDelivery,
    ) -> Result<(), HyperlaneDuskError> {
        let Some(store) = &self.event_store else {
            self.endpoint
                .keep_prepared_delivery(signer, message, *delivery);
            return Ok(());
        };
        let key = prepared_delivery_key(signer, message);
        let prefix = prepared_delivery_prefix(signer);
        let mut others = Vec::new();
        for item in store.iterator(IteratorMode::From(&prefix, Direction::Forward)) {
            let (other, value) = item.map_err(in_flight_error)?;
            if !other.starts_with(&prefix) {
                break;
            }
            if *other != *key {
                let prepared_at =
                    PreparedDelivery::decode(&value).map_or(0, |delivery| delivery.prepared_at);
                others.push((prepared_at, other));
            }
        }
        if others.len() >= MAX_PREPARED_DELIVERIES {
            others.sort();
            for (_, other) in &others[..=others.len() - MAX_PREPARED_DELIVERIES] {
                store
                    .delete_opt(other, &synced())
                    .map_err(in_flight_error)?;
            }
        }
        store
            .put_opt(key, delivery.encode(), &synced())
            .map_err(in_flight_error)
    }

    /// Forget the transaction `signer` prepared for `message`, if one is
    /// kept.
    pub(crate) fn forget_prepared_delivery(
        &self,
        signer: &[u8; 32],
        message: H256,
    ) -> Result<(), HyperlaneDuskError> {
        let Some(store) = &self.event_store else {
            self.endpoint.forget_prepared_delivery(signer, message);
            return Ok(());
        };
        let key = prepared_delivery_key(signer, message);
        if store.get(&key).map_err(in_flight_error)?.is_none() {
            return Ok(());
        }
        store.delete_opt(key, &synced()).map_err(in_flight_error)
    }

    /// The nonces `signer` holds, in nonce order. Fails on one the event
    /// store holds that does not decode: its transactions are unknown, and
    /// nothing may be sent until it is looked at.
    pub(crate) fn reservations(
        &self,
        signer: &[u8; 32],
    ) -> Result<Vec<Reservation>, HyperlaneDuskError> {
        let Some(store) = &self.event_store else {
            return Ok(self.endpoint.reservations(signer));
        };
        let prefix = reservation_prefix(signer);
        let mut reservations = Vec::new();
        for item in store.iterator(IteratorMode::From(&prefix, Direction::Forward)) {
            let (key, value) = item.map_err(in_flight_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let nonce = key
                .get(prefix.len()..)
                .and_then(|nonce| <[u8; 8]>::try_from(nonce).ok())
                .map(u64::from_be_bytes);
            match nonce.and_then(|nonce| Reservation::decode(nonce, &value)) {
                Some(reservation) => reservations.push(reservation),
                None => {
                    return Err(in_flight_error(
                        "a nonce reservation there is corrupt; remove eventCursorDir only once the signer's transactions have settled",
                    ))
                }
            }
        }
        Ok(reservations)
    }

    /// Keep `reservation` of `signer`, durably, before anything is signed
    /// or sent with its nonce.
    pub(crate) fn keep_reservation(
        &self,
        signer: &[u8; 32],
        reservation: &Reservation,
    ) -> Result<(), HyperlaneDuskError> {
        if reservation.txs.len() > MAX_RESERVATION_TXS {
            return Err(in_flight_error(format!(
                "nonce {} would list more than {MAX_RESERVATION_TXS} transactions",
                reservation.nonce
            )));
        }
        let Some(store) = &self.event_store else {
            self.endpoint.keep_reservation(signer, reservation.clone());
            return Ok(());
        };
        store
            .put_opt(
                reservation_key(signer, reservation.nonce),
                reservation.encode(),
                &synced(),
            )
            .map_err(in_flight_error)
    }

    /// Release `signer`'s reservation of `nonce`: one of its transactions
    /// is in a final block, or it never sent one.
    pub(crate) fn release_reservation(
        &self,
        signer: &[u8; 32],
        nonce: u64,
    ) -> Result<(), HyperlaneDuskError> {
        let Some(store) = &self.event_store else {
            self.endpoint.release_reservation(signer, nonce);
            return Ok(());
        };
        store
            .delete_opt(reservation_key(signer, nonce), &synced())
            .map_err(in_flight_error)
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

    /// Send signed transaction `tx` to the node by `deadline`, as the
    /// helper would: preverify it, then propagate it. Preverification
    /// changes nothing at the node, so a transaction it refuses, or whose
    /// preverification fails or runs past the deadline, was not
    /// propagated. One refused only because its nonce is ahead of the
    /// account's next one goes on to propagation, which queues it until the
    /// nonces before it arrive. A propagation that fails, is refused or
    /// runs past the deadline leaves it unknown whether the node took it.
    pub(crate) async fn submit_transaction(
        &self,
        tx: &[u8],
        deadline: tokio::time::Instant,
    ) -> Submission {
        let preverified = tokio::time::timeout_at(deadline, async {
            self.ensure_identity().await?;
            self.post_and_read(
                self.endpoint("/on/transactions/preverify"),
                tx,
                "transaction preverification",
            )
            .await
        })
        .await;
        match preverified {
            Err(_) => {
                return Submission::Refused(
                    "preverification did not answer before the deadline".into(),
                )
            }
            Ok(Err(error)) => {
                return Submission::Refused(format!(
                    "preverification failed: {}",
                    bounded_error(&error)
                ))
            }
            Ok(Ok((status, body))) if !status.is_success() => {
                if !String::from_utf8_lossy(&body).contains(MISSING_INTERMEDIATE_NONCE) {
                    return Submission::Refused(format!(
                        "preverification refused it ({status}): {}",
                        bounded_body(&body)
                    ));
                }
            }
            Ok(Ok(_)) => {}
        }
        let propagated = tokio::time::timeout_at(
            deadline,
            self.post_and_read(
                self.endpoint("/on/transactions/propagate"),
                tx,
                "transaction propagation",
            ),
        )
        .await;
        match propagated {
            Err(_) => Submission::Unknown("propagation did not answer before the deadline".into()),
            Ok(Err(error)) => {
                Submission::Unknown(format!("propagation failed: {}", bounded_error(&error)))
            }
            Ok(Ok((status, body))) if !status.is_success() => Submission::Unknown(format!(
                "propagation answered {status}: {}",
                bounded_body(&body)
            )),
            Ok(Ok(_)) => Submission::Propagated,
        }
    }

    /// POST `body` to `url`, a route of the `Other` class, and read the
    /// reply.
    async fn post_and_read(
        &self,
        url: Url,
        body: &[u8],
        context: &'static str,
    ) -> Result<(StatusCode, Vec<u8>), HyperlaneDuskError> {
        let response = self.send(RequestClass::Other, url, body.to_vec()).await?;
        let status = response.status();
        Ok((status, read_bounded_body(response, context).await?))
    }
}

/// How Rusk 1.7.3 refuses a Moonlight transaction whose nonce is ahead of
/// the account's next one, with a nonce between missing from the mempool
/// (`TxAcceptanceError::MissingIntermediateNonce`). Propagation queues
/// such a transaction.
const MISSING_INTERMEDIATE_NONCE: &str = "Missing intermediate nonce";

/// Write options that reach the disk before the write returns.
fn synced() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options
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
    fn reservations_round_trip_and_reject_corruption() {
        for reservation in [
            Reservation {
                nonce: 7,
                holder: Holder::Filler,
                txs: vec![],
                sent_at: 0,
            },
            Reservation {
                nonce: u64::MAX,
                holder: Holder::Delivery {
                    message: H256::repeat_byte(3),
                    gas_limit: 5_000_000,
                    args: vec![1, 2, 3],
                },
                txs: vec![[4; 32], [5; 32]],
                sent_at: 1_700_000_000_000,
            },
        ] {
            let encoded = reservation.encode();
            assert_eq!(
                Reservation::decode(reservation.nonce, &encoded),
                Some(reservation.clone())
            );
            // Cut short, or with a byte too many.
            assert!(
                Reservation::decode(reservation.nonce, &encoded[..encoded.len() - 1]).is_none()
            );
            let mut longer = encoded.clone();
            longer.push(0);
            assert!(Reservation::decode(reservation.nonce, &longer).is_none());
        }
        // More transactions than a reservation lists, or an unknown holder.
        let mut too_many = vec![0; 8];
        too_many.push(u8::try_from(MAX_RESERVATION_TXS + 1).unwrap());
        too_many.extend(std::iter::repeat_n(0, 32 * (MAX_RESERVATION_TXS + 1)));
        too_many.push(0);
        assert!(Reservation::decode(1, &too_many).is_none());
        let mut unknown = vec![0; 9];
        unknown.push(2);
        assert!(Reservation::decode(1, &unknown).is_none());
    }

    #[test]
    fn prepared_deliveries_round_trip_and_reject_corruption() {
        let delivery = PreparedDelivery {
            tx_id: [7; 32],
            prepared_at: 1_700_000_000_000,
        };
        assert_eq!(PreparedDelivery::decode(&delivery.encode()), Some(delivery));
        let encoded = delivery.encode();
        assert!(PreparedDelivery::decode(&encoded[1..]).is_none());
        let mut longer = encoded;
        longer.push(0);
        assert!(PreparedDelivery::decode(&longer).is_none());
    }

    /// What a signer keeps for its deliveries one at a time survives a
    /// restart, stays with the signer and the message, and is bounded,
    /// in the event store and in memory alike.
    #[test]
    fn prepared_deliveries_survive_a_restart_and_are_bounded() {
        let delivery = |tx: u8, prepared_at: u64| PreparedDelivery {
            tx_id: [tx; 32],
            prepared_at,
        };
        let directory = tempfile::tempdir().unwrap();
        let url = Url::parse("http://127.0.0.1:9/prepared-deliveries").unwrap();
        {
            let client =
                super::super::tests::test_client_with_event_store(url.clone(), directory.path());
            client
                .keep_prepared_delivery(&[1; 32], H256::repeat_byte(2), &delivery(3, 4))
                .unwrap();
            client
                .keep_prepared_delivery(&[6; 32], H256::repeat_byte(2), &delivery(5, 4))
                .unwrap();
            // Kept again, the newer one replaces it.
            client
                .keep_prepared_delivery(&[1; 32], H256::repeat_byte(2), &delivery(8, 9))
                .unwrap();
        }
        let durable = super::super::tests::test_client_with_event_store(url, directory.path());
        let in_memory =
            RuesClient::new(Url::parse("http://127.0.0.1:9/prepared-in-memory").unwrap()).unwrap();
        in_memory
            .keep_prepared_delivery(&[1; 32], H256::repeat_byte(2), &delivery(8, 9))
            .unwrap();
        in_memory
            .keep_prepared_delivery(&[6; 32], H256::repeat_byte(2), &delivery(5, 4))
            .unwrap();
        for client in [&durable, &in_memory] {
            assert_eq!(
                client
                    .prepared_delivery(&[1; 32], H256::repeat_byte(2))
                    .unwrap(),
                Some(delivery(8, 9))
            );
            assert_eq!(
                client
                    .prepared_delivery(&[1; 32], H256::repeat_byte(3))
                    .unwrap(),
                None
            );
            client
                .forget_prepared_delivery(&[1; 32], H256::repeat_byte(2))
                .unwrap();
            // Forgetting one that is not kept changes nothing.
            client
                .forget_prepared_delivery(&[1; 32], H256::repeat_byte(2))
                .unwrap();
            assert_eq!(
                client
                    .prepared_delivery(&[1; 32], H256::repeat_byte(2))
                    .unwrap(),
                None
            );
            assert_eq!(
                client
                    .prepared_delivery(&[6; 32], H256::repeat_byte(2))
                    .unwrap(),
                Some(delivery(5, 4))
            );
            // Past the bound, the one prepared longest ago goes, whatever
            // its message: here the messages come in falling order.
            let messages = MAX_PREPARED_DELIVERIES as u64 + 2;
            for prepared_at in 0..messages {
                client
                    .keep_prepared_delivery(
                        &[1; 32],
                        H256::from_low_u64_be(messages - prepared_at),
                        &delivery(1, prepared_at),
                    )
                    .unwrap();
            }
            let kept: Vec<u64> = (1..=messages)
                .filter(|message| {
                    client
                        .prepared_delivery(&[1; 32], H256::from_low_u64_be(*message))
                        .unwrap()
                        .is_some()
                })
                .collect();
            assert_eq!(kept.len(), MAX_PREPARED_DELIVERIES);
            // The two prepared first went.
            assert_eq!(kept.first(), Some(&1));
            assert_eq!(kept.last(), Some(&(messages - 2)));
            // The other signer's is still there.
            assert!(client
                .prepared_delivery(&[6; 32], H256::repeat_byte(2))
                .unwrap()
                .is_some());
        }
    }

    #[test]
    fn deliveries_in_flight_survive_a_restart_and_stay_with_their_signer() {
        let directory = tempfile::tempdir().unwrap();
        let url = Url::parse("http://127.0.0.1:9/sent-deliveries").unwrap();
        let delivery = SentDelivery {
            nonce: 3,
            tx_id: Some([4; 32]),
            sent_at: 5,
        };
        let reservation = Reservation {
            nonce: 9,
            holder: Holder::Filler,
            txs: vec![[7; 32]],
            sent_at: 8,
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
            client.keep_reservation(&[1; 32], &reservation).unwrap();
            client
                .keep_reservation(
                    &[1; 32],
                    &Reservation {
                        nonce: 2,
                        ..reservation.clone()
                    },
                )
                .unwrap();
            client.keep_reservation(&[6; 32], &reservation).unwrap();
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
        // In nonce order, and the signer's own.
        let nonces: Vec<u64> = client
            .reservations(&[1; 32])
            .unwrap()
            .iter()
            .map(|reservation| reservation.nonce)
            .collect();
        assert_eq!(nonces, [2, 9]);
        client.release_reservation(&[1; 32], 9).unwrap();
        assert_eq!(client.reservations(&[1; 32]).unwrap().len(), 1);
        assert_eq!(client.reservations(&[6; 32]).unwrap(), [reservation]);
    }
}
