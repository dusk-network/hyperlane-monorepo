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
//! that transaction may still land, and whether its failure was reported
//! to the relayer, so that the gas it spent is reported once.

use super::*;

/// The prefix of the transactions prepared for deliveries sent one at a
/// time.
const PREPARED_DELIVERY_PREFIX: &[u8] = b"dusk-prepared-delivery-v1:";
/// Bytes of an encoded prepared delivery: when it was prepared, its
/// transaction, whether its failure was reported, and the nonce it is
/// signed with, when the helper named it. One kept before the nonce lacks
/// its 8 bytes; one kept before the flag also lacks that byte, and reads
/// as reported.
const PREPARED_DELIVERY_BYTES: usize = 8 + 32 + 1 + 8;
/// Most deliveries sent one at a time whose transaction a signer keeps by
/// default (`SubmissionLimits::prepared`): past it, only those whose
/// transactions are in final blocks are forgotten to make room, and
/// deliveries wait for room.
pub(crate) const MAX_PREPARED_DELIVERIES: usize = 1_024;

/// The transaction `dusk-tx` prepared for the last submission of a
/// delivery sent one at a time: the hash it printed before it propagated
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PreparedDelivery {
    pub tx_id: [u8; 32],
    /// When the helper named it, in milliseconds since the Unix epoch.
    pub prepared_at: u64,
    /// Its failure in a block was reported to the relayer as an outcome,
    /// which charged the gas it spent to its message.
    pub failure_reported: bool,
    /// The nonce it is signed with, which the helper names with its hash
    /// (one from before it did names none): once the signer's account has
    /// used it in a final block, the transaction can no longer execute.
    pub nonce: Option<u64>,
}

impl PreparedDelivery {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(PREPARED_DELIVERY_BYTES);
        bytes.extend_from_slice(&self.prepared_at.to_be_bytes());
        bytes.extend_from_slice(&self.tx_id);
        bytes.push(u8::from(self.failure_reported));
        if let Some(nonce) = self.nonce {
            bytes.extend_from_slice(&nonce.to_be_bytes());
        }
        bytes
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let mut fields = Fields(bytes);
        let prepared_at = fields.u64()?;
        let tx_id = fields.hash()?;
        let (flag, nonce) = match fields.0.len() {
            0 | 1 => (fields.0, None),
            9 => (fields.take(1)?, Some(fields.u64()?)),
            _ => return None,
        };
        let failure_reported = match flag {
            // Kept before the flag, by a build of 2026-11-16 before
            // c28d00d47b: no released relayer wrote one. That build
            // reported a failure it saw at once and kept the record, so
            // the relayer may have charged it already, and upstream adds
            // up the gas of every outcome reported. Read as reported, the
            // record is never charged twice; the cost is at most one late
            // failure's gas left uncharged.
            [] => true,
            [0] => false,
            [1] => true,
            _ => return None,
        };
        Some(Self {
            tx_id,
            prepared_at,
            failure_reported,
            nonce,
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
    /// instead of the one kept before. A signer keeps at most `limit`:
    /// past it, nothing is forgotten to make room, since a transaction kept
    /// may still land, and this fails without keeping `delivery`
    /// (`DuskMailbox::room_for_prepared` makes room beforehand).
    pub(crate) fn keep_prepared_delivery(
        &self,
        signer: &[u8; 32],
        message: H256,
        delivery: &PreparedDelivery,
        limit: usize,
    ) -> Result<(), HyperlaneDuskError> {
        if self.endpoint.prepared_write_fails(true) {
            return Err(in_flight_error("a failure a test injected"));
        }
        let full = |kept: usize| {
            in_flight_error(format!(
                "the signer keeps the transactions of {kept} deliveries, the most it keeps, and none may be forgotten while it may still land"
            ))
        };
        let Some(store) = &self.event_store else {
            return self
                .endpoint
                .keep_prepared_delivery(signer, message, *delivery, limit)
                .map_err(full);
        };
        let key = prepared_delivery_key(signer, message);
        let _writes = self.endpoint.prepared_writes();
        if store.get(&key).map_err(in_flight_error)?.is_none() {
            let kept = self.prepared_deliveries(signer)?.len();
            if kept >= limit {
                return Err(full(kept));
            }
        }
        store
            .put_opt(key, delivery.encode(), &synced())
            .map_err(in_flight_error)
    }

    /// Note that the failure of `tx_id`, kept for `message`, was reported
    /// to the relayer, if that transaction is still the one kept for it.
    pub(crate) fn report_prepared_failure(
        &self,
        signer: &[u8; 32],
        message: H256,
        tx_id: [u8; 32],
    ) -> Result<(), HyperlaneDuskError> {
        if self.endpoint.prepared_write_fails(true) {
            return Err(in_flight_error("a failure a test injected"));
        }
        let Some(store) = &self.event_store else {
            self.endpoint
                .report_prepared_failure(signer, message, tx_id);
            return Ok(());
        };
        let key = prepared_delivery_key(signer, message);
        let _writes = self.endpoint.prepared_writes();
        let kept = store
            .get(&key)
            .map_err(in_flight_error)?
            .and_then(|bytes| PreparedDelivery::decode(&bytes))
            .filter(|delivery| delivery.tx_id == tx_id);
        let Some(delivery) = kept else {
            return Ok(());
        };
        let reported = PreparedDelivery {
            failure_reported: true,
            ..delivery
        };
        store
            .put_opt(key, reported.encode(), &synced())
            .map_err(in_flight_error)
    }

    /// Every delivery `signer` keeps a transaction for, with that
    /// transaction, or `None` for one the event store holds that does not
    /// decode: its transaction is unknown.
    pub(crate) fn prepared_deliveries(
        &self,
        signer: &[u8; 32],
    ) -> Result<Vec<(H256, Option<PreparedDelivery>)>, HyperlaneDuskError> {
        let Some(store) = &self.event_store else {
            return Ok(self
                .endpoint
                .prepared_deliveries(signer)
                .into_iter()
                .map(|(message, delivery)| (message, Some(delivery)))
                .collect());
        };
        let prefix = prepared_delivery_prefix(signer);
        let mut kept = Vec::new();
        for item in store.iterator(IteratorMode::From(&prefix, Direction::Forward)) {
            let (key, value) = item.map_err(in_flight_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let message = key
                .get(prefix.len()..)
                .filter(|message| message.len() == 32)
                .map_or_else(H256::zero, H256::from_slice);
            kept.push((message, PreparedDelivery::decode(&value)));
        }
        Ok(kept)
    }

    /// Forget the transaction `signer` keeps for `message` only if it is
    /// still `tx_id`: one kept since, for another submission of the
    /// message, stays. Whether it was.
    pub(crate) fn forget_prepared_transaction(
        &self,
        signer: &[u8; 32],
        message: H256,
        tx_id: [u8; 32],
    ) -> Result<bool, HyperlaneDuskError> {
        if self.endpoint.prepared_write_fails(false) {
            return Err(in_flight_error("a failure a test injected"));
        }
        let Some(store) = &self.event_store else {
            return Ok(self
                .endpoint
                .forget_prepared_transaction(signer, message, tx_id));
        };
        let key = prepared_delivery_key(signer, message);
        let _writes = self.endpoint.prepared_writes();
        let kept = store
            .get(&key)
            .map_err(in_flight_error)?
            .and_then(|bytes| PreparedDelivery::decode(&bytes))
            .is_some_and(|delivery| delivery.tx_id == tx_id);
        if kept {
            store.delete_opt(key, &synced()).map_err(in_flight_error)?;
        }
        Ok(kept)
    }

    /// Forget the transaction `signer` prepared for `message`, if one is
    /// kept.
    pub(crate) fn forget_prepared_delivery(
        &self,
        signer: &[u8; 32],
        message: H256,
    ) -> Result<(), HyperlaneDuskError> {
        if self.endpoint.prepared_write_fails(false) {
            return Err(in_flight_error("a failure a test injected"));
        }
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

    /// Make the next `keeps` keeps and `forgets` forgets of the
    /// transactions kept for deliveries sent one at a time fail, as they
    /// do when the event store cannot be written, for tests of what such a
    /// failure does. Noting that a failure was reported is a keep.
    pub(crate) fn fail_prepared_writes(&self, keeps: usize, forgets: usize) {
        self.endpoint.fail_prepared_writes(keeps, forgets);
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
    ///
    /// One query asks for both, which is all a lookup costs while the
    /// transaction is found. Its two fields share no read, though: Rusk
    /// 1.7.3 resolves `tx` and `mempoolTx` each from its own database view
    /// (`tx_by_hash` and `mempool_by_hash` in
    /// `rusk/src/lib/http/chain/graphql/tx.rs`), in either order. In Rusk
    /// 1.7.3 a transaction moves between the two in steps a single read
    /// never sees it missing from both: a rollback deletes a block and puts
    /// its transactions back into the mempool in one `db.update`, one
    /// RocksDB transaction, committed at once, over the one database that
    /// holds the ledger and the mempool (`Acceptor::try_revert`); a block's
    /// acceptance stores it with its transactions in one `db.update`, and
    /// only a later one deletes them from the mempool
    /// (`Acceptor::accept_block`), so a transaction is in both meanwhile.
    /// The query can still miss it: its mempool read before a rollback and
    /// its ledger read after, or its ledger read before an acceptance and
    /// its mempool read after that acceptance's clean-up. So "neither" is
    /// read again, one place at a time, the ledger, the mempool, then the
    /// ledger again, and stands only if all three miss it too: a
    /// transaction that stays on the node is then missed only if a block
    /// holding it is accepted, and taken out of the mempool, after the
    /// first ledger read and rolled back before the second.
    pub(crate) async fn transaction_status(
        &self,
        tx_id: &str,
    ) -> Result<TransactionStatus, HyperlaneDuskError> {
        validate_transaction_id(tx_id)?;
        let ledger = format!(r#"tx(hash: "{tx_id}") {{ blockHeight gasSpent err }}"#);
        let mempool = format!(r#"mempoolTx(hash: "{tx_id}") {{ id }}"#);
        let data = self
            .graphql_query(&format!("query {{ {mempool} {ledger} }}"))
            .await?;
        if let Some(status) = found(tx_id, &data, &["mempoolTx", "tx"])? {
            return Ok(status);
        }
        for (field, asked) in [(&ledger, "tx"), (&mempool, "mempoolTx"), (&ledger, "tx")] {
            let data = self.graphql_query(&format!("query {{ {field} }}")).await?;
            if let Some(status) = found(tx_id, &data, &[asked])? {
                return Ok(status);
            }
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

/// Where a lookup of transaction `tx_id` that asked for the fields `asked`
/// and answered `data` found it: in a block (`tx`), in the mempool
/// (`mempoolTx`), or, `None`, in neither of them.
///
/// "Neither" lets the relayer forget a transaction it kept and sign its
/// message again, so only a reply that says so stands for it: one that
/// holds every field asked for, each null, as Rusk answers for a hash it
/// does not know. A reply without one of them, or that is not an object at
/// all, such as `{}` or a `null` data from a proxy or cache in front of the
/// node, is an error.
fn found(
    tx_id: &str,
    data: &JsonValue,
    asked: &[&str],
) -> Result<Option<TransactionStatus>, HyperlaneDuskError> {
    if let Some(field) = asked.iter().find(|field| data.get(**field).is_none()) {
        return Err(HyperlaneDuskError::Other(format!(
            "Lookup of Dusk transaction {tx_id} answered without its {field} field: {}",
            bounded_json(data)
        )));
    }
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
        return Ok(Some(TransactionStatus::Included {
            block_height,
            receipt: parse_confirmed_transaction(tx_id, transaction)?,
        }));
    }
    if data.get("mempoolTx").is_some_and(|value| !value.is_null()) {
        return Ok(Some(TransactionStatus::Pending));
    }
    Ok(None)
}

/// Write options that reach the disk before the write returns.
fn synced() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A lookup reply that does not hold the fields asked for, as a proxy,
    /// a cache or another schema might answer, was read as the transaction
    /// being in neither place, after which the relayer forgets the
    /// transaction it kept and signs its message again.
    #[test]
    fn a_reply_without_the_fields_asked_for_is_an_error_not_neither_place() {
        use serde_json::json;

        let both = ["mempoolTx", "tx"];
        for reply in [
            json!(null),
            json!({}),
            json!("ok"),
            json!([]),
            json!({ "tx": null }),
            json!({ "mempoolTx": null }),
        ] {
            assert!(found("ab", &reply, &both).is_err(), "{reply}");
        }
        assert!(found("ab", &json!({}), &["tx"]).is_err());
        assert!(found("ab", &json!({ "tx": null }), &["mempoolTx"]).is_err());
        // What Rusk answers for a hash it does not know.
        let unknown = json!({ "mempoolTx": null, "tx": null });
        assert!(matches!(found("ab", &unknown, &both), Ok(None)));
        assert!(matches!(
            found("ab", &json!({ "tx": null }), &["tx"]),
            Ok(None)
        ));
        assert!(matches!(
            found("ab", &json!({ "mempoolTx": null }), &["mempoolTx"]),
            Ok(None)
        ));
    }

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
        for (failure_reported, nonce) in [(false, Some(u64::MAX)), (true, Some(0)), (false, None)] {
            let delivery = PreparedDelivery {
                tx_id: [7; 32],
                prepared_at: 1_700_000_000_000,
                failure_reported,
                nonce,
            };
            assert_eq!(PreparedDelivery::decode(&delivery.encode()), Some(delivery));
            let encoded = delivery.encode();
            // Without the nonce it reads as one kept before the nonce was.
            assert_eq!(
                PreparedDelivery::decode(&encoded[..41]),
                Some(PreparedDelivery {
                    nonce: None,
                    ..delivery
                })
            );
            // One kept before the flag reads as reported, so it is never
            // charged twice.
            assert_eq!(
                PreparedDelivery::decode(&encoded[..40]),
                Some(PreparedDelivery {
                    failure_reported: true,
                    nonce: None,
                    ..delivery
                })
            );
            assert!(PreparedDelivery::decode(&encoded[..39]).is_none());
            let mut longer = encoded.clone();
            longer.push(0);
            assert!(PreparedDelivery::decode(&longer).is_none());
            let mut unknown = encoded;
            unknown[40] = 2;
            assert!(PreparedDelivery::decode(&unknown).is_none());
        }
    }

    /// What a signer keeps for its deliveries one at a time survives a
    /// restart, stays with the signer and the message, and is bounded,
    /// in the event store and in memory alike: past the bound nothing is
    /// forgotten to make room, and a new one is not kept.
    #[test]
    fn prepared_deliveries_survive_a_restart_and_are_bounded() {
        const LIMIT: usize = 3;
        let delivery = |tx: u8, prepared_at: u64| PreparedDelivery {
            tx_id: [tx; 32],
            prepared_at,
            failure_reported: false,
            nonce: Some(prepared_at),
        };
        let directory = tempfile::tempdir().unwrap();
        let url = Url::parse("http://127.0.0.1:9/prepared-deliveries").unwrap();
        {
            let client =
                super::super::tests::test_client_with_event_store(url.clone(), directory.path());
            client
                .keep_prepared_delivery(&[1; 32], H256::repeat_byte(2), &delivery(3, 4), LIMIT)
                .unwrap();
            client
                .keep_prepared_delivery(&[6; 32], H256::repeat_byte(2), &delivery(5, 4), LIMIT)
                .unwrap();
            // Kept again, the newer one replaces it.
            client
                .keep_prepared_delivery(&[1; 32], H256::repeat_byte(2), &delivery(8, 9), LIMIT)
                .unwrap();
        }
        let durable = super::super::tests::test_client_with_event_store(url, directory.path());
        let in_memory =
            RuesClient::new(Url::parse("http://127.0.0.1:9/prepared-in-memory").unwrap()).unwrap();
        in_memory
            .keep_prepared_delivery(&[1; 32], H256::repeat_byte(2), &delivery(8, 9), LIMIT)
            .unwrap();
        in_memory
            .keep_prepared_delivery(&[6; 32], H256::repeat_byte(2), &delivery(5, 4), LIMIT)
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
            // At the bound, a new one is not kept, and nothing kept goes;
            // one kept already is still replaced.
            for message in 1..=LIMIT as u64 {
                client
                    .keep_prepared_delivery(
                        &[1; 32],
                        H256::from_low_u64_be(message),
                        &delivery(1, message),
                        LIMIT,
                    )
                    .unwrap();
            }
            let error = client
                .keep_prepared_delivery(&[1; 32], H256::from_low_u64_be(9), &delivery(9, 0), LIMIT)
                .unwrap_err();
            assert!(error.to_string().contains("the most it keeps"), "{error}");
            client
                .keep_prepared_delivery(&[1; 32], H256::from_low_u64_be(1), &delivery(7, 10), LIMIT)
                .unwrap();
            let kept = client.prepared_deliveries(&[1; 32]).unwrap();
            assert_eq!(
                kept,
                [
                    (H256::from_low_u64_be(1), Some(delivery(7, 10))),
                    (H256::from_low_u64_be(2), Some(delivery(1, 2))),
                    (H256::from_low_u64_be(3), Some(delivery(1, 3))),
                ]
            );
            // The other signer's is kept apart.
            assert_eq!(client.prepared_deliveries(&[6; 32]).unwrap().len(), 1);
            // A failure is noted only on the transaction it is of.
            client
                .report_prepared_failure(&[1; 32], H256::from_low_u64_be(2), [9; 32])
                .unwrap();
            client
                .report_prepared_failure(&[1; 32], H256::from_low_u64_be(3), [1; 32])
                .unwrap();
            client
                .report_prepared_failure(&[1; 32], H256::from_low_u64_be(8), [1; 32])
                .unwrap();
            let reported = |message: u64| {
                client
                    .prepared_delivery(&[1; 32], H256::from_low_u64_be(message))
                    .unwrap()
                    .map(|delivery| delivery.failure_reported)
            };
            assert_eq!(
                [reported(2), reported(3), reported(8)],
                [Some(false), Some(true), None]
            );
            // A transaction is forgotten for room only while it is still the
            // one kept for its message.
            assert!(!client
                .forget_prepared_transaction(&[1; 32], H256::from_low_u64_be(1), [1; 32])
                .unwrap());
            assert!(client
                .forget_prepared_transaction(&[1; 32], H256::from_low_u64_be(1), [7; 32])
                .unwrap());
            assert!(!client
                .forget_prepared_transaction(&[1; 32], H256::from_low_u64_be(1), [7; 32])
                .unwrap());
            client
                .keep_prepared_delivery(&[1; 32], H256::from_low_u64_be(9), &delivery(9, 0), LIMIT)
                .unwrap();
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
