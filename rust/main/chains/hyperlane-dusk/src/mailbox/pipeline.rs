//! Pipelined deliveries (`pipelineDeliveries`, experimental): a batch sent
//! as consecutive transactions of the signer's account, with nonces this
//! agent chooses.
//!
//! Every nonce the agent signs with is held by a reservation
//! (`rues::Reservation`), kept in `eventCursorDir` before anything is
//! signed with it, for a delivery or for a filler. The rules:
//!
//! - One reservation per nonce, and one per message: a message is never
//!   signed with another nonce while a transaction of it might land, and a
//!   nonce is never given to a second message.
//! - A transaction's hash is kept before it is sent: the helper signs
//!   against an endpoint of this process (`tx_sender::dusk_tx_sign`), and
//!   the agent sends the transaction itself once its hash is in the
//!   reservation, so no transaction of the agent reaches the node unnamed.
//! - A reservation is released only when one of its transactions is in a
//!   final block, read after the node's last final block, or when it never
//!   sent one. A delivery that lands, or that another relayer makes, does
//!   not release it: its nonce stays held until it is final, or filled.
//! - Every reservation not released counts toward the signer's bound of
//!   outstanding deliveries, those in blocks that are not final included,
//!   so that a rollback cannot put more than the bound back into the
//!   mempool.
//! - A reservation whose transactions the node dropped goes again with its
//!   nonce: its delivery with the batch's arguments when its message is in
//!   the batch, otherwise with the arguments it kept, after a simulation,
//!   or a filler when that simulation fails. A free nonce below held ones
//!   gets a filler. Neither needs room under the bound, so the signer
//!   never stalls behind a gap.
//! - When the account has used a held nonce for none of its transactions,
//!   another process signs with the key: pipelining stops in this process
//!   with a critical error, and deliveries go one at a time.

use std::collections::{BTreeMap, HashMap, VecDeque};

use tracing::{debug, error, info, warn};

use hyperlane_core::{
    BatchResult, ChainResult, FixedPointNumber, HyperlaneMessage, Metadata, TxOutcome, H256, U256,
};

use super::{signer_key, unix_millis, DuskMailbox, RUSK_BLOCK_GAS_LIMIT};
use crate::rues::{
    bounded_error, bounded_text, Holder, Reservation, SentDelivery, Submission, TransactionStatus,
    MAX_RESERVATION_TXS,
};
use crate::{AccountStatus, DuskSigner, HyperlaneDuskError};

/// Rusk 1.7.3 queues at most this many transactions of one account whose
/// earlier nonces it has not seen yet (`max_moonlight_future_nonce_per_account`):
/// the most a batch sends.
pub(super) const MAX_PIPELINED_DELIVERIES: usize = 64;
/// What a filler calls: the Mailbox's `state_version`, a view that changes
/// nothing. A filler that fails still uses its nonce.
const FILLER_FUNCTION: &str = "state_version";

/// A delivery of a batch, ready to be sent: its place in the batch, its
/// message, and its `process` arguments and gas limit.
#[derive(Debug, Clone)]
pub(super) struct Ready {
    pub index: usize,
    pub message_id: H256,
    pub args: Vec<u8>,
    pub gas_limit: u64,
}

/// What became of a delivery of a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Fate {
    /// A transaction of it sent earlier may still be in a block: it is
    /// followed, and none is sent.
    Followed(String),
    /// Sent now, or sent with an outcome the node did not confirm.
    Sent(String),
    /// Neither sent nor followed; why. The relayer submits it again.
    LeftOut(String),
}

/// Where a reservation stands, reconciled with the node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Standing {
    /// One of its transactions is in a block at or below the node's last
    /// final block: its nonce is used for good. Released.
    Final,
    /// One of its transactions, named, is in a block that is not final yet.
    Landed(String),
    /// One of its transactions, named, is in the mempool.
    Pending(String),
    /// It may still reach the mempool or a block: it was sent within the
    /// drop grace, the mempool holds its nonce for a transaction it does
    /// not name, or a lookup failed. Its newest transaction.
    Unsettled(Option<String>),
    /// None of its transactions is in the mempool or a block, past the drop
    /// grace, and its nonce is neither used nor held by the mempool: it
    /// goes again with its nonce.
    Dropped,
    /// It never sent a transaction: a transaction's hash is kept before it
    /// is sent. Released.
    Unsent,
    /// The account used its nonce, and none of its transactions is in a
    /// block: another sender signed that nonce.
    Taken,
}

impl Standing {
    /// The transaction a delivery of it is followed by, while one may
    /// still be in a block.
    fn followed(&self) -> Option<&str> {
        match self {
            Self::Landed(tx_id) | Self::Pending(tx_id) | Self::Unsettled(Some(tx_id)) => {
                Some(tx_id)
            }
            _ => None,
        }
    }
}

/// The signer's account and deliveries in flight, reconciled with the node.
#[derive(Debug)]
pub(super) struct Survey {
    pub account: AccountStatus,
    /// Every reservation not released, by nonce, and where it stands.
    pub held: BTreeMap<u64, (Reservation, Standing)>,
    /// How many deliveries the previous version left in flight may still
    /// be admitted.
    pub legacy_live: usize,
}

/// What a batch does at one nonce.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Step {
    /// A new delivery: the batch's `ready`th.
    Fresh { ready: usize },
    /// The reservation's delivery or filler again, with the arguments of
    /// the batch's `ready`th delivery when its message is in the batch.
    Again { ready: Option<usize> },
    /// A filler at a free nonce below held ones.
    Fill,
}

/// What a batch sends, nonce by nonce from the account's next one, and
/// what becomes of each of its deliveries `ready` unless a step stops
/// short. Deliveries whose reservation has a transaction that may still be
/// in a block are followed. A free nonce below held ones gets a filler,
/// and a dropped reservation goes again; neither needs room under
/// `outstanding`. A nonce the mempool holds for a transaction this agent
/// did not send is passed over. New deliveries take the nonces past every
/// held one while the signer has fewer than `outstanding` reservations. The
/// walk stops before a held nonce the node does not hold yet: it would hold
/// every later one back.
pub(super) fn plan(
    survey: &Survey,
    ready: &[Ready],
    outstanding: usize,
) -> (Vec<(u64, Step)>, Vec<Fate>) {
    let unsent = || Fate::LeftOut("an earlier transaction of the batch was not sent".into());
    let mut fates: Vec<Fate> = ready.iter().map(|_| unsent()).collect();
    let by_message: HashMap<H256, u64> = survey
        .held
        .iter()
        .filter_map(|(nonce, (reservation, _))| {
            reservation
                .holder
                .message()
                .map(|message| (message, *nonce))
        })
        .collect();
    let mut fresh = VecDeque::new();
    let mut again = HashMap::new();
    for (position, delivery) in ready.iter().enumerate() {
        let Some((nonce, (_, standing))) = by_message
            .get(&delivery.message_id)
            .and_then(|nonce| survey.held.get(nonce).map(|held| (*nonce, held)))
        else {
            fresh.push_back(position);
            continue;
        };
        fates[position] = match (standing, standing.followed()) {
            (Standing::Dropped, _) => {
                again.insert(nonce, position);
                unsent()
            }
            (_, Some(tx_id)) => Fate::Followed(tx_id.to_owned()),
            (_, None) => Fate::LeftOut(format!(
                "a transaction of it with nonce {nonce} may still land and is not named yet"
            )),
        };
    }
    let highest = survey.held.keys().next_back().copied();
    let mut allowed = outstanding.saturating_sub(survey.held.len());
    let mut steps = Vec::new();
    let mut nonce = survey.account.nonce.saturating_add(1);
    let mut waits_for = None;
    while steps.len() < MAX_PIPELINED_DELIVERIES {
        match survey.held.get(&nonce) {
            Some((_, Standing::Dropped)) => steps.push((
                nonce,
                Step::Again {
                    ready: again.remove(&nonce),
                },
            )),
            Some((_, Standing::Pending(_) | Standing::Landed(_))) => {}
            Some(_) => {
                waits_for = Some(nonce);
                break;
            }
            None if nonce < survey.account.next_nonce => {}
            None if highest.is_some_and(|highest| nonce < highest) => {
                steps.push((nonce, Step::Fill));
            }
            None => {
                if fresh.is_empty() {
                    break;
                }
                if allowed == 0 {
                    for position in fresh.drain(..) {
                        fates[position] = Fate::LeftOut(format!(
                            "the signer has {} deliveries outstanding, its most; it goes once their nonces are used in final blocks",
                            survey.held.len()
                        ));
                    }
                    break;
                }
                let Some(position) = fresh.pop_front() else {
                    break;
                };
                allowed -= 1;
                steps.push((nonce, Step::Fresh { ready: position }));
            }
        }
        nonce = nonce.saturating_add(1);
    }
    let reason = match waits_for {
        Some(nonce) => format!("it waits until the node holds nonce {nonce}"),
        None => "the batch is full".into(),
    };
    for position in fresh.into_iter().chain(again.into_values()) {
        fates[position] = Fate::LeftOut(reason.clone());
    }
    (steps, fates)
}

/// The 32 bytes of a transaction ID in hex.
fn tx_bytes(tx_id: &str) -> Option<[u8; 32]> {
    <[u8; 32]>::try_from(hex::decode(tx_id).ok()?).ok()
}

impl DuskMailbox {
    /// Whether deliveries are pipelined now: the chain opted in, the
    /// helper can sign with a given nonce, and no other sender was found
    /// using the signer's nonces in this process.
    pub(super) async fn pipelines_now(&self, signer: &DuskSigner) -> bool {
        self.pipelined
            && !self.rues.pipelining_stopped(&signer_key(signer))
            && crate::tx_sender::helper_pipelines().await
    }

    /// Submit `deliveries`, each a message, its metadata and the relayer's
    /// gas limit, as consecutive transactions of the signer's account, so
    /// that several share a block.
    ///
    /// Each delivery is simulated first (`simulate_process`), and one that
    /// fails is left out. Then the signer's deliveries in flight are
    /// reconciled with the node (`survey`), and the batch sends, nonce by
    /// nonce from the account's next one (`plan`): a delivery whose
    /// transaction may still be in a block is followed, not sent again; one
    /// the node dropped goes again with its nonce; new ones take the nonces
    /// past every held one, while the signer has fewer than
    /// `SubmissionLimits::outstanding` reservations. Each is sent once the
    /// node holds the one before. A transaction the node refuses ends the
    /// batch there, and so does one whose outcome is unknown, one the node
    /// does not hold within `SubmissionLimits::held`, and the end of the
    /// batch's `SubmissionLimits::batch`, which every wait before sending,
    /// the reconciliation and the helper included, ends by. Then the batch
    /// follows every transaction it sent or follows to its block, under one
    /// deadline.
    ///
    /// Returns the deliveries it left out, which the relayer submits again,
    /// and, when it sent or followed any, one outcome for all of them, as
    /// the relayer's batches have: the first transaction's ID, the gas they
    /// spent together, which the relayer shares out among them, and whether
    /// every one executed. Every delivery is either left out or in the
    /// outcome. A delivery whose transaction is in no block by the deadline
    /// still counts as sent: the relayer checks that it was delivered
    /// before it gives up on it, and submits it again if not, which follows
    /// that transaction instead of sending another. Once one transaction is
    /// sent this never fails, so the relayer never sends the batch again.
    ///
    /// Unless deliveries are pipelined (`pipelines_now`), every delivery
    /// is left out, and the relayer sends each on its own.
    pub async fn process_deliveries(
        &self,
        deliveries: &[(HyperlaneMessage, Metadata, Option<U256>)],
    ) -> ChainResult<BatchResult> {
        let signer = self
            .signer
            .as_ref()
            .ok_or(HyperlaneDuskError::SignerUnavailable)?;
        if !self.pipelines_now(signer).await {
            return Ok(BatchResult::failed(deliveries.len()));
        }
        let deadline = tokio::time::Instant::now() + self.limits.batch;
        let mut fates: Vec<Fate> = deliveries
            .iter()
            .map(|_| Fate::LeftOut("the batch is full".into()))
            .collect();
        let mut ready: Vec<Ready> = Vec::new();
        for (index, (message, metadata, gas_limit)) in deliveries.iter().enumerate() {
            if ready.len() == MAX_PIPELINED_DELIVERIES {
                continue;
            }
            // A message goes with one nonce.
            if ready
                .iter()
                .any(|delivery| delivery.message_id == message.id())
            {
                fates[index] = Fate::LeftOut("the batch delivers its message already".into());
                continue;
            }
            let encoded = Self::encode_message(message);
            let args = match crate::tx_sender::process_args(metadata, &encoded) {
                Ok(args) => args,
                Err(error) => {
                    warn!(message_id = ?message.id(), %error, "Dusk delivery cannot be sent");
                    fates[index] = Fate::LeftOut(error.to_string());
                    continue;
                }
            };
            if let Err(error) = self.simulate_process(message.id(), &args).await {
                fates[index] = Fate::LeftOut(error.to_string());
                continue;
            }
            ready.push(Ready {
                index,
                message_id: message.id(),
                args,
                gas_limit: self.submission_gas_limit(message, *gas_limit),
            });
        }
        if !ready.is_empty() {
            let sent = self.send_pipelined(signer, &ready, deadline).await?;
            for (delivery, fate) in ready.iter().zip(sent) {
                fates[delivery.index] = fate;
            }
        }
        Ok(self.batch_result(fates).await)
    }

    /// One outcome for the deliveries of a batch that were sent or
    /// followed, once their transactions are in blocks or the receipt wait
    /// is over, and every other one left out.
    async fn batch_result(&self, fates: Vec<Fate>) -> BatchResult {
        let mut failed_indexes = Vec::new();
        let mut txs: Vec<String> = Vec::new();
        for (index, fate) in fates.into_iter().enumerate() {
            match fate {
                Fate::Followed(tx_id) | Fate::Sent(tx_id) => {
                    if !txs.contains(&tx_id) {
                        txs.push(tx_id);
                    }
                }
                Fate::LeftOut(reason) => {
                    debug!(index, reason = %bounded_text(&reason), "Dusk delivery left out of the batch");
                    failed_indexes.push(index);
                }
            }
        }
        let Some(first) = txs.first() else {
            return BatchResult {
                outcome: None,
                failed_indexes,
            };
        };
        let transaction_id = crate::tx_sender::dusk_tx_id_to_h512(first).unwrap_or_default();
        let deadline = tokio::time::Instant::now() + self.limits.receipt;
        let mut gas_spent = 0u64;
        let mut executed = true;
        for tx_id in &txs {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match self
                .rues
                .wait_for_tx_in_blocks_of(tx_id, remaining, self.limits.block)
                .await
            {
                Ok(receipt) => {
                    gas_spent = gas_spent.saturating_add(receipt.gas_spent);
                    if let Some(error) = &receipt.error {
                        executed = false;
                        let error = bounded_text(error);
                        warn!(tx_id, %error, "Dusk delivery transaction execution failed");
                    }
                }
                Err(error) => {
                    executed = false;
                    warn!(
                        tx_id,
                        %error,
                        "Dusk delivery transaction is in no block yet; the relayer checks the delivery before it relays it again"
                    );
                }
            }
        }
        BatchResult {
            outcome: Some(TxOutcome {
                transaction_id,
                executed,
                gas_used: U256::from(gas_spent),
                gas_price: FixedPointNumber::from(self.conn.gas_price),
            }),
            failed_indexes,
        }
    }

    /// Submit one delivery when deliveries are pipelined, as a batch of
    /// one: a delivery in flight is followed, one dropped goes again with
    /// its nonce, a new one takes the next free one. Fails when it is left
    /// out, and when its transaction is in no block within
    /// `SubmissionLimits::receipt`.
    pub(super) async fn process_pipelined(
        &self,
        signer: &DuskSigner,
        message_id: H256,
        args: &[u8],
        gas_limit: u64,
    ) -> ChainResult<TxOutcome> {
        let deadline = tokio::time::Instant::now() + self.limits.batch;
        let ready = [Ready {
            index: 0,
            message_id,
            args: args.to_vec(),
            gas_limit,
        }];
        let fate = self
            .send_pipelined(signer, &ready, deadline)
            .await?
            .into_iter()
            .next();
        match fate {
            Some(Fate::Followed(tx_id)) => {
                info!(
                    ?message_id,
                    %tx_id,
                    "Dusk delivery's transaction may still be in a block; following it instead of sending another"
                );
                self.outcome_of(&tx_id).await
            }
            Some(Fate::Sent(tx_id)) => self.outcome_of(&tx_id).await,
            Some(Fate::LeftOut(reason)) => Err(HyperlaneDuskError::Other(format!(
                "Dusk delivery of message {message_id:?} was not sent: {reason}"
            ))
            .into()),
            None => Err(HyperlaneDuskError::Other(format!(
                "Dusk delivery of message {message_id:?} was not sent"
            ))
            .into()),
        }
    }

    /// Send `ready` under the signer's submission lock, as `plan` says,
    /// each transaction's hash kept before it is sent. Fails only before
    /// anything is sent.
    async fn send_pipelined(
        &self,
        signer: &DuskSigner,
        ready: &[Ready],
        deadline: tokio::time::Instant,
    ) -> ChainResult<Vec<Fate>> {
        let left_out = |reason: String| {
            ready
                .iter()
                .map(|_| Fate::LeftOut(reason.clone()))
                .collect::<Vec<_>>()
        };
        let key = signer_key(signer);
        let lock = self.rues.submission_lock(key);
        let Ok(_submission) = tokio::time::timeout_at(deadline, lock.lock()).await else {
            return Ok(left_out(
                "another submission of the signer held its nonces until the batch's time to send was up".into(),
            ));
        };
        // Nothing is sent before the endpoint's deployment, the signer's
        // deliveries in flight and its account are known.
        tokio::time::timeout_at(deadline, self.rues.ensure_identity())
            .await
            .map_err(|_| {
                HyperlaneDuskError::Other(
                    "Dusk endpoint's deployment was not checked before the batch's time to send was up".into(),
                )
            })??;
        let survey = self.survey(signer, deadline).await?;
        if survey.legacy_live > 0 {
            warn!(
                live = survey.legacy_live,
                "Dusk deliveries the previous version left in flight may still be admitted; pipelined deliveries wait until they are in blocks or gone"
            );
            return Ok(left_out(format!(
                "{} deliveries the previous version left in flight may still be admitted",
                survey.legacy_live
            )));
        }
        let taken: Vec<u64> = survey
            .held
            .iter()
            .filter(|(_, (_, standing))| *standing == Standing::Taken)
            .map(|(nonce, _)| *nonce)
            .collect();
        if !taken.is_empty() {
            self.rues.stop_pipelining(key);
            error!(
                nonces = ?taken,
                signer = %signer.address_string(),
                "CRITICAL: another sender used nonces of the Dusk signer that this relayer's pipelined deliveries hold. Only one process may sign with a key while it pipelines deliveries: check every relayer and validator configured with this key. This process no longer pipelines this signer's deliveries and sends them one at a time"
            );
            return Ok(left_out(
                "another sender used the signer's nonces, and deliveries go one at a time".into(),
            ));
        }
        let (steps, mut fates) = plan(&survey, ready, self.limits.outstanding);
        let mut held = survey.held;
        for (nonce, step) in steps {
            if tokio::time::Instant::now() >= deadline {
                warn!(
                    nonce,
                    "Dusk delivery batch has used its time to send; its later deliveries are sent later"
                );
                break;
            }
            let (mut reservation, member) = match step {
                Step::Fresh { ready: position } => {
                    let delivery = &ready[position];
                    (
                        Reservation {
                            nonce,
                            holder: Holder::Delivery {
                                message: delivery.message_id,
                                gas_limit: delivery.gas_limit,
                                args: delivery.args.clone(),
                            },
                            txs: Vec::new(),
                            sent_at: 0,
                        },
                        Some(position),
                    )
                }
                Step::Fill => (
                    Reservation {
                        nonce,
                        holder: Holder::Filler,
                        txs: Vec::new(),
                        sent_at: 0,
                    },
                    None,
                ),
                Step::Again { ready: member } => {
                    let Some((mut reservation, _)) = held.remove(&nonce) else {
                        break;
                    };
                    if let Some(position) = member {
                        let delivery = &ready[position];
                        reservation.holder = Holder::Delivery {
                            message: delivery.message_id,
                            gas_limit: delivery.gas_limit,
                            args: delivery.args.clone(),
                        };
                    } else if let Holder::Delivery { message, args, .. } = &reservation.holder {
                        // Not in this batch: it goes again as it was
                        // signed, unless its simulation fails now, as
                        // when another relayer delivered it; then a
                        // filler uses its nonce.
                        if self.simulate_process_now(*message, args).await.is_err() {
                            info!(
                                nonce,
                                message_id = ?message,
                                "Dusk delivery the node dropped would fail now; a filler takes its nonce"
                            );
                            reservation.holder = Holder::Filler;
                        }
                    }
                    (reservation, member)
                }
            };
            if reservation.txs.len() >= MAX_RESERVATION_TXS {
                error!(
                    nonce,
                    sent = reservation.txs.len(),
                    "CRITICAL: the Dusk node dropped every transaction this relayer signed with this nonce; it is not sent again, and the signer's later deliveries wait. Check the node's mempool and the signer's balance"
                );
                break;
            }
            // Its nonce is held before anything is signed with it.
            if let Err(error) = self.rues.keep_reservation(&key, &reservation) {
                warn!(nonce, error = %bounded_error(&error), "Dusk nonce reservation cannot be kept in eventCursorDir; the batch sends nothing more");
                break;
            }
            let (function, args, gas_limit) = match &reservation.holder {
                Holder::Delivery {
                    args, gas_limit, ..
                } => ("process", args.clone(), *gas_limit),
                Holder::Filler => (FILLER_FUNCTION, Vec::new(), self.filler_gas_limit()),
            };
            let signed = match crate::tx_sender::dusk_tx_sign(
                &self.conn,
                signer,
                &self.mailbox_id,
                function,
                &args,
                gas_limit,
                nonce,
                deadline,
            )
            .await
            {
                Ok(signed) => signed,
                Err(error) => {
                    warn!(nonce, error = %bounded_error(&error), "Dusk pipelined transaction was not signed; the batch sends nothing more");
                    if reservation.txs.is_empty() {
                        self.release(&key, nonce);
                    }
                    break;
                }
            };
            let Some(tx) = tx_bytes(&signed.tx_id) else {
                if reservation.txs.is_empty() {
                    self.release(&key, nonce);
                }
                break;
            };
            // Signed again alike, a transaction has the same hash.
            let added = !reservation.txs.contains(&tx);
            if added {
                reservation.txs.push(tx);
            }
            reservation.sent_at = unix_millis();
            // Its hash is kept before it is sent.
            if let Err(error) = self.rues.keep_reservation(&key, &reservation) {
                warn!(nonce, error = %bounded_error(&error), "Dusk transaction's hash cannot be kept in eventCursorDir; it is not sent");
                if added && reservation.txs.len() == 1 {
                    self.release(&key, nonce);
                }
                break;
            }
            let filler = reservation.holder == Holder::Filler;
            match self.rues.submit_transaction(&signed.bytes, deadline).await {
                Submission::Refused(reason) => {
                    warn!(nonce, tx_id = %signed.tx_id, %reason, "Dusk node did not take a pipelined transaction; the batch sends no later nonce");
                    if added {
                        reservation.txs.pop();
                    }
                    if reservation.txs.is_empty() {
                        self.release(&key, nonce);
                    } else if let Err(error) = self.rues.keep_reservation(&key, &reservation) {
                        warn!(nonce, error = %bounded_error(&error), "Dusk nonce reservation cannot be kept in eventCursorDir");
                    }
                    if let Some(position) = member {
                        fates[position] =
                            Fate::LeftOut(format!("the node did not take it: {reason}"));
                    }
                    break;
                }
                Submission::Unknown(reason) => {
                    warn!(nonce, tx_id = %signed.tx_id, %reason, "Dusk pipelined transaction's outcome is unknown; its hash is kept, and the batch sends no later nonce");
                    if let Some(position) = member {
                        fates[position] = Fate::Sent(signed.tx_id.clone());
                    }
                    break;
                }
                Submission::Propagated => {
                    info!(nonce, tx_id = %signed.tx_id, filler, "Dusk pipelined transaction sent");
                    if let Some(position) = member {
                        fates[position] = Fate::Sent(signed.tx_id.clone());
                    }
                    let wait = self
                        .limits
                        .held
                        .min(deadline.saturating_duration_since(tokio::time::Instant::now()));
                    let held = self.rues.wait_until_held(&signed.tx_id, wait).await;
                    if !matches!(held, Ok(true)) {
                        warn!(
                            tx_id = %signed.tx_id,
                            ?held,
                            "Dusk node does not hold a pipelined transaction yet; sending no later nonce"
                        );
                        break;
                    }
                }
            }
        }
        Ok(fates)
    }

    /// The gas limit of a filler: the configured `gasLimit`, at most the
    /// block gas limit. A filler that runs out of gas still uses its nonce.
    fn filler_gas_limit(&self) -> u64 {
        self.conn.gas_limit.min(RUSK_BLOCK_GAS_LIMIT)
    }

    /// Release `signer`'s reservation of `nonce`.
    pub(super) fn release(&self, key: &[u8; 32], nonce: u64) {
        if let Err(error) = self.rues.release_reservation(key, nonce) {
            warn!(nonce, error = %bounded_error(&error), "Dusk nonce reservation cannot be released in eventCursorDir");
        }
    }

    /// The signer's account and deliveries in flight, reconciled with the
    /// node by `deadline`, whatever is pipelined. The node's last final
    /// block is read first, then the account, then each transaction:
    /// a transaction found in a block at or below that final block is in a
    /// final block whatever happened since. A reservation that settled
    /// (`Standing::Final`) or never sent anything (`Standing::Unsent`) is
    /// released, and a delivery the previous version left that can no
    /// longer be admitted is forgotten. A lookup that fails or runs past
    /// the deadline leaves its reservation unsettled.
    pub(super) async fn survey(
        &self,
        signer: &DuskSigner,
        deadline: tokio::time::Instant,
    ) -> ChainResult<Survey> {
        let key = signer_key(signer);
        let reservations = self.rues.reservations(&key)?;
        let legacy = self.rues.sent_deliveries(&key)?;
        let late = || {
            HyperlaneDuskError::Other(
                "Dusk signer's deliveries in flight were not reconciled with the node before the deadline".into(),
            )
        };
        let finalized = tokio::time::timeout_at(deadline, self.rues.block_pair())
            .await
            .map_err(|_| late())??
            .finalized();
        let account =
            tokio::time::timeout_at(deadline, self.rues.account_status(&signer.address_string()))
                .await
                .map_err(|_| late())??;
        let now = unix_millis();
        let grace = u64::try_from(self.limits.drop_grace.as_millis()).unwrap_or(u64::MAX);
        let mut held = BTreeMap::new();
        for reservation in reservations {
            let standing = self
                .standing_of(&reservation, finalized, &account, now, grace, deadline)
                .await;
            match standing {
                Standing::Final | Standing::Unsent => {
                    debug!(
                        nonce = reservation.nonce,
                        ?standing,
                        "Dusk nonce reservation settled"
                    );
                    self.release(&key, reservation.nonce);
                }
                standing => {
                    held.insert(reservation.nonce, (reservation, standing));
                }
            }
        }
        let mut legacy_live = 0;
        for (message_id, sent) in legacy {
            match self
                .legacy_verdict(&sent, finalized, &account, now, grace, deadline)
                .await
            {
                Legacy::Live => legacy_live += 1,
                Legacy::Kept => {}
                Legacy::Gone => {
                    debug!(?message_id, nonce = sent.nonce, "Dusk delivery the previous version left in flight can no longer be admitted; forgetting it");
                    if let Err(error) = self.rues.forget_sent_delivery(&key, message_id) {
                        warn!(?message_id, error = %bounded_error(&error), "Dusk delivery in flight cannot be forgotten in eventCursorDir");
                    }
                }
            }
        }
        Ok(Survey {
            account,
            held,
            legacy_live,
        })
    }

    /// Where `reservation` stands, with the node's last final block
    /// `finalized` and the signer's `account` read before.
    async fn standing_of(
        &self,
        reservation: &Reservation,
        finalized: u64,
        account: &AccountStatus,
        now: u64,
        grace: u64,
        deadline: tokio::time::Instant,
    ) -> Standing {
        let Some(newest) = reservation.txs.last().map(hex::encode) else {
            return Standing::Unsent;
        };
        let mut pending = None;
        let mut unseen = false;
        for tx in reservation.txs.iter().rev() {
            let tx_id = hex::encode(tx);
            match tokio::time::timeout_at(deadline, self.rues.transaction_status(&tx_id)).await {
                // One transaction of a nonce at most is in a block. One the
                // node still lists in its mempool too is kept until it has
                // left.
                Ok(Ok(TransactionStatus::Included {
                    block_height,
                    in_mempool,
                    ..
                })) => {
                    return if block_height <= finalized && !in_mempool {
                        Standing::Final
                    } else {
                        Standing::Landed(tx_id)
                    };
                }
                Ok(Ok(TransactionStatus::Pending)) => {
                    pending.get_or_insert(tx_id);
                }
                Ok(Ok(TransactionStatus::Unknown)) => {}
                Ok(Err(error)) => {
                    debug!(%tx_id, error = %bounded_error(&error), "Dusk transaction cannot be looked up; its nonce stays held");
                    unseen = true;
                }
                Err(_) => unseen = true,
            }
        }
        if let Some(tx_id) = pending {
            return Standing::Pending(tx_id);
        }
        if unseen {
            return Standing::Unsettled(Some(newest));
        }
        if reservation.nonce <= account.nonce {
            return Standing::Taken;
        }
        if now.saturating_sub(reservation.sent_at) < grace || reservation.nonce < account.next_nonce
        {
            return Standing::Unsettled(Some(newest));
        }
        Standing::Dropped
    }

    /// What a delivery the previous version left in flight means now, with
    /// the node's last final block `finalized` and the signer's `account`
    /// read before.
    async fn legacy_verdict(
        &self,
        sent: &SentDelivery,
        finalized: u64,
        account: &AccountStatus,
        now: u64,
        grace: u64,
        deadline: tokio::time::Instant,
    ) -> Legacy {
        let status = match sent.tx_id {
            Some(tx) => Some(
                tokio::time::timeout_at(deadline, self.rues.transaction_status(&hex::encode(tx)))
                    .await
                    .ok()
                    .and_then(Result::ok),
            ),
            None => None,
        };
        match status {
            Some(Some(TransactionStatus::Included {
                block_height,
                in_mempool,
                ..
            })) => {
                if block_height <= finalized && !in_mempool {
                    Legacy::Gone
                } else {
                    Legacy::Kept
                }
            }
            Some(Some(TransactionStatus::Pending) | None) => Legacy::Live,
            Some(Some(TransactionStatus::Unknown)) | None => {
                if sent.nonce > account.nonce
                    && (sent.nonce < account.next_nonce || now.saturating_sub(sent.sent_at) < grace)
                {
                    Legacy::Live
                } else {
                    Legacy::Gone
                }
            }
        }
    }
}

/// What a delivery the previous version left in flight means now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Legacy {
    /// Its transaction is in a final block, or nothing of it can be
    /// admitted any more: its nonce is used, or its transaction is in
    /// neither the mempool nor a block past the drop grace with its nonce
    /// not held. Forgotten.
    Gone,
    /// Its transaction is in a block that is not final yet: kept until it
    /// is, holding nothing up. A rollback would put it back into the
    /// mempool, where the node refuses another transaction of its nonce.
    Kept,
    /// It may still be admitted: its transaction is in the mempool or
    /// cannot be looked up, or its nonce is unused and held by the mempool
    /// or within the drop grace.
    Live,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account(nonce: u64, next_nonce: u64) -> AccountStatus {
        AccountStatus {
            balance: 1 << 40,
            nonce,
            next_nonce,
        }
    }

    fn reservation(nonce: u64, message: u8) -> Reservation {
        Reservation {
            nonce,
            holder: Holder::Delivery {
                message: H256::repeat_byte(message),
                gas_limit: 5_000_000,
                args: vec![message],
            },
            txs: vec![[message; 32]],
            sent_at: 1,
        }
    }

    fn ready(index: usize, message: u8) -> Ready {
        Ready {
            index,
            message_id: H256::repeat_byte(message),
            args: vec![message, 0],
            gas_limit: 5_000_000,
        }
    }

    fn survey(
        account: AccountStatus,
        held: impl IntoIterator<Item = (Reservation, Standing)>,
    ) -> Survey {
        Survey {
            account,
            held: held
                .into_iter()
                .map(|(reservation, standing)| (reservation.nonce, (reservation, standing)))
                .collect(),
            legacy_live: 0,
        }
    }

    /// A dropped delivery's nonce stays its own: a new delivery of the batch
    /// takes the next one, and both are sent. Before, a fresh delivery took
    /// a dropped one's nonce, and the second was lost from the outcome.
    #[test]
    fn a_dropped_delivery_keeps_its_nonce_and_a_new_one_takes_the_next() {
        let survey = survey(account(0, 1), [(reservation(1, 0xa), Standing::Dropped)]);
        let (steps, fates) = plan(&survey, &[ready(0, 0xa), ready(1, 0xb)], 64);
        assert_eq!(
            steps,
            [
                (1, Step::Again { ready: Some(0) }),
                (2, Step::Fresh { ready: 1 })
            ]
        );
        assert_eq!(fates.len(), 2);
    }

    /// At the bound, with every nonce dropped and one below them free, the
    /// batch fills the free nonce and sends the dropped ones again: none of
    /// that needs room under the bound. New deliveries wait.
    #[test]
    fn a_free_nonce_below_dropped_ones_is_filled_at_the_bound() {
        let survey = survey(
            account(0, 1),
            (2..=4).map(|nonce| (reservation(nonce, nonce as u8), Standing::Dropped)),
        );
        let (steps, fates) = plan(
            &survey,
            &[ready(0, 3), ready(1, 0xf)],
            // Three held, three allowed.
            3,
        );
        assert_eq!(
            steps,
            [
                (1, Step::Fill),
                (2, Step::Again { ready: None }),
                (3, Step::Again { ready: Some(0) }),
                (4, Step::Again { ready: None }),
            ]
        );
        assert!(matches!(&fates[1], Fate::LeftOut(reason) if reason.contains("outstanding")));
    }

    /// Reservations in blocks that are not final count toward the bound,
    /// so a rollback cannot put more than it back into the mempool.
    #[test]
    fn deliveries_in_blocks_that_are_not_final_count_toward_the_bound() {
        let survey = survey(
            account(2, 3),
            [
                (reservation(1, 1), Standing::Landed("aa".into())),
                (reservation(2, 2), Standing::Landed("bb".into())),
            ],
        );
        let (steps, fates) = plan(&survey, &[ready(0, 1), ready(1, 9)], 2);
        assert!(steps.is_empty());
        assert_eq!(fates[0], Fate::Followed("aa".into()));
        assert!(matches!(&fates[1], Fate::LeftOut(reason) if reason.contains("outstanding")));
    }

    /// The walk stops before a held nonce the node does not hold yet, and
    /// passes over one the mempool holds for a transaction this agent did
    /// not send.
    #[test]
    fn the_walk_stops_before_an_unsettled_nonce_and_passes_over_foreign_ones() {
        let unsettled = survey(
            account(0, 1),
            [
                (reservation(1, 1), Standing::Unsettled(Some("aa".into()))),
                (reservation(2, 2), Standing::Dropped),
            ],
        );
        let (steps, fates) = plan(&unsettled, &[ready(0, 2), ready(1, 9)], 64);
        assert!(steps.is_empty());
        for fate in fates {
            assert!(matches!(fate, Fate::LeftOut(reason) if reason.contains("nonce 1")));
        }
        // The mempool holds nonces 1 and 2 for transactions not ours.
        let foreign = survey(account(0, 3), []);
        let (steps, _) = plan(&foreign, &[ready(0, 1)], 64);
        assert_eq!(steps, [(3, Step::Fresh { ready: 0 })]);
    }

    /// Every delivery of a batch is followed, planned or left out with a
    /// reason, also past the batch's most.
    #[test]
    fn every_delivery_of_a_batch_is_accounted_for() {
        let survey = survey(account(0, 1), []);
        let batch: Vec<Ready> = (0..MAX_PIPELINED_DELIVERIES + 2)
            .map(|index| ready(index, (index % 250) as u8 + 1))
            .collect();
        let (steps, fates) = plan(&survey, &batch, usize::MAX);
        assert_eq!(steps.len(), MAX_PIPELINED_DELIVERIES);
        assert_eq!(fates.len(), batch.len());
        let left_out = fates
            .iter()
            .filter(|fate| matches!(fate, Fate::LeftOut(reason) if reason == "the batch is full"))
            .count();
        assert_eq!(left_out, 2);
    }
}
