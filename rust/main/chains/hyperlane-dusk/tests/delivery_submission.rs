//! How the relayer's Mailbox submits deliveries, against a node that
//! handles the account's transactions as Rusk 1.7.3 does (see
//! `submission/mod.rs`): one at a time by default, and pipelined only when
//! a chain opts in.
#![cfg(unix)]

mod submission;

use std::time::Duration;

use hyperlane_core::{ChainCommunicationError, Mailbox, U256};
use hyperlane_dusk::SubmissionLimits;
use submission::*;

fn tx(id: &str, nonce: u64) -> Tx {
    Tx {
        id: id.into(),
        nonce,
        gas_price: 1,
        gas_limit: GAS_LIMIT,
        function: "process".into(),
        args: id.into(),
    }
}

/// The fixture node takes an account's transactions as Rusk 1.7.3 does.
#[test]
fn the_node_takes_transactions_as_rusk_does() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = Model::new(dir.path().to_path_buf(), None);
    // Ahead of the account's next nonce, with the ones between missing:
    // refused at preverification, queued at propagation.
    assert_eq!(
        node.preverify(&tx("c", 3)),
        Err(Refusal::MissingIntermediateNonce(1))
    );
    node.propagate(tx("c", 3)).unwrap();
    assert!(node.mempool.is_empty());
    // The account's next nonce is admitted. Another transaction of that
    // nonce is refused at the same gas price and would replace it at a
    // higher one; the same transaction again is refused.
    node.propagate(tx("a", 1)).unwrap();
    assert_eq!(node.preverify(&tx("a2", 1)), Err(Refusal::NonceHeld));
    assert_eq!(node.preverify(&tx("a", 1)), Err(Refusal::InMempool));
    let mut richer = tx("a3", 1);
    richer.gas_price = 2;
    assert_eq!(node.preverify(&richer), Ok(()));
    // Admitting nonce 2 admits the queued nonce 3 after it.
    node.propagate(tx("b", 2)).unwrap();
    assert_eq!(node.mempool.keys().copied().collect::<Vec<_>>(), [1, 2, 3]);
    assert_eq!(node.next_nonce(), 4);
    // A block holds what the mempool held when the block before it came.
    node.produce();
    assert_eq!(node.nonce, 0);
    node.produce();
    assert_eq!(node.nonce, 3);
    assert_eq!(node.next_nonce(), 4);
    assert_eq!(node.preverify(&tx("d", 2)), Err(Refusal::NonceUsed));
    assert_eq!(node.preverify(&tx("a", 4)), Err(Refusal::InLedger));
    // A message delivered again fails, paying its whole gas limit.
    let mut again = tx("again", 4);
    again.args = "a".into();
    node.propagate(again).unwrap();
    node.produce();
    node.produce();
    let failed = node.included("again").unwrap();
    assert!(failed.error.is_some());
    assert_eq!(failed.gas_spent, GAS_LIMIT);

    // The queue of future nonces tries a transaction `MAX_RETRIES` times,
    // then drops it.
    node.propagate(tx("f", 7)).unwrap();
    let start = std::time::Instant::now();
    for retry in 1..=MAX_RETRIES {
        node.retry_queue(start + RETRY_DELAY * (2 * retry));
    }
    assert!(node
        .events
        .iter()
        .any(|event| matches!(event, Event::Expired(tx) if tx.id == "f")));
    assert!(node.mempool.is_empty());

    // Evicting a transaction drops the later nonces after it.
    for (id, nonce) in [("g", 5), ("h", 6), ("i", 7)] {
        node.propagate(tx(id, nonce)).unwrap();
    }
    node.evict_from(6);
    assert_eq!(node.mempool.keys().copied().collect::<Vec<_>>(), [5]);

    // Finality moves on its own; a rollback reverts the blocks above a
    // height that is not final, and their transactions go back to the
    // mempool with the account's nonce.
    node.finalized = 3;
    node.produce();
    node.produce();
    assert_eq!(node.nonce, 5);
    let height = node.included("g").unwrap().height;
    node.rollback_to(height - 1);
    assert_eq!(node.nonce, 4);
    assert!(node.included("g").is_none());
    assert_eq!(node.mempool.get(&5).map(|tx| tx.id.as_str()), Some("g"));
    // The restored transaction lands again, in a block with another hash.
    node.produce();
    assert_eq!(node.included("g").unwrap().height, height);
    assert_eq!(node.nonce, 5);
}

/// Every delivery of `deliveries`, one after the other, through `process`.
async fn one_by_one(
    mailbox: &hyperlane_dusk::DuskMailbox,
    deliveries: &[(
        hyperlane_core::HyperlaneMessage,
        hyperlane_core::Metadata,
        Option<U256>,
    )],
) {
    for (message, metadata, gas_limit) in deliveries {
        let outcome = mailbox
            .process(message, metadata, *gas_limit)
            .await
            .unwrap();
        assert!(outcome.executed);
        assert_eq!(outcome.gas_used, U256::from(PROCESS_GAS));
    }
}

/// Whether the stand-in was ever asked to sign with a nonce of the agent's
/// choosing, or to return before the block.
fn chose_a_nonce(node: &Node) -> bool {
    node.invocations()
        .iter()
        .any(|call| call.contains("--nonce") || call.contains("--no-wait"))
}

#[tokio::test]
async fn deliveries_go_one_at_a_time_unless_pipelining_is_opted_in() {
    let _env = HELPER_ENV.lock().await;
    // A helper that could pipeline, and a configuration with a batch size,
    // as the deploy scripts write it, but no opt-in.
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(false);
    assert!(!mailbox.supports_batching());
    let deliveries: Vec<_> = (0..3).map(delivery).collect();
    // A batch is left to the relayer to send one at a time.
    let batch = mailbox.process_deliveries(&deliveries).await.unwrap();
    assert!(batch.outcome.is_none());
    assert_eq!(batch.failed_indexes, [0, 1, 2]);
    assert!(node.invocations().is_empty());

    one_by_one(&mailbox, &deliveries).await;
    // dusk-tx chose every nonce, the account's nonce plus one, and waited
    // for each block: each went once its predecessor was in a block.
    assert!(!chose_a_nonce(&node), "{:?}", node.invocations());
    assert_eq!(node.propagated(), [1, 2, 3]);
    let heights: Vec<u64> = node.included().iter().map(|tx| tx.height).collect();
    assert!(
        heights.windows(2).all(|pair| pair[1] > pair[0]),
        "{heights:?}"
    );
    node.settle(3).await;

    // Opted in, but with batches of one: still one at a time.
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let rues = std::sync::Arc::new(hyperlane_dusk::RuesClient::new(node.url.clone()).unwrap());
    let provider = std::sync::Arc::new(hyperlane_dusk::DuskProvider::new(domain(), rues.clone()));
    let mut conn = connection(&node.url, node.directory.path(), true);
    conn.op_submission_config.max_batch_size = 1;
    let mailbox = hyperlane_dusk::DuskMailbox::new(
        provider,
        rues,
        hyperlane_core::H256::from(MAILBOX),
        hyperlane_core::H256::from(HOOK),
        domain(),
        Some(hyperlane_dusk::DuskSigner::new(hyperlane_core::H256::repeat_byte(1)).unwrap()),
        conn,
    );
    assert!(!mailbox.supports_batching());
    one_by_one(&mailbox, &deliveries[..2]).await;
    assert!(!chose_a_nonce(&node), "{:?}", node.invocations());
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn a_delivery_sent_again_while_its_transaction_waits_is_refused() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(false);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    // The helper waits 200 ms for a block, the Mailbox 600 ms for the
    // transaction's.
    node.helper_waits(10);
    let mailbox = node.mailbox(false).with_submission_limits(patient(64));
    let (message, metadata, gas_limit) = delivery(0);
    // The chain takes no transaction for longer than either waits.
    node.pause(true);
    let error = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("to be included"), "{error}");
    assert_eq!(node.propagated(), [1]);

    // The relayer sends it again: dusk-tx signs with the account's nonce
    // plus one again, which the mempool holds for the first transaction, so
    // preverification refuses it before propagation.
    let error = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("Preverify rejected before propagation"),
        "{error}"
    );
    assert_eq!(node.propagated(), [1]);
    assert_eq!(node.refusals(), [(1, Refusal::NonceHeld)]);
    assert!(!chose_a_nonce(&node));

    // Once the chain takes the first, the message is delivered once, and
    // another submission, past the 2 s a passed simulation stands for,
    // fails its simulation without reaching the helper.
    node.settle(1).await;
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    let calls = node.invocations().len();
    let error = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap_err();
    assert!(
        matches!(error, ChainCommunicationError::SimulationFailed(_)),
        "{error}"
    );
    assert_eq!(node.invocations().len(), calls);
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn an_unknown_outcome_is_followed_by_the_prepared_hash() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(false);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    // The Mailbox looks a transaction up every 2 s: one in the next block
    // is found by the second look.
    let mailbox = node
        .mailbox(false)
        .with_submission_limits(SubmissionLimits {
            receipt: Duration::from_millis(2_500),
            ..SubmissionLimits::default()
        });
    let deliveries: Vec<_> = (0..2).map(delivery).collect();

    // The node takes the transaction, but the helper's answer is lost: the
    // Mailbox follows the hash the helper printed before it propagated.
    node.next_submission(1, "unknown");
    let (message, metadata, gas_limit) = &deliveries[0];
    let outcome = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap();
    assert!(outcome.executed);
    let included = node.included();
    assert_eq!(included.len(), 1);
    assert_eq!(
        &outcome.transaction_id.as_bytes()[32..],
        &hex::decode(&included[0].tx.id).unwrap()[..]
    );

    // The helper prints its hash but never sends: nothing is found by it,
    // and the delivery fails; sent again, it goes with the same nonce.
    node.next_submission(2, "lose");
    let (message, metadata, gas_limit) = &deliveries[1];
    let error = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("to be included"), "{error}");
    assert_eq!(node.propagated(), [1]);
    let outcome = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap();
    assert!(outcome.executed);
    assert_eq!(node.propagated(), [1, 2]);
    node.settle(2).await;
    assert!(!chose_a_nonce(&node));
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn queued_deliveries_share_blocks_when_pipelined() {
    const QUEUED: u32 = 12;
    let _env = HELPER_ENV.lock().await;

    // One at a time: each transaction is sent once the one before is in a
    // block, after the next block's round started, so it waits a block.
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(false);
    let deliveries: Vec<_> = (0..QUEUED).map(delivery).collect();
    let start = node.height();
    one_by_one(&mailbox, &deliveries).await;
    let serial_blocks = node.included().last().unwrap().height - start;
    drop(node);

    // Pipelined: consecutive nonces, each sent once the node holds the one
    // before, all in the next blocks.
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(true);
    assert!(mailbox.supports_batching());
    let start = node.height();
    let batch = mailbox.process_deliveries(&deliveries).await.unwrap();
    let included = node.included();
    let pipelined_blocks = included.last().unwrap().height - start;
    let outcome = batch.outcome.unwrap();
    assert!(batch.failed_indexes.is_empty());
    assert!(outcome.executed);
    assert_eq!(
        outcome.gas_used,
        U256::from(PROCESS_GAS * u64::from(QUEUED))
    );
    let blocks: std::collections::HashSet<u64> = included.iter().map(|tx| tx.height).collect();
    eprintln!(
        "{QUEUED} deliveries: one at a time {serial_blocks} blocks, pipelined {pipelined_blocks} blocks, in {} distinct blocks",
        blocks.len()
    );
    assert!(
        serial_blocks >= u64::from(QUEUED) * 2 - 1,
        "{serial_blocks}"
    );
    assert!(pipelined_blocks * 4 <= serial_blocks, "{pipelined_blocks}");
    assert!(blocks.len() <= 3, "{blocks:?}");
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn a_pipelined_batch_is_left_out_with_a_helper_that_cannot_take_a_nonce() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(false);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let batch = node
        .mailbox(true)
        .process_deliveries(&(0..3).map(delivery).collect::<Vec<_>>())
        .await
        .unwrap();
    assert!(batch.outcome.is_none());
    assert_eq!(batch.failed_indexes, [0, 1, 2]);
    assert!(node.invocations().is_empty());
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn a_refused_delivery_ends_the_batch_and_the_rest_go_one_at_a_time() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    // The node refuses the third transaction, nonce 3.
    node.inject(Route::Preverify, Some(3), Fault::Refuse("refused".into()));
    let batch = node
        .mailbox(true)
        .process_deliveries(&(0..5).map(delivery).collect::<Vec<_>>())
        .await
        .unwrap();
    // The first two are sent; the refused one and every later one are left
    // out, so no later nonce waits behind the one the node refused.
    assert_eq!(batch.failed_indexes, vec![2, 3, 4]);
    let outcome = batch.outcome.unwrap();
    assert_eq!(outcome.gas_used, U256::from(2 * PROCESS_GAS));
    node.wait_until("two transactions are in blocks", |model| model.nonce == 2)
        .await;
    assert_eq!(
        &outcome.transaction_id.as_bytes()[32..],
        &hex::decode(&node.included()[0].tx.id).unwrap()[..]
    );
    assert_eq!(node.propagated(), vec![1, 2]);
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn a_batch_sends_no_delivery_after_its_deadline() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    // Each submission takes the helper 300 ms.
    for nonce in 1..=10 {
        node.next_submission(nonce, "hang 0.3");
    }
    let mailbox = node.mailbox(true).with_submission_limits(SubmissionLimits {
        batch: Duration::from_secs(1),
        ..SubmissionLimits::default()
    });
    let batch = mailbox
        .process_deliveries(&(0..10).map(delivery).collect::<Vec<_>>())
        .await
        .unwrap();
    // Those sent within the second, and the rest left to be sent one at a
    // time, in order.
    let sent = 10 - batch.failed_indexes.len();
    assert!((1..=5).contains(&sent), "{sent} sent");
    assert_eq!(batch.failed_indexes, (sent..10).collect::<Vec<_>>());
    assert!(batch.outcome.unwrap().executed);
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn a_batch_still_held_after_its_receipt_wait_is_not_sent_again() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(true).with_submission_limits(patient(64));
    // The chain takes no transaction for longer than the batch waits for
    // their blocks.
    node.pause(true);
    let deliveries: Vec<_> = (0..3).map(delivery).collect();
    let batch = mailbox.process_deliveries(&deliveries).await.unwrap();
    assert!(batch.failed_indexes.is_empty());
    let first = batch.outcome.unwrap();
    assert!(!first.executed);
    assert_eq!(first.gas_used, U256::zero());
    assert_eq!(node.propagated(), [1, 2, 3]);

    // The relayer finds the messages undelivered and submits them again,
    // as a batch and one at a time: their transactions are still in the
    // mempool, so none is sent again, and the batch follows them.
    tokio::time::sleep(PAST_DROP_GRACE).await;
    let again = mailbox.process_deliveries(&deliveries).await.unwrap();
    assert!(again.failed_indexes.is_empty());
    assert_eq!(again.outcome.unwrap().transaction_id, first.transaction_id);
    let (message, metadata, gas_limit) = delivery(1);
    assert!(mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .is_err());
    assert_eq!(node.propagated(), [1, 2, 3]);

    // Once the chain takes them, each message is delivered once, and what
    // the relayer submits again is left out: its simulation fails.
    node.settle(3).await;
    let settled = mailbox.process_deliveries(&deliveries).await.unwrap();
    assert!(settled.outcome.is_none());
    assert_eq!(node.propagated(), [1, 2, 3]);
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn deliveries_the_mempool_dropped_are_sent_again_with_their_own_nonces() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(true).with_submission_limits(patient(64));
    node.pause(true);
    let deliveries: Vec<_> = (0..4).map(delivery).collect();
    mailbox.process_deliveries(&deliveries[..3]).await.unwrap();
    // The mempool drops the transactions of nonces 2 and 3, as Rusk's does
    // when it evicts one or it expires, and keeps nonce 1's.
    node.evict_from(2);
    tokio::time::sleep(PAST_DROP_GRACE).await;
    // Submitted again with a new message: the first is in flight and
    // followed, the dropped ones go with the nonces they own, and the new
    // one with the next.
    let again = mailbox.process_deliveries(&deliveries).await.unwrap();
    assert!(again.failed_indexes.is_empty());
    assert_eq!(node.propagated(), [1, 2, 3, 2, 3, 4]);
    node.settle(4).await;
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn deliveries_in_flight_are_not_sent_again_after_a_restart() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let store = tempfile::tempdir().unwrap();
    node.pause(true);
    let deliveries: Vec<_> = (0..3).map(delivery).collect();
    let relayer = node
        .durable_mailbox(store.path(), true)
        .with_submission_limits(patient(64));
    relayer.process_deliveries(&deliveries).await.unwrap();
    // The relayer restarts: its event store is closed and opened again.
    drop(relayer);
    node.restart(store.path());
    let relayer = node
        .durable_mailbox(store.path(), true)
        .with_submission_limits(patient(64));
    tokio::time::sleep(PAST_DROP_GRACE).await;
    relayer.process_deliveries(&deliveries).await.unwrap();
    assert_eq!(node.propagated(), [1, 2, 3]);
    node.settle(3).await;
    drop(relayer);
    node.restart(store.path());
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn a_delivery_whose_submission_outcome_is_unknown_is_reconciled_before_it_is_sent_again() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(true).with_submission_limits(patient(64));
    node.pause(true);
    let deliveries: Vec<_> = (0..4).map(delivery).collect();
    // The node takes nonce 2's transaction, but its answer is lost: the
    // batch sends no later nonce.
    node.next_submission(2, "unknown");
    let batch = mailbox.process_deliveries(&deliveries[..3]).await.unwrap();
    assert_eq!(batch.failed_indexes, [2]);
    assert_eq!(node.propagated(), [1, 2]);
    // Submitted again, the delivery in the mempool is followed, not sent
    // again; the one left out takes the next nonce.
    tokio::time::sleep(PAST_DROP_GRACE).await;
    let again = mailbox.process_deliveries(&deliveries[1..3]).await.unwrap();
    assert!(again.failed_indexes.is_empty());
    assert_eq!(node.propagated(), [1, 2, 3]);
    node.settle(3).await;

    // A submission whose hash the helper printed, but that never reached
    // the node: once past the drop grace, it goes again with its own nonce.
    node.pause(true);
    node.next_submission(4, "lose");
    let lost = mailbox.process_deliveries(&deliveries[3..]).await.unwrap();
    assert!(!lost.outcome.unwrap().executed);
    assert_eq!(node.propagated(), [1, 2, 3]);
    tokio::time::sleep(PAST_DROP_GRACE).await;
    mailbox.process_deliveries(&deliveries[3..]).await.unwrap();
    assert_eq!(node.propagated(), [1, 2, 3, 4]);
    node.settle(4).await;
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn one_at_a_time_a_dropped_delivery_goes_again_with_its_own_nonce() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(true).with_submission_limits(patient(64));
    node.pause(true);
    let deliveries: Vec<_> = (0..2).map(delivery).collect();
    mailbox.process_deliveries(&deliveries).await.unwrap();
    node.evict_from(1);
    tokio::time::sleep(PAST_DROP_GRACE).await;
    // The relayer sends each again on its own: each with the nonce it owns.
    for (message, metadata, gas_limit) in &deliveries {
        assert!(mailbox
            .process(message, metadata, *gas_limit)
            .await
            .is_err());
    }
    assert_eq!(node.propagated(), [1, 2, 1, 2]);
    node.settle(2).await;
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn a_signer_keeps_a_bounded_number_of_deliveries_outstanding() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(true).with_submission_limits(patient(4));
    node.pause(true);
    let deliveries: Vec<_> = (0..8).map(delivery).collect();
    let batch = mailbox.process_deliveries(&deliveries[..6]).await.unwrap();
    assert_eq!(batch.failed_indexes, [4, 5]);
    // While four are outstanding, no other delivery is sent, in a batch or
    // on its own.
    let batch = mailbox.process_deliveries(&deliveries[6..]).await.unwrap();
    assert!(batch.outcome.is_none());
    assert_eq!(batch.failed_indexes, [0, 1]);
    let (message, metadata, gas_limit) = &deliveries[6];
    let error = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("outstanding"), "{error}");
    assert_eq!(node.propagated(), [1, 2, 3, 4]);
    // Once they are in final blocks, the next ones go.
    node.settle(4).await;
    let batch = mailbox.process_deliveries(&deliveries[4..]).await.unwrap();
    assert!(batch.failed_indexes.is_empty());
    assert_eq!(node.propagated(), [1, 2, 3, 4, 5, 6, 7, 8]);
    node.settle(8).await;
    std::env::remove_var("DUSK_TX_BIN");
}

/// Whether a call after the first `calls` signed with a nonce the agent
/// chose.
fn chose_a_nonce_since(node: &Node, calls: usize) -> bool {
    node.invocations()[calls..]
        .iter()
        .any(|call| call.contains("--nonce") || call.contains("--no-wait"))
}

#[tokio::test]
async fn one_at_a_time_waits_out_deliveries_a_pipelined_run_left_in_flight() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let store = tempfile::tempdir().unwrap();
    let deliveries: Vec<_> = (0..4).map(delivery).collect();
    node.pause(true);
    let pipelined = node
        .durable_mailbox(store.path(), true)
        .with_submission_limits(patient(64));
    pipelined
        .process_deliveries(&deliveries[..3])
        .await
        .unwrap();
    assert_eq!(node.propagated(), [1, 2, 3]);
    drop(pipelined);
    node.restart(store.path());

    // The relayer comes back with pipelining off. While the pipelined
    // transactions wait in the mempool, neither a delivery that run sent
    // nor a new one goes.
    let relayer = node
        .durable_mailbox(store.path(), false)
        .with_submission_limits(patient(64));
    let calls = node.invocations().len();
    for (message, metadata, gas_limit) in [&deliveries[1], &deliveries[3]] {
        let error = relayer
            .process(message, metadata, *gas_limit)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("pipelined run"), "{error}");
    }
    assert_eq!(node.invocations().len(), calls);
    assert_eq!(node.propagated(), [1, 2, 3]);
    assert_eq!(relayer.deliveries_in_flight().unwrap(), 3);

    // Once they are in final blocks, deliveries go one at a time again,
    // each signed by dusk-tx, and nothing is kept in flight.
    node.pause(false);
    node.wait_until_final(3).await;
    node.expire_block_pair().await;
    let (message, metadata, gas_limit) = &deliveries[3];
    let outcome = relayer
        .process(message, metadata, *gas_limit)
        .await
        .unwrap();
    assert!(outcome.executed);
    assert_eq!(node.propagated(), [1, 2, 3, 4]);
    assert!(!chose_a_nonce_since(&node, calls));
    assert_eq!(relayer.deliveries_in_flight().unwrap(), 0);
    node.settle(4).await;
    drop(relayer);
    node.restart(store.path());
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn deliveries_the_mempool_dropped_hold_up_one_at_a_time_delivery_only_briefly() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let store = tempfile::tempdir().unwrap();
    let deliveries: Vec<_> = (0..2).map(delivery).collect();
    // Deliveries in neither the mempool nor a block count as dropped after
    // a second, and a batch waits 100 ms for its blocks.
    let limits = SubmissionLimits {
        receipt: Duration::from_millis(100),
        drop_grace: Duration::from_secs(1),
        ..SubmissionLimits::default()
    };
    node.pause(true);
    let pipelined = node
        .durable_mailbox(store.path(), true)
        .with_submission_limits(limits);
    pipelined.process_deliveries(&deliveries).await.unwrap();
    // The mempool drops both, and the relayer comes back with pipelining
    // off.
    node.evict_from(1);
    drop(pipelined);
    node.restart(store.path());
    let relayer = node
        .durable_mailbox(store.path(), false)
        .with_submission_limits(limits);
    let calls = node.invocations().len();
    // Within the drop grace they may still be on their way to the mempool.
    let (message, metadata, gas_limit) = &deliveries[0];
    let error = relayer
        .process(message, metadata, *gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("pipelined run"), "{error}");
    // After it, nothing of them can be admitted: they are forgotten, and
    // the deliveries go one at a time, with the nonces dusk-tx reads.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    node.pause(false);
    one_by_one(&relayer, &deliveries).await;
    assert_eq!(node.propagated(), [1, 2, 1, 2]);
    assert!(!chose_a_nonce_since(&node, calls));
    assert_eq!(relayer.deliveries_in_flight().unwrap(), 0);
    node.settle(2).await;
    drop(relayer);
    node.restart(store.path());
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn a_pipelined_delivery_in_a_block_that_is_not_final_holds_nothing_up() {
    let _env = HELPER_ENV.lock().await;
    // Finality moves only by hand.
    let node = Node::start_with(true, None);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let store = tempfile::tempdir().unwrap();
    let deliveries: Vec<_> = (0..3).map(delivery).collect();
    let start = node.height();
    let pipelined = node
        .durable_mailbox(store.path(), true)
        .with_submission_limits(patient(64));
    pipelined
        .process_deliveries(&deliveries[..1])
        .await
        .unwrap();
    node.wait_until("the pipelined delivery is in a block", |model| {
        model.nonce == 1
    })
    .await;
    drop(pipelined);
    node.restart(store.path());

    // Its block is not final: it is kept, but the next delivery goes one
    // at a time at once, above its nonce.
    let relayer = node
        .durable_mailbox(store.path(), false)
        .with_submission_limits(patient(64));
    let calls = node.invocations().len();
    let (message, metadata, gas_limit) = &deliveries[1];
    assert!(
        relayer
            .process(message, metadata, *gas_limit)
            .await
            .unwrap()
            .executed
    );
    assert_eq!(node.propagated(), [1, 2]);
    assert_eq!(relayer.deliveries_in_flight().unwrap(), 1);

    // A rollback puts both back into the mempool; the chain takes them
    // again in nonce order, and each message is delivered once.
    node.rollback_to(start);
    node.settle(2).await;
    assert_eq!(
        node.included()
            .iter()
            .map(|tx| tx.tx.nonce)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    // Once its block is final, it is forgotten.
    let height = node.height();
    node.finalize_to(height);
    node.expire_block_pair().await;
    let (message, metadata, gas_limit) = &deliveries[2];
    assert!(
        relayer
            .process(message, metadata, *gas_limit)
            .await
            .unwrap()
            .executed
    );
    assert_eq!(relayer.deliveries_in_flight().unwrap(), 0);
    assert!(!chose_a_nonce_since(&node, calls));
    node.settle(3).await;
    drop(relayer);
    node.restart(store.path());
    std::env::remove_var("DUSK_TX_BIN");
}
