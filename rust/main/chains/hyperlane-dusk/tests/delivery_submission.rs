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
async fn a_delivery_sent_again_while_its_transaction_waits_follows_that_transaction() {
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

    // The relayer sends it again: the Mailbox finds the first transaction
    // in the mempool and follows it, without running the helper, which
    // would sign the account's nonce plus one again for the node to refuse.
    let calls = node.invocations().len();
    let error = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("to be included"), "{error}");
    assert_eq!(node.invocations().len(), calls);
    assert_eq!(node.propagated(), [1]);
    assert!(node.refusals().is_empty(), "{:?}", node.refusals());
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
    // The Mailbox looks a transaction up at once, then a block later and
    // every quarter of a block after that: one in the next block is found
    // within a few looks.
    let mailbox = node
        .mailbox(false)
        .with_submission_limits(SubmissionLimits {
            receipt: Duration::from_millis(2_500),
            ..limits()
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
    // and the delivery fails. Sent again while its transaction may still
    // be on its way to the node, it is not sent; once it cannot be, it goes
    // with the same nonce.
    node.next_submission(2, "lose");
    let (message, metadata, gas_limit) = &deliveries[1];
    let mailbox = mailbox.with_submission_limits(SubmissionLimits {
        drop_grace: Duration::from_secs(60),
        ..patient(64)
    });
    let error = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("to be included"), "{error}");
    assert_eq!(node.propagated(), [1]);
    let calls = node.invocations().len();
    let error = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("may still reach the node"), "{error}");
    assert_eq!(node.invocations().len(), calls);
    let mailbox = mailbox.with_submission_limits(patient(64));
    tokio::time::sleep(PAST_DROP_GRACE).await;
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

/// The inherited race of deliveries sent one at a time: the relayer
/// submits a delivery again after its first transaction's outcome was
/// unknown and the wait for its block ended; the simulation passes, and the
/// first transaction lands before `dusk-tx` reads the account's nonce. The
/// helper then signed the next nonce, and a second transaction landed and
/// failed, paying its whole gas limit. The Mailbox kept the first
/// transaction's hash, and looks it up before it runs the helper again.
#[tokio::test]
async fn a_delivery_whose_first_transaction_lands_during_its_retry_is_not_sent_again() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(false);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    // The helper waits 200 ms for a block, the Mailbox 600 ms.
    node.helper_waits(10);
    let mailbox = node.mailbox(false).with_submission_limits(patient(64));
    let (message, metadata, gas_limit) = delivery(0);
    node.pause(true);
    let error = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("to be included"), "{error}");
    assert_eq!(node.propagated(), [1]);
    let first = node.model().mempool[&1].id.clone();

    // Submitted again past the 2 s a passed simulation stands for: the
    // simulation passes, and its answer comes once the first transaction is
    // in a block.
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    let gate = std::sync::Arc::new(Gate::default());
    node.inject(Route::Simulation, None, Fault::HoldReply(gate.clone()));
    let retry = tokio::spawn({
        let mailbox = mailbox.clone();
        let (message, metadata) = (message.clone(), metadata.clone());
        async move { mailbox.process(&message, &metadata, gas_limit).await }
    });
    node.wait_until("the simulation is answered", |_| {
        gate.waiting.load(std::sync::atomic::Ordering::SeqCst)
    })
    .await;
    node.pause(false);
    node.wait_until("the first transaction is in a block", |model| {
        model.included(&first).is_some()
    })
    .await;
    gate.open();
    // The Mailbox follows the first transaction instead of sending another.
    let outcome = retry.await.unwrap().unwrap();
    assert!(outcome.executed);
    assert_eq!(hex::encode(&outcome.transaction_id.as_bytes()[32..]), first);
    assert_eq!(node.propagated(), [1]);
    node.settle(1).await;
    assert!(!chose_a_nonce(&node));
    std::env::remove_var("DUSK_TX_BIN");
}

/// A delivery whose kept transaction failed in a block spent that nonce and
/// delivered nothing: once the block is final, it goes again; before, a
/// rollback could still run that transaction again.
#[tokio::test]
async fn a_delivery_whose_transaction_failed_goes_again_once_its_block_is_final() {
    let _env = HELPER_ENV.lock().await;
    // Finality moves only by hand.
    let node = Node::start_with(false, None);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(false).with_submission_limits(patient(64));
    let delivery = delivery(0);
    let (message, metadata, gas_limit) = &delivery;
    // Another relayer's delivery lands first, so ours fails in its block.
    node.pause(true);
    let first = tokio::spawn({
        let mailbox = mailbox.clone();
        let (message, metadata, gas_limit) = delivery.clone();
        async move { mailbox.process(&message, &metadata, gas_limit).await }
    });
    node.wait_until("the first transaction is in the mempool", |model| {
        !model.mempool.is_empty()
    })
    .await;
    node.deliver_elsewhere(&delivery);
    node.pause(false);
    let outcome = first.await.unwrap().unwrap();
    assert!(!outcome.executed);
    // The other delivery does not count after all, and the relayer
    // submits ours again: its failed transaction's block is not final.
    node.undeliver(&delivery);
    let calls = node.invocations().len();
    let error = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("not final yet"), "{error}");
    assert_eq!(node.invocations().len(), calls);
    // Once it is, the delivery goes again, with the next nonce.
    let height = node.height();
    node.finalize_to(height);
    node.expire_block_pair().await;
    let outcome = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap();
    assert!(outcome.executed);
    assert_eq!(node.propagated(), [1, 2]);
    std::env::remove_var("DUSK_TX_BIN");
}

/// A delivery refused before propagation reached nothing: it leaves no
/// transaction to follow, and goes again at once.
#[tokio::test]
async fn a_delivery_refused_before_propagation_goes_again_at_once() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(false);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(false).with_submission_limits(patient(64));
    let (message, metadata, gas_limit) = delivery(0);
    node.inject(
        Route::Preverify,
        None,
        Fault::Refuse("insufficient balance".into()),
    );
    let error = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("Preverify rejected before propagation"),
        "{error}"
    );
    assert!(node.propagated().is_empty());
    assert_eq!(mailbox.prepared_transaction(message.id()).unwrap(), None);
    let outcome = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap();
    assert!(outcome.executed);
    assert_eq!(node.propagated(), [1]);
    // The relayer confirms the delivery, and the kept transaction goes.
    assert!(mailbox
        .prepared_transaction(message.id())
        .unwrap()
        .is_some());
    mailbox.on_delivered(&message);
    assert_eq!(mailbox.prepared_transaction(message.id()).unwrap(), None);
    std::env::remove_var("DUSK_TX_BIN");
}

/// The transaction kept for a delivery survives a restart: submitted again
/// by the new process while that transaction waits in the mempool, the
/// delivery follows it.
#[tokio::test]
async fn a_delivery_kept_in_flight_is_followed_after_a_restart() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(false);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    node.helper_waits(10);
    let store = tempfile::tempdir().unwrap();
    let (message, metadata, gas_limit) = delivery(0);
    node.pause(true);
    let mailbox = node
        .durable_mailbox(store.path(), false)
        .with_submission_limits(patient(64));
    let error = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("to be included"), "{error}");
    drop(mailbox);
    node.restart(store.path());
    let mailbox = node
        .durable_mailbox(store.path(), false)
        .with_submission_limits(patient(64));
    let calls = node.invocations().len();
    let error = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("to be included"), "{error}");
    assert_eq!(node.invocations().len(), calls);
    assert!(node.refusals().is_empty(), "{:?}", node.refusals());
    node.settle(1).await;
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
        ..limits()
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
    node.inject(Route::Propagate, Some(2), Fault::LoseReply);
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

    // A transaction whose propagation never reached the node: once past
    // the drop grace, it goes again with its own nonce.
    node.pause(true);
    node.inject(Route::Propagate, Some(4), Fault::Drop);
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
    node.wait_until_final(4).await;
    node.expire_block_pair().await;
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
        ..limits()
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

// Regressions of the pipelining review. Each would fail against the
// pipelined deliveries of 841b494e9d.

/// The transaction of `nonce` in a block, if any.
fn included_at(node: &Node, nonce: u64) -> Option<Included> {
    node.included().into_iter().find(|tx| tx.tx.nonce == nonce)
}

#[tokio::test]
async fn a_dropped_delivery_keeps_its_nonce_and_no_delivery_vanishes_from_a_batch() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(true).with_submission_limits(patient(64));
    let deliveries: Vec<_> = (0..2).map(delivery).collect();
    node.pause(true);
    // The first goes with nonce 1, and the node drops it.
    mailbox.process_deliveries(&deliveries[..1]).await.unwrap();
    node.evict_from(1);
    tokio::time::sleep(PAST_DROP_GRACE).await;
    // A new delivery on its own used to take nonce 1 as well. The first
    // keeps it now, and goes again with it so that the new one, with the
    // next nonce, can land.
    let (message, metadata, gas_limit) = &deliveries[1];
    let _ = mailbox.process(message, metadata, *gas_limit).await;
    assert_eq!(node.propagated(), [1, 1, 2]);
    // The node drops both. Submitted again together, both go, each with
    // its own nonce: the first used to vanish from the batch, neither sent
    // nor left out, and the relayer dropped it.
    node.evict_from(1);
    tokio::time::sleep(PAST_DROP_GRACE).await;
    let batch = mailbox.process_deliveries(&deliveries).await.unwrap();
    assert!(
        batch.failed_indexes.is_empty(),
        "{:?}",
        batch.failed_indexes
    );
    assert!(batch.outcome.is_some());
    assert_eq!(node.propagated(), [1, 1, 2, 1, 2]);
    node.settle(2).await;
    assert_eq!(node.deliveries_included().len(), 2);
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn deliveries_the_previous_version_left_with_one_nonce_are_waited_out() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(true).with_submission_limits(patient(64));
    let deliveries: Vec<_> = (0..3).map(delivery).collect();
    // The previous version gave two messages nonce 1, the first with a
    // transaction the node never saw, the second with none named.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    mailbox
        .record_legacy_delivery(deliveries[0].0.id(), 1, Some([0xaa; 32]), now)
        .unwrap();
    mailbox
        .record_legacy_delivery(deliveries[1].0.id(), 1, None, now)
        .unwrap();
    // While they may still be admitted, nothing is sent, and every
    // delivery of the batch is left out.
    let batch = mailbox.process_deliveries(&deliveries).await.unwrap();
    assert!(batch.outcome.is_none());
    assert_eq!(batch.failed_indexes, [0, 1, 2]);
    let (message, metadata, gas_limit) = &deliveries[2];
    assert!(mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap_err()
        .to_string()
        .contains("previous version"));
    assert!(node.propagated().is_empty());
    // Past the drop grace they can no longer be admitted: they are
    // forgotten, and the batch takes consecutive nonces.
    tokio::time::sleep(PAST_DROP_GRACE).await;
    let batch = mailbox.process_deliveries(&deliveries).await.unwrap();
    assert!(batch.failed_indexes.is_empty());
    assert_eq!(node.propagated(), [1, 2, 3]);
    assert_eq!(mailbox.legacy_in_flight().unwrap(), 0);
    node.settle(3).await;
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn a_signer_at_its_bound_fills_a_nonce_whose_message_another_relayer_delivered() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(true).with_submission_limits(patient(4));
    let deliveries: Vec<_> = (0..5).map(delivery).collect();
    node.pause(true);
    let batch = mailbox.process_deliveries(&deliveries[..4]).await.unwrap();
    assert!(batch.failed_indexes.is_empty());
    // Another relayer delivers the first message, and the node drops all
    // four transactions: the first, and the later ones with it.
    node.deliver_elsewhere(&deliveries[0]);
    node.evict_from(1);
    tokio::time::sleep(PAST_DROP_GRACE).await;
    // At the bound, the signer used to stall for good. Its first nonce
    // now gets a filler, since that delivery would fail, and the others go
    // again with their nonces; the new delivery waits for room.
    let batch = mailbox.process_deliveries(&deliveries[1..]).await.unwrap();
    assert_eq!(batch.failed_indexes, [3]);
    assert_eq!(node.propagated(), [1, 2, 3, 4, 1, 2, 3, 4]);
    node.settle(4).await;
    assert_eq!(included_at(&node, 1).unwrap().tx.function, "state_version");
    for nonce in 2..=4 {
        assert_eq!(included_at(&node, nonce).unwrap().tx.function, "process");
        assert!(included_at(&node, nonce).unwrap().error.is_none());
    }
    // Once they are final, the new one goes.
    node.wait_until_final(4).await;
    node.expire_block_pair().await;
    let batch = mailbox.process_deliveries(&deliveries[4..]).await.unwrap();
    assert!(batch.failed_indexes.is_empty());
    assert_eq!(node.propagated(), [1, 2, 3, 4, 1, 2, 3, 4, 5]);
    node.settle(5).await;
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn a_dropped_delivery_outside_the_batch_goes_again_as_it_was_signed() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(true).with_submission_limits(patient(64));
    let deliveries: Vec<_> = (0..2).map(delivery).collect();
    node.pause(true);
    mailbox.process_deliveries(&deliveries).await.unwrap();
    node.evict_from(1);
    tokio::time::sleep(PAST_DROP_GRACE).await;
    // Only the second is submitted again: the first goes again too, with
    // the arguments it was signed with, so that the second can land.
    let batch = mailbox.process_deliveries(&deliveries[1..]).await.unwrap();
    assert!(batch.failed_indexes.is_empty());
    assert_eq!(node.propagated(), [1, 2, 1, 2]);
    node.settle(2).await;
    assert_eq!(node.deliveries_included().len(), 2);
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn a_transaction_is_named_before_it_is_sent_and_outlives_a_crash_and_a_rollback() {
    let _env = HELPER_ENV.lock().await;
    // Finality moves only by hand.
    let node = Node::start_with(true, None);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let store = tempfile::tempdir().unwrap();
    let deliveries: Vec<_> = (0..3).map(delivery).collect();
    let start = node.height();
    // The node holds the propagation of nonce 1 until the gate opens.
    let gate = std::sync::Arc::new(Gate::default());
    node.inject(Route::Propagate, Some(1), Fault::Hold(gate.clone()));
    let relayer = node
        .durable_mailbox(store.path(), true)
        .with_submission_limits(patient(64));
    let batch = {
        let relayer = relayer.clone();
        let deliveries = deliveries.clone();
        tokio::spawn(async move { relayer.process_deliveries(&deliveries[..1]).await })
    };
    node.wait_until("the propagation reaches the node", |_| {
        gate.waiting.load(std::sync::atomic::Ordering::SeqCst)
    })
    .await;
    // Before the node took it, its hash was kept with its nonce.
    let reservations = relayer.nonce_reservations().unwrap();
    assert_eq!(reservations.len(), 1);
    assert_eq!(reservations[0].0, 1);
    assert_eq!(reservations[0].1.len(), 1);
    let tx_id = reservations[0].1[0].clone();
    // The relayer stops while the node takes the transaction, which lands
    // in a block that is not final.
    batch.abort();
    let _ = batch.await;
    gate.open();
    node.wait_until("the transaction is in a block", |model| model.nonce == 1)
        .await;
    drop(relayer);
    node.restart(store.path());

    // Restarted, the relayer keeps that nonce although the account has
    // used it, since its block is not final: the next delivery takes the
    // nonce after it. It used to forget it, its hash never kept.
    let relayer = node
        .durable_mailbox(store.path(), true)
        .with_submission_limits(patient(64));
    let batch = relayer.process_deliveries(&deliveries[1..2]).await.unwrap();
    assert!(batch.failed_indexes.is_empty());
    assert_eq!(node.propagated(), [1, 2]);
    let held: Vec<u64> = relayer
        .nonce_reservations()
        .unwrap()
        .iter()
        .map(|(nonce, _)| *nonce)
        .collect();
    assert_eq!(held, [1, 2]);
    node.wait_until("both are in blocks", |model| model.nonce == 2)
        .await;
    // A rollback puts them back into the mempool, and the first message
    // is undelivered at the tip. Submitted again, it follows its
    // transaction instead of taking another nonce.
    node.pause(true);
    node.rollback_to(start);
    let again = relayer.process_deliveries(&deliveries[..1]).await.unwrap();
    assert!(again.failed_indexes.is_empty());
    assert_eq!(
        &again.outcome.unwrap().transaction_id.as_bytes()[32..],
        &hex::decode(&tx_id).unwrap()[..]
    );
    assert_eq!(node.propagated(), [1, 2]);
    node.settle(2).await;
    // Once their blocks are final, their nonces are released.
    let height = node.height();
    node.finalize_to(height);
    node.expire_block_pair().await;
    let batch = relayer.process_deliveries(&deliveries[2..]).await.unwrap();
    assert!(batch.failed_indexes.is_empty());
    assert_eq!(node.propagated(), [1, 2, 3]);
    let held: Vec<u64> = relayer
        .nonce_reservations()
        .unwrap()
        .iter()
        .map(|(nonce, _)| *nonce)
        .collect();
    assert_eq!(held, [3]);
    node.settle(3).await;
    drop(relayer);
    node.restart(store.path());
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn deliveries_in_blocks_that_are_not_final_count_toward_the_bound_through_a_rollback() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start_with(true, None);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(true).with_submission_limits(patient(2));
    let deliveries: Vec<_> = (0..3).map(delivery).collect();
    let start = node.height();
    let batch = mailbox.process_deliveries(&deliveries[..2]).await.unwrap();
    assert!(batch.failed_indexes.is_empty());
    node.wait_until("both are in blocks", |model| model.nonce == 2)
        .await;
    // In blocks that are not final, they still count: no third is sent,
    // in a batch or on its own.
    let batch = mailbox.process_deliveries(&deliveries[2..]).await.unwrap();
    assert!(batch.outcome.is_none());
    let (message, metadata, gas_limit) = &deliveries[2];
    let error = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("outstanding"), "{error}");
    // A rollback puts both back into the mempool: never more than the
    // bound is in flight.
    node.rollback_to(start);
    let batch = mailbox.process_deliveries(&deliveries[2..]).await.unwrap();
    assert!(batch.outcome.is_none());
    assert_eq!(node.propagated(), [1, 2]);
    // Once they are final, the third goes.
    node.wait_until("both are in blocks again", |model| model.nonce == 2)
        .await;
    let height = node.height();
    node.finalize_to(height);
    node.expire_block_pair().await;
    let batch = mailbox.process_deliveries(&deliveries[2..]).await.unwrap();
    assert!(batch.failed_indexes.is_empty());
    assert_eq!(node.propagated(), [1, 2, 3]);
    node.settle(3).await;
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn another_sender_of_the_key_stops_pipelining() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(true).with_submission_limits(patient(64));
    let deliveries: Vec<_> = (0..2).map(delivery).collect();
    node.pause(true);
    mailbox.process_deliveries(&deliveries).await.unwrap();
    // The node drops both, and another process that signs with the key
    // takes nonce 1.
    node.evict_from(1);
    node.send_foreign(1).unwrap();
    node.pause(false);
    node.wait_until("the other sender's transaction is in a block", |model| {
        model.nonce == 1
    })
    .await;
    tokio::time::sleep(PAST_DROP_GRACE).await;
    // The relayer finds a nonce it holds used by a transaction it did not
    // sign: it stops pipelining, and sends nothing.
    assert!(mailbox.supports_batching());
    let batch = mailbox.process_deliveries(&deliveries).await.unwrap();
    assert!(batch.outcome.is_none());
    assert_eq!(batch.failed_indexes, [0, 1]);
    assert!(!mailbox.supports_batching());
    // Deliveries go one at a time from then on, signed by dusk-tx.
    let calls = node.invocations().len();
    one_by_one(&mailbox, &deliveries).await;
    assert!(!chose_a_nonce_since(&node, calls));
    assert_eq!(node.propagated(), [1, 2, 1, 2, 3]);
    assert_eq!(mailbox.deliveries_in_flight().unwrap(), 0);
    node.settle(3).await;
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn a_batch_sends_nothing_past_its_deadline_whatever_the_helper_or_the_node_do() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(true).with_submission_limits(SubmissionLimits {
        batch: Duration::from_secs(1),
        ..patient(64)
    });
    let deliveries: Vec<_> = (0..2).map(delivery).collect();
    // The helper takes 5 s to sign: it is stopped at the batch's deadline,
    // nothing is sent, and the nonce it would have used is free again.
    node.next_submission(1, "hang 5");
    let started = std::time::Instant::now();
    let batch = mailbox.process_deliveries(&deliveries).await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    assert!(batch.outcome.is_none());
    assert_eq!(batch.failed_indexes, [0, 1]);
    assert!(node.propagated().is_empty());
    assert_eq!(mailbox.deliveries_in_flight().unwrap(), 0);

    // The node does not answer the account read that reconciliation needs:
    // the batch fails by its deadline, before anything is sent.
    let gate = std::sync::Arc::new(Gate::default());
    node.inject(Route::AccountStatus, None, Fault::Hold(gate.clone()));
    let started = std::time::Instant::now();
    assert!(mailbox.process_deliveries(&deliveries).await.is_err());
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    assert!(node.propagated().is_empty());
    gate.open();

    // Then the batch goes.
    let batch = mailbox.process_deliveries(&deliveries).await.unwrap();
    assert!(batch.failed_indexes.is_empty());
    assert_eq!(node.propagated(), [1, 2]);
    node.settle(2).await;
    std::env::remove_var("DUSK_TX_BIN");
}

// Regressions of the agents' last review: a delivery's transaction is kept
// before the helper may send it.

/// The stand-in sends its transaction only on the word `send`, which it
/// reads once it has printed the hash. A relayer that dies leaves it with a
/// closed stdin, and one that writes anything else gets nothing sent
/// either: it says so, as dusk-tx does.
#[test]
fn the_helper_sends_only_on_the_word_to_send() {
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Command, Stdio};

    let node = Node::start(false);
    for (word, sent) in [(None, false), (Some("nope"), false), (Some("send"), true)] {
        let mut helper = Command::new(node.helper())
            .args([
                "call",
                "--rues-url-stdin",
                "--expected-chain-id",
                "0",
                "--secret-key-stdin",
                "--contract",
                &"02".repeat(32),
                "--fn-name",
                "process",
                "--args",
                "aa",
                "--gas-limit",
                "5000000",
                "--gas-price",
                "1",
                "--ack-before-send",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdin = helper.stdin.take().unwrap();
        writeln!(stdin, "{}", node.url).unwrap();
        writeln!(stdin, "{}", "11".repeat(32)).unwrap();
        let mut stderr = BufReader::new(helper.stderr.take().unwrap());
        // Its first lines can be bash's warnings about the chain ID's byte.
        let mut line = String::new();
        while !line.contains("Prepared TX") {
            line.clear();
            assert!(stderr.read_line(&mut line).unwrap() > 0, "no hash printed");
        }
        if let Some(word) = word {
            writeln!(stdin, "{word}").unwrap();
        }
        drop(stdin);
        let mut rest = String::new();
        std::io::Read::read_to_string(&mut stderr, &mut rest).unwrap();
        let status = helper.wait().unwrap();
        assert_eq!(status.success(), sent, "{word:?}: {rest}");
        assert_eq!(rest.contains("Not submitted:"), !sent, "{word:?}: {rest}");
        assert_eq!(node.propagated().len(), usize::from(sent), "{word:?}");
    }
    assert_eq!(node.words(), ["1 <eof>", "1 nope", "1 send"]);
}

/// A delivery's transaction is kept before the node sees any of it: when
/// the helper preverifies it, the Mailbox keeps its hash already, since the
/// helper was let send it only then. Every delivery goes as before.
#[tokio::test]
async fn a_delivery_is_kept_before_the_helper_may_send_it() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(false);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(false).with_submission_limits(patient(64));
    let (message, metadata, gas_limit) = delivery(0);
    let gate = std::sync::Arc::new(Gate::default());
    node.inject(Route::Preverify, None, Fault::Hold(gate.clone()));
    let first = tokio::spawn({
        let mailbox = mailbox.clone();
        let (message, metadata) = (message.clone(), metadata.clone());
        async move { mailbox.process(&message, &metadata, gas_limit).await }
    });
    node.wait_until("the preverification reaches the node", |_| {
        gate.waiting.load(std::sync::atomic::Ordering::SeqCst)
    })
    .await;
    let kept = mailbox.prepared_transaction(message.id()).unwrap();
    assert!(kept.is_some());
    assert_eq!(node.words(), ["1 send"]);
    gate.open();
    let outcome = first.await.unwrap().unwrap();
    assert!(outcome.executed);
    assert_eq!(
        Some(hex::encode(&outcome.transaction_id.as_bytes()[32..])),
        kept
    );
    one_by_one(&mailbox, &(1..3).map(delivery).collect::<Vec<_>>()).await;
    assert_eq!(node.propagated(), [1, 2, 3]);
    assert_eq!(node.words(), ["1 send", "2 send", "3 send"]);
    assert!(node
        .invocations()
        .iter()
        .all(|call| call.contains("--ack-before-send")));
    node.settle(3).await;
    std::env::remove_var("DUSK_TX_BIN");
}

/// The relayer stops after the helper printed its transaction's hash and
/// before it was let send it: the helper sends nothing, and since nothing
/// was kept, the delivery goes again at once. The helper used to send it
/// on its own, whether the relayer kept its hash or not.
#[tokio::test]
async fn a_relayer_that_stops_before_it_lets_the_helper_send_sends_nothing() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(false);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    // A kept transaction would hold the next submission back for 60 s.
    let mailbox = node
        .mailbox(false)
        .with_submission_limits(SubmissionLimits {
            drop_grace: Duration::from_secs(60),
            ..patient(64)
        });
    let (message, metadata, gas_limit) = delivery(0);
    node.next_submission(1, "gate");
    let submission = tokio::spawn({
        let mailbox = mailbox.clone();
        let (message, metadata) = (message.clone(), metadata.clone());
        async move { mailbox.process(&message, &metadata, gas_limit).await }
    });
    node.wait_until("the helper waits at its gate", |_| {
        node.helper_reached("gated", 1)
    })
    .await;
    // This test's runtime runs on this thread alone, which from here on
    // runs nothing of the relayer: the helper prints its hash, and the
    // relayer stops before it reads it.
    node.open_gate(1);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !node.helper_reached("printed", 1) {
        assert!(
            std::time::Instant::now() < deadline,
            "the helper never printed its hash"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    submission.abort();
    assert!(submission.await.unwrap_err().is_cancelled());
    // Nothing reached the node, nothing was kept, and the helper was never
    // let send.
    tokio::time::sleep(BLOCK_TIME * 3).await;
    assert!(node.propagated().is_empty());
    assert!(node.refusals().is_empty(), "{:?}", node.refusals());
    assert!(node.words().iter().all(|word| word != "1 send"));
    assert_eq!(mailbox.prepared_transaction(message.id()).unwrap(), None);
    let outcome = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap();
    assert!(outcome.executed);
    assert_eq!(node.propagated(), [1]);
    node.settle(1).await;
    std::env::remove_var("DUSK_TX_BIN");
}

/// A delivery whose transaction's hash cannot be kept is not sent: the
/// helper is not let send it, and since nothing was kept, the delivery
/// goes again at once. The helper used to send it all the same.
#[tokio::test]
async fn a_delivery_whose_transaction_cannot_be_kept_is_not_sent_and_goes_again_at_once() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(false);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    // A kept transaction would hold the next submission back for 60 s.
    let mailbox = node
        .mailbox(false)
        .with_submission_limits(SubmissionLimits {
            drop_grace: Duration::from_secs(60),
            ..patient(64)
        });
    let (message, metadata, gas_limit) = delivery(0);
    mailbox.fail_prepared_writes(1, 0);
    let error = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("was not let send it"), "{error}");
    assert!(error.contains("Not submitted:"), "{error}");
    assert!(node.propagated().is_empty());
    assert!(node.refusals().is_empty(), "{:?}", node.refusals());
    assert_eq!(node.words(), ["1 <eof>"]);
    assert_eq!(mailbox.prepared_transaction(message.id()).unwrap(), None);
    let outcome = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap();
    assert!(outcome.executed);
    assert_eq!(node.propagated(), [1]);
    node.settle(1).await;
    std::env::remove_var("DUSK_TX_BIN");
}

/// A helper that sends on its own, before the agent kept its hash, is not
/// run: deliveries fail, naming the dusk-tx they need.
#[tokio::test]
async fn deliveries_need_a_helper_that_waits_for_the_word_to_send() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(false);
    node.without_ack();
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(false);
    let (message, metadata, gas_limit) = delivery(0);
    for _ in 0..2 {
        let error = mailbox
            .process(&message, &metadata, gas_limit)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("--ack-before-send"), "{error}");
        assert!(error.contains("hyperlane-dusk"), "{error}");
    }
    assert!(node.invocations().is_empty());
    assert!(node.propagated().is_empty());
    // Nor is a helper whose help cannot be read.
    std::env::set_var("DUSK_TX_BIN", node.directory.path().join("missing"));
    let error = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("Could not read `call --help`"), "{error}");
    assert!(node.propagated().is_empty());
    std::env::remove_var("DUSK_TX_BIN");
}

/// A signer keeps the transactions of at most `SubmissionLimits::prepared`
/// deliveries. At the bound, the kept ones are looked up, and only those in
/// final blocks are forgotten for room: one that may still land stays, and
/// while none can go, deliveries are not signed but fail, and go once kept
/// transactions settle. The one prepared longest ago used to go, whatever
/// became of its transaction.
#[tokio::test]
async fn a_signer_at_its_bound_of_kept_transactions_forgets_only_final_ones() {
    let _env = HELPER_ENV.lock().await;
    // Finality moves only by hand.
    let node = Node::start_with(false, None);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node
        .mailbox(false)
        .with_submission_limits(SubmissionLimits {
            prepared: 2,
            ..patient(64)
        });
    let lost = delivery(9);
    let deliveries: Vec<_> = (0..2).map(delivery).collect();
    // The helper names the first transaction but never sends it: for all
    // the relayer knows, it may still land, and it stays kept.
    node.next_submission(1, "lose");
    let (message, metadata, gas_limit) = &lost;
    let error = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("to be included"), "{error}");
    // A delivery lands, in a block that is not final yet.
    let (message, metadata, gas_limit) = &deliveries[0];
    assert!(
        mailbox
            .process(message, metadata, *gas_limit)
            .await
            .unwrap()
            .executed
    );
    assert_eq!(node.propagated(), [1]);
    // At the bound, neither kept transaction is in a final block: the next
    // delivery is not signed.
    let calls = node.invocations().len();
    let (message, metadata, gas_limit) = &deliveries[1];
    let error = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("may still land"), "{error}");
    assert_eq!(node.invocations().len(), calls);
    // Once the landed one's block is final, its transaction is forgotten
    // for room and the delivery goes; the one that may still land stays.
    let height = node.height();
    node.finalize_to(height);
    node.expire_block_pair().await;
    assert!(
        mailbox
            .process(message, metadata, *gas_limit)
            .await
            .unwrap()
            .executed
    );
    assert_eq!(node.propagated(), [1, 2]);
    assert!(mailbox.prepared_transaction(lost.0.id()).unwrap().is_some());
    assert_eq!(
        mailbox.prepared_transaction(deliveries[0].0.id()).unwrap(),
        None
    );
    assert!(mailbox
        .prepared_transaction(deliveries[1].0.id())
        .unwrap()
        .is_some());
    node.settle(2).await;
    std::env::remove_var("DUSK_TX_BIN");
}
