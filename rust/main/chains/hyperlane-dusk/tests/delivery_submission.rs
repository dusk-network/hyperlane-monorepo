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
/// either: it says so, as dusk-tx does. Like a Moonlight transaction's, its
/// hash is that of what it signs, so the same call signs the same
/// transaction each time.
#[test]
fn the_helper_sends_only_on_the_word_to_send() {
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Command, Stdio};

    let node = Node::start(false);
    let mut hashes = Vec::new();
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
        hashes.push(line.trim_start()["Prepared TX ".len()..][..64].to_owned());
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
    assert!(hashes.iter().all(|hash| *hash == hashes[0]), "{hashes:?}");
    assert_eq!(node.included()[0].tx.id, hashes[0]);
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
    // With the nonce the helper named.
    assert_eq!(mailbox.prepared_nonce(message.id()).unwrap(), Some(1));
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
/// deliveries. At the bound, the kept ones are looked up, and only those
/// that can no longer land are forgotten for room: one that may still land
/// stays, and while none can go, deliveries are not signed but fail, and go
/// once kept transactions settle. The one prepared longest ago used to go,
/// whatever became of its transaction.
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
    // The helper names the first transaction, with nonce 1, but never
    // sends it: for all the relayer knows, it may still land, and it stays
    // kept while nonce 1 may still be free.
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
    // At the bound, neither kept transaction can go: the landed one is in a
    // block that is not final, and nonce 1, which the lost one is signed
    // with, is used only there, so a rollback could free it again. The next
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
    // for room and the delivery goes. The lost one stays: its nonce is used
    // in a final block now, but a second reading must show it used too, 10
    // minutes after the first.
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

/// The agent and a dusk-tx build agree on `--ack-before-send`. A hash the
/// agent cannot keep leaves dusk-tx without the word: the node gets the
/// chain ID and account queries alone, and dusk-tx's refusal reads as
/// `NotSubmitted`. A hash the agent keeps lets dusk-tx preverify its
/// transaction, only then; the stand-in node refuses a real Moonlight
/// transaction there, so that delivery is not submitted either. Runs only
/// when asked (`--ignored`), with `DUSK_TX_REAL_BIN` naming the build.
#[tokio::test]
#[ignore = "runs a dusk-tx build: set DUSK_TX_REAL_BIN to its path"]
async fn a_dusk_tx_build_sends_only_on_the_agents_word() {
    let _env = HELPER_ENV.lock().await;
    let bin = std::env::var("DUSK_TX_REAL_BIN").expect("DUSK_TX_REAL_BIN names a dusk-tx build");
    let node = Node::start(false);
    std::env::set_var("DUSK_TX_BIN", &bin);
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
    assert!(
        error.contains("Not submitted: stdin closed before the line \"send\""),
        "{error}"
    );
    assert_eq!(node.requests_to("/chain_id"), 1);
    assert_eq!(node.requests_to("/account"), 1);
    assert_eq!(node.requests_to("/preverify"), 0);
    assert_eq!(node.requests_to("/propagate"), 0);
    assert_eq!(mailbox.prepared_transaction(message.id()).unwrap(), None);

    // Kept, the transaction is preverified only once it is.
    let gate = std::sync::Arc::new(Gate::default());
    node.inject(Route::Preverify, None, Fault::Hold(gate.clone()));
    let submission = tokio::spawn({
        let mailbox = mailbox.clone();
        let (message, metadata) = (message.clone(), metadata.clone());
        async move { mailbox.process(&message, &metadata, gas_limit).await }
    });
    node.wait_until("the preverification reaches the node", |_| {
        gate.waiting.load(std::sync::atomic::Ordering::SeqCst)
    })
    .await;
    let kept = mailbox.prepared_transaction(message.id()).unwrap().unwrap();
    // A dusk-tx that names the nonce it signed with: the account's next.
    assert_eq!(mailbox.prepared_nonce(message.id()).unwrap(), Some(1));
    gate.open();
    let error = submission.await.unwrap().unwrap_err().to_string();
    assert!(
        error.contains(&format!(
            "Transaction {kept} submission failed: Preverify rejected before propagation"
        )),
        "{error}"
    );
    assert_eq!(node.requests_to("/propagate"), 0);
    assert_eq!(mailbox.prepared_transaction(message.id()).unwrap(), None);
    std::env::remove_var("DUSK_TX_BIN");
}

/// A delivery's transaction that fails in a block after its submission
/// returned, its outcome unknown, is the delivery's outcome when the
/// relayer submits it again: a failure, with the transaction and the gas it
/// spent, and no other transaction is sent in that call. The relayer then
/// charges that gas to the message before its gas payment policy decides on
/// another attempt. The transaction used to be forgotten once its block was
/// final, and another sent at once, so the first one's gas was never
/// reported and the message's payment paid for more attempts.
#[tokio::test]
async fn a_transaction_that_failed_after_its_submission_returned_is_reported_with_its_gas() {
    let _env = HELPER_ENV.lock().await;
    // Finality moves only by hand.
    let node = Node::start_with(false, None);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    // The helper waits 200 ms for a block, the Mailbox 600 ms.
    node.helper_waits(10);
    let mailbox = node.mailbox(false).with_submission_limits(patient(64));
    let delivery = delivery(0);
    let (message, metadata, gas_limit) = &delivery;
    // The chain takes no transaction for longer than either waits.
    node.pause(true);
    let error = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("to be included"), "{error}");
    let first = node.model().mempool[&1].id.clone();
    // Meanwhile the recipient started to revert: the transaction fails in
    // its block, which becomes final, and the message stays undelivered.
    node.recipient_reverts(&delivery, true);
    node.pause(false);
    node.wait_until("the first transaction is in a block", |model| {
        model.included(&first).is_some()
    })
    .await;
    let height = node.height();
    node.finalize_to(height);
    node.expire_block_pair().await;
    // Once the recipient takes it again, the relayer submits the delivery
    // again: it reports that failure and sends nothing.
    node.recipient_reverts(&delivery, false);
    let calls = node.invocations().len();
    let outcome = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap();
    assert!(!outcome.executed);
    assert_eq!(outcome.gas_used, U256::from(GAS_LIMIT));
    assert_eq!(hex::encode(&outcome.transaction_id.as_bytes()[32..]), first);
    assert_eq!(node.invocations().len(), calls);
    assert_eq!(node.propagated(), [1]);
    // Submitted again, as the relayer does while the message's payment
    // covers another attempt, it goes with the next nonce; the failure is
    // not reported twice.
    let outcome = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap();
    assert!(outcome.executed);
    assert_eq!(node.propagated(), [1, 2]);
    std::env::remove_var("DUSK_TX_BIN");
}

/// When the node refuses a delivery at preverification, the Mailbox forgets
/// the transaction it kept for it. When that fails, the delivery still
/// fails with the helper's error, not the event store's, since 72981977cb:
/// the transaction stays kept, and holds the next submission back only for
/// the drop grace.
#[tokio::test]
async fn a_refused_delivery_keeps_the_helpers_error_when_its_transaction_cannot_be_forgotten() {
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
    mailbox.fail_prepared_writes(0, 1);
    let error = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("Preverify rejected before propagation"),
        "{error}"
    );
    assert!(error.contains("insufficient balance"), "{error}");
    assert!(!error.contains("eventCursorDir"), "{error}");
    assert!(node.propagated().is_empty());
    assert!(mailbox
        .prepared_transaction(message.id())
        .unwrap()
        .is_some());
    let error = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("may still reach the node"), "{error}");
    tokio::time::sleep(PAST_DROP_GRACE).await;
    let outcome = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap();
    assert!(outcome.executed);
    assert_eq!(node.propagated(), [1]);
    node.settle(1).await;
    std::env::remove_var("DUSK_TX_BIN");
}

/// At the bound, a kept transaction that failed in a final block is not
/// forgotten for room before its failure is reported: its gas would never
/// be charged to its message. Once the relayer submits that message again
/// and the failure is reported, it can go. Room-making used to forget
/// every transaction in a final block, failed or not.
#[tokio::test]
async fn a_failure_not_reported_yet_is_not_forgotten_for_room() {
    let _env = HELPER_ENV.lock().await;
    // Finality moves only by hand.
    let node = Node::start_with(false, None);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    // The helper waits 200 ms for a block, the Mailbox 600 ms.
    node.helper_waits(10);
    let mailbox = node
        .mailbox(false)
        .with_submission_limits(SubmissionLimits {
            prepared: 2,
            ..patient(64)
        });
    let deliveries: Vec<_> = (0..3).map(delivery).collect();
    // The first delivery's outcome is unknown; its recipient then reverts,
    // and its transaction fails in a block that becomes final.
    node.pause(true);
    let (message, metadata, gas_limit) = &deliveries[0];
    let error = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("to be included"), "{error}");
    node.recipient_reverts(&deliveries[0], true);
    node.pause(false);
    node.wait_until("the first transaction is in a block", |model| {
        model.nonce == 1
    })
    .await;
    let height = node.height();
    node.finalize_to(height);
    node.expire_block_pair().await;
    // The second lands, in a block that is not final yet.
    let (message, metadata, gas_limit) = &deliveries[1];
    assert!(
        mailbox
            .process(message, metadata, *gas_limit)
            .await
            .unwrap()
            .executed
    );
    // At the bound, neither can go: the third delivery is not signed.
    let calls = node.invocations().len();
    let (message, metadata, gas_limit) = &deliveries[2];
    let error = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("the most it keeps"), "{error}");
    assert_eq!(node.invocations().len(), calls);
    assert!(mailbox
        .prepared_transaction(deliveries[0].0.id())
        .unwrap()
        .is_some());
    // The recipient takes the first message again, and submitted again,
    // its failure is reported.
    node.recipient_reverts(&deliveries[0], false);
    let (message, metadata, gas_limit) = &deliveries[0];
    assert!(
        !mailbox
            .process(message, metadata, *gas_limit)
            .await
            .unwrap()
            .executed
    );
    // Reported, and with the second's block final, both go for room.
    let height = node.height();
    node.finalize_to(height);
    node.expire_block_pair().await;
    let (message, metadata, gas_limit) = &deliveries[2];
    assert!(
        mailbox
            .process(message, metadata, *gas_limit)
            .await
            .unwrap()
            .executed
    );
    assert_eq!(
        mailbox.prepared_transaction(deliveries[0].0.id()).unwrap(),
        None
    );
    assert_eq!(node.propagated(), [1, 2, 3]);
    std::env::remove_var("DUSK_TX_BIN");
}

/// Name a transaction for `delivery` with `nonce` and keep it, but have the
/// helper never send it, as when its propagation is lost: the delivery
/// fails with its outcome unknown.
async fn lose(
    node: &Node,
    mailbox: &hyperlane_dusk::DuskMailbox,
    delivery: &(
        hyperlane_core::HyperlaneMessage,
        hyperlane_core::Metadata,
        Option<U256>,
    ),
    nonce: u64,
) {
    node.next_submission(nonce, "lose");
    let (message, metadata, gas_limit) = delivery;
    let error = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("to be included"), "{error}");
}

/// Deliver `delivery`, and confirm it delivered, as the relayer does.
async fn deliver(
    mailbox: &hyperlane_dusk::DuskMailbox,
    delivery: &(
        hyperlane_core::HyperlaneMessage,
        hyperlane_core::Metadata,
        Option<U256>,
    ),
) {
    let (message, metadata, gas_limit) = delivery;
    assert!(
        mailbox
            .process(message, metadata, *gas_limit)
            .await
            .unwrap()
            .executed
    );
    mailbox.on_delivered(message);
}

/// The review's scenario. Transactions of messages the relayer gave up on,
/// which never landed, fill the bound, while the signer's account uses
/// their nonces. Once two readings, the gap apart, show those nonces used,
/// the first at a final block, the transactions can never execute: they are
/// forgotten for room, and deliveries go again. A nonce used only in a
/// block that is not final is not enough, since a rollback could free it,
/// nor is a single reading; and a transaction kept without its nonce, by a
/// dusk-tx from before it named it, stays until it is in a final block.
/// They used to stay for good, and no delivery could be signed again.
#[tokio::test]
async fn abandoned_transactions_go_for_room_once_their_nonces_are_used_for_good() {
    let _env = HELPER_ENV.lock().await;
    // Finality moves only by hand.
    let node = Node::start_with(false, None);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let gap = Duration::from_secs(3);
    let mailbox = node
        .mailbox(false)
        .with_submission_limits(SubmissionLimits {
            prepared: 3,
            nonce_gap: gap,
            ..patient(64)
        });
    let abandoned: Vec<_> = (7..10).map(delivery).collect();
    // Two are kept with nonces 1 and 2, which deliveries then use; the
    // third with nonce 3, by a dusk-tx that names no nonce.
    lose(&node, &mailbox, &abandoned[0], 1).await;
    deliver(&mailbox, &delivery(0)).await;
    lose(&node, &mailbox, &abandoned[1], 2).await;
    deliver(&mailbox, &delivery(1)).await;
    node.old_prepared_line(true);
    lose(&node, &mailbox, &abandoned[2], 3).await;
    node.old_prepared_line(false);
    let nonces: Vec<_> = abandoned
        .iter()
        .map(|(message, _, _)| mailbox.prepared_nonce(message.id()).unwrap())
        .collect();
    assert_eq!(nonces, [Some(1), Some(2), None]);
    // At the bound, with nonces 1 and 2 used in blocks that are not final,
    // nothing can go: the next delivery is not signed, also once a reading
    // of the account has waited a few blocks.
    let calls = node.invocations().len();
    let (message, metadata, gas_limit) = delivery(4);
    for _ in 0..2 {
        let error = mailbox
            .process(&message, &metadata, gas_limit)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("the most it keeps"), "{error}");
        tokio::time::sleep(BLOCK_TIME * 3).await;
    }
    assert_eq!(node.invocations().len(), calls);
    // Once those blocks are final, the reading is proven, but a single
    // reading does not count: the delivery still waits.
    let height = node.height();
    node.finalize_to(height);
    let error = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("the most it keeps"), "{error}");
    assert_eq!(node.invocations().len(), calls);
    // The gap later, a second reading shows nonces 1 and 2 used too: those
    // transactions go, and the delivery goes; the one kept without its
    // nonce stays.
    tokio::time::sleep(gap).await;
    let outcome = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap();
    assert!(outcome.executed);
    assert_eq!(node.propagated(), [1, 2, 3]);
    let kept: Vec<_> = abandoned
        .iter()
        .map(|(message, _, _)| {
            mailbox
                .prepared_transaction(message.id())
                .unwrap()
                .is_some()
        })
        .collect();
    assert_eq!(kept, [false, false, true]);
    std::env::remove_var("DUSK_TX_BIN");
}

/// Deliveries of any message sweep the kept transactions of others, well
/// below the bound: an abandoned one goes once its nonce is used for good,
/// so that such transactions never pile up to the bound.
#[tokio::test]
async fn deliveries_sweep_abandoned_transactions_whose_nonces_are_used_for_good() {
    let _env = HELPER_ENV.lock().await;
    // Finality follows the tip one block behind.
    let node = Node::start(false);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node
        .mailbox(false)
        .with_submission_limits(SubmissionLimits {
            sweep: Duration::ZERO,
            nonce_gap: Duration::from_secs(1),
            ..patient(64)
        });
    let abandoned = delivery(9);
    lose(&node, &mailbox, &abandoned, 1).await;
    // Deliveries go on, the first using nonce 1, and each sweeps: once two
    // readings the gap apart show nonce 1 used, the abandoned one goes.
    let mut delivered = 0;
    while mailbox
        .prepared_transaction(abandoned.0.id())
        .unwrap()
        .is_some()
    {
        assert!(delivered < 12, "the abandoned transaction was never swept");
        deliver(&mailbox, &delivery(delivered)).await;
        delivered += 1;
        tokio::time::sleep(BLOCK_TIME * 3).await;
    }
    assert_eq!(
        node.propagated(),
        (1..=u64::from(delivered)).collect::<Vec<_>>()
    );
    std::env::remove_var("DUSK_TX_BIN");
}

/// A Mailbox that submits one at a time, on a node whose blocks come, and
/// become final, only by hand, with kept transactions forgotten once two
/// readings of the account, `gap` apart, show their nonces used; and the
/// delivery whose transaction T, signed with nonce 1, it keeps while T
/// waits in the mempool.
async fn keep_one_in_the_mempool(
    gap: Duration,
) -> (
    Node,
    hyperlane_dusk::DuskMailbox,
    (
        hyperlane_core::HyperlaneMessage,
        hyperlane_core::Metadata,
        Option<U256>,
    ),
) {
    let node = Node::start_with(false, None);
    node.freeze(true);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    node.helper_waits(10);
    let mailbox = node
        .mailbox(false)
        .with_submission_limits(SubmissionLimits {
            nonce_gap: gap,
            ..patient(64)
        });
    let kept = delivery(9);
    let (message, metadata, gas_limit) = &kept;
    let error = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("to be included"), "{error}");
    assert_eq!(mailbox.prepared_nonce(message.id()).unwrap(), Some(1));
    assert_eq!(
        node.model().mempool.keys().copied().collect::<Vec<_>>(),
        [1]
    );
    (node, mailbox, kept)
}

/// Look the signer's kept transactions up, as a delivery's sweep does,
/// while the node's chain goes from its tip P to a child C and back to P
/// around the reading of the account: the first read of the tip returns P;
/// C comes, with what the mempool held at P, and makes P final; the account
/// read shows C's transactions; C is rolled back, they go back into the
/// mempool, and `then` runs; and the second read of the tip returns P
/// again. How many kept transactions were forgotten.
async fn reclaim_across_a_rolled_back_block(
    node: &Node,
    mailbox: &hyperlane_dusk::DuskMailbox,
    then: impl FnOnce(&Node),
) -> usize {
    let parent = node.height();
    let account = std::sync::Arc::new(Gate::default());
    node.inject(Route::AccountStatus, None, Fault::Hold(account.clone()));
    let reclaim = tokio::spawn({
        let mailbox = mailbox.clone();
        async move { mailbox.reclaim_kept_transactions().await }
    });
    node.wait_until("the account read reaches the node", |_| {
        account.waiting.load(std::sync::atomic::Ordering::SeqCst)
    })
    .await;
    let tip = std::sync::Arc::new(Gate::default());
    node.inject(Route::Graphql, None, Fault::Hold(tip.clone()));
    node.produce_block();
    node.finalize_to(parent);
    account.open();
    node.wait_until("the second read of the tip reaches the node", |_| {
        tip.waiting.load(std::sync::atomic::Ordering::SeqCst)
    })
    .await;
    node.rollback_to(parent);
    then(node);
    tip.open();
    reclaim.await.unwrap().unwrap()
}

/// The review's sequence (w14). While the agent reads the signer's account
/// between two reads of the tip, block C, child of the tip P, comes with
/// the kept transaction T at nonce 1 and makes P final; the account read
/// shows nonce 1; C is rolled back, and T goes back into the mempool; the
/// second read of the tip returns P again. Nonce 1 was taken for P's,
/// final, and T's record went while T could still land: a delivery sent
/// again would then have paid for a second, failing transaction. T now
/// stays: one reading never counts, and a transaction in the mempool
/// always stays, even once two readings the gap apart, each across such a
/// rollback, show its nonce used. T then lands, its record goes once its
/// block is final, and nothing else was ever signed.
#[tokio::test]
async fn a_kept_transaction_a_rollback_puts_back_stays_despite_matching_tips() {
    let _env = HELPER_ENV.lock().await;
    let gap = Duration::from_secs(1);
    let (node, mailbox, (message, _, _)) = keep_one_in_the_mempool(gap).await;
    let kept = node.model().mempool[&1].id.clone();
    let calls = node.invocations().len();
    // P, the block C will follow.
    node.produce_block();
    assert_eq!(
        reclaim_across_a_rolled_back_block(&node, &mailbox, |_| {}).await,
        0
    );
    assert!(mailbox
        .prepared_transaction(message.id())
        .unwrap()
        .is_some());
    assert_eq!(node.model().mempool[&1].id, kept);
    // The gap later, the same again: two readings show nonce 1 used, and T,
    // in the mempool, stays.
    tokio::time::sleep(gap).await;
    assert_eq!(
        reclaim_across_a_rolled_back_block(&node, &mailbox, |_| {}).await,
        0
    );
    assert!(mailbox
        .prepared_transaction(message.id())
        .unwrap()
        .is_some());
    // T lands, and once its block is final its record goes; nothing else
    // was signed.
    node.finality_lag(Some(1));
    node.freeze(false);
    node.wait_until_final(1).await;
    assert_eq!(mailbox.reclaim_kept_transactions().await.unwrap(), 1);
    assert_eq!(node.included()[0].tx.id, kept);
    assert_eq!(node.invocations().len(), calls);
    assert_eq!(node.propagated(), [1]);
    node.settle(1).await;
    std::env::remove_var("DUSK_TX_BIN");
}

/// One reading of the signer's account never counts. As in the review's
/// sequence, a block holding the kept transaction T at nonce 1 is rolled
/// back around a reading, which takes nonce 1 for its final parent's; then
/// the node drops T, now in neither its mempool nor a block. Nonce 1 is
/// free, and T, which peers may hold, could still land: it stays, also the
/// gap later, when the account, read again, shows nonce 1 unused. Once
/// another transaction uses nonce 1 for good, two readings the gap apart
/// show it, and T goes.
#[tokio::test]
async fn a_single_reading_of_the_account_never_forgets_a_kept_transaction() {
    let _env = HELPER_ENV.lock().await;
    let gap = Duration::from_secs(1);
    let (node, mailbox, (message, _, _)) = keep_one_in_the_mempool(gap).await;
    node.produce_block();
    assert_eq!(
        reclaim_across_a_rolled_back_block(&node, &mailbox, |node| node.evict_from(1)).await,
        0
    );
    assert!(node.model().mempool.is_empty());
    assert!(mailbox
        .prepared_transaction(message.id())
        .unwrap()
        .is_some());
    // The chain goes on without nonce 1: the gap later, the account read
    // again shows it unused, and T stays.
    node.produce_block();
    assert_eq!(mailbox.reclaim_kept_transactions().await.unwrap(), 0);
    tokio::time::sleep(gap).await;
    assert_eq!(mailbox.reclaim_kept_transactions().await.unwrap(), 0);
    assert!(mailbox
        .prepared_transaction(message.id())
        .unwrap()
        .is_some());
    // Another transaction uses nonce 1 in a block made final: T goes once
    // two readings the gap apart show it.
    node.send_foreign(1).unwrap();
    node.produce_block();
    node.produce_block();
    node.finalize_to(node.height());
    let deadline = std::time::Instant::now() + gap * 5;
    while mailbox
        .prepared_transaction(message.id())
        .unwrap()
        .is_some()
    {
        assert!(
            std::time::Instant::now() < deadline,
            "T was never forgotten"
        );
        mailbox.reclaim_kept_transactions().await.unwrap();
        tokio::time::sleep(gap / 4).await;
    }
    std::env::remove_var("DUSK_TX_BIN");
}

/// A reading whose block was replaced before its height became final does
/// not count: its nonce came from a block the chain left. Here another
/// transaction uses nonce 1 in a block a reading reads; that block is
/// rolled back, the node drops the transaction, and a block that does not
/// use nonce 1 takes its place and becomes final. A block at the tip then
/// uses nonce 1 again, and the account, read the gap later, shows it. The
/// transaction kept with nonce 1, lost before it was sent, stays: nonce 1
/// is used only in a block that is not final, and once that one is rolled
/// back too, the kept one could still execute.
#[tokio::test]
async fn a_reading_whose_block_was_replaced_never_counts() {
    let _env = HELPER_ENV.lock().await;
    // Finality moves only by hand.
    let node = Node::start_with(false, None);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let gap = Duration::from_secs(1);
    let mailbox = node
        .mailbox(false)
        .with_submission_limits(SubmissionLimits {
            nonce_gap: gap,
            ..patient(64)
        });
    let kept = delivery(9);
    lose(&node, &mailbox, &kept, 1).await;
    node.freeze(true);
    // Another transaction uses nonce 1 in block B, which a reading reads
    // while B is not final.
    node.send_foreign(1).unwrap();
    node.produce_block();
    node.produce_block();
    let height = node.height();
    assert_eq!(node.model().nonce, 1);
    assert_eq!(mailbox.reclaim_kept_transactions().await.unwrap(), 0);
    // B is rolled back, the node drops that transaction, and a block that
    // does not use nonce 1 takes B's place and becomes final.
    node.rollback_to(height - 1);
    node.evict_from(1);
    node.produce_block();
    node.finalize_to(height);
    assert_eq!(node.model().nonce, 0);
    // Nonce 1 is used again at the tip, and the gap later the account
    // shows it: the kept transaction stays.
    node.send_foreign(1).unwrap();
    node.produce_block();
    node.produce_block();
    tokio::time::sleep(gap).await;
    assert_eq!(mailbox.reclaim_kept_transactions().await.unwrap(), 0);
    assert!(mailbox.prepared_transaction(kept.0.id()).unwrap().is_some());
    // That block is rolled back too, and nonce 1 is free for it again.
    node.rollback_to(height);
    assert_eq!(node.model().nonce, 0);
    std::env::remove_var("DUSK_TX_BIN");
}

/// The E2E's restart backlog: the relayer restarts while a delivery's
/// transaction waits in the mempool, and submits other ready messages
/// first. Each used to be simulated, signed at the account's nonce plus
/// one, which that transaction holds, and refused at preverification, one
/// error each. They now wait for that transaction to leave the mempool,
/// without being signed, and then go, each once.
#[tokio::test]
async fn deliveries_wait_for_the_signers_transaction_in_flight_after_a_restart() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(false);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    // The helper waits 200 ms for a block.
    node.helper_waits(10);
    let store = tempfile::tempdir().unwrap();
    let deliveries: Vec<_> = (0..4).map(delivery).collect();
    // The first delivery's transaction waits in the mempool when the
    // relayer stops.
    node.pause(true);
    let relayer = node
        .durable_mailbox(store.path(), false)
        .with_submission_limits(patient(64));
    let (message, metadata, gas_limit) = &deliveries[0];
    let error = relayer
        .process(message, metadata, *gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("to be included"), "{error}");
    drop(relayer);
    node.restart(store.path());
    // Restarted, the relayer submits the others first: they wait.
    let relayer = node
        .durable_mailbox(store.path(), false)
        .with_submission_limits(SubmissionLimits {
            receipt: Duration::from_secs(10),
            ..patient(64)
        });
    let calls = node.invocations().len();
    let others = tokio::spawn({
        let relayer = relayer.clone();
        let deliveries = deliveries.clone();
        async move {
            for (message, metadata, gas_limit) in &deliveries[1..] {
                relayer.process(message, metadata, *gas_limit).await?;
            }
            Ok::<_, ChainCommunicationError>(())
        }
    });
    tokio::time::sleep(BLOCK_TIME * 3).await;
    assert_eq!(node.invocations().len(), calls);
    node.pause(false);
    others.await.unwrap().unwrap();
    assert!(node.refusals().is_empty(), "{:?}", node.refusals());
    assert_eq!(node.propagated(), [1, 2, 3, 4]);
    node.settle(4).await;
    drop(relayer);
    node.restart(store.path());
    std::env::remove_var("DUSK_TX_BIN");
}

/// A Mailbox that submits one at a time, waiting at most `budget` for the
/// signer's transaction in flight, and the transaction of a first delivery
/// it keeps, waiting in the mempool of a node whose blocks hold nothing.
async fn wait_behind_one_in_the_mempool(
    budget: Duration,
) -> (Node, hyperlane_dusk::DuskMailbox, String) {
    let node = Node::start(false);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    node.helper_waits(10);
    let mailbox = node
        .mailbox(false)
        .with_submission_limits(SubmissionLimits {
            receipt: budget,
            ..patient(64)
        });
    node.pause(true);
    let (message, metadata, gas_limit) = delivery(0);
    let error = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("to be included"), "{error}");
    let kept = node.model().mempool[&1].id.clone();
    (node, mailbox, kept)
}

/// A delivery behind the signer's transaction in the mempool waits for it
/// `SubmissionLimits::receipt`, lookups included, and no longer: behind one
/// that stays there, it fails then, signing nothing, and goes once that one
/// has landed.
#[tokio::test]
async fn a_delivery_behind_a_transaction_that_stays_in_the_mempool_waits_its_budget_only() {
    let _env = HELPER_ENV.lock().await;
    let budget = Duration::from_secs(1);
    let (node, mailbox, _) = wait_behind_one_in_the_mempool(budget).await;
    let calls = node.invocations().len();
    let (message, metadata, gas_limit) = delivery(1);
    let started = std::time::Instant::now();
    let error = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap_err()
        .to_string();
    let waited = started.elapsed();
    assert!(
        error.contains("is still in the mempool, or could not be looked up in time"),
        "{error}"
    );
    assert!(
        waited >= budget && waited < budget + Duration::from_millis(300),
        "waited {waited:?}"
    );
    assert_eq!(node.invocations().len(), calls);
    node.pause(false);
    deliver(&mailbox, &(message, metadata, gas_limit)).await;
    node.settle(2).await;
    std::env::remove_var("DUSK_TX_BIN");
}

/// The review's case (w14): the wait's lookup of the signer's transaction
/// in flight is answered only past the wait's deadline, and says it landed.
/// The wait used to take that answer however late, and sign the delivery
/// then; behind a node that stalls, it waited as long as the node did. It
/// now ends at its deadline, signing nothing, and the delivery goes when
/// the relayer tries it again.
#[tokio::test]
async fn the_signer_wait_ends_at_its_deadline_while_a_lookup_goes_unanswered() {
    let _env = HELPER_ENV.lock().await;
    let budget = Duration::from_secs(1);
    let (node, mailbox, kept) = wait_behind_one_in_the_mempool(budget).await;
    let calls = node.invocations().len();
    let gate = std::sync::Arc::new(Gate::default());
    node.inject(Route::Graphql, None, Fault::Hold(gate.clone()));
    let (message, metadata, gas_limit) = delivery(1);
    let started = tokio::time::Instant::now();
    let waiting = tokio::spawn({
        let mailbox = mailbox.clone();
        let (message, metadata) = (message.clone(), metadata.clone());
        async move { mailbox.process(&message, &metadata, gas_limit).await }
    });
    node.wait_until("the lookup reaches the node", |_| {
        gate.waiting.load(std::sync::atomic::Ordering::SeqCst)
    })
    .await;
    // Meanwhile the transaction lands, as the held lookup would answer.
    node.pause(false);
    node.wait_until("the kept transaction is in a block", |model| {
        model.included(&kept).is_some()
    })
    .await;
    let ended =
        tokio::time::timeout_at(started + budget + Duration::from_millis(300), waiting).await;
    gate.open();
    let error = ended
        .expect("the wait outlived its deadline")
        .unwrap()
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("is still in the mempool, or could not be looked up in time"),
        "{error}"
    );
    assert_eq!(node.invocations().len(), calls);
    deliver(&mailbox, &(message, metadata, gas_limit)).await;
    node.settle(2).await;
    std::env::remove_var("DUSK_TX_BIN");
}

/// Keep a first delivery's transaction T, signed with nonce 1, waiting in
/// the mempool of a node whose blocks come, and become final, only by
/// hand, through `mailbox`, which then returns without its outcome. T's ID.
async fn keep_in_the_mempool(
    node: &Node,
    mailbox: &hyperlane_dusk::DuskMailbox,
    delivery: &(
        hyperlane_core::HyperlaneMessage,
        hyperlane_core::Metadata,
        Option<U256>,
    ),
) -> String {
    node.pause(true);
    let (message, metadata, gas_limit) = delivery;
    let error = mailbox
        .process(message, metadata, *gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("to be included"), "{error}");
    node.freeze(true);
    node.pause(false);
    let kept = node.model().mempool[&1].id.clone();
    kept
}

/// The review's race (w15). Rusk resolves a lookup's `mempoolTx` and `tx`
/// from separate reads: its mempool read finds the kept transaction T in no
/// mempool, as T is in block C, then C is rolled back, putting T back, and
/// its ledger read finds T in no block. The delivery, submitted again past
/// the drop grace, took T for dropped and sent the message again, signing
/// nonce 1 while T held it. T's other reads, one place at a time, find it
/// in the mempool: it is followed to its block, and nothing is signed.
#[tokio::test]
async fn a_rollback_between_a_lookups_two_reads_does_not_send_a_delivery_again() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start_with(false, None);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    node.helper_waits(10);
    let mailbox = node.mailbox(false).with_submission_limits(patient(64));
    let (message, metadata, gas_limit) = delivery(0);
    let kept = keep_in_the_mempool(
        &node,
        &mailbox,
        &(message.clone(), metadata.clone(), gas_limit),
    )
    .await;
    let calls = node.invocations().len();
    // Past the drop grace, and the 2 s a passed simulation stands for.
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    let simulation = std::sync::Arc::new(Gate::default());
    node.inject(
        Route::Simulation,
        None,
        Fault::HoldReply(simulation.clone()),
    );
    let fields = std::sync::Arc::new(Gate::default());
    node.inject(
        Route::Graphql,
        None,
        Fault::BetweenFields {
            ledger_first: false,
            gate: fields.clone(),
        },
    );
    let retry = tokio::spawn({
        let mailbox = mailbox.clone();
        let (message, metadata) = (message.clone(), metadata.clone());
        async move { mailbox.process(&message, &metadata, gas_limit).await }
    });
    // The simulation passes while T is in the mempool; T then lands in C.
    node.wait_until("the simulation is answered", |_| {
        simulation.waiting.load(std::sync::atomic::Ordering::SeqCst)
    })
    .await;
    let parent = node.height();
    node.produce_block();
    assert!(node.model().included(&kept).is_some());
    simulation.open();
    // The lookup's mempool read misses T; C is rolled back before its
    // ledger read.
    node.wait_until("the lookup's mempool read is made", |_| {
        fields.waiting.load(std::sync::atomic::Ordering::SeqCst)
    })
    .await;
    node.rollback_to(parent);
    fields.open();
    node.freeze(false);
    let outcome = retry.await.unwrap().unwrap();
    assert!(outcome.executed);
    assert_eq!(hex::encode(&outcome.transaction_id.as_bytes()[32..]), kept);
    assert_eq!(node.invocations().len(), calls);
    assert_eq!(node.propagated(), [1]);
    node.settle(1).await;
    std::env::remove_var("DUSK_TX_BIN");
}

/// The same race at a lookup of kept transactions for room: two readings
/// of the account, the gap apart, show nonce 1 used, and the lookup of the
/// kept transaction T misses it, as block C holding T is rolled back
/// between its two reads. T, in neither place as far as that lookup saw,
/// went, while the rollback had put it back into the mempool. Its other
/// reads find it there, and it stays.
#[tokio::test]
async fn a_rollback_between_a_lookups_two_reads_does_not_forget_a_kept_transaction() {
    let _env = HELPER_ENV.lock().await;
    let gap = Duration::from_secs(1);
    let (node, mailbox, (message, _, _)) = keep_one_in_the_mempool(gap).await;
    // A first reading shows nonce 1 used, at block P, now final.
    node.produce_block();
    assert_eq!(
        reclaim_across_a_rolled_back_block(&node, &mailbox, |_| {}).await,
        0
    );
    tokio::time::sleep(gap).await;
    // The gap later, block C holds T: the account shows nonce 1 used again.
    let parent = node.height();
    node.produce_block();
    let fields = std::sync::Arc::new(Gate::default());
    node.inject(
        Route::Graphql,
        None,
        Fault::BetweenFields {
            ledger_first: false,
            gate: fields.clone(),
        },
    );
    let reclaim = tokio::spawn({
        let mailbox = mailbox.clone();
        async move { mailbox.reclaim_kept_transactions().await }
    });
    node.wait_until("the lookup's mempool read is made", |_| {
        fields.waiting.load(std::sync::atomic::Ordering::SeqCst)
    })
    .await;
    node.rollback_to(parent);
    fields.open();
    assert_eq!(reclaim.await.unwrap().unwrap(), 0);
    assert!(mailbox
        .prepared_transaction(message.id())
        .unwrap()
        .is_some());
    assert_eq!(
        node.model().mempool.keys().copied().collect::<Vec<_>>(),
        [1]
    );
    std::env::remove_var("DUSK_TX_BIN");
}

/// A lookup whose two reads miss the transaction around a rollback looks
/// again, one place at a time, and finds it in the mempool: a delivery
/// waiting for the signer's transaction in flight goes on waiting for it,
/// signing nothing, and goes once it has landed. Taken for gone, it was
/// signed with the nonce that transaction held and refused.
#[tokio::test]
async fn a_lookup_racing_a_rollback_still_finds_the_transaction_in_the_mempool() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start_with(false, None);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    node.helper_waits(10);
    let mailbox = node.mailbox(false).with_submission_limits(patient(64));
    let kept = keep_in_the_mempool(&node, &mailbox, &delivery(0)).await;
    let calls = node.invocations().len();
    // T lands in block C, and the next delivery waits for it.
    let parent = node.height();
    node.produce_block();
    assert!(node.model().included(&kept).is_some());
    let fields = std::sync::Arc::new(Gate::default());
    node.inject(
        Route::Graphql,
        None,
        Fault::BetweenFields {
            ledger_first: false,
            gate: fields.clone(),
        },
    );
    let waiting = mailbox.clone().with_submission_limits(SubmissionLimits {
        receipt: Duration::from_secs(5),
        ..patient(64)
    });
    let next = tokio::spawn(async move {
        let (message, metadata, gas_limit) = delivery(1);
        waiting.process(&message, &metadata, gas_limit).await
    });
    node.wait_until("the lookup's mempool read is made", |_| {
        fields.waiting.load(std::sync::atomic::Ordering::SeqCst)
    })
    .await;
    node.rollback_to(parent);
    fields.open();
    tokio::time::sleep(BLOCK_TIME * 2).await;
    assert_eq!(node.invocations().len(), calls);
    node.freeze(false);
    assert!(next.await.unwrap().unwrap().executed);
    assert!(node.refusals().is_empty(), "{:?}", node.refusals());
    assert_eq!(node.propagated(), [1, 2]);
    node.settle(2).await;
    std::env::remove_var("DUSK_TX_BIN");
}

/// A lookup whose two reads miss the transaction around its block's
/// acceptance, the ledger read before and the mempool read after, looks
/// again and finds it in a block: within the drop grace, the delivery
/// submitted again reports that block's outcome, where it failed as if the
/// transaction could still be on its way.
#[tokio::test]
async fn a_lookup_racing_an_acceptance_still_finds_the_transaction_in_its_block() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start_with(false, None);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    node.helper_waits(10);
    let mailbox = node
        .mailbox(false)
        .with_submission_limits(SubmissionLimits {
            drop_grace: Duration::from_secs(60),
            ..patient(64)
        });
    let (message, metadata, gas_limit) = delivery(0);
    let kept = keep_in_the_mempool(
        &node,
        &mailbox,
        &(message.clone(), metadata.clone(), gas_limit),
    )
    .await;
    let calls = node.invocations().len();
    let fields = std::sync::Arc::new(Gate::default());
    node.inject(
        Route::Graphql,
        None,
        Fault::BetweenFields {
            ledger_first: true,
            gate: fields.clone(),
        },
    );
    let retry = tokio::spawn({
        let mailbox = mailbox.clone();
        let (message, metadata) = (message.clone(), metadata.clone());
        async move { mailbox.process(&message, &metadata, gas_limit).await }
    });
    node.wait_until("the lookup's ledger read is made", |_| {
        fields.waiting.load(std::sync::atomic::Ordering::SeqCst)
    })
    .await;
    node.produce_block();
    assert!(node.model().included(&kept).is_some());
    fields.open();
    let outcome = retry.await.unwrap().unwrap();
    assert!(outcome.executed);
    assert_eq!(hex::encode(&outcome.transaction_id.as_bytes()[32..]), kept);
    assert_eq!(node.invocations().len(), calls);
    node.freeze(false);
    node.settle(1).await;
    std::env::remove_var("DUSK_TX_BIN");
}

/// A kept transaction past the drop grace that every read of one lookup
/// misses, as when a block holding it is accepted and rolled back between
/// them, is looked up again half a block later: found in the mempool then,
/// it is followed, and nothing is signed. One the node has really dropped
/// is missed again, and its delivery goes again, only after that pause.
#[tokio::test]
async fn a_kept_transaction_missed_for_a_moment_is_not_sent_again() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(false);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    node.helper_waits(10);
    let block = Duration::from_secs(1);
    let mailbox = node.mailbox(false).with_submission_limits(patient(64));
    let (message, metadata, gas_limit) = delivery(0);
    node.pause(true);
    let error = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("to be included"), "{error}");
    let kept = node.model().mempool[&1].id.clone();
    let calls = node.invocations().len();
    let mailbox = mailbox.with_submission_limits(SubmissionLimits {
        block,
        receipt: Duration::from_millis(1_500),
        ..patient(64)
    });
    // Missed for less than half a block.
    node.hide_for(&kept, block / 4);
    assert!(mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .is_err());
    assert_eq!(
        mailbox.prepared_transaction(message.id()).unwrap(),
        Some(kept)
    );
    assert_eq!(node.invocations().len(), calls);
    // Dropped for good: the delivery goes again, after the pause.
    node.evict_from(1);
    node.pause(false);
    let started = std::time::Instant::now();
    let outcome = mailbox
        .process(&message, &metadata, gas_limit)
        .await
        .unwrap();
    assert!(started.elapsed() >= block / 2);
    assert!(outcome.executed);
    assert_eq!(node.invocations().len(), calls + 1);
    assert_eq!(node.propagated(), [1, 1]);
    node.settle(1).await;
    std::env::remove_var("DUSK_TX_BIN");
}

/// Kept transactions for room likewise: with two readings the gap apart
/// showing nonce 1 used, a kept transaction every read of one lookup
/// misses is looked up again half a block later, and found in the mempool
/// then, stays. One the node has really dropped, with nonce 1 used for
/// good, is missed again and goes, only after that pause.
#[tokio::test]
async fn a_kept_transaction_missed_for_a_moment_is_not_forgotten() {
    let _env = HELPER_ENV.lock().await;
    let gap = Duration::from_secs(1);
    let block = Duration::from_secs(1);
    let (node, mailbox, (message, _, _)) = keep_one_in_the_mempool(gap).await;
    let mailbox = mailbox.with_submission_limits(SubmissionLimits {
        nonce_gap: gap,
        block,
        ..patient(64)
    });
    let kept = node.model().mempool[&1].id.clone();
    // Two readings the gap apart show nonce 1 used, as in the review's
    // sequence, and the lookup after the second misses T.
    node.produce_block();
    assert_eq!(
        reclaim_across_a_rolled_back_block(&node, &mailbox, |_| {}).await,
        0
    );
    tokio::time::sleep(gap).await;
    node.hide_for(&kept, block / 4);
    assert_eq!(
        reclaim_across_a_rolled_back_block(&node, &mailbox, |_| {}).await,
        0
    );
    assert!(mailbox
        .prepared_transaction(message.id())
        .unwrap()
        .is_some());
    // Dropped for good, with nonce 1 used by another transaction in a
    // final block: T goes, after the pause.
    node.evict_from(1);
    node.send_foreign(1).unwrap();
    node.produce_block();
    node.produce_block();
    node.finalize_to(node.height());
    let started = std::time::Instant::now();
    assert_eq!(mailbox.reclaim_kept_transactions().await.unwrap(), 1);
    assert!(started.elapsed() >= block / 2);
    assert_eq!(mailbox.prepared_transaction(message.id()).unwrap(), None);
    std::env::remove_var("DUSK_TX_BIN");
}

/// The review's sequence (w16). A delivery's kept transaction T failed in
/// block C, which is not final, and its failure was reported. Submitted
/// again, the delivery looks T up and finds C's failed receipt; before it
/// read the last final block, C was rolled back, which put T back into the
/// mempool, and another block took C's height and became final. T counted
/// as failed in a final block: its record went and the message was signed
/// again, while T could still land and deliver it. The last final block is
/// now read before the lookup, when C's height is not final: the delivery
/// waits, signing nothing, T stays kept, and T then lands and delivers it.
#[tokio::test]
async fn a_failure_found_in_a_block_rolled_back_after_the_lookup_is_not_sent_again() {
    let _env = HELPER_ENV.lock().await;
    // Finality moves only by hand.
    let node = Node::start_with(false, None);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let mailbox = node.mailbox(false).with_submission_limits(patient(64));
    let delivery = delivery(0);
    let message = &delivery.0;
    // Another relayer's delivery lands first, so T fails in block C.
    node.pause(true);
    let first = tokio::spawn({
        let mailbox = mailbox.clone();
        let (message, metadata, gas_limit) = delivery.clone();
        async move { mailbox.process(&message, &metadata, gas_limit).await }
    });
    node.wait_until("T is in the mempool", |model| !model.mempool.is_empty())
        .await;
    node.deliver_elsewhere(&delivery);
    node.pause(false);
    assert!(!first.await.unwrap().unwrap().executed);
    let kept = mailbox.prepared_transaction(message.id()).unwrap().unwrap();
    let height = node.model().included(&kept).unwrap().height;
    // The other delivery does not count after all, and the relayer submits
    // ours again; its lookup of T answers with C's failed receipt.
    node.undeliver(&delivery);
    node.freeze(true);
    node.expire_block_pair().await;
    let calls = node.invocations().len();
    let lookup = std::sync::Arc::new(Gate::default());
    node.inject(Route::Graphql, None, Fault::HoldLookupReply(lookup.clone()));
    let retry = tokio::spawn({
        let mailbox = mailbox.clone();
        let (message, metadata, gas_limit) = delivery.clone();
        async move { mailbox.process(&message, &metadata, gas_limit).await }
    });
    node.wait_until("T is looked up", |_| {
        lookup.waiting.load(std::sync::atomic::Ordering::SeqCst)
    })
    .await;
    // Meanwhile C is rolled back, and a block without T takes its height
    // and becomes final.
    node.rollback_to(height - 1);
    node.pause(true);
    node.produce_block();
    node.finalize_to(height);
    node.expire_block_pair().await;
    assert!(node.model().included(&kept).is_none());
    lookup.open();
    let error = retry.await.unwrap().unwrap_err().to_string();
    assert!(error.contains("not final yet"), "{error}");
    assert_eq!(node.invocations().len(), calls);
    assert_eq!(
        mailbox.prepared_transaction(message.id()).unwrap(),
        Some(kept.clone())
    );
    // T lands again, and delivers the message, once.
    node.pause(false);
    node.freeze(false);
    node.settle(1).await;
    assert!(node.model().included(&kept).unwrap().error.is_none());
    std::env::remove_var("DUSK_TX_BIN");
}

/// A look at kept transactions for room ends within the receipt wait.
/// Behind a node that does not answer it, the delivery at the bound fails
/// as usual once the wait is out, signing nothing and keeping every
/// record. It used to wait as long as the node did, and every later
/// delivery to Dusk with it.
#[tokio::test]
async fn a_look_at_kept_transactions_for_room_ends_within_the_receipt_wait() {
    let _env = HELPER_ENV.lock().await;
    let node = Node::start(false);
    std::env::set_var("DUSK_TX_BIN", node.helper());
    let budget = Duration::from_secs(1);
    let mailbox = node
        .mailbox(false)
        .with_submission_limits(SubmissionLimits {
            prepared: 1,
            receipt: budget,
            ..patient(64)
        });
    let lost = delivery(9);
    lose(&node, &mailbox, &lost, 1).await;
    let calls = node.invocations().len();
    // The look's read of the account is answered only once the gate opens.
    let account = std::sync::Arc::new(Gate::default());
    node.inject(Route::AccountStatus, None, Fault::Hold(account.clone()));
    let (message, metadata, gas_limit) = delivery(0);
    let started = std::time::Instant::now();
    let result =
        tokio::time::timeout(budget * 3, mailbox.process(&message, &metadata, gas_limit)).await;
    let took = started.elapsed();
    account.open();
    let error = result
        .expect("the look for room outlived the receipt wait")
        .unwrap_err()
        .to_string();
    assert!(error.contains("the most it keeps"), "{error}");
    assert!(took < budget * 2, "took {took:?}");
    assert_eq!(node.invocations().len(), calls);
    assert!(mailbox.prepared_transaction(lost.0.id()).unwrap().is_some());
    std::env::remove_var("DUSK_TX_BIN");
}
