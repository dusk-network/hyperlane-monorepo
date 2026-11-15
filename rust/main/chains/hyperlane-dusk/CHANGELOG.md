# Changelog

## Unreleased

### Changed

- The default Dusk `gasLimit` was 5,000,000 instead of 500,000,000. A failed transaction pays its whole limit, and a relayed `process` costs about 1.95M gas plus 0.22M per ISM signature; the deploy scripts already set 5,000,000 for the relayer.
- Dispatch indexing read each message from its finalized `Dispatch` event, matched by the message ID the Mailbox stores (`dispatched_id`), because Mailbox state version 4 no longer stores dispatched messages.

- Finalized-event indexing matched every record of a block in one pass over that block's archive rows, 200 rows per `finalizedEvents` page (Rusk's maximum), instead of rescanning the block from its first row for each record. A block of 256 dispatches took 3 pages instead of 4,224.
- Archive scan positions were kept in `eventCursorDir`, about one per 200 rows, so a restart resumed near the block it needed instead of at the contract's first row. A position was reused only after the archive returned the row it was taken from, unchanged.
- Startup refused a Mailbox below `state_version` 4 with an explicit error, instead of every dispatch fetch failing later with an unexplained HTTP 500.
- Checkpoint roots and the validator's start tree were computed from the message IDs the MerkleTreeHook stores, instead of read with `root_at`, which hook state version 2 no longer has. The tree is kept between checkpoints and extended with new IDs only, read in pages of 256 whose length is checked. Its last ID is checked against `message_id_at`, and whenever the hook holds exactly its leaves, its count, root and the branch nodes the count's set bits select are checked against the hook's own `tree`. The other branch nodes are not compared: the hook starts them at zero, hyperlane-core at the empty subtrees' hashes. A tree that fails a check is dropped and the read fails.
- Startup required Mailbox `state_version` 5, MerkleTreeHook 2 and ValidatorAnnounce 2, the versions this agent reads, and refused any other, newer ones included. It used to accept any Mailbox from version 4 on, so an agent started against the redesigned contracts and then failed on queries they no longer have.

### Fixed

- A finalized block whose archive rows a Rusk node never wrote, which happens when its archive write fails, left the dispatch, Merkle hook and IGP indexers waiting for good, reporting the records as not archived yet. When contract state records events in such a block and the archive has finalized a later one, the lookup now fails with an error naming the block, its hash, the contract and the records, and the agent logs it as a critical error, at most every 10 minutes per block. Records are still accepted only with their rows.
- A block's archive rows were kept in memory without checking each row's block hash and transaction hash, and rows with data that is not hex were left out while the block still counted as read whole. A lookup of a row that did not parse then failed from memory on every retry, and a transaction whose row was left out was reported to have emitted nothing: the Mailbox and Merkle hook indexers returned no logs for it and the IGP indexer returned early. A block with such a row is now never kept; a lookup by transaction checks the block with `checkBlock` from memory as from the archive, and when the transaction's rows name no record, contract state decides whether the block holds its records. A cached block whose selected row fails to parse is dropped.
- A saved archive scan position was kept whenever the archive returned no row from it and had not finalized its row's block, which also happens when the position names another hash for that block (a row served under a forged block hash), when the node's ledger lacks the block, or when the archive has passed the block without finalizing it. Lookups from such a position failed for good. A position is now kept only while the ledger holds its block under its hash and the node has not finalized that block yet or the archive has not finalized the node's last final block either; otherwise it is dropped and the scan starts over.
- A relayed `process` was refused once its metadata and message passed 60,416 bytes, 1 KiB short of the 61,440 bytes of serialized arguments the `dusk-tx` helper accepts. The arguments are now serialized first and refused exactly when the helper would refuse them.
- A Dusk delivery counted only once its block was final, whoever delivered the message. The relayer used to mark a message processed as soon as the node's tip held its delivery, and never retried it: if finality stalled past the relayer's confirmation delay and a rollback dropped the delivery, the message was lost. While no final block held the delivery, `delivered` failed and the relayer checked again later; a rolled-back delivery read as not delivered, and the message was relayed again.
- A validator's startup waited until its announcement's block was final, instead of ending as soon as the node's tip held an announcement that a rollback could still drop.
- Checkpoint quorums counted only signatures over the same checkpoint, and message-ID multisig metadata only signatures over the message's own ID. A validator that signed the right root and index with another message ID used to join the honest signatures' group, which then failed to form one quorum and aborted the whole fetch: a validator that answered first could hold back delivery for good.
- A lookup read its whole block and kept the block's rows in memory, so other lookups in the block matched against them instead of reading the archive again. Progress through a block was saved as a position after the block, which another part of the same block could not use: with `index.chunk` 1, a cursor over 1,000 Merkle insertions in reverse archive order read 3,000 archive pages, and the Mailbox's 2,000 rows 5,500. Each now reads one pass, 5 and 10 pages. The cache keeps only blocks whose rows carry the hash `checkBlock(onlyFinalized)` accepted, at most 64 blocks and 64 MiB, and 32 MiB for one block.
- A transaction lookup by the Merkle hook, Mailbox and IGP indexers looked up only the records that the transaction's own archive rows name, instead of every record of its block. Contract sync looks up each dispatch's transaction, so a block of 1,000 dispatches cost the hook and Mailbox indexers about 2 million state reads each; it now costs 12,000.
- Parse errors were cut to 1 KiB before they went into errors and log lines, like the response bodies they come from. A parse error quotes the value it fails on, so a `finalizedEvents` page, account status or gas price reply with a 1 MiB string in a numeric field put the whole string into an error that contract sync logged on every retry. The `dusk-tx` helper's stdout, stderr and error text, up to 1 MiB each, are cut the same way.
- A saved archive scan position was reused only when its row is below the block the position names. A position that contradicts its own row was dropped and the scan started over, instead of resuming after that block's first rows and missing them on every retry.
- When the archive returned no row from a saved scan position's row on, and had not finalized that row's block yet, the lookup failed until the archive caught up and kept the position, instead of dropping the contract's positions and rescanning from its first row. An archive that had finalized the block without the row, or returned later rows without it, numbers its rows differently, and the scan still started over. Positions saved by the previous version lack the row's block, are dropped once, and the next scan of each contract starts from its first row.
- A `finalizedEvents` page over the agent's 4 MiB response limit was asked for again at the same position with half the rows, down to one, instead of failing at that position on every retry. 200 Mailbox rows exceed the limit once dispatched message bodies average more than about 20 KiB, and dispatch and delivery indexing stopped for good at the first such page.
- A lookup that ran ahead of the archive, between the node finalizing a block and the archive inserting its rows, kept its scan position instead of sending the next scan back to the contract's first row.
- The `process` simulation passed only on the encoding of the `()` that `process` returns. Any other success reply, such as a proxy's error page sent with status 200, failed the simulation instead of letting the transaction burn its gas limit.
- Response bodies were cut to 1 KiB before they went into errors or logs, instead of being copied in whole, up to 4 MiB.

- The relayer ran `process` as an unsigned contract query before submitting it and retried later when the query failed, instead of paying the full gas limit for a delivery that reverts.

- Startup retries were limited to Dusk identity observations; local event-store and signer errors returned promptly ([Dusk invariant review]).

- Validator indexer and metrics-provider startup recovered from unavailable identity reads before announcement ([Dusk invariant review]).

- Announcement-client construction retried failed identity observations before validator startup continued ([Dusk invariant review]).

- Prepared transaction IDs remained authoritative when helper diagnostics or results disagreed ([Dusk invariant review]).
- Equivalent Dusk request URLs counted as one validator checkpoint endpoint ([Dusk invariant review]).
- Self-announcement required a successful state read before submitting a transaction ([Dusk invariant review]).

- Interrupted helper calls retained their prepared transaction identity for exact-hash receipt reconciliation ([Dusk invariant review]).
- RUES routes preserved RPC base-path prefixes and query parameters ([Dusk invariant review]).
- Deployment checks bound ValidatorAnnounce and the required Merkle-hook topology to the configured Mailbox ([Dusk invariant review]).
- Transient block-check failures retained durable event rows for revalidation ([Dusk invariant review]).
- Agent helper calls carried authenticated RPC URLs over private stdin ([Dusk invariant review]).
- Unsupported multi-endpoint provider modes failed configuration instead of dropping endpoints ([Dusk invariant review]).

- Included failed transactions returned their receipts for gas expenditure accounting ([Dusk invariant review]).
- Checkpoint endpoint identity checks deferred to reads so unavailable quorum members could recover without blocking startup ([Dusk invariant review]).
- Exhausted archive cursor hints reset so repaired endpoints recovered without an agent restart ([Dusk invariant review]).

- Finalized event indexing matched contract state independently of transaction-hash order within a block ([Dusk invariant review]).
- Checkpoint reads honored configured numeric block delays while remaining bounded by consensus finality ([Dusk invariant review]).
- Checkpoint reads rejected unsupported block tags instead of silently treating them as finalized ([Dusk invariant review]).

[Dusk invariant review]: https://github.com/dusk-network/hyperlane-dusk/pull/11
