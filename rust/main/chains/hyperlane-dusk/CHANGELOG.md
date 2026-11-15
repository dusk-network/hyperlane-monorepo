# Changelog

## Unreleased

### Changed

- The default Dusk `gasLimit` was 5,000,000 instead of 500,000,000. A failed transaction pays its whole limit, and a relayed `process` costs about 1.95M gas plus 0.22M per ISM signature; the deploy scripts already set 5,000,000 for the relayer.
- Dispatch indexing read each message from its finalized `Dispatch` event, matched by the message ID the Mailbox stores (`dispatched_id`), because Mailbox state version 4 no longer stores dispatched messages.

- Finalized-event indexing matched every record of a block in one pass over that block's archive rows, 200 rows per `finalizedEvents` page (Rusk's maximum), instead of rescanning the block from its first row for each record. A block of 256 dispatches took 3 pages instead of 4,224.
- Archive scan positions were kept in `eventCursorDir`, about one per 200 rows, so a restart resumed near the block it needed instead of at the contract's first row. A position was reused only after the archive returned the row it was taken from, unchanged.
- Startup refused a Mailbox below `state_version` 4 with an explicit error, instead of every dispatch fetch failing later with an unexplained HTTP 500.

### Fixed

- A lookup read its whole block and kept the block's rows in memory, so other lookups in the block matched against them instead of reading the archive again. Progress through a block was saved as a position after the block, which another part of the same block could not use: with `index.chunk` 1, a cursor over 1,000 Merkle insertions in reverse archive order read 3,000 archive pages, and the Mailbox's 2,000 rows 5,500. Each now reads one pass, 5 and 10 pages. The cache keeps only blocks whose rows carry the hash `checkBlock(onlyFinalized)` accepted, at most 64 blocks and 64 MiB, and 32 MiB for one block.
- A transaction lookup by the Merkle hook, Mailbox and IGP indexers looked up only the records that the transaction's own archive rows name, instead of every record of its block. Contract sync looks up each dispatch's transaction, so a block of 1,000 dispatches cost the hook and Mailbox indexers about 2 million state reads each; it now costs 12,000.
- Parse errors were cut to 1 KiB before they went into errors and log lines, like the response bodies they come from. A parse error quotes the value it fails on, so a `finalizedEvents` page, account status or gas price reply with a 1 MiB string in a numeric field put the whole string into an error that contract sync logged on every retry. The `dusk-tx` helper's stdout, stderr and error text, up to 1 MiB each, are cut the same way.
- A saved archive scan position was reused only when its row is below the block the position names. A position that contradicts its own row was dropped and the scan started over, instead of resuming after that block's first rows and missing them on every retry.
- When the row a saved scan position was taken from was missing, and the archive had not finalized that row's block yet, the lookup failed until the archive caught up and kept the position, instead of dropping the contract's positions and rescanning from its first row. An archive that had finalized the block without the row numbers its rows differently, and the scan still started over. Positions saved by the previous version lack the row's block, are dropped once, and the next scan of each contract starts from its first row.
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
