# Changelog

## Unreleased

### Fixed

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
