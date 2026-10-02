# Changelog

## Unreleased

### Fixed

- Included failed transactions returned their receipts for gas expenditure accounting ([Dusk invariant review]).
- Checkpoint endpoint identity checks deferred to reads so unavailable quorum members could recover without blocking startup ([Dusk invariant review]).
- Exhausted archive cursor hints reset so repaired endpoints recovered without an agent restart ([Dusk invariant review]).

- Finalized event indexing matched contract state independently of transaction-hash order within a block ([Dusk invariant review]).
- Checkpoint reads honored configured numeric block delays while remaining bounded by consensus finality ([Dusk invariant review]).
- Checkpoint reads rejected unsupported block tags instead of silently treating them as finalized ([Dusk invariant review]).

[Dusk invariant review]: https://github.com/dusk-network/hyperlane-dusk/pull/11
