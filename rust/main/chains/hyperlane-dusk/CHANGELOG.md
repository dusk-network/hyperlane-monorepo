# Changelog

## Unreleased

### Fixed

- Checkpoint reads honored configured numeric block delays while remaining bounded by consensus finality ([Dusk invariant review]).
- Checkpoint reads rejected unsupported block tags instead of silently treating them as finalized ([Dusk invariant review]).

[Dusk invariant review]: https://github.com/dusk-network/hyperlane-dusk/pull/11
