# hyperlane-dusk

Hyperlane agent support for Dusk. It implements the chain traits (Mailbox,
MerkleTreeHook, InterchainSecurityModule, MultisigIsm, ValidatorAnnounce,
InterchainGasPaymaster, provider and indexers) over a Rusk node's HTTP API, so
the relayer, validator and scraper can run with Dusk as an origin or
destination.

The Dusk contracts, deployment tooling and end-to-end tests live in
[`dusk-network/hyperlane-dusk`](https://github.com/dusk-network/hyperlane-dusk).
That repository's README covers how the contracts differ from EVM Hyperlane and
how to run the local bridge.

## How it works

- **Reads** go to Rusk contract queries (`POST /on/contracts:<id>/<method>`)
  with rkyv-encoded arguments, and to Rusk's GraphQL endpoint for blocks,
  transactions and finalized events. Events come from the archive, so the
  node must be built with the `archive` feature. Response bodies, the parse
  errors that quote them and the `dusk-tx` helper's output are cut to 1 KiB
  before they appear in errors or logs.
- **Pacing.** Rusk 1.7.3 allows all of a node's clients together 180
  contract queries a second, 360 in any 2 s and 12 at once, and answers the
  rest `429 too_many_requests` with `Retry-After: 1`. Every client of one
  endpoint in an agent process shares one pace for contract queries:
  `contractQueriesPerSecond` (100 by default) and `contractQueryBurst` (20),
  with at most 8 in flight, which leaves room for the node's other clients.
  A query takes one of the 8 in-flight slots before its place in the pace,
  so the queries that wait behind slow ones go out at the pace once those
  finish, not all at once.
  A request the node refuses for its rate limit, on any route, holds back
  every request of its class from that process for the `Retry-After` plus up
  to as long again at random, and is sent again after a wait that doubles
  with each refusal in a row, also at random within it, so that indexers do
  not come back together; after 4 refusals in a row (15 to 30 s) it fails.
  A request checks for such a hold just before it goes out, after any wait
  for its pace or an in-flight slot.
  The node refuses a request before it runs it, so sending it again is safe.
- **Finality is read once per tick.** Finalized data changes only when the
  node's last final block does. Every client of an endpoint in a process
  shares one `lastBlockPair` read per 5 s tick, and what the agent computes
  from final data is kept for the last final block it was computed at: the
  indexers' counts and tips, checkpoints, `delivered` and the announcement
  checks. Until the node finalizes another block they cost no request; the
  relayer's `delivered` checks every half second while it waits for
  signatures are answered from the last one. A delivery or announcement
  check that its event log could not settle, because the archive failed or
  the log does not reach the last final block yet, is kept for at most 1 s,
  and only while the log has not read on: the next check tries the archive
  again, or reads on in it, also while the last final block stays. When the
  node does, each
  record indexer reads its contract's count once, and the heights of only
  the records past the last final one it knows, from there (see
  `final_records.rs`); a validator's indexer and checkpoint reader share
  that read. The delivery log reads the archive again only then, from where
  it stopped, and a checkpoint whose leaf count has not changed is not
  checked against the hook again. An idle relayer sends one request a tick
  plus about 6 per newly final block, and a validator one a tick plus one,
  whatever the history (48 and 18 a minute on a 10 s chain). Finality is
  seen up to one tick later than a fresh read would see it.
- **Record heights are kept.** A record's height read at or below a final
  block is final, so each contract keeps the heights its searches read, of
  the 4,096 most recent blocks with records they read. A search for an
  earlier block, such as the block of a transaction that
  `txIdIndexingEnabled` looks up for each new message's IGP payment and
  Merkle insertion, looks only between the closest kept records on either
  side of it, and reads only records no search has read. A message's
  lookups read no record height once the indexer has read its block,
  whatever the history: the IGP's used to read every payment's height
  again, about 100 a Dusk to EVM message after 100 payments and 295 after
  10,000 through the multicall, and 8,100 for a restart with 40 of 100
  messages waiting, which now reads each payment's height once. A search
  whose reads contradict each other, the kept heights or the last one
  searched is not used: the kept heights are forgotten and every record's
  height is read again, and when those contradict each other too, the
  search fails instead of answering.
- **Batched reads.** A chain whose `batchContractAddress` names Multicall
  for Dusk sends reads that do not depend on each other as one `aggregate`
  query, which answers them all at one state; an aggregate is one request to
  the pace and to the node's rate limit. Before its first aggregate, a
  process checks that the multicall answers `version` with 1 and an empty
  `aggregate` with no results. One that does not, or later answers wrongly,
  is set aside with one warning, every read is one query again, and it is
  probed again 10 minutes later. Without a `batchContractAddress` every read
  is one query, as before. Through the multicall:
  - the deployment check reads its ten views in one aggregate, and an
    aggregation hook's three in one more, after the probe's two queries;
  - the reads made once per final block, the record counts and the
    delivery checks of messages the relayer is waiting on, go in one
    aggregate per final block, which also carries those asked at the two
    blocks before: an idle relayer sends 30 requests a minute instead of 48.
    A validator reads one count per final block, which gains nothing (18).
    An endpoint keeps the replies of at most 1,024 such reads, the least
    recently asked going first, and 1 MiB of them, also while finality
    stalls and new messages are checked at the same final block;
  - indexers read the heights, IDs and pages of a range of records together,
    and search record heights 256 at a time: a restart's catch-up of 1,000
    records sends 25 contract queries instead of 3,048, and of 10,000
    records 151 instead of 30,132;
  - a checkpoint's checks against the hook are one aggregate at one state
    (see Checkpoints);
  - messages prepared together read their recipient's ISM and its
    `validators_and_threshold` once, and reads asked while an aggregate is
    in flight, such as the first delivery checks of a batch of messages, go
    together in the next one.

  Each aggregate keeps within the multicall's 256 calls and 64 KiB of
  argument and of reply by the longest reply each view gives, even if every
  call fails with the longest panic message. One that fails as a whole is
  split in two and each half sent again, and a half of one read goes as a
  single query; when both halves fail, the reads go one query each. Rusk
  1.7.3 answers every failed query with `500 Internal server error`, so a
  reply past 64 KiB cannot be told from other failures. A call that fails
  gives the error its single query would, `500`, here with the reason; one
  that runs out of its share of gas is sent again as a single query, with
  the query's whole gas. A call can change what the later calls of its query
  read, so reads of contracts outside the deployment, such as an ISM a
  recipient chose, go after the deployment's own, and their failures never
  set the multicall aside. Every view the agent batches answers its caller
  alike, so its reply in an aggregate is the bytes a single query returns.
- **Indexing** uses finalized contract events only. Each event is matched to
  its block and transaction, and stored in the agent's `eventCursorDir`, which
  every agent process needs its own copy of. The Mailbox stores only the ID of
  each dispatched message (`dispatched_id`); the message itself comes from the
  finalized `Dispatch` event whose message hashes to that ID.
- **Event logs.** The Mailbox keeps no list of deliveries, so they come from
  its finalized `ProcessId` events alone: the agent keeps a log of them in
  `eventCursorDir`, every non-reverted row the archive has finalized, in
  block order and within a block in archive order (by transaction hash),
  numbered from zero. A delivery's sequence is its number there, the same for
  any endpoint whose archive holds the same blocks, but not its order of
  execution within a block. The log takes a block once it has read all of
  its rows, they all name one block hash, the block is at or below the
  node's last final block and `checkBlock(onlyFinalized)` accepted it, also
  when the block has no row of the topic, since the log then holds that it
  has none; a row that does not parse stops it before its block, and a row
  more than 64 blocks above the node's tip fails the read. It reads on from
  the closest saved scan position below the first block it lacks, at most 16
  pages per indexer call and 4 per delivery check, even inside a block: a
  block not read whole by then is staged in `eventCursorDir` with its rows of
  the topic and the position after the last row read, and the next call
  reads on from there. A block of more than 1,000,000 rows of the contract,
  or 32 MiB of rows of the topic, which no Rusk block holds, stops the log
  with an error. The log never numbers rows of a block it already holds
  again, so its numbering survives restarts and archives that only number
  their rows differently. An archive's history can change all the same: rows
  can come out of block order, and a rebuilt archive can hold rows of a block
  the old one skipped, or lack rows it had. Every block the log holds that a
  pass reads again from its first row, as it does from the contract's first
  row once the archive numbers its rows differently, must match the log's
  rows of it, and every block the log holds rows of must come again before a
  later block does, or before the archive's end when the archive had
  finalized the node's last final block before the read. A row below a
  block read before it, or a block that
  differs, drops the log's rows from that block on, and the log reads them
  again; a block that `checkBlock` no longer accepts when a delivery is
  served drops them all. The delivery indexer marks as published every
  delivery below a count it reports to contract sync, and every delivery it
  serves; a delivery log the previous version wrote, which records none,
  counts every delivery it holds as published. The log keeps the dropped
  rows of published deliveries, and where
  the rows it reads again would give one of those sequences another
  delivery, it halts (see [Event log halts](#event-log-halts)), as it does
  once it holds every finalized delivery up to the block of one of them
  without having taken it again. The delivery
  indexer's tip is the height up to which the log holds every finalized
  delivery.
- **Announced locations.** ValidatorAnnounce keeps only the keccak256 of
  each location a validator announced, oldest first
  (`get_announced_storage_location_hashes`), and the locations are in its
  `ValidatorAnnouncement` events. A relayer keeps a log of the finalized
  announcements like the deliveries' (at most 16 pages per read) and returns,
  for each validator, the logged locations whose hash the contract lists for
  it, in the list's order, so the last is the latest. A listed hash whose
  announcement it has not read yet, such as one that is not final, is left
  out until it has. A validator's startup check has announced its location
  iff the location's hash is in its own list, and reports it only once the
  announcement is final (see below); otherwise the validator announces.
- **Archive scans** read a contract's `finalizedEvents` 200 rows at a time
  (Rusk's maximum) and match every record of a block in one pass over that
  block's rows. The agent reads at most 4 MiB of a reply, and 200 rows of
  large dispatches are more than that, so a page over the limit is asked for
  again at the same position with half the rows, down to one. A Rusk event
  carries at most 64 KiB of data, so a single row always fits; one that does
  not is reported as an endpoint fault. Each scan starts from a saved
  position just before the block, kept in `eventCursorDir` (about one per
  200 rows), so a restart or an archive that lags behind finality does not
  send the scan back to the contract's first row. Before reusing a position,
  the scan checks that the archive still has the row it was taken from,
  unchanged and below the block the position names. Rusk numbers rows in
  insertion order and never deletes one, so if the archive returns another
  row first, it numbers its rows differently: the contract's positions are
  dropped and the scan starts over. If it returns no row from there on, the
  archive may only be behind, and the positions are kept, with the lookup
  failing until it catches up, only on evidence of that: the archive has not
  finalized that row's block (`checkBlock(onlyFinalized)`, which Rusk
  records together with the block's rows), the node's ledger holds that
  block under the position's hash, and the node has not finalized the block
  yet or the archive has not finalized the node's last final block either.
  Otherwise the archive will never return the row, and the scan starts over.
- **Missing archive rows.** When contract state records an event in a
  finalized block and the archive has passed that block without a matching
  row, the archive will not add it: Rusk archives a block's rows in one
  transaction and blocks in height order, and a node whose archive write
  fails skips the block for good. The archive has passed the block once
  `checkBlock(onlyFinalized)` accepts a later block holding a row of the
  contract, or else the node's last final block, at or above this one,
  which covers a contract that has emitted nothing since. The lookup then
  fails with an error naming the block, its hash, the contract and the
  records, and the agent logs it as a critical error at most every 10
  minutes per block. Its node's archive has to be repaired or rebuilt, or
  another endpoint used; the agent never accepts a record without its row.
- **Block cache.** A scan reads the whole block it is looking in, and the
  block's rows stay in memory, so other lookups in the same block (a cursor
  with a small `index.chunk`, or one lookup per transaction) match against
  them instead of reading the archive again. Every lookup still checks
  contract state and `checkBlock` itself, from memory or not. Only blocks
  whose rows carry the hash `checkBlock(onlyFinalized)` accepted are kept,
  at most 64 blocks and 64 MiB, and 32 MiB for one block; a larger block is
  read per lookup. A block with a row of the topic whose block hash,
  transaction hash or data does not parse, which Rusk never writes, is not
  kept either. The Merkle hook, dispatch and IGP indexers look a transaction
  up by the records its own rows name, instead of every record of its block;
  when its rows name none, or the block cannot be read whole, contract state
  decides which records of the block are the transaction's.
- **Writes** are Moonlight transactions built and signed by the `dusk-tx`
  helper from the companion repository. The agent runs it as a subprocess
  (`DUSK_TX_BIN`, or `dusk-tx` on `PATH`) and passes the RPC URL and signing
  key on stdin. Before retrying an uncertain submission, the agent reconciles
  it against the transaction hash it prepared. A delivery is submitted with
  the gas limit the relayer's gas policy asks for, which follows the gas the
  sender paid for, but at most `gasLimit` and Rusk's block gas limit of
  3,000,000,000: the mempool keeps a transaction over the block gas limit
  without any block taking it, and a failed transaction pays its whole limit.
- **Deliveries go one at a time by default.** The helper signs each delivery
  with the account's nonce plus one, as the node reports it, sends it and
  returns once it is in a block; the next delivery goes after that. The
  agent chooses no nonce. The node refuses a transaction whose nonce its
  mempool holds for another one at preverification, before propagation, so
  at most one transaction of a nonce can be in a block. When the helper's
  outcome is unknown, the agent follows the transaction it prepared by its
  exact hash. A relayer key delivers about one message every two blocks this
  way: the next one reaches the node after the next block's round has
  started. Before a delivery is simulated and signed, it waits for the
  transaction kept for the signer's latest delivery of another message to
  leave the mempool, for at most the 120 s receipt wait, lookups included,
  signing nothing meanwhile (a lookup the node has not answered by then is
  given up, and the delivery fails, to be tried again): that transaction
  holds the nonce the helper would sign with, and a relayer restarted while
  one waited there used to sign every other ready message with it, each
  refused at preverification with an error. Only that transaction is looked
  up, and only until its outcome is known.
- **A delivery's transaction is kept before it is sent, until the delivery
  is done.** The helper signs the transaction, prints its hash, and waits
  for the agent's word before it preverifies and propagates it
  (`call --ack-before-send`). The agent keeps that hash for the message in
  `eventCursorDir` (in memory without one), and only once it is kept writes
  `send` to the helper's stdin. A hash it cannot keep leaves the helper
  without the word: the helper sends nothing and says `Not submitted:`, and
  the delivery fails and goes again at once, since nothing was kept. So does
  a delivery whose relayer stops or dies before the word: the helper's stdin
  closes with it. The relayer needs a `dusk-tx` whose `call --help` lists
  `--ack-before-send`, which it asks once per process; with another, it
  refuses to deliver and says which `dusk-tx` it needs: one built from
  `dusk-network/hyperlane-dusk` at `de7e2a6` or later. The helper also names
  the nonce it signed with (`Prepared TX <hash>; nonce <N>; ...`, from
  `dusk-network/hyperlane-dusk` `3c3ce7f`), which the agent keeps with the
  hash; a `dusk-tx` from before names none. The hash stays until the relayer
  confirms the message delivered, or until it can no longer land and is not
  owed a report: in a final block, executed or failed with its failure
  reported; or in neither a block nor the node's mempool, signed with a
  nonce two readings of the signer's account, at least 10 minutes apart,
  both show used, so that it can never execute (but see the known issue of
  kept transactions below). Rusk reads an account at its tip only, so a
  reading is the account between two reads of the tip that agree; the first
  counts once that tip block is final and still the node's block at its
  height, and the second is the account read again, at least 10 minutes
  after the first and after its block is final, right before the transaction
  is looked up. One in the mempool always stays, whatever the readings show:
  a rollback puts a transaction back there, and it can land again. A lookup
  finds a transaction in neither place only when every read of it misses
  it: Rusk answers the query that asks for both from two separate reads, so
  a transaction that query misses is read again, in the ledger, the mempool
  and the ledger, one after the other, and one that every read misses is
  looked up again half a block (5 s) later, and goes only if it is missed
  again. A kept transaction without its nonce goes only once it is in a
  final block. A signer keeps those of at most 1,024 deliveries: at the
  bound, a delivery looks kept transactions up, the oldest first and on
  later tries on from where the last look stopped, at most 16 new ones at
  a time, plus second looks at up to 16 earlier misses before them and
  again at the end (at most 48 lookups in all), and forgets those it can,
  within the 120 s receipt wait: a look that runs out keeps what it found,
  the transactions it missed included, and the next looks those up again
  before anything else. Every 5 minutes a
  delivery does the same for the kept transactions of other messages, so
  that those of messages the
  relayer gave up on do not pile up to the bound; it reads nothing while
  nothing else is kept. A transaction whose nonce is used goes 10 minutes
  after the first reading that shows it, at the earliest; a reading taken
  shortly before, which does not show it, keeps the next one from being
  taken until it is 10 minutes old itself, so it can take about 20 minutes,
  plus the wait for the next look: a delivery's next try at the bound, or
  the next 5-minute sweep. One that may still land, in the mempool, in a
  block that is not final, or in neither with its nonce not yet shown used
  by both readings, is never forgotten, and neither is one that failed in a
  final block before its failure was reported, whose gas would then never be
  charged: while none can go, deliveries fail without being signed, and the
  relayer tries them again once kept transactions settle. A delivery
  submitted again looks its transaction up first: in a block and executed,
  it is the delivery's outcome, and the relayer's confirmation goes on from
  it; failed in a block, and not reported yet, as when it failed after the
  submission that prepared it had returned, that failure is the delivery's
  outcome, with the gas it spent, so that the relayer charges it to the
  message and its gas payment policy decides on another attempt, and nothing
  else is sent then; failed and reported, the delivery goes again once that
  block is final, by the last final block read before the lookup, as a
  block rolled back after it can have another, final, take its height; in
  the mempool, it is followed to its block; in neither, the delivery goes
  again only 60 s after the transaction was prepared, as
  it may still be on its way to the node, and only if a lookup half a block
  later misses it too. A transaction the node refused at preverification is
  not kept. Otherwise, after a submission whose outcome was unknown, the
  first transaction could land after the relayer's simulation of the next
  submission passed and before the helper read the account's nonce: the
  helper signed the next nonce, and a second transaction landed and failed,
  paying its whole gas limit.
- **Known issue of deliveries one at a time: a dropped transaction can land
  after its message went again.** A kept transaction that is in neither the
  node's mempool nor a block 60 s after it was prepared, as two lookups half
  a block apart find, is forgotten, and the next submission has `dusk-tx`
  sign the message again with the account's nonce plus one. A signed
  Moonlight transaction does not expire, though, so this can cost one failed
  transaction when all of these happen:
  1. a delivery's transaction M, with nonce n, reached the network while
     another transaction of the relayer's key, A with nonce n−1, was in a
     block that was not final yet;
  2. that block was rolled back: A went back into the mempool, ahead of M,
     whose outcome the relayer did not know, and the account's nonce went
     back below n−1;
  3. the relayer's node then dropped A, and M with it, as Rusk drops an
     account's later nonces with the one it evicts, while another node
     kept M;
  4. M's message was submitted again more than 60 s after M was prepared:
     M is in neither the node's mempool nor a block, so the relayer forgets
     it, and `dusk-tx` signs the message again with nonce n−1, which lands
     and delivers it.

  The node that kept M can then put it in a block, or hand it back to the
  relayer's node: nonce n is now the account's next, so M lands and fails,
  the message being delivered, and pays its whole gas limit (the delivery's,
  at most `gasLimit`), which no outcome reports to the relayer. The
  recipient cannot run twice: the Mailbox delivers a message once. Signing
  the message again with M's nonce instead, which the agent now keeps, would
  need a transaction for every nonce below it that nothing else fills, or
  the signer would wait on that gap for good. The fix, needed before
  mainnet: keep each delivery's nonce and signed transaction until a
  transaction with that nonce is in a final block, and send that same
  transaction again when it was dropped, filling any free nonce below it
  (the pipelined path's reservations, once their own known issues are
  fixed), or a Rusk transaction expiry that lets a dropped transaction be
  waited out for good.
- **Known issue of kept transactions: forgetting one on its nonce rests on
  two readings of the account.** Rusk serves an account at its tip only,
  and does not say which block it read it at, so a reading pairs the
  account with the tips read before and after it. A block accepted and
  rolled back between those two reads shows in the account read between
  them: with the tip at P, a block C holding the kept transaction T at
  nonce n comes, the account read shows n, C is rolled back, which puts T
  back into the mempool, and the tip is P again, so the reading takes n for
  P's. One such reading used to be enough, and T's record went while T could
  still land. T now goes on its nonce only when two readings at least 10
  minutes apart, the first at a block that is final and still the node's,
  both show n used, and the lookups after them miss T in both the node's
  mempool and its blocks. Its record can then go while T can still land only
  when all of these happen:
  1. both readings, 10 minutes or more apart, are wrong: each is taken
     while the node's tip is a block that uses n and is rolled back later;
  2. every read of T after the second reading misses it: the lookup's
     query, which Rusk answers from two separate reads, then reads of the
     ledger, the mempool and the ledger, one after the other, and all of
     that again half a block later. In Rusk 1.7.3 a rollback deletes a
     block and puts its transactions back into the mempool in one database
     commit, and an acceptance stores a block before a later commit takes
     its transactions out of the mempool, so a transaction is never missing
     from both for a single read (checked in Rusk 1.7.3's source; a Rusk
     that moved it in two commits would leave a moment for that). Reads
     like these then miss a T that stays on the node only if, both times, a
     block holding it is accepted, and its transactions taken out of the
     mempool, after the first ledger read and rolled back before the
     second; otherwise T is gone from the node: it dropped T, say, while a
     peer kept it.

  The agent also assumes its RUES URL is one node, or nodes that agree: a
  reading pairs the tip one request returns with the account another
  returns, and a URL in front of several nodes can answer them from
  different ones, which makes a reading wrong without any rollback. The
  cost, when it happens: T's message, submitted again, is signed with the
  next nonce, and one of the two transactions fails, paying its whole gas
  limit, as in the issue above. The fix, needed before mainnet: an account
  read tied to the block it was read at, which Rusk does not serve, or
  forgetting a transaction on its nonce only once a transaction with that
  nonce is found in a final block.
- **Known issue of delivery gas accounting: a crash at one point leaves one
  failure uncharged.** The relayer charges a delivery's gas to its message
  at most once, never twice. Before `process` returns a failed outcome, the
  agent notes on the delivery's kept transaction that its failure was
  reported, so that it is never reported again; only then does the relayer
  add that gas to the message's expenditure, in its own database. A relayer
  that stops between the two, or whose write to that database fails (it logs
  the error and goes on), leaves that failure's gas uncharged: at most one
  failed attempt's gas, its whole gas limit, per crash at that exact point,
  and the message's payment pays for one more attempt than it covers. A
  record kept before the agent noted reported failures, which only builds of
  2026-11-16 wrote, reads as reported for the same reason, at the same cost.
  Charging exactly once would need both databases written together, or the
  relayer to record expenditure by transaction; neither is done.
- **Waiting for a block.** The node builds a block from what its mempool
  holds when the block's round starts, so a transaction it admits is in a
  block 10 to 20 s later on a 10 s chain. `dusk-tx` waits for a delivery
  sent one at a time itself: it looks the transaction up right after
  propagating it and every 3 s after that, for up to 60 s, about 6 `tx`
  reads a delivery. Where the agent waits itself, for the receipt once
  `dusk-tx` has returned, which it finds at once, for a transaction whose
  outcome is unknown, and for pipelined deliveries, it looks the
  transaction up at once, again one block (10 s) later, then every 2.5 s
  until 20 s have passed, and after that every quarter of the time waited,
  at most every 10 s: 17 lookups over its 120 s wait, where one every 2 s
  made 61, and at most 6 for a transaction in a block 10 to 20 s after it
  was sent, found at most 2.5 s after its block, where the pipelined
  deliveries the benchmark measured took 9.3.
- **Pipelined deliveries (experimental, off by default, not for public
  networks until their known issues, listed below, are fixed).** Only a
  chain that sets `pipelineDeliveries: true` and a `maxBatchSize` above 1,
  with a helper that takes `call --nonce <N> --no-wait` (found from its
  `call --help`, once per process), pipelines: the relayer submits a batch
  of deliveries as consecutive transactions of its account, each once the
  node holds the one before, and they share a block. A configuration that
  only sets `maxBatchSize` delivers one at a time; the deploy scripts write
  neither setting. The agent logs the mode once at startup. **While it pipelines, a
  relayer must be the only process that signs with its key on that chain:
  no other relayer, validator or wallet may use the key.** One that finds a
  nonce it holds used by a transaction it did not sign logs a critical
  error and delivers one at a time from then on, until restarted. The
  helper signs each pipelined transaction without sending it: the agent
  gives it the URL of an endpoint of its own on the loopback interface,
  under a path no other process can guess, which answers its chain ID query
  and keeps the transaction it preverifies and propagates there. The agent
  keeps the transaction's hash in `eventCursorDir` and only then sends it
  to the node, preverification first. A transaction the node refuses at
  preverification was not sent; one whose propagation fails or goes
  unanswered may have been, and its hash stays. A refusal, an unknown
  outcome, or a transaction the node does not hold within 5 s ends the
  batch, and so does the end of the batch's 60 s to send, which the
  reconciliation, the helper's signing and every wait end by: no
  transaction is sent after it. The batch then follows each transaction it
  sent or follows to its block; one that is in none by then still counts
  as sent, and the relayer checks that its message was delivered before it
  submits it again, which then follows the same transaction. At most 64 go
  in a batch, Rusk's queue of an account's future nonces. With another
  helper, every delivery is submitted one at a time.
- **Nonces in flight (pipelined only).** Every nonce a pipelined
  transaction is signed with is held by a reservation in `eventCursorDir`
  (in memory for a client without one), kept before anything is signed
  with it: for a delivery, with its arguments, or for a filler, a call of
  the Mailbox's `state_version` that only uses the nonce. A reservation
  lists every transaction signed with its nonce that may have reached the
  node, and is released only once one of them is in a block at or below
  the node's last final block, read before the transaction, or when it
  never sent one. A delivery that lands, or that another relayer makes,
  releases nothing. No nonce is held twice, and no message holds two.
  Before it sends, as a batch or one at a time, also after a restart, the
  relayer reconciles the reservations with the node, reading its last final
  block, then the account, then each transaction, each by the batch's
  deadline. A delivery whose transaction is in the mempool or a block, was
  sent less than 60 s ago, or whose nonce the mempool holds, or that
  cannot be looked up, is not sent again but followed. One whose
  transactions are in neither, with its nonce unused, goes again with its
  nonce: with the batch's arguments when its message is in the batch,
  otherwise as it was signed, after a fresh simulation, or a filler takes
  the nonce when that simulation fails, as when another relayer delivered
  the message. A free nonce below held ones gets a filler. New deliveries
  take the nonces past every held one while the signer holds fewer than
  64, those in blocks that are not final included, so that a rollback
  cannot put more than 64 back into the mempool. Going again and filling
  need no room under that bound, so the signer never stalls behind a gap.
  A nonce whose 8 different transactions the node all dropped is not sent
  again, with a critical error. Deliveries the previous version kept in flight, one
  record per message, are waited out as below.
- **Switching pipelining off.** Turning `pipelineDeliveries` off after a
  pipelined run needs no manual step, and the reservations that run left in
  `eventCursorDir` must not be deleted by hand. Before a delivery goes one
  at a time, the relayer looks for them, and for the records the previous
  version kept; with none, it reads nothing from the node, and they never
  change how a delivery is signed. It reconciles them as above, under the
  signer's lock. One whose transaction is in a final block is released, and
  so is one whose transactions are in neither the mempool nor a block while
  the account has used its nonce, or after 60 s with its nonce unused and
  not held by the mempool: nothing of it can be admitted any more. One whose
  transaction is in a block that is not final yet is kept until it is, and
  holds nothing up: dusk-tx signs above its nonce, and a rollback puts it
  back into the mempool, where the node refuses another transaction of its
  nonce. While one may still be admitted ahead of a delivery (its
  transaction is in the mempool, it was sent less than 60 s ago, the mempool
  holds its nonce for a transaction it does not name, or it cannot be looked
  up), every delivery fails and the relayer tries it again later, usually
  for a block or two. The relayer waits these out rather than refusing to
  start: the records drain only while a relayer runs, and only the ones that
  could still land ahead of a delivery sent one at a time hold anything up.
- **Known issues of pipelined deliveries.** The last review of the
  pipelined path found six faults that are not fixed. **Do not enable
  `pipelineDeliveries` on a public network until they are.** The default,
  one delivery at a time, has none of them; its own known issue is listed
  above.
  1. *Switching pipelining off forgets transactions that can still land.*
     A reservation whose transactions are in neither the mempool nor a
     block, past the 60 s drop grace and with its nonce unused, is
     released, and the previous version's records alike. A signed Moonlight
     transaction does not expire, though: anyone who kept it, a peer node
     included, can send it again later. When its message went meanwhile in
     another transaction, with another nonce, the old one can still land
     and fail, paying its whole gas limit. Its nonce range has to be used
     up (with fillers where needed) and final before it is forgotten.
  2. *Proxy settings reach the loopback signer.* The helper inherits the
     agent's environment, and its HTTP client uses `HTTP_PROXY` and
     `ALL_PROXY` for every URL `NO_PROXY` does not exempt, the loopback
     capture endpoint's included. A proxy then answers the chain ID query
     and receives the signed transaction at preverification, before the
     agent keeps its hash; the agent captured nothing, releases the
     reservation, and the proxy holds a transaction it can send. The
     helper's capture runs need to bypass every proxy.
  3. *A filler loses the message it replaced.* A dropped delivery whose
     simulation fails while it is sent again, a transport error included,
     is turned into a filler for good, and its message is forgotten while
     its signed delivery stays valid. When the message's next simulation
     passes, it is planned as a new delivery with another nonce, and the
     old transaction can land first, so the new one fails and pays its
     whole gas limit. The reservation has to keep its message until the
     nonce is final, and a transport failure has to defer the recovery.
  4. *Recovered deliveries escape gas accounting.* A dropped delivery that
     a later batch sends again, outside that batch's own messages, gets no
     outcome, so its gas is never reported to the relayer. The relayer
     charges a message's gas payment only with the outcomes reported for
     it, so that message's payment stays unspent and pays for another
     attempt. A filler's gas is never reported either.
  5. *Fillers bypass the bound of 64.* Only new deliveries count against
     the 64 deliveries a signer keeps outstanding; a filler adds a
     reservation past it. A gap below 64 held nonces (another wallet's
     transaction left pending, say) ends with 65 reservations or more, so
     a rollback can put more than 64 back into the mempool, which the bound
     is there to prevent. Recovery has to fill gaps within the bound.
  6. *Simulations run past the batch's deadline.* The `process` simulations
     that prepare a batch, and those of dropped deliveries sent again under
     the signer's lock, are not bounded by the batch's 60 s: a stalled one
     waits out the HTTP client's 30 s timeout, so 64 of them can take about
     32 minutes. They have to stop at the deadline.
- **Delivery simulation.** Dusk charges a failed transaction its whole gas
  limit. Before submitting `process`, the relayer runs it as an unsigned
  contract query, which Rusk executes in a throwaway session. If the query
  fails, or replies with anything other than the encoding of the `()` that
  `process` returns, nothing is submitted and the message is retried later.
  A simulation of the same arguments that passed less than 2 s before
  stands for the next one, so the relayer's preparation, its dry run before
  submitting and `process` itself, which ran three in a row, run one when
  the submission follows the preparation. Rusk's query reports no gas use,
  and its transaction simulation needs a signed transaction a node could
  include, so `gasLimit` stays the estimate of every delivery.
- **Delivery and announcement finality.** Contract queries read the node's
  tip, which Rusk can roll back until a block is final, and Rusk 1.7.3 has
  no query of contract state at the last finalized block. The relayer stops
  retrying a message once the Mailbox reports it delivered, so `delivered`
  reports a delivery the tip holds only once the agent's delivery log holds
  its `ProcessId` row in a block `checkBlock(onlyFinalized)` accepts, which
  no rollback can undo, whoever delivered the message. Until then the check
  fails as not final and the relayer tries again later with its usual
  backoff; a rolled-back delivery reads as not delivered, and the message is
  relayed again. That holds as well while the log does not reach the last
  final block (read with `lastBlockPair` before the state), cannot be read,
  or the client has no `eventCursorDir`: the blocks above the last final
  one never make a delivery count, since a check reads them up to a tip
  older than the state it read, and a block accepted and rolled back within
  the check's own requests leaves no trace there. Where the log holds every
  final block, the check reads those blocks (`blocks`, each block's events
  from `contractEvents`) only to name the one that holds the delivery while
  it is not final; when none does, the block was rolled back during the
  check or the node's archive skipped it, which it logs as a warning. When
  finality falls more than 64 blocks behind the tip, it waits for finality
  to catch up instead of reading every block. A validator's startup reports
  its own announcement only once its announcement log holds it in a block
  `checkBlock(onlyFinalized)` accepts, and waits until then. Each check
  reads at most 4 pages of the archive (16 for announcements) to bring its
  log up to date, so an agent started with an empty `eventCursorDir`
  confirms deliveries, or its announcement, only once its log has read the
  contract's history that far, a few checks for a young contract and more
  for a long history.
- **Identity checks.** Building the agent's Dusk components makes no
  request. Before its first request, each client checks, once, that the
  endpoint serves the configured deployment: the node's chain ID, the
  Mailbox and ValidatorAnnounce domains, each contract's `state_version`,
  and the ValidatorAnnounce and Merkle hook bindings to that Mailbox. Every
  client of one endpoint in a process that requires the same deployment,
  such as a validator's indexer and its checkpoint reader, shares the check.
  Until a check succeeds, every request fails without being sent and the
  next one checks again; requests that wait while a check runs share its
  result. A
  node that is unreachable, or serves another deployment, when an agent
  starts therefore delays its reads, and the relayer keeps the chain instead
  of dropping it. Submissions through the `dusk-tx` helper are checked the
  same way first. The agent accepts only the state versions it reads
  (Mailbox 5, MerkleTreeHook 2, ValidatorAnnounce 2) and refuses any other
  with an explicit error, since another version lacks queries the agent
  uses.
- **Checkpoints** are read at consensus finality, adjusted by the configured
  reorg period. The Merkle hook keeps no root per insertion, so a
  checkpoint's root is computed: the agent reads the message IDs the hook
  stores (`message_ids`, pages of 256, each as long as asked) and inserts
  them into hyperlane-core's incremental tree, which it keeps between
  checkpoints and only extends (32 branch nodes and a count, whatever the
  tree's size). Before the root is used, the tree's last ID must be the
  hook's `message_id_at` for that index, and when the hook holds exactly
  those leaves, the hook's own `tree` must have the same count, root and
  root-making branch nodes. Otherwise the read fails, the tree is dropped and
  the next read rebuilds it, so a validator never signs a root computed from
  IDs that disagree with the hook. Through a multicall, the checks
  (`count`, `root`, `tree` and `message_id_at` of the last leaf) and the
  last pages of IDs that fit beside them are one aggregate, at one state,
  instead of queries that can straddle a block; the hook's `root` must be
  the computed one too, and the state's height at or above the block the
  leaves were counted at, or the read fails and is tried again. The height
  only shows that the state is not older than that block: it does not name a
  block, and the leaves are final by their count, found as before.

## Supported

- `index.mode: sequence` only.
- Mailbox `state_version` 5, MerkleTreeHook 2 and ValidatorAnnounce 2: the
  contracts whose growing state is chunked, which keep deliveries, Merkle
  roots and announced locations out of contract state.
- Security modules: `MessageIdMultisig`, plus the companion repository's
  test-only null ISM. Routing, Aggregation and CCIP-read ISMs return an
  explicit unsupported error.
- A Merkle hook attached either directly or inside a one-level static
  AggregationHook as the Mailbox's required hook.
- One RPC endpoint for indexing and submission. Validators can list several
  endpoints for checkpoint quorum; there is no provider failover.

## Configuration

```
"dusk": {
  "name": "dusk",
  "protocol": "dusk",
  "domainId": <domain>,
  "chainId": <chain id reported by the node>,
  "rpcUrls": [{ "http": "https://<rusk-node>" }],
  "eventCursorDir": "/var/lib/hyperlane/dusk-events",
  "gasLimit": 5000000,
  "gasPrice": 2000,
  "contractQueriesPerSecond": 100,
  "contractQueryBurst": 20,
  "batchContractAddress": "0x93de287b7d349d678a0038fe7772e8960e1a84a1b5da2d0b1f62f532616ce1d4",
  "mailbox": "0x<contract id>",
  "interchainGasPaymaster": "0x<contract id>",
  "validatorAnnounce": "0x<contract id>",
  "merkleTreeHook": "0x<contract id>",
  "signer": { "type": "duskKey", "keyFile": "/run/secrets/dusk-signer.key" }
}
```

`gasLimit` is the gas ceiling of every transaction the agent sends, and
defaults to 5,000,000, at most Rusk's block gas limit of 3,000,000,000. A
failed Dusk transaction pays its whole limit, so keep it close to what the
agent needs: a relayed `process` costs about 1.95M gas plus 0.22M per
signature the Dusk ISM checks, and a validator announcement about 2.1M. It
is also the estimate the relayer's gas payment policy compares payments
with, since Rusk reports no gas for a simulation.

`contractQueriesPerSecond` and `contractQueryBurst` pace the agent's
contract queries to its endpoint (see Pacing above); an operator of a node
that serves only its own agents can raise them together with the node's
`[http.policy.global_limits.classes.contract_query]`.

`batchContractAddress` names the Multicall for Dusk contract
([HDauven/dusk-multicall](https://github.com/HDauven/dusk-multicall)) the
agent batches its contract reads through (see Batched reads); on EVM chains
the same setting names Multicall3. Mainnet and testnet run it at
`0x93de287b7d349d678a0038fe7772e8960e1a84a1b5da2d0b1f62f532616ce1d4`. A
contract ID follows the Wasm, the deploy nonce and the owner, not the chain,
so a local chain that deploys the published Wasm with nonce 0 and the
published owner gets the same ID. It has no default: without it, every read
is one query.

`pipelineDeliveries` (false by default, left out above, as the deploy
scripts leave it and `maxBatchSize` out) opts the chain into pipelined
deliveries, which are experimental. Only with it and a `maxBatchSize` (1 by
default) above 1 does the relayer submit up to `maxBatchSize` prepared
deliveries together, as consecutive transactions (see Pipelined
deliveries); otherwise every delivery goes one at a time, whatever
`maxBatchSize` says. **A pipelining relayer must be the only process that
signs with its key on the chain**: give every relayer and validator its
own key. Pipelined deliveries have known issues that are not fixed (see
Known issues of pipelined deliveries): **do not set `pipelineDeliveries`
on a public network until they are.**

The `duskKey` signer takes exactly one of `keyFile` (a regular file with no
group or world permissions), `keyEnv` (an environment variable name) or `key`
(inline; local development only).

## Event log halts

Contract sync indexes deliveries by their sequence in the delivery log, so
the log never numbers a delivery it has published differently. When the
endpoint's archive changes its finalized history under published
deliveries (an archive rebuilt or repaired with rows it lacked, or without
rows it had, or another node's archive with a different history behind the
same URL), every read
of the delivery log fails with an error that starts with `CRITICAL` and
names the contract, the topic, the block, the sequence and the rows, and
the agent logs it as a critical error at most every 10 minutes. The
indexer stops; the relayer's delivery checks, which never publish
sequences, read their own log again instead of halting. To recover:

1. Check the endpoint's archive, and use one whose finalized history you
   trust.
2. Stop the agent.
3. Reset its database (the store its contract sync indexed deliveries
   into, such as the scraper's) and its `eventCursorDir`, so that both
   index the archive afresh. A relayer's `eventCursorDir` also holds the
   transactions of its deliveries in flight, and a pipelining relayer's
   their nonces (see Nonces in flight): reset it only once the signer's
   transactions are in final blocks, or a delivery still in the mempool
   could be sent again with another nonce.
4. Start the agent again.

An archive that only numbers its rows differently, or changes blocks whose
deliveries no consumer has been told of, needs nothing: the log checks the
blocks it holds against the archive and reads changed ones again. The
announcement log is never read by sequence and never halts.

## Shared types

Message, metadata and token-message encoding come from the
`hyperlane-dusk-types` crate, pinned in `Cargo.toml` to an exact
`hyperlane-dusk` commit. Deploy contracts that match that pin. There is no
in-place migration between contract ABI versions.

## Testing

From `rust/main`:

```bash
cargo test -p hyperlane-dusk
cargo test -p hyperlane-base dusk_
cargo test -p hyperlane-base types::multisig
cargo test -p relayer msg::metadata::multisig
cargo test -p validator dusk
cargo test -p validator reorg
cargo clippy -p hyperlane-dusk --all-targets -- -D warnings
```

`cargo test -p hyperlane-dusk --test node_load -- --ignored --nocapture`
measures a restart's catch-up of 10,000 records, with and without a
multicall, in a few minutes. CI runs the others through
`.github/workflows/dusk-agent-gate.yml`. End-to-end tests against a live
node are in the companion repository's `demo/` directory.

CI tests the delivery path against a shell stand-in for `dusk-tx`, since
`dusk-tx` builds against Dusk's private Rusk repository and the agent gate
can't check that out. One test runs the real helper instead:
`a_dusk_tx_build_sends_only_on_the_agents_word` checks that the agent and
`dusk-tx` agree on the `Prepared TX` line, the go-ahead to send and the
error prefixes that decide whether a delivery may have been sent. It is a
release check. Before a release, and whenever `DUSK_REF` or `dusk-tx`
changes, run it with `rusk-private` at hand:

```bash
bash rust/main/chains/hyperlane-dusk/scripts/dusk-tx-interop-check.sh
```

It builds `dusk-tx` from the companion repository at the agent gate's
`DUSK_REF`, which must be the commit `Cargo.toml` pins, and runs the test
against it. `--dusk-ref <sha>` builds another commit, and `--dusk-tx-bin
<path>` tests a build you already have. It expects the layout of the
companion repository's README (`rusk-private` and `hyperlane/dusk` next to
this checkout); `--dusk-dir` and `--rusk-dir` point elsewhere. The
companion repository's `make repro-check-agent` runs the same test against
the `dusk-tx` it builds.

## Changes outside this crate

The integration also touches shared code: Dusk connection and signer parsing
in `hyperlane-base`, the `Dusk` protocol in `hyperlane-core`, and validator
startup, checkpoint consensus and reorg reporting. The backward
sequence-aware cursor starts a range above the logs its store already holds
below the gap it fills, so a restart reads only the logs that arrived while
the agent was stopped, instead of the whole chunk below them. It also carries a fix for
upstream checkpoint quorums, which counted signatures over different message
IDs together (`hyperlane-base`'s multisig syncer, `hyperlane-core`'s
checkpoint types and the relayer's message-ID multisig metadata). The
`dusk-proposal-validation.yml` workflow lists every file the fork may change
relative to upstream.
