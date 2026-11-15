//! What one agent process shares about each Rusk endpoint it sends requests
//! to: how fast it sends contract queries, how it waits when the node
//! refuses requests for its rate limit, the node's last final block, where
//! each contract's records cross final heights, and what it knows of the
//! multicall it batches reads through.
//!
//! Rusk 1.7.3's HTTP policy limits each class of request for all of a
//! node's clients together. Contract queries get 180 a second, at most 360 in
//! any 2 s and 12 at once; GraphQL and the other routes get far more. A
//! request over the limit is answered `429 too_many_requests` with a
//! `Retry-After` of one second. Every client of one endpoint in a process,
//! whichever component built it, paces its contract queries through one
//! schedule here, so the agent's indexers cannot together run past the
//! node's limit, and a refusal holds all of the process's requests of that
//! class back, not only the one refused.
//!
//! Finalized data only changes when the node's last final block does. Every
//! client of an endpoint reads that block from one `lastBlockPair` read per
//! `FINALITY_TICK`, and what the agent computes from final data is kept for
//! the final block it was computed at, so a tick at which the node finalized
//! nothing new costs that one read.

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, PoisonError, Weak};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use url::Url;

use super::batch::Multicall;
use super::{BlockPair, DeploymentIdentity, IdentityGate};
use crate::final_records::FinalRecords;

/// How long every client of an endpoint uses one read of its tip and last
/// final block. The agents' indexers look every 5 s, and Rusk finalizes a
/// block about every 10 s.
pub(crate) const FINALITY_TICK: Duration = Duration::from_secs(5);

/// How many contract queries one agent process sends one Rusk endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryRateLimit {
    /// Contract queries a second, over time.
    pub per_second: u32,
    /// Contract queries that may go out at once after a quiet spell.
    pub burst: u32,
}

impl QueryRateLimit {
    /// 100 a second, 20 at once: well inside the 180 a second and 360 in
    /// 2 s that Rusk 1.7.3 allows all clients of a node together, which
    /// leaves room for the node's other clients, such as validators.
    pub const DEFAULT: Self = Self {
        per_second: 100,
        burst: 20,
    };
}

impl Default for QueryRateLimit {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Contract queries one process has in flight to one endpoint at most. Rusk
/// 1.7.3 runs at most 12 at once for all clients and refuses more.
const MAX_QUERIES_IN_FLIGHT: usize = 8;
/// How often refused requests are logged as a warning per endpoint.
const REFUSAL_WARNING_INTERVAL: Duration = Duration::from_secs(60);
/// The shortest and the longest wait a refusal asks for that the agent
/// honours.
const MIN_RETRY_AFTER: Duration = Duration::from_secs(1);
const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);

/// The classes of request Rusk's HTTP policy limits separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestClass {
    /// `/on/contracts:<id>/<method>`, the class with the lowest limit.
    ContractQuery,
    /// `/on/graphql/query`.
    Graphql,
    /// The other routes the agent uses: transaction preverification and
    /// propagation, account status, gas prices and contract metadata.
    Other,
}

impl RequestClass {
    const ALL: [Self; 3] = [Self::ContractQuery, Self::Graphql, Self::Other];

    fn index(self) -> usize {
        match self {
            Self::ContractQuery => 0,
            Self::Graphql => 1,
            Self::Other => 2,
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::ContractQuery => "contract query",
            Self::Graphql => "GraphQL",
            Self::Other => "RUES",
        }
    }
}

/// When the requests of one class may go out: a steady pace with a burst
/// allowance (the generic cell rate algorithm, delaying instead of
/// refusing), and a hold after the endpoint refused one.
#[derive(Debug)]
struct Schedule {
    /// Time between requests once a burst is spent; `None` paces nothing.
    interval: Option<Duration>,
    /// How far ahead of the steady pace a burst may run.
    tolerance: Duration,
    /// The steady pace's next slot.
    next: Instant,
    /// No request goes out before this.
    held_until: Instant,
}

impl Schedule {
    fn new(limit: Option<QueryRateLimit>, now: Instant) -> Self {
        let mut schedule = Self {
            interval: None,
            tolerance: Duration::ZERO,
            next: now,
            held_until: now,
        };
        if let Some(limit) = limit {
            schedule.set_limit(limit);
        }
        schedule
    }

    fn set_limit(&mut self, limit: QueryRateLimit) {
        let interval = Duration::from_secs_f64(1.0 / f64::from(limit.per_second.max(1)));
        self.interval = Some(interval);
        self.tolerance = interval.saturating_mul(limit.burst.max(1) - 1);
    }

    /// The time a request that asks `now` may go out, which it reserves.
    fn reserve(&mut self, now: Instant) -> Instant {
        let earliest = now.max(self.held_until);
        let Some(interval) = self.interval else {
            return earliest;
        };
        let slot = self
            .next
            .checked_sub(self.tolerance)
            .map_or(earliest, |slot| slot.max(earliest));
        self.next = self.next.max(slot) + interval;
        slot
    }

    /// Hold every request back until `until`; after it they go at the
    /// steady pace, without a burst.
    fn hold(&mut self, until: Instant) {
        self.held_until = self.held_until.max(until);
        if self.interval.is_some() {
            self.next = self.next.max(self.held_until + self.tolerance);
        }
    }
}

/// One endpoint, as every client of it in this process shares it.
#[derive(Debug)]
pub(crate) struct Endpoint {
    schedules: StdMutex<[Schedule; 3]>,
    queries_in_flight: Arc<Semaphore>,
    /// Requests the endpoint refused for its rate limit, ever.
    refusals: AtomicU64,
    /// Refusals since the last warning, and when that was.
    unreported: StdMutex<(u64, Option<Instant>)>,
    /// The node's tip and last final block, and when they were read. A
    /// request for them while they are read waits for that read.
    block_pair: tokio::sync::Mutex<Option<(BlockPair, Instant)>>,
    /// Where each contract's records cross final heights, by contract and
    /// the query that reads a record's height.
    final_records: StdMutex<FinalRecordsByContract>,
    /// Whether the endpoint has been shown to serve a deployment, by
    /// deployment.
    identity_gates: StdMutex<HashMap<DeploymentIdentity, Arc<IdentityGate>>>,
    /// What clients know of each multicall they batch reads through, by
    /// contract.
    multicalls: StdMutex<HashMap<[u8; 32], Arc<Multicall>>>,
}

type Endpoints = HashMap<String, Weak<Endpoint>>;
type FinalRecordsByContract = HashMap<([u8; 32], &'static str), Arc<FinalRecords>>;
static ENDPOINTS: OnceLock<StdMutex<Endpoints>> = OnceLock::new();

impl Endpoint {
    /// The endpoint every client of `url` in this process shares. A new one
    /// paces contract queries at `QueryRateLimit::DEFAULT`.
    pub(crate) fn of(url: &Url) -> Arc<Self> {
        let key = endpoint_key(url);
        let endpoints = ENDPOINTS.get_or_init(|| StdMutex::new(HashMap::new()));
        let mut endpoints = endpoints.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(endpoint) = endpoints.get(&key).and_then(Weak::upgrade) {
            return endpoint;
        }
        // Forget endpoints whose clients are gone, such as a test's.
        endpoints.retain(|_, endpoint| endpoint.strong_count() > 0);
        let now = Instant::now();
        let endpoint = Arc::new(Self {
            schedules: StdMutex::new(RequestClass::ALL.map(|class| {
                Schedule::new(
                    (class == RequestClass::ContractQuery).then_some(QueryRateLimit::DEFAULT),
                    now,
                )
            })),
            queries_in_flight: Arc::new(Semaphore::new(MAX_QUERIES_IN_FLIGHT)),
            refusals: AtomicU64::new(0),
            unreported: StdMutex::new((0, None)),
            block_pair: tokio::sync::Mutex::new(None),
            final_records: StdMutex::new(HashMap::new()),
            identity_gates: StdMutex::new(HashMap::new()),
            multicalls: StdMutex::new(HashMap::new()),
        });
        endpoints.insert(key, Arc::downgrade(&endpoint));
        endpoint
    }

    /// The node's tip and last final block: the last read, while it is
    /// younger than `FINALITY_TICK`, and otherwise `read`, whose result
    /// every caller then shares.
    pub(crate) async fn block_pair<F, Fut, E>(&self, read: F) -> Result<BlockPair, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<BlockPair, E>>,
    {
        let mut last = self.block_pair.lock().await;
        if let Some((pair, read_at)) = *last {
            if read_at.elapsed() < FINALITY_TICK {
                return Ok(pair);
            }
        }
        let pair = read().await?;
        *last = Some((pair, Instant::now()));
        Ok(pair)
    }

    /// The gate of every client of the endpoint that requires `identity`.
    pub(crate) fn identity_gate(&self, identity: DeploymentIdentity) -> Arc<IdentityGate> {
        self.identity_gates
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(identity)
            .or_insert_with(|| {
                Arc::new(IdentityGate {
                    identity,
                    verified: std::sync::atomic::AtomicBool::new(false),
                    finished: AtomicU64::new(0),
                    attempt: tokio::sync::Mutex::new(None),
                })
            })
            .clone()
    }

    /// The multicall at `contract`, as every client of the endpoint that
    /// batches reads through it shares it.
    pub(crate) fn multicall(&self, contract: [u8; 32]) -> Arc<Multicall> {
        self.multicalls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(contract)
            .or_insert_with(|| Arc::new(Multicall::new(contract)))
            .clone()
    }

    /// Forget the last read of the tip and last final block, as if a tick
    /// had passed.
    pub(crate) async fn expire_block_pair(&self) {
        *self.block_pair.lock().await = None;
    }

    /// Where `contract`'s records, whose heights `height_query` reads, cross
    /// final heights, as every client of the endpoint shares it.
    pub(crate) fn final_records(
        &self,
        contract: [u8; 32],
        height_query: &'static str,
    ) -> Arc<FinalRecords> {
        self.final_records
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry((contract, height_query))
            .or_default()
            .clone()
    }

    /// Pace contract queries to this endpoint at `limit`, for every client
    /// of it in this process.
    pub(crate) fn set_query_rate_limit(&self, limit: QueryRateLimit) {
        self.schedules()[RequestClass::ContractQuery.index()].set_limit(limit);
    }

    fn schedules(&self) -> std::sync::MutexGuard<'_, [Schedule; 3]> {
        self.schedules
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Wait until a request of `class` may go out. A contract query also
    /// takes one of the endpoint's in-flight slots, which it holds until it
    /// drops the permit.
    pub(crate) async fn admit(&self, class: RequestClass) -> Option<OwnedSemaphorePermit> {
        loop {
            let slot = self.schedules()[class.index()].reserve(Instant::now());
            if slot > Instant::now() {
                tokio::time::sleep_until(slot).await;
            }
            // A refusal while this request waited holds it back as well.
            if Instant::now() >= self.schedules()[class.index()].held_until {
                break;
            }
        }
        match class {
            // The semaphore is never closed, so this always holds a permit.
            RequestClass::ContractQuery => {
                self.queries_in_flight.clone().acquire_owned().await.ok()
            }
            RequestClass::Graphql | RequestClass::Other => None,
        }
    }

    /// The endpoint refused a request of `class` for its rate limit and
    /// asked to wait `retry_after`, the `refusals`th time in a row for that
    /// request. Hold every request of the class back that long, plus up to
    /// as long again at random, so that the process's tasks, and other
    /// agents, do not all come back at once. Returns how long the refused
    /// request waits before it is sent again: twice as long after each
    /// refusal in a row, and at random up to twice that, so that requests
    /// refused together come back apart.
    pub(crate) fn refused(
        &self,
        class: RequestClass,
        retry_after: Option<Duration>,
        refusals: u32,
    ) -> Duration {
        let retry_after = retry_after
            .unwrap_or(MIN_RETRY_AFTER)
            .clamp(MIN_RETRY_AFTER, MAX_RETRY_AFTER);
        let now = Instant::now();
        let hold = retry_after.mul_f64(1.0 + jitter());
        self.schedules()[class.index()].hold(now + hold);
        self.refusals.fetch_add(1, Ordering::Relaxed);
        {
            let mut unreported = self
                .unreported
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            unreported.0 += 1;
            let due = unreported
                .1
                .is_none_or(|last| now.duration_since(last) >= REFUSAL_WARNING_INTERVAL);
            if due {
                tracing::warn!(
                    class = class.name(),
                    refused = unreported.0,
                    retry_after_secs = retry_after.as_secs(),
                    "Dusk endpoint refused requests for its rate limit; holding this agent's requests back and retrying them"
                );
                *unreported = (0, Some(now));
            }
        }
        let backoff = retry_after
            .saturating_mul(1 << refusals.saturating_sub(1).min(4))
            .min(MAX_RETRY_AFTER);
        backoff.mul_f64(1.0 + jitter())
    }

    /// Requests the endpoint has refused for its rate limit.
    #[cfg(test)]
    pub(crate) fn refusals(&self) -> u64 {
        self.refusals.load(Ordering::Relaxed)
    }
}

/// The key of an endpoint: its URL without a trailing slash or fragment, as
/// requests to it are built.
fn endpoint_key(url: &Url) -> String {
    let mut canonical = url.clone();
    canonical.set_path(url.path().trim_end_matches('/'));
    canonical.set_fragment(None);
    canonical.to_string()
}

/// The `Retry-After` of a refusal, in seconds, if it carries one.
pub(crate) fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

/// A number in `[0, 1)` that differs between calls, threads and processes.
fn jitter() -> f64 {
    static CALLS: AtomicU64 = AtomicU64::new(0);
    // Each `RandomState` has its own random keys.
    let random = RandomState::new().hash_one(CALLS.fetch_add(1, Ordering::Relaxed));
    (random >> 11) as f64 / (1u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(per_second: u32, burst: u32) -> QueryRateLimit {
        QueryRateLimit { per_second, burst }
    }

    #[test]
    fn a_burst_goes_at_once_and_the_rest_at_the_steady_pace() {
        let now = Instant::now();
        let mut schedule = Schedule::new(Some(limit(100, 5)), now);
        let slots: Vec<Duration> = (0..8)
            .map(|_| schedule.reserve(now).duration_since(now))
            .collect();
        let ms = Duration::from_millis;
        assert_eq!(slots[..5], [Duration::ZERO; 5]);
        assert_eq!(slots[5..], [ms(10), ms(20), ms(30)]);
        // After a quiet spell the burst is there again.
        let later = now + Duration::from_secs(1);
        assert_eq!(schedule.reserve(later), later);
    }

    #[test]
    fn an_unpaced_class_only_waits_out_holds() {
        let now = Instant::now();
        let mut schedule = Schedule::new(None, now);
        for _ in 0..1000 {
            assert_eq!(schedule.reserve(now), now);
        }
        schedule.hold(now + Duration::from_secs(2));
        assert_eq!(schedule.reserve(now), now + Duration::from_secs(2));
    }

    #[test]
    fn a_hold_delays_everyone_and_spends_the_burst() {
        let now = Instant::now();
        let mut schedule = Schedule::new(Some(limit(100, 20)), now);
        let held = now + Duration::from_secs(1);
        schedule.hold(held);
        let slots: Vec<Instant> = (0..3).map(|_| schedule.reserve(now)).collect();
        let ms = Duration::from_millis;
        assert_eq!(slots, [held, held + ms(10), held + ms(20)]);
    }

    #[test]
    fn the_pace_holds_over_many_requests() {
        // 1,000 queries at 100 a second with a burst of 20 take 9.8 s.
        let now = Instant::now();
        let mut schedule = Schedule::new(Some(limit(100, 20)), now);
        let last = (0..1000).map(|_| schedule.reserve(now)).last().unwrap();
        assert_eq!(last.duration_since(now), Duration::from_millis(9_800));
    }

    #[test]
    fn retry_after_is_read_in_seconds() {
        let mut headers = reqwest::header::HeaderMap::new();
        assert_eq!(retry_after(&headers), None);
        headers.insert(reqwest::header::RETRY_AFTER, "3".parse().unwrap());
        assert_eq!(retry_after(&headers), Some(Duration::from_secs(3)));
        headers.insert(reqwest::header::RETRY_AFTER, "soon".parse().unwrap());
        assert_eq!(retry_after(&headers), None);
    }

    #[test]
    fn refused_requests_back_off_apart() {
        let url = Url::parse("http://127.0.0.1:9/refusals").unwrap();
        let endpoint = Endpoint::of(&url);
        let delays: Vec<Duration> = (1..=6)
            .map(|refusals| {
                endpoint.refused(
                    RequestClass::ContractQuery,
                    Some(Duration::from_secs(1)),
                    refusals,
                )
            })
            .collect();
        for (refusals, delay) in (1u32..).zip(&delays) {
            let base = Duration::from_secs(1 << (refusals - 1).min(4)).min(MAX_RETRY_AFTER);
            assert!(*delay >= base && *delay < base * 2, "{refusals}: {delay:?}");
        }
        assert_eq!(endpoint.refusals(), 6);
        // Jitter differs between calls, so requests refused together do not
        // come back together.
        let spread: std::collections::HashSet<u128> = (0..16)
            .map(|_| endpoint.refused(RequestClass::Graphql, None, 1).as_nanos())
            .collect();
        assert!(spread.len() > 8, "{spread:?}");
    }

    #[test]
    fn clients_of_one_url_share_its_endpoint() {
        let url = Url::parse("http://127.0.0.1:9/shared/#fragment").unwrap();
        let endpoint = Endpoint::of(&url);
        assert!(Arc::ptr_eq(
            &endpoint,
            &Endpoint::of(&Url::parse("http://127.0.0.1:9/shared").unwrap())
        ));
        assert!(!Arc::ptr_eq(
            &endpoint,
            &Endpoint::of(&Url::parse("http://127.0.0.1:9/other").unwrap())
        ));
    }
}
