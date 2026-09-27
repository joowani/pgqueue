//! The embedded web dashboard: an axum router serving a JSON API and a
//! no-build-step static frontend for managing queues and jobs.
//!
//! Run it as a standalone server:
//!
//! ```ignore
//! Dashboard::new([queue])
//!     .basic_auth("admin", "password")
//!     .secure_cookies(false) // only for direct HTTP on a trusted network
//!     .serve_on("0.0.0.0", 8080)
//!     .run()
//!     .await?;
//! ```
//!
//! Or host it inside a worker process:
//!
//! ```ignore
//! let dashboard = Dashboard::new([queue.clone()])
//!     .basic_auth("admin", "password")
//!     .secure_cookies(false) // only for direct HTTP on a trusted network
//!     .serve_on("0.0.0.0", 8080);
//! Worker::builder(queue)
//!     .register_job(job)
//!     .dashboard(dashboard)
//!     .run()
//!     .await?;
//! ```
//!
//! Or mount its router in an existing axum application. Serve that application
//! with `into_make_service_with_connect_info::<SocketAddr>()` (or set
//! [`Dashboard::trusted_proxy_hops`] behind a reverse proxy): the
//! authentication throttle is keyed by client address, and without one every
//! client in the world shares a single budget, so a flood of wrong passwords
//! from anywhere answers the operator's correct login `429` for as long as it
//! runs:
//!
//! ```ignore
//! app.nest(
//!     "/admin",
//!     Dashboard::new([queue])
//!         .allow_unauthenticated() // application middleware protects this router
//!         .allowed_hosts(["app.example.com"]) // the names the application is served under
//!         .mount_path("/admin")
//!         .router()?,
//! );
//! axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>());
//! ```
//!
//! Building requires an explicit choice: use [`Dashboard::basic_auth`], or call
//! [`Dashboard::allow_unauthenticated`] when application middleware protects
//! the router or it stays on a trusted network. Serve credentials only over TLS.
//!
//! Every state-changing route — the job retry and abort actions, the password
//! change and the logout — requires the request header
//! `X-PGQueue-Request: dashboard`. It is the CSRF guard: a cross-site form post
//! cannot set a request header, so the credentials a browser attaches on its
//! own cannot reach an action. A `POST` without it is answered `403 Forbidden`,
//! so a script driving the API has to send it as well.
//!
//! `POST /login` is the one state-changing route that cannot require it — it is
//! a real HTML form, so nothing of ours runs before the browser sends it. It is
//! guarded on `Sec-Fetch-Site` instead: a post the browser reports as coming
//! from anywhere but the dashboard itself is answered `403 Forbidden` before it
//! can spend any of the account's rate-limit budget. Without Fetch Metadata, a
//! post whose `Origin` names a different authority than `Host` is refused the
//! same way. Clients that send neither header — every non-browser client — are
//! unaffected.
//!
//! Neither guard stops DNS rebinding, where the browser takes an attacker's page for the dashboard's own, so the
//! dashboard also refuses, with `403 Forbidden`, every request addressed to a host name it was not given. It answers
//! IP addresses, `localhost`, the host given to [`Dashboard::serve_on`], and the names listed with
//! [`Dashboard::allowed_hosts`]. `/health` answers under any name.

use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, DefaultBodyLimit, Form, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::serve::Listener as AxumListener;
use axum::{Extension, Json, Router};
use include_dir::{Dir, include_dir};
use jiff::Timestamp;
use jiff_sqlx::ToSqlx;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use uuid::Uuid;

use crate::Error;
use crate::database::{Database, is_lock_refusal};
use crate::job::{JobRetryBackoff, JobRow, JobStatus, MIN_TIMESTAMPTZ};
use crate::queue::Queue;
use crate::worker::{WorkerHealth, WorkerHealthStatus, WorkerInfo};

pub(crate) struct DashboardState {
    queues: Vec<Queue>,
    worker_health: Option<WorkerHealth>,
    /// Last `/health` probe round. The route is deliberately unauthenticated,
    /// so without this a request flood would run one query per queue per
    /// request on the very pool the worker dequeues and finalizes with.
    health_probe: RoundCache<bool>,
    /// Last `/api/queues` fan-out. It issues one `DASHBOARD_SIGNALS_SQL` per
    /// configured queue and `app.mjs` polls it every 5s per open tab, so
    /// ungated it turned 20 queues across 30 open tabs into 600 concurrent
    /// queries parked on `pool.acquire()` — enough for the worker's own
    /// dequeue, heartbeat and `finish` calls to hit `acquire_timeout`, let
    /// their leases lapse, and have the sweeper reclaim still-running
    /// attempts. `None` is a round that failed; it logged the error itself.
    queue_signals: RoundCache<Option<Vec<DashboardQueueSignals>>>,
    /// How long Retry and Abort wait for a lock on their job in a request no [`ActionDeadline`] reached: one served by
    /// [`Dashboard::router`] in an application's own server, which this crate sets no request deadline in.
    action_lock_wait: Duration,
}

impl DashboardState {
    /// When an action in this request stops waiting for a lock: its [`ActionDeadline`], or for a router without
    /// request limits, [`DashboardState::action_lock_wait`] from now. Bounded there too, because the application's
    /// own request timeout, or a client hanging up, drops the action exactly as the limiter does, and would park its
    /// connection just the same. `None` only past the clock's range.
    fn action_deadline(&self, deadline: Option<Extension<ActionDeadline>>) -> Option<tokio::time::Instant> {
        match deadline {
            Some(Extension(ActionDeadline(at))) => Some(at),
            None => tokio::time::Instant::now().checked_add(self.action_lock_wait),
        }
    }
}

/// How long a `/health` probe result is reused. Short enough that an
/// orchestrator still sees a real outage promptly, long enough that request
/// rate cannot translate into database load.
const HEALTH_PROBE_TTL: Duration = Duration::from_millis(500);

/// How long a `/api/queues` fan-out is reused. Well under the 5s poll `app.mjs`
/// runs, so an open dashboard still repaints on its own cadence while extra
/// tabs — and extra clients — cost nothing.
const QUEUE_SIGNALS_TTL: Duration = Duration::from_secs(1);

/// The default [`DashboardServer::request_timeout`], and how long Retry and Abort wait for a lock on their job in a
/// router no `DashboardServer` serves: see [`DashboardState::action_lock_wait`].
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// When Retry or Abort stops waiting for a lock another transaction holds on its job, handed to the action in the
/// request's extensions by [`enforce_request_limits`].
///
/// The abort waited for the job's row lock, and the retry for its dedupe key's, with no bound of the server's own,
/// and cutting a request off at its deadline ends only the client's side of a wait. sqlx pings a connection before
/// pooling it again, so the ping waited for the statement, and the statement for the lock holder — an operator's
/// `SELECT ... FOR UPDATE` left open, a claim whose `COMMIT` was lost. Every click answered 504 kept one connection of
/// the queue's pool, which an in-process worker dequeues, heartbeats and finalizes with, for as long as the holder
/// lived; the button re-arms after an error, so a few more clicks took the rest; and an abort reported as timed out
/// still aborted the job once the holder let go. Bounded on the server (see `Database::abort_within`), the wait ends
/// with the request, and the action with it, having changed nothing.
///
/// It ends ahead of the request's own deadline — by a tenth of the timeout, and by a second at most — so that the
/// refusal is still answered as what it is, a job locked by another transaction, rather than as the limiter's bare
/// 504.
#[derive(Clone, Copy)]
pub(crate) struct ActionDeadline(tokio::time::Instant);

impl ActionDeadline {
    /// The deadline for actions in a request that the limiter cuts off `timeout` from now; `None` past the clock's
    /// range.
    fn ahead_of(timeout: Duration) -> Option<Self> {
        let lead = (timeout / 10).min(Duration::from_secs(1));
        tokio::time::Instant::now().checked_add(timeout - lead).map(Self)
    }
}

/// How long a caller waits for the round it is riding on before giving up.
///
/// Without a bound, a wedged database parked every request — including every
/// unauthenticated `/health` — on the round forever, which is worse than an
/// answer: an orchestrator learns nothing from a probe that never returns.
pub(crate) const ROUND_WAIT_TIMEOUT: Duration = Duration::from_secs(2);

/// How long the round *itself* may run before it is abandoned.
///
/// Bounding only the waiter left the round unbounded, and the round holds the
/// one permit that lets a later round start: a round that never resolves
/// therefore stopped every future round as well, permanently — every caller,
/// including every unauthenticated `/health`, waited [`ROUND_WAIT_TIMEOUT`] and
/// answered 503 until the process was restarted, which a readiness-only probe
/// never triggers.
///
/// A round can hang for far longer than any query should take: a failover can
/// leave an *already acquired* pooled connection black-holed, which
/// `acquire_timeout` does not cover (the connection is out of the pool
/// already) and no statement timeout ends (these queries set none), so the
/// send waits on the OS TCP keepalive — hours on a default Linux — while new
/// connections work fine and the worker recovers around it. Abandoned instead,
/// the wedge costs one round: the next caller opens a round of its own on a
/// healthy connection.
///
/// The bound is well past [`ROUND_WAIT_TIMEOUT`] rather than a small multiple
/// of it, because a *slow* round is one this cache is built to serve and a
/// round cut short publishes nothing at all. sqlx's default `acquire_timeout`
/// is 30s and a saturated pool spends it before the query even starts — exactly
/// the load this cache exists to absorb — so a bound under that would abandon
/// rounds that were merely queued. Past it, one minute trades a wedge of 503s
/// for the hours a black-holed connection would otherwise hang.
const ROUND_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the answer of a *slow* round — one that ran for
/// [`ROUND_WAIT_TIMEOUT`] or longer — that no caller has received yet stays
/// deliverable to the next caller, whatever the TTL says.
///
/// A round slower than [`ROUND_WAIT_TIMEOUT`] publishes after its waiters have
/// answered 503, and its value is already older than the TTL by the time the
/// next request arrives — `app.mjs` polls every 5s against a 1s TTL, and an
/// orchestrator probes `/health` every few seconds against a 500ms one. Gated on
/// the TTL alone, that answer was never served: each request found it stale,
/// started another round just as slow, and gave up on that one too, so a
/// database that was answering every query — only slowly — kept both routes at
/// 503 for as long as it stayed slow. Handing the late answer to the next caller
/// is what makes such a round "only late, not lost".
///
/// Only a slow round's answer. A quicker one that nobody received was not given
/// up on for slowness: its only caller hung up — a probe client with a timeout
/// of its own under [`ROUND_WAIT_TIMEOUT`] — or it is the refresh started by a
/// caller that took a late answer, which returns without waiting for it. Taken
/// as late too, that refresh became the next caller's late answer, whose own
/// refresh became the one after, and so on: a single slow round left every
/// later answer one poll behind, outages and recoveries alike, however fast the
/// database was again. Not late, it is merely stale, and the next caller starts
/// a round and waits for it as usual.
///
/// Delivered once, and only while this young, so the staleness it can add is
/// one response, bounded by a poll period rather than by how long the process
/// has been idle: a wedged round still turns into a 503 at the next request.
/// While rounds stay slow, each poll is answered by the round the poll before
/// it started.
const LATE_ANSWER_TTL: Duration = Duration::from_secs(30);

/// A value produced by at most one round at a time and reused for a TTL.
///
/// The round runs *detached* rather than inside the request that started it.
/// `/health` and `/api/queues` are reached by clients this process does not
/// control, and dropping the request future — which is exactly what a client
/// that sends `GET /health` and resets the connection does — aborted the round
/// with it, so nothing was ever cached. Repeated at line rate that kept one
/// probe round permanently in flight against the pool the worker dequeues,
/// heartbeats and finalizes with, plus the connection churn of sqlx discarding
/// connections whose query was cancelled mid-flight: precisely the load the
/// cache exists to prevent. Detached, a round finishes and publishes however
/// the requester ended, so whoever asks next is served from the cache, bounded
/// by [`ROUND_TIMEOUT`].
pub(crate) struct RoundCache<T> {
    ttl: Duration,
    /// The last published round. Callers subscribe *before* reading it, so a
    /// round that publishes while they are still deciding wakes them instead
    /// of being missed.
    published: tokio::sync::watch::Sender<Option<RoundAnswer<T>>>,
    /// One permit, taken by the round in flight and released once it has
    /// published or been abandoned. The TTL alone bounds the rate only while
    /// rounds are *fast*, which is exactly when it does not matter: with the
    /// query slow (lock contention, `max_connections` pressure) every
    /// concurrent request raced past the not-yet-written cache and took a
    /// pooled connection of its own.
    round: Arc<tokio::sync::Semaphore>,
}

/// One published round.
#[derive(Clone)]
struct RoundAnswer<T> {
    /// When the round published, on Tokio's clock so a paused test clock ages
    /// it too.
    at: tokio::time::Instant,
    value: T,
    /// Whether the round ran for [`ROUND_WAIT_TIMEOUT`] or longer, timed from
    /// before the caller that started it began to wait. Every caller that gave
    /// up on the round began waiting no earlier, so a round that outlasted any
    /// of them is always counted slow.
    slow: bool,
    /// Whether any caller has received this value yet. A slow round that
    /// published after all of its waiters gave up leaves this `false`, and the
    /// next caller takes it even past the TTL — see [`LATE_ANSWER_TTL`].
    delivered: Arc<std::sync::atomic::AtomicBool>,
}

impl<T: Clone> RoundAnswer<T> {
    /// The value, recorded as having reached a caller.
    fn deliver(&self) -> T {
        self.delivered.store(true, std::sync::atomic::Ordering::Release);
        self.value.clone()
    }
}

impl<T: Clone + Send + Sync + 'static> RoundCache<T> {
    fn new(ttl: Duration) -> Self {
        Self { ttl, published: tokio::sync::watch::channel(None).0, round: Arc::new(tokio::sync::Semaphore::new(1)) }
    }

    /// The cached value, starting `round` first when nothing fresh is
    /// published and no round is already running.
    ///
    /// `None` means no round published within [`ROUND_WAIT_TIMEOUT`]. The
    /// round itself keeps going, and its answer goes to the next caller even if
    /// it lands past the TTL, so the answer is only late, not lost.
    async fn get<F, R>(&self, round: F) -> Option<T>
    where
        F: FnOnce() -> R,
        R: Future<Output = T> + Send + 'static,
    {
        let mut published = self.published.subscribe();
        let late = match published.borrow().as_ref() {
            Some(answer) if answer.at.elapsed() < self.ttl => return Some(answer.deliver()),
            // Claimed here, before this caller starts a round of its own, so
            // that two callers racing for one late answer cannot both take it.
            // Any other stale answer falls through to a round this caller waits
            // for: see [`LATE_ANSWER_TTL`] for why only a slow one is late.
            Some(answer) if answer.slow && answer.at.elapsed() < LATE_ANSWER_TTL => {
                (!answer.delivered.swap(true, std::sync::atomic::Ordering::AcqRel)).then(|| answer.value.clone())
            }
            _ => None,
        };
        // `try_acquire_owned` rather than `acquire`: losing the race means a
        // round is already running, and this caller's job is then to wait for
        // it, not to queue up behind it with a second one.
        //
        // The permit is dropped *after* the publish, never before: released
        // first, it let the next caller start a redundant round in the moment
        // between — with a fresh value already computed and about to land. It
        // is not what guarantees the wait below is woken, either. A round that
        // is never polled again (runtime shutdown) releases its permit on drop
        // without publishing, so [`ROUND_WAIT_TIMEOUT`] is what bounds the
        // waiter; the permit only bounds how many rounds run at once.
        if let Ok(permit) = Arc::clone(&self.round).try_acquire_owned() {
            let published = self.published.clone();
            // Here, before this caller's wait below starts: timed inside the
            // task instead, a round that a busy runtime started late could
            // outlast that wait and still not be counted slow.
            let started = tokio::time::Instant::now();
            let round = round();
            tokio::spawn(async move {
                // Bounded, because the permit this round holds is what stops
                // the *next* one from starting: see [`ROUND_TIMEOUT`]. An
                // abandoned round publishes nothing, which leaves the last
                // published value in place to age out of the TTL normally
                // rather than overwriting it with a failure this round never
                // actually observed. Its waiters are unaffected either way —
                // they gave up a [`ROUND_WAIT_TIMEOUT`] ago.
                if let Ok(value) = tokio::time::timeout(ROUND_TIMEOUT, round).await {
                    let at = tokio::time::Instant::now();
                    published.send_replace(Some(RoundAnswer {
                        at,
                        value,
                        slow: at.duration_since(started) >= ROUND_WAIT_TIMEOUT,
                        delivered: Arc::default(),
                    }));
                }
                drop(permit);
            });
        }
        // Served after the refresh above is under way, so the round that replaces
        // this answer is already running when the next caller arrives.
        if late.is_some() {
            return late;
        }
        tokio::time::timeout(ROUND_WAIT_TIMEOUT, published.changed()).await.ok()?.ok()?;
        // Whatever that round published is the newest answer there is, so it is
        // taken without re-testing the TTL: a round slower than the TTL would
        // otherwise be rejected the instant it landed, leaving this caller
        // waiting on a round nobody is running any more.
        published.borrow_and_update().as_ref().map(RoundAnswer::deliver)
    }
}

#[cfg(test)]
mod round_cache_tests {
    use super::*;

    /// Covers the wedge [`ROUND_TIMEOUT`] exists for: an unbounded round that
    /// never resolves holds the permit that lets a later round start, so
    /// without the bound the cache never serves another request.
    ///
    /// A paused clock, so the wait bounds are reached deterministically and
    /// without spending them: nothing here touches a database.
    #[tokio::test(start_paused = true)]
    async fn test_a_wedged_round_gives_way_to_the_next_one() {
        // Long enough that nothing below is served from a previous round.
        let cache = RoundCache::<u32>::new(Duration::from_secs(600));

        assert_eq!(
            cache.get(std::future::pending::<u32>).await,
            None,
            "a round that never resolves must not park its caller"
        );
        // The wedge outlives its waiter by design; only the round's own bound
        // hands the permit back.
        assert_eq!(cache.get(std::future::pending::<u32>).await, None);
        tokio::time::sleep(ROUND_TIMEOUT).await;

        assert_eq!(
            cache.get(|| async { 7 }).await,
            Some(7),
            "an abandoned round must leave the cache able to start another"
        );
        // And that one published, so the wedge cost rounds rather than a
        // restart.
        assert_eq!(cache.get(|| async { 9 }).await, Some(7));
    }

    /// A round slower than the callers' patience still has to reach someone. Polled at the dashboard's cadence — far
    /// apart next to the TTL — every late answer had aged out of the TTL by the time the next caller arrived, which
    /// then started a round of its own, just as slow, and gave up on it too: a database answering every query, only
    /// slowly, kept `/health` and `/api/queues` at 503 indefinitely.
    #[tokio::test(start_paused = true)]
    async fn test_a_late_answer_reaches_the_next_caller() {
        const ROUND: Duration = Duration::from_secs(3);
        const POLL: Duration = Duration::from_secs(5);
        let cache = RoundCache::<u32>::new(Duration::from_secs(1));
        let slow = |value| {
            move || async move {
                tokio::time::sleep(ROUND).await;
                value
            }
        };

        assert_eq!(cache.get(slow(1)).await, None, "the first round is slower than the wait");
        tokio::time::sleep(POLL - ROUND_WAIT_TIMEOUT).await;
        assert_eq!(cache.get(slow(2)).await, Some(1), "the late answer goes to the next caller");
        // That caller started the refresh, which is still running: a late answer is delivered once, not re-served.
        assert_eq!(cache.get(slow(3)).await, None);
        tokio::time::sleep(POLL - ROUND_WAIT_TIMEOUT).await;
        assert_eq!(cache.get(slow(4)).await, Some(2), "every poll keeps being answered while rounds stay slow");

        // An answer nobody collected is not served once it is older than a poll period could explain.
        tokio::time::sleep(ROUND + LATE_ANSWER_TTL).await;
        assert_eq!(cache.get(slow(5)).await, None);
    }

    /// A late answer is how a slow round reaches someone, not a mode the cache stays in once the database is fast
    /// again. Every answer nobody had received yet was handed over as late, the refresh started by the caller that took
    /// a late answer included: that caller returns without waiting, so the refresh published to nobody however fast it
    /// was, went to the next poll in place of a fresh round, and that poll started the next unwatched refresh — one
    /// slow round left every later `/health` and `/api/queues` answer a poll behind, outages and recoveries alike.
    #[tokio::test(start_paused = true)]
    async fn test_fast_rounds_after_a_late_answer_are_served_fresh() {
        const POLL: Duration = Duration::from_secs(5);
        let cache = RoundCache::<u32>::new(Duration::from_secs(1));

        let slow = || async {
            tokio::time::sleep(ROUND_WAIT_TIMEOUT + Duration::from_secs(1)).await;
            1
        };
        assert_eq!(cache.get(slow).await, None, "the first round is slower than the wait");
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(cache.get(|| async { 2 }).await, Some(1), "the late answer goes to the next caller");
        for value in 3..=8 {
            tokio::time::sleep(POLL).await;
            assert_eq!(
                cache.get(move || async move { value }).await,
                Some(value),
                "once rounds are fast again, each poll is answered by its own round"
            );
        }
    }

    /// A round nobody stayed for is not late unless it was slow. A probe client whose own timeout is shorter than
    /// [`ROUND_WAIT_TIMEOUT`] — a kubelet's default is one second — hangs up inside a round that then answers well
    /// within the wait; taken as late, that answer went to the next probe a probe period after it was measured, and so
    /// on for every probe after it, so an orchestrator saw each outage and each recovery one probe late.
    #[tokio::test(start_paused = true)]
    async fn test_a_round_whose_caller_hung_up_is_not_served_late() {
        const PROBE: Duration = Duration::from_secs(10);
        let cache = RoundCache::<u32>::new(Duration::from_millis(500));

        let round = || async {
            tokio::time::sleep(Duration::from_millis(1200)).await;
            1
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(500), cache.get(round)).await.is_err(),
            "the caller hangs up before the round answers"
        );
        for value in 2..=6 {
            tokio::time::sleep(PROBE).await;
            assert_eq!(
                cache.get(move || async move { value }).await,
                Some(value),
                "a probe after a round only its caller abandoned is answered by its own round"
            );
        }
    }
}

/// Configures the dashboard router. Requires the `dashboard` feature. See the module docs.
#[must_use = "a Dashboard does nothing until turned into a router or a server"]
pub struct Dashboard {
    queues: Vec<Queue>,
    auth: DashboardAuthentication,
    mount_path: String,
    secure_cookies: bool,
    trusted_proxy_hops: usize,
    /// The host names [`Dashboard::allowed_hosts`] lists, and the one [`Dashboard::serve_on`] binds, as given.
    allowed_hosts: Vec<String>,
    /// [`HEALTH_PROBE_TTL`] unless a test overrode it. Not a public knob: the
    /// shipped value is a trade-off between how promptly an orchestrator sees a
    /// real outage and how much database load a request flood can create, and
    /// neither is the caller's to tune. It exists so a test can assert *that*
    /// the cache answered without racing a 500ms window while the rest of the
    /// suite runs beside it.
    health_probe_ttl: Duration,
    /// [`QUEUE_SIGNALS_TTL`] unless a test overrode it, for the same reason and
    /// on the same terms.
    queue_signals_ttl: Duration,
    /// [`DEFAULT_REQUEST_TIMEOUT`] unless a test overrode it, on the same terms: it exists so that a test can see an
    /// action refused a held lock without waiting out half a minute.
    action_lock_wait: Duration,
}

enum DashboardAuthentication {
    Unconfigured,
    Unauthenticated,
    Basic { username: String, password: String },
}

/// A complete dashboard server configuration, created with
/// [`Dashboard::serve_on`].
///
/// Run it as a standalone server with [`DashboardServer::run`], or pass it to
/// [`crate::WorkerBuilder::dashboard`] to host it in a worker process. Use
/// [`Dashboard::router`] instead when an application already owns an axum
/// server.
pub struct DashboardServer {
    dashboard: Dashboard,
    host: String,
    port: u16,
    limits: DashboardServerLimits,
    ready: tokio::sync::watch::Sender<Option<SocketAddr>>,
}

/// Observes a dashboard server as it starts.
///
/// Obtain a handle with [`DashboardServer::server_handle`] before running or
/// passing the server to [`crate::WorkerBuilder::dashboard`]. This is
/// especially useful with port `0`, where the operating system chooses the
/// listening port.
#[derive(Clone)]
pub struct DashboardServerHandle {
    ready: tokio::sync::watch::Receiver<Option<SocketAddr>>,
}

impl Dashboard {
    /// A dashboard over the given queues (one row per queue on the overview).
    pub fn new(queues: impl IntoIterator<Item = Queue>) -> Self {
        Self {
            queues: queues.into_iter().collect(),
            auth: DashboardAuthentication::Unconfigured,
            mount_path: "/".to_string(),
            secure_cookies: true,
            trusted_proxy_hops: 0,
            allowed_hosts: Vec::new(),
            health_probe_ttl: HEALTH_PROBE_TTL,
            queue_signals_ttl: QUEUE_SIGNALS_TTL,
            action_lock_wait: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    /// Overrides how long a `/health` probe result is reused. See the
    /// `health_probe_ttl` field: for this crate's own tests, not for callers,
    /// which is why it is behind the same feature `__test_support` is.
    #[cfg(feature = "_test")]
    pub(crate) fn with_health_probe_ttl(mut self, ttl: Duration) -> Self {
        self.health_probe_ttl = ttl;
        self
    }

    /// The same, for the `/api/queues` fan-out.
    #[cfg(feature = "_test")]
    pub(crate) fn with_queue_signals_ttl(mut self, ttl: Duration) -> Self {
        self.queue_signals_ttl = ttl;
        self
    }

    /// The same, for how long Retry and Abort wait for a lock on their job in a router without request limits.
    #[cfg(feature = "_test")]
    pub(crate) fn with_action_lock_wait(mut self, wait: Duration) -> Self {
        self.action_lock_wait = wait;
        self
    }

    /// Protects the dashboard with a browser login and HTTP Basic
    /// authentication for API clients. Password changes made in the dashboard
    /// last for the lifetime of the running dashboard process — after a
    /// restart the password configured here is valid again, and separate
    /// dashboard processes share neither sessions nor rotations. A credential
    /// rotated *because this one leaked* is therefore only retired once the
    /// deployment's own configuration changes; treat the in-dashboard change
    /// as a session tool, not as credential storage. The `/health` endpoint
    /// remains unauthenticated for orchestrator probes.
    ///
    /// Both values must be non-empty: an empty one compares equal to the empty
    /// credential every client can send, which is a dashboard that looks
    /// protected and admits anyone. [`Dashboard::router`] and
    /// [`Dashboard::serve_on`] refuse it with [`Error::Config`] rather than
    /// serving it. Passwords under eight characters are refused with the same
    /// floor enforced by the in-dashboard password change.
    ///
    /// The username must not contain `:`, which RFC 7617 forbids in a userid
    /// anyway: HTTP Basic joins the pair with that separator, so a username
    /// carrying one makes the encoded credential ambiguous — `("ops:admin",
    /// "s3cretpw")` would be satisfied by the password `admin:s3cretpw` under the
    /// username `ops` as well. Refused with [`Error::Config`].
    ///
    /// Each value may contain at most 512 bytes. This keeps every accepted credential small enough for the dashboard's
    /// login and password-change request limits, including the extra bytes added by form and JSON encoding. Refused
    /// with [`Error::Config`].
    pub fn basic_auth(mut self, user: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = DashboardAuthentication::Basic { username: user.into(), password: password.into() };
        self
    }

    /// Explicitly disables the dashboard's built-in authentication.
    ///
    /// Use this only on a trusted network or when the router is protected by
    /// application middleware. A dashboard refuses to build until either this
    /// method or [`Dashboard::basic_auth`] has been called.
    pub fn allow_unauthenticated(mut self) -> Self {
        self.auth = DashboardAuthentication::Unauthenticated;
        self
    }

    /// Controls the `Secure` attribute on browser session cookies. Defaults
    /// to `true`; disable it only for direct plain-HTTP access on a trusted
    /// network. TLS-terminated deployments should keep the secure default.
    ///
    /// ```no_run
    /// # fn dashboard(queue: pgqueue::Queue) -> Result<axum::Router, pgqueue::Error> {
    /// let router = pgqueue::Dashboard::new([queue])
    ///     .basic_auth("admin", "password")
    ///     .secure_cookies(false)
    ///     .router()?;
    /// # Ok(router)
    /// # }
    /// ```
    pub fn secure_cookies(mut self, secure: bool) -> Self {
        self.secure_cookies = secure;
        self
    }

    /// How many trusted reverse proxies sit in front of this dashboard, each
    /// appending the address it saw to `X-Forwarded-For`. Defaults to `0`, which
    /// ignores the header entirely and charges authentication attempts to the
    /// socket peer.
    ///
    /// Behind a proxy the socket peer is the proxy, so every request in the
    /// world shares one throttle bucket and a flood of wrong passwords from
    /// anywhere keeps the operator's own login refused (see
    /// [`Dashboard::basic_auth`]). Setting this to the number of proxies restores
    /// per-client keying: the client is the `hops`-th address from the *right*
    /// of the chain, which is the last one your own proxies appended, so
    /// anything a client puts in the header itself is pushed out of reach.
    ///
    /// Set it only when the dashboard cannot be reached except through those
    /// proxies. A client that can connect directly supplies the whole chain, and
    /// so can pick a fresh bucket per request and evade the throttle — or write
    /// any *other* client's address as the entry this picks, spending that
    /// client's budget instead of its own and holding a chosen operator's
    /// correct login at `429` for as long as the flood runs.
    ///
    /// Count only the proxies that actually append to the header. nginx appends
    /// only when told to, with `proxy_set_header X-Forwarded-For
    /// $proxy_add_x_forwarded_for;`, and otherwise passes on whatever the client
    /// sent. Counting one too many is not a safe margin: no chain then reaches
    /// the configured hop count, so every request falls back to the socket peer
    /// — one shared bucket for every honest client — while a client writing its
    /// own entries reaches the selected index and mints a fresh bucket per
    /// request. The first request that cannot satisfy the count logs a warning
    /// naming this method.
    ///
    /// ```no_run
    /// # fn dashboard(queue: pgqueue::Queue) -> Result<axum::Router, pgqueue::Error> {
    /// let router = pgqueue::Dashboard::new([queue])
    ///     .basic_auth("admin", "password")
    ///     // One TLS-terminating proxy, which no client can bypass.
    ///     .trusted_proxy_hops(1)
    ///     .router()?;
    /// # Ok(router)
    /// # }
    /// ```
    pub fn trusted_proxy_hops(mut self, hops: usize) -> Self {
        self.trusted_proxy_hops = hops;
        self
    }

    /// The path prefix the router will be nested under (default `/`), so the
    /// frontend can locate its static files and API. [`DashboardServer`] instances
    /// must keep the default and are served at `/`. A relative path is
    /// normalized to start with `/`. Path segments may contain ASCII letters,
    /// digits, `-`, `_`, `.`, and `~`.
    pub fn mount_path(mut self, path: impl Into<String>) -> Self {
        let path = path.into();
        self.mount_path = if path.starts_with('/') { path } else { format!("/{path}") };
        self
    }

    /// Adds host names the dashboard answers requests for. It always answers requests addressed to an IP address, to
    /// `localhost`, or to the host given to [`Dashboard::serve_on`], and refuses every other request with
    /// `403 Forbidden`, except `/health`, so health probes work under any name.
    ///
    /// The check blocks DNS rebinding. A page on a domain an attacker controls can point that domain at the
    /// dashboard's address, and the browser then treats the page as the dashboard's own, so every check against
    /// cross-site requests passes. An unauthenticated dashboard hands that page every job and every action, and a
    /// protected one its login form. The one thing the page cannot change is the `Host` header, which still names the
    /// attacker's domain.
    ///
    /// List every other name clients use to reach the dashboard, such as a reverse proxy's public name, without a
    /// scheme or port. A reverse proxy must forward the client's `Host` header (nginx: `proxy_set_header Host $host;`).
    /// [`Dashboard::router`] returns [`Error::Config`] for an entry that is not a host name.
    ///
    /// ```no_run
    /// # fn build(queue: pgqueue::Queue) -> Result<axum::Router, pgqueue::Error> {
    /// let router = pgqueue::Dashboard::new([queue])
    ///     .basic_auth("admin", "password")
    ///     .allowed_hosts(["dashboard.example.com"])
    ///     .router()?;
    /// # Ok(router)
    /// # }
    /// ```
    pub fn allowed_hosts(mut self, hosts: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.allowed_hosts.extend(hosts.into_iter().map(Into::into));
        self
    }

    /// Converts this dashboard into a server bound to `host` and `port`.
    ///
    /// `host` may be a hostname such as `"localhost"` or an IP address.
    /// Hostnames are resolved asynchronously when the server starts, and the dashboard listens on every address the
    /// name resolves to — `localhost` is usually both `127.0.0.1` and `::1` — on the one port. A fixed port already
    /// taken on any of them fails the bind. The dashboard answers requests addressed to that name; list any other name
    /// clients use with [`Dashboard::allowed_hosts`].
    ///
    /// Dashboard servers are served at `/`; use [`Dashboard::router`] to mount
    /// the dashboard under a custom path in an existing application.
    ///
    /// # Put it behind a reverse proxy on an untrusted network
    ///
    /// The standalone server defaults to a 10-second header deadline, a
    /// 30-second request and connection deadline, 256 accepted connections, and 128 executing requests.
    /// [`DashboardServer`] exposes setters for each limit. Once the application has prepared the first response on a
    /// connection, its absolute connection deadline does not restart when the client makes progress reading responses.
    /// Connections that never finish their first header are bounded by the header deadline instead.
    ///
    /// A reverse proxy that terminates slow clients — any of nginx, HAProxy, Envoy or an ALB in a default configuration
    /// — is what removes that, and it is still the normal place to terminate TLS and what
    /// [`Dashboard::trusted_proxy_hops`] assumes. The standalone server rejects requests carrying `Transfer-Encoding` —
    /// which covers every request that also carries `Content-Length`, in either order — and closes their connections.
    /// Configure the proxy to reject or normalise the same pair so ambiguous framing never crosses either HTTP hop.
    /// Have it forward the client's `Host` header too, and list the name clients use with
    /// [`Dashboard::allowed_hosts`].
    ///
    /// ```no_run
    /// # async fn run(queue: pgqueue::Queue) -> anyhow::Result<()> {
    /// pgqueue::Dashboard::new([queue])
    ///     .basic_auth("admin", "password")
    ///     .secure_cookies(false) // only for direct HTTP on a trusted network
    ///     .serve_on("localhost", 8080)
    ///     .run()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn serve_on(mut self, host: impl Into<String>, port: u16) -> DashboardServer {
        let host = host.into();
        self.allowed_hosts.push(host.clone());
        let (ready, _) = tokio::sync::watch::channel(None);
        DashboardServer { dashboard: self, host, port, limits: DashboardServerLimits::default(), ready }
    }

    /// Builds the axum router: serve it standalone or `.nest(...)` it into an
    /// existing application. Queue names are the dashboard's URL identifiers,
    /// so duplicate names are rejected rather than leaving one queue unrouted
    /// and unprobed by `/health`.
    ///
    /// At least one queue is required: `/health` probes the configured queues,
    /// so with none it would report ready without ever reaching a database.
    /// Returns [`Error::Config`] otherwise.
    ///
    /// The router answers any request carrying `Transfer-Encoding` with `400` and `Connection: close`, as the
    /// standalone server does (see [`Dashboard::serve_on`]); no dashboard route takes a streamed body. A proxy that
    /// re-frames request bodies as chunked, as some do between an HTTP/2 client and an HTTP/1.1 upstream, must buffer
    /// them instead, or the dashboard refuses them.
    ///
    /// It also refuses requests addressed to a host name it was not given: list the application's own names with
    /// [`Dashboard::allowed_hosts`].
    ///
    /// The router sets no request deadline of its own (see [`DashboardServer::request_timeout`]), so the Retry and
    /// Abort actions wait at most 30 seconds for a lock another transaction holds on their job, then answer
    /// `503 Service Unavailable` with the job unchanged.
    ///
    /// ```no_run
    /// # fn build(queue: pgqueue::Queue) -> Result<axum::Router, pgqueue::Error> {
    /// let router = pgqueue::Dashboard::new([queue]).allow_unauthenticated().router()?;
    /// # Ok(router)
    /// # }
    /// ```
    pub fn router(self) -> Result<Router, Error> {
        self.router_with_health(None, None)
    }

    fn router_with_health(
        self,
        worker_health: Option<WorkerHealth>,
        request_limits: Option<Arc<DashboardRequestLimits>>,
    ) -> Result<Router, Error> {
        validate_mount_path(&self.mount_path)?;
        // `constant_time_eq(b"", b"")` is true, so an empty username or password
        // matches the credential any client can send. The instance still 401s
        // without credentials and still renders a login page, so nothing
        // distinguishes it from a correctly protected one — and it exposes every
        // job payload plus Retry and Abort. `basic_auth(user,
        // env::var("...").unwrap_or_default())` reaches it from one missing
        // environment variable, so refuse to build the router at all.
        let DashboardAuthentication::Basic { username, password } = &self.auth else {
            if matches!(self.auth, DashboardAuthentication::Unconfigured) {
                return Err(Error::Config(
                    "dashboard authentication mode is not configured; call basic_auth or allow_unauthenticated".into(),
                ));
            }
            return self.build_router(worker_health, request_limits);
        };
        if username.is_empty() || password.is_empty() {
            return Err(Error::Config("dashboard basic_auth requires a non-empty username and password".into()));
        }
        // `basic_credentials_match` compares the client's base64 against
        // `base64("{username}:{password}")`, which is what keeps a decoder away
        // from hostile input — but it makes the pair ambiguous when the username
        // itself carries the separator. Configured as `("ops:admin", "s3cretpw")`,
        // the expected string is `ops:admin:s3cretpw`, which the username `ops`
        // with the password `admin:s3cretpw` satisfies equally. The login form
        // compares the two fields separately and refuses that second reading, so
        // one deployment would accept a credential over Basic that it rejects
        // over the form. RFC 7617 forbids a colon in the userid; refuse it here
        // rather than silently widen the accepted set.
        if username.contains(':') {
            return Err(Error::Config("dashboard basic_auth username must not contain ':'".into()));
        }
        if username.len() > MAX_CREDENTIAL_BYTES || password.len() > MAX_CREDENTIAL_BYTES {
            return Err(Error::Config(format!(
                "dashboard basic_auth username and password must each be at most {MAX_CREDENTIAL_BYTES} bytes"
            )));
        }
        // Apply the same floor `change_password` enforces. A weak credential is
        // a configuration error, and failing before the socket binds keeps the
        // dashboard from starting in an unexpectedly exposed state.
        if password.chars().count() < 8 {
            return Err(Error::Config("dashboard basic_auth password must be at least 8 characters".into()));
        }
        self.build_router(worker_health, request_limits)
    }

    fn build_router(
        self,
        worker_health: Option<WorkerHealth>,
        request_limits: Option<Arc<DashboardRequestLimits>>,
    ) -> Result<Router, Error> {
        let allowed_hosts = allowed_host_names(&self.allowed_hosts)?;
        if matches!(self.auth, DashboardAuthentication::Unauthenticated) {
            tracing::warn!(
                "dashboard built without authentication after an explicit opt-out: protect it with application \
                 middleware or keep it on a trusted network"
            );
        }
        let mut queues: Vec<Queue> = Vec::new();
        for queue in self.queues {
            if queues.iter().any(|existing| existing.name() == queue.name()) {
                return Err(Error::Config(format!(
                    "dashboard queue name {:?} is configured more than once",
                    queue.name()
                )));
            }
            queues.push(queue);
        }
        // `/health` is a per-queue probe fanned out over the configured queues,
        // so with none it proves nothing about any database and still answers
        // `200 OK` — a green readiness signal from a dashboard that has never
        // issued a query. `Dashboard::new(queues_from_config)` reaches this from
        // one empty config list, exactly as the empty-credential case above
        // reaches its refusal from one missing environment variable, so refuse
        // it the same way rather than serve a probe that cannot fail.
        if queues.is_empty() {
            return Err(Error::Config("dashboard requires at least one queue".into()));
        }
        let state = Arc::new(DashboardState {
            queues,
            worker_health,
            health_probe: RoundCache::new(self.health_probe_ttl),
            queue_signals: RoundCache::new(self.queue_signals_ttl),
            action_lock_wait: self.action_lock_wait,
        });

        let root = self.mount_path.trim_end_matches('/').to_string();
        let auth_enabled = matches!(self.auth, DashboardAuthentication::Basic { .. });
        let username = match &self.auth {
            DashboardAuthentication::Basic { username, .. } => username.as_str(),
            DashboardAuthentication::Unconfigured | DashboardAuthentication::Unauthenticated => "anonymous",
        };
        let index = render_index(&root, username, auth_enabled);
        let shell = get(move || {
            let index = index.clone();
            async move { Html(index) }
        });

        // Health probes must remain usable by an orchestrator even when the
        // interactive dashboard is protected by browser/basic authentication,
        // and even when every request slot is taken: inside the limiter, a
        // stalled database that parks the dashboard's own traffic long enough
        // answers the probe 504, and an orchestrator restarts a worker that is
        // serving fine. It is merged below *after* the request limiter, because
        // `Router::layer` only wraps the routes a router already carries;
        // [`RoundCache`] bounds the probe at [`ROUND_WAIT_TIMEOUT`] on its own.
        let health_route = Router::new().route("/health", get(health)).with_state(state.clone());
        let protected = Router::new()
            .route("/", shell.clone())
            .route("/queues/{queue}", shell.clone())
            .route("/queues/{queue}/workers/{id}", shell.clone())
            .route("/queues/{queue}/jobs/{id}", shell)
            .route("/api/queues", get(list_queues))
            .route("/api/queues/{queue}/workers", get(list_workers))
            .route("/api/queues/{queue}/workers/{id}", get(worker_detail))
            .route("/api/queues/{queue}/jobs", get(list_jobs))
            .route("/api/queues/{queue}/job-names", get(list_job_names))
            .route("/api/queues/{queue}/jobs/{id}", get(job_detail))
            .route("/api/queues/{queue}/jobs/{id}/retry", post(retry_job))
            .route("/api/queues/{queue}/jobs/{id}/abort", post(abort_job))
            .with_state(state);

        let router = match self.auth {
            DashboardAuthentication::Basic { username, password } => {
                let auth =
                    DashboardAuthState::new(username, password, root, self.secure_cookies, self.trusted_proxy_hops);
                let protected = protected
                    .merge(account_router(auth.clone()))
                    .layer(axum::middleware::from_fn_with_state(auth.clone(), require_auth));
                static_file_router().merge(login_router(auth)).merge(protected)
            }
            DashboardAuthentication::Unauthenticated => static_file_router().merge(protected),
            DashboardAuthentication::Unconfigured => {
                return Err(Error::Config("dashboard authentication mode is not configured".into()));
            }
        };
        let router = match request_limits {
            Some(limits) => router.layer(axum::middleware::from_fn_with_state(limits, enforce_request_limits)),
            None => router,
        };
        // Outside the request limiter, so a refused request never holds a request slot, and applied before `/health`
        // is merged, so probes answer under any name.
        let router = router.layer(axum::middleware::from_fn_with_state(allowed_hosts, require_allowed_host));
        Ok(router
            .merge(health_route)
            .layer(axum::middleware::from_fn(reject_ambiguous_framing))
            .layer(axum::middleware::from_fn_with_state(self.secure_cookies, security_headers)))
    }
}

impl DashboardServer {
    /// Sets the total time allowed to read each HTTP request header. Default 10 seconds.
    pub fn header_read_timeout(mut self, timeout: Duration) -> Self {
        self.limits.header_read_timeout = timeout;
        self
    }

    /// Sets the deadline for one parsed request, including its body and any wait
    /// for a request slot. Default 30 seconds. It also sets the absolute lifetime of each connection starting when the
    /// application has prepared its first response, so a slow response reader cannot keep a connection slot by making
    /// occasional progress. Later keep-alive requests share the connection's remaining lifetime; one still executing
    /// when it ends is not cut off, but finishes, within one more such timeout, on a connection taking no further
    /// requests.
    ///
    /// The Retry and Abort actions stop waiting for a lock another transaction holds on their job a little before the
    /// deadline — a tenth of the timeout, and a second at most — and answer `503 Service Unavailable` with the job
    /// unchanged.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.limits.request_timeout = timeout;
        self
    }

    /// Sets the maximum accepted client connections. Default 256.
    pub fn max_connections(mut self, maximum: usize) -> Self {
        self.limits.max_connections = maximum;
        self
    }

    /// Sets the maximum requests executing at once across all connections. Default 128, clamped to
    /// [`DashboardServer::max_connections`] because no more requests than that can be in flight anyway.
    pub fn max_concurrent_requests(mut self, maximum: usize) -> Self {
        self.limits.max_concurrent_requests = maximum;
        self
    }

    /// Runs the dashboard until `SIGINT` or `SIGTERM`, then shuts down
    /// gracefully.
    ///
    /// Use [`DashboardServer::run_until`] when another component owns the
    /// shutdown signal.
    pub async fn run(self) -> Result<(), Error> {
        let token = CancellationToken::new();
        let run = self.run_until(token.clone());
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => result,
            _ = crate::worker::wait_for_shutdown_signal() => {
                token.cancel();
                run.await
            }
        }
    }

    /// Runs the dashboard until `shutdown` is cancelled.
    ///
    /// Dropping this future requests the same bounded graceful shutdown in a
    /// background task, making this the embeddable, test-friendly entry point.
    pub async fn run_until(self, shutdown: CancellationToken) -> Result<(), Error> {
        let dropped = CancellationToken::new();
        let drop_guard = dropped.clone().drop_guard();
        let result = tokio::spawn(self.run_until_inner(shutdown, dropped)).await?;
        drop_guard.disarm();
        result
    }

    async fn run_until_inner(self, shutdown: CancellationToken, dropped: CancellationToken) -> Result<(), Error> {
        let config = self.into_server_config(None)?;
        let bound = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = dropped.cancelled() => return Ok(()),
            bound = bind_dashboard_server(&config) => bound?,
        };
        let mut runtime = DashboardRuntime::start(bound);
        let error = tokio::select! {
            _ = crate::worker::wait_for_shutdown_or_drop(&shutdown, &dropped) => None,
            error = runtime.unexpected_exit() => Some(error),
        };
        match error {
            Some(error) => Err(error),
            None => runtime.finish_shutdown().await,
        }
    }

    /// Returns a handle that reports the actual address once the dashboard
    /// server task is running.
    ///
    /// ```no_run
    /// # async fn run(queue: pgqueue::Queue) -> anyhow::Result<()> {
    /// let dashboard = pgqueue::Dashboard::new([queue])
    ///     .allow_unauthenticated()
    ///     .serve_on("localhost", 0);
    /// let mut handle = dashboard.server_handle();
    /// let shutdown = tokio_util::sync::CancellationToken::new();
    /// let task = tokio::spawn(dashboard.run_until(shutdown.clone()));
    /// let address = handle.wait_until_ready().await;
    /// assert!(address.is_some());
    /// assert_eq!(handle.local_addr(), address);
    /// shutdown.cancel();
    /// task.await??;
    /// # Ok(())
    /// # }
    /// ```
    pub fn server_handle(&self) -> DashboardServerHandle {
        DashboardServerHandle { ready: self.ready.subscribe() }
    }

    pub(crate) fn into_server_config(
        self,
        worker_health: Option<WorkerHealth>,
    ) -> Result<DashboardServerConfig, Error> {
        if !self.dashboard.mount_path.trim_end_matches('/').is_empty() {
            return Err(Error::Config(
                "DashboardServer requires mount_path `/`; use Dashboard::router for a custom path".into(),
            ));
        }
        self.limits.validate()?;
        let request_limits = Arc::new(DashboardRequestLimits {
            timeout: self.limits.request_timeout,
            // A connection cap under the request cap is a cap of its own, so the request cap follows it down
            // instead of being refused against it: a lone `.max_connections(64)` sits under the default 128 and
            // would otherwise fail startup, taking any worker hosting the dashboard with it. The clamp is also
            // what keeps the permit count under `Semaphore::MAX_PERMITS`, which `validate` bounds
            // `max_connections` by and `Semaphore::new` panics above.
            requests: tokio::sync::Semaphore::new(self.limits.max_concurrent_requests.min(self.limits.max_connections)),
        });
        Ok(DashboardServerConfig {
            host: self.host,
            port: self.port,
            router: self.dashboard.router_with_health(worker_health, Some(request_limits))?,
            limits: self.limits,
            ready: self.ready,
        })
    }
}

impl DashboardServerHandle {
    /// The actual listening address, once the dashboard task is ready. For a host that resolved to several addresses,
    /// it is the first one bound; the others listen on the same port.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        *self.ready.borrow()
    }

    /// Waits for the dashboard task to start and returns its actual listening
    /// address, or `None` if the server exits before the dashboard is ready.
    pub async fn wait_until_ready(&mut self) -> Option<SocketAddr> {
        loop {
            let address = *self.ready.borrow_and_update();
            if address.is_some() {
                return address;
            }
            if self.ready.changed().await.is_err() {
                return None;
            }
        }
    }
}

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
struct DashboardServerLimits {
    header_read_timeout: Duration,
    request_timeout: Duration,
    max_connections: usize,
    max_concurrent_requests: usize,
}

impl Default for DashboardServerLimits {
    fn default() -> Self {
        Self {
            header_read_timeout: Duration::from_secs(10),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_connections: 256,
            max_concurrent_requests: 128,
        }
    }
}

impl DashboardServerLimits {
    fn validate(self) -> Result<(), Error> {
        for (name, timeout) in [
            ("dashboard header read timeout", self.header_read_timeout),
            ("dashboard request timeout", self.request_timeout),
        ] {
            if timeout.is_zero() {
                return Err(Error::Config(format!("{name} must be greater than zero")));
            }
            if Instant::now().checked_add(timeout).is_none() {
                return Err(Error::Config(format!("{name} is too large for the runtime clock")));
            }
        }
        if self.max_connections == 0 || self.max_connections > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(Error::Config(format!(
                "dashboard max connections must be between 1 and {}",
                tokio::sync::Semaphore::MAX_PERMITS
            )));
        }
        if self.max_concurrent_requests == 0 {
            return Err(Error::Config("dashboard max concurrent requests must be greater than zero".into()));
        }
        Ok(())
    }
}

struct DashboardRequestLimits {
    timeout: Duration,
    requests: tokio::sync::Semaphore,
}

pub(crate) struct DashboardServerConfig {
    host: String,
    port: u16,
    router: Router,
    limits: DashboardServerLimits,
    ready: tokio::sync::watch::Sender<Option<SocketAddr>>,
}

pub(crate) struct DashboardBoundServer {
    /// The first of `listeners`' addresses, which the server handle reports.
    bind: SocketAddr,
    /// One per address the configured host resolved to, every one on the same port.
    listeners: Vec<tokio::net::TcpListener>,
    router: Router,
    limits: DashboardServerLimits,
    ready: tokio::sync::watch::Sender<Option<SocketAddr>>,
}

pub(crate) struct DashboardRuntime {
    bind: SocketAddr,
    shutdown: CancellationToken,
    task: Option<JoinHandle<std::io::Result<()>>>,
}

pub(crate) async fn bind_dashboard(
    dashboard: Option<&DashboardServerConfig>,
) -> Result<Option<DashboardBoundServer>, Error> {
    let Some(dashboard) = dashboard else {
        return Ok(None);
    };
    Ok(Some(bind_dashboard_server(dashboard).await?))
}

async fn bind_dashboard_server(dashboard: &DashboardServerConfig) -> Result<DashboardBoundServer, Error> {
    let (bound, listeners) = bind_every_address(&dashboard.host, dashboard.port).await.map_err(Error::Dashboard)?;
    tracing::info!(
        dashboard.addrs = ?bound,
        configured.host = dashboard.host,
        configured.port = dashboard.port,
        "dashboard bound"
    );
    Ok(DashboardBoundServer {
        bind: bound[0],
        listeners,
        router: dashboard.router.clone(),
        limits: dashboard.limits,
        ready: dashboard.ready.clone(),
    })
}

/// Binds every address `host` resolves to, all on one port, so the dashboard answers on each of them.
///
/// Binding only the first that succeeded — `TcpListener::bind`'s own rule — served `localhost` on whichever family the
/// resolver listed first: `[::1]` on a typical Linux host, leaving `127.0.0.1` refused to every IPv4 client, an nginx
/// `proxy_pass http://127.0.0.1:...` among them. It also made a port conflict pass silently whenever the conflicting
/// server held only the first family: the second server bound the other one instead of failing, and which of the two
/// `localhost` reached depended on the client. Any local process could do the same, and serve its own login page on
/// the family the dashboard left free.
///
/// So a fixed port another server already holds on any of the addresses fails the bind, exactly as it does for a lone
/// address. An address that cannot be bound for any other reason — `::1` on a host with IPv6 disabled, say — is skipped
/// as long as another one binds, which is what the first-success rule tolerated. An ephemeral port is the one the first
/// listener was given, so every family shares it; a family where that port happens to be taken already is skipped.
///
/// Answers the bound addresses, never none, beside their listeners.
async fn bind_every_address(host: &str, port: u16) -> std::io::Result<(Vec<SocketAddr>, Vec<tokio::net::TcpListener>)> {
    let mut addresses = Vec::new();
    for address in tokio::net::lookup_host((host, port)).await? {
        if !addresses.contains(&address) {
            addresses.push(address);
        }
    }
    let mut bound: Vec<SocketAddr> = Vec::with_capacity(addresses.len());
    let mut listeners = Vec::with_capacity(addresses.len());
    let mut skipped = None;
    for mut address in addresses {
        if let Some(first) = bound.first() {
            address.set_port(first.port());
        }
        match tokio::net::TcpListener::bind(address).await {
            Ok(listener) => {
                bound.push(listener.local_addr()?);
                listeners.push(listener);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse && port != 0 => return Err(error),
            Err(error) => {
                tracing::debug!(%address, %error, "dashboard skipped an address its host resolved to");
                skipped.get_or_insert(error);
            }
        }
    }
    if bound.is_empty() {
        return Err(skipped.unwrap_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, format!("{host:?} resolved to no address"))
        }));
    }
    Ok((bound, listeners))
}

/// The next connection on any of `listeners`. Each accept is cancel-safe, so those that lose the race drop nothing.
async fn accept_any(listeners: &mut [tokio::net::TcpListener]) -> (tokio::net::TcpStream, SocketAddr) {
    let mut accepts = listeners.iter_mut().map(|listener| Box::pin(AxumListener::accept(listener))).collect::<Vec<_>>();
    std::future::poll_fn(|context| {
        accepts
            .iter_mut()
            .find_map(|accept| match accept.as_mut().poll(context) {
                std::task::Poll::Ready(accepted) => Some(accepted),
                std::task::Poll::Pending => None,
            })
            .map_or(std::task::Poll::Pending, std::task::Poll::Ready)
    })
    .await
}

impl DashboardRuntime {
    pub(crate) fn start(bound: DashboardBoundServer) -> Self {
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let bind = bound.bind;
        let task = tokio::spawn(async move {
            bound.ready.send_replace(Some(bind));
            tracing::info!(dashboard.addr = %bind, "dashboard ready");
            serve_dashboard(bound, server_shutdown).await
        });
        Self { bind, shutdown, task: Some(task) }
    }

    fn begin_shutdown(&self) {
        if !self.shutdown.is_cancelled() {
            tracing::info!(dashboard.addr = %self.bind, "dashboard shutting down");
            self.shutdown.cancel();
        }
    }

    async fn wait(&mut self) -> Result<(), Error> {
        let result = match self.task.as_mut() {
            Some(task) => task.await,
            None => return Ok(()),
        };
        self.task = None;
        dashboard_task_result(result)
    }

    async fn unexpected_exit(&mut self) -> Error {
        match self.wait().await {
            Ok(()) => Error::Dashboard(std::io::Error::other("dashboard server stopped unexpectedly")),
            Err(error) => error,
        }
    }

    pub(crate) async fn finish_shutdown(&mut self) -> Result<(), Error> {
        self.begin_shutdown();
        let result = match self.task.as_mut() {
            Some(task) => tokio::time::timeout(SHUTDOWN_TIMEOUT, task).await,
            None => return Ok(()),
        };
        match result {
            Ok(result) => {
                self.task = None;
                dashboard_task_result(result)
            }
            Err(_) => {
                tracing::warn!(
                    dashboard.addr = %self.bind,
                    timeout = ?SHUTDOWN_TIMEOUT,
                    "dashboard graceful shutdown timed out; aborting server task"
                );
                if let Some(task) = self.task.take() {
                    task.abort();
                    let _ = task.await;
                }
                Ok(())
            }
        }
    }
}

async fn serve_dashboard(bound: DashboardBoundServer, shutdown: CancellationToken) -> std::io::Result<()> {
    let DashboardBoundServer { mut listeners, router, limits, .. } = bound;
    let connections = Arc::new(tokio::sync::Semaphore::new(limits.max_connections));
    let mut tasks = tokio::task::JoinSet::new();

    loop {
        let permit = tokio::select! {
            _ = shutdown.cancelled() => break,
            permit = Arc::clone(&connections).acquire_owned() => permit.map_err(|_| {
                std::io::Error::other("dashboard connection limiter closed unexpectedly")
            })?,
        };
        // Axum's listener adapter retries connection errors and backs off on resource errors. A raw accept error must
        // not stop a worker-hosted dashboard.
        let accepted = tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = accept_any(&mut listeners) => accepted,
        };
        let (stream, peer) = accepted;
        let router = router.clone();
        let connection_shutdown = shutdown.clone();
        tasks.spawn(async move {
            let _permit = permit;
            if let Err(error) = serve_dashboard_connection(stream, peer, router, limits, connection_shutdown).await {
                tracing::debug!(%peer, %error, "dashboard connection closed with an error");
            }
        });
        while let Some(result) = tasks.try_join_next() {
            report_dashboard_connection_task(result);
        }
    }

    while let Some(result) = tasks.join_next().await {
        report_dashboard_connection_task(result);
    }
    Ok(())
}

fn report_dashboard_connection_task(result: Result<(), tokio::task::JoinError>) {
    if let Err(error) = result {
        tracing::error!(%error, "dashboard connection task failed; continuing to serve");
    }
}

async fn serve_dashboard_connection(
    stream: tokio::net::TcpStream,
    peer: SocketAddr,
    router: Router,
    limits: DashboardServerLimits,
    shutdown: CancellationToken,
) -> Result<(), hyper::Error> {
    let first_response = CancellationToken::new();
    let response_ready = first_response.clone();
    // Handlers running on this connection right now, as opposed to responses merely being written out.
    let executing = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let handlers = Arc::clone(&executing);
    let service = hyper::service::service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
        let router = router.clone();
        let response_ready = response_ready.clone();
        let handlers = Arc::clone(&handlers);
        async move {
            if has_ambiguous_framing(request.headers()) {
                response_ready.cancel();
                return Ok::<_, Infallible>(ambiguous_framing_response());
            }
            let _executing = ExecutingHandler::enter(handlers);
            let mut request = request.map(axum::body::Body::new);
            request.extensions_mut().insert(ConnectInfo(peer));
            let response = router.oneshot(request).await;
            response_ready.cancel();
            response
        }
    });
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder.timer(hyper_util::rt::TokioTimer::new()).header_read_timeout(limits.header_read_timeout);
    let connection = builder.serve_connection(hyper_util::rt::TokioIo::new(stream), service);
    tokio::pin!(connection);
    let connection_deadline = async move {
        first_response.cancelled().await;
        tokio::time::sleep(limits.request_timeout).await;
    };
    tokio::pin!(connection_deadline);
    tokio::select! {
        result = connection.as_mut() => result,
        _ = &mut connection_deadline => {
            // Nothing is executing, so all that is left is writing responses out — the slow reader this deadline
            // exists to cut off — or waiting for another request: dropped at once, as before.
            if executing.load(std::sync::atomic::Ordering::Acquire) == 0 {
                return Ok(());
            }
            // A handler mid-execution is not cut off. Dropping the connection cancelled it wherever it stood, so a
            // Retry or Abort could commit and lose its answer, and a browser resending the post then read its own
            // committed action back as refused. The connection takes no further request, and the one in flight —
            // already bounded by the request deadline — gets one more request timeout to finish and be written.
            connection.as_mut().graceful_shutdown();
            tokio::time::timeout(limits.request_timeout, connection.as_mut()).await.unwrap_or(Ok(()))
        }
        _ = shutdown.cancelled() => {
            connection.as_mut().graceful_shutdown();
            connection.await
        }
    }
}

/// Counts one handler as executing on its connection for as long as it lives, however its future ends.
struct ExecutingHandler(Arc<std::sync::atomic::AtomicUsize>);

impl ExecutingHandler {
    fn enter(count: Arc<std::sync::atomic::AtomicUsize>) -> Self {
        count.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Self(count)
    }
}

impl Drop for ExecutingHandler {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// Whether a request's framing may have been ambiguous on the wire: any `Transfer-Encoding` at all, not only one beside
/// a `Content-Length`.
///
/// hyper keeps a `Content-Length` that precedes the `Transfer-Encoding`, but silently drops one that follows it, so
/// after parsing, a request carrying both in that order is indistinguishable from a plain chunked one — and checking
/// for both let exactly that order through, to be answered on a connection kept open behind a proxy that may have
/// framed it by the length. No route here takes a body a client would stream.
fn has_ambiguous_framing(headers: &HeaderMap) -> bool {
    headers.contains_key(header::TRANSFER_ENCODING)
}

fn ambiguous_framing_response() -> Response {
    let mut response = (StatusCode::BAD_REQUEST, "ambiguous HTTP request framing").into_response();
    response.headers_mut().insert(header::CONNECTION, HeaderValue::from_static("close"));
    response
}

async fn reject_ambiguous_framing(request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    if has_ambiguous_framing(request.headers()) {
        return ambiguous_framing_response();
    }
    next.run(request).await
}

#[cfg(test)]
mod dashboard_server_tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Notify;

    use super::*;

    async fn panic_after_entering(entered: Arc<Notify>) -> &'static str {
        entered.notify_one();
        panic!("intentional dashboard connection panic");
    }

    /// The name a server is bound by is one its clients use, so the host check allows it without it being listed again.
    #[test]
    fn test_serve_on_allows_the_host_it_binds() {
        let server = Dashboard::new(Vec::new()).serve_on("dash.internal", 0);
        let allowed_hosts = allowed_host_names(&server.dashboard.allowed_hosts).unwrap();
        assert!(is_allowed_host("dash.internal:8080", &allowed_hosts));
        assert!(!is_allowed_host("other.internal:8080", &allowed_hosts));
    }

    #[tokio::test]
    async fn test_connection_panic_does_not_stop_dashboard_server() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let entered = Arc::new(Notify::new());
        let handler_entered = Arc::clone(&entered);
        let router = Router::new()
            .route("/panic", get(move || panic_after_entering(Arc::clone(&handler_entered))))
            .route("/health", get(|| async { "OK" }));
        let (ready, _) = tokio::sync::watch::channel(None);
        let bound = DashboardBoundServer {
            bind: address,
            listeners: vec![listener],
            router,
            limits: DashboardServerLimits::default(),
            ready,
        };
        let shutdown = CancellationToken::new();
        let run = tokio::spawn(serve_dashboard(bound, shutdown.clone()));

        let mut panicking = tokio::net::TcpStream::connect(address).await.unwrap();
        panicking
            .write_all(format!("GET /panic HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n").as_bytes())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered.notified()).await.unwrap();
        drop(panicking);

        let mut healthy = tokio::net::TcpStream::connect(address).await.unwrap();
        healthy
            .write_all(format!("GET /health HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = String::new();
        tokio::time::timeout(Duration::from_secs(1), healthy.read_to_string(&mut response)).await.unwrap().unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), run).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_standalone_server_rejects_ambiguous_request_framing() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handled = Arc::clone(&requests);
        let router = Router::new().route(
            "/handled",
            get(move || {
                handled.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async { "OK" }
            }),
        );
        let (ready, _) = tokio::sync::watch::channel(None);
        let bound = DashboardBoundServer {
            bind: address,
            listeners: vec![listener],
            router,
            limits: DashboardServerLimits::default(),
            ready,
        };
        let shutdown = CancellationToken::new();
        let run = tokio::spawn(serve_dashboard(bound, shutdown.clone()));

        // Both orders. hyper drops a `Content-Length` that follows the `Transfer-Encoding` before any handler can see
        // it, so the second reached the handler — and kept the connection open for the smuggled request behind it —
        // while a check for both headers stayed green.
        for framing in [
            "Content-Length: 4\r\nTransfer-Encoding: chunked",
            "Transfer-Encoding: chunked\r\nContent-Length: 4",
        ] {
            let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
            stream
                .write_all(
                    format!(
                        "POST /handled HTTP/1.1\r\nHost: {address}\r\n{framing}\r\n\
                         Connection: keep-alive\r\n\r\n0\r\n\r\n\
                         GET /handled HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let mut response = String::new();
            tokio::time::timeout(Duration::from_secs(1), stream.read_to_string(&mut response)).await.unwrap().unwrap();
            assert!(response.starts_with("HTTP/1.1 400"), "{framing:?}: {response}");
            assert_eq!(response.matches("HTTP/1.1 ").count(), 1, "{framing:?}: the connection was kept open");
        }
        assert_eq!(requests.load(std::sync::atomic::Ordering::SeqCst), 0, "an ambiguous request reached its handler");

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), run).await.unwrap().unwrap().unwrap();
    }
}

pub(crate) async fn wait_for_dashboard_exit(dashboard: &mut Option<DashboardRuntime>) -> Error {
    match dashboard {
        Some(dashboard) => dashboard.unexpected_exit().await,
        None => std::future::pending().await,
    }
}

fn dashboard_task_result(result: Result<std::io::Result<()>, tokio::task::JoinError>) -> Result<(), Error> {
    match result {
        Ok(Ok(())) => Ok(()),
        // Axum 0.8 handles accept errors internally, but retain the typed
        // mapping in case a future server implementation returns one.
        Ok(Err(error)) => Err(Error::Dashboard(error)),
        Err(error) => Err(Error::Dashboard(std::io::Error::other(error))),
    }
}

// Dashboard API

const MAX_PAGE_SIZE: i64 = 100;
const JOB_NAME_SUGGESTION_LIMIT: i64 = 20;
const ALL_STATUSES: [JobStatus; 6] = [
    JobStatus::Queued,
    JobStatus::Running,
    JobStatus::Complete,
    JobStatus::Failed,
    JobStatus::Aborting,
    JobStatus::Aborted,
];

/// API failure: infrastructure errors become 500s, malformed requests 400s,
/// lookups 404s, rejected state-changing requests 403s, throttled requests
/// 429s, and a shared round that could not answer in time a 503, as does an
/// action whose job stayed locked by another transaction until its deadline.
pub(crate) enum DashboardApiError {
    BadRequest(&'static str),
    NotFound(&'static str),
    Forbidden(&'static str),
    TooManyRequests(&'static str),
    Unavailable(&'static str),
    /// A shared [`RoundCache`] round failed. The round logged its own error —
    /// once, rather than once per caller riding it — so this only renders the
    /// same 500 an unshared query would have produced.
    RoundFailed,
    Internal(Error),
}

impl From<Error> for DashboardApiError {
    fn from(error: Error) -> Self {
        match error {
            Error::JobNotFound(_) => DashboardApiError::NotFound("job not found"),
            other => DashboardApiError::Internal(other),
        }
    }
}

impl IntoResponse for DashboardApiError {
    fn into_response(self) -> Response {
        match self {
            DashboardApiError::BadRequest(what) => {
                (StatusCode::BAD_REQUEST, Json(json!({ "error": what }))).into_response()
            }
            DashboardApiError::NotFound(what) => {
                (StatusCode::NOT_FOUND, Json(json!({ "error": what }))).into_response()
            }
            DashboardApiError::Forbidden(what) => {
                (StatusCode::FORBIDDEN, Json(json!({ "error": what }))).into_response()
            }
            DashboardApiError::TooManyRequests(what) => {
                (StatusCode::TOO_MANY_REQUESTS, [(header::RETRY_AFTER, "1")], Json(json!({ "error": what })))
                    .into_response()
            }
            DashboardApiError::Unavailable(what) => {
                (StatusCode::SERVICE_UNAVAILABLE, [(header::RETRY_AFTER, "1")], Json(json!({ "error": what })))
                    .into_response()
            }
            DashboardApiError::RoundFailed => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": "internal server error" }))).into_response()
            }
            DashboardApiError::Internal(error) => {
                tracing::error!(%error, "dashboard api error");
                (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": "internal server error" }))).into_response()
            }
        }
    }
}

fn require_action_header(headers: &HeaderMap) -> Result<(), DashboardApiError> {
    if headers.get(ACTION_HEADER).is_some_and(|value| value.as_bytes() == ACTION_HEADER_VALUE) {
        Ok(())
    } else {
        Err(DashboardApiError::Forbidden("missing dashboard action header"))
    }
}

/// Whether the browser says this post came from somewhere other than the
/// dashboard itself.
///
/// [`require_action_header`] cannot guard the login form: it is a real
/// `<form method="post">`, so nothing of ours runs before the browser sends it
/// and no request header can be attached. Its
/// `application/x-www-form-urlencoded` body is a CORS-simple content type, so
/// any page the operator visits can post it with no preflight — and every post
/// spends one comparison from the *victim's* interactive budget before
/// anything is compared, keyed to the victim's own address. Enough concurrent
/// posts and the operator's correct password is answered `429` on their own
/// dashboard, however privately it is bound.
///
/// `Sec-Fetch-Site` is set by the browser and forbidden to scripts, so the
/// attacking page cannot forge it. `same-site` is *not* accepted: the login
/// form is served by the dashboard itself, so a genuine submission is always
/// `same-origin`, and anything else is a sibling origin posting credentials at
/// us, which is the vector.
///
/// Without Fetch Metadata — a legacy browser, or an intermediary that stripped
/// it — `Origin` is the fallback those same browsers still attach to a
/// cross-origin POST, and it is equally forbidden to scripts: a post whose
/// `Origin` names a different authority than `Host` (or the opaque `null` a
/// sandboxed context sends) is refused the same way. The comparison is by
/// authority alone, because a dashboard behind a TLS-terminating proxy cannot
/// know the scheme its clients used. A client that sends *neither* header — a
/// curl, a password manager, a script driving the form — is not a browser, and
/// is not a cross-site request either: refusing those would break every
/// non-browser client to no benefit, since anything that can omit both headers
/// can equally forge them.
fn is_cross_site_post(headers: &HeaderMap, uri: &axum::http::Uri) -> bool {
    if let Some(site) = headers.get(SITE_HEADER) {
        return !matches!(site.as_bytes(), b"same-origin" | b"none");
    }
    let Some(origin) = headers.get(header::ORIGIN).and_then(|value| value.to_str().ok()) else {
        return false;
    };
    // HTTP/2 carries the authority in the request target rather than a Host
    // header, so that is where it is read without one, exactly as
    // `require_allowed_host` reads it. Skipping the comparison instead let a
    // foreign `Origin` through on every h2 request from a browser without Fetch
    // Metadata. With neither there is nothing to compare against, and the post
    // falls back to the pre-Fetch-Metadata behavior rather than refusing a
    // legitimate login.
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .or_else(|| uri.authority().map(axum::http::uri::Authority::as_str));
    let Some(host) = host else {
        return false;
    };
    !origin_names_host(origin, host)
}

/// Whether `origin`'s authority names `host`, with each scheme's default port
/// treated as absent on both sides so `https://x` matches `Host: x:443`.
/// An origin naming no authority — `null`, or any other opaque form — names
/// nothing, and so never matches.
fn origin_names_host(origin: &str, host: &str) -> bool {
    let Some((scheme, authority)) = origin.split_once("://") else {
        return false;
    };
    let default_port = match scheme {
        "http" => Some(":80"),
        "https" => Some(":443"),
        _ => None,
    };
    let canonical = |value: &str| {
        let value = value.trim();
        default_port.and_then(|port| value.strip_suffix(port)).unwrap_or(value).to_ascii_lowercase()
    };
    !authority.is_empty() && canonical(authority) == canonical(host)
}

/// The names [`Dashboard::allowed_hosts`] lists, as [`is_allowed_host`] compares them. An entry that is not a bare
/// host name or IP address can never match a `Host` header — `https://dashboard.example.com` or
/// `dashboard.example.com:8443` would leave the dashboard refusing the very clients it was listed for — so it fails
/// the build instead.
fn allowed_host_names(hosts: &[String]) -> Result<Arc<[String]>, Error> {
    hosts
        .iter()
        .map(|host| {
            let name = canonical_host_name(host);
            let is_name = !name.is_empty()
                && name.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_'));
            if is_name || name.parse::<IpAddr>().is_ok() {
                Ok(name)
            } else {
                Err(Error::Config(format!(
                    "dashboard host {host:?} must be a bare host name or IP address, without a scheme or port"
                )))
            }
        })
        .collect()
}

/// Refuses a request addressed to a host name the dashboard was not given; [`Dashboard::allowed_hosts`] describes the
/// DNS rebinding this blocks. A request that names no host at all comes from no browser, and rebinding needs one:
/// browsers always send `Host` over HTTP/1.1 and an authority over HTTP/2.
async fn require_allowed_host(
    State(allowed_hosts): State<Arc<[String]>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let allowed = match request.headers().get(header::HOST) {
        Some(host) => host.to_str().is_ok_and(|host| is_allowed_host(host, &allowed_hosts)),
        None => request.uri().authority().is_none_or(|authority| is_allowed_host(authority.as_str(), &allowed_hosts)),
    };
    if !allowed {
        return DashboardApiError::Forbidden("this host name is not allowed; list it with Dashboard::allowed_hosts")
            .into_response();
    }
    next.run(request).await
}

/// Whether `authority`, a `Host` header or an HTTP/2 request's authority, names an IP address, `localhost`, or one of
/// `allowed_hosts`. The port is ignored: a rebinding page reaches the dashboard on the dashboard's own port.
fn is_allowed_host(authority: &str, allowed_hosts: &[String]) -> bool {
    // An IPv6 literal is bracketed, so that its colons are not read as a port.
    if let Some(literal) = authority.strip_prefix('[') {
        return literal.split_once(']').is_some_and(|(address, _)| address.parse::<Ipv6Addr>().is_ok());
    }
    let host = match authority.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit()) => host,
        _ => authority,
    };
    if host.parse::<Ipv4Addr>().is_ok() {
        return true;
    }
    let name = canonical_host_name(host);
    name == "localhost" || allowed_hosts.contains(&name)
}

/// A host name as the allowlist compares it: lowercase, and without the trailing dot of a fully qualified name.
fn canonical_host_name(host: &str) -> String {
    host.strip_suffix('.').unwrap_or(host).to_ascii_lowercase()
}

fn queue_of(state: &DashboardState, name: &str) -> Result<Queue, DashboardApiError> {
    state
        .queues
        .iter()
        .find(|queue| queue.name() == name)
        .cloned()
        .ok_or(DashboardApiError::NotFound("queue not found"))
}

pub(crate) async fn health(State(state): State<Arc<DashboardState>>) -> Response {
    // A degraded component is deliberately survivable, but readiness still
    // requires database access: degradation must not hide a worker that can no
    // longer reach its queue.
    let degraded = if let Some(status) = state.worker_health.as_ref().map(|health| health.snapshot().status) {
        match status {
            WorkerHealthStatus::Starting | WorkerHealthStatus::Stopped => {
                return (StatusCode::SERVICE_UNAVAILABLE, "unhealthy").into_response();
            }
            WorkerHealthStatus::Degraded => true,
            WorkerHealthStatus::Ready => false,
        }
    } else {
        false
    };
    // Single-flight. Everything that loses the race waits on the same round
    // rather than opening a probe of its own, so a cold cache plus a slow probe
    // costs one pooled connection per queue in total instead of one per
    // request. The clone is inside the closure, as it is in `list_queues`: a
    // cache hit — which is the overwhelmingly common case, since the round is
    // reused for its whole TTL — never calls it, and so never pays for it.
    let probed = state
        .health_probe
        .get(|| {
            let queues = state.queues.clone();
            async move {
                let mut probes = tokio::task::JoinSet::new();
                for queue in queues {
                    probes.spawn(async move { queue.database().dashboard_probe().await });
                }
                let mut healthy = true;
                while let Some(result) = probes.join_next().await {
                    healthy &= matches!(result, Ok(Ok(())));
                }
                healthy
            }
        })
        .await;
    match probed {
        Some(healthy) => health_response(healthy, degraded),
        // A round that has not answered inside `ROUND_WAIT_TIMEOUT` is a
        // database this dashboard cannot reach *yet*, which is not the same
        // claim as `unhealthy` — and an orchestrator can retry a 503.
        None => (StatusCode::SERVICE_UNAVAILABLE, "unavailable").into_response(),
    }
}

fn health_response(healthy: bool, degraded: bool) -> Response {
    if healthy {
        if degraded { (StatusCode::OK, "DEGRADED").into_response() } else { (StatusCode::OK, "OK").into_response() }
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, "unhealthy").into_response()
    }
}

pub(crate) async fn list_queues(State(state): State<Arc<DashboardState>>) -> Result<Response, DashboardApiError> {
    // Cached and gated exactly like `/health`: one fan-out at a time, reused
    // for `QUEUE_SIGNALS_TTL`, and detached so a client that walks away cannot
    // leave the next one to pay for the round again.
    let signals = state.queue_signals.get(|| queue_signals_round(state.queues.clone())).await;
    match signals {
        Some(Some(queues)) => Ok(Json(json!({ "queues": queues })).into_response()),
        Some(None) => Err(DashboardApiError::RoundFailed),
        None => Err(DashboardApiError::Unavailable("queue signals unavailable")),
    }
}

/// One `/api/queues` fan-out: the per-queue signal query for every configured
/// queue, or `None` if any of them failed.
///
/// Spawned up front so the queries overlap, then awaited in order: the response
/// follows the configured queue order without an index-tagged result set to
/// reassemble. The round is itself detached, so no request dropping out from
/// under it can cut the fan-out short.
///
/// The handles are abort-on-drop because the round returns on the first error
/// with the remaining ones unawaited. A bare `JoinHandle` *detaches* on drop
/// rather than cancelling, so every still-running sibling kept a pooled
/// connection long after the round it belonged to had already answered —
/// against the pool the worker dequeues, heartbeats and finalizes with.
async fn queue_signals_round(queues: Vec<Queue>) -> Option<Vec<DashboardQueueSignals>> {
    let tasks: Vec<_> = queues
        .into_iter()
        .map(|queue| {
            tokio_util::task::AbortOnDropHandle::new(tokio::spawn(
                async move { queue.database().dashboard_signals().await },
            ))
        })
        .collect();
    let mut signals = Vec::with_capacity(tasks.len());
    for task in tasks {
        match task.await.map_err(Error::from).and_then(|signals| signals) {
            Ok(queue) => signals.push(queue),
            // Logged here, once per round, rather than once per waiter: every
            // caller riding this round reports the same failure.
            Err(error) => {
                tracing::error!(%error, "dashboard api error");
                return None;
            }
        }
    }
    Some(signals)
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DashboardJobsQuery {
    status: Option<String>,
    name: Option<String>,
    kind: Option<String>,
    limit: Option<i64>,
    cursor_enqueued_at: Option<Timestamp>,
    cursor_id: Option<Uuid>,
}

struct DashboardFilteredJobsQuery {
    statuses: Vec<JobStatus>,
    name: Option<String>,
    kind: String,
    limit: i64,
    cursor: Option<(Timestamp, Uuid)>,
}

/// The statuses a `status=a,b` filter names, or all of them when it names none.
/// Shared by the job listing and the name typeahead so a suggestion cannot
/// offer a name that the listing beside it then filters away.
fn filter_statuses(status: Option<&str>) -> Result<Vec<JobStatus>, DashboardApiError> {
    let mut statuses = Vec::new();
    if let Some(value) = status {
        for value in value.split(',').map(str::trim).filter(|value| !value.is_empty()) {
            let status = value.parse::<JobStatus>().map_err(|_| DashboardApiError::BadRequest("unknown status"))?;
            if !statuses.contains(&status) {
                statuses.push(status);
            }
        }
    }
    if statuses.is_empty() {
        statuses.extend(ALL_STATUSES);
    }
    Ok(statuses)
}

impl DashboardJobsQuery {
    fn filter(self) -> Result<DashboardFilteredJobsQuery, DashboardApiError> {
        let statuses = filter_statuses(self.status.as_deref())?;
        let kind = job_kind(self.kind.as_deref())?.to_string();
        let cursor = cursor_pair(self.cursor_enqueued_at, self.cursor_id)?;
        let name = self.name.filter(|name| !name.is_empty());
        if name.as_ref().is_some_and(|name| name.len() > 255) {
            return Err(DashboardApiError::BadRequest("job name is too long"));
        }
        // `%00` decodes into the `String` like any other byte, and PostgreSQL
        // `text` cannot hold it (`22021`). Left to reach the query it came back
        // as an `Internal`: a 500 and an error-level log for a request this
        // type promises to 400, having burned a pooled connection to find out.
        if name.as_ref().is_some_and(|name| name.contains('\0')) {
            return Err(DashboardApiError::BadRequest("job name must not contain NUL"));
        }
        Ok(DashboardFilteredJobsQuery { statuses, name, kind, limit: page_limit(self.limit), cursor })
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DashboardWorkersQuery {
    limit: Option<i64>,
    cursor_started_at: Option<Timestamp>,
    cursor_id: Option<Uuid>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DashboardJobNamesQuery {
    kind: Option<String>,
    prefix: Option<String>,
    status: Option<String>,
}

fn cursor_pair(timestamp: Option<Timestamp>, id: Option<Uuid>) -> Result<Option<(Timestamp, Uuid)>, DashboardApiError> {
    match (timestamp, id) {
        (None, None) => Ok(None),
        (Some(timestamp), Some(id)) => {
            // `Timestamp` reaches ISO year -9999, so every cursor between there
            // and PostgreSQL's floor deserialized, reached the query and
            // came back as `22008` -> `Internal`: a 500 and an error-level log
            // for a request this type promises to 400, having burned a pooled
            // connection to find out. Same class as the `%00` name filter above,
            // and checked here because both paged endpoints funnel through it.
            if timestamp < MIN_TIMESTAMPTZ {
                return Err(DashboardApiError::BadRequest("page cursor timestamp is out of range"));
            }
            Ok(Some((timestamp, id)))
        }
        _ => Err(DashboardApiError::BadRequest("incomplete page cursor")),
    }
}

fn page_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(50).clamp(1, MAX_PAGE_SIZE)
}

fn job_kind(kind: Option<&str>) -> Result<&str, DashboardApiError> {
    match kind {
        None | Some("") => Ok("job"),
        Some(kind @ ("job" | "cron")) => Ok(kind),
        Some(_) => Err(DashboardApiError::BadRequest("unknown job kind")),
    }
}

/// Shared cursor-pagination epilogue: the page was fetched with `limit + 1`,
/// so trim the probe row and, when it existed, project the last visible item
/// into the response's `next_cursor`.
fn next_cursor<T>(items: &mut Vec<T>, limit: i64, cursor: impl Fn(&T) -> Value) -> Option<Value> {
    let Ok(limit) = usize::try_from(limit) else {
        return None;
    };
    if items.len() <= limit {
        return None;
    }
    items.pop();
    items.last().map(cursor)
}

pub(crate) async fn list_jobs(
    State(state): State<Arc<DashboardState>>,
    Path(name): Path<String>,
    Query(query): Query<DashboardJobsQuery>,
) -> Result<Response, DashboardApiError> {
    let queue = queue_of(&state, &name)?;
    let DashboardFilteredJobsQuery { statuses, name, kind, limit, cursor } = query.filter()?;
    let mut jobs = queue.database().dashboard_jobs_page(&statuses, &kind, name.as_deref(), cursor, limit + 1).await?;
    let next_cursor = next_cursor(&mut jobs, limit, |job| {
        json!({
            "enqueued_at": job.enqueued_at,
            "id": job.id,
        })
    });
    Ok(Json(json!({
        "jobs": jobs,
        "next_cursor": next_cursor,
    }))
    .into_response())
}

pub(crate) async fn list_workers(
    State(state): State<Arc<DashboardState>>,
    Path(name): Path<String>,
    Query(query): Query<DashboardWorkersQuery>,
) -> Result<Response, DashboardApiError> {
    let queue = queue_of(&state, &name)?;
    let limit = page_limit(query.limit);
    let cursor = cursor_pair(query.cursor_started_at, query.cursor_id)?;
    let mut workers = queue.database().dashboard_workers_page(cursor, limit + 1).await?;
    let next_cursor = next_cursor(&mut workers, limit, |row| {
        json!({
            "started_at": row.worker.started_at,
            "id": row.worker.id,
        })
    });
    Ok(Json(json!({
        "workers": workers,
        "next_cursor": next_cursor,
    }))
    .into_response())
}

pub(crate) async fn list_job_names(
    State(state): State<Arc<DashboardState>>,
    Path(name): Path<String>,
    Query(query): Query<DashboardJobNamesQuery>,
) -> Result<Response, DashboardApiError> {
    let queue = queue_of(&state, &name)?;
    let kind = job_kind(query.kind.as_deref())?;
    // The suggestions have to answer the same question the listing beside them
    // does. Ignoring the status filter offered names that exist only under some
    // other status, and choosing one rendered "No jobs found".
    let statuses = filter_statuses(query.status.as_deref())?;
    let prefix = query.prefix.unwrap_or_default();
    if prefix.len() > 255 {
        return Err(DashboardApiError::BadRequest("job name prefix is too long"));
    }
    // Same as the `name` filter: a NUL is a malformed request, not a 500.
    if prefix.contains('\0') {
        return Err(DashboardApiError::BadRequest("job name prefix must not contain NUL"));
    }
    let names = if prefix.is_empty() {
        Vec::new()
    } else {
        queue.database().dashboard_job_names(&statuses, kind, &prefix, JOB_NAME_SUGGESTION_LIMIT).await?
    };
    Ok(Json(json!({ "names": names })).into_response())
}

pub(crate) async fn worker_detail(
    State(state): State<Arc<DashboardState>>,
    Path((name, id)): Path<(String, Uuid)>,
) -> Result<Response, DashboardApiError> {
    let queue = queue_of(&state, &name)?;
    let worker = queue.database().dashboard_worker(id).await?.ok_or(DashboardApiError::NotFound("worker not found"))?;
    Ok(Json(json!({ "worker": worker })).into_response())
}

pub(crate) async fn job_detail(
    State(state): State<Arc<DashboardState>>,
    Path((name, id)): Path<(String, Uuid)>,
) -> Result<Response, DashboardApiError> {
    let queue = queue_of(&state, &name)?;
    let job = queue.database().dashboard_job(id).await?.ok_or(DashboardApiError::NotFound("job not found"))?;
    Ok(Json(json!({ "job": job })).into_response())
}

pub(crate) async fn retry_job(
    State(state): State<Arc<DashboardState>>,
    Path((name, id)): Path<(String, Uuid)>,
    deadline: Option<Extension<ActionDeadline>>,
    headers: HeaderMap,
) -> Result<Response, DashboardApiError> {
    require_action_header(&headers)?;
    let queue = queue_of(&state, &name)?;
    let job_id = queue
        .database()
        .retry_job_occurrence_within(id, "retried from dashboard", state.action_deadline(deadline))
        .await
        .map_err(|error| {
            action_error(error, "the job or its dedupe key is locked by another transaction; try again")
        })?;
    Ok(Json(json!({ "retried": job_id.is_some(), "job_id": job_id })).into_response())
}

pub(crate) async fn abort_job(
    State(state): State<Arc<DashboardState>>,
    Path((name, id)): Path<(String, Uuid)>,
    deadline: Option<Extension<ActionDeadline>>,
    headers: HeaderMap,
) -> Result<Response, DashboardApiError> {
    require_action_header(&headers)?;
    let queue = queue_of(&state, &name)?;
    let aborted = queue
        .database()
        .abort_within(id, "aborted from dashboard", state.action_deadline(deadline))
        .await
        .map_err(|error| action_error(error, "the job is locked by another transaction; try again"))?;
    Ok(Json(json!({ "aborted": aborted })).into_response())
}

/// What an action answers when the bound [`ActionDeadline`] puts on its lock waits ended it, which leaves the job
/// unchanged. A wait refused there, or by a shorter `lock_timeout` of the session's own, is answered with `locked`,
/// which names the cause and says that trying again can succeed; as a 500 it read as a fault. A deadline already gone
/// before the statement was sent is answered as the timeout it is.
fn action_error(error: Error, locked: &'static str) -> DashboardApiError {
    if is_lock_refusal(&error) {
        return DashboardApiError::Unavailable(locked);
    }
    match error {
        Error::WaitTimeout => DashboardApiError::Unavailable("dashboard request timed out"),
        error => DashboardApiError::from(error),
    }
}

#[cfg(test)]
mod dashboard_api_tests {
    use super::*;

    #[test]
    fn test_jobs_query_clamps_page_size() {
        let query = DashboardJobsQuery {
            status: None,
            name: None,
            kind: None,
            limit: Some(MAX_PAGE_SIZE + 1),
            cursor_enqueued_at: None,
            cursor_id: None,
        };
        let Ok(filter) = query.filter() else {
            panic!("valid jobs query should produce a filter");
        };
        assert_eq!(filter.limit, MAX_PAGE_SIZE);
        assert_eq!(filter.statuses, ALL_STATUSES);
        assert_eq!(filter.kind, "job");

        let query = DashboardJobsQuery {
            status: None,
            name: None,
            kind: None,
            limit: Some(0),
            cursor_enqueued_at: None,
            cursor_id: None,
        };
        let Ok(filter) = query.filter() else {
            panic!("valid jobs query should produce a filter");
        };
        assert_eq!(filter.limit, 1);
        assert_eq!(page_limit(Some(MAX_PAGE_SIZE + 1)), MAX_PAGE_SIZE);
        assert_eq!(page_limit(Some(0)), 1);
    }

    #[test]
    fn test_cursor_requires_timestamp_and_id() {
        let error = cursor_pair(Some(Timestamp::now()), None);
        assert!(matches!(error, Err(DashboardApiError::BadRequest(_))));
        let error = cursor_pair(None, Some(Uuid::now_v7()));
        assert!(matches!(error, Err(DashboardApiError::BadRequest(_))));
    }
}

// Embedded dashboard files

static DASHBOARD: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/dashboard");

fn render_index(root: &str, username: &str, auth_enabled: bool) -> axum::body::Bytes {
    let root = html_attr_escape(root);
    let username = html_attr_escape(username);
    axum::body::Bytes::from(render_template(
        DASHBOARD.get_file("index.html").and_then(|file| file.contents_utf8()).unwrap_or_default(),
        &[
            ("root", root.as_str()),
            ("username", username.as_str()),
            ("auth_enabled", if auth_enabled { "true" } else { "false" }),
            ("version", PUBLIC_FILES_VERSION.as_str()),
        ],
    ))
}

/// Substitutes `{name}` placeholders in one pass, so a substituted value
/// that itself contains a placeholder literal (a username of
/// `"{version}"`, say) is never substituted again.
fn render_template(template: &str, values: &[(&str, &str)]) -> String {
    let mut rendered = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        rendered.push_str(&rest[..start]);
        rest = &rest[start..];
        let placeholder = values
            .iter()
            .find(|(name, _)| rest.as_bytes().get(name.len() + 1) == Some(&b'}') && rest[1..].starts_with(name));
        match placeholder {
            Some((name, value)) => {
                rendered.push_str(value);
                rest = &rest[name.len() + 2..];
            }
            None => {
                rendered.push('{');
                rest = &rest[1..];
            }
        }
    }
    rendered.push_str(rest);
    rendered
}

/// The fingerprint every `?v=` in the templates carries.
///
/// It covers `PUBLIC_FILES` rather than a list of its own: those are exactly
/// the files `/static/` serves with `max-age=3600`, so a file missing from the
/// fingerprint is one browsers keep serving stale for an hour after an upgrade
/// — the failure the versioning exists to prevent, and the one the vendored
/// `pico.min.css` had while it was linked without a `?v=` at all.
///
/// Hashed once rather than per request: the files are embedded at compile
/// time, so this cannot change while the process runs, and the shell and the
/// login page — the one page an unauthenticated flood reaches — folded every
/// byte of all of them again on every single render.
static PUBLIC_FILES_VERSION: LazyLock<String> = LazyLock::new(|| {
    file_fingerprint(
        PUBLIC_FILES
            .iter()
            .filter_map(|(path, _)| DASHBOARD.get_file(path))
            .flat_map(|file| file.contents().iter().copied()),
    )
});

fn render_login(root: &str, error: &str) -> String {
    let root = html_attr_escape(root);
    let error = html_attr_escape(error);
    render_template(
        DASHBOARD.get_file("login.html").and_then(|file| file.contents_utf8()).unwrap_or_default(),
        &[
            ("root", root.as_str()),
            ("error", error.as_str()),
            ("version", PUBLIC_FILES_VERSION.as_str()),
        ],
    )
}

fn validate_mount_path(path: &str) -> Result<(), Error> {
    let valid_characters =
        path.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.' | b'~'));
    let without_trailing_slashes = path.trim_end_matches('/');
    let valid_segments = without_trailing_slashes.is_empty()
        || without_trailing_slashes
            .strip_prefix('/')
            .is_some_and(|rest| rest.split('/').all(|segment| !segment.is_empty() && !matches!(segment, "." | "..")));
    if !path.starts_with('/') || path.starts_with("//") || !valid_characters || !valid_segments {
        return Err(Error::Config(
            "dashboard mount_path must be a same-origin absolute path with safe ASCII segments".into(),
        ));
    }
    Ok(())
}

fn static_file_router() -> Router {
    Router::new().route("/static/{*path}", get(serve_file))
}

async fn enforce_request_limits(
    State(limits): State<Arc<DashboardRequestLimits>>,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    // Taken before the wait for a request slot, where the timeout below starts as well.
    if let Some(deadline) = ActionDeadline::ahead_of(limits.timeout) {
        request.extensions_mut().insert(deadline);
    }
    let run = async {
        let permit = match limits.requests.acquire().await {
            Ok(permit) => permit,
            Err(_) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({ "error": "dashboard request limiter is unavailable" })),
                )
                    .into_response();
            }
        };
        let response = next.run(request).await;
        drop(permit);
        response
    };
    match tokio::time::timeout(limits.timeout, run).await {
        Ok(response) => response,
        Err(_) => {
            (StatusCode::GATEWAY_TIMEOUT, Json(json!({ "error": "dashboard request timed out" }))).into_response()
        }
    }
}

#[cfg(test)]
mod dashboard_request_limit_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::body::Body;
    use axum::http::Request;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn test_request_limit_bounds_concurrency_and_execution_time() {
        let entered = Arc::new(AtomicUsize::new(0));
        let handler_entered = Arc::clone(&entered);
        let limits = Arc::new(DashboardRequestLimits {
            timeout: Duration::from_millis(100),
            requests: tokio::sync::Semaphore::new(1),
        });
        let router = Router::new()
            .route(
                "/",
                get(move || {
                    let entered = Arc::clone(&handler_entered);
                    async move {
                        entered.fetch_add(1, Ordering::SeqCst);
                        std::future::pending::<()>().await;
                    }
                }),
            )
            .layer(axum::middleware::from_fn_with_state(limits, enforce_request_limits));

        let first = tokio::spawn(router.clone().oneshot(Request::new(Body::empty())));
        while entered.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let second = tokio::spawn(router.oneshot(Request::new(Body::empty())));
        tokio::task::yield_now().await;
        assert_eq!(entered.load(Ordering::SeqCst), 1, "the second request entered without a permit");

        tokio::time::advance(Duration::from_millis(100)).await;
        assert_eq!(first.await.unwrap().unwrap().status(), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(second.await.unwrap().unwrap().status(), StatusCode::GATEWAY_TIMEOUT);
    }

    /// The limiter hands every request the deadline its actions' lock waits end at, ahead of its own by a tenth of the
    /// timeout and by a second at most, so that the server's refusal is answered before the limiter's 504.
    #[tokio::test(start_paused = true)]
    async fn test_request_limit_hands_actions_a_deadline_ahead_of_its_own() {
        for (timeout, lead) in [
            (Duration::from_millis(500), Duration::from_millis(50)),
            (Duration::from_secs(30), Duration::from_secs(1)),
        ] {
            let seen = Arc::new(Mutex::new(None));
            let handler_seen = Arc::clone(&seen);
            let limits = Arc::new(DashboardRequestLimits { timeout, requests: tokio::sync::Semaphore::new(1) });
            let router = Router::new()
                .route(
                    "/",
                    get(move |deadline: Option<Extension<ActionDeadline>>| {
                        *handler_seen.lock().unwrap() = deadline.map(|Extension(ActionDeadline(at))| at);
                        async {}
                    }),
                )
                .layer(axum::middleware::from_fn_with_state(limits, enforce_request_limits));

            // The clock is paused, so no time passes between this reading and the limiter's.
            let sent = tokio::time::Instant::now();
            assert_eq!(router.oneshot(Request::new(Body::empty())).await.unwrap().status(), StatusCode::OK);
            assert_eq!(*seen.lock().unwrap(), Some(sent + timeout - lead), "for a {timeout:?} request timeout");
        }
    }
}

async fn security_headers(
    axum::extract::State(secure_cookies): axum::extract::State<bool>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let sensitive = !request.uri().path().starts_with("/static/");
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            // `form-action 'self'` beside `base-uri 'none'`: the login form is
            // the one form here and it posts to this mount, so an injected
            // `action` pointing anywhere else is only ever an exfiltration of
            // the credentials typed into it.
            //
            // `style-src` deliberately has no `'unsafe-inline'`: every sheet is
            // served from `/static/` (the login page's block moved into
            // `login.css` for exactly this), so an HTML-injection bug that ever
            // appeared could not bring style injection along with it.
            "default-src 'self'; script-src 'self'; style-src 'self'; \
             connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; \
             base-uri 'none'; form-action 'self'",
        ),
    );
    headers.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    // `same-origin`, not `no-referrer`: both withhold every dashboard URL from other origins, but the login form's
    // `Origin` follows this policy too, and under `no-referrer` browsers send `Origin: null` even on a same-origin
    // post. Over plain HTTP to anything but loopback, browsers also omit `Sec-Fetch-Site`, so `is_cross_site_post`
    // then refused the operator's own login as an opaque origin.
    headers.insert(header::REFERRER_POLICY, HeaderValue::from_static("same-origin"));
    // `frame-ancestors 'none'` above is the real defence against framing the
    // Retry and Abort buttons; this repeats it for anything that predates CSP
    // level 2, which is the only reader that needs it and costs one header.
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    // Gated on `secure_cookies`, the same flag that marks the session cookie
    // `Secure` and so the deployment's own statement that it is behind TLS.
    // Sending HSTS over plain HTTP would pin a scheme this mount does not serve;
    // withholding it entirely leaves the *login* form — the one request that
    // carries credentials — open to an SSL-strip downgrade on first contact.
    if secure_cookies {
        headers.insert(header::STRICT_TRANSPORT_SECURITY, HeaderValue::from_static("max-age=31536000"));
    }
    if sensitive {
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }
    response
}

fn html_attr_escape(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for c in input.chars() {
        match c {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(c),
        }
    }
    escaped
}

fn file_fingerprint(contents: impl IntoIterator<Item = u8>) -> String {
    format!("{:016x}", crate::database::stable_hash(contents))
}

/// The only files `/static/` serves, with the content type each is served as.
///
/// `DASHBOARD` embeds the whole `dashboard/` directory, including the HTML
/// templates — which are meant to be reached only through the shell and login
/// routes, rendered and (when configured) behind `require_auth`. `/static/` is
/// mounted outside that layer, so serving the directory wholesale made every
/// file in it a public endpoint of an otherwise authenticated dashboard, and
/// would keep doing so for every file added later. This list makes that
/// exposure a deliberate choice instead.
const PUBLIC_FILES: &[(&str, &str)] = &[
    ("app.css", "text/css; charset=utf-8"),
    ("app.mjs", "application/javascript; charset=utf-8"),
    ("favicon.svg", "image/svg+xml"),
    ("login.css", "text/css; charset=utf-8"),
    ("pico.min.css", "text/css; charset=utf-8"),
];

async fn serve_file(path: Path<String>) -> Response {
    // `and_then` folds the "allowlisted but not embedded" case into the same
    // 404 as any other unknown path rather than leaving an arm nothing can
    // reach.
    let public = PUBLIC_FILES
        .iter()
        .find(|(name, _)| *name == path.as_str())
        .and_then(|(name, content_type)| Some((DASHBOARD.get_file(name)?, *content_type)));
    let Some((file, content_type)) = public else {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    };
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "max-age=3600"),
        ],
        file.contents(),
    )
        .into_response()
}

#[cfg(test)]
mod dashboard_files_tests {
    use super::*;

    #[test]
    fn test_file_fingerprint_is_stable_and_content_sensitive() {
        assert_eq!(file_fingerprint(*b"app"), file_fingerprint(b"app".iter().copied()));
        assert_ne!(file_fingerprint(*b"app"), file_fingerprint(*b"changed"));
    }

    #[test]
    fn test_render_template_substitutes_each_placeholder_once() {
        let rendered = render_template(
            r#"<meta root="{root}" user="{username}" other="{unknown}">"#,
            &[("root", "/pg"), ("username", "{root}")],
        );
        // A substituted value containing a placeholder literal stays
        // literal, and unknown placeholders survive untouched.
        assert_eq!(rendered, r#"<meta root="/pg" user="{root}" other="{unknown}">"#);
    }

    #[test]
    fn test_render_template_keeps_unterminated_braces() {
        assert_eq!(render_template("{root {root} {roots}", &[("root", "/pg")]), "{root /pg {roots}");
    }
}

// Authentication

const SESSION_COOKIE_PREFIX: &str = "pgqueue_session_";
const ACTION_HEADER: &str = "x-pgqueue-request";
const ACTION_HEADER_VALUE: &[u8] = b"dashboard";
/// Maximum UTF-8 bytes stored for a dashboard username or password.
const MAX_CREDENTIAL_BYTES: usize = 512;
/// Enough for two maximum-sized credentials after worst-case form or JSON escaping, with room for field names.
const CREDENTIAL_REQUEST_BODY_LIMIT: usize = 8 * 1024;
/// The browser's own statement of where a request came from; see
/// [`is_cross_site_post`].
const SITE_HEADER: &str = "sec-fetch-site";
const SESSION_TTL: Duration = Duration::from_secs(12 * 60 * 60);
const MAX_SESSIONS: usize = 64;
const AUTH_FAILURE_DELAY: Duration = Duration::from_millis(100);
/// How often one credential comparison is handed back to the account, and how
/// many an idle account may spend at once. Together they cap guessing at ten a
/// second sustained — the delay above only ever made *one* guess slow.
const AUTH_ATTEMPT_REFILL: Duration = Duration::from_millis(100);
const AUTH_ATTEMPT_BURST: u32 = 16;
/// How many client/channel budgets are tracked at once. Bounds what a client
/// hopping addresses can make this process allocate; see
/// [`make_room_for_a_bucket`] for what gives way when it is reached.
const MAX_AUTH_CLIENTS: usize = 1_024;
const AUTH_SATURATED_MESSAGE: &str = "too many authentication attempts";

enum CredentialCheck {
    Accepted(Uuid),
    Rejected,
    Saturated,
}

struct DashboardCredentials {
    password: String,
    revision: Uuid,
}

struct DashboardSession {
    expires_at: Instant,
    credential_revision: Uuid,
}

enum SessionCreation {
    Created(String),
    StaleCredentials,
    Unavailable,
}

enum PasswordRotation {
    /// The password changed. `session` is a freshly minted token for the caller
    /// — paired with the surviving expiry it inherited — when the request was
    /// authenticated by a session cookie, and `None` when it came in over HTTP
    /// Basic and so has no session to re-issue.
    Changed {
        session: Option<(String, Instant)>,
    },
    StaleCredentials,
    Unavailable,
}

struct DashboardAuthState {
    username: String,
    credentials: RwLock<DashboardCredentials>,
    sessions: Mutex<HashMap<String, DashboardSession>>,
    throttle: AuthThrottle,
    root: String,
    session_cookie_name: String,
    secure_cookies: bool,
    trusted_proxy_hops: usize,
    /// Guards the one-shot warning in
    /// [`DashboardAuthState::warn_if_throttle_is_unkeyed`]. The
    /// condition holds for every request once it holds for one, and a line per
    /// request would bury the deployment problem it is reporting.
    unkeyed_throttle_warned: std::sync::Once,
    /// The same, for the one-shot warning in [`DashboardAuthState::client_of`]
    /// about a chain that
    /// does not reach [`Dashboard::trusted_proxy_hops`].
    short_chain_warned: std::sync::Once,
}

/// Which client a credential comparison is charged to.
///
/// The socket peer, and `X-Forwarded-For` only as far back as
/// [`Dashboard::trusted_proxy_hops`] says the deployment's own proxies reach:
/// the header is otherwise attacker-controlled, so honouring it would let one
/// client mint a fresh budget per request and erase the throttle entirely.
///
/// [`DashboardServer`] serves its router with connection info, so its requests
/// always carry a peer. A [`Dashboard::router`] nested in another application
/// carries one only if that application supplies it (axum's
/// `into_make_service_with_connect_info`), and behind a reverse proxy every peer
/// is the proxy. Requests with no distinguishable peer share [`AuthClient::Any`].
///
/// Splitting the budget by [`AuthChannel`] does *not* rescue that bucket for the
/// login form: `POST /login` is itself [`AuthChannel::Interactive`] and needs no
/// credentials to reach, so `Any`-bucket traffic spends the interactive budget
/// directly. The split only keeps an HTTP Basic flood out of the form. Where
/// every request shares one bucket — behind a proxy, or nested without connect
/// info — a sustained flood of wrong passwords therefore keeps the form refused
/// for everyone, which is what `trusted_proxy_hops` exists to prevent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AuthClient {
    Peer(std::net::IpAddr),
    Any,
}

/// How the credentials arrived, and so which budget they draw on.
///
/// The two are deliberately separate. Anyone can put an `Authorization` header
/// on an API request, so that is the flood surface; the login form is the only
/// way an operator who holds no session can get in. Sharing one budget meant an
/// unauthenticated flood on the API locked the operator out of the form, which
/// is a denial of service rather than a throttle. Each budget is still the same
/// size, so neither channel is easier to guess at than before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AuthChannel {
    /// HTTP Basic credentials on a protected route.
    Basic,
    /// The login form and the password-change endpoint.
    Interactive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct AuthThrottleKey {
    client: AuthClient,
    channel: AuthChannel,
}

/// A token bucket per client and channel over the credential comparisons this
/// dashboard performs.
///
/// [`AUTH_FAILURE_DELAY`] bounds the latency of a single rejection, not the
/// rate of rejections: concurrent guesses were compared as fast as the network
/// could deliver them, because the comparison happened before anything
/// throttled it. Spending budget *first* bounds the rate instead, and — because
/// an exhausted budget refuses the attempt without comparing anything — a
/// correct password is then refused exactly like a wrong one, leaving no
/// 303-versus-429 oracle to guess against.
///
/// The budget is spent before the first `await`, so a client that cancels its
/// request mid-check has already paid for the attempt.
///
/// A comparison that *matches* hands its token straight back. A legitimate
/// client polling the JSON API over HTTP Basic would otherwise throttle itself
/// out of its own dashboard, and an attacker holding the password has nothing
/// left to guess.
///
/// The buckets are keyed because one shared budget is spent by whoever asks
/// most: an unauthenticated flood — which never gets a refund, having nothing
/// that matches — held the only budget at zero and refused the operator's
/// correct password for as long as it ran, with no reset short of a restart.
struct AuthThrottle {
    buckets: Mutex<HashMap<AuthThrottleKey, AuthThrottleTokens>>,
}

struct AuthThrottleTokens {
    available: u32,
    refilled_at: tokio::time::Instant,
}

impl AuthThrottleTokens {
    fn full(now: tokio::time::Instant) -> Self {
        Self { available: AUTH_ATTEMPT_BURST, refilled_at: now }
    }

    /// The bucket a key arriving into a full map starts with: what the bucket
    /// it displaced had left, if it displaced one that was still spent.
    ///
    /// Never below one comparison, so an operator arriving while an attacker
    /// holds every bucket at zero is still heard — and a correct password hands
    /// that comparison straight back and leaves with a session cookie, which is
    /// not throttled at all.
    fn arriving(now: tokio::time::Instant, displaced: Option<u32>) -> Self {
        match displaced {
            Some(available) => Self { available: available.max(1), refilled_at: now },
            None => Self::full(now),
        }
    }

    fn refill(&mut self, now: tokio::time::Instant) {
        let elapsed = now.saturating_duration_since(self.refilled_at).as_nanos() / AUTH_ATTEMPT_REFILL.as_nanos();
        let headroom = u128::from(AUTH_ATTEMPT_BURST - self.available);
        if elapsed >= headroom {
            // Time past a full bucket is not banked — a client nobody heard from
            // all day is still worth exactly one burst — and resetting the mark
            // keeps the interval arithmetic small.
            self.available = AUTH_ATTEMPT_BURST;
            self.refilled_at = now;
        } else if elapsed > 0 {
            // Still filling, so carry the sub-interval remainder instead of
            // rounding it away on every attempt.
            let refill = elapsed as u32;
            self.available += refill;
            self.refilled_at += AUTH_ATTEMPT_REFILL * refill;
        }
    }
}

impl AuthThrottle {
    fn new() -> Self {
        Self { buckets: Mutex::new(HashMap::new()) }
    }

    /// Spends one comparison from `key`'s budget, reporting `false` once that
    /// budget is exhausted.
    fn spend(&self, key: AuthThrottleKey) -> bool {
        let Ok(mut buckets) = self.buckets.lock() else {
            // Refuse rather than admit an unmetered attempt: the lock is only
            // ever held across integer arithmetic and map bookkeeping, so
            // poisoning it takes a panic that cannot come from here.
            tracing::error!("dashboard authentication throttle lock poisoned");
            return false;
        };
        let now = tokio::time::Instant::now();
        let displaced = if buckets.contains_key(&key) { None } else { make_room_for_a_bucket(&mut buckets, now) };
        let tokens = buckets.entry(key).or_insert_with(|| AuthThrottleTokens::arriving(now, displaced));
        tokens.refill(now);
        if tokens.available == 0 {
            return false;
        }
        tokens.available -= 1;
        true
    }

    /// Returns a token spent by a comparison that turned out to match.
    ///
    /// A bucket evicted in between is gone and its refund is dropped:
    /// [`make_room_for_a_bucket`] first drops the buckets that have refilled to
    /// full, which had their token back already, and only then evicts the
    /// fullest *still-spent* one — whose refund is genuinely lost. Losing one
    /// only ever errs toward throttling, and the client's next arrival is
    /// granted a comparison regardless.
    fn refund(&self, key: AuthThrottleKey) {
        let Ok(mut buckets) = self.buckets.lock() else {
            tracing::error!("dashboard authentication throttle lock poisoned");
            return;
        };
        if let Some(tokens) = buckets.get_mut(&key) {
            tokens.available = tokens.available.saturating_add(1).min(AUTH_ATTEMPT_BURST);
        }
    }
}

/// Bounds the map without letting one client's pressure deny another a bucket,
/// reporting what the client that gave way still had left.
///
/// A bucket that has refilled to full is indistinguishable from an absent one,
/// so those go first, and displacing one takes nothing from anybody. When none
/// has refilled, the fullest still-spent bucket gives way and its remaining
/// budget is what the arriving key starts from ([`AuthThrottleTokens::arriving`])
/// rather than a fresh burst — otherwise evicting a *saturated* bucket handed
/// the client being throttled its burst straight back, which is exactly what an
/// attacker filling the map was buying.
///
/// This bounds what cycling identities is worth; it does not make it worthless.
/// An arriving key is always granted one comparison, because refusing it is the
/// operator lockout this keying exists to avoid, so a flood that can mint
/// unlimited identities still buys one guess per identity instead of a burst of
/// [`AUTH_ATTEMPT_BURST`]. Minting them is what [`AuthClient::peer`] makes
/// expensive.
fn make_room_for_a_bucket(
    buckets: &mut HashMap<AuthThrottleKey, AuthThrottleTokens>,
    now: tokio::time::Instant,
) -> Option<u32> {
    if buckets.len() < MAX_AUTH_CLIENTS {
        return None;
    }
    buckets.retain(|_, tokens| {
        tokens.refill(now);
        tokens.available < AUTH_ATTEMPT_BURST
    });
    let mut displaced = None;
    while buckets.len() >= MAX_AUTH_CLIENTS
        && let Some((fullest, available)) =
            buckets.iter().max_by_key(|(_, tokens)| tokens.available).map(|(key, tokens)| (*key, tokens.available))
    {
        buckets.remove(&fullest);
        // Each pass takes the fullest of what is left, so the last one taken is
        // the smallest budget displaced.
        displaced = Some(available);
    }
    displaced
}

impl AuthClient {
    fn of(extensions: &axum::http::Extensions) -> Self {
        extensions
            .get::<axum::extract::ConnectInfo<SocketAddr>>()
            .map(|axum::extract::ConnectInfo(peer)| Self::peer(peer.ip()))
            .unwrap_or(Self::Any)
    }

    /// The client an address names, with IPv6 folded to its /64.
    ///
    /// A single IPv6 subscriber is normally handed a whole /64, so an
    /// individual address is not an identity anyone had to pay for: taking a
    /// fresh one per request costs nothing, and 2^64 of them is enough to cycle
    /// every bucket out of the map for as long as the flood lasts. The /64 is
    /// the smallest unit an attacker cannot mint more of. IPv4 addresses are
    /// scarce and routed one at a time, so they stay whole — folding them to a
    /// /24 would make unrelated customers of one ISP share a budget, which is
    /// the lockout this keying exists to avoid.
    fn peer(address: std::net::IpAddr) -> Self {
        // An IPv4 client arriving on a dual-stack socket is mapped into IPv6
        // (`::ffff:a.b.c.d`); folding *that* to a /64 would put every such
        // client in one bucket.
        match address.to_canonical() {
            std::net::IpAddr::V6(address) => {
                let mut octets = address.octets();
                octets[8..].fill(0);
                Self::Peer(std::net::Ipv6Addr::from(octets).into())
            }
            canonical => Self::Peer(canonical),
        }
    }
}

/// The client `trusted_proxy_hops` hops back along `X-Forwarded-For`, or `None`
/// where the header cannot be trusted to name one.
///
/// Counting from the *right* is what makes this unspoofable: each trusted proxy
/// appends the address it saw, so whatever a client writes into the header
/// itself is pushed one place further left per hop it travels, and the entry
/// this picks is always one of our own proxies' observations. A chain shorter
/// than the configured hops did not come through them all, and an entry that
/// names no address (`unknown`, an obfuscated identifier) names no client, so
/// both fall back to the socket peer rather than to what the header claims.
///
/// Split over bytes rather than `to_str`: `X-Forwarded-For` is one header line
/// by the time it arrives, and a single byte a client wrote outside ASCII made
/// `to_str` reject the *whole* line — discarding the trusted entries our own
/// proxies appended along with the attacker's prefix, and charging the request
/// to the shared socket-peer bucket. nginx passes such bytes through and
/// `$proxy_add_x_forwarded_for` concatenates them into that one line, so a
/// saturated client could buy itself a second budget with an unreadable prefix.
/// Parsed per entry, an unreadable entry stays an unreadable *entry*, exactly
/// like `unknown`.
fn forwarded_client(headers: &HeaderMap, trusted_proxy_hops: usize) -> Option<AuthClient> {
    if trusted_proxy_hops == 0 {
        return None;
    }
    let chain: Vec<&[u8]> = headers
        .get_all("x-forwarded-for")
        .iter()
        .flat_map(|value| value.as_bytes().split(|byte| *byte == b','))
        .map(<[u8]>::trim_ascii)
        .filter(|entry| !entry.is_empty())
        .collect();
    let entry = chain.len().checked_sub(trusted_proxy_hops).and_then(|index| chain.get(index))?;
    forwarded_address(std::str::from_utf8(entry).ok()?).map(AuthClient::peer)
}

/// The address in one `X-Forwarded-For` entry, which proxies write bare, with a
/// port, or bracketed when it is IPv6.
fn forwarded_address(entry: &str) -> Option<std::net::IpAddr> {
    if let Ok(address) = entry.parse::<std::net::IpAddr>() {
        return Some(address);
    }
    if let Ok(address) = entry.parse::<SocketAddr>() {
        return Some(address.ip());
    }
    entry.strip_prefix('[')?.strip_suffix(']')?.parse::<std::net::IpAddr>().ok()
}

impl axum::extract::FromRequestParts<Arc<DashboardAuthState>> for AuthClient {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &Arc<DashboardAuthState>,
    ) -> Result<Self, Self::Rejection> {
        Ok(state.client_of(&parts.headers, &parts.extensions))
    }
}

#[derive(Deserialize)]
struct DashboardLoginForm {
    username: String,
    password: String,
}

#[derive(Deserialize)]
struct DashboardPasswordChange {
    current_password: String,
    new_password: String,
}

impl DashboardAuthState {
    fn new(
        username: String,
        password: String,
        root: String,
        secure_cookies: bool,
        trusted_proxy_hops: usize,
    ) -> Arc<Self> {
        // `__Host-` locks the cookie to this exact host over TLS: a browser
        // refuses to *store* a cookie under the prefix from an insecure origin
        // or with a `Domain`, so a sibling subdomain or plain-HTTP
        // man-in-the-middle cannot plant a session cookie under our name —
        // fixation goes from mitigated (random per-process names) to refused
        // outright. The prefix is only valid with `Secure` and `Path=/` and no
        // `Domain`, which is exactly the shipped default; a dashboard opting
        // out of secure cookies or served under a mount path keeps the
        // unprefixed name those attributes allow.
        let host_prefix = if secure_cookies && root.is_empty() { "__Host-" } else { "" };
        Arc::new(Self {
            username,
            credentials: RwLock::new(DashboardCredentials { password, revision: Uuid::now_v7() }),
            sessions: Mutex::new(HashMap::new()),
            throttle: AuthThrottle::new(),
            session_cookie_name: format!("{host_prefix}{SESSION_COOKIE_PREFIX}{}", Uuid::now_v7().simple()),
            root,
            secure_cookies,
            trusted_proxy_hops,
            unkeyed_throttle_warned: std::sync::Once::new(),
            short_chain_warned: std::sync::Once::new(),
        })
    }

    /// Which client this request's credential comparison is charged to.
    ///
    /// A configured hop count that the arriving chain cannot satisfy is the one
    /// state that *proves* [`Dashboard::trusted_proxy_hops`] does not match the
    /// deployment, and silently swallowing it is expensive: counting one proxy
    /// too many — "CDN plus load balancer" where only the balancer appends —
    /// leaves every honest client (which sends no header at all) sharing the
    /// proxy's bucket, while a client that writes its own entries pushes the
    /// selected index onto forged ground and mints a fresh bucket per request.
    /// The throttle is then keyed by exactly the thing it exists to resist.
    /// Under a conforming deployment every request traverses the proxies and
    /// this never fires, so it is a warning rather than a refusal: the fallback
    /// to the socket peer is still the safe answer for the request in hand.
    fn client_of(&self, headers: &HeaderMap, extensions: &axum::http::Extensions) -> AuthClient {
        match forwarded_client(headers, self.trusted_proxy_hops) {
            Some(client) => client,
            None => {
                if self.trusted_proxy_hops > 0 {
                    self.short_chain_warned.call_once(|| {
                        tracing::warn!(
                            trusted_proxy_hops = self.trusted_proxy_hops,
                            "a dashboard request's X-Forwarded-For chain did not name a client at the configured \
                             hop count (a shorter chain, an unreadable or address-less entry, or no header at all), \
                             so its authentication attempts are charged to the socket peer; check \
                             Dashboard::trusted_proxy_hops against the number of proxies that actually append"
                        );
                    });
                }
                AuthClient::of(extensions)
            }
        }
    }

    /// Warns once when this dashboard's requests cannot be told apart.
    ///
    /// [`AuthClient::Any`] is the fallback for a request carrying no peer, and
    /// it collapses every client in the world into one throttle bucket: the
    /// module header's own `app.nest("/admin", ...router()?)` recipe served
    /// with a plain `axum::serve(listener, app)` — the default — produces
    /// exactly that. Sixteen wrong Basic guesses plus ten a second from
    /// anywhere then hold the interactive bucket at zero, and the operator's
    /// *correct* password is answered `429` for as long as the flood runs, with
    /// `trusted_proxy_hops` unable to help. The behaviour is documented on
    /// [`AuthClient`], but a deployment only ever meets it during the attack it
    /// describes, so say it at the first request instead.
    fn warn_if_throttle_is_unkeyed(&self, client: AuthClient) {
        if client != AuthClient::Any {
            return;
        }
        self.unkeyed_throttle_warned.call_once(|| {
            tracing::warn!(
                "dashboard requests carry no client address, so every authentication attempt \
                 shares one throttle bucket and a flood from anywhere can lock the operator out; \
                 serve the router with axum's into_make_service_with_connect_info::<SocketAddr>(), \
                 or set Dashboard::trusted_proxy_hops behind a reverse proxy"
            );
        });
    }

    fn credentials_match(&self, username: &str, password: &str) -> Option<Uuid> {
        let Ok(credentials) = self.credentials.read() else {
            tracing::error!("dashboard password lock poisoned");
            return None;
        };
        let username_matches = constant_time_eq(username.as_bytes(), self.username.as_bytes());
        let password_matches = constant_time_eq(password.as_bytes(), credentials.password.as_bytes());
        (username_matches & password_matches).then_some(credentials.revision)
    }

    /// Compares the client's credential against this crate's own canonical
    /// padded base64 of `user:password`, never decoding what the client sent:
    /// hostile input then reaches only a constant-time byte comparison, not a
    /// decoder. The deliberate cost is that a *decodable but non-canonical*
    /// encoding of the correct credentials — unpadded, whitespace-wrapped — is
    /// refused. Every mainstream client (browsers, curl, HTTP libraries)
    /// emits the canonical form RFC 7617 shows, so the trade buys decode-free
    /// safety for an interop cost that in practice nothing pays.
    fn basic_credentials_match(&self, supplied: &[u8]) -> Option<Uuid> {
        let Ok(credentials) = self.credentials.read() else {
            tracing::error!("dashboard password lock poisoned");
            return None;
        };
        let expected = base64(format!("{}:{}", self.username, credentials.password));
        constant_time_eq(supplied, expected.as_bytes()).then_some(credentials.revision)
    }

    async fn check_credentials(&self, client: AuthClient, username: &str, password: &str) -> CredentialCheck {
        self.check(AuthChannel::Interactive, client, || self.credentials_match(username, password)).await
    }

    async fn check_basic_credentials(&self, client: AuthClient, headers: &HeaderMap) -> CredentialCheck {
        // A header carrying no Basic credential is refused without spending
        // budget: there is nothing to compare, so letting it through would let
        // any anonymous client hold the throttle saturated for everyone else.
        let Some(supplied) = basic_credentials(headers) else {
            return CredentialCheck::Rejected;
        };
        self.check(AuthChannel::Basic, client, || self.basic_credentials_match(supplied)).await
    }

    /// Spends one attempt from this client's budget for this channel and, if
    /// there was one to spend, compares the supplied credentials.
    async fn check(
        &self,
        channel: AuthChannel,
        client: AuthClient,
        compare: impl FnOnce() -> Option<Uuid>,
    ) -> CredentialCheck {
        let key = AuthThrottleKey { client, channel };
        if !self.throttle.spend(key) {
            return CredentialCheck::Saturated;
        }
        if let Some(revision) = compare() {
            self.throttle.refund(key);
            return CredentialCheck::Accepted(revision);
        }
        // The delay makes one guess expensive; the budget above is what makes a
        // million of them expensive.
        tokio::time::sleep(AUTH_FAILURE_DELAY).await;
        CredentialCheck::Rejected
    }

    fn create_session(&self, credential_revision: Uuid) -> SessionCreation {
        let token = random_session_token();
        let now = Instant::now();
        let Ok(credentials) = self.credentials.read() else {
            tracing::error!("dashboard password lock poisoned");
            return SessionCreation::Unavailable;
        };
        if credentials.revision != credential_revision {
            return SessionCreation::StaleCredentials;
        }
        let Ok(mut sessions) = self.sessions.lock() else {
            tracing::error!("dashboard session lock poisoned");
            return SessionCreation::Unavailable;
        };
        sessions.retain(|_, session| session.expires_at > now && session.credential_revision == credentials.revision);
        if sessions.len() >= MAX_SESSIONS {
            let oldest = sessions.iter().min_by_key(|(_, session)| session.expires_at).map(|(token, _)| token.clone());
            if let Some(oldest) = oldest {
                sessions.remove(&oldest);
                // `info`, not `warn`: a session exists only behind a correct
                // password, so reaching the cap is ordinary use by an operator
                // with many tabs or browsers rather than an attack signal. It is
                // logged because the displaced browser is silently logged out,
                // which is otherwise unexplainable from the outside.
                tracing::info!(sessions = MAX_SESSIONS, "dashboard session cap reached; retiring the oldest session");
            }
        }
        sessions.insert(token.clone(), DashboardSession { expires_at: now + SESSION_TTL, credential_revision });
        SessionCreation::Created(token)
    }

    fn session_is_valid(&self, headers: &HeaderMap) -> bool {
        let now = Instant::now();
        let Ok(credentials) = self.credentials.read() else {
            tracing::error!("dashboard password lock poisoned");
            return false;
        };
        let Ok(mut sessions) = self.sessions.lock() else {
            tracing::error!("dashboard session lock poisoned");
            return false;
        };
        sessions.retain(|_, session| session.expires_at > now && session.credential_revision == credentials.revision);
        session_tokens(headers, &self.session_cookie_name).any(|token| stored_session_key(&sessions, token).is_some())
    }

    /// Retires every session this request names, not merely the first: a logout
    /// that answered from one candidate cookie reported success while leaving
    /// the session it was asked to revoke live for the rest of its TTL.
    fn remove_session(&self, headers: &HeaderMap) {
        let Ok(mut sessions) = self.sessions.lock() else {
            tracing::error!("dashboard session lock poisoned");
            return;
        };
        let stored = session_tokens(headers, &self.session_cookie_name)
            .filter_map(|token| stored_session_key(&sessions, token))
            .collect::<Vec<_>>();
        for key in stored {
            sessions.remove(&key);
        }
    }

    fn rotate_password(&self, expected_revision: Uuid, new_password: String, headers: &HeaderMap) -> PasswordRotation {
        let current = session_tokens(headers, &self.session_cookie_name).map(str::to_owned).collect::<Vec<_>>();
        let Ok(mut credentials) = self.credentials.write() else {
            tracing::error!("dashboard password lock poisoned");
            return PasswordRotation::Unavailable;
        };
        if credentials.revision != expected_revision {
            return PasswordRotation::StaleCredentials;
        }
        let Ok(mut sessions) = self.sessions.lock() else {
            tracing::error!("dashboard session lock poisoned");
            return PasswordRotation::Unavailable;
        };
        let revision = Uuid::now_v7();
        credentials.password = new_password;
        credentials.revision = revision;
        let now = Instant::now();
        // Every token minted under the old password dies, the caller's
        // included: an admin who changes the password because a token may have
        // leaked would otherwise leave the one token worth rotating — the one
        // that crossed the network, in cleartext under `secure_cookies(false)`
        // — valid for the rest of its TTL. The caller gets a fresh token in
        // exchange, expiring when the old one would have, so a rotation neither
        // logs the admin out nor extends their session.
        // The first candidate that names a live session, so a planted cookie
        // sitting ahead of the caller's own cannot cost them the expiry they
        // were entitled to inherit.
        let expires_at = current
            .iter()
            .filter_map(|token| stored_session_key(&sessions, token))
            .filter_map(|stored| sessions.get(&stored))
            .map(|session| session.expires_at)
            .find(|expires_at| *expires_at > now);
        sessions.clear();
        let session = expires_at.map(|expires_at| {
            let token = random_session_token();
            sessions.insert(token.clone(), DashboardSession { expires_at, credential_revision: revision });
            (token, expires_at)
        });
        PasswordRotation::Changed { session }
    }

    fn login_html(&self, error: &str) -> String {
        render_login(&self.root, error)
    }

    fn home_path(&self) -> String {
        if self.root.is_empty() { "/".to_string() } else { self.root.clone() }
    }

    fn login_path(&self) -> String {
        format!("{}/login", self.root)
    }

    fn cookie_path(&self) -> &str {
        if self.root.is_empty() { "/" } else { &self.root }
    }
}

/// The credentials a request supplies over HTTP Basic, if it supplies any.
fn basic_credentials(headers: &HeaderMap) -> Option<&[u8]> {
    // RFC 7617: the auth-scheme token is case-insensitive and is separated from
    // the credentials by one or more spaces.
    let value = headers.get(header::AUTHORIZATION)?.as_bytes();
    let (scheme, rest) = value.split_at_checked(5)?;
    if !scheme.eq_ignore_ascii_case(b"basic") || !rest.starts_with(b" ") {
        return None;
    }
    Some(&rest[rest.iter().position(|byte| *byte != b' ').unwrap_or(rest.len())..])
}

fn account_router(auth: Arc<DashboardAuthState>) -> Router {
    Router::new()
        .route(
            "/api/account/password",
            post(change_password).layer(DefaultBodyLimit::max(CREDENTIAL_REQUEST_BODY_LIMIT)),
        )
        .route("/api/account/logout", post(logout))
        .with_state(auth)
}

fn login_router(auth: Arc<DashboardAuthState>) -> Router {
    Router::new()
        .route("/login", get(login_page))
        .route("/login", post(login).layer(DefaultBodyLimit::max(CREDENTIAL_REQUEST_BODY_LIMIT)))
        .with_state(auth)
}

async fn require_auth(
    State(auth): State<Arc<DashboardAuthState>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let supplied_authorization = request.headers().contains_key(header::AUTHORIZATION);
    auth.warn_if_throttle_is_unkeyed(auth.client_of(request.headers(), request.extensions()));
    if auth.session_is_valid(request.headers()) {
        return next.run(request).await;
    }
    if supplied_authorization {
        let client = auth.client_of(request.headers(), request.extensions());
        match auth.check_basic_credentials(client, request.headers()).await {
            CredentialCheck::Accepted(_) => return next.run(request).await,
            CredentialCheck::Rejected => {}
            CredentialCheck::Saturated => {
                return DashboardApiError::TooManyRequests(AUTH_SATURATED_MESSAGE).into_response();
            }
        }
    }

    let wants_html = !request.uri().path().starts_with("/api/")
        && request
            .headers()
            .get(header::ACCEPT)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("text/html"));
    if wants_html {
        return redirect_response(&auth.login_path(), None);
    }

    // The dashboard's own `fetch` carries the action header on every request, and a Basic challenge answering it made
    // the browser raise its native credentials dialog over the page — on every session expiry and after every dashboard
    // restart — instead of letting the page redirect to the form. Credentials typed into that dialog are cached for the
    // origin, beyond the reach of logout. Other clients still get the challenge.
    if require_action_header(request.headers()).is_ok() {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    (StatusCode::UNAUTHORIZED, [(header::WWW_AUTHENTICATE, "Basic realm=\"pgqueue\"")], "unauthorized").into_response()
}

async fn login_page(State(auth): State<Arc<DashboardAuthState>>) -> Html<String> {
    Html(auth.login_html(""))
}

async fn login(
    State(auth): State<Arc<DashboardAuthState>>,
    client: AuthClient,
    uri: axum::http::Uri,
    headers: HeaderMap,
    Form(form): Form<DashboardLoginForm>,
) -> Response {
    if is_cross_site_post(&headers, &uri) {
        return (StatusCode::FORBIDDEN, Html(auth.login_html("Cross-site login posts are refused."))).into_response();
    }
    let credential_revision = match auth.check_credentials(client, &form.username, &form.password).await {
        CredentialCheck::Accepted(revision) => revision,
        CredentialCheck::Rejected => {
            return (StatusCode::UNAUTHORIZED, Html(auth.login_html("Invalid username or password."))).into_response();
        }
        CredentialCheck::Saturated => {
            // This arm answers a browser posting the login form, so it renders
            // the page like every other outcome of that form. The API's JSON
            // body arrived as a bare document with no way back to the form.
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, "1")],
                Html(auth.login_html("Too many attempts. Try again shortly.")),
            )
                .into_response();
        }
    };
    let token = match auth.create_session(credential_revision) {
        SessionCreation::Created(token) => token,
        SessionCreation::StaleCredentials => {
            return (StatusCode::UNAUTHORIZED, Html(auth.login_html("Invalid username or password."))).into_response();
        }
        SessionCreation::Unavailable => {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    redirect_response(
        &auth.home_path(),
        Some(&session_cookie(&auth.session_cookie_name, &token, auth.secure_cookies, auth.cookie_path())),
    )
}

async fn change_password(
    State(auth): State<Arc<DashboardAuthState>>,
    client: AuthClient,
    headers: HeaderMap,
    Json(change): Json<DashboardPasswordChange>,
) -> Result<Response, DashboardApiError> {
    require_action_header(&headers)?;
    // Characters, as the message says: `len()` counts UTF-8 bytes, so a
    // four-character Latin-1 password — or a three-character CJK one — passed a
    // rule stated in characters. The maximum below stays a byte bound: it is a
    // size guard on what the process stores, not a policy.
    if change.new_password.chars().count() < 8 {
        return Err(DashboardApiError::BadRequest("new password must be at least 8 characters"));
    }
    if change.new_password.len() > MAX_CREDENTIAL_BYTES {
        return Err(DashboardApiError::BadRequest("new password is too long"));
    }
    let credential_revision = match auth.check_credentials(client, &auth.username, &change.current_password).await {
        CredentialCheck::Accepted(revision) => revision,
        CredentialCheck::Rejected => {
            return Err(DashboardApiError::Forbidden("current password is incorrect"));
        }
        CredentialCheck::Saturated => {
            return Err(DashboardApiError::TooManyRequests(AUTH_SATURATED_MESSAGE));
        }
    };
    match auth.rotate_password(credential_revision, change.new_password, &headers) {
        PasswordRotation::Changed { session } => {
            let mut response = Json(json!({ "changed": true })).into_response();
            if let Some((token, expires_at)) = session {
                // The re-minted session inherits the old one's server-side
                // expiry, so the cookie has to inherit it too. Issuing the full
                // `SESSION_TTL` here left the browser holding a credential the
                // server had already forgotten — up to a whole TTL longer than
                // the rotation intends.
                let cookie = session_cookie_attributes(
                    &format!("{}={token}", auth.session_cookie_name),
                    auth.secure_cookies,
                    auth.cookie_path(),
                    Some(expires_at.saturating_duration_since(Instant::now()).as_secs()),
                );
                let Ok(cookie) = HeaderValue::from_str(&cookie) else {
                    tracing::error!("invalid dashboard session cookie");
                    return Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response());
                };
                response.headers_mut().insert(header::SET_COOKIE, cookie);
            }
            Ok(response)
        }
        PasswordRotation::StaleCredentials => Err(DashboardApiError::Forbidden("current password is incorrect")),
        PasswordRotation::Unavailable => Err(DashboardApiError::Internal(Error::Dashboard(std::io::Error::other(
            "dashboard authentication state unavailable",
        )))),
    }
}

async fn logout(
    State(auth): State<Arc<DashboardAuthState>>,
    headers: HeaderMap,
) -> Result<Response, DashboardApiError> {
    require_action_header(&headers)?;
    auth.remove_session(&headers);
    let mut response = Json(json!({ "logged_out": true })).into_response();
    let clear_cookie = session_cookie_attributes(
        &format!("{}=", auth.session_cookie_name),
        auth.secure_cookies,
        auth.cookie_path(),
        Some(0),
    );
    let Ok(clear_cookie) = HeaderValue::from_str(&clear_cookie) else {
        tracing::error!("invalid dashboard session cookie name");
        return Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response());
    };
    response.headers_mut().insert(header::SET_COOKIE, clear_cookie);
    Ok(response)
}

/// Every session token this request carries, in the order it sent them.
///
/// *Every* candidate, not the first: a cookie name is not unique. A browser
/// sends one line per stored cookie of that name — RFC 6265 §5.4.2 orders the
/// longer `Path` first — so anyone able to set a cookie on this host can put a
/// value of their choosing ahead of the genuine session. Answering from the
/// first match then hid the real token behind it: every request 401'd while
/// carrying a valid session, the operator could not reach `logout` to revoke it,
/// a `logout` driven through HTTP Basic reported success while revoking nothing,
/// and logging in again did not help, because the planted cookie still came
/// first. Emptiness is filtered for the same reason — a cleared cookie leaves an
/// empty duplicate behind — and both are the same rule: a candidate that names
/// no live session is not the session, whatever its position.
///
/// The `__Host-` prefix stops the cookie being planted at all, but it requires
/// `Path=/`, so it is dropped for a dashboard under a mount path — which is the
/// deployment [`Dashboard::router`]'s own documentation recommends.
///
/// Every `Cookie` field line is searched, not just the first: RFC 9113 §8.2.3
/// lets an HTTP/2 client split `cookie` into several field lines, and neither
/// `hyper` nor `h2` rejoins them. The standalone [`DashboardServer`] is
/// HTTP/1.1 only, but [`Dashboard::router`] is documented for nesting into an
/// application that does serve h2 — and there a browser that split its cookies
/// looped login → home → login forever.
///
/// Split over bytes rather than `to_str`, for the reason `forwarded_client` gives: a browser sends every cookie for the
/// host on one line, and one byte outside visible ASCII in *any* of them — `theme=café`, set by another application on
/// the same host — made `to_str` reject the whole line, the session token with it. Every request then answered 401 and
/// a correct login looped straight back to the form.
fn session_tokens<'a>(headers: &'a HeaderMap, cookie_name: &'a str) -> impl Iterator<Item = &'a str> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .flat_map(|line| line.as_bytes().split(|byte| *byte == b';'))
        .map(<[u8]>::trim_ascii)
        .filter_map(move |cookie| {
            let (name, token) = cookie.split_at(cookie.iter().position(|byte| *byte == b'=')?);
            let token = &token[1..];
            (name == cookie_name.as_bytes() && !token.is_empty()).then_some(token)
        })
        .filter_map(|token| std::str::from_utf8(token).ok())
}

fn random_session_token() -> String {
    rand::random::<[u8; 32]>().iter().map(|byte| format!("{byte:02x}")).collect()
}

fn session_cookie(cookie_name: &str, token: &str, secure: bool, path: &str) -> String {
    session_cookie_attributes(&format!("{cookie_name}={token}"), secure, path, Some(SESSION_TTL.as_secs()))
}

fn session_cookie_attributes(value: &str, secure: bool, path: &str, max_age: Option<u64>) -> String {
    let secure = if secure { "; Secure" } else { "" };
    let max_age = max_age.map(|seconds| format!("; Max-Age={seconds}")).unwrap_or_default();
    format!("{value}; Path={path}{secure}; HttpOnly; SameSite=Strict{max_age}")
}

fn redirect_response(location: &str, cookie: Option<&str>) -> Response {
    let Ok(location) = HeaderValue::from_str(location) else {
        tracing::error!(location, "invalid dashboard redirect path");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let mut response = StatusCode::SEE_OTHER.into_response();
    response.headers_mut().insert(header::LOCATION, location);
    if let Some(cookie) = cookie {
        let Ok(cookie) = HeaderValue::from_str(cookie) else {
            tracing::error!("invalid dashboard session cookie");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        response.headers_mut().insert(header::SET_COOKIE, cookie);
    }
    response
}

/// The stored session key matching `token`, every candidate compared in
/// constant time.
///
/// A plain `HashMap` key lookup ends in a variable-time string equality — the
/// comparison shape the credential checks here deliberately avoid — and a
/// session token is a credential like any other. The map holds at most
/// [`MAX_SESSIONS`] entries, so the scan costs nothing measurable, and every
/// mint path inserts distinct random tokens, so at most one can match.
fn stored_session_key(sessions: &HashMap<String, DashboardSession>, token: &str) -> Option<String> {
    sessions.keys().find(|stored| constant_time_eq(stored.as_bytes(), token.as_bytes())).cloned()
}

/// Byte comparison whose running time depends only on the operands' length, not
/// on where they first differ (a length mismatch short-circuits, which leaks the
/// credential length and nothing else).
///
/// The accumulator crosses `black_box` before it is tested. Folding with `|` and
/// comparing once is the *source* shape of a constant-time comparison, but
/// nothing in the language or in LLVM forbids lowering it back to a
/// short-circuiting loop — the property this backs (the password, the HTTP Basic
/// credential, and the session-token lookup in [`stored_session_key`]) would then
/// rest on an optimiser's present-day choices. `black_box` makes the accumulator
/// opaque, so the whole fold has to run before anything can branch on it. It is a
/// hint rather than a guarantee, but it is the strongest one available without
/// taking a dependency for a single comparison.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && std::hint::black_box(a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y))) == 0
}

/// Standard base64 (with padding), kept local to avoid a dependency for one
/// HTTP Basic header.
fn base64(input: impl AsRef<[u8]>) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_ref();
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> (18 - 6 * i)) as usize & 0x3f] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod dashboard_auth_tests {
    use axum::body::Body;
    use axum::http::Request;

    use super::*;

    #[test]
    fn test_base64_matches_reference_vectors() {
        assert_eq!(base64(""), "");
        assert_eq!(base64("f"), "Zg==");
        assert_eq!(base64("fo"), "Zm8=");
        assert_eq!(base64("foo"), "Zm9v");
        assert_eq!(base64("foob"), "Zm9vYg==");
        assert_eq!(base64("fooba"), "Zm9vYmE=");
        assert_eq!(base64("foobar"), "Zm9vYmFy");
        assert_eq!(base64("admin:s3cretpw"), "YWRtaW46czNjcmV0cHc=");
    }

    #[test]
    fn test_constant_time_eq_accepts_only_equal_bytes() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_eq(b"", b""));
    }

    /// A session token is a credential, so its lookup goes through the same
    /// constant-time comparison the passwords do rather than a `HashMap` key
    /// probe's variable-time equality — and it must still match only exactly.
    #[test]
    fn test_stored_session_key_matches_only_the_exact_token() {
        let mut sessions = HashMap::new();
        let token = random_session_token();
        sessions.insert(
            token.clone(),
            DashboardSession { expires_at: Instant::now() + SESSION_TTL, credential_revision: Uuid::now_v7() },
        );
        assert_eq!(stored_session_key(&sessions, &token).as_deref(), Some(token.as_str()));
        let near_miss = format!("{}0", &token[..token.len() - 1]);
        let near_miss = if near_miss == token { format!("{}1", &token[..token.len() - 1]) } else { near_miss };
        assert!(stored_session_key(&sessions, &near_miss).is_none(), "an equal-length near miss must not match");
        assert!(stored_session_key(&sessions, &token[..token.len() - 1]).is_none(), "a prefix is not the token");
        assert!(stored_session_key(&sessions, "").is_none());
    }

    #[test]
    fn test_session_cookie_security_is_configurable() {
        let secure = session_cookie("cookie", "token", true, "/");
        assert!(secure.contains("; Secure;"));
        let plain_http = session_cookie("cookie", "token", false, "/");
        assert!(!plain_http.contains("; Secure;"));
        assert!(plain_http.contains("; HttpOnly; SameSite=Strict;"));
    }

    #[test]
    fn test_session_cookie_uses_configured_path() {
        let cookie = session_cookie("cookie", "token", true, "/admin");
        assert!(cookie.contains("; Path=/admin;"));
    }

    #[test]
    fn test_dashboard_auth_states_use_distinct_session_cookie_names() {
        let first = test_auth_state();
        let second = test_auth_state();

        // Secure cookies at the root mount: the name carries `__Host-`, so a
        // browser refuses a same-named cookie planted by a sibling subdomain
        // or an insecure origin.
        assert!(first.session_cookie_name.starts_with("__Host-"));
        assert!(first.session_cookie_name.contains(SESSION_COOKIE_PREFIX));
        assert!(second.session_cookie_name.starts_with("__Host-"));
        assert_ne!(first.session_cookie_name, second.session_cookie_name);

        // The prefix is only valid with `Secure` and `Path=/`, so opting out
        // of either keeps the plain name those attributes allow.
        let insecure = DashboardAuthState::new("admin".into(), "password".into(), String::new(), false, 0);
        assert!(insecure.session_cookie_name.starts_with(SESSION_COOKIE_PREFIX));
        let mounted = DashboardAuthState::new("admin".into(), "password".into(), "/admin".into(), true, 0);
        assert!(mounted.session_cookie_name.starts_with(SESSION_COOKIE_PREFIX));
    }

    #[test]
    fn test_dashboard_refuses_credentials_larger_than_its_request_limit_can_represent() {
        for (username, password) in [
            ("u".repeat(MAX_CREDENTIAL_BYTES + 1), "password".to_string()),
            ("admin".to_string(), "p".repeat(MAX_CREDENTIAL_BYTES + 1)),
        ] {
            let result = Dashboard::new(Vec::<Queue>::new()).basic_auth(username, password).router();
            match result {
                Err(Error::Config(message)) => assert!(message.contains("at most 512 bytes"), "{message}"),
                Err(error) => panic!("unexpected error: {error}"),
                Ok(_) => panic!("an oversized dashboard credential built a router"),
            }
        }
    }

    #[tokio::test]
    async fn test_credential_routes_limit_request_bodies_before_authentication() {
        let auth = test_auth_state();
        let login = login_router(auth.clone());
        let oversized = "x".repeat(CREDENTIAL_REQUEST_BODY_LIMIT);
        let response = login
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/login")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from(format!("username=admin&password={oversized}")))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(auth.throttle.buckets.lock().unwrap().is_empty(), "an oversized login reached the auth throttle");

        let response = login
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/login")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from("username=admin&password=password"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        let cookie = response.headers()[header::SET_COOKIE].to_str().unwrap().split(';').next().unwrap();

        let account = account_router(auth);
        let response = account
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/account/password")
                    .header(header::COOKIE, cookie)
                    .header(ACTION_HEADER, ACTION_HEADER_VALUE)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json!({ "current_password": "password", "new_password": oversized }).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let response = account
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/account/password")
                    .header(header::COOKIE, cookie)
                    .header(ACTION_HEADER, ACTION_HEADER_VALUE)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"current_password":"password","new_password":"replacement"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Every accepted credential still fits after its transport applies the largest possible expansion. A non-ASCII
        // form byte becomes `%XX`, while a JSON control byte becomes `\u00XX`.
        let maximum = "é".repeat(MAX_CREDENTIAL_BYTES / "é".len());
        let encoded_maximum = "%C3%A9".repeat(MAX_CREDENTIAL_BYTES / "é".len());
        let auth = DashboardAuthState::new(maximum.clone(), maximum, String::new(), true, 0);
        let response = login_router(auth)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/login")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from(format!("username={encoded_maximum}&password={encoded_maximum}")))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);

        let current = "\0".repeat(MAX_CREDENTIAL_BYTES);
        let next = "\u{1}".repeat(MAX_CREDENTIAL_BYTES);
        let auth = DashboardAuthState::new("admin".into(), current.clone(), String::new(), true, 0);
        let response = account_router(auth)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/account/password")
                    .header(ACTION_HEADER, ACTION_HEADER_VALUE)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json!({ "current_password": current, "new_password": next }).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// The Fetch-Metadata fallback: `Origin` names an authority or nothing,
    /// and only the dashboard's own authority — with the scheme's default
    /// port treated as absent on either side — may pass.
    #[test]
    fn test_origin_names_host_compares_authorities_with_default_ports_normalized() {
        assert!(origin_names_host("https://dash.example", "dash.example"));
        assert!(origin_names_host("https://dash.example", "DASH.example:443"));
        assert!(origin_names_host("http://dash.example:80", "dash.example"));
        assert!(origin_names_host("https://dash.example:8443", "dash.example:8443"));

        assert!(!origin_names_host("https://evil.example", "dash.example"));
        assert!(!origin_names_host("https://dash.example:8443", "dash.example"));
        assert!(!origin_names_host("null", "dash.example"), "an opaque origin names nothing");
        assert!(!origin_names_host("https://", "dash.example"));
        // http's default port is not https's, so an explicit `:443` over http
        // is a real difference, not one to normalize away.
        assert!(!origin_names_host("http://dash.example:443", "dash.example"));
    }

    /// An HTTP/2 request names its authority in the request target, not in a `Host` header. Read only from the header,
    /// such a login post had nothing to compare its `Origin` with and was let through from any site.
    #[test]
    fn test_login_cross_site_check_reads_an_h2_request_authority() {
        let h2: axum::http::Uri = "https://dash.example/login".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(header::ORIGIN, HeaderValue::from_static("https://evil.example"));
        assert!(is_cross_site_post(&headers, &h2), "a foreign origin was let through without a Host header");
        headers.insert(header::ORIGIN, HeaderValue::from_static("https://dash.example"));
        assert!(!is_cross_site_post(&headers, &h2));

        // With no authority anywhere there is nothing to compare, and the post is not refused for it.
        let origin_form: axum::http::Uri = "/login".parse().unwrap();
        headers.insert(header::ORIGIN, HeaderValue::from_static("https://evil.example"));
        assert!(!is_cross_site_post(&headers, &origin_form));
        // A `Host` header still decides when there is one.
        headers.insert(header::HOST, HeaderValue::from_static("dash.example"));
        assert!(is_cross_site_post(&headers, &origin_form));
    }

    #[test]
    fn test_create_session_rejects_validated_credentials_when_password_rotates_before_mint() {
        let auth = test_auth_state();
        let validated_revision = auth.credentials_match("admin", "password").unwrap();

        // No cookie on the request, so there is no session to re-issue.
        assert!(matches!(
            auth.rotate_password(validated_revision, "new-secret".into(), &HeaderMap::new()),
            PasswordRotation::Changed { session: None }
        ));
        assert!(matches!(auth.create_session(validated_revision), SessionCreation::StaleCredentials));
    }

    /// The auth state these tests share: secure cookies, mounted at the root,
    /// and trusting no proxy — which is [`Dashboard`]'s default.
    fn test_auth_state() -> Arc<DashboardAuthState> {
        DashboardAuthState::new("admin".into(), "password".into(), String::new(), true, 0)
    }

    fn test_client(last: u8) -> AuthClient {
        AuthClient::Peer(std::net::IpAddr::from([10, 0, 0, last]))
    }

    fn interactive(client: AuthClient) -> AuthThrottleKey {
        AuthThrottleKey { client, channel: AuthChannel::Interactive }
    }

    /// Real time, not paused: every assertion below runs in microseconds, while
    /// handing an attempt back takes [`AUTH_ATTEMPT_REFILL`] — and a paused
    /// clock auto-advances past exactly that while a cancelled task settles.
    #[tokio::test]
    async fn test_cancelled_credential_check_still_spends_its_attempt_budget() {
        let auth = test_auth_state();
        let client = test_client(1);
        let attempt_auth = auth.clone();
        let attempt = tokio::spawn(async move { attempt_auth.check_credentials(client, "admin", "incorrect").await });
        tokio::task::yield_now().await;

        assert!(!attempt.is_finished());
        attempt.abort();
        match attempt.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("failed credential check completed before cancellation"),
        }
        // The budget is spent before the first await, so cancelling the request
        // that spent it buys nothing: only time hands attempts back.
        for _ in 1..AUTH_ATTEMPT_BURST {
            assert!(auth.throttle.spend(interactive(client)));
        }
        assert!(!auth.throttle.spend(interactive(client)), "the cancelled attempt kept the budget it spent");
    }

    #[tokio::test]
    async fn test_credential_check_refuses_a_correct_password_when_the_budget_is_spent() {
        let auth = test_auth_state();
        let client = test_client(1);
        // A correct password hands its own attempt straight back, so guessing
        // is what draws the budget down.
        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(matches!(auth.check_credentials(client, "admin", "password").await, CredentialCheck::Accepted(_)));
        }
        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(auth.throttle.spend(interactive(client)));
        }

        // No comparison happens at all now, so the correct password is refused
        // exactly like a wrong one: the reply says nothing about the guess.
        assert!(matches!(auth.check_credentials(client, "admin", "password").await, CredentialCheck::Saturated));
        assert!(matches!(auth.check_credentials(client, "admin", "incorrect").await, CredentialCheck::Saturated));
    }

    /// The client whose guessing spent a budget is the only one refused: a
    /// shared budget made one flooding client a lockout for everybody, and the
    /// operator's correct password was refused without ever being read.
    #[tokio::test]
    async fn test_credential_check_spends_only_the_budget_of_the_client_that_guessed() {
        let auth = test_auth_state();
        let attacker = test_client(1);
        let operator = test_client(2);

        // Drawn down the way a flood of wrong guesses draws it down, without
        // paying [`AUTH_FAILURE_DELAY`] for each one — which would hand the
        // bucket a refill per guess and never saturate it.
        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(auth.throttle.spend(interactive(attacker)));
        }
        assert!(matches!(auth.check_credentials(attacker, "admin", "password").await, CredentialCheck::Saturated));

        assert!(matches!(auth.check_credentials(operator, "admin", "password").await, CredentialCheck::Accepted(_)));
        // And an anonymous flood with no address at all — everything behind one
        // proxy, or a router nested without connection info — cannot spend the
        // budget of a client that has one either.
        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(auth.throttle.spend(interactive(AuthClient::Any)));
        }
        assert!(matches!(
            auth.check_credentials(AuthClient::Any, "admin", "password").await,
            CredentialCheck::Saturated
        ));
        assert!(matches!(auth.check_credentials(operator, "admin", "password").await, CredentialCheck::Accepted(_)));
    }

    /// Basic-auth traffic is anybody's to send, so it must not be able to spend
    /// the budget the login form needs — the operator's only way in when they
    /// hold no session.
    #[tokio::test]
    async fn test_basic_auth_guessing_leaves_the_login_form_budget_alone() {
        let auth = test_auth_state();
        let client = AuthClient::Any;
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Basic d3Jvbmc6Y3JlZHM="));

        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(auth.throttle.spend(AuthThrottleKey { client, channel: AuthChannel::Basic }));
        }
        assert!(matches!(auth.check_basic_credentials(client, &headers).await, CredentialCheck::Saturated));
        assert!(matches!(auth.check_credentials(client, "admin", "password").await, CredentialCheck::Accepted(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn test_auth_throttle_refills_one_attempt_per_interval() {
        let throttle = AuthThrottle::new();
        let key = interactive(test_client(1));
        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(throttle.spend(key));
        }
        assert!(!throttle.spend(key));

        tokio::time::advance(AUTH_ATTEMPT_REFILL - Duration::from_millis(1)).await;
        assert!(!throttle.spend(key), "a partial interval refills nothing");
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(throttle.spend(key), "one interval refills one attempt");
        assert!(!throttle.spend(key));

        // An idle client accumulates at most a full burst, however long it waited, and a refund never pushes it past
        // that ceiling either: two refunds against one spend from a full bucket leave it full, not over. (A refund
        // alone lands on the stored count, which only a spend refills, so refunding the empty bucket above would prove
        // nothing.)
        tokio::time::advance(AUTH_ATTEMPT_REFILL * (AUTH_ATTEMPT_BURST + 10)).await;
        assert!(throttle.spend(key));
        throttle.refund(key);
        throttle.refund(key);
        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(throttle.spend(key));
        }
        assert!(!throttle.spend(key));
    }

    /// Tracking a bucket per client cannot become a way to make this process
    /// allocate without bound, and the eviction it does instead must not hand a
    /// throttled client its burst back.
    #[tokio::test(start_paused = true)]
    async fn test_auth_throttle_bounds_its_buckets_without_reviving_a_saturated_one() {
        let throttle = AuthThrottle::new();
        let saturated = interactive(AuthClient::Any);
        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(throttle.spend(saturated));
        }

        for index in 0..MAX_AUTH_CLIENTS as u32 * 2 {
            let key = interactive(AuthClient::Peer(std::net::IpAddr::from(index.to_be_bytes())));
            // Two attempts each, so no bucket is full and eviction has to choose
            // among partly spent ones.
            assert!(throttle.spend(key));
            assert!(throttle.spend(key));
        }

        let tracked = throttle.buckets.lock().unwrap().len();
        assert!(tracked <= MAX_AUTH_CLIENTS, "{tracked} buckets tracked");
        assert!(!throttle.spend(saturated), "eviction must not refund the client being throttled");
    }

    /// Cycling identities must not be a way to buy budget back. Once every
    /// bucket is spent, a key that displaces one starts from what that client
    /// had left instead of a fresh burst — while still being granted the single
    /// comparison that keeps an operator arriving into a full map from being
    /// locked out.
    #[tokio::test(start_paused = true)]
    async fn test_auth_throttle_grants_no_burst_to_a_client_that_displaced_a_spent_bucket() {
        let throttle = AuthThrottle::new();
        // Every bucket saturated: the state a flood cycling identities creates,
        // where eviction has nothing but spent buckets to choose from.
        for index in 0..MAX_AUTH_CLIENTS as u32 * 2 {
            let key = interactive(AuthClient::Peer(std::net::IpAddr::from(index.to_be_bytes())));
            while throttle.spend(key) {}
        }
        let tracked = throttle.buckets.lock().unwrap().len();
        assert!(tracked <= MAX_AUTH_CLIENTS, "{tracked} buckets tracked");

        let arriving = interactive(test_client(1));
        assert!(throttle.spend(arriving), "an arriving key must still be heard once");
        assert!(!throttle.spend(arriving), "displacing a spent bucket must not mint a burst");
        // Time, and only time, hands comparisons back.
        tokio::time::advance(AUTH_ATTEMPT_REFILL).await;
        assert!(throttle.spend(arriving));
        assert!(!throttle.spend(arriving));
    }

    /// An IPv6 client is charged to its /64: the whole prefix is normally one
    /// subscriber's, so addresses within it are free to mint and worthless as
    /// identities. IPv4 stays per-address, including the mapped form a
    /// dual-stack listener reports.
    #[test]
    fn test_auth_client_charges_an_ipv6_client_to_its_prefix() {
        let peer = |address: &str| AuthClient::peer(address.parse().unwrap());
        let folded = AuthClient::Peer("2001:db8:1:2::".parse().unwrap());

        assert_eq!(peer("2001:db8:1:2::1"), folded);
        assert_eq!(peer("2001:db8:1:2:ffff:ffff:ffff:ffff"), folded);
        assert_ne!(peer("2001:db8:1:3::1"), folded, "a different /64 is a different client");
        assert_eq!(peer("203.0.113.5"), AuthClient::Peer("203.0.113.5".parse().unwrap()));
        assert_eq!(
            peer("::ffff:203.0.113.5"),
            AuthClient::Peer("203.0.113.5".parse().unwrap()),
            "a mapped IPv4 client keeps its own budget"
        );
        assert_ne!(peer("::ffff:203.0.113.6"), peer("::ffff:203.0.113.5"));
    }

    #[tokio::test]
    async fn test_credential_check_spends_no_budget_when_no_basic_credential_is_supplied() {
        let auth = test_auth_state();
        let client = test_client(1);
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer x"));

        for _ in 0..AUTH_ATTEMPT_BURST * 4 {
            assert!(matches!(auth.check_basic_credentials(client, &headers).await, CredentialCheck::Rejected));
        }
        // A header with nothing to compare cannot be used to hold a budget at
        // zero for the operator who does have credentials.
        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(auth.throttle.spend(AuthThrottleKey { client, channel: AuthChannel::Basic }));
        }
    }

    /// `X-Forwarded-For` is read from the right, so the entry picked is one our
    /// own proxies wrote and everything a client forges is out of reach. A chain
    /// too short to have passed through them all, an entry naming no address,
    /// and the default hop count of zero all fall back to the socket peer.
    #[test]
    fn test_forwarded_client_reads_the_chain_from_the_trusted_end() {
        let chain = |hops: usize, entries: &[&str]| {
            let mut headers = HeaderMap::new();
            for entry in entries {
                headers.append("x-forwarded-for", HeaderValue::from_str(entry).unwrap());
            }
            forwarded_client(&headers, hops)
        };
        let peer = |address: &str| Some(AuthClient::Peer(address.parse().unwrap()));

        // One proxy: the client is the last entry, whatever it claims to the
        // left of it.
        assert_eq!(chain(1, &["203.0.113.5"]), peer("203.0.113.5"));
        assert_eq!(chain(1, &["1.2.3.4, 203.0.113.5"]), peer("203.0.113.5"));
        assert_eq!(chain(1, &["1.2.3.4", "203.0.113.5"]), peer("203.0.113.5"));
        // Two proxies: one more entry back, and the forged prefix stays out.
        assert_eq!(chain(2, &["9.9.9.9, 203.0.113.5, 10.0.0.2"]), peer("203.0.113.5"));
        // Ports and brackets are how proxies write an address, not a client.
        // A forwarded IPv6 client is charged to its /64, like a socket peer.
        assert_eq!(chain(1, &["203.0.113.5:41234"]), peer("203.0.113.5"));
        assert_eq!(chain(1, &["[2001:db8::1]:41234"]), peer("2001:db8::"));
        assert_eq!(chain(1, &["[2001:db8::1]"]), peer("2001:db8::"));
        assert_eq!(chain(1, &["2001:db8::1"]), peer("2001:db8::"));

        // Nothing trustworthy: too short a chain, an unusable entry, no header,
        // and the default that ignores the header entirely.
        assert_eq!(chain(2, &["203.0.113.5"]), None);
        assert_eq!(chain(1, &["unknown"]), None);
        assert_eq!(chain(1, &["_obfuscated"]), None);
        assert_eq!(chain(1, &[" , "]), None);
        assert_eq!(chain(1, &[]), None);
        assert_eq!(chain(0, &["203.0.113.5"]), None);
    }

    /// One unreadable byte in a client-written entry must not cost the trusted
    /// entries beside it. Reading the line through `to_str` rejected all of it,
    /// so the request fell back to the shared socket-peer bucket — a second
    /// budget for a client that had already spent its own, bought with a byte
    /// the proxy passes straight through.
    #[test]
    fn test_forwarded_client_survives_an_unreadable_client_prefix() {
        let mut headers = HeaderMap::new();
        headers.append("x-forwarded-for", HeaderValue::from_bytes(b"\xff\xfe, 198.51.100.9").unwrap());
        assert_eq!(forwarded_client(&headers, 1), Some(AuthClient::Peer("198.51.100.9".parse().unwrap())));
        // And the unreadable entry still names no client when it is the one the
        // hop count selects, exactly as `unknown` does.
        assert_eq!(forwarded_client(&headers, 2), None);
    }

    /// The extractor and `require_auth` must agree, and both must honour the
    /// configured hop count rather than the socket peer alone.
    #[test]
    fn test_auth_state_charges_a_forwarded_client_only_when_a_proxy_is_trusted() {
        let mut extensions = axum::http::Extensions::new();
        let peer: SocketAddr = "10.0.0.7:54321".parse().unwrap();
        extensions.insert(axum::extract::ConnectInfo(peer));
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", HeaderValue::from_static("1.2.3.4, 203.0.113.5"));

        assert_eq!(
            test_auth_state().client_of(&headers, &extensions),
            AuthClient::Peer(peer.ip()),
            "the header is attacker-controlled until a proxy is trusted"
        );
        let proxied = DashboardAuthState::new("admin".into(), "password".into(), String::new(), true, 1);
        assert_eq!(proxied.client_of(&headers, &extensions), AuthClient::Peer("203.0.113.5".parse().unwrap()));
        assert_eq!(
            proxied.client_of(&HeaderMap::new(), &extensions),
            AuthClient::Peer(peer.ip()),
            "a request that carried no chain is still charged to its peer"
        );
    }

    #[test]
    fn test_auth_client_is_the_socket_peer_when_the_server_records_one() {
        let mut extensions = axum::http::Extensions::new();
        assert_eq!(AuthClient::of(&extensions), AuthClient::Any);

        let peer: SocketAddr = "10.0.0.7:54321".parse().unwrap();
        extensions.insert(axum::extract::ConnectInfo(peer));
        assert_eq!(
            AuthClient::of(&extensions),
            AuthClient::Peer(peer.ip()),
            "the port changes per connection, so only the address identifies a client"
        );
    }
}

// Dashboard persistence

/// Dashboard representation with persisted job and cron metadata.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct DashboardJobRow {
    /// The common execution fields.
    #[serde(flatten)]
    pub job: JobRow,
    /// Whether `payload` is the leading [`MAX_BODY_CHARS`] characters of
    /// the stored JSON rather than the value itself.
    pub payload_truncated: bool,
    /// The same, for `result`.
    pub result_truncated: bool,
    /// The same, for `meta`.
    pub meta_truncated: bool,
    /// The same, for `error` — which is `text`, not JSON, so the prefix is the
    /// message itself rather than a rendering of it.
    pub error_truncated: bool,
    /// Most recent enqueue, lifecycle update, or completion time.
    pub updated_at: Timestamp,
}

/// How much of a stored body a dashboard route returns: a job's `payload`,
/// `result`, `meta` and `error`, and a worker's `stats` and `metadata`.
///
/// Library writers cap each document at 1 MiB, and the database also enforces
/// that limit for `error`. Foreign SQL can still put arbitrarily large JSON in
/// the JSONB columns, so every body is cut in PostgreSQL before it reaches this
/// process. See `Database::dashboard_job` for what that does and does not prevent.
/// 65,536 characters is far more than a browser renders usefully and still
/// shows the shape of any realistic body.
///
/// This is a bound on characters, not bytes: `left()` counts characters, so the
/// byte cost is up to four times it under UTF-8. Measured on PostgreSQL 18.4,
/// `left()` over a document of U+1F600 returns 65,536 characters in 262,141
/// bytes — 256 KiB per document, so ~50 MiB for a full worker page rather than
/// the 12.8 MiB reading this as 64 KiB suggests. Still bounded, which is the
/// point, and still four orders of magnitude under the 200 MB payload that
/// motivated it.
const MAX_BODY_CHARS: i32 = 64 * 1024;

/// One bounded `text` column as the dashboard shows it: the string, or the
/// prefix that fit plus the flag that says so.
///
/// The query asked for one character past the cap, so a body that reaches that
/// length is exactly one that did not fit.
fn job_text(text: String) -> (String, bool) {
    match text.char_indices().nth(MAX_BODY_CHARS as usize) {
        Some((cut, _)) => (text[..cut].to_string(), true),
        None => (text, false),
    }
}

/// One body column as the dashboard shows it: the parsed value, or the prefix
/// that fit plus the flag that says so.
fn job_body(text: String) -> (Value, bool) {
    let (text, truncated) = job_text(text);
    if truncated {
        return (Value::String(text), true);
    }
    match serde_json::from_str(&text) {
        Ok(value) => (value, false),
        // `jsonb::text` is always valid JSON, so this is a body the server
        // rendered and this client cannot read back; shown raw rather than
        // dropped or turned into a 500.
        Err(_) => (Value::String(text), false),
    }
}

/// Dashboard list representation without the potentially large job bodies.
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub(crate) struct DashboardJobSummaryRow {
    pub id: Uuid,
    pub dedupe_key: Option<String>,
    pub queue: String,
    pub name: String,
    pub status: JobStatus,
    pub priority: i16,
    pub attempts: i32,
    pub max_attempts: i32,
    pub timeout_ms: Option<i64>,
    pub retry_delay_ms: i64,
    pub backoff: JobRetryBackoff,
    pub result_ttl_ms: Option<i64>,
    pub failed_ttl_ms: Option<i64>,
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    pub scheduled_at: Timestamp,
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    pub enqueued_at: Timestamp,
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    pub started_at: Option<Timestamp>,
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    pub touched_at: Option<Timestamp>,
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    pub completed_at: Option<Timestamp>,
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    pub expires_at: Option<Timestamp>,
    pub worker_id: Option<Uuid>,
    pub kind: String,
    pub cron_expr: Option<String>,
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    pub updated_at: Timestamp,
}

/// Dashboard representation of a worker lease, with its two operator-supplied
/// documents bounded exactly as a job's bodies are.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct DashboardWorkerRow {
    /// The lease as [`Queue::workers_page`] reports it, except that
    /// a document past the cap is the prefix that fit.
    #[serde(flatten)]
    pub worker: WorkerInfo,
    /// Whether `stats` is the leading [`MAX_BODY_CHARS`] characters of the
    /// stored JSON rather than the value itself.
    pub stats_truncated: bool,
    /// The same, for `metadata`.
    pub metadata_truncated: bool,
}

/// Flat SQLx record used to assemble one bounded worker lease.
///
/// `stats` and `metadata` arrive as `text` capped at [`MAX_BODY_CHARS`], not as
/// `jsonb`; see [`job_body`].
#[derive(sqlx::FromRow)]
struct DashboardWorkerRecord {
    id: Uuid,
    queue: String,
    stats: String,
    metadata: Option<String>,
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    started_at: Timestamp,
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    heartbeat_at: Timestamp,
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    expires_at: Timestamp,
}

impl From<DashboardWorkerRecord> for DashboardWorkerRow {
    fn from(row: DashboardWorkerRecord) -> Self {
        let (stats, stats_truncated) = job_body(row.stats);
        let (metadata, metadata_truncated) = match row.metadata {
            Some(metadata) => {
                let (metadata, truncated) = job_body(metadata);
                (Some(metadata), truncated)
            }
            None => (None, false),
        };
        Self {
            worker: WorkerInfo {
                id: row.id,
                queue: row.queue,
                stats,
                metadata,
                started_at: row.started_at,
                heartbeat_at: row.heartbeat_at,
                expires_at: row.expires_at,
            },
            stats_truncated,
            metadata_truncated,
        }
    }
}

/// Flat SQLx record used to assemble the full dashboard job detail.
///
/// The three body columns arrive as `text` capped at [`MAX_BODY_CHARS`],
/// not as `jsonb`; see [`job_body`].
#[derive(sqlx::FromRow)]
struct DashboardJobRecord {
    id: Uuid,
    dedupe_key: Option<String>,
    queue: String,
    name: String,
    payload: String,
    status: JobStatus,
    priority: i16,
    attempts: i32,
    refunds: i32,
    max_attempts: i32,
    timeout_ms: Option<i64>,
    retry_delay_ms: i64,
    backoff: JobRetryBackoff,
    result_ttl_ms: Option<i64>,
    failed_ttl_ms: Option<i64>,
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    scheduled_at: Timestamp,
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    enqueued_at: Timestamp,
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    started_at: Option<Timestamp>,
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    touched_at: Option<Timestamp>,
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    completed_at: Option<Timestamp>,
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    expires_at: Option<Timestamp>,
    result: Option<String>,
    error: Option<String>,
    meta: String,
    worker_id: Option<Uuid>,
    kind: String,
    cron_expr: Option<String>,
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    retried_at: Option<Timestamp>,
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    updated_at: Timestamp,
}

impl From<DashboardJobRecord> for DashboardJobRow {
    fn from(row: DashboardJobRecord) -> Self {
        let (payload, payload_truncated) = job_body(row.payload);
        let (result, result_truncated) = match row.result {
            Some(result) => {
                let (result, truncated) = job_body(result);
                (Some(result), truncated)
            }
            None => (None, false),
        };
        let (meta, meta_truncated) = job_body(row.meta);
        let (error, error_truncated) = match row.error {
            Some(error) => {
                let (error, truncated) = job_text(error);
                (Some(error), truncated)
            }
            None => (None, false),
        };
        Self {
            job: JobRow {
                id: row.id,
                dedupe_key: row.dedupe_key,
                queue: row.queue,
                name: row.name,
                payload,
                status: row.status,
                priority: row.priority,
                attempts: row.attempts,
                refunds: row.refunds,
                max_attempts: row.max_attempts,
                timeout_ms: row.timeout_ms,
                retry_delay_ms: row.retry_delay_ms,
                backoff: row.backoff,
                result_ttl_ms: row.result_ttl_ms,
                failed_ttl_ms: row.failed_ttl_ms,
                scheduled_at: row.scheduled_at,
                enqueued_at: row.enqueued_at,
                started_at: row.started_at,
                touched_at: row.touched_at,
                completed_at: row.completed_at,
                expires_at: row.expires_at,
                result,
                error,
                meta,
                worker_id: row.worker_id,
                kind: row.kind,
                cron_expr: row.cron_expr,
                retried_at: row.retried_at,
            },
            payload_truncated,
            result_truncated,
            meta_truncated,
            error_truncated,
            updated_at: row.updated_at,
        }
    }
}

/// Bounded operational signals used instead of exact retained-job counts.
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub(crate) struct DashboardQueueSignals {
    /// Queue name.
    pub name: String,
    /// When the oldest job ready to dequeue now last became ready: its due time,
    /// or when it was last put back in the queue, if that was later.
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    pub oldest_ready_at: Option<Timestamp>,
    /// Next future-scheduled job.
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    pub next_scheduled_at: Option<Timestamp>,
    /// `running`, `aborting`, or `idle`.
    pub execution: String,
    /// Whether at least one unexpired worker exists.
    pub has_live_workers: bool,
    /// Completion time of the most recent retained failed or aborted job.
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    pub latest_failure_or_abort_at: Option<Timestamp>,
}

/// The queue-signal poll, which every open dashboard runs every 5s per queue.
///
/// The `execution` arm is a single `max()` over `jobs_active_idx` rather than
/// the pair of `EXISTS` probes it reads like. sqlx prepares every statement, so
/// PostgreSQL switches to a *generic* plan whose estimate for
/// `queue = $1 AND status = 'running'` is the table-wide frequency of `running`
/// spread over the average queue: when `running` rows are common elsewhere but
/// absent here, an early-exit sequential scan looks cheap and reads every
/// retained row of the table. `max()` over the partial index is answered by a
/// backward index-only scan with `LIMIT 1`, which is O(1) either way; `'running'
/// > 'aborting'` orders the two correctly, and `max()` of no rows is NULL, so
/// the `ELSE 'idle'` fallback is unchanged.
///
/// `latest_failure_or_abort_at` takes each branch's newest row through
/// `jobs_dashboard_terminal_idx`, so each branch has to exclude a NULL
/// `completed_at` explicitly: nothing in the schema requires a terminal row to
/// carry one — a foreign writer naming neither column lands exactly that — and
/// PostgreSQL sorts `DESC` as NULLS FIRST, so such a row would win the
/// `LIMIT 1` and hide the branch's real latest timestamp. The filter stays an
/// index condition; `ORDER BY ... NULLS LAST` would not, because the index is
/// declared `DESC`.
///
/// `oldest_ready_at` is the same bounded walk [`Queue::counts`] reports, for
/// the reasons [`oldest_ready_at_sql!`](crate::database::oldest_ready_at_sql)
/// gives.
pub(crate) const DASHBOARD_SIGNALS_SQL: &str = concat!(
    r#"
            SELECT
                $1::text AS name,
                "#,
    crate::database::oldest_ready_at_sql!(),
    r#" AS oldest_ready_at,
                (
                    SELECT scheduled_at
                    FROM pgqueue.jobs
                    WHERE queue = $1 AND status = 'queued' AND scheduled_at > now()
                    ORDER BY scheduled_at, id
                    LIMIT 1
                ) AS next_scheduled_at,
                (
                    SELECT CASE max(status)
                        WHEN 'running' THEN 'running'
                        WHEN 'aborting' THEN 'aborting'
                        ELSE 'idle'
                    END
                    FROM pgqueue.jobs
                    WHERE queue = $1 AND status IN ('running', 'aborting')
                ) AS execution,
                EXISTS (
                    SELECT 1 FROM pgqueue.workers
                    WHERE queue = $1 AND expires_at > now()
                    LIMIT 1
                ) AS has_live_workers,
                (
                    SELECT max(completed_at)
                    FROM (
                        (SELECT completed_at
                         FROM pgqueue.jobs
                         WHERE queue = $1 AND status = 'failed' AND completed_at IS NOT NULL
                         ORDER BY completed_at DESC, id DESC
                         LIMIT 1)
                        UNION ALL
                        (SELECT completed_at
                         FROM pgqueue.jobs
                         WHERE queue = $1 AND status = 'aborted' AND completed_at IS NOT NULL
                         ORDER BY completed_at DESC, id DESC
                         LIMIT 1)
                    ) AS terminal
                ) AS latest_failure_or_abort_at
            "#
);

/// The `/health` liveness probe, one per configured queue.
///
/// The `ORDER BY` is load-bearing and has to sit *outside* the existence test,
/// which strips it. `EXISTS (SELECT 1 ... WHERE queue = $1 LIMIT 1)` gives the
/// planner no ordering to satisfy, so under the generic plan sqlx's prepared
/// statement settles into, an early-exit sequential scan costed against
/// average-rows-per-queue wins — and for a queue whose rows are not near the
/// front of the heap it then reads the whole table. `/health` is deliberately
/// unauthenticated: [`RoundCache`] bounds how often this
/// runs, not what one run costs, and the cost is otherwise linear in retained
/// history, which [`JobRetention::Forever`](crate::JobRetention::Forever) never
/// bounds. Sorted, it is an index-only scan of `jobs_page_idx`.
pub(crate) const HEALTH_PROBE_SQL: &str = r#"
            SELECT count(*) > 0 FROM (
                SELECT 1 FROM pgqueue.jobs
                WHERE queue = $1
                ORDER BY enqueued_at DESC, id DESC
                LIMIT 1
            ) AS probe
            "#;

/// The job-name typeahead, run once per keystroke.
///
/// A loose index scan (the classic "skip scan" recursion): one index descent
/// per suggestion instead of a scan proportional to how many *rows* carry the
/// matching names. `app.mjs` debounces a keystroke by 250ms and every open tab
/// has its own timer, so this runs against the pool the worker dequeues,
/// heartbeats and finalizes with at whatever rate a held-down key produces; it
/// has to cost the same on a queue retaining 300,000 rows as on one retaining
/// 300. Grouping the matches instead — even bounded by `LIMIT` — cannot: rows
/// arrive from the index clustered by name, so a row bound returns a handful of
/// suggestions on a busy queue, and no row bound at all reads every matching
/// row (300,000 rows, ~6,200 buffers, 87ms per keystroke here, and unbounded
/// under [`JobRetention::Forever`](crate::JobRetention::Forever)).
///
/// The recursion measures 562 buffers and 1.1ms on that same data: PostgreSQL
/// 18.4, 300,000 retained rows over 520 distinct names spread across all six
/// statuses, under `force_generic_plan`, for a prefix broad enough to fill the
/// 20-suggestion budget in every one of them — which is this statement's worst
/// case, since it walks one index descent per suggestion per status. What makes
/// it the right shape is not that the number is small but that it is the *same*
/// number on a queue retaining 300 rows: 6 seeds plus at most 6 × 20 steps,
/// whatever the history behind them.
///
/// Each step asks `jobs_dashboard_name_prefix_idx` for the first row whose
/// folded name is strictly greater than the previous one, so it visits each
/// distinct name once and stops at the first name past the prefix — or after
/// `$5` of them.
///
/// Two details are what keep the index under the *generic* plan sqlx's prepared
/// statements settle into, which is where the previous shape lost it:
///
/// * `~>=~`/`~>~` are the `text_pattern_ops` comparisons the index is built on,
///   and they take a parameter as an index condition. `starts_with` (and `^@`,
///   and `LIKE`) can only become a range when the planner can see the prefix as
///   a *constant*, so they reached this index under a custom plan and demoted
///   to a Filter over a sequential scan under a generic one.
/// * `ORDER BY ... USING ~<~` asks for the index's own ordering rather than the
///   collation's, so each step is a one-row index scan instead of a sort of the
///   whole remaining range.
///
/// The statuses are per-status laterals rather than one `= ANY`: an array
/// membership test is not equality, so under it the index cannot deliver the
/// `lower(name)` ordering each step needs. `starts_with` stays as the exact
/// filter, since `~>=~` alone is only a lower bound.
///
/// One consequence: names that differ only in case fold to one index key, so
/// within a status only one of them is suggested. The walks are independent
/// across statuses, though, and each may land on a different variant of the same
/// folded key — and the closing `GROUP BY name` groups by the name, not by the
/// fold, so it keeps every variant it was given. A full `$5`-row response can
/// therefore carry fewer than `$5` names once case is ignored. The listing's
/// `?name=` filter is exact, so every variant stays reachable by typing it out.
pub(crate) const JOB_NAME_TYPEAHEAD_SQL: &str = r#"
            WITH RECURSIVE walk AS (
                SELECT requested.status, seed.folded, seed.name, 1 AS depth
                FROM unnest($2::text[]) AS requested(status)
                CROSS JOIN LATERAL (
                    SELECT lower(j.name) AS folded, j.name
                    FROM pgqueue.jobs j
                    WHERE j.queue = $1
                      AND j.kind = $3
                      AND j.status = requested.status
                      AND lower(j.name) ~>=~ lower($4::text)
                    ORDER BY lower(j.name) USING ~<~
                    LIMIT 1
                ) seed
                UNION ALL
                SELECT walk.status, next.folded, next.name, walk.depth + 1
                FROM walk
                CROSS JOIN LATERAL (
                    SELECT lower(j.name) AS folded, j.name
                    FROM pgqueue.jobs j
                    WHERE j.queue = $1
                      AND j.kind = $3
                      AND j.status = walk.status
                      AND lower(j.name) ~>~ walk.folded
                    ORDER BY lower(j.name) USING ~<~
                    LIMIT 1
                ) next
                -- The step that leaves the prefix behind is the one that ends
                -- the walk, so its row is read and then dropped below.
                WHERE walk.depth < $5
                  AND starts_with(walk.folded, lower($4::text))
            )
            SELECT name
            FROM walk
            WHERE starts_with(folded, lower($4::text))
            GROUP BY name
            ORDER BY lower(name), name
            LIMIT $5
            "#;

/// The job listing's page, given the keyset source to page through and the
/// placeholder that bounds it.
///
/// Two statements share this because the `?name=` filter cannot be a parameter
/// inside one: see [`JOB_PAGE_BY_NAME_SQL`]. Only the `keys` CTE differs, and
/// duplicating a twenty-two column projection to vary one function call is how
/// two statements that must agree stop agreeing.
///
/// The row lookup is a correlated `LATERAL`, and its `LIMIT 1` is what keeps it
/// one: a subquery the planner can flatten is flattened straight back into
/// `JOIN pgqueue.jobs ON jobs.id = keys.id`, which is the shape this replaces.
/// As a join it was costed against `LIMIT $6` — and a generic plan, which is
/// what sqlx's prepared statements settle into, estimates a parameterized
/// `LIMIT` at 10% of its input rather than the 51 rows a page returns. That is
/// past the point where a hash join whose probe side is a bare
/// `Seq Scan on jobs` out-costs one `jobs_pkey` descent per row. Measured on
/// PostgreSQL 18.4 under `force_generic_plan` over a freshly compacted
/// 300,000-row table with 520 distinct names across all six statuses — where
/// the estimate is 1683 — the dashboard's default unfiltered page was
/// `Hash Join ... -> Seq Scan on jobs (rows=300000)`, 5,791 buffers and 31ms:
/// linear in retained history, unbounded under
/// [`JobRetention::Forever`](crate::JobRetention::Forever), on the pool the
/// worker dequeues, heartbeats and finalizes with, once per open tab every 5s.
/// As a lateral it is a nested loop of primary-key lookups on the same data:
/// 225 buffers and 0.26ms, and 424 buffers at the largest page the API allows.
/// `id` is the primary key, so `LIMIT 1` changes no result — only the plan.
///
/// [`JOB_PAGE_BY_NAME_SQL`] already planned that nested loop and still does; it
/// got there because its keys estimate happened to land lower, which is luck.
/// This makes it so by construction for both.
macro_rules! job_page_sql {
    ($keys:literal, $limit:literal) => {
        concat!(
            r#"
            WITH keys AS (
                SELECT enqueued_at, id
                FROM "#,
            $keys,
            r#"
                ORDER BY enqueued_at DESC, id DESC
                LIMIT "#,
            $limit,
            r#"
            )
            SELECT
                jobs.id,
                jobs.dedupe_key,
                jobs.queue,
                jobs.name,
                jobs.status,
                jobs.priority,
                jobs.attempts,
                jobs.max_attempts,
                jobs.timeout_ms,
                jobs.retry_delay_ms,
                jobs.backoff,
                jobs.result_ttl_ms,
                jobs.failed_ttl_ms,
                jobs.scheduled_at,
                jobs.enqueued_at,
                jobs.started_at,
                jobs.touched_at,
                jobs.completed_at,
                jobs.expires_at,
                jobs.worker_id,
                jobs.kind,
                jobs.cron_expr,
                GREATEST(
                    jobs.enqueued_at,
                    COALESCE(jobs.touched_at, jobs.enqueued_at),
                    COALESCE(jobs.completed_at, jobs.enqueued_at)
                ) AS updated_at
            FROM keys
            CROSS JOIN LATERAL (
                SELECT * FROM pgqueue.jobs j WHERE j.id = keys.id LIMIT 1
            ) jobs
            ORDER BY keys.enqueued_at DESC, keys.id DESC
            "#
        )
    };
}

/// The job listing's page with no name filter, riding
/// `jobs_dashboard_status_page_idx`. This is the dashboard's default view, so it
/// is the one polled per open tab; its row lookup is why `job_page_sql!` is a
/// lateral.
pub(crate) const JOB_PAGE_SQL: &str =
    job_page_sql!("pgqueue.job_page_keys($1, $3, $2::text[], $4::timestamptz, $5::uuid, $6)", "$6");

/// The same page with the listing's `?name=` filter applied, riding
/// `jobs_dashboard_name_page_idx`.
///
/// A separate statement, and a separate SQL function, because the filter cannot
/// be optional *inside* one. `(p_name IS NULL OR j.name = p_name)` only becomes
/// an index condition if the planner folds the parameter into a constant, and
/// the generic plan sqlx's prepared statements settle into never does. Measured
/// on PostgreSQL 18.4 over 350,000 retained rows, this page across all six
/// statuses under `force_generic_plan`: as one function it was an
/// `Index Scan using jobs_dashboard_status_page_idx` with
/// `Filter: (($4 IS NULL) OR (name = $4))` and `Rows Removed by Filter: 35556`
/// — 29,422 buffers and 105 ms, linear in retained history and unbounded under
/// [`JobRetention::Forever`](crate::JobRetention::Forever), on the pool the
/// worker dequeues and finalizes with. Split, the name is in the Index Cond of
/// an `Index Only Scan using jobs_dashboard_name_page_idx`: 374 buffers and
/// 0.9 ms. `plan_cache_mode = auto` kept the fast path only because the custom
/// plan happened to cost less; `force_generic_plan` — a common cure for
/// planning overhead — lost it.
pub(crate) const JOB_PAGE_BY_NAME_SQL: &str =
    job_page_sql!("pgqueue.job_page_keys_by_name($1, $3, $2::text[], $4::text, $5::timestamptz, $6::uuid, $7)", "$7");

impl Database {
    pub(crate) async fn dashboard_jobs_page(
        &self,
        statuses: &[JobStatus],
        kind: &str,
        name: Option<&str>,
        cursor: Option<(Timestamp, Uuid)>,
        limit: i64,
    ) -> Result<Vec<DashboardJobSummaryRow>, Error> {
        if statuses.is_empty() {
            return Err(Error::Config("dashboard jobs page requires at least one status".into()));
        }
        if limit <= 0 {
            return Err(Error::Config("dashboard jobs page limit must be greater than zero".into()));
        }
        let statuses = statuses.iter().map(|status| status.as_str().to_owned()).collect::<Vec<_>>();
        let (cursor_time, cursor_id) = cursor.unzip();
        // The filter picks the statement rather than a parameter within one, so
        // the name reaches its index as an equality.
        let page = match name {
            Some(name) => sqlx::query_as::<_, DashboardJobSummaryRow>(JOB_PAGE_BY_NAME_SQL)
                .bind(self.name())
                .bind(&statuses)
                .bind(kind)
                .bind(name)
                .bind(cursor_time.map(|timestamp| timestamp.to_sqlx()))
                .bind(cursor_id)
                .bind(limit),
            None => sqlx::query_as::<_, DashboardJobSummaryRow>(JOB_PAGE_SQL)
                .bind(self.name())
                .bind(&statuses)
                .bind(kind)
                .bind(cursor_time.map(|timestamp| timestamp.to_sqlx()))
                .bind(cursor_id)
                .bind(limit),
        };
        Ok(page.fetch_all(self.pool()).await?)
    }

    pub(crate) async fn dashboard_job_names(
        &self,
        statuses: &[JobStatus],
        kind: &str,
        prefix: &str,
        limit: i64,
    ) -> Result<Vec<String>, Error> {
        let statuses = statuses.iter().map(|status| status.as_str().to_owned()).collect::<Vec<_>>();
        Ok(sqlx::query_scalar::<_, String>(JOB_NAME_TYPEAHEAD_SQL)
            .bind(self.name())
            .bind(&statuses)
            .bind(kind)
            .bind(prefix)
            .bind(limit)
            .fetch_all(self.pool())
            .await?)
    }

    pub(crate) async fn dashboard_job(&self, id: Uuid) -> Result<Option<DashboardJobRow>, Error> {
        let row = sqlx::query_as::<_, DashboardJobRecord>(
            r#"
            -- One character past the cap, so the client can tell a body that
            -- exactly filled it from one that was cut.
            --
            -- The cut is where the process this dashboard runs in stops paying
            -- for the body: it never reaches the wire and never becomes a
            -- `serde_json::Value` here. It is *not* where the body stops being
            -- allocated. The backend still detoasts the whole `jsonb` and
            -- renders it to text before `left` takes its prefix, so a 200 MB
            -- payload is 200 MB of backend memory per concurrent detail
            -- request. The OOM this bound prevents is moved to a process that
            -- has `work_mem`, `max_connections` and an OOM killer of its own
            -- rather than removed, and a database sized for its own workload
            -- absorbs what a worker's heap could not.
            SELECT
                id,
                dedupe_key,
                queue,
                name,
                left(payload::text, $3 + 1) AS payload,
                status,
                priority,
                attempts,
                refunds,
                max_attempts,
                timeout_ms,
                retry_delay_ms,
                backoff,
                result_ttl_ms,
                failed_ttl_ms,
                scheduled_at,
                enqueued_at,
                started_at,
                touched_at,
                completed_at,
                expires_at,
                left(result::text, $3 + 1) AS result,
                -- Cut like the three `jsonb` bodies, and for the same reason:
                -- Stored errors can reach 1 MiB, which is still much more than
                -- an operator page should transfer and render.
                left(error, $3 + 1) AS error,
                left(meta::text, $3 + 1) AS meta,
                worker_id,
                kind,
                cron_expr,
                retried_at,
                GREATEST(
                    enqueued_at,
                    COALESCE(touched_at, enqueued_at),
                    COALESCE(completed_at, enqueued_at)
                ) AS updated_at
            FROM pgqueue.jobs
            WHERE id = $1 AND queue = $2
            "#,
        )
        .bind(id)
        .bind(self.name())
        .bind(MAX_BODY_CHARS)
        .fetch_optional(self.pool())
        .await?;

        Ok(row.map(DashboardJobRow::from))
    }

    pub(crate) async fn dashboard_signals(&self) -> Result<DashboardQueueSignals, Error> {
        Ok(sqlx::query_as::<_, DashboardQueueSignals>(DASHBOARD_SIGNALS_SQL)
            .bind(self.name())
            .fetch_one(self.pool())
            .await?)
    }

    pub(crate) async fn dashboard_probe(&self) -> Result<(), Error> {
        let _ = sqlx::query_scalar::<_, bool>(HEALTH_PROBE_SQL).bind(self.name()).fetch_one(self.pool()).await?;
        Ok(())
    }

    pub(crate) async fn dashboard_workers_page(
        &self,
        cursor: Option<(Timestamp, Uuid)>,
        limit: i64,
    ) -> Result<Vec<DashboardWorkerRow>, Error> {
        if limit <= 0 {
            return Err(Error::Config("dashboard workers page limit must be greater than zero".into()));
        }
        let (cursor_time, cursor_id) = cursor.unzip();
        Ok(sqlx::query_as::<_, DashboardWorkerRecord>(
            r#"
            -- Cut exactly as a job's bodies are, and for the same reason: both
            -- foreign SQL can bypass the library's 1 MiB document cap, and this
            -- returns up to a hundred leases. See `MAX_BODY_CHARS`.
            SELECT
                id,
                queue,
                left(stats::text, $4 + 1) AS stats,
                left(metadata::text, $4 + 1) AS metadata,
                started_at,
                heartbeat_at,
                expires_at
            FROM pgqueue.workers
            WHERE queue = $1
              AND expires_at > now()
              AND ($2::timestamptz IS NULL OR (started_at, id) > ($2, $3))
            ORDER BY started_at, id
            LIMIT $5
            "#,
        )
        .bind(self.name())
        .bind(cursor_time.map(|timestamp| timestamp.to_sqlx()))
        .bind(cursor_id)
        .bind(MAX_BODY_CHARS)
        .bind(limit)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(DashboardWorkerRow::from)
        .collect())
    }

    pub(crate) async fn dashboard_worker(&self, id: Uuid) -> Result<Option<DashboardWorkerRow>, Error> {
        Ok(sqlx::query_as::<_, DashboardWorkerRecord>(
            r#"
            SELECT
                id,
                queue,
                left(stats::text, $3 + 1) AS stats,
                left(metadata::text, $3 + 1) AS metadata,
                started_at,
                heartbeat_at,
                expires_at
            FROM pgqueue.workers
            WHERE id = $1 AND queue = $2 AND expires_at > now()
            "#,
        )
        .bind(id)
        .bind(self.name())
        .bind(MAX_BODY_CHARS)
        .fetch_optional(self.pool())
        .await?
        .map(DashboardWorkerRow::from))
    }
}
