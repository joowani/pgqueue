//! Dashboard router tests, driven with `tower::ServiceExt::oneshot` — no
//! listener needed.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::{
    EnqueueResultTestExt, QUEUE_SIGNALS_STATEMENT, QueueProtocolTestExt, Stats, TestDb, new_job, test_timers,
    wait_for_some, wait_for_worker_intake_closed, wait_until,
};
use axum::Router;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode, header};
use axum::response::Response;
use pgqueue::{
    CronMisfirePolicy, CronOptions, Dashboard, Error, JobRequest, JobState, JobStatus, Queue, Worker,
    WorkerHealthStatus, WorkerTimers,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use uuid::Uuid;

#[pgqueue::job]
async fn dashboard_probe(_: ()) {}

#[pgqueue::job]
async fn dashboard_slow(_: (), state: JobState<DashboardDrain>) {
    state.0.started.notify_one();
    state.0.release.notified().await;
}

#[derive(Clone)]
struct DashboardDrain {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

fn dashboard(queues: impl IntoIterator<Item = Queue>) -> Dashboard {
    Dashboard::new(queues).allow_unauthenticated()
}

async fn get_json(router: &Router, path: &str) -> (StatusCode, Value) {
    request(router, "GET", path, None).await
}

async fn post_json(router: &Router, path: &str) -> (StatusCode, Value) {
    request(router, "POST", path, None).await
}

pub(crate) async fn request(router: &Router, method: &str, path: &str, auth: Option<&str>) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(auth) = auth {
        builder = builder.header(header::AUTHORIZATION, auth);
    }
    if method == "POST" {
        builder = builder.header("x-pgqueue-request", "dashboard");
    }
    let response = router.clone().oneshot(builder.body(Body::empty()).unwrap()).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::String(String::from_utf8_lossy(&bytes).to_string()));
    (status, value)
}

async fn login_cookie(router: &Router) -> String {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("username=admin&password=s3cretpw"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    response.headers()[header::SET_COOKIE].to_str().unwrap().split(';').next().unwrap().to_string()
}

async fn http_get(address: SocketAddr, path: &str, auth: Option<&str>) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut stream = loop {
        match tokio::net::TcpStream::connect(address).await {
            Ok(stream) => break stream,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("dashboard did not listen at {address}: {error}"),
        }
    };
    let auth = auth.map(|value| format!("Authorization: {value}\r\n")).unwrap_or_default();
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: {address}\r\n{auth}Connection: close\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

/// Whether the dashboard that was listening on `address` has stopped serving.
///
/// Not a bare `TcpStream::connect(..).is_err()`. `serve_on(.., 0)` takes an ephemeral port and gives it back on
/// shutdown, and the operating system is free to hand the same one straight to any other test binding `127.0.0.1:0` —
/// including this file's own bind-failure test, whose listener accepts connections and answers nothing. Connecting to a
/// reuser like that reads as a dashboard that never stopped, and fails an assertion about a server that really did.
/// Requiring an HTTP answer asks what the assertion means instead: a refused connection is gone, and so is a peer that
/// will not answer `/health` within a second.
///
/// Any status counts as an answer, not only `200`: a worker-hosted dashboard answers `/health` with `503` once its
/// worker has stopped, so a server leaked past its worker's shutdown — still serving every route, retry and abort
/// included — passed for a stopped one.
async fn dashboard_stopped_serving(address: SocketAddr) -> bool {
    let Ok(Ok(mut stream)) =
        tokio::time::timeout(Duration::from_secs(1), tokio::net::TcpStream::connect(address)).await
    else {
        return true;
    };
    let probe = async {
        stream
            .write_all(format!("GET /health HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n").as_bytes())
            .await
            .ok()?;
        let mut response = String::new();
        stream.read_to_string(&mut response).await.ok()?;
        Some(response)
    };
    !tokio::time::timeout(Duration::from_secs(1), probe)
        .await
        .ok()
        .flatten()
        .is_some_and(|response| response.starts_with("HTTP/1.1 "))
}

#[sqlx::test(migrations = "./migrations")]
async fn test_health_endpoint_reports_ok(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).router().unwrap();
    let (status, body) = get_json(&router, "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Value::String("OK".into()));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_requires_an_explicit_authentication_mode(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let error = Dashboard::new([db.queue.clone()]).router().unwrap_err();
    assert!(
        matches!(&error, Error::Config(message) if message.contains("authentication mode")),
        "unexpected error: {error}"
    );

    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let error = Dashboard::new([db.queue.clone()]).serve_on("127.0.0.1", 0).run_until(shutdown).await.unwrap_err();
    assert!(
        matches!(&error, Error::Config(message) if message.contains("authentication mode")),
        "unexpected error: {error}"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_hosted_health_reports_degraded_worker_components(pool: PgPool) {
    // A pool of the test's own, because the test closes the queue's pool below: closing the one the `#[sqlx::test]`
    // fixture owns stalled the harness's own teardown, often by ten seconds or more, and reported the test as having
    // held onto its pool.
    let db = TestDb::new(crate::pool_with_max(&pool, 5).await).await;
    let key = "dashboard-health-revision";
    // Publish revision 1, then bring up a worker that reuses that revision for a
    // different schedule. The database rejects the definition, which is a real
    // deploy mistake and degrades `Scheduler` health. (A *superseded* revision
    // is not a failure — that is the normal state of a not-yet-upgraded worker.)
    let authority = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(
            "0 0 1 1 *",
            dashboard_probe::job(()).dedupe_key(key),
            CronOptions { revision: 1, misfire: CronMisfirePolicy::default() },
        )
        .timers(test_timers())
        .build()
        .unwrap();
    let authority_shutdown = CancellationToken::new();
    let authority_run = tokio::spawn(authority.run_until(authority_shutdown.clone()));
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "authoritative cron revision was not stored",
        || async {
            sqlx::query_scalar::<_, i64>(
                "SELECT revision FROM pgqueue.cron_schedules WHERE queue = $1 AND dedupe_key = $2",
            )
            .bind(db.queue.name())
            .bind(key)
            .fetch_optional(&pool)
            .await
            .unwrap()
                == Some(1)
        },
    )
    .await;
    authority_shutdown.cancel();
    authority_run.await.unwrap().unwrap();

    let dashboard = dashboard([db.queue.clone()]).serve_on("127.0.0.1", 0);
    let mut dashboard_handle = dashboard.server_handle();
    let lower = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(
            "0 0 2 1 *",
            dashboard_probe::job(()).dedupe_key(key),
            CronOptions { revision: 1, misfire: CronMisfirePolicy::default() },
        )
        .timers(WorkerTimers { schedule: Duration::from_millis(50), ..test_timers() })
        .dashboard(dashboard)
        .build()
        .unwrap();
    let health = lower.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(lower.run_until(shutdown.clone()));
    let address =
        tokio::time::timeout(Duration::from_secs(5), dashboard_handle.wait_until_ready()).await.unwrap().unwrap();
    wait_until(Duration::from_secs(5), Duration::from_millis(10), "lower revision worker did not degrade", || async {
        health.snapshot().status == WorkerHealthStatus::Degraded
    })
    .await;

    // A degraded component remains ready while its queue is reachable, and the
    // degradation is still visible in the body and in `Worker::health`.
    let response = http_get(address, "/health", None).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("DEGRADED"), "{response}");

    // Degradation must not bypass the database half of the readiness check.
    // Let the successful probe age out, then make the shared queue pool fail
    // immediately without depending on an external Postgres outage.
    db.queue.pool().close().await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let response = http_get(address, "/health", None).await;
    assert!(response.starts_with("HTTP/1.1 500"), "{response}");
    assert!(response.contains("unhealthy"), "{response}");
    shutdown.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_queues_overview_lists_bounded_signals_and_workers_pages(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(new_job("a", |_| {})).await.unwrap();
    let worker_id = Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({"complete": 1}), None, Duration::from_secs(60)).await.unwrap();

    let router = dashboard([db.queue.clone()]).router().unwrap();
    let (status, body) = get_json(&router, "/api/queues").await;
    assert_eq!(status, StatusCode::OK);
    let queues = body["queues"].as_array().unwrap();
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0]["name"], "default");
    assert!(queues[0]["oldest_ready_at"].is_string());
    assert_eq!(queues[0]["execution"], "idle");
    assert_eq!(queues[0]["has_live_workers"], true);
    assert!(queues[0]["latest_failure_or_abort_at"].is_null());

    let (status, body) = get_json(&router, "/api/queues/default/workers?limit=1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["workers"].as_array().unwrap().len(), 1);
    assert!(body["next_cursor"].is_null());

    let (status, body) = get_json(&router, &format!("/api/queues/default/workers/{worker_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["worker"]["id"], worker_id.to_string());

    let missing = Uuid::now_v7();
    let (status, _) = get_json(&router, &format!("/api/queues/default/workers/{missing}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_pages_accept_non_object_stats(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let null_stats_worker = Uuid::now_v7();
    db.queue.write_worker_info(null_stats_worker, Value::Null, None, Duration::from_secs(60)).await.unwrap();
    let scalar_stats_worker = Uuid::now_v7();
    db.queue.write_worker_info(scalar_stats_worker, json!(7), None, Duration::from_secs(60)).await.unwrap();

    let router = dashboard([db.queue.clone()]).router().unwrap();
    let (status, body) = get_json(&router, "/api/queues/default/workers").await;
    assert_eq!(status, StatusCode::OK);
    let workers = body["workers"].as_array().unwrap();
    assert!(workers.iter().any(|worker| worker["stats"].is_null()));
    assert!(workers.iter().any(|worker| worker["stats"] == 7));

    let (status, body) = get_json(&router, &format!("/api/queues/default/workers/{null_stats_worker}")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["worker"]["stats"].is_null());

    let (status, body) = get_json(&router, &format!("/api/queues/default/workers/{scalar_stats_worker}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["worker"]["stats"], 7);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_pages_use_cursors_without_exact_totals(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for complete in 1..=3 {
        db.queue
            .write_worker_info(Uuid::now_v7(), json!({"complete": complete}), None, Duration::from_secs(60))
            .await
            .unwrap();
    }
    let router = dashboard([db.queue.clone()]).router().unwrap();

    let (status, first) = get_json(&router, "/api/queues/default/workers?limit=2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["workers"].as_array().unwrap().len(), 2);
    assert!(first.get("total").is_none());
    let first_ids = first["workers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|worker| worker["id"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    let cursor = first["next_cursor"].as_object().unwrap();
    let cursor_time = cursor["started_at"].as_str().unwrap();
    let cursor_id = cursor["id"].as_str().unwrap();

    let (status, second) = get_json(
        &router,
        &format!("/api/queues/default/workers?limit=2&cursor_started_at={cursor_time}&cursor_id={cursor_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["workers"].as_array().unwrap().len(), 1);
    assert!(second["next_cursor"].is_null());
    assert!(!first_ids.contains(&second["workers"][0]["id"].as_str().unwrap().to_owned()));

    let (status, _) =
        get_json(&router, "/api/queues/default/workers?cursor_id=00000000-0000-0000-0000-000000000000").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_queue_signals_report_ready_scheduled_execution_and_terminal_states(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(new_job("ready", |_| {})).await.unwrap();
    db.queue.enqueue(dashboard_probe::job(()).delay(Duration::from_secs(3_600))).await.unwrap().unwrap();
    let mut running = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
    let running = running.remove(0);
    db.queue.enqueue_raw(new_job("failure", |_| {})).await.unwrap();
    let mut failed = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
    let failed = failed.remove(0);
    assert!(db.queue.finish(&failed, JobStatus::Failed, None, Some("test failure")).await.unwrap());
    let failed_at = db.queue.fetch_job(failed.id).await.unwrap().unwrap().completed_at.unwrap();
    // Nothing in the schema requires a terminal row to carry a `completed_at`, and a foreign writer
    // naming neither column lands exactly this one. `ORDER BY completed_at DESC` sorts it first.
    sqlx::query("INSERT INTO pgqueue.jobs (queue, name, status) VALUES ($1, 'foreign-failure', 'failed')")
        .bind(db.queue.name())
        .execute(&pool)
        .await
        .unwrap();

    // The signal is `max()` over a failed branch and an aborted branch, so once an aborted job exists
    // the failed branch can return nothing and the assertion below still holds. A router of its own,
    // because the fan-out is served from a shared 1s round.
    let (_, body) = get_json(&dashboard([db.queue.clone()]).router().unwrap(), "/api/queues").await;
    assert_eq!(body["queues"][0]["latest_failure_or_abort_at"], failed_at.to_string());

    let aborted = db.queue.enqueue_raw(new_job("aborted", |_| {})).await.unwrap().unwrap();
    assert!(db.queue.abort_job(aborted, "test abort").await.unwrap());
    let aborted_at = db.queue.fetch_job(aborted).await.unwrap().unwrap().completed_at.unwrap();

    let router = dashboard([db.queue.clone()]).router().unwrap();
    let (status, body) = get_json(&router, "/api/queues").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["queues"][0]["oldest_ready_at"].is_null());
    assert!(body["queues"][0]["next_scheduled_at"].is_string());
    assert_eq!(body["queues"][0]["execution"], "running");
    assert_eq!(body["queues"][0]["latest_failure_or_abort_at"], aborted_at.to_string());

    assert!(db.queue.abort_job(running.id, "dashboard signal test").await.unwrap());
    // The overview is served from a shared 1s round, so this repeats until the
    // cached fan-out ages out — the same way an open dashboard's 5s poll sees
    // the change.
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(50),
        "the queue overview never reported the aborting execution",
        || async {
            let (_, body) = get_json(&router, "/api/queues").await;
            body["queues"][0]["execution"] == "aborting"
        },
    )
    .await;

    let (status, _) = get_json(&router, "/api/queues/default").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_jobs_listing_filters_by_status_and_name(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for name in ["alpha", "alpha", "beta"] {
        db.queue.enqueue_raw(new_job(name, |_| {})).await.unwrap();
    }
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();

    let router = dashboard([db.queue.clone()]).router().unwrap();

    let (_, body) = get_json(&router, "/api/queues/default/jobs").await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 3);

    let (_, body) = get_json(&router, "/api/queues/default/jobs?status=queued").await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 2);

    let (_, body) = get_json(&router, "/api/queues/default/jobs?status=queued,running").await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 3);

    let (_, body) = get_json(&router, "/api/queues/default/jobs?status=queued,queued").await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 2);

    let (_, body) = get_json(&router, "/api/queues/default/jobs?status=").await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 3);

    let (_, body) = get_json(&router, "/api/queues/default/jobs?name=beta").await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 1);

    let (_, body) = get_json(&router, "/api/queues/default/jobs?name=ALP").await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 0, "job listing uses an exact handler name");

    let (_, body) = get_json(&router, "/api/queues/default/jobs?limit=1").await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 1);
    let first_id = body["jobs"][0]["id"].as_str().unwrap();
    let cursor = body["next_cursor"].as_object().unwrap();
    let cursor_time = cursor["enqueued_at"].as_str().unwrap();
    let cursor_id = cursor["id"].as_str().unwrap();
    let (_, body) = get_json(
        &router,
        &format!("/api/queues/default/jobs?limit=1&cursor_enqueued_at={cursor_time}&cursor_id={cursor_id}"),
    )
    .await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 1);
    assert_ne!(body["jobs"][0]["id"], first_id);

    let (_, body) = get_json(&router, "/api/queues/default/job-names?kind=job&prefix=ALP").await;
    assert_eq!(body["names"], json!(["alpha"]));

    let (status, _) =
        get_json(&router, "/api/queues/default/jobs?cursor_id=00000000-0000-0000-0000-000000000000").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = get_json(&router, "/api/queues/default/jobs?status=bogus").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = get_json(&router, "/api/queues/default/jobs?status=queued,bogus").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = get_json(&router, "/api/queues/default/jobs?status=active").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = get_json(&router, "/api/queues/default/jobs?kind=bogus").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = get_json(&router, "/api/queues/default/job-names?kind=bogus&prefix=job").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = get_json(&router, "/api/queues/default/jobs?offset=1").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = get_json(&router, "/api/queues/default/jobs?updated_within=60").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// The listing pages by keyset cursor, and the `?name=` filter swaps in a
/// separate statement — `JOB_PAGE_BY_NAME_SQL` over the seven-argument
/// `pgqueue.job_page_keys_by_name` — with its own cursor binds. A single-page
/// request keeps both green through a swapped cursor bind or a reversed
/// `ORDER BY`, so both paths follow their cursors to exhaustion here and the
/// exact newest-first id sequence is asserted.
#[sqlx::test(migrations = "./migrations")]
async fn test_jobs_listing_pages_newest_first_with_and_without_name_filter(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // Distinct `enqueued_at` per row so the expected order is set by timestamp
    // rather than insertion luck; a decoy inside the span so the filtered walk
    // has to skip it mid-sequence; statuses straddling two values so each page
    // draws more candidate keys than it keeps and the re-sort of the per-status
    // union decides which survive; one status holding more rows of the
    // filtered name than a page fetches, so each per-status lateral's LIMIT
    // truncates and its own ordering decides which keys are offered at all.
    let mut newest_first = Vec::new();
    for (name, status, minutes_ago) in [
        ("walk", "complete", 1),
        ("noise", "failed", 2),
        ("walk", "failed", 3),
        ("walk", "complete", 4),
        ("walk", "complete", 5),
    ] {
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO pgqueue.jobs (queue, name, status, kind, enqueued_at)
             VALUES ('default', $1, $2, 'job', now() - $3 * interval '1 minute')
             RETURNING id",
        )
        .bind(name)
        .bind(status)
        .bind(minutes_ago)
        .fetch_one(&pool)
        .await
        .unwrap();
        newest_first.push((name, id.to_string()));
    }
    let all = newest_first.iter().map(|(_, id)| id.clone()).collect::<Vec<_>>();
    let walk = newest_first.iter().filter(|(name, _)| *name == "walk").map(|(_, id)| id.clone()).collect::<Vec<_>>();

    let router = dashboard([db.queue.clone()]).router().unwrap();
    for (base, expected) in [
        ("/api/queues/default/jobs?limit=1", all),
        ("/api/queues/default/jobs?name=walk&limit=1", walk),
    ] {
        let mut seen = Vec::new();
        let mut url = base.to_string();
        // Bounded so a cursor that never advances cannot hang the test.
        for _ in 0..=expected.len() {
            let (status, body) = get_json(&router, &url).await;
            assert_eq!(status, StatusCode::OK, "{url} answered {body}");
            for job in body["jobs"].as_array().unwrap() {
                seen.push(job["id"].as_str().unwrap().to_string());
            }
            let Some(cursor) = body["next_cursor"].as_object() else {
                break;
            };
            let time = cursor["enqueued_at"].as_str().unwrap();
            let id = cursor["id"].as_str().unwrap();
            url = format!("{base}&cursor_enqueued_at={time}&cursor_id={id}");
        }
        assert_eq!(seen, expected, "{base} must page newest-first");
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_jobs_list_omits_bodies_while_detail_includes_them(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db.queue.enqueue_raw(new_job("body-shape", |_| {})).await.unwrap().unwrap();
    let router = dashboard([db.queue.clone()]).router().unwrap();

    let (status, body) = get_json(&router, "/api/queues/default/jobs").await;
    assert_eq!(status, StatusCode::OK);
    let summary = body["jobs"][0].as_object().unwrap();
    assert_eq!(summary["id"], id.to_string());
    for field in ["payload", "result", "error", "meta"] {
        assert!(!summary.contains_key(field), "list summary unexpectedly included {field}");
    }

    let (status, body) = get_json(&router, &format!("/api/queues/default/jobs/{id}")).await;
    assert_eq!(status, StatusCode::OK);
    let detail = body["job"].as_object().unwrap();
    for field in ["payload", "result", "error", "meta"] {
        assert!(detail.contains_key(field), "job detail omitted required field {field}");
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_separates_jobs_and_crons(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue(dashboard_probe::job(())).await.unwrap().unwrap();
    let shutdown = CancellationToken::new();
    let worker = Worker::builder(db.queue.clone())
        .schedule_cron("0 * * * *", dashboard_probe::job(()).dedupe_key("custom-dashboard-cron"))
        .timers(test_timers())
        .poll_interval(Duration::from_millis(20))
        .dequeue_timeout(Duration::from_millis(50))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    let router = dashboard([db.queue.clone()]).router().unwrap();

    wait_until(Duration::from_secs(5), Duration::from_millis(20), "cron schedule was not reconciled", || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                    SELECT 1 FROM pgqueue.cron_schedules
                    WHERE queue = $1 AND dedupe_key = $2
                )",
        )
        .bind(db.queue.name())
        .bind("custom-dashboard-cron")
        .fetch_one(&pool)
        .await
        .unwrap()
    })
    .await;
    sqlx::query(
        "UPDATE pgqueue.cron_schedules
         SET next_run_at = clock_timestamp()
         WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind("custom-dashboard-cron")
    .execute(&pool)
    .await
    .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let cron = loop {
        let (_, body) = get_json(&router, "/api/queues/default/jobs?kind=cron").await;
        if let Some(cron) = body["jobs"].as_array().and_then(|jobs| jobs.first()) {
            break cron.clone();
        }
        assert!(tokio::time::Instant::now() < deadline, "cron row did not appear");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    assert_eq!(cron["kind"], "cron");
    assert_eq!(cron["cron_expr"], "0 * * * *");
    assert_eq!(cron["dedupe_key"], "custom-dashboard-cron");
    let (_, body) = get_json(&router, "/api/queues/default/jobs?kind=job").await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 1);
    assert_eq!(body["jobs"][0]["kind"], "job");
    assert!(body["jobs"][0]["cron_expr"].is_null());

    let id = cron["id"].as_str().unwrap();
    let (_, body) = get_json(&router, &format!("/api/queues/default/jobs/{id}")).await;
    assert_eq!(body["job"]["kind"], "cron");
    assert_eq!(body["job"]["cron_expr"], "0 * * * *");
    // The detail is `{"job": ...}`, so the field would sit on the job.
    assert!(body["job"].get("cron_description").is_none());

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), run).await.unwrap().unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_job_detail_retry_and_abort_actions(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db.queue.enqueue_raw(new_job("j", |_| {})).await.unwrap().unwrap();
    let router = dashboard([db.queue.clone()]).router().unwrap();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/queues/default/jobs/{id}/abort"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN, "CSRF guard");

    let (status, body) = get_json(&router, &format!("/api/queues/default/jobs/{id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["job"]["id"], json!(id.to_string()));
    assert_eq!(body["job"]["status"], "queued");

    // Abort the queued job from the dashboard.
    let (status, body) = post_json(&router, &format!("/api/queues/default/jobs/{id}/abort")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["aborted"], true);
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("aborted from dashboard"));

    // Retry it as a fresh occurrence, preserving the terminal row.
    let (status, body) = post_json(&router, &format!("/api/queues/default/jobs/{id}/retry")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["retried"], true);
    let retry_id: Uuid = body["job_id"].as_str().unwrap().parse().unwrap();
    assert_ne!(retry_id, id);
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Aborted);
    let row = db.queue.fetch_job(retry_id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued);

    // Retrying a queued job is a no-op.
    let (_, body) = post_json(&router, &format!("/api/queues/default/jobs/{retry_id}/retry")).await;
    assert_eq!(body["retried"], false);

    // Missing job.
    let missing = Uuid::now_v7();
    let (status, _) = get_json(&router, &format!("/api/queues/default/jobs/{missing}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_mutations_are_scoped_to_the_route_queue(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|builder| builder.name("other")).await;
    let id = db.queue.enqueue_raw(new_job("owned", |_| {})).await.unwrap().unwrap();
    let router = dashboard([db.queue.clone(), other]).router().unwrap();

    let (status, body) = post_json(&router, &format!("/api/queues/other/jobs/{id}/abort")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["aborted"], false);
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Queued);

    let (_, body) = post_json(&router, &format!("/api/queues/default/jobs/{id}/abort")).await;
    assert_eq!(body["aborted"], true);
    let (status, body) = post_json(&router, &format!("/api/queues/other/jobs/{id}/retry")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["retried"], false);
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Aborted);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_retry_reruns_a_cron_occurrence_when_the_next_occurrence_is_live(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // A failed cron occurrence...
    let failed = db.queue.enqueue_raw(new_job("tick", |_| {})).await.unwrap().unwrap();
    sqlx::query(
        "UPDATE pgqueue.jobs SET kind = 'cron', cron_expr = '* * * * *', \
         dedupe_key = 'cron:tick', status = 'failed', completed_at = now(), \
         error = 'failed: boom' WHERE id = $1",
    )
    .bind(failed)
    .execute(db.queue.pool())
    .await
    .unwrap();
    // ...while the schedule loop has already enqueued the next occurrence
    // under the same dedupe key.
    let next =
        db.queue.enqueue_raw(new_job("tick", |job| job.dedupe_key = Some("cron:tick".into()))).await.unwrap().unwrap();
    sqlx::query("UPDATE pgqueue.jobs SET kind = 'cron', cron_expr = '* * * * *' WHERE id = $1")
        .bind(next)
        .execute(db.queue.pool())
        .await
        .unwrap();

    let router = dashboard([db.queue.clone()]).router().unwrap();
    let (status, body) = post_json(&router, &format!("/api/queues/default/jobs/{failed}/retry")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["retried"], true, "{body}");
    let retry_id: Uuid = body["job_id"].as_str().unwrap().parse().unwrap();

    // The manual rerun is a keyless one-off beside the live next occurrence.
    let rerun = db.queue.fetch_job(retry_id).await.unwrap().unwrap();
    assert_eq!(rerun.status, JobStatus::Queued);
    assert_eq!(rerun.dedupe_key, None);
    assert_eq!(
        db.queue.fetch_job(next).await.unwrap().unwrap().status,
        JobStatus::Queued,
        "the scheduled occurrence is untouched"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_basic_auth_gates_every_route(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let job_id = db.queue.enqueue_raw(new_job("protected", |_| {})).await.unwrap().unwrap();
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    let (status, _) = get_json(&router, "/api/queues").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = get_json(&router, "/").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // echo -n "admin:s3cretpw" | base64 => YWRtaW46czNjcmV0cHc=
    let (status, _) = request(&router, "GET", "/api/queues", Some("Basic YWRtaW46czNjcmV0cHc=")).await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = request(&router, "GET", "/", Some("Basic YWRtaW46czNjcmV0cHc=")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.as_str().unwrap().contains("name=\"pgqueue-user\" content=\"admin\""));

    // RFC 7617: the auth-scheme token is case-insensitive, and more than one
    // space may separate it from the credentials.
    let (status, _) = request(&router, "GET", "/api/queues", Some("basic YWRtaW46czNjcmV0cHc=")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = request(&router, "GET", "/api/queues", Some("BASIC  YWRtaW46czNjcmV0cHc=")).await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = request(&router, "GET", "/api/queues", Some("Basic d3Jvbmc6Y3JlZHM=")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/queues/default/jobs/{job_id}/abort"))
                .header("x-pgqueue-request", "dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        db.queue.fetch_job(job_id).await.unwrap().unwrap().status,
        JobStatus::Queued,
        "unauthenticated mutation reached the queue"
    );

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/queues/default/jobs/{job_id}/abort"))
                .header(header::AUTHORIZATION, "Basic YWRtaW46czNjcmV0cHc=")
                .header("x-pgqueue-request", "dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(db.queue.fetch_job(job_id).await.unwrap().unwrap().status, JobStatus::Aborted);
}

/// A Basic challenge on the dashboard page's own `fetch` makes the browser raise its native credentials dialog over the
/// page — on every session expiry and after every restart — instead of letting the page redirect to the form, and
/// whatever is typed there stays cached past logout. The page marks every request with the action header, so only
/// unmarked clients are challenged.
#[sqlx::test(migrations = "./migrations")]
async fn test_unauthenticated_dashboard_requests_get_no_basic_challenge(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    let response =
        router.clone().oneshot(Request::builder().uri("/api/queues").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()[header::WWW_AUTHENTICATE], "Basic realm=\"pgqueue\"");

    let response = router
        .oneshot(
            Request::builder().uri("/api/queues").header("x-pgqueue-request", "dashboard").body(Body::empty()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(
        response.headers().get(header::WWW_AUTHENTICATE).is_none(),
        "the dashboard page's own request was challenged"
    );
}

/// `/health` fans a probe out over the configured queues, so with none the round
/// finished having issued no query at all and answered `200 OK`: an orchestrator
/// reading readiness off a dashboard that has never reached a database. Building
/// queues from a config list that is empty in one environment reaches it, so it
/// is refused where the empty-credential case is.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_refuses_to_serve_with_no_queues(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let error = dashboard([]).router().unwrap_err();
    assert!(
        matches!(&error, Error::Config(message) if message.contains("at least one queue")),
        "unexpected error: {error}"
    );
    // As with empty credentials, the served path must refuse before it binds.
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let error = dashboard([]).serve_on("127.0.0.1", 0).run_until(shutdown).await.unwrap_err();
    assert!(
        matches!(&error, Error::Config(message) if message.contains("at least one queue")),
        "unexpected error: {error}"
    );

    // One queue is enough, and its `/health` reaches the database.
    let router = dashboard([db.queue.clone()]).router().unwrap();
    let (status, _) = request(&router, "GET", "/health", None).await;
    assert_eq!(status, StatusCode::OK);
}

/// `constant_time_eq(b"", b"")` is true, so an empty username or password
/// matched the credential every client can send: `Authorization: Basic
/// YWRtaW46` (`admin:`) was answered `200 OK` on every protected route. Nothing
/// on the wire distinguished such an instance from a correctly protected one —
/// it still 401s without credentials and still renders the login page — while
/// exposing every job payload plus Retry and Abort.
/// `basic_auth(user, env::var("PGQUEUE_DASHBOARD_PASSWORD").unwrap_or_default())`
/// is one unset environment variable away from it.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_refuses_basic_auth_with_an_empty_username_or_password(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for (user, password) in [("admin", ""), ("", "s3cretpw"), ("", "")] {
        match dashboard([db.queue.clone()]).basic_auth(user, password).router() {
            Err(Error::Config(message)) => assert!(message.contains("basic_auth"), "{user:?}/{password:?}: {message}"),
            Err(error) => panic!("{user:?}/{password:?}: unexpected error: {error}"),
            Ok(_) => panic!("{user:?}/{password:?}: empty credentials built a router"),
        }
        // The served path is the one an operator actually deploys, and it must
        // refuse the same way rather than binding a port first. The token is
        // pre-cancelled so a regression that builds the router cannot park the
        // suite on a running server.
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let error = dashboard([db.queue.clone()])
            .basic_auth(user, password)
            .serve_on("127.0.0.1", 0)
            .run_until(shutdown)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, Error::Config(message) if message.contains("basic_auth")),
            "{user:?}/{password:?}: unexpected error: {error}"
        );
    }

    // Credentials that are actually set still build, and so does an explicit no-auth choice.
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();
    let (status, _) = request(&router, "GET", "/api/queues", Some("Basic YWRtaW46")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "an empty password must never match a configured one");
    let unprotected = dashboard([db.queue.clone()]).router().unwrap();
    let (status, _) = get_json(&unprotected, "/api/queues").await;
    assert_eq!(status, StatusCode::OK);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_refuses_a_short_initial_password(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    match dashboard([db.queue.clone()]).basic_auth("admin", "short").router() {
        Err(Error::Config(message)) => assert!(message.contains("at least 8 characters"), "{message}"),
        Err(error) => panic!("unexpected error: {error}"),
        Ok(_) => panic!("a short dashboard password built a router"),
    }
}

/// HTTP Basic joins the credential with a colon, and `basic_credentials_match`
/// compares the client's base64 against `base64("{user}:{password}")` — which is
/// what keeps a decoder away from hostile input, and what makes a username
/// carrying the separator ambiguous. Configured as `("ops:admin", "s3cretpw")`
/// the expected string is `ops:admin:s3cretpw`, which the username `ops` with
/// the password `admin:s3cretpw` satisfies equally, so the deployment accepts
/// over Basic a credential the login form (which compares the two fields
/// separately) refuses. RFC 7617 forbids a colon in the userid.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_refuses_a_basic_auth_username_containing_a_colon(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    match dashboard([db.queue.clone()]).basic_auth("ops:admin", "s3cretpw").router() {
        Err(Error::Config(message)) => assert!(message.contains("':'"), "{message}"),
        Err(error) => panic!("unexpected error: {error}"),
        Ok(_) => panic!("an ambiguous Basic credential built a router"),
    }
    // A colon in the *password* is unambiguous — everything after the first one
    // is the password — so it must still be accepted.
    let router = dashboard([db.queue.clone()]).basic_auth("ops", "admin:s3cretpw").router().unwrap();
    // echo -n "ops:admin:s3cretpw" | base64 => b3BzOmFkbWluOnMzY3JldHB3
    let (status, _) = request(&router, "GET", "/api/queues", Some("Basic b3BzOmFkbWluOnMzY3JldHB3")).await;
    assert_eq!(status, StatusCode::OK, "a colon in the password is unambiguous and must work");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_health_endpoint_bypasses_dashboard_auth(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    let (status, body) = get_json(&router, "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Value::String("OK".into()));

    let (status, _) = get_json(&router, "/api/queues").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_browser_auth_supports_password_changes_and_logout(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    let response = router
        .clone()
        .oneshot(Request::builder().uri("/").header(header::ACCEPT, "text/html").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(response.headers()[header::LOCATION], "/login");

    let (status, body) = get_json(&router, "/login").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.as_str().unwrap().contains("<span>PGQueue</span>"));
    assert!(!body.as_str().unwrap().contains("value=\"admin\""));

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("username=admin&password=s3cretpw"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(response.headers()[header::LOCATION], "/");
    assert!(response.headers()[header::SET_COOKIE].to_str().unwrap().contains("; Secure;"));
    let cookie = response.headers()[header::SET_COOKIE].to_str().unwrap().split(';').next().unwrap().to_string();
    let other_cookie = login_cookie(&router).await;

    let response = router
        .clone()
        .oneshot(Request::builder().uri("/api/queues").header(header::COOKIE, &cookie).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/password")
                .header(header::COOKIE, &cookie)
                .header("x-pgqueue-request", "dashboard")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"current_password":"s3cretpw","new_password":"newsecret"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    // The caller keeps a session, but not the *same* token: a password change
    // is how an admin evicts a leaked cookie, so the one token that survives is
    // re-minted and re-issued.
    let rotated_cookie =
        response.headers()[header::SET_COOKIE].to_str().unwrap().split(';').next().unwrap().to_string();
    assert_ne!(rotated_cookie, cookie);

    let response = router
        .clone()
        .oneshot(
            Request::builder().uri("/api/queues").header(header::COOKIE, &rotated_cookie).body(Body::empty()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    for stale in [&cookie, &other_cookie] {
        let response = router
            .clone()
            .oneshot(Request::builder().uri("/api/queues").header(header::COOKIE, stale).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{stale}");
    }

    let (status, _) = request(&router, "GET", "/api/queues", Some("Basic YWRtaW46czNjcmV0cHc=")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = request(&router, "GET", "/api/queues", Some("Basic YWRtaW46bmV3c2VjcmV0")).await;
    assert_eq!(status, StatusCode::OK);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/logout")
                .header(header::COOKIE, &rotated_cookie)
                .header("x-pgqueue-request", "dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers()[header::SET_COOKIE].to_str().unwrap().contains("Max-Age=0"));
    assert!(response.headers()[header::SET_COOKIE].to_str().unwrap().contains("; Secure;"));

    let response = router
        .oneshot(
            Request::builder().uri("/api/queues").header(header::COOKIE, rotated_cookie).body(Body::empty()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// `HeaderMap::get` yields the first `Cookie` field line only. RFC 9113 §8.2.3
/// lets an HTTP/2 client split `cookie` across several field lines, and neither
/// `hyper` nor `h2` rejoins them — so a dashboard nested into an application
/// that serves h2 saw a browser's session cookie only when it happened to land
/// in the first line, looping login → home → login forever while
/// `remove_session` and `rotate_password` silently did nothing.
#[sqlx::test(migrations = "./migrations")]
async fn test_browser_auth_reads_a_session_cookie_from_any_cookie_header(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();
    let cookie = login_cookie(&router).await;

    let split_cookies = |request: axum::http::request::Builder| {
        request.header(header::COOKIE, "theme=dark").header(header::COOKIE, &cookie)
    };
    let response = router
        .clone()
        .oneshot(split_cookies(Request::builder().uri("/api/queues")).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // The action endpoints resolve the same token, so they were no-ops too.
    let response = router
        .clone()
        .oneshot(
            split_cookies(
                Request::builder().method("POST").uri("/api/account/logout").header("x-pgqueue-request", "dashboard"),
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = router
        .oneshot(Request::builder().uri("/api/queues").header(header::COOKIE, &cookie).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "the logout found and removed the session");
}

/// A browser sends every cookie for the host on one line, including those of other applications on it, and nothing
/// stops one of them carrying bytes outside visible ASCII. Read through `to_str`, such a line was rejected whole — the
/// session token with it — so every request answered 401 and a correct login looped straight back to the form.
#[sqlx::test(migrations = "./migrations")]
async fn test_browser_auth_reads_a_session_cookie_beside_a_non_ascii_cookie(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();
    let cookie = login_cookie(&router).await;
    let lines = [
        format!("theme=caf\u{e9}; {cookie}"),
        format!("{cookie}; theme=caf\u{e9}"),
    ];

    for line in &lines {
        let line = axum::http::HeaderValue::from_bytes(line.as_bytes()).unwrap();
        let response = router
            .clone()
            .oneshot(Request::builder().uri("/api/queues").header(header::COOKIE, line).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "a neighbouring cookie hid the session");
    }

    // The action endpoints resolve the same token, so logout has to find it too.
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/logout")
                .header(header::COOKIE, axum::http::HeaderValue::from_bytes(lines[0].as_bytes()).unwrap())
                .header("x-pgqueue-request", "dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = router
        .oneshot(Request::builder().uri("/api/queues").header(header::COOKIE, &cookie).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "the logout found and removed the session");
}

/// A password change is how an admin evicts a session token that may have
/// leaked — under `secure_cookies(false)` on plain HTTP it crossed the network
/// in cleartext. Keeping the caller's session *by key* and only re-stamping its
/// credential revision left exactly that token valid for the rest of its 12h
/// TTL, and issued no `Set-Cookie`. A caller authenticated by HTTP Basic has no
/// session to re-issue.
#[sqlx::test(migrations = "./migrations")]
async fn test_password_change_rotates_the_callers_session_token(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();
    let cookie = login_cookie(&router).await;

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/password")
                .header(header::COOKIE, &cookie)
                .header("x-pgqueue-request", "dashboard")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"current_password":"s3cretpw","new_password":"newsecret"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let rotated = response.headers()[header::SET_COOKIE].to_str().unwrap().split(';').next().unwrap().to_string();
    assert_ne!(rotated, cookie, "the surviving token is a fresh one");

    for (stale, expected) in [
        (&cookie, StatusCode::UNAUTHORIZED),
        (&rotated, StatusCode::OK),
    ] {
        let response = router
            .clone()
            .oneshot(Request::builder().uri("/api/queues").header(header::COOKIE, stale).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "{stale}");
    }

    // Basic auth carries no session, so there is nothing to re-issue.
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/password")
                .header(header::AUTHORIZATION, "Basic YWRtaW46bmV3c2VjcmV0")
                .header("x-pgqueue-request", "dashboard")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"current_password":"newsecret","new_password":"thirdsecret"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.headers().contains_key(header::SET_COOKIE));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_browser_auth_can_opt_out_of_secure_cookies_for_direct_http(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").secure_cookies(false).router().unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("username=admin&password=s3cretpw"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let cookie = response.headers()[header::SET_COOKIE].to_str().unwrap();
    assert!(!cookie.contains("; Secure;"));
    assert!(cookie.contains("; HttpOnly; SameSite=Strict;"));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_authentication_failure_waits_before_rejecting_supplied_credentials(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    let started = tokio::time::Instant::now();
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("username=admin&password=wrong"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(started.elapsed() >= Duration::from_millis(90));

    let started = tokio::time::Instant::now();
    let (status, _) = request(&router, "GET", "/api/queues", Some("Basic d3Jvbmc6Y3JlZHM=")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(started.elapsed() >= Duration::from_millis(90));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_valid_basic_credentials_are_accepted_when_failed_comparison_is_in_flight(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    let invalid = router.clone().oneshot(
        Request::builder()
            .uri("/api/queues")
            .header(header::AUTHORIZATION, "Basic d3Jvbmc6Y3JlZHM=")
            .body(Body::empty())
            .unwrap(),
    );
    let valid = router.clone().oneshot(
        Request::builder()
            .uri("/api/queues")
            .header(header::AUTHORIZATION, "Basic YWRtaW46czNjcmV0cHc=")
            .body(Body::empty())
            .unwrap(),
    );
    let (invalid, valid) = tokio::join!(
        biased;
        invalid,
        valid
    );

    assert_eq!(invalid.unwrap().status(), StatusCode::UNAUTHORIZED);
    let valid = valid.unwrap();
    assert_eq!(valid.status(), StatusCode::OK);

    let (status, _) = request(&router, "GET", "/api/queues", Some("Basic YWRtaW46czNjcmV0cHc=")).await;
    assert_eq!(status, StatusCode::OK);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_authentication_attempts_are_refused_when_the_attempt_budget_is_spent(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    let invalid_request = || {
        let router = router.clone();
        async move {
            router
                .oneshot(
                    Request::builder()
                        .uri("/api/queues")
                        .header(header::AUTHORIZATION, "Basic d3Jvbmc6Y3JlZHM=")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
        }
    };

    // Enough guesses at once to outrun the budget however large the burst is:
    // each spends its attempt before it waits, so the ones past the burst find
    // nothing left to spend and are refused without being compared at all.
    let mut guesses = tokio::task::JoinSet::new();
    for _ in 0..64 {
        guesses.spawn(invalid_request());
    }
    let mut compared = 0;
    let mut refused = 0;
    while let Some(response) = guesses.join_next().await {
        let response = response.unwrap();
        match response.status() {
            StatusCode::UNAUTHORIZED => compared += 1,
            StatusCode::TOO_MANY_REQUESTS => {
                assert_eq!(response.headers()[header::RETRY_AFTER], "1");
                refused += 1;
            }
            other => panic!("unexpected status {other}"),
        }
    }
    assert!(compared > 0, "the burst must reach the comparison at all");
    // The bound has to be on the comparisons: every request is answered either
    // way, so asserting that the refusals make up the remainder is arithmetic
    // rather than a claim about the throttle. `MAX_COMPARED` is the burst (16)
    // with room for the handful of refills a burst this short can earn — an
    // unthrottled account would compare all 64.
    const MAX_COMPARED: usize = 32;
    assert!(
        compared <= MAX_COMPARED,
        "guessing must be bounded by the budget, not by how fast the guesses arrive: \
         {compared} compared, {refused} refused"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_password_change_throttles_wrong_current_password_and_accepts_correct(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();
    let cookie = login_cookie(&router).await;

    let started = tokio::time::Instant::now();
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/password")
                .header(header::COOKIE, &cookie)
                .header("x-pgqueue-request", "dashboard")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"current_password":"wrong","new_password":"newsecret"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(started.elapsed() >= Duration::from_millis(90));

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/password")
                .header(header::COOKIE, cookie)
                .header("x-pgqueue-request", "dashboard")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"current_password":"s3cretpw","new_password":"newsecret"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_browser_auth_namespaces_session_cookies_per_dashboard(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let first = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();
    let second = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    let first_cookie = login_cookie(&first).await;
    let second_cookie = login_cookie(&second).await;
    assert_ne!(first_cookie.split_once('=').map(|(name, _)| name), second_cookie.split_once('=').map(|(name, _)| name));

    let browser_cookies = format!("{first_cookie}; {second_cookie}");
    for router in [&first, &second] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/queues")
                    .header(header::COOKIE, &browser_cookies)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_browser_auth_scopes_session_cookie_to_mount_path(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).mount_path("/admin").basic_auth("admin", "s3cretpw").router().unwrap();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("username=admin&password=s3cretpw"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let set_cookie = response.headers()[header::SET_COOKIE].to_str().unwrap();
    assert!(set_cookie.contains("; Path=/admin;"));
    let cookie = set_cookie.split(';').next().unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/logout")
                .header(header::COOKIE, cookie)
                .header("x-pgqueue-request", "dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers()[header::SET_COOKIE].to_str().unwrap().contains("; Path=/admin;"));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_spa_shell_and_static_files_are_served(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).mount_path("/admin").router().unwrap();

    for path in [
        "/",
        "/queues/default",
        &format!("/queues/default/workers/{}", Uuid::now_v7()),
        &format!("/queues/default/jobs/{}", Uuid::now_v7()),
    ] {
        let (status, _) = get_json(&router, path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
    }

    let (_, shell) = get_json(&router, "/").await;
    assert!(
        shell.as_str().is_some_and(|html| html.contains("<script type=\"module\" src=\"/admin/static/app.mjs?v=")),
        "the shell must load the dashboard as an ES module: {shell}"
    );

    for file in [
        "app.mjs",
        "app.css",
        "favicon.svg",
        "login.css",
        "pico.min.css",
    ] {
        let response = router
            .clone()
            .oneshot(Request::builder().uri(format!("/static/{file}")).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{file}");
        assert_eq!(response.headers().get(header::CACHE_CONTROL).unwrap(), "max-age=3600", "{file}");
    }

    let (status, _) = get_json(&router, "/static/nope.css").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let response = router.clone().oneshot(Request::builder().uri("/").body(Body::empty()).unwrap()).await.unwrap();
    let csp = response
        .headers()
        .get(header::CONTENT_SECURITY_POLICY)
        .and_then(|value| value.to_str().ok())
        .expect("a content security policy");
    // The three directives `default-src` does not cover. `form-action` is the
    // one that was missing: the login page is the only form here, and without
    // it an injected `action` posts the credentials typed into it off-origin.
    for directive in [
        "frame-ancestors 'none'",
        "base-uri 'none'",
        "form-action 'self'",
    ] {
        assert!(csp.contains(directive), "{directive} missing from {csp:?}");
    }
    // Every stylesheet is served from `/static/` (the login page's block lives
    // in `login.css`), so nothing here needs — or gets — inline styles: with
    // `'unsafe-inline'`, an HTML-injection bug anywhere would have brought
    // style injection along with it.
    assert!(csp.contains("style-src 'self';"), "style-src must be exactly 'self' in {csp:?}");
    assert!(!csp.contains("unsafe-inline"), "no directive may license inline styles: {csp:?}");
    assert_eq!(response.headers().get(header::CACHE_CONTROL).unwrap(), "no-store");
    // HSTS rides on `secure_cookies`, which is the deployment's statement that
    // it is behind TLS. Without it, the login form — the one request carrying
    // credentials — is downgradeable on first contact.
    assert_eq!(response.headers().get(header::STRICT_TRANSPORT_SECURITY).unwrap(), "max-age=31536000");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_embedded_router_rejects_ambiguous_request_framing(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).router().unwrap();
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/queues")
                .header(header::CONTENT_LENGTH, "4")
                .header(header::TRANSFER_ENCODING, "chunked")
                .body(Body::from("0\r\n\r\n"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.headers().get(header::CONNECTION).unwrap(), "close");
    assert!(response.headers().contains_key(header::CONTENT_SECURITY_POLICY));
    assert_eq!(response.headers().get(header::X_CONTENT_TYPE_OPTIONS).unwrap(), "nosniff");
    assert_eq!(response.headers().get(header::X_FRAME_OPTIONS).unwrap(), "DENY");
}

/// The counterpart: HSTS must not be sent by a mount that serves plain HTTP,
/// where it would pin a scheme the deployment does not answer on.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_omits_hsts_when_cookies_are_not_secure(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).secure_cookies(false).router().unwrap();

    let response = router.oneshot(Request::builder().uri("/").body(Body::empty()).unwrap()).await.unwrap();

    assert!(response.headers().get(header::STRICT_TRANSPORT_SECURITY).is_none());
}

/// `/static/` is merged outside the `require_auth` layer so the stylesheet and
/// script the login page needs stay reachable, which makes everything it serves
/// public on an otherwise authenticated dashboard. It serves an allowlist, not
/// the embedded directory: the HTML templates belong to the shell and login
/// routes, and a file added to `pgqueue/dashboard/` must not become an endpoint
/// on its own.
#[sqlx::test(migrations = "./migrations")]
async fn test_static_route_serves_only_the_public_file_allowlist(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    // Authentication really is on, so these are anonymous requests.
    for guarded in ["/api/queues", "/api/account/password", "/"] {
        let (status, _) = get_json(&router, guarded).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{guarded}");
    }

    for public in [
        "/static/app.mjs",
        "/static/app.css",
        "/static/favicon.svg",
        "/static/login.css",
        "/static/pico.min.css",
    ] {
        let (status, _) = get_json(&router, public).await;
        assert_eq!(status, StatusCode::OK, "{public}");
    }

    for private in ["/static/index.html", "/static/login.html"] {
        let (status, _) = get_json(&router, private).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{private}");
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_nested_static_files_keep_their_cache_policy(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Router::new().nest("/admin", dashboard([db.queue.clone()]).mount_path("/admin").router().unwrap());
    let response =
        router.oneshot(Request::builder().uri("/admin/static/app.mjs").body(Body::empty()).unwrap()).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get(header::CACHE_CONTROL).unwrap(), "max-age=3600");
}

/// The href the shipped script renders for `breadcrumb("/", ...)`.
///
/// The home view is served at the mount path itself, never at
/// `mount_path + "/"`: axum collapses the nested `"/"` route to exactly the
/// mount path, so `/admin/` is a URL the router does not answer. The Node tests pin the frontend's matching URL
/// helper; these assertions pin the server half of the same contract.
fn home_breadcrumb_href(root: &str) -> String {
    if root.is_empty() { "/".to_string() } else { root.to_string() }
}

/// Under a non-root `mount_path` the home breadcrumb — rendered on every queue,
/// worker, job and error page — was built as `ROOT + "/"`, i.e. `/admin/`. Axum
/// collapses the nested `"/"` route to exactly `/admin` and matchit has no
/// trailing-slash tolerance, so that href names a URL the router does not
/// serve: a refresh, a bookmark, a share, or the Cmd/Ctrl-click deliberately
/// handed to the browser all 404. `DashboardAuthState::home_path` already
/// answers `/admin`, which is why the post-login redirect worked; the script
/// now agrees with it.
#[sqlx::test(migrations = "./migrations")]
async fn test_nested_home_breadcrumb_points_at_a_url_the_router_serves(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Router::new().nest("/admin", dashboard([db.queue.clone()]).mount_path("/admin").router().unwrap());

    // The root the shell hands the script is the input to that rule.
    let (status, body) = get_json(&router, "/admin/queues/default").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.as_str().unwrap_or_default().contains("name=\"pgqueue-root\" content=\"/admin\""),
        "the shell must publish the mount path the script builds hrefs from"
    );

    let home = home_breadcrumb_href("/admin");
    assert_eq!(home, "/admin", "the breadcrumb must not append a slash");
    let (status, _) = get_json(&router, &home).await;
    assert_eq!(status, StatusCode::OK, "the home breadcrumb must name a URL the nested router answers: {home}");

    // And the URL the old rule produced really is unroutable, which is why the
    // script cannot simply concatenate.
    let (status, _) = get_json(&router, "/admin/").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_mount_path_rejects_protocol_relative_and_cookie_unsafe_values(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for path in [
        "//attacker.example",
        "/admin; SameSite=None",
        "/admin\"><script>",
        "/admin?redirect=elsewhere",
        "/admin\\login",
        "/admin/../login",
    ] {
        match dashboard([db.queue.clone()]).mount_path(path).router() {
            Err(Error::Config(message)) => {
                assert!(message.contains("mount_path"), "{path}: {message}");
            }
            Err(error) => panic!("{path}: unexpected error: {error}"),
            Ok(_) => panic!("{path}: unsafe mount path was accepted"),
        }
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_surfaces_worker_and_job_data_for_multiple_queues(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|b| b.name("emails")).await;
    let other_id = other.enqueue_raw(JobRequest::new("send", json!({"to": "x"}))).await.unwrap().unwrap();

    let router = dashboard([db.queue.clone(), other.clone()]).router().unwrap();
    let (_, body) = get_json(&router, "/api/queues").await;
    let queues = body["queues"].as_array().unwrap();
    assert_eq!(queues.len(), 2);
    assert_eq!(queues[0]["name"], "default");
    assert_eq!(queues[1]["name"], "emails");
    assert!(queues[1]["oldest_ready_at"].is_string());

    let (status, _) = get_json(&router, &format!("/api/queues/default/jobs/{other_id}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "queue path cannot cross-read ids");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_rejects_repeated_queue_names(pool: PgPool) {
    let first = TestDb::new(pool.clone()).await;
    let second = TestDb::new(pool.clone()).await;
    match dashboard([first.queue.clone(), second.queue.clone()]).router() {
        Err(Error::Config(message)) => assert!(message.contains("configured more than once"), "{message}"),
        Err(error) => panic!("unexpected error: {error}"),
        Ok(_) => panic!("duplicate queue names must not leave one database outside /health"),
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_broken_database_yields_500s(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).router().unwrap();
    // Nuke the schema out from under the dashboard.
    sqlx::query("DROP SCHEMA pgqueue CASCADE").execute(db.queue.pool()).await.unwrap();

    let (status, body) = get_json(&router, "/api/queues").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["error"], "internal server error");

    let (status, _) = get_json(&router, "/health").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_standalone_dashboard_runs_until_cancelled(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let dashboard = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").serve_on("localhost", 0);
    let mut dashboard_handle = dashboard.server_handle();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(dashboard.run_until(shutdown.clone()));
    let address =
        tokio::time::timeout(Duration::from_secs(5), dashboard_handle.wait_until_ready()).await.unwrap().unwrap();
    assert_eq!(dashboard_handle.local_addr(), Some(address));

    let response = http_get(address, "/api/queues", None).await;
    assert!(response.starts_with("HTTP/1.1 401"), "{response}");
    let response = http_get(address, "/api/queues", Some("Basic YWRtaW46czNjcmV0cHc=")).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("\"name\":\"default\""), "{response}");

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run).await.unwrap().unwrap().unwrap();
    assert!(dashboard_stopped_serving(address).await, "the dashboard kept serving after its run returned");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_standalone_dashboard_rejects_invalid_server_limits(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let servers = [
        dashboard([db.queue.clone()]).serve_on("127.0.0.1", 0).header_read_timeout(Duration::ZERO),
        dashboard([db.queue.clone()]).serve_on("127.0.0.1", 0).request_timeout(Duration::ZERO),
        dashboard([db.queue.clone()]).serve_on("127.0.0.1", 0).max_connections(0),
        dashboard([db.queue.clone()]).serve_on("127.0.0.1", 0).max_concurrent_requests(0),
    ];

    for server in servers {
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let error = server.run_until(shutdown).await.unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
    }
}

/// A connection cap under the default request cap is a cap, not a
/// misconfiguration: refused, one `max_connections` call turned into a startup
/// failure for a dashboard — and for the worker hosting it.
#[sqlx::test(migrations = "./migrations")]
async fn test_standalone_dashboard_accepts_a_connection_cap_below_the_request_cap(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    dashboard([db.queue.clone()]).serve_on("127.0.0.1", 0).max_connections(64).run_until(shutdown).await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_standalone_dashboard_times_out_partial_headers(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let dashboard =
        dashboard([db.queue.clone()]).serve_on("127.0.0.1", 0).header_read_timeout(Duration::from_millis(50));
    let mut handle = dashboard.server_handle();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(dashboard.run_until(shutdown.clone()));
    let address = handle.wait_until_ready().await.unwrap();

    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(b"GET /health HTTP/1.1\r\nHost:").await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
        .await
        .expect("partial header connection outlived its configured timeout")
        .unwrap();

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_standalone_dashboard_caps_accepted_connections(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let dashboard = dashboard([db.queue.clone()])
        .serve_on("127.0.0.1", 0)
        .header_read_timeout(Duration::from_secs(5))
        .max_connections(1)
        .max_concurrent_requests(1);
    let mut handle = dashboard.server_handle();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(dashboard.run_until(shutdown.clone()));
    let address = handle.wait_until_ready().await.unwrap();

    let mut first = tokio::net::TcpStream::connect(address).await.unwrap();
    first.write_all(b"GET /health HTTP/1.1\r\nHost:").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut second = tokio::net::TcpStream::connect(address).await.unwrap();
    second
        .write_all(format!("GET /health HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut byte = [0_u8; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(100), second.read(&mut byte)).await.is_err(),
        "a second connection was served while the only connection slot was occupied"
    );

    drop(first);
    let mut response = String::new();
    tokio::time::timeout(Duration::from_secs(2), second.read_to_string(&mut response))
        .await
        .expect("second connection was not accepted after the slot opened")
        .unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

/// A client that slowly reads its response must lose the connection by a fixed deadline. A permit is acquired before
/// accepting each connection, so clients that pipeline a large public file and occasionally make write progress would
/// otherwise hold every slot forever. `/health` would then never be accepted, which is a restart for a worker that is
/// fine.
#[sqlx::test(migrations = "./migrations")]
async fn test_served_dashboard_drops_a_slow_reader_that_keeps_making_progress(pool: PgPool) {
    const CONNECTIONS: usize = 2;
    const CONNECTION_DEADLINE: Duration = Duration::from_secs(1);
    // The 81 KB static file, asked for enough times that socket buffers and the deliberately slow reads below cannot
    // finish all the responses before the assertion.
    const REQUESTS: usize = 64;
    let db = TestDb::new(pool.clone()).await;
    let dashboard = dashboard([db.queue.clone()])
        .serve_on("127.0.0.1", 0)
        .max_connections(CONNECTIONS)
        // Longer than the connection deadline, so Hyper's keep-alive header timer cannot free these slots first.
        .header_read_timeout(Duration::from_secs(30))
        .request_timeout(CONNECTION_DEADLINE);
    let mut handle = dashboard.server_handle();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(dashboard.run_until(shutdown.clone()));
    let address = handle.wait_until_ready().await.unwrap();

    let stop_reading = CancellationToken::new();
    let readers_ready = Arc::new(tokio::sync::Barrier::new(CONNECTIONS + 1));
    let slow_read_started = tokio::time::Instant::now();
    let mut slow_readers = Vec::new();
    for _ in 0..CONNECTIONS {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(1024).unwrap();
        let mut stream = socket.connect(address).await.unwrap();
        let requests = format!("GET /static/pico.min.css HTTP/1.1\r\nHost: {address}\r\n\r\n").repeat(REQUESTS);
        stream.write_all(requests.as_bytes()).await.unwrap();
        let stop_reading = stop_reading.clone();
        let readers_ready = Arc::clone(&readers_ready);
        slow_readers.push(tokio::spawn(async move {
            let mut buffer = [0_u8; 1024];
            for _ in 0..3 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let read = tokio::time::timeout(Duration::from_millis(100), stream.read(&mut buffer))
                    .await
                    .expect("a slow reader received no response bytes before the connection deadline")
                    .expect("a slow reader was disconnected before the connection deadline");
                assert!(read > 0, "a slow reader reached EOF before the connection deadline");
            }

            readers_ready.wait().await;
            loop {
                tokio::select! {
                    _ = stop_reading.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        match stream.read(&mut buffer).await {
                            Ok(0) | Err(_) => break,
                            Ok(_) => {}
                        }
                    }
                }
            }
        }));
    }

    readers_ready.wait().await;
    let readers_confirmed = tokio::time::Instant::now();
    assert!(
        slow_read_started.elapsed() + Duration::from_millis(100) < CONNECTION_DEADLINE,
        "the slow readers did not make progress early enough to check the pre-deadline connection state"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), http_get(address, "/health", None)).await.is_err(),
        "the server released a slow reader before its connection deadline"
    );

    tokio::time::sleep_until(readers_confirmed + CONNECTION_DEADLINE + Duration::from_millis(200)).await;
    let response = tokio::time::timeout(Duration::from_secs(2), http_get(address, "/health", None))
        .await
        .expect("slow readers making progress kept every connection slot past the absolute deadline");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");

    stop_reading.cancel();
    for slow_reader in slow_readers {
        slow_reader.await.unwrap();
    }
    shutdown.cancel();
    run.await.unwrap().unwrap();
}

/// `max_concurrent_requests` and `request_timeout` only reach a router that a
/// `DashboardServer` builds for itself, so both are taken against a served
/// dashboard rather than a hand-built limiter: one permit lets one of four
/// concurrent requests reach the database at a time, and every one of them
/// answers instead of hanging on a database that never replies.
#[sqlx::test(migrations = "./migrations")]
async fn test_served_dashboard_bounds_concurrent_requests_and_times_them_out(pool: PgPool) {
    const REQUESTS: usize = 4;
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
    // Room for every request to reach the database at once, which is exactly what the permit prevents.
    let db = TestDb::new(crate::pool_with_max(&pool, REQUESTS as u32).await).await;
    let dashboard = dashboard([db.queue.clone()])
        .serve_on("127.0.0.1", 0)
        .max_concurrent_requests(1)
        .request_timeout(REQUEST_TIMEOUT);
    let mut handle = dashboard.server_handle();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(dashboard.run_until(shutdown.clone()));
    let address = handle.wait_until_ready().await.unwrap();

    // `ACCESS EXCLUSIVE` blocks even a plain SELECT until this transaction ends,
    // standing in for any stalled database.
    let mut lock = pool.begin().await.unwrap();
    sqlx::raw_sql("LOCK TABLE pgqueue.jobs IN ACCESS EXCLUSIVE MODE").execute(&mut *lock).await.unwrap();
    let spawned = tokio::time::Instant::now();
    let mut requests = tokio::task::JoinSet::new();
    for _ in 0..REQUESTS {
        requests.spawn(http_get(address, "/api/queues/default/jobs", None));
    }
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "no dashboard request ever reached the database",
        || async { blocked_backends(&pool, None).await >= 1 },
    )
    .await;
    // Sampled across a window rather than read once at a fixed offset: the invariant is that only one request is ever
    // in flight, and the window sits inside the request timeout the first one spends. Measured from `spawned`, where
    // that timeout started: past it, the first request answers 504 while its statement stays parked on the lock, and
    // the next request's parks beside it. Sampled at least once, so a detection above that took longer than the window
    // still checks the invariant rather than passing without looking.
    let window = spawned + REQUEST_TIMEOUT / 2;
    loop {
        assert!(
            blocked_backends(&pool, None).await <= 1,
            "a single request permit must let only one request at a time reach the database"
        );
        if tokio::time::Instant::now() >= window {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let responses = tokio::time::timeout(Duration::from_secs(6), requests.join_all())
        .await
        .expect("dashboard requests outlived the configured request timeout");
    for response in responses {
        assert!(response.starts_with("HTTP/1.1 504"), "{response}");
        assert!(response.contains("dashboard request timed out"), "{response}");
    }

    lock.rollback().await.unwrap();
    shutdown.cancel();
    run.await.unwrap().unwrap();
}

/// `/health` is what an orchestrator restarts a worker on, so it is merged
/// outside the request limiter: inside it, a stalled database that pinned every
/// request permit answered 504 there — a restart for a worker that was fine —
/// instead of the probe's own bounded answer.
#[sqlx::test(migrations = "./migrations")]
async fn test_health_answers_while_every_request_permit_is_held(pool: PgPool) {
    // One connection for the request holding the permit and one for the probe.
    let db = TestDb::new(crate::pool_with_max(&pool, 2).await).await;
    let dashboard = dashboard([db.queue.clone()])
        .serve_on("127.0.0.1", 0)
        .max_concurrent_requests(1)
        // Past the probe's own bounded wait, so the permit is still held when the assertion below
        // gives up and a 504 there could only have come from the limiter.
        .request_timeout(round_wait_timeout() * 3);
    let mut handle = dashboard.server_handle();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(dashboard.run_until(shutdown.clone()));
    let address = handle.wait_until_ready().await.unwrap();

    let mut lock = pool.begin().await.unwrap();
    sqlx::raw_sql("LOCK TABLE pgqueue.jobs IN ACCESS EXCLUSIVE MODE").execute(&mut *lock).await.unwrap();
    let parked = tokio::spawn(http_get(address, "/api/queues/default/jobs", None));
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "no dashboard request ever took the only request permit",
        || async { blocked_backends(&pool, None).await >= 1 },
    )
    .await;

    let response = tokio::time::timeout(round_wait_timeout() * 2, http_get(address, "/health", None))
        .await
        .expect("/health queued behind the request limiter");
    assert!(response.starts_with("HTTP/1.1 503"), "{response}");
    assert!(response.contains("unavailable"), "{response}");

    lock.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), parked).await.expect("the parked request never answered").unwrap();
    shutdown.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_standalone_dashboard_reports_bind_failure(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = occupied.local_addr().unwrap();
    let dashboard = dashboard([db.queue.clone()]).serve_on(address.ip().to_string(), address.port());
    let mut dashboard_handle = dashboard.server_handle();

    let error = dashboard.run_until(CancellationToken::new()).await.unwrap_err();
    match error {
        Error::Dashboard(error) => assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse),
        other => panic!("expected dashboard bind error, got {other}"),
    }
    assert_eq!(dashboard_handle.wait_until_ready().await, None);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_hosts_authenticated_dashboard_and_stops_it_on_shutdown(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let dashboard = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").serve_on("127.0.0.1", 0);
    let mut dashboard_handle = dashboard.server_handle();
    let worker = Worker::builder(db.queue.clone()).register_job(dashboard_probe).dashboard(dashboard).build().unwrap();
    let worker_id = worker.id();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    let address =
        tokio::time::timeout(Duration::from_secs(5), dashboard_handle.wait_until_ready()).await.unwrap().unwrap();
    assert_eq!(dashboard_handle.local_addr(), Some(address));

    let response = http_get(address, "/api/queues", None).await;
    assert!(response.starts_with("HTTP/1.1 401"), "{response}");

    let response = http_get(address, "/api/queues", Some("Basic YWRtaW46czNjcmV0cHc=")).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("\"name\":\"default\""), "{response}");
    let response = http_get(address, "/api/queues/default/workers", Some("Basic YWRtaW46czNjcmV0cHc=")).await;
    assert!(response.contains(&worker_id.to_string()), "{response}");

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run).await.unwrap().unwrap().unwrap();
    assert!(dashboard_stopped_serving(address).await, "the dashboard kept serving after its run returned");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_remains_available_while_worker_drains(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let drain = DashboardDrain { started: Arc::new(Notify::new()), release: Arc::new(Notify::new()) };
    let job = db.queue.enqueue(dashboard_slow::job(())).await.unwrap().unwrap();
    let dashboard = dashboard([db.queue.clone()]).serve_on("127.0.0.1", 0);
    let mut dashboard_handle = dashboard.server_handle();
    let worker = Worker::builder(db.queue.clone())
        .register_job(dashboard_slow)
        .state(drain.clone())
        .dashboard(dashboard)
        .shutdown_grace(Duration::from_secs(2))
        .build()
        .unwrap();
    let worker_id = worker.id();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    let address =
        tokio::time::timeout(Duration::from_secs(5), dashboard_handle.wait_until_ready()).await.unwrap().unwrap();

    tokio::time::timeout(Duration::from_secs(5), drain.started.notified()).await.expect("job did not start");
    assert_eq!(job.fetch_job().await.unwrap().status, JobStatus::Running);

    shutdown.cancel();
    wait_for_worker_intake_closed(&db, worker_id).await;
    let response = http_get(address, "/health", None).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");

    drain.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), run).await.unwrap().unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_bind_failure_prevents_worker_startup(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = occupied.local_addr().unwrap();
    let dashboard = dashboard([db.queue.clone()]).serve_on(address.ip().to_string(), address.port());
    let mut dashboard_handle = dashboard.server_handle();
    let worker = Worker::builder(db.queue.clone()).register_job(dashboard_probe).dashboard(dashboard).build().unwrap();

    let error = worker.run_until(CancellationToken::new()).await.unwrap_err();
    match error {
        Error::Dashboard(error) => assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse),
        other => panic!("expected dashboard bind error, got {other}"),
    }
    assert_eq!(dashboard_handle.wait_until_ready().await, None);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_bind_is_skipped_when_shutdown_is_pre_cancelled(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = occupied.local_addr().unwrap();
    let dashboard = dashboard([db.queue.clone()]).serve_on(address.ip().to_string(), address.port());
    let mut dashboard_handle = dashboard.server_handle();
    let worker = Worker::builder(db.queue.clone()).register_job(dashboard_probe).dashboard(dashboard).build().unwrap();
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    tokio::time::timeout(Duration::from_secs(1), worker.run_until(shutdown))
        .await
        .expect("pre-cancelled dashboard worker should stop promptly")
        .expect("pre-cancelled dashboard worker should stop cleanly");

    assert_eq!(dashboard_handle.local_addr(), None);
    assert_eq!(dashboard_handle.wait_until_ready().await, None);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_hosted_dashboard_rejects_custom_mount_path(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let dashboard = dashboard([db.queue.clone()]).mount_path("/admin").serve_on("127.0.0.1", 0);
    let result = Worker::builder(db.queue.clone()).register_job(dashboard_probe).dashboard(dashboard).build();

    match result {
        Err(Error::Config(message)) => assert!(message.contains("requires mount_path `/`")),
        Err(other) => panic!("expected configuration error, got {other}"),
        Ok(_) => panic!("custom mount path should be rejected"),
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_burst_completion_stops_worker_hosted_dashboard(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let dashboard = dashboard([db.queue.clone()]).serve_on("127.0.0.1", 0);
    let mut dashboard_handle = dashboard.server_handle();
    let worker = Worker::builder(db.queue.clone())
        .register_job(dashboard_probe)
        .dashboard(dashboard)
        .burst(true)
        .dequeue_timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(CancellationToken::new()));
    let address =
        tokio::time::timeout(Duration::from_secs(5), dashboard_handle.wait_until_ready()).await.unwrap().unwrap();

    let response = http_get(address, "/health", None).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    tokio::time::timeout(Duration::from_secs(10), run).await.unwrap().unwrap().unwrap();
    assert!(dashboard_stopped_serving(address).await, "the dashboard kept serving after its run returned");
}

/// A rare name must stay suggestible however much newer traffic sits on top of
/// it: the suggestions once sampled the newest rows and filtered afterwards, so
/// anything older than that sample was invisible.
#[sqlx::test(migrations = "./migrations")]
async fn test_job_name_suggestions_find_names_buried_under_newer_ones(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // One older job with a distinctive name...
    sqlx::query(
        "INSERT INTO pgqueue.jobs (queue, name, status, kind, enqueued_at)
         VALUES ('default', 'nightly_report', 'complete', 'job', now() - interval '1 day')",
    )
    .execute(&pool)
    .await
    .unwrap();
    // ...buried under a busier one.
    sqlx::query(
        "INSERT INTO pgqueue.jobs (queue, name, status, kind, enqueued_at)
         SELECT 'default', 'send_email', 'complete', 'job', now() - (g * interval '1 second')
         FROM generate_series(1, 1200) AS g",
    )
    .execute(&pool)
    .await
    .unwrap();

    let router = dashboard([db.queue.clone()]).router().unwrap();
    let (status, body) = get_json(&router, "/api/queues/default/job-names?kind=job&prefix=night").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["names"],
        json!(["nightly_report"]),
        "a name is suggested on the prefix matching it, not on being recent"
    );

    let (_, body) = get_json(&router, "/api/queues/default/job-names?kind=job&prefix=send").await;
    assert_eq!(body["names"], json!(["send_email"]));
}

/// A suggestion is a distinct *name*, so the work per keystroke has to be bounded
/// in names, not in rows. Grouping every matching row read the queue's whole
/// matching history (300,000 rows and 68ms per keystroke on the measured
/// dataset, unbounded under `JobRetention::Forever`) against the pool the worker
/// dequeues, heartbeats and finalizes with; capping the *rows* instead would be
/// bounded but wrong, because the index hands rows over clustered by name — the
/// cap would be spent inside the first name or two and the rest would vanish
/// from the suggestions.
///
/// Here the alphabetically-first name carries far more rows than any row cap
/// would allow, and its siblings must still be offered.
#[sqlx::test(migrations = "./migrations")]
async fn test_job_name_suggestions_survive_one_name_dominating_the_rows(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    sqlx::query(
        "INSERT INTO pgqueue.jobs (queue, name, status, kind)
         SELECT 'default', 'report_00_busy', 'complete', 'job'
         FROM generate_series(1, 5000)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO pgqueue.jobs (queue, name, status, kind)
         SELECT 'default', 'report_' || lpad(g::text, 2, '0') || '_rare', 'complete', 'job'
         FROM generate_series(1, 5) AS g",
    )
    .execute(&pool)
    .await
    .unwrap();

    let router = dashboard([db.queue.clone()]).router().unwrap();
    let (status, body) = get_json(&router, "/api/queues/default/job-names?kind=job&prefix=report").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["names"],
        json!([
            "report_00_busy",
            "report_01_rare",
            "report_02_rare",
            "report_03_rare",
            "report_04_rare",
            "report_05_rare",
        ]),
        "every name under the prefix is suggested, whatever the row counts \
         behind them"
    );

    // The walk steps case-insensitively, and the prefix is matched that way too.
    let (_, body) = get_json(&router, "/api/queues/default/job-names?kind=job&prefix=REPORT_01").await;
    assert_eq!(body["names"], json!(["report_01_rare"]));

    // The step past the prefix ends the walk rather than joining the results.
    let (_, body) = get_json(&router, "/api/queues/default/job-names?kind=job&prefix=report_99").await;
    assert_eq!(body["names"], json!([]));
}

/// The typeahead answers the question the listing beside it asks. Ignoring the
/// status filter offered names that exist only under some other status, and
/// choosing one rendered "No jobs found" — the one outcome a typeahead exists
/// to make impossible.
#[sqlx::test(migrations = "./migrations")]
async fn test_job_name_suggestions_are_filtered_by_the_selected_statuses(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    sqlx::query(
        "INSERT INTO pgqueue.jobs (queue, name, status, kind) VALUES
             ('default', 'report_done', 'complete', 'job'),
             ('default', 'report_failed', 'failed', 'job')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let router = dashboard([db.queue.clone()]).router().unwrap();

    let (status, body) = get_json(&router, "/api/queues/default/job-names?kind=job&prefix=report&status=failed").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["names"], json!(["report_failed"]), "a suggestion must name a job the filtered listing can show");

    let (_, body) =
        get_json(&router, "/api/queues/default/job-names?kind=job&prefix=report&status=complete,failed").await;
    assert_eq!(body["names"], json!(["report_done", "report_failed"]));

    // No status filter still means every status, as the listing does.
    let (_, body) = get_json(&router, "/api/queues/default/job-names?kind=job&prefix=report").await;
    assert_eq!(body["names"], json!(["report_done", "report_failed"]));

    let (status, _) = get_json(&router, "/api/queues/default/job-names?kind=job&prefix=report&status=bogus").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// `/health` is deliberately unauthenticated, so its request rate must not turn
/// into query rate on the pool the worker dequeues and finalizes with.
#[sqlx::test(migrations = "./migrations")]
async fn test_health_probes_are_cached_so_request_rate_does_not_become_database_load(pool: PgPool) {
    let single = crate::pool_with_max(&pool, 1).await;
    let db = TestDb::new(single.clone()).await;
    // Every request below has to be a cache hit — the pool's one connection is
    // held, so a miss opens a round that parks on `acquire`, gives up after
    // `ROUND_WAIT_TIMEOUT` and answers 503. Under the shipped 500ms TTL that
    // made the priming request plus the whole loop a 500ms budget, unbounded
    // while 300-odd tests run beside it, and blew it by hanging for up to 25 ×
    // 2s before failing. Raised, the loop asserts *that* the cache answered.
    let router =
        pgqueue::__test_support::dashboard_health_probe_ttl(dashboard([db.queue.clone()]), Duration::from_secs(300))
            .router()
            .unwrap();

    let (status, _) = get_json(&router, "/health").await;
    assert_eq!(status, StatusCode::OK);

    // Hold the only connection, exactly as a worker dequeue would. Further
    // requests must be served from the cached probe rather than queueing for it.
    let held = single.acquire().await.unwrap();
    for _ in 0..25 {
        let (status, _) = get_json(&router, "/health").await;
        assert_eq!(status, StatusCode::OK, "a flood of /health requests must not each need a pooled connection");
    }
    drop(held);
}

/// How long a request waits for the round it is riding on before it gives up
/// and answers 503, which is the budget every test that parks a round and then
/// observes it has to finish inside.
fn round_wait_timeout() -> Duration {
    pgqueue::__test_support::dashboard_round_wait_timeout()
}

/// How many backends are parked on a lock in this test's own database, counting
/// only those running `query` when one is given.
async fn blocked_backends(pool: &PgPool, query: Option<&str>) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT count(*)::bigint FROM pg_stat_activity
         WHERE datname = current_database()
           AND wait_event_type = 'Lock'
           AND ($1::text IS NULL OR query = $1)",
    )
    .bind(query)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// How many backends are parked on a lock while running the `/health` probe.
///
/// Matched against the shipped statement itself rather than a copy of its text,
/// so tuning the probe's plan cannot silently turn this into a matcher that
/// counts nothing.
async fn blocked_health_probes(pool: &PgPool) -> i64 {
    blocked_backends(pool, Some(pgqueue::__test_support::health_probe_sql())).await
}

/// `/health` is merged outside `require_auth`, so an anonymous client sets its
/// rate. The result cache was *read* before the probes and *written* only after
/// they returned, and nothing marked a round in flight: while a probe was slow
/// — lock contention, `max_connections` pressure, exactly when it matters —
/// every concurrent request raced past the not-yet-written cache and took a
/// pooled connection of its own, draining the pool the worker dequeues and
/// finalizes with. The TTL bounds the steady-state rate only while probes are
/// fast, which is precisely when the bound is not needed.
#[sqlx::test(migrations = "./migrations")]
async fn test_concurrent_health_requests_run_one_probe_when_the_probe_is_slow(pool: PgPool) {
    const REQUESTS: usize = 8;
    let db = TestDb::new(crate::pool_with_max(&pool, REQUESTS as u32).await).await;
    let router = dashboard([db.queue.clone()]).router().unwrap();

    // Park every probe: `ACCESS EXCLUSIVE` blocks even a plain SELECT until
    // this transaction ends, standing in for any slow probe.
    let mut lock = pool.begin().await.unwrap();
    sqlx::raw_sql("LOCK TABLE pgqueue.jobs IN ACCESS EXCLUSIVE MODE").execute(&mut *lock).await.unwrap();

    // A cold cache, so nothing can be served from a previous round.
    let spawned = tokio::time::Instant::now();
    let mut requests = tokio::task::JoinSet::new();
    for _ in 0..REQUESTS {
        let router = router.clone();
        requests.spawn(async move {
            router.oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap()).await.unwrap().status()
        });
    }

    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "no /health probe ever reached the database",
        || async { blocked_health_probes(&pool).await >= 1 },
    )
    .await;
    // Sampled across the window rather than read once at a fixed offset from
    // `spawned`. The invariant is "at most one probe is ever in flight", and it
    // is checked continuously here — which is both stronger than one reading and
    // immune to *when* that reading lands. The single reading was not: the wait
    // above is allowed five seconds and this offset is one, so a loaded runner
    // reached the assertion at an arbitrary point, and one that arrived after the
    // round it was watching had already gone read zero and failed a test whose
    // subject had behaved perfectly.
    //
    // The window is measured from `spawned`, not from the detection above: each request's own `ROUND_WAIT_TIMEOUT`
    // started when it was spawned, so a window that began only once a slow runner had detected the probe could outlast
    // it, and a waiter answering 503 then failed the final `OK` below. Sampled at least once, so a detection that took
    // longer than the window still checks the invariant rather than passing without looking.
    let window = spawned + round_wait_timeout() / 2;
    loop {
        assert!(
            blocked_health_probes(&pool).await <= 1,
            "{REQUESTS} concurrent /health requests must share one probe rather than \
             taking a pooled connection each"
        );
        if tokio::time::Instant::now() >= window {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    lock.rollback().await.unwrap();
    while let Some(status) = requests.join_next().await {
        assert_eq!(status.unwrap(), StatusCode::OK);
    }
}

// ---------------------------------------------------------------------------
// The unauthenticated probe and the 5s signal poll must stay O(1) under the
// generic plan sqlx's prepared statements settle into
// ---------------------------------------------------------------------------

/// Retained history for a queue *other* than the one these plans are taken for,
/// and all of it `running`. One queue holding every row is what makes the
/// generic plan's average-rows-per-queue estimate for `queue = $1` wrong for a
/// queue that has none, and 500 rows is enough for an early-exit sequential scan
/// to out-cost an index on that estimate.
async fn seed_history_for_another_queue(pool: &PgPool) {
    sqlx::query(
        // `started_at`, because an active row has to carry the clock recovery
        // reads (`jobs_active_started_at_check`). Without it these decoys are
        // exactly the unrecoverable shape that check exists to refuse.
        "INSERT INTO pgqueue.jobs (queue, name, status, started_at)
         SELECT 'plan-decoy', 'seed', 'running', now() FROM generate_series(1, 500)",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::raw_sql("ANALYZE pgqueue.jobs").execute(pool).await.unwrap();
}

/// The plan PostgreSQL runs `sql` under once it is prepared, which sqlx always
/// does. `force_generic_plan` is the sixth-and-later execution of any prepared
/// statement, reached deterministically: the generic plan is the one that costs
/// `queue = $1` against the table-wide average instead of this queue's real
/// count, and it is where an early-exit sequential scan looks cheap — and the
/// one that cannot fold a parameter into a constant, which is what a prefix
/// range needs. `types` declares the placeholders and `args` supplies them as
/// literals, because a generic plan is by definition independent of their
/// values.
async fn generic_plan_for(pool: &PgPool, sql: &str, types: &str, args: &str) -> String {
    let mut connection = pool.acquire().await.unwrap();
    sqlx::raw_sql("SET plan_cache_mode = force_generic_plan").execute(&mut *connection).await.unwrap();
    // The only interpolation is this crate's own statement text and this
    // function's own callers' literals.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("PREPARE plan_under_test({types}) AS {sql}")))
        .execute(&mut *connection)
        .await
        .unwrap();
    let plan = sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(format!(
        "EXPLAIN (COSTS OFF) EXECUTE plan_under_test({args})"
    )))
    .fetch_all(&mut *connection)
    .await
    .unwrap()
    .join("\n");
    // Leave the session as it was found. Both statements are session-scoped and
    // sqlx runs no reset when a connection goes back to the pool, so a second
    // plan taken on the same connection would fail on `42P05 prepared statement
    // "plan_under_test" already exists` and every later user of it would inherit
    // `force_generic_plan`. No test here asks for two plans today, which is the
    // only reason it has not bitten; the sibling helper in `queue_test` does, and
    // it did.
    sqlx::raw_sql("DEALLOCATE plan_under_test").execute(&mut *connection).await.unwrap();
    sqlx::raw_sql("RESET plan_cache_mode").execute(&mut *connection).await.unwrap();
    plan
}

/// `/health` is deliberately unauthenticated, and its shared round bounds how
/// *often* the probe runs, not what one run costs. As
/// `EXISTS (SELECT 1 ... WHERE queue = $1 LIMIT 1)` it cost a full sequential
/// scan of `pgqueue.jobs` — `EXISTS` strips any `ORDER BY`, so the planner had
/// no ordering to satisfy and costed an early exit against average-rows-per-
/// queue. Linear in retained history, which `JobRetention::Forever` never
/// bounds, on the pool the worker dequeues with.
#[sqlx::test(migrations = "./migrations")]
async fn test_health_probe_uses_an_index_when_history_belongs_to_another_queue(pool: PgPool) {
    seed_history_for_another_queue(&pool).await;
    let plan = generic_plan_for(&pool, pgqueue::__test_support::health_probe_sql(), "text", "'plan-quiet'").await;
    assert!(
        !plan.contains("Seq Scan on jobs"),
        "the unauthenticated health probe must not read the whole jobs table: {plan}"
    );
    assert!(
        plan.contains("Index Only Scan using jobs_page_idx on jobs"),
        "the probe answers from the queue's own page index: {plan}"
    );
}

/// Every open dashboard polls this per queue every 5s. The `execution` signal
/// evaluated its `running` arm first and always, and that arm carried the same
/// trap: with `running` rows common table-wide but absent from *this* queue, the
/// generic plan scanned every retained row, while its `aborting` sibling used
/// `jobs_active_idx` correctly.
#[sqlx::test(migrations = "./migrations")]
async fn test_queue_signals_use_an_index_when_running_jobs_belong_to_another_queue(pool: PgPool) {
    seed_history_for_another_queue(&pool).await;
    let plan = generic_plan_for(&pool, pgqueue::__test_support::dashboard_signals_sql(), "text", "'plan-quiet'").await;
    assert!(!plan.contains("Seq Scan on jobs"), "no signal may read the whole jobs table: {plan}");
    assert!(
        plan.contains("Index Only Scan Backward using jobs_active_idx on jobs"),
        "the execution signal answers from one backward walk of the active index: {plan}"
    );
    assert_eq!(
        plan.matches("Index Only Scan using jobs_dashboard_terminal_idx on jobs").count(),
        2,
        "failed and aborted recency must each use the terminal index: {plan}"
    );
}

/// `jobs_dashboard_name_prefix_idx` was built for the typeahead and the planner
/// never chose it. The suggestions went through `pgqueue.job_page_keys`, which
/// attaches `ORDER BY enqueued_at DESC, id DESC LIMIT` to every per-status
/// lateral, and `lower(name)` is a *range* qual sitting ahead of those sort
/// columns in that index — so it could not deliver the ordering the lateral
/// demanded. The planner fell back to `jobs_page_idx`, which could, and demoted
/// the prefix to a Filter: measured over 300,000 retained rows and 520 distinct
/// names, `Rows Removed by Filter: 254167` and ~2.6M buffers *per keystroke*,
/// growing with retention and unbounded under `JobRetention::Forever`.
///
/// Taken under `force_generic_plan` like its two siblings, and for a sharper
/// reason than theirs: a generic plan cannot fold `$4` into a constant, and
/// `starts_with` — like `^@` and `LIKE` — only becomes a prefix *range* when the
/// planner can see the pattern as one. Dropping the ordering was therefore not
/// enough; measured on the dataset below, the custom plan was a
/// `Bitmap Index Scan on jobs_dashboard_name_prefix_idx` while the generic plan
/// was a `Seq Scan on jobs` with the prefix back to a Filter — the exact
/// regression the index exists to prevent, held off only by the custom plan
/// happening to cost less under `plan_cache_mode = auto`. The statement now asks
/// for the index's own `text_pattern_ops` comparisons and ordering, which take a
/// parameter, and walks one distinct name per index descent.
#[sqlx::test(migrations = "./migrations")]
async fn test_job_name_typeahead_reaches_the_index_built_for_it(pool: PgPool) {
    let _db = TestDb::new(pool.clone()).await;
    // Enough retained history — and few enough rows under the prefix — that
    // walking the queue's own pages is the cheaper plan unless the prefix
    // itself can be a range.
    sqlx::query(
        "INSERT INTO pgqueue.jobs (queue, name, status, kind, enqueued_at)
         SELECT 'default', 'job_' || lpad((g % 200)::text, 3, '0'), 'complete', 'job',
                now() - (g * interval '1 second')
         FROM generate_series(1, 20000) AS g",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::raw_sql("ANALYZE pgqueue.jobs").execute(&pool).await.unwrap();

    let plan = generic_plan_for(
        &pool,
        pgqueue::__test_support::job_name_typeahead_sql(),
        "text, text[], text, text, bigint",
        "'default', ARRAY['queued', 'running', 'complete', 'failed', 'aborting', \
         'aborted'], 'job', 'job_01', 20",
    )
    .await;

    assert!(
        plan.contains("Index Scan using jobs_dashboard_name_prefix_idx"),
        "the typeahead must reach the index built for its prefix: {plan}"
    );
    assert!(
        !plan.contains("jobs_page_idx") && !plan.contains("Seq Scan on jobs"),
        "a keystroke must not read the queue's whole retained history: {plan}"
    );
    // Both halves of the walk, so a shape that finds the first name by index and
    // then scans forward for the rest cannot pass.
    assert_eq!(
        plan.matches("Index Scan using jobs_dashboard_name_prefix_idx").count(),
        2,
        "the seed and the step must each be an index descent: {plan}"
    );
}

/// The listing's `?name=` filter is the fourth of the five statements whose plan
/// is pinned. `pgqueue.job_page_keys` carried the name
/// as `(p_name IS NULL OR j.name = p_name)`, which the planner turns into an
/// index condition only by folding the parameter into a constant — so the fast
/// path was held up by nothing more than the custom plan happening to cost less
/// under `plan_cache_mode = auto`, exactly the fragility the typeahead was fixed
/// for. Measured over 350,000 retained rows, the generic plan was an
/// `Index Scan using jobs_dashboard_status_page_idx` with the name back to a
/// `Filter` and `Rows Removed by Filter: 35556`: 29,422 buffers and 105 ms per
/// page, growing with retention and unbounded under `JobRetention::Forever`, on
/// the pool the worker dequeues and finalizes with. An operator setting
/// `plan_cache_mode = force_generic_plan` — a common cure for planning overhead
/// — got that plan every time.
#[sqlx::test(migrations = "./migrations")]
async fn test_the_name_filtered_listing_reaches_the_name_index(pool: PgPool) {
    let _db = TestDb::new(pool.clone()).await;
    // Enough rows per status that reading a status partition to filter one name
    // out of it is measurably the wrong plan.
    sqlx::query(
        // `started_at` for the two active statuses (`g % 6` of 1 and 2), which
        // have to carry the clock recovery reads: see
        // `jobs_active_started_at_check`.
        "INSERT INTO pgqueue.jobs (queue, name, status, kind, enqueued_at, started_at)
         SELECT 'default', 'job_' || lpad((g % 200)::text, 3, '0'),
                (ARRAY['queued','running','aborting','complete','failed','aborted'])[1 + (g % 6)],
                'job', now() - (g * interval '1 second'),
                CASE WHEN g % 6 IN (1, 2) THEN now() END
         FROM generate_series(1, 20000) AS g",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::raw_sql("ANALYZE pgqueue.jobs").execute(&pool).await.unwrap();

    let plan = generic_plan_for(
        &pool,
        pgqueue::__test_support::job_page_by_name_sql(),
        "text, text[], text, text, timestamptz, uuid, bigint",
        "'default', ARRAY['queued', 'running', 'complete', 'failed', 'aborting', \
         'aborted'], 'job', 'job_007', NULL, NULL, 50",
    )
    .await;

    assert!(
        plan.contains("jobs_dashboard_name_page_idx"),
        "a name-filtered page must ride the index that carries the name: {plan}"
    );
    // The name as an index condition, not as a filter applied to rows the scan
    // had to read anyway — which is the whole difference.
    assert!(plan.contains("(name = $4)"), "the name must be an index qual under the generic plan: {plan}");
    assert!(
        !plan.contains("jobs_dashboard_status_page_idx") && !plan.contains("Seq Scan on jobs"),
        "a name-filtered page must not read a whole status partition: {plan}"
    );
}

/// And the fifth: the same page with *no* `?name=`, which is the dashboard's
/// default view and so the one every open tab polls every 5s. Its `keys` CTE was
/// never the problem — the per-status laterals ride
/// `jobs_dashboard_status_page_idx` either way. The row lookup was. As
/// `JOIN pgqueue.jobs ON jobs.id = keys.id` it was costed against `LIMIT $6`,
/// and a generic plan estimates a parameterized `LIMIT` at 10% of its input:
/// 561 rows on the seed below against the 51 the page actually returns, which is
/// past the point where a hash join whose probe side is a bare sequential scan
/// looks cheaper than one primary-key descent per row. Measured on this dataset
/// under `force_generic_plan`: `Hash Join ... -> Seq Scan on jobs (rows=100000)`,
/// 1,983 buffers and 9.7 ms *per poll*, growing with retention and unbounded
/// under `JobRetention::Forever`, on the pool the worker dequeues, heartbeats and
/// finalizes with. As a lateral it is 51 `jobs_pkey` descents: 263 buffers and
/// 0.26 ms.
///
/// Two-character names, deliberately: the heap's *row width* is what decides
/// this plan, and the same 100,000 rows under the sibling test's
/// `'job_' || lpad(...)` pack 49 rows to a page instead of 52 — enough for the
/// sequential scan to out-cost the estimate and for the old shape to survive on
/// this machine. A compact table is the case that loses, and it is the case a
/// queue that keeps its payloads small and its history forever ends up in.
#[sqlx::test(migrations = "./migrations")]
async fn test_the_unfiltered_listing_looks_its_rows_up_by_primary_key(pool: PgPool) {
    let _db = TestDb::new(pool.clone()).await;
    sqlx::query(
        // `started_at` for the two active statuses, as in the sibling test
        // above: `jobs_active_started_at_check`.
        "INSERT INTO pgqueue.jobs (queue, name, status, kind, enqueued_at, started_at)
         SELECT 'default', chr(97 + ((g % 200) / 26)) || chr(97 + ((g % 200) % 26)),
                (ARRAY['queued','running','aborting','complete','failed','aborted'])[1 + (g % 6)],
                'job', now() - (g * interval '1 second'),
                CASE WHEN g % 6 IN (1, 2) THEN now() END
         FROM generate_series(1, 100000) AS g",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::raw_sql("ANALYZE pgqueue.jobs").execute(&pool).await.unwrap();

    let plan = generic_plan_for(
        &pool,
        pgqueue::__test_support::job_page_sql(),
        "text, text[], text, timestamptz, uuid, bigint",
        "'default', ARRAY['queued', 'running', 'complete', 'failed', 'aborting', \
         'aborted'], 'job', NULL, NULL, 51",
    )
    .await;

    assert!(
        !plan.contains("Seq Scan on jobs"),
        "the dashboard's default page must not read every retained row: {plan}"
    );
    assert!(
        plan.contains("Index Scan using jobs_pkey"),
        "its rows are looked up one primary-key descent at a time: {plan}"
    );
    assert!(
        plan.contains("Index Only Scan using jobs_dashboard_status_page_idx"),
        "and its keys still come from the status page index: {plan}"
    );
}

// ---------------------------------------------------------------------------
// A shared round must outlive the request that started it, and must not park
// the requests riding it forever
// ---------------------------------------------------------------------------

/// How many backends are parked on a lock while running the queue-signal poll.
///
/// Prefix-matched, unlike [`blocked_health_probes`]: `pg_stat_activity.query` is
/// truncated at `track_activity_query_size` (1024 bytes by default) and this
/// statement is longer than that, so its full text can never compare equal. The
/// prefix is still taken from the shipped statement rather than a copy of it.
async fn blocked_queue_signals(pool: &PgPool) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT count(*)::bigint FROM pg_stat_activity
         WHERE datname = current_database()
           AND wait_event_type = 'Lock'
           AND left(query, 200) = left($1, 200)",
    )
    .bind(pgqueue::__test_support::dashboard_signals_sql())
    .fetch_one(pool)
    .await
    .unwrap()
}

/// `/health` is unauthenticated, so a client this process does not control sets
/// its rate — and could set it by *leaving*. The probe round ran inside the
/// request future, so a `GET /health` followed by a connection reset dropped the
/// handler, aborted every `dashboard_probe()` in its `JoinSet`, and left the
/// cache empty. Repeated at line rate that kept one round permanently in flight
/// against the pool the worker dequeues, heartbeats and finalizes with, which is
/// exactly the load the cache exists to prevent.
#[sqlx::test(migrations = "./migrations")]
async fn test_health_probe_round_outlives_the_request_that_started_it(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // Counted, not merely observed parked. `blocked_health_probes` reports
    // *backends* waiting on the lock, and aborting the Rust task does not stop
    // the backend — sqlx sends no `CancelRequest`, and a backend in `ProcSleep`
    // never notices a closed client socket — so that count is 1 whether the round
    // was detached or ran inline in the request. It cannot tell the two worlds
    // apart, and this test's whole subject is which world it is in. Executions
    // can: a detached round completes and fills the cache, so the probe runs
    // once for the whole test; an inline one is dropped with its request, leaves
    // the cache empty, and the `/health` below opens a second round.
    let Some(mut stats) = Stats::new(&db.database).await else {
        Stats::skipped("test_health_probe_round_outlives_the_request_that_started_it");
        return;
    };
    let probes = stats.since_now(crate::HEALTH_PROBE_STATEMENT).await;
    // The shipped 500ms probe TTL is a budget this test cannot hold to: between
    // observing that the round finished and asking the question that depends on
    // its result, it runs a `pg_stat_activity` query, opens a transaction, takes
    // an `ACCESS EXCLUSIVE` lock and issues a request — ~1.2ms of it measured,
    // and none of it bounded while 300-odd tests run beside it. Over the TTL the
    // value goes stale, the next request opens a round of its own, that round
    // blocks on this test's own lock, and both assertions below fail on a
    // schedule rather than on the behaviour they describe. Raised, they assert
    // *that* the cache answered.
    let router =
        pgqueue::__test_support::dashboard_health_probe_ttl(dashboard([db.queue.clone()]), Duration::from_secs(300))
            .router()
            .unwrap();

    // Parks the probe inside PostgreSQL, standing in for any slow probe.
    let mut lock = pool.begin().await.unwrap();
    sqlx::raw_sql("LOCK TABLE pgqueue.jobs IN ACCESS EXCLUSIVE MODE").execute(&mut *lock).await.unwrap();

    let abandoned = tokio::spawn({
        let router = router.clone();
        async move { router.oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap()).await }
    });
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "the /health probe never reached the database",
        || async { blocked_health_probes(&pool).await >= 1 },
    )
    .await;

    // The client walks away mid-probe.
    abandoned.abort();
    let _ = abandoned.await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        blocked_health_probes(&pool).await,
        1,
        "the probe round must survive the request that started it, not be \
         aborted with it"
    );

    lock.rollback().await.unwrap();
    // No intervening request before the check below, deliberately. One that finds
    // the cache empty *fills* it, so a `/health` here would leave the next
    // request answered from cache whether the abandoned round survived or died
    // with its request — which is exactly how this test used to pass in both
    // worlds. Statement counts cannot separate them either: an inline round
    // dropped mid-query leaves a backend PostgreSQL never finishes delivering to,
    // so it is not recorded, and the refill that replaces it is. The only thing
    // that distinguishes them is whether a value reaches the cache with nobody
    // asking for one.
    //
    // The round's backend leaving the lock wait is its statement finishing; the
    // publish is the next thing that task does.
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "the abandoned round never finished its probe",
        || async { blocked_health_probes(&pool).await == 0 },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // With the table locked against every reader again, a prompt answer is only
    // possible from a value already in the cache — and only the abandoned
    // request's round can have written one. A round that died with its request
    // leaves this to open a probe of its own, which blocks on this lock and
    // answers 503.
    let mut lock = pool.begin().await.unwrap();
    sqlx::raw_sql("LOCK TABLE pgqueue.jobs IN ACCESS EXCLUSIVE MODE").execute(&mut *lock).await.unwrap();
    let (status, _) = get_json(&router, "/health").await;
    assert_eq!(status, StatusCode::OK, "the request was not answered from the abandoned round's published value");
    assert_eq!(
        blocked_health_probes(&pool).await,
        0,
        "the next request must be served from the cache the completed round wrote"
    );
    lock.rollback().await.unwrap();

    // One probe for the whole test: the round the abandoned request started is
    // the one that filled the cache, so neither `/health` above needed a round of
    // its own. Two would mean the abandoned round died with its request.
    assert_eq!(stats.delta(&probes).await, 1, "the abandoned request's round must be the one that fills the cache");
}

/// The CSRF guard is a per-handler call, not a layer, so it is only as good as
/// its weakest route — and the shared request helper attaches
/// `X-PGQueue-Request` to every POST it builds, which left three of the four
/// guarded routes with no test that they refuse a request without it.
///
/// `SameSite=Strict` does not cover this. The guard exists because a browser
/// *does* replay cached HTTP Basic credentials on a cross-site form post, which
/// is the deployment [`Dashboard::basic_auth`] documents: without the header
/// check, any page the operator visits could force arbitrary jobs to re-run, or
/// log them out.
#[sqlx::test(migrations = "./migrations")]
async fn test_every_state_changing_route_refuses_a_request_without_the_action_header(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db.queue.enqueue_raw(new_job("guarded", |_| {})).await.unwrap().unwrap();
    // Authenticated, and with `basic_auth` configured at all: the account routes
    // are only mounted when it is, and `require_auth` runs ahead of the guard, so
    // an anonymous request would be answered 404 or 401 and prove nothing about
    // the header check.
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();
    let credential = "Basic YWRtaW46czNjcmV0cHc=";

    // Bodies that *extract*, so the guard is what refuses them. Axum runs the
    // `Json` extractor before the handler, so a malformed body is answered 422
    // without the guard ever being consulted — which says nothing about it.
    for (path, body) in [
        (format!("/api/queues/default/jobs/{id}/abort"), "{}"),
        (format!("/api/queues/default/jobs/{id}/retry"), "{}"),
        ("/api/account/password".to_string(), r#"{"current_password":"s3cretpw","new_password":"a-new-password"}"#),
        ("/api/account/logout".to_string(), "{}"),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&path)
                    .header(header::AUTHORIZATION, credential)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path} accepted a POST with no action header");
    }

    // And the job is untouched: a refused request must not have run first.
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Queued);
}

/// `/api/queues` issued one signal query per configured queue with neither the
/// TTL cache nor the single-flight gate `/health` was given, and `app.mjs` polls
/// it every 5s per open tab: 20 queues across 30 tabs is 600 concurrent queries
/// parked on `pool.acquire()` against the worker's own pool, which is how
/// dashboard load turns into the duplicate execution the lease protocol exists
/// to bound.
#[sqlx::test(migrations = "./migrations")]
async fn test_queue_overview_shares_one_round_and_a_cached_result(pool: PgPool) {
    const REQUESTS: usize = 8;
    let db = TestDb::new(crate::pool_with_max(&pool, REQUESTS as u32).await).await;
    // The cached half below has to be a fact about the cache rather than about
    // how quickly this runner got from the publish to the assertion: the
    // shipped window is 1s, and joining eight requests, taking every connection
    // in the pool and issuing 25 more requests is not bounded by anything while
    // 300-odd tests run beside it.
    let router =
        pgqueue::__test_support::dashboard_queue_signals_ttl(dashboard([db.queue.clone()]), Duration::from_secs(300))
            .router()
            .unwrap();

    let mut lock = pool.begin().await.unwrap();
    sqlx::raw_sql("LOCK TABLE pgqueue.jobs IN ACCESS EXCLUSIVE MODE").execute(&mut *lock).await.unwrap();

    let spawned = tokio::time::Instant::now();
    let mut requests = tokio::task::JoinSet::new();
    for _ in 0..REQUESTS {
        let router = router.clone();
        requests.spawn(async move {
            router.oneshot(Request::builder().uri("/api/queues").body(Body::empty()).unwrap()).await.unwrap().status()
        });
    }
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "no queue-signal poll ever reached the database",
        || async { blocked_queue_signals(&pool).await >= 1 },
    )
    .await;
    // Sampled across the window, for the reason given in
    // `test_concurrent_health_requests_run_one_probe_when_the_probe_is_slow`: the invariant is "at most one fan-out is
    // ever in flight", and checking it continuously is both stronger than a single reading and independent of when that
    // reading would have landed. Reproduced as `left: 0, right: 1` on a loaded run, where the reading arrived after the
    // round had gone. Measured from `spawned`, and sampled at least once, for the same reasons as there, too.
    let window = spawned + round_wait_timeout() / 2;
    loop {
        assert!(
            blocked_queue_signals(&pool).await <= 1,
            "{REQUESTS} concurrent overview requests must share one fan-out rather \
             than taking a pooled connection each"
        );
        if tokio::time::Instant::now() >= window {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    lock.rollback().await.unwrap();
    while let Some(status) = requests.join_next().await {
        assert_eq!(status.unwrap(), StatusCode::OK);
    }

    // And the result is reused, so a second tab polling costs nothing. Every
    // connection is held, not just one: the pool is sized for the fan-out
    // above, so leaving seven free meant all 25 requests answered 200 with the
    // cache removed entirely and this half asserted nothing. Held, a miss has
    // to park on `acquire`, give up after `ROUND_WAIT_TIMEOUT` and answer 503.
    let mut held = Vec::new();
    for _ in 0..REQUESTS {
        held.push(db.queue.pool().acquire().await.unwrap());
    }
    for _ in 0..25 {
        let (status, body) = get_json(&router, "/api/queues").await;
        assert_eq!(status, StatusCode::OK, "a flood of overview polls must not each need a pooled connection");
        assert_eq!(body["queues"][0]["name"], "default");
    }
    drop(held);
}

/// The fan-out returns on the first failing queue, and dropping a bare
/// `JoinHandle` *detaches* its task rather than cancelling it. So every sibling
/// still waiting went on to take a pooled connection and run its query — against
/// the pool the worker dequeues, heartbeats and finalizes with — long after the
/// round it belonged to had already answered. The round only owns its tasks if
/// the handles abort on drop.
///
/// The sibling here waits on `pool.acquire()` rather than on a lock inside
/// PostgreSQL, which is where a saturated pool actually parks it, and is the
/// only wait a dropped task can still be released from: sqlx sends no
/// `CancelRequest`, so a statement already on the wire runs to completion
/// whatever happens to the future awaiting it.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_failed_queue_signal_cancels_the_siblings_still_waiting(pool: PgPool) {
    let db = TestDb::new(crate::pool_with_max(&pool, 1).await).await;
    let Some(mut stats) = Stats::new(&db.database).await else {
        Stats::skipped("test_a_failed_queue_signal_cancels_the_siblings_still_waiting");
        return;
    };
    // A queue whose pool is closed, so its signal query fails immediately.
    let doomed_pool = crate::pool_with_max(&pool, 1).await;
    let doomed = Queue::builder("postgres://unused").pool(doomed_pool.clone()).name("doomed").connect().await.unwrap();
    doomed_pool.close().await;

    // The sibling's only connection, so its task parks on `acquire`.
    let held = db.queue.pool().acquire().await.unwrap();
    let signals = stats.since_now(QUEUE_SIGNALS_STATEMENT).await;

    // The failing queue is configured first, so the round answers while the
    // sibling is still waiting for a connection.
    let router = dashboard([doomed, db.queue.clone()]).router().unwrap();
    let (status, _) = get_json(&router, "/api/queues").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    // Releasing the connection is what a detached sibling was waiting for.
    drop(held);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        stats.delta(&signals).await,
        0,
        "a round that has already answered must not go on spending the \
         worker's pool on the queues it never reported"
    );
}

/// Waiters used to queue on the gate with no bound at all, so a wedged database
/// parked every unauthenticated `/health` — and every overview poll — forever.
/// An orchestrator learns nothing from a probe that never returns.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_wedged_probe_answers_503_instead_of_parking_the_request(pool: PgPool) {
    let db = TestDb::new(crate::pool_with_max(&pool, 4).await).await;
    let router = dashboard([db.queue.clone()]).router().unwrap();

    let mut lock = pool.begin().await.unwrap();
    sqlx::raw_sql("LOCK TABLE pgqueue.jobs IN ACCESS EXCLUSIVE MODE").execute(&mut *lock).await.unwrap();

    let health = tokio::spawn({
        let router = router.clone();
        async move { get_json(&router, "/health").await }
    });
    let overview = tokio::spawn({
        let router = router.clone();
        async move { get_json(&router, "/api/queues").await }
    });
    let (health, _) = health.await.unwrap();
    let (overview, body) = overview.await.unwrap();
    assert_eq!(health, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(overview, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"], "queue signals unavailable");

    lock.rollback().await.unwrap();
}

// ---------------------------------------------------------------------------
// Job bodies must not leave PostgreSQL unbounded
// ---------------------------------------------------------------------------

/// `DashboardJobSummaryRow` is documented as "the list representation without
/// the potentially large job bodies", and the detail route then returned
/// `payload`, `result` and `meta` whole. Public writers now cap them at 1 MiB,
/// but foreign SQL can still put larger JSON into the bare JSONB columns. The
/// response therefore needs its own smaller bound before parsing.
///
/// `error` is the fourth of them, and was left whole after the other three were
/// capped. Its database-enforced 1 MiB ceiling is still much more than an
/// operator page should transfer and render.
#[sqlx::test(migrations = "./migrations")]
async fn test_job_detail_truncates_bodies_instead_of_returning_them_whole(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let huge: Uuid = sqlx::query_scalar(
        "INSERT INTO pgqueue.jobs (queue, name, status, kind, payload, result, meta, error)
         VALUES ('default', 'huge', 'complete', 'job',
                 jsonb_build_object('blob', repeat('x', 200000)),
                 jsonb_build_object('blob', repeat('y', 200000)),
                 jsonb_build_object('blob', repeat('z', 200000)),
                 repeat('e', 200000))
         RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let router = dashboard([db.queue.clone()]).router().unwrap();
    let (status, body) = get_json(&router, &format!("/api/queues/default/jobs/{huge}")).await;
    assert_eq!(status, StatusCode::OK);
    for field in ["payload", "result", "meta", "error"] {
        let value = body["job"][field]
            .as_str()
            .unwrap_or_else(|| panic!("{field} must come back as the prefix that fit, not the whole body"));
        assert_eq!(value.chars().count(), 64 * 1024, "{field} must be cut to the cap");
        assert_eq!(body["job"][format!("{field}_truncated")], json!(true));
    }

    // A body that fits is still the value it always was.
    let small = db.queue.enqueue_raw(new_job("small", |_| {})).await.unwrap().unwrap();
    let (status, body) = get_json(&router, &format!("/api/queues/default/jobs/{small}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["job"]["payload"], json!({"n": 1}));
    assert_eq!(body["job"]["payload_truncated"], json!(false));
    assert_eq!(body["job"]["result_truncated"], json!(false));
    assert_eq!(body["job"]["meta_truncated"], json!(false));
    assert_eq!(body["job"]["error_truncated"], json!(false));

    // An `error` that fits is the message itself, not a prefix of it: the cap
    // is the only thing `left()` changes about a column that was already text.
    let short: Uuid = sqlx::query_scalar(
        "INSERT INTO pgqueue.jobs (queue, name, status, kind, payload, error)
         VALUES ('default', 'short', 'failed', 'job', '{}', 'boom')
         RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let (status, body) = get_json(&router, &format!("/api/queues/default/jobs/{short}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["job"]["error"], json!("boom"));
    assert_eq!(body["job"]["error_truncated"], json!(false));
}

/// Library-written worker documents are capped at 1 MiB, but foreign SQL can
/// bypass that guard and the worker routes return up to a hundred leases. The
/// response cap applies unchanged; only the author differs.
#[sqlx::test(migrations = "./migrations")]
async fn test_worker_views_truncate_stats_and_metadata_instead_of_returning_them_whole(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let huge = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO pgqueue.workers (id, queue, stats, metadata, expires_at)
         VALUES ($1, $2,
                 jsonb_build_object('blob', repeat('x', 200000)),
                 jsonb_build_object('blob', repeat('y', 200000)),
                 now() + interval '60 seconds')",
    )
    .bind(huge)
    .bind(db.queue.name())
    .execute(&pool)
    .await
    .unwrap();

    let router = dashboard([db.queue.clone()]).router().unwrap();
    let (status, body) = get_json(&router, "/api/queues/default/workers").await;
    assert_eq!(status, StatusCode::OK);
    let listed = &body["workers"][0];
    let (status, body) = get_json(&router, &format!("/api/queues/default/workers/{huge}")).await;
    assert_eq!(status, StatusCode::OK);
    let detailed = &body["worker"];

    // The list route is the one that multiplies the cost by the page size, so
    // both are asserted rather than the detail alone.
    for worker in [listed, detailed] {
        for field in ["stats", "metadata"] {
            let value = worker[field]
                .as_str()
                .unwrap_or_else(|| panic!("{field} must come back as the prefix that fit, not the whole document"));
            assert_eq!(value.chars().count(), 64 * 1024, "{field} must be cut to the cap");
            assert_eq!(worker[format!("{field}_truncated")], json!(true));
        }
    }

    // A document that fits is still the value it always was, and `metadata` is
    // still allowed to be absent.
    let small = Uuid::now_v7();
    db.queue
        .write_worker_info(small, json!({"complete": 3}), Some(json!({"region": "eu"})), Duration::from_secs(60))
        .await
        .unwrap();
    let (status, body) = get_json(&router, &format!("/api/queues/default/workers/{small}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["worker"]["stats"], json!({"complete": 3}));
    assert_eq!(body["worker"]["metadata"], json!({"region": "eu"}));
    assert_eq!(body["worker"]["stats_truncated"], json!(false));
    assert_eq!(body["worker"]["metadata_truncated"], json!(false));

    let bare = Uuid::now_v7();
    db.queue.write_worker_info(bare, json!({}), None, Duration::from_secs(60)).await.unwrap();
    let (status, body) = get_json(&router, &format!("/api/queues/default/workers/{bare}")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["worker"]["metadata"].is_null());
    assert_eq!(body["worker"]["metadata_truncated"], json!(false));
}

/// The cut is `left(body::text, cap + 1)` and the flag is "did it come back
/// longer than the cap", so the one length where the two could disagree is the
/// cap exactly: off by one there and every body of precisely 65,536 characters
/// is reported as a truncated string instead of the value it is, which the
/// dashboard then renders as JSON-in-a-string with a "… truncated" marker.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_body_of_exactly_the_cap_comes_back_parsed(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // Sized against the empty document's own rendering rather than against a
    // hand-counted wrapper, so a change in how PostgreSQL spaces `jsonb::text`
    // moves the fixture instead of silently moving it off the boundary.
    let insert = "INSERT INTO pgqueue.jobs (queue, name, status, kind, payload)
         SELECT 'default', 'at-the-cap', 'complete', 'job',
                jsonb_build_object('blob', repeat('x', $1 - length(jsonb_build_object('blob', '')::text)))
         RETURNING id";
    let exact: Uuid = sqlx::query_scalar(insert).bind(64 * 1024_i32).fetch_one(&pool).await.unwrap();
    let over: Uuid = sqlx::query_scalar(insert).bind(64 * 1024_i32 + 1).fetch_one(&pool).await.unwrap();
    let rendered = async |id: Uuid| {
        sqlx::query_scalar::<_, i32>("SELECT length(payload::text)::int FROM pgqueue.jobs WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap()
    };
    assert_eq!(rendered(exact).await, 64 * 1024, "fixture is off the cap");
    assert_eq!(rendered(over).await, 64 * 1024 + 1, "fixture is off the cap");

    let router = dashboard([db.queue.clone()]).router().unwrap();
    let (status, body) = get_json(&router, &format!("/api/queues/default/jobs/{exact}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["job"]["payload_truncated"], json!(false), "a body that reaches the cap fits in it");
    assert_eq!(
        body["job"]["payload"]["blob"].as_str().map(str::len),
        Some(64 * 1024 - 12),
        "and comes back parsed, not as its own rendering"
    );

    // One character more is the first that does not fit.
    let (status, body) = get_json(&router, &format!("/api/queues/default/jobs/{over}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["job"]["payload_truncated"], json!(true));
    assert_eq!(body["job"]["payload"].as_str().map(|body| body.chars().count()), Some(64 * 1024));
}

/// PostgreSQL can store JSON nested deeper than serde_json's recursion limit. Show an unreadable body as text so
/// an operator can inspect it, regardless of whether dependency feature unification enables arbitrary-precision numbers.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_body_this_client_cannot_parse_is_shown_rather_than_dropped(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO pgqueue.jobs (queue, name, status, kind, payload)
         VALUES ('default', 'unreadable', 'complete', 'job', $1::jsonb)
         RETURNING id",
    )
    .bind(format!("{}0{}", "[".repeat(256), "]".repeat(256)))
    .fetch_one(&pool)
    .await
    .unwrap();
    let stored: String = sqlx::query_scalar("SELECT payload::text FROM pgqueue.jobs WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        serde_json::from_str::<Value>(&stored).is_err(),
        "the fixture must be a body this client really cannot parse"
    );

    let router = dashboard([db.queue.clone()]).router().unwrap();
    let (status, body) = get_json(&router, &format!("/api/queues/default/jobs/{id}")).await;
    assert_eq!(status, StatusCode::OK, "an unreadable body is not a 500");
    assert_eq!(body["job"]["payload"], json!(stored), "it is handed over as the text PostgreSQL rendered");
    assert_eq!(body["job"]["payload_truncated"], json!(false), "and it is not truncation: the whole body is there");
}

/// The typeahead walks `lower(name)` through an index keyed on the fold, so
/// within one status two names differing only in case are one key and only one
/// of them is offered. The per-status walks are independent, though, and the
/// closing `GROUP BY name` groups by the name rather than by the fold, so
/// across statuses every variant a walk landed on survives — which is why a
/// full response can carry fewer names than the limit once case is ignored.
#[sqlx::test(migrations = "./migrations")]
async fn test_job_name_suggestions_collapse_case_within_a_status_but_not_across_them(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    sqlx::query(
        "INSERT INTO pgqueue.jobs (queue, name, status, kind) VALUES
             ('default', 'Report', 'complete', 'job'),
             ('default', 'report', 'complete', 'job'),
             ('default', 'REPORT', 'failed', 'job')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let router = dashboard([db.queue.clone()]).router().unwrap();

    let (status, body) = get_json(&router, "/api/queues/default/job-names?kind=job&prefix=rep&status=complete").await;
    assert_eq!(status, StatusCode::OK);
    let names = body["names"].as_array().unwrap();
    assert_eq!(names.len(), 1, "one folded key inside one status is one suggestion: {names:?}");
    assert!(names[0] == "Report" || names[0] == "report", "and it is one of the variants that key holds: {names:?}");

    // A second status walks the same key independently and lands on its own
    // variant, which the grouping then keeps beside the first.
    let (status, body) =
        get_json(&router, "/api/queues/default/job-names?kind=job&prefix=rep&status=complete,failed").await;
    assert_eq!(status, StatusCode::OK);
    let names = body["names"].as_array().unwrap();
    assert_eq!(names.len(), 2, "{names:?}");
    assert!(names.contains(&json!("REPORT")), "{names:?}");
}

/// `pgqueue.jobs.name` is `varchar(255)`, so anything longer is a request the
/// query can only answer with zero rows — and answering it costs a pooled
/// connection and a round trip against the pool the worker dequeues with, on a
/// route an unauthenticated typeahead drives per keystroke.
#[sqlx::test(migrations = "./migrations")]
async fn test_overlong_job_names_and_prefixes_are_refused_before_the_query(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).router().unwrap();
    let longest = "n".repeat(255);
    let overlong = "n".repeat(256);

    for (route, value) in [
        ("/api/queues/default/jobs?name=", &longest),
        ("/api/queues/default/job-names?prefix=", &longest),
    ] {
        let (status, _) = get_json(&router, &format!("{route}{value}")).await;
        assert_eq!(status, StatusCode::OK, "255 is a name the column can hold");
    }
    for (route, message) in [
        ("/api/queues/default/jobs?name=", "job name is too long"),
        ("/api/queues/default/job-names?prefix=", "job name prefix is too long"),
    ] {
        let (status, body) = get_json(&router, &format!("{route}{overlong}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{route}");
        assert_eq!(body["error"], message);
    }
}

/// `?kind=` is what a form submits for an unset select, and it is not an unknown
/// kind: both routes read it as the default, exactly as an absent one.
#[sqlx::test(migrations = "./migrations")]
async fn test_an_empty_kind_parameter_means_the_default_kind(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    sqlx::query(
        "INSERT INTO pgqueue.jobs (queue, name, status, kind) VALUES
             ('default', 'plain_job', 'complete', 'job'),
             ('default', 'nightly', 'complete', 'cron')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let router = dashboard([db.queue.clone()]).router().unwrap();

    let (status, empty) = get_json(&router, "/api/queues/default/jobs?kind=").await;
    assert_eq!(status, StatusCode::OK);
    let (_, absent) = get_json(&router, "/api/queues/default/jobs").await;
    assert_eq!(empty, absent);
    assert_eq!(empty["jobs"].as_array().unwrap().len(), 1);
    assert_eq!(empty["jobs"][0]["name"], "plain_job");

    let (status, body) = get_json(&router, "/api/queues/default/job-names?kind=&prefix=p").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["names"], json!(["plain_job"]));
}

/// Every route that answers whether a worker is *live* carries
/// `expires_at > now()`, so the lease the sweeper is about to purge is already
/// invisible: a detail page reached from a stale bookmark or an open tab must
/// 404 rather than present a dead worker as one you can still reason about.
#[sqlx::test(migrations = "./migrations")]
async fn test_an_expired_worker_lease_is_not_served(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let live = Uuid::now_v7();
    let lapsed = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO pgqueue.workers (id, queue, stats, expires_at) VALUES
             ($1, $3, '{}'::jsonb, now() + interval '60 seconds'),
             ($2, $3, '{}'::jsonb, now() - interval '1 second')",
    )
    .bind(live)
    .bind(lapsed)
    .bind(db.queue.name())
    .execute(&pool)
    .await
    .unwrap();

    let router = dashboard([db.queue.clone()]).router().unwrap();
    let (status, body) = get_json(&router, &format!("/api/queues/default/workers/{live}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["worker"]["id"], json!(live));

    let (status, body) = get_json(&router, &format!("/api/queues/default/workers/{lapsed}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "worker not found");

    // And the listing agrees, so the page cannot link to what the detail refuses.
    let (status, body) = get_json(&router, "/api/queues/default/workers").await;
    assert_eq!(status, StatusCode::OK);
    let listed = body["workers"].as_array().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["id"], json!(live));
}

// ---------------------------------------------------------------------------
// A rotated session cookie must inherit the expiry it preserved server-side
// ---------------------------------------------------------------------------

/// The `Max-Age` seconds of a `Set-Cookie` header.
fn cookie_max_age(response: &Response) -> u64 {
    response.headers()[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .map(str::trim)
        .find_map(|attribute| attribute.strip_prefix("Max-Age="))
        .expect("session cookie carries a Max-Age")
        .parse()
        .expect("Max-Age is a number of seconds")
}

/// `rotate_password` deliberately carries the *old* expiry onto the re-minted
/// session, "so a rotation neither logs the admin out nor extends their
/// session". The cookie half did not hold up its end: it was built with a
/// hard-coded full `SESSION_TTL`, so an admin who logged in at 09:00 and
/// changed their password at 20:55 was handed a cookie the browser kept until
/// ~08:55 the next day while the session itself died at 21:00 — a dead
/// credential persisted on disk almost a whole TTL longer than intended.
#[sqlx::test(migrations = "./migrations")]
async fn test_rotated_session_cookie_expires_when_the_replaced_session_would_have(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    let login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("username=admin&password=s3cretpw"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login.status(), StatusCode::SEE_OTHER);
    let issued_max_age = cookie_max_age(&login);
    let cookie = login.headers()[header::SET_COOKIE].to_str().unwrap().split(';').next().unwrap().to_string();

    let rotation = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/password")
                .header(header::COOKIE, &cookie)
                .header("x-pgqueue-request", "dashboard")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"current_password":"s3cretpw","new_password":"newsecret"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rotation.status(), StatusCode::OK);
    let rotated_max_age = cookie_max_age(&rotation);

    assert!(
        rotated_max_age < issued_max_age,
        "a rotation must not restart the cookie's lifetime: issued {issued_max_age}, \
         rotated {rotated_max_age}"
    );
    // ...and it inherits the surviving expiry rather than inventing a new one,
    // so the admin is not logged out either.
    assert!(
        rotated_max_age + 60 >= issued_max_age,
        "the rotated cookie must inherit the replaced session's expiry: issued \
         {issued_max_age}, rotated {rotated_max_age}"
    );
}

// ---------------------------------------------------------------------------
// SQL branches that no integration test reached
// ---------------------------------------------------------------------------

async fn dashboard_login(router: &Router, password: &str) -> Response {
    dashboard_login_from(router, None, password).await
}

/// The peer address a served request would carry. `axum::serve` records it as a
/// `ConnectInfo` extension, so setting it here is what a real connection from
/// that address looks like to the router.
fn dashboard_peer(host: u8) -> ConnectInfo<SocketAddr> {
    ConnectInfo(SocketAddr::from(([10, 0, 0, host], 40_000 + u16::from(host))))
}

async fn dashboard_login_from(router: &Router, peer: Option<ConnectInfo<SocketAddr>>, password: &str) -> Response {
    let mut request = Request::builder()
        .method("POST")
        .uri("/login")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded");
    if let Some(peer) = peer {
        request = request.extension(peer);
    }
    router
        .clone()
        .oneshot(request.body(Body::from(format!("username=admin&password={password}"))).unwrap())
        .await
        .unwrap()
}

/// The same form post with the `Sec-Fetch-Site` a browser attaches on its own.
/// `dashboard_login_from` is the header-less shape a curl or a script sends.
async fn dashboard_login_from_site(
    router: &Router,
    peer: Option<ConnectInfo<SocketAddr>>,
    site: &str,
    password: &str,
) -> Response {
    let mut request = Request::builder()
        .method("POST")
        .uri("/login")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header("sec-fetch-site", site);
    if let Some(peer) = peer {
        request = request.extension(peer);
    }
    router
        .clone()
        .oneshot(request.body(Body::from(format!("username=admin&password={password}"))).unwrap())
        .await
        .unwrap()
}

/// `POST /login` is the one state-changing route that cannot require the action
/// header — it is a real `<form method="post">`, so nothing of ours runs before
/// the browser sends it — and its `application/x-www-form-urlencoded` body is a
/// CORS-simple content type, so any page the operator visits can post it with no
/// preflight. Each post spent a comparison from the *victim's* interactive
/// budget, keyed to the victim's own address, before anything was compared: a
/// page they merely visited locked them out of their own dashboard, however
/// privately it is bound. Sequential posts do not do it — the failure delay
/// matches the refill rate — so the flood below is concurrent, which is what an
/// attacking page actually does.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_login_refuses_a_cross_site_post_before_spending_the_budget(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    let mut flood = tokio::task::JoinSet::new();
    for attempt in 0..64 {
        let router = router.clone();
        flood.spawn(async move {
            dashboard_login_from_site(&router, Some(dashboard_peer(1)), "cross-site", &format!("wrong-{attempt}"))
                .await
                .status()
        });
    }
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    // The operator's own address — the one the flood is charged to, if it is
    // charged to anything — while the flood is still in flight, because a
    // budget spent and refilled by the time they arrive is no lockout.
    let login = dashboard_login_from(&router, Some(dashboard_peer(1)), "s3cretpw").await;
    assert_eq!(
        login.status(),
        StatusCode::SEE_OTHER,
        "a cross-site flood must not spend the operator's own login budget"
    );
    assert!(login.headers().get(header::SET_COOKIE).is_some());

    let mut refused = 0;
    while let Some(status) = flood.join_next().await {
        refused += usize::from(status.unwrap() == StatusCode::FORBIDDEN);
    }
    assert_eq!(refused, 64, "every cross-site post must be refused");
}

/// The guard is only ever a statement about a browser's own origin, so it must
/// not turn away the operator's real form post or a client that sends no
/// `Sec-Fetch-Site` at all. `none` is a typed URL or a bookmark; a missing
/// header is curl, a password manager, or a script — none of which a page can
/// cause. `same-site` is refused: the form is served by the dashboard itself,
/// so a genuine submission is always `same-origin`.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_login_accepts_every_post_a_browser_calls_its_own(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    for site in ["same-origin", "none"] {
        let login = dashboard_login_from_site(&router, None, site, "s3cretpw").await;
        assert_eq!(login.status(), StatusCode::SEE_OTHER, "a {site} login must be accepted");
    }
    assert_eq!(
        dashboard_login(&router, "s3cretpw").await.status(),
        StatusCode::SEE_OTHER,
        "a client that sends no Sec-Fetch-Site must be accepted"
    );
    let refused = dashboard_login_from_site(&router, None, "same-site", "s3cretpw").await;
    assert_eq!(refused.status(), StatusCode::FORBIDDEN);
    assert!(
        String::from_utf8(axum::body::to_bytes(refused.into_body(), usize::MAX).await.unwrap().to_vec())
            .unwrap()
            .contains("Cross-site login posts are refused."),
        "the refusal must say so on the login form"
    );
}

async fn dashboard_login_with_origin(
    router: &Router,
    origin: Option<&str>,
    host: Option<&str>,
    password: &str,
) -> Response {
    let mut request = Request::builder()
        .method("POST")
        .uri("/login")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded");
    if let Some(origin) = origin {
        request = request.header(header::ORIGIN, origin);
    }
    if let Some(host) = host {
        request = request.header(header::HOST, host);
    }
    router
        .clone()
        .oneshot(request.body(Body::from(format!("username=admin&password={password}"))).unwrap())
        .await
        .unwrap()
}

/// Without Fetch Metadata — a legacy browser, or an intermediary that stripped
/// it — `Origin` is the fallback those same browsers still attach to a
/// cross-origin POST. A mismatched or opaque origin is refused before it can
/// spend the account's login budget; a matching one (default ports normalized
/// on either side) and a client that sends neither header stay accepted.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_login_refuses_a_mismatched_origin_without_fetch_metadata(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()])
        .basic_auth("admin", "s3cretpw")
        .allowed_hosts(["dash.example", "elsewhere.example"])
        .router()
        .unwrap();

    for (origin, host) in [
        (Some("https://evil.example"), Some("dash.example")),
        (Some("null"), Some("dash.example")),
        (Some("https://dash.example:8443"), Some("dash.example")),
    ] {
        let refused = dashboard_login_with_origin(&router, origin, host, "s3cretpw").await;
        assert_eq!(refused.status(), StatusCode::FORBIDDEN, "{origin:?} against {host:?} must be refused");
    }

    for (origin, host) in [
        (Some("https://dash.example"), Some("dash.example")),
        (Some("https://dash.example"), Some("dash.example:443")),
        (Some("https://dash.example:8443"), Some("dash.example:8443")),
        // No Origin at all is a non-browser client, whatever the Host says.
        (None, Some("dash.example")),
    ] {
        let accepted = dashboard_login_with_origin(&router, origin, host, "s3cretpw").await;
        assert_eq!(accepted.status(), StatusCode::SEE_OTHER, "{origin:?} against {host:?} must be accepted");
    }

    // `Sec-Fetch-Site` remains authoritative when present: a browser that says
    // `same-origin` is believed without an authority comparison.
    let mut request = Request::builder()
        .method("POST")
        .uri("/login")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header("sec-fetch-site", "same-origin")
        .header(header::ORIGIN, "https://dash.example")
        .header(header::HOST, "elsewhere.example");
    request = request.extension(dashboard_peer(3));
    let response = router
        .clone()
        .oneshot(request.body(Body::from("username=admin&password=s3cretpw".to_string())).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
}

/// DNS rebinding points a domain an attacker controls at the dashboard's address, so the browser takes the attacker's
/// page for the dashboard's own and every check against cross-site requests passes. The `Host` header still names the
/// attacker's domain, so the dashboard refuses, pages and API alike, any name it was not given. `/health` answers under
/// any name, so probes need no configuration.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_refuses_a_host_name_it_was_not_given(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).allowed_hosts(["dash.example"]).router().unwrap();
    let status = |host: Option<&'static str>, path: &'static str| {
        let router = router.clone();
        async move {
            let mut request = Request::builder().uri(path);
            if let Some(host) = host {
                request = request.header(header::HOST, host);
            }
            router.oneshot(request.body(Body::empty()).unwrap()).await.unwrap().status()
        }
    };

    for host in [
        "127.0.0.1:8080",
        "10.1.2.3",
        "[::1]:8080",
        "localhost",
        "LOCALHOST.:8080",
        "dash.example",
        "Dash.Example.:443",
    ] {
        assert_eq!(status(Some(host), "/api/queues").await, StatusCode::OK, "{host}");
    }
    for host in [
        "evil.example",
        "evil.example:8080",
        "dash.example.evil.example",
        "localhost.evil.example",
        "[not-an-ip]",
    ] {
        for path in ["/", "/api/queues"] {
            assert_eq!(status(Some(host), path).await, StatusCode::FORBIDDEN, "{host} {path}");
        }
        assert_eq!(status(Some(host), "/health").await, StatusCode::OK, "{host} /health");
    }
    // A request that names no host at all comes from no browser, so it cannot be a rebinding.
    assert_eq!(status(None, "/api/queues").await, StatusCode::OK);

    // An entry that can never match a `Host` header fails the build, rather than refusing the clients it was for.
    for entry in ["dash.example:8443", "https://dash.example", "*.example", ""] {
        let error = dashboard([db.queue.clone()]).allowed_hosts([entry]).router().unwrap_err();
        assert!(matches!(&error, Error::Config(message) if message.contains("host")), "{entry:?}: {error}");
    }
}

async fn dashboard_basic_guess(router: &Router, peer: Option<ConnectInfo<SocketAddr>>) -> StatusCode {
    let mut request = Request::builder().uri("/api/queues").header(header::AUTHORIZATION, "Basic d3Jvbmc6Y3JlZHM=");
    if let Some(peer) = peer {
        request = request.extension(peer);
    }
    router.clone().oneshot(request.body(Body::empty()).unwrap()).await.unwrap().status()
}

/// The session cookie is found by name, and an empty value is not a session.
/// Testing emptiness on the *result* of the scan let the first same-name cookie
/// end it: a planted `name=` — the shape a cleared cookie leaves behind, and
/// one anybody can set over cleartext HTTP under `secure_cookies(false)` — hid
/// the real session sitting behind it. `remove_session` then silently no-opped
/// and `rotate_password` issued no replacement cookie: a persistent lockout.
#[sqlx::test(migrations = "./migrations")]
async fn test_session_cookie_is_read_past_an_empty_cookie_of_the_same_name(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    let login = dashboard_login(&router, "s3cretpw").await;
    assert_eq!(login.status(), StatusCode::SEE_OTHER);
    let cookie = login.headers()[header::SET_COOKIE].to_str().unwrap().split(';').next().unwrap().to_string();
    let (name, _) = cookie.split_once('=').expect("a name=value cookie");
    let empty = format!("{name}=");

    // One header carrying the decoy first, then the same pair split across two
    // `Cookie` field lines the way an HTTP/2 client may send them.
    for cookies in [
        vec![format!("{empty}; {cookie}")],
        vec![empty.clone(), cookie.clone()],
        vec![cookie.clone(), empty.clone()],
    ] {
        let mut request = Request::builder().uri("/api/queues");
        for line in &cookies {
            request = request.header(header::COOKIE, line);
        }
        let response = router.clone().oneshot(request.body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "the session must be found behind {cookies:?}");
    }
}

/// A cookie name is not unique. A browser sends one line per stored cookie of
/// that name and orders the longer `Path` first (RFC 6265 §5.4.2), so anyone who
/// can set a cookie on this host — a sibling subdomain, or a plain-HTTP
/// intermediary under `secure_cookies(false)` — can put a value of their
/// choosing *ahead* of the genuine session. Answering from the first match then
/// hid the real token behind it: every request 401'd while carrying a valid
/// session, the operator could not reach `logout` to revoke it, and a `logout`
/// driven through HTTP Basic reported success while revoking nothing.
///
/// The `__Host-` prefix stops the cookie being planted at all, but it requires
/// `Path=/`, so it is dropped under a mount path — which is the deployment the
/// module docs recommend, and the one this test uses.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_planted_cookie_of_the_same_name_cannot_hide_the_session(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // Nested, which is the deployment the module docs recommend and the one
    // that drops the `__Host-` cookie prefix — so it is the one where a planted
    // cookie is reachable at all.
    let router = Router::new().nest(
        "/admin",
        dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").mount_path("/admin").router().unwrap(),
    );

    let login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("username=admin&password=s3cretpw"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login.status(), StatusCode::SEE_OTHER);
    let cookie = login.headers()[header::SET_COOKIE].to_str().unwrap().split(';').next().unwrap().to_string();
    let (name, token) = cookie.split_once('=').expect("a name=value cookie");
    // A non-empty decoy: the shape anyone able to plant a cookie can arrange.
    let planted = format!("{name}=0000000000000000000000000000000000000000000000000000000000000000");

    // On one line and split across two, with the decoy first either way.
    for cookies in [
        vec![format!("{planted}; {cookie}")],
        vec![planted.clone(), cookie.clone()],
    ] {
        let mut request = Request::builder().uri("/admin/api/queues");
        for line in &cookies {
            request = request.header(header::COOKIE, line);
        }
        let response = router.clone().oneshot(request.body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "a planted cookie hid the session behind {cookies:?}");
    }

    // And logout must revoke the session it names rather than reporting success
    // for a decoy: after it, the genuine token is dead.
    let logout = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/api/account/logout")
                .header(header::COOKIE, format!("{planted}; {cookie}"))
                .header("x-pgqueue-request", "dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(logout.status(), StatusCode::OK);
    let after = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/admin/api/queues")
                .header(header::COOKIE, format!("{name}={token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(after.status(), StatusCode::UNAUTHORIZED, "logout reported success without revoking the session");
}

/// Posts a password change as the browser does, authenticated by `cookie`.
async fn dashboard_change_password(router: &Router, cookie: &str, current: &str, new: &str) -> Response {
    router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/password")
                .header(header::COOKIE, cookie)
                .header("x-pgqueue-request", "dashboard")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"current_password": current, "new_password": new}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

/// Credential bodies are tiny in normal use. Letting their extractors inherit Axum's 2 MiB default meant that many
/// public login requests could make hundreds of MiB of raw and decoded credentials coexist before the handler could
/// reject them. The limit must count actual body bytes without relying on a `Content-Length` header, and a refusal must
/// happen before the request can spend the client's authentication budget.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_limits_credential_request_bodies(pool: PgPool) {
    const OVERSIZED_CREDENTIAL_BYTES: usize = 8 * 1024;
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();
    let oversized_form = format!("username=admin&password={}", "x".repeat(OVERSIZED_CREDENTIAL_BYTES));

    // More than one full authentication burst, all from one client. If extraction reaches the handler, these requests
    // spend its entire budget before the valid login below arrives.
    let mut requests = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let router = router.clone();
        let body = oversized_form.clone();
        requests.spawn(async move {
            router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/login")
                        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                        .extension(dashboard_peer(1))
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap()
        });
    }
    while let Some(response) = requests.join_next().await {
        assert_eq!(response.unwrap().status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    let login = dashboard_login_from(&router, Some(dashboard_peer(1)), "s3cretpw").await;
    assert_eq!(login.status(), StatusCode::SEE_OTHER, "oversized bodies spent the login budget");
    let cookie = login.headers()[header::SET_COOKIE].to_str().unwrap().split(';').next().unwrap().to_string();

    let oversized_change = json!({
        "current_password": "s3cretpw",
        "new_password": "x".repeat(OVERSIZED_CREDENTIAL_BYTES),
    });
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/password")
                .header(header::COOKIE, &cookie)
                .header("x-pgqueue-request", "dashboard")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(oversized_change.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let response = dashboard_change_password(&router, &cookie, "s3cretpw", "replacement").await;
    assert_eq!(response.status(), StatusCode::OK, "the body limit refused an ordinary password change");
}

/// The minimum is stated in characters and was measured in `String::len()`,
/// which is UTF-8 bytes. `éééé` is four characters and eight bytes, so it was
/// accepted end to end — `200 {"changed": true}` — as an eight-character
/// password, and a three-character CJK one would have been too.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_password_minimum_counts_characters_not_bytes(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();
    let cookie = dashboard_login(&router, "s3cretpw").await.headers()[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let response = dashboard_change_password(&router, &cookie, "s3cretpw", "éééé").await;
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "four characters is under the minimum, whatever they weigh in bytes"
    );

    // Not a refusal of non-ASCII, and proof the account is untouched: the same
    // alphabet at the stated length is accepted, on the current password the
    // refused change would have replaced.
    let response = dashboard_change_password(&router, &cookie, "s3cretpw", "ééééééée").await;
    assert_eq!(response.status(), StatusCode::OK);
}

/// The delay a rejection carries bounds the latency of one guess, never the
/// rate of guesses: every concurrent failure past the single in-flight permit
/// was refused instantly, and a *correct* password skipped the gate entirely.
/// 303-versus-429 was therefore an unthrottled oracle — measured at ~4,800
/// guesses a second. The budget is now spent before anything is compared, so a
/// saturated account answers a correct password exactly as it answers a wrong
/// one.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_refuses_a_correct_password_while_guesses_have_the_budget_spent(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    let mut guesses = tokio::task::JoinSet::new();
    for attempt in 0..64 {
        let router = router.clone();
        guesses.spawn(async move { dashboard_login(&router, &format!("wrong-{attempt}")).await.status() });
    }
    // Every guess spends its attempt before its first await, so a handful of
    // scheduler turns — microseconds, far short of a refill — leaves the state
    // a burst of concurrent guesses leaves behind. No database work happens on
    // either path, so nothing here waits on the pool.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    let correct = dashboard_login(&router, "s3cretpw").await;
    assert_eq!(
        correct.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "a spent budget must refuse the correct password too, or the refusal is an oracle"
    );
    assert!(correct.headers().get(header::SET_COOKIE).is_none());

    let mut refused = 0;
    while let Some(status) = guesses.join_next().await {
        refused += usize::from(status.unwrap() == StatusCode::TOO_MANY_REQUESTS);
    }
    assert!(
        refused > 0,
        "concurrent guessing must run out of budget rather than being compared as fast as it \
         arrives"
    );

    // It is a throttle, not a lockout: the budget refills and the operator gets
    // back in.
    let recovered = wait_for_some(
        Duration::from_secs(10),
        Duration::from_millis(50),
        "the account never recovered after the guessing stopped",
        || async {
            let response = dashboard_login(&router, "s3cretpw").await;
            (response.status() == StatusCode::SEE_OTHER).then_some(())
        },
    );
    recovered.await;
}

/// One budget for the whole process is spent by whoever asks most. An attacker
/// never gets a refund — nothing they send matches — so a flood held the only
/// budget at zero and the operator's *correct* password was refused without
/// ever being read: measured at 5 logins in 100 against a moderate flood, and 0
/// in 100 against a saturating one, with no reset short of restarting the
/// process. Charging each client its own budget makes a flood cost the flooder.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_login_survives_a_guessing_flood_from_another_client(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    // Same endpoint, same channel, different client: only the address tells the
    // attacker's guesses apart from the operator's login.
    let mut flood = tokio::task::JoinSet::new();
    for attempt in 0..64 {
        let router = router.clone();
        flood.spawn(async move {
            dashboard_login_from(&router, Some(dashboard_peer(1)), &format!("wrong-{attempt}")).await.status()
        });
    }
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    let login = dashboard_login_from(&router, Some(dashboard_peer(2)), "s3cretpw").await;
    assert_eq!(
        login.status(),
        StatusCode::SEE_OTHER,
        "an operator elsewhere must still be able to sign in during a flood"
    );
    assert!(login.headers().get(header::SET_COOKIE).is_some());

    let mut refused = 0;
    while let Some(status) = flood.join_next().await {
        refused += usize::from(status.unwrap() == StatusCode::TOO_MANY_REQUESTS);
    }
    assert!(refused > 0, "the flooding client must have spent its own budget");
}

/// Behind a reverse proxy, or in a router nested without connection info, every
/// request looks like the same client — so the budget is split by channel too.
/// An `Authorization` header is anybody's to send and needs no session, no form
/// and no CSRF header, which makes the API the flood surface; the login form is
/// the only way in for an operator holding no session. Neither budget is any
/// larger than the single one was, so this costs nothing in guessing resistance.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_login_survives_a_basic_auth_flood_from_an_indistinguishable_client(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    let mut flood = tokio::task::JoinSet::new();
    for _ in 0..64 {
        let router = router.clone();
        flood.spawn(async move { dashboard_basic_guess(&router, None).await });
    }
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    let login = dashboard_login(&router, "s3cretpw").await;
    assert_eq!(login.status(), StatusCode::SEE_OTHER, "API guessing must not spend the login form's budget");

    let mut refused = 0;
    while let Some(status) = flood.join_next().await {
        refused += usize::from(status.unwrap() == StatusCode::TOO_MANY_REQUESTS);
    }
    assert!(refused > 0, "the flood must have spent the API budget");
}

/// Sends one request per connection and returns the whole response text.
async fn http_once(address: SocketAddr, request: String) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream =
        tokio::net::TcpStream::connect(address).await.unwrap_or_else(|error| panic!("connect to {address}: {error}"));
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

async fn http_login(address: SocketAddr, password: &str) -> String {
    let body = format!("username=admin&password={password}");
    http_once(
        address,
        format!(
            "POST /login HTTP/1.1\r\nHost: {address}\r\n\
             Content-Type: application/x-www-form-urlencoded\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ),
    )
    .await
}

/// A per-client budget is only per-client if the server records who the client
/// is, and nothing about a router served without connection info looks any
/// different — the throttle simply charges every request to one bucket again.
/// So this drives the real [`pgqueue::DashboardServer`] over sockets, from two
/// peer addresses at once: the IPv4-mapped and IPv6 loopbacks of one dual-stack
/// listener. Both sides post the login form, so the channel split cannot cover
/// for a missing address and the flood locks the operator out without it.
///
/// Skipped where a dual-stack listener will not accept IPv4, since one process
/// cannot then be reached from two addresses at all.
#[sqlx::test(migrations = "./migrations")]
async fn test_served_dashboard_charges_a_guess_to_the_client_that_made_it(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let Some(()) = dual_stack_loopback_available().await else {
        eprintln!("skipping: this host has no dual-stack loopback");
        return;
    };

    let dashboard =
        dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").secure_cookies(false).serve_on("::", 0);
    let mut handle = dashboard.server_handle();
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(dashboard.run_until(shutdown.clone()));
    let bound = tokio::time::timeout(Duration::from_secs(5), handle.wait_until_ready()).await.unwrap().unwrap();
    let attacker = SocketAddr::from(([127, 0, 0, 1], bound.port()));
    let operator = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], bound.port()));

    let flooding = CancellationToken::new();
    let refused = Arc::new(AtomicUsize::new(0));
    let mut flood = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let stop = flooding.clone();
        let refused = refused.clone();
        flood.spawn(async move {
            while !stop.is_cancelled() {
                let response = tokio::select! {
                    biased;
                    _ = stop.cancelled() => break,
                    response = http_login(attacker, "wrong") => response,
                };
                if response.starts_with("HTTP/1.1 429") {
                    refused.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
    }
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "the flooding client never ran out of budget",
        || async { refused.load(Ordering::SeqCst) > 0 },
    )
    .await;

    // The flood is still running throughout: the operator's budget is untouched
    // by it, so every one of these succeeds rather than winning a share of a
    // budget the flood keeps at zero.
    for attempt in 0..10 {
        let response = http_login(operator, "s3cretpw").await;
        assert!(
            response.starts_with("HTTP/1.1 303"),
            "attempt {attempt} was locked out by another client's flood: {response}"
        );
    }

    flooding.cancel();
    flood.join_all().await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), server).await.unwrap().unwrap().unwrap();
}

/// `Some(())` when a listener bound to `::` accepts an IPv4 loopback connection,
/// which is how one process is reached from two peer addresses.
async fn dual_stack_loopback_available() -> Option<()> {
    let listener = tokio::net::TcpListener::bind("[::]:0").await.ok()?;
    let port = listener.local_addr().ok()?.port();
    let connect = tokio::net::TcpStream::connect(SocketAddr::from(([127, 0, 0, 1], port)));
    tokio::time::timeout(Duration::from_secs(2), connect).await.ok()?.ok()?;
    listener.accept().await.ok()?;
    Some(())
}

/// Every other outcome of the login form re-renders the page. The throttled one
/// returned the JSON API's body, so a browser posting the form during a throttle
/// window was shown `{"error":"too many authentication attempts"}` as a bare
/// document with no way back to the form.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_login_form_renders_the_login_page_when_the_budget_is_spent(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    let mut guesses = tokio::task::JoinSet::new();
    for attempt in 0..64 {
        let router = router.clone();
        guesses.spawn(async move { dashboard_login(&router, &format!("wrong-{attempt}")).await });
    }
    let mut throttled = None;
    while let Some(response) = guesses.join_next().await {
        let response = response.unwrap();
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            throttled = Some(response);
        }
    }
    let throttled = throttled.expect("concurrent guessing must exhaust the budget");

    assert_eq!(throttled.headers()[header::RETRY_AFTER], "1", "a throttled form post still says when to come back");
    let content_type = throttled.headers()[header::CONTENT_TYPE].to_str().unwrap().to_string();
    assert!(content_type.starts_with("text/html"), "a form post must be answered with a page, not {content_type}");
}

// ---------------------------------------------------------------------------
// A cancelled sweep must not strand the sweep-leadership advisory lock
// ---------------------------------------------------------------------------

/// Posts the login form as a request that reached the dashboard through a
/// proxy: the socket peer is the proxy, and `forwarded` is the chain it appended
/// to.
async fn dashboard_login_forwarded(
    router: &Router,
    peer: ConnectInfo<SocketAddr>,
    forwarded: &str,
    password: &str,
) -> Response {
    let request = Request::builder()
        .method("POST")
        .uri("/login")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header("x-forwarded-for", forwarded)
        .extension(peer);
    router
        .clone()
        .oneshot(request.body(Body::from(format!("username=admin&password={password}"))).unwrap())
        .await
        .unwrap()
}

/// Starts a burst of wrong logins that all arrive through `peer`, each carrying
/// the chain `chain(attempt)`, and returns once they have spent what they can.
///
/// The guesses are still in flight: a budget refills a token every
/// `AUTH_ATTEMPT_REFILL`, and joining them would wait out the rejection delay
/// they each sleep — so what the caller is measuring has to be measured now.
async fn dashboard_forwarded_flood(
    router: &Router,
    peer: ConnectInfo<SocketAddr>,
    chain: impl Fn(usize) -> String,
) -> tokio::task::JoinSet<StatusCode> {
    let mut flood = tokio::task::JoinSet::new();
    for attempt in 0..64 {
        let router = router.clone();
        let forwarded = chain(attempt);
        flood.spawn(async move {
            dashboard_login_forwarded(&router, peer, &forwarded, &format!("wrong-{attempt}")).await.status()
        });
    }
    // Every guess spends its attempt before its first await, so a few scheduler
    // turns leave the state a concurrent burst leaves behind.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    flood
}

/// How many of a flood's guesses were refused without being compared.
async fn dashboard_flood_refusals(mut flood: tokio::task::JoinSet<StatusCode>) -> usize {
    let mut refused = 0;
    while let Some(status) = flood.join_next().await {
        refused += usize::from(status.unwrap() == StatusCode::TOO_MANY_REQUESTS);
    }
    refused
}

/// `POST /login` is itself the interactive channel and needs no credentials to
/// reach, so splitting the budget by channel does nothing for the login form
/// when every request lands in one client bucket. Behind a TLS-terminating
/// proxy — the deployment the docs recommend, since `DashboardServer` has no TLS
/// of its own — every socket peer *is* the proxy, so a flood of wrong passwords
/// from anywhere on the internet kept the operator's own login refused
/// indefinitely. Trusting the proxy's `X-Forwarded-For` restores per-client
/// keying, and charges the flood to the flooder.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_login_survives_a_proxied_flood_when_a_trusted_proxy_hop_is_configured(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // One proxy in front, so every request arrives from its address.
    let proxy = dashboard_peer(1);
    let attacker = "203.0.113.5";
    let operator = "198.51.100.9";

    // The default ignores the header, which behind a proxy is one bucket for
    // the whole internet: the operator is locked out by somebody else's flood.
    let shared = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();
    let flood = dashboard_forwarded_flood(&shared, proxy, |_| attacker.to_string()).await;
    assert_eq!(
        dashboard_login_forwarded(&shared, proxy, operator, "s3cretpw").await.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "keying by peer alone cannot tell two clients behind one proxy apart"
    );
    assert!(dashboard_flood_refusals(flood).await > 0, "the flood must have spent a budget");

    // Trusting exactly the proxies that are there makes the flood cost the
    // flooder and nobody else.
    let proxied = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").trusted_proxy_hops(1).router().unwrap();
    let flood = dashboard_forwarded_flood(&proxied, proxy, |_| attacker.to_string()).await;
    let login = dashboard_login_forwarded(&proxied, proxy, operator, "s3cretpw").await;
    assert_eq!(
        login.status(),
        StatusCode::SEE_OTHER,
        "an operator behind the same proxy must still be able to sign in during a flood"
    );
    assert!(login.headers().get(header::SET_COOKIE).is_some());
    assert!(dashboard_flood_refusals(flood).await > 0, "the flooding client must still spend its own budget");
}

/// Trusting a proxy must not hand the header to whoever sends it. The chain is
/// read from the right, so the entry charged is one the trusted proxies wrote
/// and a client forging entries only pushes its own address further along; a
/// chain too short to have crossed them all is not trusted at all and falls back
/// to the socket peer. Either way a flood cannot mint a fresh budget per
/// request, which is what honouring the leftmost entry would have cost.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_trusted_proxy_hops_ignore_forwarded_entries_a_client_forged(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let proxy = dashboard_peer(1);

    let one_hop = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").trusted_proxy_hops(1).router().unwrap();
    // Every guess claims a different origin, and the proxy appends the one real
    // address behind them all.
    let flood =
        dashboard_forwarded_flood(&one_hop, proxy, |attempt| format!("192.0.2.{}, 203.0.113.5", attempt % 256)).await;
    assert_eq!(
        dashboard_login_forwarded(&one_hop, proxy, "198.51.100.9", "s3cretpw").await.status(),
        StatusCode::SEE_OTHER,
        "the flood must stay charged to the client the proxy observed"
    );
    assert!(dashboard_flood_refusals(flood).await > 0, "a forged prefix must not mint a budget per request");

    // Two proxies configured, one entry supplied: the chain never crossed them,
    // so it names nobody and the peer pays.
    let two_hops =
        dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").trusted_proxy_hops(2).router().unwrap();
    let flood = dashboard_forwarded_flood(&two_hops, proxy, |attempt| format!("192.0.2.{}", attempt % 256)).await;
    assert_eq!(
        dashboard_login_forwarded(&two_hops, dashboard_peer(2), "192.0.2.1", "s3cretpw").await.status(),
        StatusCode::SEE_OTHER,
        "a different peer must keep its own budget"
    );
    assert!(
        dashboard_flood_refusals(flood).await > 0,
        "a chain shorter than the trusted hops must fall back to the peer, not be believed"
    );
}

// ---------------------------------------------------------------------------
// A dedupe collision the guarded read could not see is still a collision
// ---------------------------------------------------------------------------

/// `%00` percent-decodes into the `String` like any other byte, so a NUL sailed
/// past the length guards on `?name=` and `?prefix=`, reached PostgreSQL — which
/// cannot hold one in `text` (`22021`) — and came back as `Internal`: a 500 and
/// an `ERROR`-level log for a request whose own contract promises a 400, having
/// burned a pooled connection to find out. `?status=` already 400d on the same
/// input, and so does every other entry point that writes a name (`JobRequest`,
/// `JobError`). This is on a router that is unauthenticated unless `basic_auth`
/// is configured.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_name_filters_reject_a_nul_byte_as_a_bad_request(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(new_job("alpha", |_| {})).await.unwrap();
    let router = dashboard([db.queue.clone()]).router().unwrap();

    async fn get(router: &Router, path: &str) -> (StatusCode, String) {
        let response = router.clone().oneshot(Request::builder().uri(path).body(Body::empty()).unwrap()).await.unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    for (path, message) in [
        ("/api/queues/default/jobs?name=%00", "job name must not contain NUL"),
        ("/api/queues/default/jobs?name=alp%00ha", "job name must not contain NUL"),
        ("/api/queues/default/job-names?prefix=%00", "job name prefix must not contain NUL"),
        ("/api/queues/default/job-names?prefix=alp%00ha", "job name prefix must not contain NUL"),
    ] {
        let (status, body) = get(&router, path).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path} must be refused as malformed, not answered with {body}");
        assert!(body.contains(message), "{path} must say why it was refused: {body}");
    }

    // And the same filters without the NUL still answer normally, so the guard
    // rejects the byte rather than the endpoint.
    for path in [
        "/api/queues/default/jobs?name=alpha",
        "/api/queues/default/job-names?prefix=alp",
    ] {
        let (status, body) = get(&router, path).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        assert!(body.contains("alpha"), "{path} must still match: {body}");
    }
}

// ---------------------------------------------------------------------------
// A page cursor PostgreSQL cannot hold is a malformed request, not a 500
// ---------------------------------------------------------------------------

/// `cursor_pair` checked only that both halves of the cursor were present.
/// `Timestamp` reaches ISO year -9999 while `timestamptz` stops at
/// `4714-11-24 00:00:00 BC`, so every timestamp in between deserialized,
/// reached the query and came back as `22008` -> `Internal`: a 500 and an
/// `ERROR`-level log for a request this type promises to 400, having burned a
/// pooled connection to find out — the same class of defect as the `%00` name
/// filter above. Both paged endpoints funnel through that one helper, so both
/// were exposed, on a router that is unauthenticated unless `basic_auth` is
/// configured.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_page_cursors_reject_a_timestamp_postgres_cannot_hold(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(new_job("alpha", |_| {})).await.unwrap();
    let router = dashboard([db.queue.clone()]).router().unwrap();
    let cursor_id = Uuid::now_v7();

    async fn get(router: &Router, path: &str) -> (StatusCode, String) {
        let response = router.clone().oneshot(Request::builder().uri(path).body(Body::empty()).unwrap()).await.unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    for (endpoint, key) in [
        ("/api/queues/default/jobs", "cursor_enqueued_at"),
        ("/api/queues/default/workers", "cursor_started_at"),
    ] {
        // One nanosecond and one second under PostgreSQL's floor, plus a much
        // earlier Jiff timestamp.
        for timestamp in [
            "-004713-11-23T23:59:59.999999999Z",
            "-004713-11-23T23:59:59Z",
            "-009000-01-01T00:00:00Z",
        ] {
            let path = format!("{endpoint}?{key}={timestamp}&cursor_id={cursor_id}");
            let (status, body) = get(&router, &path).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "{path} must be refused as malformed, not answered with {body}"
            );
            assert!(
                body.contains("page cursor timestamp is out of range"),
                "{path} must say why it was refused: {body}"
            );
        }

        // PostgreSQL's floor exactly still pages, so the guard rejects the
        // value rather than the endpoint.
        let path = format!("{endpoint}?{key}=-004713-11-24T00:00:00Z&cursor_id={cursor_id}");
        let (status, body) = get(&router, &path).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
    }
}

// ---------------------------------------------------------------------------
// A deployment must be told when it is exposed or unthrottleable
// ---------------------------------------------------------------------------

/// Collects the rendered `message` of every event emitted while this is the
/// default subscriber. A warning nobody can observe is not a warning, and these
/// two have no other effect to assert on.
#[derive(Clone, Default)]
struct RecordedMessages(Arc<std::sync::Mutex<Vec<String>>>);

impl RecordedMessages {
    fn matching(&self, needle: &str) -> Vec<String> {
        self.0.lock().expect("recorded messages").iter().filter(|message| message.contains(needle)).cloned().collect()
    }
}

impl tracing::field::Visit for RecordedMessages {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0.lock().expect("recorded messages").push(format!("{value:?}"));
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::layer::Layer<S> for RecordedMessages {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        event.record(&mut self.clone());
    }
}

/// Starts recording, returning the recorder and the guard that stops it.
fn record_messages() -> (RecordedMessages, tracing::subscriber::DefaultGuard) {
    use tracing_subscriber::layer::SubscriberExt;

    let recorded = RecordedMessages::default();
    // Thread-local, so it takes precedence over the suite's global subscriber
    // until the guard is dropped and nothing else.
    let guard =
        tracing::subscriber::set_default(tracing_subscriber::registry::Registry::default().with(recorded.clone()));
    (recorded, guard)
}

/// Explicitly disabling authentication serves every job payload, every worker's metadata and the retry and abort
/// actions to anyone who can reach the socket. It is a legitimate choice for an application supplying its own
/// middleware, but the process must announce the exposure.
#[sqlx::test(migrations = "./migrations")]
async fn test_building_a_dashboard_without_authentication_warns(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;

    let (recorded, guard) = record_messages();
    let _ = dashboard([db.queue.clone()]).router().unwrap();
    assert_eq!(
        recorded.matching("without authentication").len(),
        1,
        "an unauthenticated dashboard must announce itself"
    );
    let _ = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();
    assert_eq!(recorded.matching("without authentication").len(), 1, "a protected dashboard has nothing to warn about");
    drop(guard);
}

/// `AuthClient::of(..).unwrap_or(Self::Any)` collapses every client into one
/// throttle bucket when requests carry no peer — which is exactly what the
/// module header's `app.nest("/admin", ...router()?)` recipe served with a plain
/// `axum::serve(listener, app)` produces. Sixteen wrong Basic guesses plus ten a
/// second from anywhere then answer the operator's *correct* password `429` for
/// as long as the flood runs, and `trusted_proxy_hops` cannot help. `oneshot`
/// supplies no `ConnectInfo` either, so it stands in for that deployment.
#[sqlx::test(migrations = "./migrations")]
async fn test_unkeyed_authentication_throttle_warns_once(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = dashboard([db.queue.clone()]).basic_auth("admin", "s3cretpw").router().unwrap();

    let (recorded, guard) = record_messages();
    for _ in 0..3 {
        let (status, _) = get_json(&router, "/api/queues").await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
    assert_eq!(
        recorded.matching("one throttle bucket").len(),
        1,
        "the deployment problem is reported once, not once per request"
    );
    drop(guard);
}
