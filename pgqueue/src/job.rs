//! Jobs: definitions, configuration, context, handlers, enqueue requests,
//! stored rows, result handles, and cron scheduling.

use std::any::{Any, TypeId};
use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use cronexpr::{Crontab, FallbackTimezoneOption, MakeTimestamp, ParseOptions};
use jiff::{RoundMode, SignedDuration, Timestamp, TimestampRound, Unit};
use serde::de::DeserializeOwned;
use serde::ser::{
    SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple, SerializeTupleStruct,
    SerializeTupleVariant,
};
use serde::{Serialize, Serializer};
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::Error;
use crate::database::DatabaseEnqueueResult;
use crate::queue::{Queue, QueueDoneEvent};

// One hundred years is beyond a useful queue delay while remaining safe for
// SQL date arithmetic and runtime clocks.
//
// `#[pgqueue::job]` emits a compile-time assertion against `MAX_DURATION_MS`
// (re-exported through `__private`), so lowering this bound turns an
// out-of-range attribute literal into a build failure rather than a runtime one.
#[doc(hidden)]
pub const MAX_DURATION_MS: u64 = 3_153_600_000_000;
const MAX_DURATION: Duration = Duration::from_millis(MAX_DURATION_MS);

/// Seven days: long enough for a failure that happened over a weekend to
/// still be on the dashboard, and to be retried from it, on Monday.
pub(crate) const DEFAULT_FAILED_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// PostgreSQL's `timestamptz` floor — `4714-11-24 00:00:00 BC` UTC.
///
/// There is deliberately no matching ceiling: PostgreSQL's is 294277 AD, which
/// [`Timestamp`] cannot represent.
pub(crate) const MIN_TIMESTAMPTZ: Timestamp = Timestamp::constant(-210_866_803_200, 0);

pub(crate) fn validate_duration(field: &str, duration: Duration) -> Result<(), Error> {
    if duration > MAX_DURATION {
        return Err(Error::Config(format!("{field} exceeds the maximum supported duration of {MAX_DURATION:?}")));
    }
    Ok(())
}

/// [`validate_duration`] for fields where zero is meaningless rather than
/// "immediately", so every such field rejects it the same way.
pub(crate) fn validate_nonzero_duration(field: &str, duration: Duration) -> Result<(), Error> {
    if duration.is_zero() {
        return Err(Error::Config(format!("{field} must be greater than zero")));
    }
    validate_duration(field, duration)
}

/// Milliseconds for `duration`, rounding up, or `None` when it does not fit.
/// The rounding rule lives here so every conversion agrees; callers pick how to
/// handle a duration too large to represent.
pub(crate) fn duration_to_ms_checked(duration: Duration) -> Option<i64> {
    i64::try_from(duration.as_nanos().div_ceil(1_000_000)).ok()
}

pub(crate) fn duration_to_ms(duration: Duration) -> i64 {
    duration_to_ms_checked(duration).unwrap_or(i64::MAX)
}

/// Whether a job has attempts remaining. Shared by every row shape that
/// carries an attempt counter so the retry policy has one definition.
pub(crate) fn has_attempts_remaining(attempts: i32, max_attempts: i32) -> bool {
    max_attempts > attempts
}

/// Delay before the next retry, applying `backoff` to `retry_delay_ms`. Shared
/// so worker-driven and sweeper-driven retries can never diverge.
pub(crate) fn retry_delay_for(retry_delay_ms: i64, backoff: &JobRetryBackoff, attempts: i32) -> Duration {
    let base = Duration::from_millis(retry_delay_ms.max(0) as u64);
    backoff.next_delay(base, attempts.max(0) as u32)
}

/// How long a finished job's row is kept. A job carries one of these for a
/// `complete` finish ([`JobConfig::retention`], which keeps the result) and one
/// for a `failed` or `aborted` finish ([`JobConfig::failed_retention`], which
/// keeps the error).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobRetention {
    /// Keep the row for this long after it finishes, then the sweeper purges it.
    For(Duration),
    /// Keep the row forever. Reused dedupe keys and cron schedules retain one
    /// row per occurrence, so high-frequency recurring jobs should normally
    /// use a finite retention period.
    Forever,
    /// Delete the row as soon as a worker finishes it (no result retrieval).
    /// A queued job aborted before execution remains until the next sweep so
    /// waiters can observe its aborted result.
    DeleteImmediately,
}

impl JobRetention {
    /// Encoding for the `result_ttl_ms` column: `NULL` = forever, `0` = delete now.
    pub(crate) fn as_result_ttl_ms(self) -> Option<i64> {
        match self {
            JobRetention::For(d) => Some(duration_to_ms(d).max(1)),
            JobRetention::Forever => None,
            JobRetention::DeleteImmediately => Some(0),
        }
    }

    pub(crate) fn from_result_ttl_ms(result_ttl_ms: Option<i64>) -> Self {
        match result_ttl_ms {
            None => JobRetention::Forever,
            // A negative TTL has no encoding — the column now rejects one — but
            // decoding must not turn a row written by hand before that check
            // into a *live* zero-length retention, which is the one reading
            // that keeps the row instead of deleting it.
            Some(ms) if ms <= 0 => JobRetention::DeleteImmediately,
            Some(ms) => JobRetention::For(Duration::from_millis(ms as u64)),
        }
    }
}

/// Retry delay growth strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum JobRetryBackoff {
    /// Every retry waits exactly `retry_delay`.
    None,
    /// Exponential backoff with full jitter: the nth retry waits a uniformly
    /// random duration in `[0, min(max, retry_delay * 2^(n-1))]`. This
    /// strategy requires a non-zero `retry_delay`.
    Exponential {
        /// Upper bound for the un-jittered delay; `None` = unbounded.
        ///
        /// `default` is load-bearing: a `with` attribute disables serde's
        /// implicit missing-`Option`-is-`None` handling, and a stored backoff
        /// of `{"type":"exponential"}` must decode rather than poison every
        /// dequeue batch that selects its row.
        #[serde(rename = "max_ms", with = "opt_duration_ms", default)]
        max: Option<Duration>,
    },
}

impl JobRetryBackoff {
    /// Computes the delay before the next attempt. `attempts` is the number of
    /// attempts already made (>= 1 when retrying).
    pub(crate) fn next_delay(self, retry_delay: Duration, attempts: u32) -> Duration {
        match self {
            JobRetryBackoff::None => retry_delay.min(MAX_DURATION),
            JobRetryBackoff::Exponential { max } => {
                let capped = exponential_delay_bound(retry_delay, attempts, max);
                // Full jitter: a uniformly random delay up to the exponential
                // bound, so simultaneous retries spread out instead of
                // stampeding together.
                capped.mul_f64(rand::random::<f64>())
            }
        }
    }
}

fn exponential_delay_bound(retry_delay: Duration, attempts: u32, max: Option<Duration>) -> Duration {
    let exp = attempts.saturating_sub(1).min(63);
    let mut delay = retry_delay.min(MAX_DURATION);
    for _ in 0..exp {
        delay = delay.saturating_mul(2).min(MAX_DURATION);
        if delay == MAX_DURATION {
            break;
        }
    }
    max.map_or(delay, |max| delay.min(max)).min(MAX_DURATION)
}

impl sqlx::Type<sqlx::Postgres> for JobRetryBackoff {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <sqlx::types::Json<Self> as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <sqlx::types::Json<Self> as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for JobRetryBackoff {
    /// A strategy this build cannot read decodes as [`JobRetryBackoff::None`],
    /// the flat `retry_delay`, rather than failing the row.
    ///
    /// The `backoff` column now refuses one (see the migration), but decoding
    /// must not turn a row written by hand before that check — or by a newer
    /// version carrying a variant this build has never heard of — into an error
    /// that poisons its whole batch. The dequeue decodes its batch inside the
    /// claiming transaction, so a refusal there rolls the claim back — and then
    /// the next dequeue selects the same rows and fails the same way: one
    /// unreadable value would park itself at the head of the queue and block
    /// every job sorted behind it, claim after claim, until repaired by hand.
    /// It also fails `Queue::jobs_page` and the dashboard listing for the
    /// whole queue — the two places an operator would look to find the bad
    /// row.
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        // `Json` borrows the raw bytes, so keep them for the warning: `Decode`
        // sees one column, not the row it belongs to, and the stored text is
        // the only thing that identifies which strategy was unreadable.
        //
        // Binary-format `jsonb` — what every query here uses — arrives with a
        // one-byte version header (currently 1) in front of the JSON text, and
        // `as_str` returns the wire bytes verbatim. `Json::decode` strips that
        // header before parsing; without the same strip the warning glued a
        // stray U+0001 to the front of the very string it exists to show. No
        // JSON text begins with a control character, so this can only ever take
        // the header off.
        let raw = value.as_str().unwrap_or("<binary>");
        let raw = raw.strip_prefix('\u{1}').unwrap_or(raw).to_owned();
        match <sqlx::types::Json<Self> as sqlx::Decode<sqlx::Postgres>>::decode(value) {
            Ok(json) => Ok(json.0),
            Err(error) => {
                tracing::warn!(
                    backoff = %raw,
                    %error,
                    "unreadable job backoff; retrying with the flat retry delay"
                );
                Ok(JobRetryBackoff::None)
            }
        }
    }
}

mod opt_duration_ms {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(d) => {
                // Unlike `duration_to_ms`, a stored backoff cap must not
                // silently saturate: a value that cannot round-trip is an error.
                let millis = super::duration_to_ms_checked(*d)
                    .and_then(|ms| u64::try_from(ms).ok())
                    .ok_or_else(|| serde::ser::Error::custom("duration does not fit in u64 ms"))?;
                s.serialize_some(&millis)
            }
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
        Ok(Option::<u64>::deserialize(d)?.map(Duration::from_millis))
    }
}

/// Per-job configuration, set by the `#[pgqueue::job]` attribute and
/// overridable per enqueue.
#[derive(Debug, Clone, PartialEq)]
pub struct JobConfig {
    /// Maximum attempts allowed (1 = no retries).
    pub max_attempts: u32,
    /// Per-attempt wall-clock limit enforced by the worker; `None` = unlimited.
    ///
    /// Enforced by cancelling the handler's task, which takes effect at its
    /// next `.await`. A handler that blocks its runtime thread cannot be
    /// force-stopped in-process: past a short grace the attempt is finalized
    /// as timed out without it, and the thread it holds stays occupied until
    /// it returns — run blocking work on `tokio::task::spawn_blocking`. The
    /// attempt guards fence off the runaway handler's *finalizations of this
    /// attempt*, never its external side effects or the jobs it enqueues
    /// through its context: a timed-out handler running past its deadline is
    /// one of the overlaps at-least-once delivery already requires handlers
    /// to tolerate, so keep them idempotent. That
    /// finalization itself needs a free runtime thread: on Tokio's
    /// current-thread runtime, or a multi-thread one whose every worker thread
    /// is blocked, the deadline waits with the rest of the worker until the
    /// handler yields, which is why workers belong on the multi-thread
    /// runtime.
    ///
    /// A handler that settles *after* the deadline fired — `abort` cannot stop
    /// one already past its last yield point — keeps its **error**, because that
    /// is the only diagnosis the attempt produced and the outcome is a failure
    /// either way. Its **success** is discarded and the attempt is recorded as
    /// timed out: accepting it would make the deadline advisory, and by then the
    /// sweeper may already have adjudicated the attempt stuck.
    pub timeout: Option<Duration>,
    /// How long the row is retained after a `complete` finish.
    pub retention: JobRetention,
    /// How long the row is retained after a `failed` or `aborted` finish.
    ///
    /// Separate from [`JobConfig::retention`] because the two are read at
    /// different times: a result is collected within moments by whoever
    /// waited for it, while a failure is investigated by an operator after
    /// the fact. Under one shared clock the default that suits results
    /// purged every failure before anyone looked, and
    /// [`Queue::retry_job`] then had no row left to retry.
    pub failed_retention: JobRetention,
    /// Base delay before a retry.
    pub retry_delay: Duration,
    /// How the retry delay grows across attempts.
    pub backoff: JobRetryBackoff,
    /// Dequeue priority; lower values are dequeued first.
    pub priority: i16,
}

impl JobConfig {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.max_attempts == 0 {
            return Err(Error::Config("job max_attempts must allow at least one attempt".into()));
        }
        if self.max_attempts >= i32::MAX as u32 {
            return Err(Error::Config(format!("job max_attempts must not exceed {}", i32::MAX - 1)));
        }
        if let Some(timeout) = self.timeout {
            validate_nonzero_duration("job timeout", timeout)?;
        }
        if let JobRetention::For(ttl) = self.retention {
            validate_duration("job retention", ttl)?;
        }
        if let JobRetention::For(ttl) = self.failed_retention {
            validate_duration("job failed retention", ttl)?;
        }
        validate_duration("job retry delay", self.retry_delay)?;
        if let JobRetryBackoff::Exponential { max: Some(max) } = self.backoff {
            validate_nonzero_duration("job backoff maximum", max)?;
        }
        if matches!(self.backoff, JobRetryBackoff::Exponential { .. }) && self.retry_delay.is_zero() {
            return Err(Error::Config("exponential job backoff requires a non-zero retry delay".into()));
        }
        Ok(())
    }
}

impl Default for JobConfig {
    /// 1 attempt, 10s timeout, 10min result retention, 7-day failure
    /// retention, immediate retries, priority 0.
    fn default() -> Self {
        Self {
            max_attempts: 1,
            timeout: Some(Duration::from_secs(10)),
            retention: JobRetention::For(Duration::from_secs(600)),
            failed_retention: JobRetention::For(DEFAULT_FAILED_RETENTION),
            retry_delay: Duration::ZERO,
            backoff: JobRetryBackoff::None,
            priority: 0,
        }
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;

    #[test]
    fn test_retention_maps_to_result_ttl_ms() {
        assert_eq!(JobRetention::Forever.as_result_ttl_ms(), None);
        assert_eq!(JobRetention::DeleteImmediately.as_result_ttl_ms(), Some(0));
        assert_eq!(JobRetention::For(Duration::from_secs(1)).as_result_ttl_ms(), Some(1000));
        // Sub-millisecond retention still rounds up to 1ms (0 would mean delete).
        assert_eq!(JobRetention::For(Duration::from_micros(10)).as_result_ttl_ms(), Some(1));

        assert_eq!(JobRetention::from_result_ttl_ms(None), JobRetention::Forever);
        assert_eq!(JobRetention::from_result_ttl_ms(Some(0)), JobRetention::DeleteImmediately);
        assert_eq!(JobRetention::from_result_ttl_ms(Some(1500)), JobRetention::For(Duration::from_millis(1500)));
    }

    #[test]
    fn test_backoff_serde_round_trip() {
        let none = serde_json::to_value(JobRetryBackoff::None).unwrap();
        assert_eq!(none, serde_json::json!({"type": "none"}));
        assert_eq!(serde_json::from_value::<JobRetryBackoff>(none).unwrap(), JobRetryBackoff::None);

        let capped = JobRetryBackoff::Exponential { max: Some(Duration::from_secs(60)) };
        let json = serde_json::to_value(capped).unwrap();
        assert_eq!(json, serde_json::json!({"type": "exponential", "max_ms": 60000}));
        assert_eq!(serde_json::from_value::<JobRetryBackoff>(json).unwrap(), capped);

        let uncapped = JobRetryBackoff::Exponential { max: None };
        let json = serde_json::to_value(uncapped).unwrap();
        assert_eq!(json, serde_json::json!({"type": "exponential", "max_ms": null}));
        assert_eq!(serde_json::from_value::<JobRetryBackoff>(json).unwrap(), uncapped);

        // A stored value may omit the key entirely (written by an external
        // client); it must decode instead of poisoning dequeue batches.
        assert_eq!(
            serde_json::from_value::<JobRetryBackoff>(serde_json::json!({"type": "exponential"})).unwrap(),
            uncapped
        );

        assert!(serde_json::from_value::<JobRetryBackoff>(serde_json::json!({"type": "bogus"})).is_err());
    }

    #[test]
    fn test_backoff_none_is_flat() {
        let d = Duration::from_millis(250);
        for attempts in [0, 1, 5, 100] {
            assert_eq!(JobRetryBackoff::None.next_delay(d, attempts), d);
        }
    }

    #[test]
    fn test_backoff_exponential_respects_bounds() {
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(1);
        let backoff = JobRetryBackoff::Exponential { max: Some(max) };
        for attempts in 1..=20 {
            let un_jittered = base.saturating_mul(2u32.saturating_pow(attempts - 1)).min(max);
            for _ in 0..10 {
                let delay = backoff.next_delay(base, attempts);
                assert!(delay <= un_jittered, "attempt {attempts}: {delay:?} > {un_jittered:?}");
            }
        }
        // Uncapped growth doubles each attempt (jitter only shrinks it).
        let uncapped = JobRetryBackoff::Exponential { max: None };
        assert!(uncapped.next_delay(base, 4) <= base * 8);
        // Huge attempt counts must not overflow.
        assert!(uncapped.next_delay(MAX_DURATION, u32::MAX) <= MAX_DURATION);
    }

    #[test]
    fn test_exponential_bound_keeps_growing_past_u32_multiplier_range() {
        let base = Duration::from_millis(1);
        assert_eq!(exponential_delay_bound(base, 34, None), Duration::from_millis(1u64 << 33));
        assert_eq!(exponential_delay_bound(base, u32::MAX, None), MAX_DURATION);
        assert_eq!(exponential_delay_bound(base, 34, Some(Duration::from_secs(2))), Duration::from_secs(2));
    }

    #[test]
    fn test_job_config_defaults_match_documented_values() {
        let cfg = JobConfig::default();
        assert_eq!(cfg.max_attempts, 1);
        assert_eq!(cfg.timeout, Some(Duration::from_secs(10)));
        assert_eq!(cfg.retention, JobRetention::For(Duration::from_secs(600)));
        assert_eq!(cfg.retry_delay, Duration::ZERO);
        assert_eq!(cfg.backoff, JobRetryBackoff::None);
        assert_eq!(cfg.priority, 0);
    }

    #[test]
    fn test_job_config_rejects_unrepresentable_values() {
        let config = JobConfig { max_attempts: 0, ..JobConfig::default() };
        assert!(config.validate().is_err());
        let config = JobConfig { max_attempts: i32::MAX as u32, ..JobConfig::default() };
        assert!(config.validate().is_err());
        let config = JobConfig { timeout: Some(Duration::ZERO), ..JobConfig::default() };
        assert!(config.validate().is_err());
        let config = JobConfig { timeout: Some(Duration::MAX), ..JobConfig::default() };
        assert!(config.validate().is_err());
        let config = JobConfig {
            retry_delay: Duration::ZERO,
            backoff: JobRetryBackoff::Exponential { max: None },
            ..JobConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_huge_backoff_durations_fail_instead_of_wrapping() {
        let error = serde_json::to_value(JobRetryBackoff::Exponential { max: Some(Duration::MAX) }).unwrap_err();
        assert!(error.to_string().contains("does not fit"), "{error}");
    }

    #[test]
    fn test_duration_to_ms_saturates() {
        assert_eq!(duration_to_ms(Duration::from_secs(2)), 2000);
        assert_eq!(duration_to_ms(Duration::from_nanos(1)), 1);
        assert_eq!(duration_to_ms(Duration::from_micros(1_500)), 2);
        assert_eq!(duration_to_ms(Duration::MAX), i64::MAX);
    }
}

/// The reason a single job attempt did not complete successfully.
///
/// `#[non_exhaustive]`: this is an output — the crate is the only thing that
/// classifies an attempt — so naming a failure mode this list does not yet
/// cover must not need a breaking release. Constructing a variant is
/// unaffected; matching one outside this crate needs a `_` arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum JobErrorKind {
    /// The handler returned an error.
    Failed,
    /// The attempt exceeded the job's `timeout`.
    Timeout,
    /// The job was aborted (by a user, the sweeper, or worker shutdown).
    Aborted,
    /// The handler panicked.
    Panic,
    /// A context extractor failed (e.g. missing `JobState<T>`).
    Extract,
    /// The payload could not be deserialized, or the handler result could not
    /// be serialized.
    Decode,
}

impl JobErrorKind {
    const ALL: [Self; 6] = [
        Self::Failed,
        Self::Timeout,
        Self::Aborted,
        Self::Panic,
        Self::Extract,
        Self::Decode,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::Failed => "failed",
            Self::Timeout => "timeout",
            Self::Aborted => "aborted",
            Self::Panic => "panic",
            Self::Extract => "extract",
            Self::Decode => "decode",
        }
    }

    /// Whether a later attempt could plausibly succeed. Decode and extract
    /// failures are deterministic — the stored payload and the worker's
    /// registrations do not change between attempts — so retrying them only
    /// burns the job's backoff schedule.
    pub(crate) fn is_retryable(self) -> bool {
        !matches!(self, Self::Decode | Self::Extract)
    }
}

/// The result of a failed job attempt, stored in the job's `error` column.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, serde::Serialize, serde::Deserialize)]
#[error("{kind}: {message}", kind = self.kind.as_str())]
pub struct JobError {
    /// What category of failure occurred.
    pub kind: JobErrorKind,
    /// Human-readable detail (error display, panic message, ...).
    pub message: String,
}

impl JobError {
    /// A handler failure, from anything displayable (the common case).
    pub fn failed(err: impl std::fmt::Display) -> Self {
        Self::new(JobErrorKind::Failed, err)
    }

    /// Builds a [`JobError`] of the given kind.
    ///
    /// A NUL in the message becomes U+FFFD, so the substitution is visible
    /// rather than silent. Messages longer than the stored-error ceiling are
    /// shortened on a UTF-8 boundary and carry a visible truncation marker.
    /// PostgreSQL `text` cannot hold `\0` (`22021`) — an
    /// `anyhow::bail!("bad\0input")` would otherwise leave the attempt
    /// unfinalizable: every write of it fails, the worker retries the write
    /// forever, and the processor slot never comes back.
    pub fn new(kind: JobErrorKind, err: impl std::fmt::Display) -> Self {
        let message = err.to_string();
        let message = if message.contains('\0') { message.replace('\0', "\u{fffd}") } else { message };
        let prefix_bytes = kind.as_str().len() + ": ".len();
        Self { kind, message: truncate_owned_utf8(message, MAX_STORED_ERROR_BYTES - prefix_bytes) }
    }

    /// Reconstructs a [`JobError`] from the `error` column (the inverse of
    /// its `Display`). Unrecognized text becomes a plain `Failed` error.
    pub(crate) fn from_stored(text: &str) -> Self {
        for kind in JobErrorKind::ALL {
            let prefix = kind.as_str();
            if let Some(message) = text.strip_prefix(prefix).and_then(|message| message.strip_prefix(": ")) {
                return Self { kind, message: message.to_string() };
            }
        }
        Self { kind: JobErrorKind::Failed, message: text.to_string() }
    }
}

#[cfg(test)]
mod job_error_tests {
    use super::*;

    #[test]
    fn test_job_error_display_includes_kind_and_message() {
        let err = JobError::failed("boom");
        assert_eq!(err.to_string(), "failed: boom");
        let err = JobError::new(JobErrorKind::Timeout, "10s elapsed");
        assert_eq!(err.to_string(), "timeout: 10s elapsed");
        let err = JobError::new(JobErrorKind::Aborted, "user");
        assert_eq!(err.to_string(), "aborted: user");
        let err = JobError::new(JobErrorKind::Panic, "oops");
        assert_eq!(err.to_string(), "panic: oops");
        let err = JobError::new(JobErrorKind::Extract, "missing state");
        assert_eq!(err.to_string(), "extract: missing state");
        let err = JobError::new(JobErrorKind::Decode, "bad json");
        assert_eq!(err.to_string(), "decode: bad json");
    }

    #[test]
    fn test_job_error_round_trips_through_the_error_column() {
        for kind in JobErrorKind::ALL {
            let original = JobError::new(kind, "some detail");
            assert_eq!(JobError::from_stored(&original.to_string()), original);
        }
        // Unrecognized text (e.g. "swept", "cancelled") becomes Failed.
        let swept = JobError::from_stored("swept");
        assert_eq!(swept.kind, JobErrorKind::Failed);
        assert_eq!(swept.message, "swept");
    }

    #[test]
    fn test_job_error_round_trips_through_json() {
        let err = JobError::new(JobErrorKind::Timeout, "slow");
        let json = serde_json::to_string(&err).unwrap();
        let back: JobError = serde_json::from_str(&json).unwrap();
        assert_eq!(err, back);
    }

    #[test]
    fn test_job_error_truncates_to_the_storage_limit_on_a_utf8_boundary() {
        let error = JobError::new(JobErrorKind::Failed, "é".repeat(MAX_STORED_ERROR_BYTES));
        let stored = error.to_string();

        assert!(stored.len() <= MAX_STORED_ERROR_BYTES);
        assert!(stored.len() >= MAX_STORED_ERROR_BYTES - 3);
        assert!(stored.ends_with(ERROR_TRUNCATION_MARKER));
        assert!(stored.is_char_boundary(stored.len()));

        let direct_message = "é".repeat(MAX_STORED_ERROR_BYTES);
        let direct = truncate_stored_error(&direct_message);
        assert!(direct.len() <= MAX_STORED_ERROR_BYTES);
        assert!(direct.len() >= MAX_STORED_ERROR_BYTES - 3);
        assert!(direct.ends_with(ERROR_TRUNCATION_MARKER));
    }
}

/// Filter for [`Queue::jobs_page`](Queue::jobs_page).
#[derive(Debug, Clone, Default)]
pub struct JobFilter {
    /// Only jobs with this status.
    pub status: Option<JobStatus>,
    /// Only jobs with this handler name.
    pub name: Option<String>,
    /// Page size (default 50, maximum 1000).
    pub limit: Option<i64>,
    /// Return rows older than this cursor.
    pub before: Option<JobCursor>,
}

impl JobFilter {
    pub(crate) fn limit(&self) -> Result<i64, Error> {
        let limit = self.limit.unwrap_or(50);
        if !(1..=1000).contains(&limit) {
            return Err(Error::Config("job page limit must be between 1 and 1000".into()));
        }
        Ok(limit)
    }
}

/// Stable cursor for newest-first job pagination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct JobCursor {
    /// Enqueue timestamp of the last row in the previous page.
    pub enqueued_at: Timestamp,
    /// Job id used to make the timestamp ordering deterministic.
    pub id: Uuid,
}

impl From<&JobRow> for JobCursor {
    fn from(job: &JobRow) -> Self {
        Self { enqueued_at: job.enqueued_at, id: job.id }
    }
}

/// Lifecycle state of a job.
///
/// `Queued -> Running -> {Complete, Failed, Aborted}`, with retries moving a
/// job back to `Queued` and aborts of running jobs passing through `Aborting`.
///
/// `#[non_exhaustive]`, so adding a state — `Aborting` was one — is not a
/// breaking release for code that reads one.
///
/// Decoding is *not* lenient, unlike [`JobRetryBackoff`], which answers a value
/// this build cannot read with its safe default rather than failing the row.
/// The two differ in both halves of that argument. There is no safe substitute
/// for a status: it is the row's identity, `is_terminal` and every dashboard
/// action key off it, and answering `queued` for a state this build has never
/// heard of tells an operator to expect a run that will not happen. That
/// failure mode does not apply to status. A backoff decode refusal rolls the
/// claim back and the next dequeue re-selects the same rows, so one unreadable
/// value blocks every job queued behind it, claim after claim. The `status` a
/// claim returns is the literal `'running'`
/// its own statement just wrote, so a batch can never carry an unreadable
/// one. Most readers that can meet one — `Queue::jobs_page`, `Queue::fetch_job`,
/// the dashboard listing — are read-only, so a new value costs an old binary a
/// visible error on a page, not a job. `Database::aborting_of` is the exception
/// and completes the list: it decodes `status` for arbitrary in-flight ids under
/// no status filter, so one unreadable row there fails the whole abort poll and
/// degrades [`WorkerComponent::Abort`](crate::WorkerComponent::Abort) until it
/// goes. Reaching that needs a *newer* writer against the same queue as an older
/// worker, which the one-way migrations already refuse to support.
///
/// ```
/// assert_eq!(pgqueue::JobStatus::Running.as_str(), "running");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, sqlx::Type)]
#[serde(rename_all = "lowercase")]
#[sqlx(type_name = "text", rename_all = "lowercase")]
#[non_exhaustive]
pub enum JobStatus {
    /// Waiting to be picked up (possibly scheduled in the future).
    Queued,
    /// Currently running on a worker.
    Running,
    /// Abort requested while running; the worker will cancel it.
    Aborting,
    /// Finished successfully (terminal).
    Complete,
    /// Exhausted its attempts with an error (terminal).
    Failed,
    /// Aborted before completion (terminal).
    Aborted,
}

impl JobStatus {
    /// The lowercase string stored in the database.
    pub fn as_str(self) -> &'static str {
        match self {
            JobStatus::Queued => "queued",
            JobStatus::Running => "running",
            JobStatus::Aborting => "aborting",
            JobStatus::Complete => "complete",
            JobStatus::Failed => "failed",
            JobStatus::Aborted => "aborted",
        }
    }

    /// Whether this status is terminal (`complete`, `failed`, or `aborted`).
    pub fn is_terminal(self) -> bool {
        matches!(self, JobStatus::Complete | JobStatus::Failed | JobStatus::Aborted)
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for JobStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "queued" => Ok(JobStatus::Queued),
            "running" => Ok(JobStatus::Running),
            "aborting" => Ok(JobStatus::Aborting),
            "complete" => Ok(JobStatus::Complete),
            "failed" => Ok(JobStatus::Failed),
            "aborted" => Ok(JobStatus::Aborted),
            other => Err(format!("unknown job status: {other}")),
        }
    }
}

/// A fully-typed snapshot of one row in the jobs table.
///
/// Read-only, and [`non_exhaustive`] so that a column added to the schema can
/// be surfaced here without a breaking release — which is exactly what `kind`,
/// `cron_expr` and `retried_at` needed, having been in the table and on the
/// dashboard while this type could not see them.
///
/// [`non_exhaustive`]: https://doc.rust-lang.org/reference/attributes/type_system.html#the-non_exhaustive-attribute
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
#[non_exhaustive]
pub struct JobRow {
    /// Primary key (UUIDv7, time-ordered).
    pub id: Uuid,
    /// Dedupe identity; `None` = no dedupe.
    pub dedupe_key: Option<String>,
    /// Queue name.
    pub queue: String,
    /// Registered handler name.
    pub name: String,
    /// JSON payload.
    pub payload: Value,
    /// Current lifecycle state.
    pub status: JobStatus,
    /// Dequeue priority; lower first.
    pub priority: i16,
    /// Claims made so far (incremented at dequeue). Counts every claim, not
    /// every handler execution: an attempt a shutdown drained or a worker
    /// without the handler bounced increments this too, with the matching
    /// refund raising `max_attempts`.
    pub attempts: i32,
    /// Maximum attempts allowed.
    ///
    /// May exceed the value the job was enqueued with: a shutdown or an
    /// unhandled-name bounce that catches an attempt mid-flight refunds it by
    /// *raising* this — `attempts` is the number every recovery guard fences
    /// a claim on, so it can never be lowered — and the refund is permanent.
    /// The difference between the pair still means what it always did: the
    /// tries the job has left, none of them spent on a shutdown or a bounce.
    pub max_attempts: i32,
    /// Per-attempt timeout in milliseconds.
    pub timeout_ms: Option<i64>,
    /// Base retry delay in milliseconds.
    pub retry_delay_ms: i64,
    /// Retry backoff strategy.
    pub backoff: JobRetryBackoff,
    /// Result retention in milliseconds (`NULL` forever, `0` delete now).
    pub result_ttl_ms: Option<i64>,
    /// Retention after a `failed` or `aborted` finish, in milliseconds, with
    /// the same encoding as `result_ttl_ms`.
    pub failed_ttl_ms: Option<i64>,
    /// Earliest execution time.
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    pub scheduled_at: Timestamp,
    /// When the job was enqueued.
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    pub enqueued_at: Timestamp,
    /// When the current/last attempt started.
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    pub started_at: Option<Timestamp>,
    /// Last lifecycle update for the current attempt.
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    pub touched_at: Option<Timestamp>,
    /// When the job reached a terminal status.
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    pub completed_at: Option<Timestamp>,
    /// When the sweeper may purge this terminal row.
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    pub expires_at: Option<Timestamp>,
    /// Serialized handler return value (terminal, successful jobs).
    pub result: Option<Value>,
    /// Last error recorded for this job.
    pub error: Option<String>,
    /// Arbitrary user metadata.
    pub meta: Value,
    /// Worker currently/last processing this job.
    pub worker_id: Option<Uuid>,
    /// `job` for a plain job, `cron` for a cron occurrence.
    pub kind: String,
    /// The UTC cron expression this occurrence was scheduled from; `None` for
    /// a plain job.
    pub cron_expr: Option<String>,
    /// When [`Queue::retry_job`] re-enqueued this
    /// terminal row as a fresh occurrence. A row carries at most one retry, so
    /// this is also what refuses a second.
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    pub retried_at: Option<Timestamp>,
}

impl JobRow {
    /// Whether the job has attempts remaining (`max_attempts > attempts`).
    pub fn is_retryable(&self) -> bool {
        has_attempts_remaining(self.attempts, self.max_attempts)
    }

    /// Per-attempt timeout as a [`Duration`]; `None` = unlimited.
    ///
    /// Zero and negative are not encodings the column accepts, and
    /// `#[pgqueue::job(timeout_ms = 0)]` already means "no timeout", so a row
    /// written by hand before that check reads the same way here. Saturating to
    /// `Duration::ZERO` instead picked the one reading that cancels every
    /// attempt before its handler runs a statement.
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout_ms.filter(|ms| *ms > 0).map(|ms| Duration::from_millis(ms as u64))
    }

    /// Retention policy after a `complete` finish.
    pub fn retention(&self) -> JobRetention {
        JobRetention::from_result_ttl_ms(self.result_ttl_ms)
    }

    /// Retention policy after a `failed` or `aborted` finish.
    pub fn failed_retention(&self) -> JobRetention {
        JobRetention::from_result_ttl_ms(self.failed_ttl_ms)
    }

    /// Delay before the next retry attempt, applying this job's backoff.
    pub(crate) fn next_retry_delay(&self) -> Duration {
        retry_delay_for(self.retry_delay_ms, &self.backoff, self.attempts)
    }
}

#[cfg(test)]
mod job_status_tests {
    use super::*;

    #[test]
    fn test_status_round_trips_and_classifies() {
        for status in [
            JobStatus::Queued,
            JobStatus::Running,
            JobStatus::Aborting,
            JobStatus::Complete,
            JobStatus::Failed,
            JobStatus::Aborted,
        ] {
            assert_eq!(status.as_str().parse::<JobStatus>().unwrap(), status);
            assert_eq!(status.to_string(), status.as_str());
        }
        assert!("bogus".parse::<JobStatus>().is_err());
        assert!(JobStatus::Complete.is_terminal());
        assert!(JobStatus::Failed.is_terminal());
        assert!(JobStatus::Aborted.is_terminal());
        assert!(!JobStatus::Queued.is_terminal());
        assert!(!JobStatus::Running.is_terminal());
        assert!(!JobStatus::Aborting.is_terminal());
    }
}

#[derive(Default)]
pub(crate) struct JobStateMap {
    values: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl JobStateMap {
    pub(crate) fn insert<T: Clone + Send + Sync + 'static>(&mut self, value: T) {
        self.values.insert(TypeId::of::<T>(), Box::new(value));
    }

    fn get<T: Clone + Send + Sync + 'static>(&self) -> Option<&T> {
        self.values.get(&TypeId::of::<T>())?.downcast_ref()
    }
}

impl std::fmt::Debug for JobStateMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobStateMap").field("len", &self.values.len()).finish()
    }
}

/// Extractor for shared worker state registered via [`crate::WorkerBuilder::state`].
///
/// `JobState<Mailer>` resolves to a clone of the `Mailer` the worker was built
/// with. A missing value fails the job attempt with an extraction error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobState<T>(pub T);

/// Everything a running job can see: its row snapshot, the queue, shared
/// worker state, and cooperative cancellation.
///
/// Cheap to clone. Extract it by adding a `ctx: JobContext` parameter to a
/// `#[pgqueue::job]` function.
#[derive(Clone)]
pub struct JobContext {
    inner: Arc<JobContextInner>,
}

struct JobContextInner {
    queue: Queue,
    job: JobRow,
    worker_id: Uuid,
    state: Arc<JobStateMap>,
    cancel: CancellationToken,
}

impl JobContext {
    pub(crate) fn new(
        queue: Queue,
        job: JobRow,
        worker_id: Uuid,
        state: Arc<JobStateMap>,
        cancel: CancellationToken,
    ) -> Self {
        Self { inner: Arc::new(JobContextInner { queue, job, worker_id, state, cancel }) }
    }

    /// Snapshot of this job's row as it was dequeued.
    pub fn job(&self) -> &JobRow {
        &self.inner.job
    }

    /// The current attempt number. 1 on a job's first run; an occurrence created
    /// by [`Queue::retry_job`](crate::Queue::retry_job) inherits the source row's
    /// counter, so its first run reports the source's spent attempts plus one.
    pub fn attempt(&self) -> u32 {
        self.inner.job.attempts.max(0) as u32
    }

    /// The id of the worker processing this job.
    pub fn worker_id(&self) -> Uuid {
        self.inner.worker_id
    }

    /// The queue this job came from (enqueue follow-up jobs through it).
    pub fn queue(&self) -> &Queue {
        &self.inner.queue
    }

    /// A token cancelled when the worker begins shutdown or observes a user
    /// abort or missing job row. Long-running handlers should `select!` on it
    /// at natural pause points and return after bounded cleanup.
    ///
    /// Shutdown allows up to the worker's configured `shutdown_grace`; a user
    /// abort allows up to
    /// [`WorkerBuilder::abort_grace`](crate::WorkerBuilder::abort_grace).
    /// A handler that returns early for cooperative cancellation should return
    /// a [`JobError`] with [`JobErrorKind::Aborted`]. During worker shutdown,
    /// that explicit classification puts the unfinished attempt back on its
    /// queue and refunds the attempt. Other errors are recorded normally even
    /// if shutdown happens at the same time.
    /// The task is forcibly stopped when that bound expires. Attempt timeouts,
    /// sweeper recovery, and a job row deleted under a running attempt stop the
    /// task immediately, so this token is a cooperative cleanup opportunity
    /// rather than an unconditional guarantee. A force-stop is task
    /// cancellation, which takes effect at the handler's next `.await`: a
    /// handler that blocks its runtime thread runs on — only its finalizations
    /// of the attempt fenced off, not its side effects — while the attempt is
    /// finalized without it. Run blocking work on
    /// `tokio::task::spawn_blocking`.
    /// Cancelling the returned child token does not cancel the job attempt.
    pub fn cancellation(&self) -> CancellationToken {
        self.inner.cancel.child_token()
    }
}

impl std::fmt::Debug for JobContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobContext")
            .field("job", &self.inner.job.id)
            .field("name", &self.inner.job.name)
            .field("worker_id", &self.inner.worker_id)
            .finish_non_exhaustive()
    }
}

/// Types that can be extracted from a [`JobContext`] — the trait behind every
/// `#[pgqueue::job]` function parameter after the payload.
pub trait FromJobContext: Sized + Send {
    /// Extracts `Self`, or fails the attempt with a
    /// [`JobErrorKind::Extract`] error.
    fn from_context(ctx: &JobContext) -> Result<Self, JobError>;
}

impl FromJobContext for JobContext {
    fn from_context(ctx: &JobContext) -> Result<Self, JobError> {
        Ok(ctx.clone())
    }
}

impl<T: Clone + Send + Sync + 'static> FromJobContext for JobState<T> {
    fn from_context(ctx: &JobContext) -> Result<Self, JobError> {
        ctx.inner.state.get::<T>().cloned().map(JobState).ok_or_else(|| {
            JobError::new(
                JobErrorKind::Extract,
                format!(
                    "no state of type `{}` registered on this worker (WorkerBuilder::state)",
                    std::any::type_name::<T>()
                ),
            )
        })
    }
}

#[cfg(test)]
mod context_tests {
    use super::*;

    #[test]
    fn test_state_map_indexes_values_by_type() {
        let mut state = JobStateMap::default();
        assert!(state.get::<String>().is_none());
        state.insert("hello".to_string());
        state.insert(42u32);
        assert_eq!(state.get::<String>().map(String::as_str), Some("hello"));
        assert_eq!(state.get::<u32>(), Some(&42));
        state.insert("world".to_string());
        assert_eq!(state.get::<String>().map(String::as_str), Some("world"));
        assert!(format!("{state:?}").contains("len"));
    }
}

/// A job type generated by the `#[pgqueue::job]` or `#[pgqueue::cron]`
/// attribute macro.
///
/// You never implement this by hand: annotate an `async fn` and the macro
/// produces a unit struct implementing it, plus a typed enqueue constructor and
/// a `::call(...)` test helper.
pub trait JobType: Copy + Send + Sync + 'static {
    /// The payload: the first parameter of the annotated function.
    type Args: Serialize + DeserializeOwned + Send + 'static;
    /// The success value: the `Ok` side of the function's return type.
    type Output: Serialize + DeserializeOwned + Send + 'static;

    /// The registry/database name of this job.
    const NAME: &'static str;

    /// The configuration from the attribute arguments (`max_attempts`,
    /// `timeout_ms`, and related options).
    fn config() -> JobConfig;

    /// The type-erased handler stored in the worker registry.
    fn erased() -> TypeErasedJobHandler;
}

/// Marker implemented by job types generated with [`macro@crate::job`].
///
/// It distinguishes ordinary job definitions from compile-time cron
/// definitions when configuring a worker.
pub trait JobDefinition: JobType {}

/// A compile-time cron definition generated with [`macro@crate::cron`].
pub trait CronDefinition: JobType {
    /// The UTC cron expression checked by the macro.
    const SCHEDULE: &'static str;

    /// Monotonic revision for the durable cron definition.
    const CRON_REVISION: u64;
}

/// How a durable cron schedule handles an occurrence missed while no current
/// scheduler was able to publish it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CronMisfirePolicy {
    /// Skip stale occurrences. `None` preserves the adaptive default of one
    /// fifth of the schedule period, with a floor of one second. The default
    /// scales with the period on purpose: a fixed ceiling would make a sparse
    /// schedule — daily, weekly — drop an occurrence over a worker gap shorter
    /// than one rolling restart, while a minutely schedule kept a grace
    /// proportionate to its own period. An explicit grace is always capped by
    /// the next occurrence.
    Skip {
        /// Maximum non-zero age at which a missed occurrence may still be
        /// published.
        grace: Option<Duration>,
    },
    /// Publish only the most recent missed occurrence, provided its successor
    /// is still in the future.
    FireOnce,
}

impl Default for CronMisfirePolicy {
    fn default() -> Self {
        Self::Skip { grace: None }
    }
}

impl CronMisfirePolicy {
    pub(crate) fn validate(self) -> Result<(), Error> {
        if let Self::Skip { grace: Some(grace) } = self {
            validate_nonzero_duration("cron misfire grace", grace)?;
        }
        Ok(())
    }

    pub(crate) fn kind(self) -> &'static str {
        match self {
            Self::Skip { .. } => "skip",
            Self::FireOnce => "fire_once",
        }
    }

    pub(crate) fn grace_ms(self) -> Option<i64> {
        match self {
            Self::Skip { grace } => grace.map(duration_to_ms),
            Self::FireOnce => None,
        }
    }
}

/// Durable cron registration options.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CronOptions {
    /// Monotonically increasing definition revision. Higher revisions replace
    /// lower ones; changing a schedule without increasing it is rejected. A
    /// template-only revision preserves the durable cursor, while changing the
    /// expression starts at its next UTC occurrence.
    ///
    /// **The revision is monotonic in the database, so a rollback does not undo
    /// it.** Deploying revision N+1 and then rolling back to N leaves every
    /// remaining worker superseded by a revision none of them holds: they log
    /// the supersession, stop scheduling that cron, and — because a superseded
    /// cron is filed once and never re-evaluated — never schedule it again, on
    /// any worker, however long the rollback lasts. Ordinary jobs keep flowing,
    /// but [`Worker::health`](crate::Worker::health) reports the scheduler as
    /// degraded. That signal is expected briefly during a rolling deploy and
    /// remains until every old worker has been replaced.
    ///
    /// Recovering means rolling *forward*: deploy the old definition under a
    /// revision above the one that superseded it. Lowering the stored revision
    /// instead needs SQL against `pgqueue.cron_schedules` **and** a restart of
    /// every worker that has already filed the cron as superseded.
    pub revision: u64,
    /// Missed-occurrence behavior.
    pub misfire: CronMisfirePolicy,
}

/// Boxed future returned by an erased handler.
pub type JobHandlerFuture = Pin<Box<dyn Future<Output = Result<Value, JobError>> + Send>>;

type JobHandlerFn = dyn Fn(JobContext) -> JobHandlerFuture + Send + Sync;

/// Normalizes `#[pgqueue::job]` return types into a serializable result.
///
/// Implemented for `Result<T: Serialize, E: Display + 'static>` (the
/// idiomatic form, including `anyhow::Result<T>`) and for `()` (infallible
/// jobs). A returned [`JobError`] keeps its original [`JobErrorKind`].
pub trait IntoJobResult {
    /// The success value stored in the job's `result` column.
    type Output: Serialize + DeserializeOwned + Send + 'static;

    /// Converts the handler return value into the attempt result.
    fn into_job_result(self) -> Result<Self::Output, JobError>;
}

impl<T, E> IntoJobResult for Result<T, E>
where
    T: Serialize + DeserializeOwned + Send + 'static,
    E: std::fmt::Display + 'static,
{
    type Output = T;

    fn into_job_result(self) -> Result<T, JobError> {
        self.map_err(|error| {
            let error_any = &error as &dyn Any;
            // Rebuilt rather than cloned: `JobError`'s fields are public, so a
            // handler's own error may never have passed through a constructor —
            // a struct literal or a deserialized one carries whatever message it
            // was given, NUL included. Only the kind is preserved verbatim.
            if let Some(job_error) = error_any.downcast_ref::<JobError>() {
                return JobError::new(job_error.kind, &job_error.message);
            }
            if let Some(error) = error_any.downcast_ref::<anyhow::Error>() {
                if let Some(job_error) = error.chain().find_map(|cause| cause.downcast_ref::<JobError>()) {
                    return JobError::new(job_error.kind, &job_error.message);
                }
                return JobError::failed(format!("{error:#}"));
            }
            // The boxed error types are searched the same way: a handler that returns `Box<dyn Error + Send + Sync>`
            // and `?`s a `JobError` into it otherwise lost the kind to `Failed` — so a cooperative `Aborted` during
            // shutdown spent its attempt instead of being refunded, and a deterministic `Decode` was retried.
            let boxed = error_any
                .downcast_ref::<Box<dyn std::error::Error + Send + Sync>>()
                .map(|error| &**error as &(dyn std::error::Error + 'static))
                .or_else(|| {
                    error_any
                        .downcast_ref::<Box<dyn std::error::Error + Send>>()
                        .map(|error| &**error as &(dyn std::error::Error + 'static))
                })
                .or_else(|| error_any.downcast_ref::<Box<dyn std::error::Error>>().map(|error| &**error));
            let boxed_job_error = boxed.and_then(|error| {
                std::iter::successors(Some(error), |cause| cause.source())
                    .find_map(|cause| cause.downcast_ref::<JobError>())
            });
            if let Some(job_error) = boxed_job_error {
                return JobError::new(job_error.kind, &job_error.message);
            }
            JobError::failed(error)
        })
    }
}

impl IntoJobResult for () {
    type Output = ();

    fn into_job_result(self) -> Result<(), JobError> {
        Ok(())
    }
}

/// A type-erased job handler: decodes the JSON payload, extracts context
/// parameters, runs the user function, and encodes the result.
#[derive(Clone)]
pub struct TypeErasedJobHandler {
    type_id: TypeId,
    name: &'static str,
    config: JobConfig,
    call: Arc<JobHandlerFn>,
}

impl TypeErasedJobHandler {
    /// Wraps the macro-generated closure for job type `J`. The closure reads
    /// its payload from the context's row snapshot rather than taking a copy:
    /// the worker builds one [`JobContext`] per attempt by *moving* the
    /// dequeued row in, so a payload is decoded straight from that snapshot
    /// and never cloned on the execution path — a cost that used to be paid
    /// per attempt whether or not anything read it.
    pub fn new<J: JobType>(call: impl Fn(JobContext) -> JobHandlerFuture + Send + Sync + 'static) -> Self {
        Self { type_id: TypeId::of::<J>(), name: J::NAME, config: J::config(), call: Arc::new(call) }
    }

    /// The registry name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The job's compile-time configuration.
    pub fn config(&self) -> &JobConfig {
        &self.config
    }

    /// The generated Rust type that owns this handler.
    pub(crate) fn type_id(&self) -> TypeId {
        self.type_id
    }

    /// Invokes the handler.
    pub(crate) fn call(&self, ctx: JobContext) -> JobHandlerFuture {
        (self.call)(ctx)
    }
}

impl std::fmt::Debug for TypeErasedJobHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeErasedJobHandler").field("name", &self.name).finish_non_exhaustive()
    }
}

#[cfg(test)]
mod handler_tests {
    use super::*;

    #[derive(Clone, Copy)]
    struct Noop;

    impl JobType for Noop {
        type Args = ();
        type Output = ();
        const NAME: &'static str = "noop";

        fn config() -> JobConfig {
            JobConfig::default()
        }

        fn erased() -> TypeErasedJobHandler {
            TypeErasedJobHandler::new::<Self>(|_ctx| Box::pin(async { Ok(Value::Null) }))
        }
    }

    #[test]
    fn test_erased_handler_exposes_name_and_config() {
        let handler = Noop::erased();
        assert_eq!(handler.name(), "noop");
        assert_eq!(*handler.config(), JobConfig::default());
        assert!(format!("{handler:?}").contains("noop"));
    }

    #[test]
    fn test_job_results_normalize_successes_and_failures() {
        let ok: Result<u32, std::io::Error> = Ok(7);
        assert_eq!(ok.into_job_result().unwrap(), 7);

        let err: Result<u32, String> = Err("boom".to_string());
        let job_err = err.into_job_result().unwrap_err();
        assert_eq!(job_err.kind, JobErrorKind::Failed);
        assert_eq!(job_err.message, "boom");

        let err: Result<u32, JobError> = Err(JobError::new(JobErrorKind::Decode, "invalid payload"));
        let job_err = err.into_job_result().unwrap_err();
        assert_eq!(job_err.kind, JobErrorKind::Decode);
        assert_eq!(job_err.message, "invalid payload");
        assert!(().into_job_result().is_ok());
    }

    #[test]
    fn test_job_result_preserves_job_error_wrapped_by_anyhow() {
        let wrapped = anyhow::Error::new(JobError::new(JobErrorKind::Timeout, "too slow")).context("handler context");
        let result: Result<(), anyhow::Error> = Err(wrapped);

        let error = result.into_job_result().unwrap_err();

        assert_eq!(error.kind, JobErrorKind::Timeout);
        assert_eq!(error.message, "too slow");
    }

    /// The boxed error types are the other common handler error, and a kind lost to `Failed` there changes what happens
    /// next: an `Aborted` returned for cooperative shutdown is refunded only while it is still `Aborted`.
    #[test]
    fn test_job_result_preserves_a_job_error_in_a_boxed_error() {
        #[derive(Debug)]
        struct Wrapper(JobError);

        impl std::fmt::Display for Wrapper {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("wrapped")
            }
        }

        impl std::error::Error for Wrapper {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }

        let stop = || JobError::new(JobErrorKind::Aborted, "cooperative stop");
        let sync: Result<(), Box<dyn std::error::Error + Send + Sync>> = Err(stop().into());
        let send: Result<(), Box<dyn std::error::Error + Send>> = Err(Box::new(stop()));
        let local: Result<(), Box<dyn std::error::Error>> = Err(Box::new(stop()));
        let nested: Result<(), Box<dyn std::error::Error + Send + Sync>> = Err(Box::new(Wrapper(stop())));
        for error in [
            sync.into_job_result().unwrap_err(),
            send.into_job_result().unwrap_err(),
            local.into_job_result().unwrap_err(),
            nested.into_job_result().unwrap_err(),
        ] {
            assert_eq!(error, stop());
        }

        let plain: Result<(), Box<dyn std::error::Error + Send + Sync>> = Err("no job error here".into());
        assert_eq!(plain.into_job_result().unwrap_err(), JobError::failed("no job error here"));
    }

    #[test]
    fn test_job_result_preserves_anyhow_cause_chain() {
        let wrapped = anyhow::Error::new(std::io::Error::other("connection closed"))
            .context("publish job")
            .context("handler failed");
        let result: Result<(), anyhow::Error> = Err(wrapped);

        let error = result.into_job_result().unwrap_err();

        assert_eq!(error.kind, JobErrorKind::Failed);
        assert_eq!(error.message, "handler failed: publish job: connection closed");
    }
}

/// A registered cron job: a parsed schedule plus the job template to enqueue.
pub(crate) struct JobCronEntry {
    pub cron: CronSchedule,
    /// The source expression stored with scheduled occurrences.
    pub expr: String,
    /// The dedupe key every occurrence fires under (also set on the template).
    pub dedupe_key: String,
    pub template: JobRequest,
    pub options: CronOptions,
    pub definition: Value,
}

/// One complete proleptic-Gregorian calendar cycle. A five-field cron schedule
/// without a year repeats within this window if it can occur at all.
const CRON_CYCLE_SECONDS: i64 = 146_097 * 86_400;

/// Safely inside `cronexpr`'s four-year lookup window. Advancing by three years
/// after an exhausted lookup overlaps the range it just checked, so no instant
/// can fall between probes even when leap days are involved.
const CRON_SEARCH_STEP_SECONDS: i64 = 3 * 365 * 86_400;

/// The headroom [`CronSchedule::next_after`] keeps below [`Timestamp::MAX`].
///
/// `Crontab::find_next` bounds its own search by computing `&zoned + 4.years()`
/// (`cronexpr-1.6.0/src/lib.rs:853`), and `Add<Span> for &Zoned` **panics** on
/// overflow rather than returning an error — so for an instant within four years
/// of Jiff's maximum the call does not merely fail, it takes the calling task
/// down. The `Err(_)` arm below is written for an *exhausted* lookup and never
/// sees it.
///
/// Every clock this crate reads comes from the database, and it is the one
/// untrusted timestamp no CHECK constraint covers: the schema bounds every
/// stored instant, but not `now()`. A panic here ends the worker — through
/// `reconcile_all_crons` at startup or `schedule_loop` later — which is exactly
/// what "a cron problem never stops the worker" exists to prevent.
///
/// Refusing here answers `None`, which [`JobCronEntry::next_occurrence`] reports
/// as the documented "schedule has no next occurrence" configuration error: the
/// cron is disabled and [`WorkerComponent::Scheduler`](crate::WorkerComponent)
/// degrades, exactly as an impossible expression does. The answer is also
/// correct on its own terms, because no occurrence past Jiff's maximum is
/// representable. Four leap-safe years is the smallest window that cannot
/// overflow.
const CRON_SEARCH_HEADROOM_SECONDS: i64 = 4 * 366 * 86_400;

/// A parsed UTC, minute-resolution cron schedule.
pub(crate) struct CronSchedule {
    cron: Crontab,
    source: String,
}

impl CronSchedule {
    pub(crate) fn source(&self) -> &str {
        &self.source
    }

    /// The next matching minute strictly after `after`, searching at most one
    /// Gregorian cycle so impossible schedules terminate deterministically.
    fn next_after(&self, after: Timestamp) -> Option<Timestamp> {
        let limit_seconds = after.as_second().saturating_add(CRON_CYCLE_SECONDS).min(Timestamp::MAX.as_second());
        let mut cursor = after;
        let searchable = Timestamp::MAX.as_second().saturating_sub(CRON_SEARCH_HEADROOM_SECONDS);
        loop {
            // Before the call, not after: `find_next` panics rather than
            // failing this close to Jiff's maximum.
            if cursor.as_second() > searchable {
                return None;
            }
            match self.cron.find_next(MakeTimestamp::from(cursor)) {
                Ok(next) if next.timestamp().as_second() <= limit_seconds => {
                    return Some(next.timestamp());
                }
                Ok(_) => return None,
                Err(_) => {
                    let next_seconds = cursor.as_second().saturating_add(CRON_SEARCH_STEP_SECONDS).min(limit_seconds);
                    if next_seconds <= cursor.as_second() {
                        return None;
                    }
                    cursor = Timestamp::from_second(next_seconds).ok()?;
                }
            }
        }
    }

    /// The latest matching minute at or before `now`. `cronexpr` only exposes
    /// forward lookup, so use its monotonic next-occurrence function to find
    /// the boundary logarithmically instead of scanning backward by minute.
    fn previous_at_or_before(&self, now: Timestamp) -> Option<Timestamp> {
        let minute_seconds = now.as_second().div_euclid(60) * 60;
        let minute = Timestamp::from_second(minute_seconds).ok()?;
        if self.cron.matches(MakeTimestamp::from(minute)).ok()? {
            return Some(minute);
        }

        let floor_seconds = now.as_second().saturating_sub(CRON_CYCLE_SECONDS).max(Timestamp::MIN.as_second());
        let floor = Timestamp::from_second(floor_seconds).ok()?;
        let mut latest = self.next_after(floor)?;
        if latest > now {
            return None;
        }

        let mut low = floor_seconds;
        let mut high = now.as_second();
        while high - low > 1 {
            let midpoint = low + (high - low) / 2;
            let probe = Timestamp::from_second(midpoint).ok()?;
            match self.next_after(probe) {
                Some(next) if next <= now => {
                    latest = next;
                    low = midpoint;
                }
                _ => high = midpoint,
            }
        }
        Some(latest)
    }
}

impl std::fmt::Debug for CronSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.source)
    }
}

/// Parses a standard five-field cron expression evaluated in UTC.
pub(crate) fn parse_cron(expr: &str) -> Result<CronSchedule, Error> {
    if expr.split_ascii_whitespace().count() != 5 {
        return Err(Error::Config(format!("invalid cron expression {expr:?}: expected 5 whitespace-separated fields")));
    }
    let mut options = ParseOptions::default();
    options.fallback_timezone_option = FallbackTimezoneOption::UTC;
    let cron = cronexpr::parse_crontab_with(expr, options)
        .map_err(|e| Error::Config(format!("invalid cron expression {expr:?}: {e}")))?;
    Ok(CronSchedule { cron, source: cronexpr::normalize_crontab(expr) })
}

impl JobCronEntry {
    /// Builds an entry, defaulting the dedupe key to `cron:{name}`.
    #[cfg(test)]
    pub(crate) fn new(expr: &str, template: JobRequest) -> Result<Self, Error> {
        Self::with_options(expr, template, CronOptions::default())
    }

    pub(crate) fn with_options(expr: &str, mut template: JobRequest, options: CronOptions) -> Result<Self, Error> {
        let cron = parse_cron(expr)?;
        options.misfire.validate()?;
        i64::try_from(options.revision)
            .map_err(|_| Error::Config("cron revision must fit PostgreSQL bigint".into()))?;
        let dedupe_key = template.dedupe_key.clone().unwrap_or_else(|| format!("cron:{}", template.name));
        template.dedupe_key = Some(dedupe_key.clone());
        template.validate()?;
        let definition = serde_json::json!({
            "payload": template.payload.clone(),
            "max_attempts": template.config.max_attempts,
            "timeout_ms": template.config.timeout.map(duration_to_ms),
            "result_ttl_ms": template.config.retention.as_result_ttl_ms(),
            "failed_ttl_ms": template.config.failed_retention.as_result_ttl_ms(),
            "retry_delay_ms": duration_to_ms(template.config.retry_delay),
            "backoff": template.config.backoff,
            "priority": template.config.priority,
            "meta": template.meta.clone(),
        });
        Ok(Self { cron, expr: expr.to_string(), dedupe_key, template, options, definition })
    }

    /// The next fire time strictly after `now`.
    pub(crate) fn next_occurrence(&self, now: Timestamp) -> Result<Timestamp, Error> {
        let now = round_cron_timestamp(now)?;
        self.cron
            .next_after(now)
            .ok_or_else(|| Error::Config("cron occurrence: schedule has no next occurrence".into()))
    }

    pub(crate) fn previous_occurrence(&self, now: Timestamp) -> Result<Timestamp, Error> {
        let now = round_cron_timestamp(now)?;
        self.cron
            .previous_at_or_before(now)
            .ok_or_else(|| Error::Config("cron occurrence: schedule has no previous occurrence".into()))
    }

    pub(crate) fn publication_deadline(&self, occurrence: Timestamp, successor: Timestamp) -> Timestamp {
        let grace = match self.options.misfire {
            CronMisfirePolicy::Skip { grace: Some(grace) } => SignedDuration::from_millis(duration_to_ms(grace)),
            // One fifth of the period, floored at a second and left unbounded
            // above. An upper clamp here is what made a daily schedule skip its
            // run after a worker gap of barely a minute: the deadline is capped
            // by `successor` below regardless, so the period itself is already
            // the real ceiling.
            CronMisfirePolicy::Skip { grace: None } => {
                (successor.duration_since(occurrence) / 5).max(SignedDuration::from_secs(1))
            }
            // "Publish only the most recent missed occurrence, provided its
            // successor is still in the future": the whole period is the grace.
            CronMisfirePolicy::FireOnce => successor.duration_since(occurrence),
        };
        successor.min(occurrence.checked_add(grace).unwrap_or(successor))
    }

    /// The job to enqueue for the occurrence at `at`.
    pub(crate) fn job_for(&self, at: Timestamp) -> JobRequest {
        let mut job = self.template.clone();
        job.scheduled_at = Some(at);
        job
    }
}

fn round_cron_timestamp(timestamp: Timestamp) -> Result<Timestamp, Error> {
    timestamp
        .round(TimestampRound::new().smallest(Unit::Second).mode(RoundMode::Floor))
        .map_err(|error| Error::Config(format!("cron occurrence: {error}")))
}

impl std::fmt::Debug for JobCronEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobCronEntry").field("cron", &self.cron.source()).field("job", &self.template.name).finish()
    }
}

#[cfg(test)]
mod cron_entry_tests {
    use super::*;

    #[test]
    fn test_cron_misfire_policy_rejects_zero_grace() {
        let error = CronMisfirePolicy::Skip { grace: Some(Duration::ZERO) }.validate().unwrap_err();

        assert!(error.to_string().contains("greater than zero"), "{error}");
    }

    #[test]
    fn test_next_occurrence_is_identical_when_now_has_subseconds() {
        let entry = JobCronEntry::new("0 0 * * *", JobRequest::new("tick", Value::Null)).unwrap();
        let base: Timestamp = "2026-07-18T23:38:17Z".parse().unwrap();
        let early = entry.next_occurrence(base).unwrap();
        let late = entry.next_occurrence(base + SignedDuration::from_micros(545_375)).unwrap();
        assert_eq!(early, late);
        assert_eq!(early, "2026-07-19T00:00:00Z".parse::<Timestamp>().unwrap());
    }

    #[test]
    fn test_publication_deadline_is_identical_when_graces_share_canonical_milliseconds() {
        let entry_with_grace = |grace| {
            JobCronEntry::with_options(
                "0 * * * *",
                JobRequest::new("tick", Value::Null),
                CronOptions { misfire: CronMisfirePolicy::Skip { grace: Some(grace) }, ..CronOptions::default() },
            )
            .unwrap()
        };
        let submillisecond = entry_with_grace(Duration::from_micros(1_500));
        let milliseconds = entry_with_grace(Duration::from_millis(2));
        let occurrence: Timestamp = "2026-01-01T00:00:00Z".parse().unwrap();
        let successor: Timestamp = "2026-01-01T01:00:00Z".parse().unwrap();

        assert_eq!(submillisecond.options.misfire.grace_ms(), milliseconds.options.misfire.grace_ms());
        assert_eq!(
            submillisecond.publication_deadline(occurrence, successor),
            milliseconds.publication_deadline(occurrence, successor)
        );
        assert_eq!(
            submillisecond.publication_deadline(occurrence, successor),
            occurrence + SignedDuration::from_millis(2)
        );
    }
}

#[cfg(test)]
mod cron_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_cron_accepts_only_five_fields() {
        assert!(parse_cron("*/5 * * * *").is_ok());
        assert!(parse_cron("30 */5 * * * *").is_err());
        assert!(parse_cron("not a cron").is_err());
        assert!(parse_cron("99 * * * *").is_err());
    }

    #[test]
    fn test_cron_uses_standard_sunday_and_vixie_day_semantics() {
        let saturday = "2026-01-03T12:00:00Z".parse::<Timestamp>().unwrap();
        let sunday = "2026-01-04T00:00:00Z".parse::<Timestamp>().unwrap();
        for expression in ["0 0 * * 0", "0 0 * * 7"] {
            let entry = JobCronEntry::new(expression, JobRequest::new("tick", json!(null))).unwrap();
            assert_eq!(entry.next_occurrence(saturday).unwrap(), sunday);
        }

        // Neither day field starts with `*`, so Vixie cron matches the 13th OR
        // Monday. April 20 is a Monday but not the 13th.
        let entry = JobCronEntry::new("0 0 13 * 1", JobRequest::new("tick", json!(null))).unwrap();
        let after_april_13 = "2026-04-13T00:00:00Z".parse::<Timestamp>().unwrap();
        assert_eq!(
            entry.next_occurrence(after_april_13).unwrap(),
            "2026-04-20T00:00:00Z".parse::<Timestamp>().unwrap()
        );
    }

    #[test]
    fn test_cron_finds_previous_sparse_occurrence() {
        let entry = JobCronEntry::new("0 0 29 2 *", JobRequest::new("tick", json!(null))).unwrap();
        assert_eq!(
            entry.previous_occurrence("2026-01-01T00:00:00Z".parse::<Timestamp>().unwrap()).unwrap(),
            "2024-02-29T00:00:00Z".parse::<Timestamp>().unwrap()
        );
    }

    /// `Crontab::find_next` bounds its own search with `&zoned + 4.years()`, and
    /// that addition *panics* on overflow. The clock is the one timestamp no
    /// CHECK constraint bounds, so a database whose clock reads a year in the
    /// 9990s reached that panic through the scheduler and ended the worker —
    /// the failure "a cron problem never stops the worker" exists to prevent.
    ///
    /// Refusing the lookup instead answers the documented "no next occurrence",
    /// which disables the cron and degrades the scheduler. Well below the
    /// headroom the same schedule must still resolve normally, or the guard has
    /// silently disabled every cron rather than the unrepresentable ones.
    #[test]
    fn test_cron_refuses_a_lookup_too_close_to_the_timestamp_maximum() {
        let entry = JobCronEntry::new("* * * * *", JobRequest::new("tick", json!(null))).unwrap();

        let inside_headroom = Timestamp::from_second(Timestamp::MAX.as_second() - 86_400).unwrap();
        let error = entry.next_occurrence(inside_headroom).unwrap_err();
        assert!(matches!(error, Error::Config(ref message) if message.contains("no next occurrence")), "{error}");

        // Five years below the maximum is outside the four-year window
        // `find_next` reserves, so the ordinary answer is still produced.
        let outside_headroom = Timestamp::from_second(Timestamp::MAX.as_second() - 5 * 366 * 86_400).unwrap();
        assert!(entry.next_occurrence(outside_headroom).is_ok());
    }

    #[test]
    fn test_cron_preserves_pre_epoch_fractional_boundaries() {
        let entry = JobCronEntry::new("* * * * *", JobRequest::new("tick", json!(null))).unwrap();
        let now = "1969-12-31T23:59:59.5Z".parse::<Timestamp>().unwrap();

        assert_eq!(entry.next_occurrence(now).unwrap(), "1970-01-01T00:00:00Z".parse::<Timestamp>().unwrap());
        assert_eq!(entry.previous_occurrence(now).unwrap(), "1969-12-31T23:59:00Z".parse::<Timestamp>().unwrap());
    }

    #[test]
    fn test_entry_defaults_dedupe_key_and_schedules() {
        let entry = JobCronEntry::new("0 * * * *", JobRequest::new("cleanup", json!(null))).unwrap();
        assert_eq!(entry.dedupe_key, "cron:cleanup");
        assert_eq!(entry.template.dedupe_key.as_deref(), Some("cron:cleanup"));
        assert!(format!("{entry:?}").contains("cleanup"));

        let now = "2026-01-01T10:15:00Z".parse::<Timestamp>().unwrap();
        let next = entry.next_occurrence(now).unwrap();
        assert_eq!(next, "2026-01-01T11:00:00Z".parse::<Timestamp>().unwrap());

        let job = entry.job_for(next);
        assert_eq!(job.scheduled_at, Some(next));
        assert_eq!(job.name, "cleanup");
    }

    #[test]
    fn test_impossible_schedule_surfaces_an_error() {
        let entry = JobCronEntry::new("0 0 30 2 *", JobRequest::new("never", json!(null))).unwrap();
        let err = entry.next_occurrence(Timestamp::now()).unwrap_err();
        assert!(err.to_string().contains("cron occurrence"), "{err}");
    }

    #[test]
    fn test_explicit_dedupe_key_is_preserved() {
        let mut template = JobRequest::new("cleanup", json!(null));
        template.dedupe_key = Some("custom".into());
        let entry = JobCronEntry::new("0 * * * *", template).unwrap();
        assert_eq!(entry.dedupe_key, "custom");
        assert_eq!(entry.template.dedupe_key.as_deref(), Some("custom"));
    }

    #[test]
    fn test_derived_dedupe_key_is_validated() {
        let error = JobCronEntry::new("0 * * * *", JobRequest::new("x".repeat(251), json!(null))).unwrap_err();
        assert!(error.to_string().contains("dedupe key"), "{error}");
    }
}

const MAX_INDEXED_KEY_BYTES: usize = 255;

pub(crate) fn validate_dedupe_key(key: &str) -> Result<(), Error> {
    if key.contains('\0') {
        return Err(Error::Config("dedupe key must not contain NUL".into()));
    }
    if key.len() > MAX_INDEXED_KEY_BYTES {
        return Err(Error::Config(format!("dedupe key must not be longer than {MAX_INDEXED_KEY_BYTES} bytes")));
    }
    Ok(())
}

/// Serializes a typed value to a JSON tree, refusing non-finite floats to
/// enforce the crate's first design invariant — job input and output must be
/// JSON serializable — where `serde_json` quietly forgives its one violation:
/// JSON has no NaN or infinity, and every `serde_json` serializer (tree and
/// text alike) maps a non-finite float to `null` instead of erroring. Enqueue
/// then succeeded and the worker terminally failed the job on a deterministic
/// decode error — or worse, an `Option<f64>` of NaN round-tripped as a clean
/// `None`, which is silent corruption. The adapter checks values during the
/// real JSON serialization, so user-provided [`Serialize`] implementations run
/// once and the refusal still happens here, at the boundary, exactly as NUL and
/// nesting depth are refused.
///
/// One member of that family is *not* refused, because it is representable and
/// refusing it would be over-strict: `-0.0` round-trips as `+0.0`. `jsonb`
/// stores numbers as `numeric`, which has no signed zero, so the sign is lost in
/// PostgreSQL rather than here. A handler that distinguishes the two — reading
/// a sign bit to recover a direction, say — must encode that distinction some
/// other way. Everything else about the value survives bit for bit.
pub(crate) fn encode_json<T: Serialize>(value: &T) -> Result<Value, serde_json::Error> {
    serde_json::to_value(CheckedJson(value))
}

/// Wraps each nested value before the real JSON serializer sees it. The root
/// and every child are serialized exactly once; the serializer adapter gets to
/// inspect floats before forwarding them.
struct CheckedJson<'a, T: ?Sized>(&'a T);

impl<T: ?Sized + Serialize> Serialize for CheckedJson<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(RejectNonFinite(serializer))
    }
}

struct RejectNonFinite<S>(S);
struct CheckedCompound<C>(C);

macro_rules! forward_scalar {
    ($(fn $method:ident($kind:ty);)*) => {
        $(
            fn $method(self, value: $kind) -> Result<Self::Ok, Self::Error> {
                self.0.$method(value)
            }
        )*
    };
}

impl<S: Serializer> Serializer for RejectNonFinite<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = CheckedCompound<S::SerializeSeq>;
    type SerializeTuple = CheckedCompound<S::SerializeTuple>;
    type SerializeTupleStruct = CheckedCompound<S::SerializeTupleStruct>;
    type SerializeTupleVariant = CheckedCompound<S::SerializeTupleVariant>;
    type SerializeMap = CheckedCompound<S::SerializeMap>;
    type SerializeStruct = CheckedCompound<S::SerializeStruct>;
    type SerializeStructVariant = CheckedCompound<S::SerializeStructVariant>;

    forward_scalar! {
        fn serialize_bool(bool);
        fn serialize_i8(i8);
        fn serialize_i16(i16);
        fn serialize_i32(i32);
        fn serialize_i64(i64);
        fn serialize_i128(i128);
        fn serialize_u8(u8);
        fn serialize_u16(u16);
        fn serialize_u32(u32);
        fn serialize_u64(u64);
        fn serialize_u128(u128);
        fn serialize_char(char);
    }

    fn serialize_f32(self, value: f32) -> Result<Self::Ok, Self::Error> {
        if !value.is_finite() {
            return Err(serde::ser::Error::custom("non-finite floats (NaN, infinity) have no JSON representation"));
        }
        self.0.serialize_f32(value)
    }

    fn serialize_f64(self, value: f64) -> Result<Self::Ok, Self::Error> {
        if !value.is_finite() {
            return Err(serde::ser::Error::custom("non-finite floats (NaN, infinity) have no JSON representation"));
        }
        self.0.serialize_f64(value)
    }

    fn serialize_str(self, value: &str) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_str(value)
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_bytes(value)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_none()
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_some(&CheckedJson(value))
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit_struct(name)
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit_variant(name, variant_index, variant)
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_newtype_struct(name, &CheckedJson(value))
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_newtype_variant(name, variant_index, variant, &CheckedJson(value))
    }

    fn serialize_seq(self, length: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.0.serialize_seq(length).map(CheckedCompound)
    }

    fn serialize_tuple(self, length: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.0.serialize_tuple(length).map(CheckedCompound)
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        length: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.0.serialize_tuple_struct(name, length).map(CheckedCompound)
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        length: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.0.serialize_tuple_variant(name, variant_index, variant, length).map(CheckedCompound)
    }

    fn serialize_map(self, length: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        self.0.serialize_map(length).map(CheckedCompound)
    }

    fn serialize_struct(self, name: &'static str, length: usize) -> Result<Self::SerializeStruct, Self::Error> {
        self.0.serialize_struct(name, length).map(CheckedCompound)
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        length: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.0.serialize_struct_variant(name, variant_index, variant, length).map(CheckedCompound)
    }

    fn collect_str<T: ?Sized + std::fmt::Display>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        self.0.collect_str(value)
    }

    fn is_human_readable(&self) -> bool {
        self.0.is_human_readable()
    }
}

impl<C: SerializeSeq> SerializeSeq for CheckedCompound<C> {
    type Ok = C::Ok;
    type Error = C::Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.0.serialize_element(&CheckedJson(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<C: SerializeTuple> SerializeTuple for CheckedCompound<C> {
    type Ok = C::Ok;
    type Error = C::Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.0.serialize_element(&CheckedJson(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<C: SerializeTupleStruct> SerializeTupleStruct for CheckedCompound<C> {
    type Ok = C::Ok;
    type Error = C::Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.0.serialize_field(&CheckedJson(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<C: SerializeTupleVariant> SerializeTupleVariant for CheckedCompound<C> {
    type Ok = C::Ok;
    type Error = C::Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.0.serialize_field(&CheckedJson(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<C: SerializeMap> SerializeMap for CheckedCompound<C> {
    type Ok = C::Ok;
    type Error = C::Error;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), Self::Error> {
        self.0.serialize_key(&CheckedJson(key))
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.0.serialize_value(&CheckedJson(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<C: SerializeStruct> SerializeStruct for CheckedCompound<C> {
    type Ok = C::Ok;
    type Error = C::Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error> {
        self.0.serialize_field(key, &CheckedJson(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<C: SerializeStructVariant> SerializeStructVariant for CheckedCompound<C> {
    type Ok = C::Ok;
    type Error = C::Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error> {
        self.0.serialize_field(key, &CheckedJson(value))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

/// Whether a JSON document carries a NUL in any string or object key.
///
/// PostgreSQL's `jsonb` cannot represent `\0`, so such a document is an error
/// on this side of the wire rather than a database one, whichever end of a job
/// it came from. An enqueued payload carrying one raises `22P05`, which on
/// `Queue::enqueue_in`/`enqueue_raw_in` aborts the *caller's* transaction and
/// destroys their whole unit of work; a handler result carrying one leaves an
/// attempt that can never be finalized (see `classify_attempt_join`).
pub(crate) fn json_contains_nul(value: &Value) -> bool {
    match value {
        Value::String(text) => text.contains('\0'),
        Value::Array(items) => items.iter().any(json_contains_nul),
        Value::Object(fields) => fields.iter().any(|(key, value)| key.contains('\0') || json_contains_nul(value)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

/// The most serialized JSON this crate will store in any single `jsonb` column — a job's `payload`, `meta` and
/// `result`, and a worker lease's `stats` and `metadata`.
///
/// A policy ceiling, not PostgreSQL's: `jsonb` stores individual strings and container bodies up to 2^28 - 1 bytes
/// (just under 256 MiB), and past *those* the write raises `54000`, which is permanent — the same class as the NUL
/// `22P05`, and caught on this side of the wire for the same reason: `finalize` treats a failed write as transient
/// and retries it once a second, so a result that can never be stored keeps its attempt `running` and its processor
/// slot forever. Capping the whole document well under the server's limits makes them unreachable in one check —
/// and a row this queue drags through every dequeue, retry and dashboard page has no business being megabytes wide;
/// store the blob elsewhere and enqueue a reference.
pub(crate) const MAX_JSON_DOCUMENT_BYTES: usize = 1_048_576;

/// The most jobs one [`Queue::enqueue_batch`] accepts.
pub const MAX_ENQUEUE_BATCH_JOBS: usize = 1_000;

/// The most serialized payload and metadata, in bytes, one
/// [`Queue::enqueue_batch`] accepts across all of its jobs. Together with
/// [`MAX_ENQUEUE_BATCH_JOBS`] this keeps the batch's one statement well under
/// the gigabyte PostgreSQL accepts in a single protocol message: every other
/// parameter of that statement is small, and a batch of the largest documents
/// [`JobRequest::validate`] admits would otherwise reach twice the limit.
pub const MAX_ENQUEUE_BATCH_BYTES: usize = 256 * 1_048_576;

/// The serialized size of `value` in bytes, counted rather than allocated.
/// Bounded by its caller the way [`json_exceeds_bytes`] is: the depth check
/// has already run, so serialization cannot recurse past [`MAX_JSON_DEPTH`].
pub(crate) fn json_byte_len(value: &Value) -> usize {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(buf.len());
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    // A `Value` cannot fail to serialize and the counter never refuses a
    // byte, so there is no error to report; the count is the answer.
    let _ = serde_json::to_writer(&mut counter, value);
    counter.0
}

/// The most UTF-8 data stored in a job's `error` column, including its error-kind prefix.
pub(crate) const MAX_STORED_ERROR_BYTES: usize = MAX_JSON_DOCUMENT_BYTES;

const ERROR_TRUNCATION_MARKER: &str = "… [truncated]";

fn truncate_owned_utf8(mut text: String, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes.saturating_sub(ERROR_TRUNCATION_MARKER.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text.push_str(ERROR_TRUNCATION_MARKER);
    text
}

/// Bounds arbitrary text immediately before it reaches the jobs table.
pub(crate) fn truncate_stored_error(text: &str) -> Cow<'_, str> {
    if text.len() <= MAX_STORED_ERROR_BYTES {
        Cow::Borrowed(text)
    } else {
        Cow::Owned(truncate_owned_utf8(text.to_string(), MAX_STORED_ERROR_BYTES))
    }
}

/// Whether `value` serializes to more than `max` bytes of JSON.
///
/// Counts through a writer that refuses bytes past its budget, so the check neither allocates the serialized
/// document nor keeps serializing one that is already over. Bounded by its caller: every call site runs the depth
/// check first, so serialization cannot recurse past [`MAX_JSON_DEPTH`], exactly as `json_contains_nul` relies on.
pub(crate) fn json_exceeds_bytes(value: &Value, max: usize) -> bool {
    struct Budget(usize);
    impl std::io::Write for Budget {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            match self.0.checked_sub(buf.len()) {
                Some(remaining) => {
                    self.0 = remaining;
                    Ok(buf.len())
                }
                None => Err(std::io::Error::other("over budget")),
            }
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    // A `Value` itself cannot fail to serialize — object keys are always
    // strings — so the only error a budgeted writer can surface is exhaustion.
    serde_json::to_writer(Budget(max), value).is_err()
}

/// The deepest container nesting this crate will write to `jsonb`.
///
/// `jsonb` itself tolerates far more, but `serde_json`'s deserializer stops at
/// 128 nested containers, and every read of `payload`, `meta`, `result` and
/// worker `metadata` goes through it. A document nested any deeper is therefore
/// one PostgreSQL stores happily and this crate can never decode again.
pub(crate) const MAX_JSON_DEPTH: usize = 127;

/// Whether `value` nests containers more than `budget` levels deep.
///
/// The damage from writing one is not confined to its own row: the dequeue
/// decodes its batch inside the claiming transaction, so a single undecodable
/// row fails the whole batch's decode, rolls the claim back — and is then
/// re-selected by the next dequeue, which fails identically. One such row
/// blocks every job queued behind it, claim after claim, until repaired by
/// hand. `fetch_job` and `jobs_page` fail for the whole queue for as long as
/// the row is retained. So it is refused here, before anything is written, the
/// way `json_contains_nul` refuses a NUL.
///
/// A foreign SQL writer can still store one — `jsonb` has no cheap depth
/// check, and taxing every legitimate enqueue with a recursive walk to refuse
/// a hand-written row is the wrong trade — so the residual is deliberate, and
/// bounded: the wedge is loud (`Dequeue` health carries the decode error), and
/// the repair needs no SQL, because [`Queue::abort_job`](crate::Queue) and the
/// dashboard's abort never decode the body — aborting the row moves it out of
/// every claim.
///
/// The walk is bounded by the same budget it is checking, so it cannot itself
/// recurse past the limit it exists to enforce.
pub(crate) fn json_exceeds_depth(value: &Value, budget: usize) -> bool {
    match value {
        Value::Array(items) => budget == 0 || items.iter().any(|item| json_exceeds_depth(item, budget - 1)),
        Value::Object(fields) => budget == 0 || fields.values().any(|field| json_exceeds_depth(field, budget - 1)),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
}

/// Refuses a document this crate will not store — nested too deep to read
/// back, carrying a NUL, or serializing past [`MAX_JSON_DOCUMENT_BYTES`] —
/// naming `field` in the message. `Err` carries the message alone, so a caller
/// can wrap it in whichever error its layer reports —
/// [`Error::Config`] before a write, a
/// [`JobError`] for a handler's result.
///
/// The three checks in one place because their *order* is load-bearing and was
/// maintained by hand at five call sites. `json_exceeds_depth` is the only
/// bounded one — it cannot recurse past the budget it checks — so it has to run
/// first, and the other two then cannot see a document deep enough to overflow
/// the thread's stack. Running them in the other order aborts the process rather
/// than returning an error, which is why `worker.rs` carries a regression test
/// pinning this sequence.
pub(crate) fn validate_json_document(field: &str, value: &Value) -> Result<(), String> {
    if json_exceeds_depth(value, MAX_JSON_DEPTH) {
        return Err(format!("{field} must not nest deeper than {MAX_JSON_DEPTH} levels"));
    }
    if json_contains_nul(value) {
        return Err(format!("{field} must not contain NUL"));
    }
    if json_exceeds_bytes(value, MAX_JSON_DOCUMENT_BYTES) {
        return Err(format!("{field} must not exceed {MAX_JSON_DOCUMENT_BYTES} bytes of serialized JSON"));
    }
    Ok(())
}

/// An untyped enqueue request: the template the typed `JobBuilder` API and the
/// cron scheduler compile down to. Public only under the `_test` feature —
/// the supported enqueue path is typed, so every job name is a compile-time
/// constant, which is what lets a worker register every name enqueued on its
/// queue.
#[derive(Debug, Clone)]
pub struct JobRequest {
    /// Registered handler name.
    pub name: String,
    /// JSON payload passed to the handler.
    pub payload: Value,
    /// Execution configuration.
    pub config: JobConfig,
    /// Dedupe identity shared by at most one live row per queue, at most 255
    /// bytes so it remains safe to store in PostgreSQL's B-tree index.
    /// Terminal occurrences retain the key for history and result lookup.
    pub dedupe_key: Option<String>,
    /// Earliest execution time; `None` = now.
    pub scheduled_at: Option<Timestamp>,
    /// Arbitrary user metadata stored on the row.
    pub meta: Value,
}

impl JobRequest {
    /// A new request for `name` with the given payload and default config.
    pub fn new(name: impl Into<String>, payload: Value) -> Self {
        Self {
            name: name.into(),
            payload,
            config: JobConfig::default(),
            dedupe_key: None,
            scheduled_at: None,
            meta: Value::Object(serde_json::Map::new()),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.name.is_empty() {
            return Err(Error::Config("job name must not be empty".into()));
        }
        if self.name.len() > 255 {
            return Err(Error::Config("job name must not be longer than 255 bytes".into()));
        }
        if self.name.contains('\0') {
            return Err(Error::Config("job name must not contain NUL".into()));
        }
        if let Some(dedupe_key) = self.dedupe_key.as_deref() {
            validate_dedupe_key(dedupe_key)?;
        }
        // Refused here rather than at the write: inside
        // `Queue::enqueue_in`/`enqueue_raw_in` a `54000` or `22P05` aborts the
        // *caller's* transaction and destroys their whole unit of work, and
        // outside it answers a permanently invalid request with a
        // transient-looking `Error::Db`.
        for (field, value) in [("job payload", &self.payload), ("job meta", &self.meta)] {
            validate_json_document(field, value).map_err(Error::Config)?;
        }
        // `delay` is bounded by the same window (see `validate_duration`), and
        // `at()` is the same instant expressed absolutely, so accepting one and
        // refusing the other would be arbitrary.
        if let Some(scheduled_at) = self.scheduled_at {
            if scheduled_at < MIN_TIMESTAMPTZ {
                return Err(Error::Config("job schedule time is below PostgreSQL's supported timestamp range".into()));
            }
            if SignedDuration::try_from(MAX_DURATION)
                .ok()
                .and_then(|window| Timestamp::now().checked_add(window).ok())
                .is_some_and(|horizon| scheduled_at > horizon)
            {
                return Err(Error::Config(format!(
                    "job schedule time exceeds the maximum supported duration of {MAX_DURATION:?} \
                     from now"
                )));
            }
        }
        self.config.validate()
    }
}

/// A typed, not-yet-enqueued job: `my_job::job(args)` with optional per-call
/// overrides, consumed by [`Queue::enqueue`].
///
/// Defaults come from the job's `#[pgqueue::job(...)]` attribute; every
/// builder method overrides just this enqueue.
#[must_use = "a JobBuilder does nothing until passed to Queue::enqueue"]
pub struct JobBuilder<J: JobType> {
    args: J::Args,
    config: JobConfig,
    dedupe_key: Option<String>,
    scheduled_at: Option<Timestamp>,
    delay: Option<Duration>,
    meta: Value,
    _job: PhantomData<J>,
}

impl<J: JobType> JobBuilder<J> {
    /// Starts a builder from the job's compile-time configuration. Generated
    /// code calls this as `my_job::job(args)`.
    pub fn new(args: J::Args) -> Self {
        Self {
            args,
            config: J::config(),
            dedupe_key: None,
            scheduled_at: None,
            delay: None,
            meta: Value::Object(serde_json::Map::new()),
            _job: PhantomData,
        }
    }

    /// Dedupe identity: at most one live (non-terminal) job per
    /// `(queue, dedupe_key)`. Enqueueing a duplicate returns
    /// `Ok(EnqueueResult::Deduplicated(handle))`.
    pub fn dedupe_key(mut self, key: impl Into<String>) -> Self {
        self.dedupe_key = Some(key.into());
        self
    }

    /// Runs no earlier than the given time.
    pub fn at(mut self, when: Timestamp) -> Self {
        self.scheduled_at = Some(when);
        self.delay = None;
        self
    }

    /// Runs no earlier than `delay` from now.
    pub fn delay(mut self, delay: Duration) -> Self {
        self.scheduled_at = None;
        self.delay = Some(delay);
        self
    }

    /// Overrides the dequeue priority (lower runs first).
    pub fn priority(mut self, priority: i16) -> Self {
        self.config.priority = priority;
        self
    }

    /// Overrides the maximum attempts allowed.
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.config.max_attempts = max_attempts;
        self
    }

    /// Overrides the per-attempt timeout. Must be greater than zero; to remove
    /// a timeout rather than change it, use [`JobBuilder::no_timeout`].
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = Some(timeout);
        self
    }

    /// Runs this job with no per-attempt timeout, whatever its definition
    /// declares.
    ///
    /// The other half of [`JobBuilder::timeout`], which can only ever *set* one.
    /// `#[pgqueue::job(timeout_ms = 0)]` already spells "no timeout" at the type
    /// level, so without this the per-enqueue overrides could reach every
    /// configuration the attribute can except that one. An untimed attempt is
    /// then bounded only by its worker staying alive: recovery falls to the
    /// dead-owner trigger, one
    /// [`QueueBuilder::sweep_grace`](crate::QueueBuilder::sweep_grace) past the
    /// lease that covered it.
    pub fn no_timeout(mut self) -> Self {
        self.config.timeout = None;
        self
    }

    /// Overrides how long the row is retained after a `complete` finish.
    pub fn retention(mut self, retention: JobRetention) -> Self {
        self.config.retention = retention;
        self
    }

    /// Overrides how long the row is retained after a `failed` or `aborted`
    /// finish.
    pub fn failed_retention(mut self, retention: JobRetention) -> Self {
        self.config.failed_retention = retention;
        self
    }

    /// Overrides the base retry delay.
    pub fn retry_delay(mut self, delay: Duration) -> Self {
        self.config.retry_delay = delay;
        self
    }

    /// Overrides the retry backoff strategy.
    pub fn backoff(mut self, backoff: JobRetryBackoff) -> Self {
        self.config.backoff = backoff;
        self
    }

    /// Attaches arbitrary JSON metadata to the row.
    pub fn meta(mut self, meta: Value) -> Self {
        self.meta = meta;
        self
    }

    /// Converts the builder into a cron template. Rejects `delay()`/`at()`
    /// instead of dropping them: the cron expression overwrites every
    /// occurrence's `scheduled_at`, so a scheduling override can never take
    /// effect.
    pub(crate) fn into_cron_template(self) -> Result<JobRequest, Error> {
        let (job, delay) = self.into_parts()?;
        if delay.is_some() || job.scheduled_at.is_some() {
            return Err(Error::Config(format!(
                "cron job {:?} cannot use delay() or at(): the cron expression schedules every occurrence",
                job.name
            )));
        }
        job.validate()?;
        Ok(job)
    }

    pub(crate) fn into_parts(self) -> Result<(JobRequest, Option<Duration>), Error> {
        let job = JobRequest {
            name: J::NAME.to_string(),
            payload: encode_json(&self.args)?,
            config: self.config,
            dedupe_key: self.dedupe_key,
            scheduled_at: self.scheduled_at,
            meta: self.meta,
        };
        Ok((job, self.delay))
    }
}

/// Result of publishing a job with an optional dedupe key.
///
/// Both variants contain the new or existing job identity. Typed enqueue
/// methods store a [`JobHandle`]; the test-only raw enqueue methods store the
/// job's [`Uuid`]. A deduplicated publish points at the live job that already
/// owns the key; it does not provide exactly-once execution.
///
/// Deliberately *not* `#[non_exhaustive]`, unlike the other output enums here.
/// A publish either inserted a row or found a live one holding the key; the
/// database protocol behind it (`DatabaseEnqueueResult`) has exactly those two
/// answers because the `ON CONFLICT` that produces them has exactly two. There
/// is no third to reserve room for, and matching both arms is how this type is
/// meant to be read — reserving it would cost every caller a `_` arm forever
/// for a variant that cannot arrive without `enqueue` meaning something else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnqueueResult<H> {
    /// A new job row was inserted.
    Enqueued(H),
    /// A live job already owned the request's dedupe key.
    Deduplicated(H),
}

impl<H> EnqueueResult<H> {
    /// Whether this publish inserted a new row.
    pub fn is_enqueued(&self) -> bool {
        matches!(self, Self::Enqueued(_))
    }

    /// Whether this publish reused a live row with the same dedupe key.
    pub fn is_deduplicated(&self) -> bool {
        matches!(self, Self::Deduplicated(_))
    }

    fn value(&self) -> &H {
        match self {
            Self::Enqueued(handle) | Self::Deduplicated(handle) => handle,
        }
    }

    fn into_value(self) -> H {
        match self {
            Self::Enqueued(handle) | Self::Deduplicated(handle) => handle,
        }
    }
}

impl EnqueueResult<Uuid> {
    /// Returns the new or existing job's id.
    pub fn job_id(&self) -> Uuid {
        *self.value()
    }

    /// Consumes the result and returns the new or existing job's id.
    pub fn into_job_id(self) -> Uuid {
        self.into_value()
    }
}

/// Collects and validates incrementally so a large or endless iterator cannot allocate past the batch limits.
fn collect_batch<J: JobType>(
    jobs: impl IntoIterator<Item = JobBuilder<J>>,
) -> Result<Vec<(JobRequest, Option<Duration>)>, Error> {
    let mut batch = Vec::new();
    let mut bytes = 0usize;
    for builder in jobs {
        if batch.len() == MAX_ENQUEUE_BATCH_JOBS {
            return Err(Error::Config(format!("enqueue batch exceeds the maximum of {MAX_ENQUEUE_BATCH_JOBS} jobs")));
        }
        let (job, delay) = builder.into_parts()?;
        crate::database::validate_enqueue(&job, delay)?;
        // Per-document validation bounds the depth and size before these serialization walks.
        bytes = bytes.saturating_add(json_byte_len(&job.payload)).saturating_add(json_byte_len(&job.meta));
        if bytes > MAX_ENQUEUE_BATCH_BYTES {
            return Err(Error::Config(format!(
                "enqueue batch exceeds the maximum of {MAX_ENQUEUE_BATCH_BYTES} bytes of serialized payload and \
                 metadata"
            )));
        }
        batch.push((job, delay));
    }
    Ok(batch)
}

impl Queue {
    /// Enqueues a typed job: `queue.enqueue(my_job::job(args)).await?`.
    ///
    /// A dedupe-key collision returns [`EnqueueResult::Deduplicated`] with a
    /// typed handle to the existing job. It is an error when that row belongs
    /// to a different job type.
    ///
    /// An `Err` means the publish is *indeterminate*, not that it did not
    /// happen: a future dropped after the statement reached the server may
    /// still commit its insert. Retrying on error can therefore publish the job
    /// twice — pass a [`JobBuilder::dedupe_key`] when the caller retries.
    pub async fn enqueue<J: JobType>(&self, job: JobBuilder<J>) -> Result<EnqueueResult<JobHandle<J>>, Error> {
        let (new_job, delay) = job.into_parts()?;
        let retentions = new_job.config.retentions();
        let result = self.database().enqueue_raw_delayed_result(new_job, delay, false).await?;
        typed_enqueue_result::<J>(self, result, retentions)
    }

    /// Enqueues jobs of one type as a batch: one statement, one round trip,
    /// and at most one wakeup notification for the whole batch, where
    /// [`Queue::enqueue`] pays for each of those per job.
    ///
    /// Results come back in input order. A job whose dedupe key is held by a live job, or by an earlier job in the same
    /// batch, is reported as [`EnqueueResult::Deduplicated`] with a handle to that holder, exactly as
    /// [`Queue::enqueue`] reports it. A key held by a job of a *different* type fails the whole call, and nothing from
    /// the batch is published. The locks are the ones a single keyed enqueue takes, acquired in lock order, so a batch
    /// cannot deadlock with another batch or with an autocommit single enqueue of the same keys. A caller transaction
    /// that takes keys over several calls takes them in its own order, which a batch can invert; PostgreSQL then aborts
    /// one of the two with `40P01`. [`Queue::enqueue_in`] explains how to avoid that.
    ///
    /// A batch is bounded by [`MAX_ENQUEUE_BATCH_JOBS`] jobs and
    /// [`MAX_ENQUEUE_BATCH_BYTES`] of serialized payload and metadata, which
    /// keeps the statement under PostgreSQL's message-size limit; a larger
    /// batch is refused as [`Error::Config`] before anything is sent. An
    /// empty batch inserts nothing and returns an empty vector.
    ///
    /// As with [`Queue::enqueue`], an `Err` after validation leaves the
    /// publish indeterminate rather than proving it did not happen.
    pub async fn enqueue_batch<J: JobType>(
        &self,
        jobs: impl IntoIterator<Item = JobBuilder<J>>,
    ) -> Result<Vec<EnqueueResult<JobHandle<J>>>, Error> {
        let batch = collect_batch(jobs)?;
        let retentions = batch.iter().map(|(job, _)| job.config.retentions()).collect::<Vec<_>>();
        let results = self.database().enqueue_batch_validated_result(batch).await?;
        typed_batch_results::<J>(self, results, retentions)
    }

    /// [`Queue::enqueue_batch`] inside a caller-owned transaction, with
    /// [`Queue::enqueue_in`]'s visibility and locking rules. An error after
    /// validation can leave rows of this batch inserted in the transaction, as
    /// any failed statement inside one can; roll the transaction back.
    pub async fn enqueue_batch_in<J: JobType>(
        &self,
        transaction: &mut sqlx::PgTransaction<'_>,
        jobs: impl IntoIterator<Item = JobBuilder<J>>,
    ) -> Result<Vec<EnqueueResult<JobHandle<J>>>, Error> {
        let batch = collect_batch(jobs)?;
        let retentions = batch.iter().map(|(job, _)| job.config.retentions()).collect::<Vec<_>>();
        let results = self.database().enqueue_batch_validated_in_result(transaction, batch).await?;
        typed_batch_results::<J>(self, results, retentions)
    }

    /// Enqueues a typed job as part of a caller-owned PostgreSQL transaction.
    ///
    /// The job and its notification become visible only if the caller commits. Dedupe-key advisory locks remain held
    /// until that commit. The exception is a savepoint: a lock taken inside one is released by `ROLLBACK TO SAVEPOINT`
    /// rather than held to the top-level commit, together with the row it guarded — so the lock still covers exactly
    /// the decision it was taken for.
    ///
    /// Because the locks are held, two transactions that each publish several keyed jobs can wait on each other, and
    /// PostgreSQL then aborts one of them with `40P01`. Publish a transaction's keyed jobs of one type through one
    /// [`Queue::enqueue_batch_in`] call, which takes their locks in the one order every batch uses, rather than through
    /// one `enqueue_in` call per key. Jobs of several types, or on several queues, need several calls: make them in the
    /// same order in every transaction, and retry a transaction aborted with `40P01`.
    ///
    /// PostgreSQL's default `READ COMMITTED` isolation is required to observe a
    /// dedupe-key owner that commits while this call waits for its lock. At
    /// `REPEATABLE READ` or `SERIALIZABLE`, retry the whole transaction if such
    /// a concurrent owner is outside the caller's snapshot.
    pub async fn enqueue_in<J: JobType>(
        &self,
        transaction: &mut sqlx::PgTransaction<'_>,
        job: JobBuilder<J>,
    ) -> Result<EnqueueResult<JobHandle<J>>, Error> {
        let (new_job, delay) = job.into_parts()?;
        let retentions = new_job.config.retentions();
        let result = self.database().enqueue_raw_delayed_in_result(transaction, new_job, delay, false).await?;
        typed_enqueue_result::<J>(self, result, retentions)
    }

    /// Enqueues a job and waits for its typed result (request/response).
    ///
    /// If the builder carries a `dedupe_key` that deduplicates against a live
    /// job, `enqueue_and_wait` waits on that existing job instead. Failures surface as
    /// [`Error::Job`]; `None` timeout waits forever.
    ///
    /// The job's retention must keep the row around long enough to read the
    /// outcome, whichever it is: `JobRetention::DeleteImmediately` for either
    /// the result or the failure is rejected before enqueue.
    ///
    /// The job is inserted already marked as awaited, so its finish emits the
    /// completion notification this wait resolves on without a second write.
    ///
    /// As with [`Queue::enqueue`], an `Err` leaves the publish indeterminate
    /// rather than proving it did not happen.
    pub async fn enqueue_and_wait<J: JobType>(
        &self,
        job: JobBuilder<J>,
        timeout: Option<Duration>,
    ) -> Result<J::Output, Error> {
        let (new_job, delay) = job.into_parts()?;
        let retentions = new_job.config.retentions();
        if retentions.deletes_immediately() {
            return Err(Error::Config(
                "enqueue_and_wait requires retention for both outcomes; DeleteImmediately removes the row before its \
                 result or error can be read"
                    .into(),
            ));
        }
        let handle: JobHandle<J> = match self.database().enqueue_raw_delayed_result(new_job, delay, true).await? {
            DatabaseEnqueueResult::Inserted(id) => JobHandle::new(id, self.clone(), retentions, true),
            DatabaseEnqueueResult::Deduplicated { id, name, retentions } => {
                if retentions.deletes_immediately() {
                    return Err(Error::Config(
                        "enqueue_and_wait cannot wait on the existing deduplicated job because it deletes its row \
                         immediately on one of its outcomes"
                            .into(),
                    ));
                }
                if name != J::NAME {
                    return Err(Error::Config(format!("dedupe key belongs to job {name:?}, not {:?}", J::NAME)));
                }
                JobHandle::new(id, self.clone(), retentions, false)
            }
        };
        handle.wait(timeout).await
    }
}

fn typed_enqueue_result<J: JobType>(
    queue: &Queue,
    result: DatabaseEnqueueResult,
    inserted_retentions: JobRetentions,
) -> Result<EnqueueResult<JobHandle<J>>, Error> {
    match result {
        DatabaseEnqueueResult::Inserted(id) => {
            Ok(EnqueueResult::Enqueued(JobHandle::new(id, queue.clone(), inserted_retentions, false)))
        }
        DatabaseEnqueueResult::Deduplicated { id, name, retentions } => {
            if name != J::NAME {
                return Err(Error::Config(format!("dedupe key belongs to job {name:?}, not {:?}", J::NAME)));
            }
            Ok(EnqueueResult::Deduplicated(JobHandle::new(id, queue.clone(), retentions, false)))
        }
    }
}

/// [`typed_enqueue_result`] over a batch, pairing each result with the
/// retention the request it answers was enqueued with.
fn typed_batch_results<J: JobType>(
    queue: &Queue,
    results: Vec<DatabaseEnqueueResult>,
    retentions: Vec<JobRetentions>,
) -> Result<Vec<EnqueueResult<JobHandle<J>>>, Error> {
    results
        .into_iter()
        .zip(retentions)
        .map(|(result, retentions)| typed_enqueue_result::<J>(queue, result, retentions))
        .collect()
}

/// The pair of retention policies a job carries, as the enqueue paths hand
/// them to the [`JobHandle`] they return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JobRetentions {
    /// After a `complete` finish.
    pub(crate) result: JobRetention,
    /// After a `failed` or `aborted` finish.
    pub(crate) failed: JobRetention,
}

impl JobRetentions {
    /// Whether either outcome leaves no row to read: a wait needs both to be
    /// durable, because it does not know in advance which one it will get.
    pub(crate) fn deletes_immediately(self) -> bool {
        self.result == JobRetention::DeleteImmediately || self.failed == JobRetention::DeleteImmediately
    }
}

impl JobConfig {
    pub(crate) fn retentions(&self) -> JobRetentions {
        JobRetentions { result: self.retention, failed: self.failed_retention }
    }
}

/// A reference to an enqueued job.
#[derive(Clone)]
pub struct JobHandle<J: JobType> {
    pub(crate) id: Uuid,
    pub(crate) queue: Queue,
    pub(super) retentions: JobRetentions,
    /// Whether the row is already marked awaited, so a wait can skip the
    /// write that registers its interest in the completion notification.
    awaited: bool,
    _job: PhantomData<fn() -> J>,
}

impl<J: JobType> JobHandle<J> {
    fn new(id: Uuid, queue: Queue, retentions: JobRetentions, awaited: bool) -> Self {
        Self { id, queue, retentions, awaited, _job: PhantomData }
    }

    /// The job's id (UUIDv7).
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Fetches the job's current row.
    pub async fn fetch_job(&self) -> Result<JobRow, Error> {
        self.queue.fetch_job(self.id).await?.ok_or(Error::JobNotFound(self.id))
    }

    /// Requests an abort (see [`Queue::abort_job`]).
    pub async fn abort(&self, reason: &str) -> Result<bool, Error> {
        self.queue.abort_job(self.id, reason).await
    }

    /// Waits for the job to finish and deserializes its result.
    ///
    /// Resolution is push-based (the queue's completion NOTIFY channel) with
    /// a polling fallback, so results arrive promptly even if a notification
    /// is lost. The timeout covers every database read made by this operation.
    /// Failures surface as [`Error::Job`]; `None` waits forever.
    /// Delete-immediately jobs have no durable result and cannot be waited on,
    /// except for a queued abort that is still present as a terminal row.
    pub async fn wait(&self, timeout: Option<Duration>) -> Result<J::Output, Error> {
        Ok(serde_json::from_value(self.wait_value(timeout).await?)?)
    }

    /// Like [`JobHandle::wait`] but returns the raw JSON result.
    pub async fn wait_value(&self, timeout: Option<Duration>) -> Result<Value, Error> {
        match timeout {
            Some(timeout) => {
                tokio::time::timeout(timeout, self.wait_value_inner()).await.map_err(|_| Error::WaitTimeout)?
            }
            None => self.wait_value_inner().await,
        }
    }

    async fn wait_value_inner(&self) -> Result<Value, Error> {
        if self.retentions.deletes_immediately() {
            // Queued aborts intentionally remain until sweep, so a caller that
            // already aborted may still read that terminal result. Running or
            // deleted rows cannot provide a reliable result.
            if let Some(outcome) = self.queue.database().job_outcome(self.id).await?
                && outcome.status.is_terminal()
            {
                return resolve(outcome);
            }
            return Err(Error::Config(
                "wait requires retention for both outcomes; a DeleteImmediately job has no durable row to read".into(),
            ));
        }
        self.wait_inner().await
    }

    /// How a wait reports an id whose row is not there any more: as the retention deletion it is when the last poll saw
    /// the row claimed, and as a missing job otherwise. The push path answers the same question with `resolve_deleted`,
    /// which knows the job finished because the completion event said so.
    fn vanished(&self, last_seen_claimed: bool) -> Error {
        if last_seen_claimed { Error::ResultExpired(self.id) } else { Error::JobNotFound(self.id) }
    }

    async fn wait_inner(&self) -> Result<Value, Error> {
        // The fallback poll only matters when a notification was lost, so it
        // backs off: short waits stay snappy while long waits settle at the
        // maximum instead of hammering the pool even though completions arrive
        // push-based.
        const INITIAL_POLL_INTERVAL: Duration = Duration::from_millis(250);
        const MAX_POLL_INTERVAL: Duration = Duration::from_secs(2);

        // Register interest before subscribing: a finish emits its completion
        // notification only for a row marked awaited, so every finish that
        // lands after this write is pushed, and one that landed before it is
        // caught by the first poll below. The write is skipped for a handle
        // whose enqueue already set the flag. A write that fails is a lost
        // registration, not a lost wait — the poll loop resolves the job at
        // its own cadence, exactly as it does while the listener is down —
        // except on a closed pool, which the poll gives up on too.
        if !self.awaited {
            match self.queue.database().mark_awaited(self.id).await {
                Ok(()) => {}
                Err(error @ Error::Db(sqlx::Error::PoolClosed)) => return Err(error),
                Err(error) => {
                    tracing::warn!(job.id = %self.id, %error, "could not register for the completion notification; polling");
                }
            }
        }

        // Subscribe before the first status check so a finish landing in
        // between can't be missed. The listener needs its own connection
        // outside the query pool, so it can be refused while the queue is
        // perfectly reachable; it reconnects in the background and the poll
        // loop below carries the wait until it does.
        let mut done = self.queue.notify_listener().subscribe_done(self.id);
        let mut poll_interval = INITIAL_POLL_INTERVAL;
        // Whether the last poll saw this id claimed: `running`, or `aborting` on its way out of a run. A poll that
        // finds no row cannot tell a purged job from one that never existed, but a wait that watched the row run and
        // *then* found it gone has watched retention delete a finished job — which is exactly `Error::ResultExpired`,
        // and is what the push path already reports through `resolve_deleted` for the same physical event. Without
        // this, whether a caller learns "completed, result expired" or "no such job" depended on whether the completion
        // notification happened to arrive, so a lagging channel — the normal state under many concurrent waiters —
        // turned a finished job into a missing one.
        //
        // A row last seen `queued` is the other case: retention deletes only finished rows, so what removes a queued
        // one is `Queue::purge_queued_jobs` or a foreign `DELETE`. A flag any live status set told the waiter on a job
        // purged before it ever ran that it had completed, and a caller trusting that skips work that never happened. A
        // row claimed, finished and purged all inside one poll interval, with its notification lost, is misreported as
        // missing instead, which errs the safe way: the caller redoes the work.
        let mut last_seen_claimed = false;
        // A terminal notification carries the one fact polling cannot recover
        // after retention deletes the row: whether the job failed or was
        // aborted. Keep it across a failed re-fetch instead of consuming that
        // fact along with the event. A later successful non-terminal read
        // disproves a stale or foreign notification and clears it.
        let mut pending_terminal_event = None;
        'poll: loop {
            // A poll that could not reach the database is a *lost poll*, not a
            // lost wait. Propagating it ended every in-flight wait on the first
            // `PoolTimedOut` of an outage — about thirty seconds in, whatever
            // deadline the caller gave — while the job itself ran to completion
            // and a later wait on the same id returned its result. The polling
            // fallback exists for exactly the interval in which notifications
            // are not arriving, and every other long-lived consumer of this pool
            // (the listener's reconnect loop, every worker loop) already retries
            // rather than giving up. `enqueue_and_wait` made it worse: it
            // returns no id, so a caller that lost the wait could not re-wait,
            // poll, or abort what it had started.
            //
            // The caller's own `timeout` still bounds the whole thing, and the
            // outcomes that mean something — a missing job, a failed job, an
            // expired result — are unaffected.
            let outcome = match self.queue.database().job_outcome(self.id).await {
                Ok(outcome) => outcome,
                // A closed pool never reopens — the process is tearing this
                // queue down — so retrying it is the one case that would spin to
                // the caller's deadline (for ever, on a `None` timeout) with no
                // possibility of an answer. The same exception the background
                // resolvers make.
                Err(error @ Error::Db(sqlx::Error::PoolClosed)) => return Err(error),
                Err(error) => {
                    tracing::warn!(job.id = %self.id, %error, "job result poll failed; retrying");
                    tokio::time::sleep(poll_interval).await;
                    poll_interval = (poll_interval * 2).min(MAX_POLL_INTERVAL);
                    continue 'poll;
                }
            };
            let missing = match outcome {
                Some(outcome) if outcome.status.is_terminal() => return resolve(outcome),
                Some(outcome) => {
                    pending_terminal_event = None;
                    last_seen_claimed = matches!(outcome.status, JobStatus::Running | JobStatus::Aborting);
                    false
                }
                // A delete-immediately finish commits the row deletion and
                // NOTIFY atomically, but listener delivery can lag this read.
                // Give the already-subscribed receiver one poll interval to
                // observe that terminal event before declaring the ID absent.
                None => match pending_terminal_event.take() {
                    Some(event) => return resolve_deleted(event),
                    None => true,
                },
            };
            let poll_deadline = tokio::time::sleep(poll_interval);
            poll_interval = (poll_interval * 2).min(MAX_POLL_INTERVAL);
            tokio::pin!(poll_deadline);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut poll_deadline => {
                        if missing {
                            return match pending_terminal_event.take() {
                                Some(event) => resolve_deleted(event),
                                None => Err(self.vanished(last_seen_claimed)),
                            };
                        }
                        continue 'poll;
                    },
                    // The subscription is keyed by this job's id, so an event here is always ours. Re-fetch for its
                    // result; if retention already removed the row, resolve from the event alone.
                    event = done.recv() => {
                        match self.queue.database().job_outcome(self.id).await {
                            Ok(Some(outcome)) if outcome.status.is_terminal() => return resolve(outcome),
                            Ok(Some(_)) => {}
                            Ok(None) => return resolve_deleted(event),
                            // The same rule as the poll above: a read that failed learned nothing, least of all that
                            // the row is gone. Keep the event while polling so a later missing row retains its terminal
                            // classification after retention deletes it.
                            Err(error) => {
                                pending_terminal_event = Some(event);
                                tracing::warn!(
                                    job.id = %self.id, %error,
                                    "job completion read failed; falling back to polling"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<J: JobType> EnqueueResult<JobHandle<J>> {
    /// Returns the new or existing job's id.
    ///
    /// ```no_run
    /// # #[pgqueue::job]
    /// # async fn cleanup(_: ()) {}
    /// # async fn enqueue(queue: pgqueue::Queue) -> Result<(), pgqueue::Error> {
    /// let result = queue.enqueue(cleanup::job(())).await?;
    /// assert_eq!(result.job_id(), result.job_handle().id());
    /// # Ok(())
    /// # }
    /// ```
    pub fn job_id(&self) -> Uuid {
        self.job_handle().id()
    }

    /// Borrows the new or existing job handle.
    pub fn job_handle(&self) -> &JobHandle<J> {
        self.value()
    }

    /// Consumes the result and returns the new or existing job handle.
    pub fn into_job_handle(self) -> JobHandle<J> {
        self.into_value()
    }
}

impl<J: JobType> std::fmt::Debug for JobHandle<J> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobHandle").field("id", &self.id).field("job", &J::NAME).finish_non_exhaustive()
    }
}

fn resolve_deleted(event: QueueDoneEvent) -> Result<Value, Error> {
    match event.status {
        // The completed row was purged (retention expiry) between the
        // notification and the re-fetch: the result is unrecoverable, which
        // must not masquerade as a successful null result.
        JobStatus::Complete => Err(Error::ResultExpired(event.id)),
        JobStatus::Failed => Err(Error::Job(JobError::new(JobErrorKind::Failed, "job failed and was deleted"))),
        JobStatus::Aborted | JobStatus::Aborting => {
            Err(Error::Job(JobError::new(JobErrorKind::Aborted, "job was aborted and deleted")))
        }
        JobStatus::Queued | JobStatus::Running => {
            Err(Error::Config(format!("job emitted a non-terminal {} completion event", event.status)))
        }
    }
}

fn resolve(outcome: crate::database::DatabaseJobOutcome) -> Result<Value, Error> {
    // Every caller guards on `JobStatus::is_terminal`, so `Aborting` — which is
    // not terminal — never arrives here and gets no arm of its own.
    match outcome.status {
        JobStatus::Complete => Ok(outcome.result.unwrap_or(Value::Null)),
        // Aborts store the raw reason (e.g. "aborted from ui"), not a
        // JobError rendering — classify by status.
        JobStatus::Aborted => {
            Err(Error::Job(JobError::new(JobErrorKind::Aborted, outcome.error.as_deref().unwrap_or("aborted"))))
        }
        _ => Err(Error::Job(
            outcome
                .error
                .as_deref()
                .map(JobError::from_stored)
                .unwrap_or_else(|| JobError::failed(format!("job {}", outcome.status))),
        )),
    }
}

#[cfg(test)]
mod api_tests {
    use std::cell::Cell;

    use super::*;

    struct StatefulFloat<'a>(&'a Cell<u32>);

    impl Serialize for StatefulFloat<'_> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let calls = self.0.get();
            self.0.set(calls + 1);
            serializer.serialize_f64(if calls == 0 { 1.5 } else { f64::NAN })
        }
    }

    #[test]
    fn test_new_job_uses_expected_defaults() {
        let job = JobRequest::new("send_email", serde_json::json!({"to": "a@b.c"}));
        assert_eq!(job.name, "send_email");
        assert_eq!(job.config, JobConfig::default());
        assert!(job.dedupe_key.is_none());
        assert!(job.scheduled_at.is_none());
        assert_eq!(job.meta, serde_json::json!({}));
    }

    #[test]
    fn test_new_job_rejects_an_oversized_dedupe_key() {
        let mut job = JobRequest::new("bounded", Value::Null);
        job.dedupe_key = Some("x".repeat(MAX_INDEXED_KEY_BYTES + 1));
        let error = job.validate().unwrap_err();
        assert!(error.to_string().contains("255 bytes"), "{error}");
    }

    /// `jsonb` cannot store `\0`, so a payload carrying one used to reach
    /// SQL and raise `22P05` — which inside `Queue::enqueue_in` aborts the
    /// *caller's* transaction and destroys their whole unit of work over an
    /// input error that is detectable here.
    #[test]
    fn test_new_job_rejects_a_nul_anywhere_in_the_payload_or_meta() {
        for (field, nul) in [
            ("job payload", serde_json::json!("bad\0value")),
            ("job payload", serde_json::json!({ "k": ["ok", "bad\0"] })),
            ("job payload", serde_json::json!({ "bad\0key": 1 })),
            ("job meta", serde_json::json!({ "trace": "bad\0" })),
        ] {
            let mut job = JobRequest::new("nul", Value::Null);
            if field == "job payload" {
                job.payload = nul.clone();
            } else {
                job.meta = nul.clone();
            }
            let error = job.validate().unwrap_err();
            assert_eq!(error.to_string(), format!("configuration error: {field} must not contain NUL"));
        }

        // Values that merely *contain* the escape's neighbours still pass.
        let mut job = JobRequest::new("fine", serde_json::json!({ "k": ["ü", "\u{1}"] }));
        job.meta = serde_json::json!({ "nested": { "deep": [null, 1, true] } });
        job.validate().unwrap();
    }

    /// An absolute schedule must fit both pgqueue's delay window and
    /// PostgreSQL's timestamp representation.
    #[test]
    fn test_new_job_bounds_an_absolute_schedule_time() {
        let window = SignedDuration::try_from(MAX_DURATION).unwrap();
        let mut job = JobRequest::new("scheduled", Value::Null);

        job.scheduled_at = Some(MIN_TIMESTAMPTZ);
        job.validate().unwrap();

        job.scheduled_at = Some(MIN_TIMESTAMPTZ - SignedDuration::from_nanos(1));
        let error = job.validate().unwrap_err();
        assert!(error.to_string().contains("below PostgreSQL's supported timestamp range"), "{error}");

        job.scheduled_at = Some(Timestamp::now() + window - SignedDuration::from_hours(24));
        job.validate().unwrap();

        job.scheduled_at = Some(Timestamp::now() + window + SignedDuration::from_hours(24));
        let error = job.validate().unwrap_err();
        assert!(error.to_string().contains("job schedule time exceeds the maximum supported duration"), "{error}");

        // A time in the past is meaningful ("run now"): cron publishes missed
        // occurrences that way.
        job.scheduled_at = Some(Timestamp::now() - window);
        job.validate().unwrap();
    }

    /// JSON has no NaN or infinity: `serde_json::to_value` maps them to
    /// `null`, so an enqueue that should have been refused succeeded and the
    /// worker terminally failed the decode — or an `Option<f64>` round-tripped
    /// a NaN as a clean `None`, which is silent corruption. No `serde_json`
    /// serializer refuses one, tree or text, which is why the typed encode gates
    /// run the value through [`RejectNonFinite`] first.
    #[test]
    fn test_typed_encoding_refuses_non_finite_floats() {
        assert_eq!(encode_json(&1.5f64).unwrap(), serde_json::json!(1.5));
        assert_eq!(encode_json(&Some(2.5f32)).unwrap(), serde_json::json!(2.5));
        assert_eq!(encode_json(&None::<f64>).unwrap(), Value::Null);

        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(encode_json(&bad).is_err(), "{bad} must be refused");
            assert!(encode_json(&Some(bad)).is_err(), "Some({bad}) must not become None");
            let nested: Vec<Option<f64>> = vec![Some(1.0), Some(bad)];
            assert!(encode_json(&nested).is_err(), "a nested {bad} must be refused");
        }
    }

    #[test]
    fn test_typed_encoding_serializes_user_values_once() {
        let calls = Cell::new(0);
        assert_eq!(encode_json(&StatefulFloat(&calls)).unwrap(), serde_json::json!(1.5));
        assert_eq!(calls.get(), 1);
    }

    /// A negative TTL has no encoding, but decoding one as a live zero-length
    /// retention is the single reading that *keeps* the row rather than
    /// deleting it, so it inverts the caller's intent.
    #[test]
    fn test_retention_decodes_a_negative_ttl_as_an_immediate_delete() {
        assert_eq!(JobRetention::from_result_ttl_ms(Some(-1)), JobRetention::DeleteImmediately);
        assert_eq!(JobRetention::from_result_ttl_ms(Some(i64::MIN)), JobRetention::DeleteImmediately);
    }

    #[test]
    fn test_resolve_deleted_preserves_failed_terminal_result() {
        let id = Uuid::now_v7();
        let error = resolve_deleted(QueueDoneEvent { id, status: JobStatus::Failed }).unwrap_err();

        let Error::Job(error) = error else {
            panic!("deleted failed row should resolve to a job error");
        };
        assert_eq!(error.kind, JobErrorKind::Failed);
        assert_eq!(error.message, "job failed and was deleted");
    }
}
