//! Parsing of `#[pgqueue::job(...)]` attribute arguments.

use std::collections::HashSet;

use proc_macro2::{Span, TokenStream};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Expr, ExprLit, ExprUnary, Lit, Meta, Token, UnOp};

/// Parsed attribute arguments; every field is optional and overrides
/// `JobConfig::default()` in the generated `config()`.
#[derive(Debug, Default)]
pub(crate) struct JobAttrs {
    /// `name = "custom"` — registry/database name (default: the fn name).
    pub name: Option<String>,
    /// `max_attempts = 3` — total attempts allowed.
    pub max_attempts: Option<u32>,
    /// `timeout_ms = 30_000` — per-attempt limit; zero disables the timeout.
    pub timeout_ms: Option<Option<u64>>,
    /// `result_ttl_ms = 3_600_000` — result retention; zero deletes immediately.
    pub result_ttl_ms: Option<ResultTtl>,
    /// `failed_ttl_ms = 604_800_000` — retention after a failed or aborted
    /// finish; zero deletes immediately.
    pub failed_ttl_ms: Option<ResultTtl>,
    /// `retry_delay_ms = 500` — base retry delay.
    pub retry_delay_ms: Option<u64>,
    /// `max_backoff_ms = 60_000` — capped exponential backoff.
    pub max_backoff_ms: Option<u64>,
    /// `priority = -1` — dequeue priority (lower first).
    pub priority: Option<i16>,
    /// `revision = 2` — durable cron definition revision.
    pub revision: Option<u64>,
    /// Every millisecond literal the attribute carried, as `(key, value,
    /// span)`, for the bound check the expansion defers to generated code.
    pub durations: Vec<(&'static str, u64, Span)>,
}

/// A retention as written in the attribute: `result_ttl_ms` or
/// `failed_ttl_ms`.
#[derive(Debug)]
pub(crate) enum ResultTtl {
    ForMs(u64),
    Delete,
}

impl ResultTtl {
    /// Zero is the delete-immediately sentinel rather than a duration.
    fn from_ms(ms: u64) -> Self {
        if ms == 0 { ResultTtl::Delete } else { ResultTtl::ForMs(ms) }
    }
}

/// Which attribute macro is parsing. `revision` is cron-only, so both the
/// "expected one of" list and the rejection of a misplaced `revision` depend on
/// it — and rejecting it here spans the key the user wrote rather than the
/// whole attribute.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AttrMode {
    Job,
    Cron,
}

impl AttrMode {
    /// The longest job name this macro can accept.
    ///
    /// `#[pgqueue::cron]` gets five fewer bytes than a plain job: a cron's
    /// durable identity is `JobCronEntry`'s derived `cron:{name}` dedupe key,
    /// which `JobRequest::validate` caps at the same 255 bytes the column does.
    /// Without the narrower bound a 251-byte cron name compiles and then fails
    /// at `Worker::build()` — the exact runtime failure this rule exists to
    /// turn into a compile error.
    pub(crate) fn max_name_bytes(self) -> usize {
        match self {
            AttrMode::Job => 255,
            AttrMode::Cron => 250,
        }
    }

    /// The keys this macro accepts, for the unknown-attribute error. Suggesting
    /// `revision` to a `#[pgqueue::job]` would point at a key the very next
    /// check rejects.
    fn expected_keys(self) -> &'static str {
        match self {
            AttrMode::Job => {
                "name, max_attempts, timeout_ms, result_ttl_ms, failed_ttl_ms, retry_delay_ms, \
                 max_backoff_ms, priority"
            }
            AttrMode::Cron => {
                "name, max_attempts, timeout_ms, result_ttl_ms, failed_ttl_ms, retry_delay_ms, \
                 max_backoff_ms, priority, revision"
            }
        }
    }
}

impl JobAttrs {
    /// Records a millisecond literal for the `MAX_DURATION_MS` bound check the
    /// expansion emits, keeping the span of the literal the user wrote.
    ///
    /// The bound cannot be checked here (this crate cannot depend on
    /// `pgqueue`), and without the span the generated assertion underlines the
    /// whole `#[pgqueue::job(...)]` attribute rather than the value that broke
    /// it — unlike every other diagnostic in this module.
    ///
    /// Zero is a sentinel (no timeout, delete the result immediately) rather
    /// than a duration, so it needs no bound check.
    fn record_duration(&mut self, key: &'static str, ms: u64, span: Span) {
        if ms != 0 {
            self.durations.push((key, ms, span));
        }
    }

    pub(crate) fn parse(tokens: TokenStream, mode: AttrMode) -> syn::Result<Self> {
        let mut attrs = JobAttrs::default();
        if tokens.is_empty() {
            return Ok(attrs);
        }
        let metas = syn::parse::Parser::parse2(Punctuated::<Meta, Token![,]>::parse_terminated, tokens)?;
        let mut seen = HashSet::new();
        let mut max_backoff_span = None;

        for meta in metas {
            match &meta {
                Meta::Path(path) if path.is_ident("backoff") => {
                    return Err(err(path, "`backoff` is not supported; use `max_backoff_ms = ...`"));
                }
                Meta::NameValue(nv) => {
                    let ident = nv
                        .path
                        .get_ident()
                        .ok_or_else(|| err(&nv.path, "expected a simple attribute name"))?
                        .to_string();
                    if !seen.insert(ident.clone()) {
                        return Err(err(&nv.path, &format!("duplicate attribute `{ident}`")));
                    }
                    match ident.as_str() {
                        "name" => {
                            let name = string_value(&nv.value)?;
                            let max = mode.max_name_bytes();
                            if name.is_empty() || name.len() > max || name.contains('\0') {
                                return Err(err(
                                    &nv.value,
                                    &format!("job name must be 1..={max} bytes and contain no NUL"),
                                ));
                            }
                            attrs.name = Some(name);
                        }
                        "max_attempts" => {
                            let max_attempts = u32::try_from(unsigned_value(&nv.value)?)
                                .ok()
                                .filter(|max_attempts| (1..i32::MAX as u32).contains(max_attempts))
                                .ok_or_else(|| err(&nv.value, "max_attempts must be between 1 and 2147483646"))?;
                            attrs.max_attempts = Some(max_attempts);
                        }
                        "timeout_ms" => {
                            let timeout_ms = milliseconds_value("timeout_ms", &nv.value)?;
                            attrs.record_duration("timeout_ms", timeout_ms, nv.value.span());
                            attrs.timeout_ms = Some((timeout_ms != 0).then_some(timeout_ms));
                        }
                        "result_ttl_ms" => {
                            let result_ttl_ms = milliseconds_value("result_ttl_ms", &nv.value)?;
                            attrs.record_duration("result_ttl_ms", result_ttl_ms, nv.value.span());
                            attrs.result_ttl_ms = Some(ResultTtl::from_ms(result_ttl_ms));
                        }
                        "failed_ttl_ms" => {
                            let failed_ttl_ms = milliseconds_value("failed_ttl_ms", &nv.value)?;
                            attrs.record_duration("failed_ttl_ms", failed_ttl_ms, nv.value.span());
                            attrs.failed_ttl_ms = Some(ResultTtl::from_ms(failed_ttl_ms));
                        }
                        "retry_delay_ms" => {
                            let retry_delay_ms = milliseconds_value("retry_delay_ms", &nv.value)?;
                            attrs.record_duration("retry_delay_ms", retry_delay_ms, nv.value.span());
                            attrs.retry_delay_ms = Some(retry_delay_ms);
                        }
                        "max_backoff_ms" => {
                            let max_backoff_ms = milliseconds_value("max_backoff_ms", &nv.value)?;
                            if max_backoff_ms == 0 {
                                return Err(err(&nv.value, "max_backoff_ms must be greater than zero"));
                            }
                            attrs.record_duration("max_backoff_ms", max_backoff_ms, nv.value.span());
                            attrs.max_backoff_ms = Some(max_backoff_ms);
                            max_backoff_span = Some(nv.path.span());
                        }
                        "priority" => attrs.priority = Some(priority_value(&nv.value)?),
                        "revision" => {
                            if mode == AttrMode::Job {
                                return Err(err(&nv.path, "`revision` is only valid on #[pgqueue::cron]"));
                            }
                            attrs.revision = Some(revision_value(&nv.value)?)
                        }
                        other => {
                            return Err(err(
                                &nv.path,
                                &format!("unknown attribute `{other}`; expected one of: {}", mode.expected_keys()),
                            ));
                        }
                    }
                }
                other => {
                    return Err(err(other, "expected `key = value`; see the pgqueue::job docs"));
                }
            }
        }
        if let Some(span) = max_backoff_span
            && attrs.retry_delay_ms.unwrap_or_default() == 0
        {
            return Err(syn::Error::new(span, "exponential backoff requires retry_delay_ms greater than zero"));
        }
        Ok(attrs)
    }
}

fn err(spanned: &impl quote::ToTokens, message: &str) -> syn::Error {
    syn::Error::new_spanned(spanned, message)
}

/// Splits a required leading string literal (the cron expression) off an
/// attribute token stream, returning it and the remaining `key = value` args.
pub(crate) fn split_leading_str(tokens: TokenStream) -> syn::Result<(syn::LitStr, TokenStream)> {
    const EXPECTED: &str = "expected a cron expression string as the first argument, \
                            e.g. #[pgqueue::cron(\"0 * * * *\")]";

    struct Leading {
        lit: syn::LitStr,
        rest: TokenStream,
    }
    impl syn::parse::Parse for Leading {
        fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
            let lit: syn::LitStr = input.parse().map_err(|e| syn::Error::new(e.span(), EXPECTED))?;
            let rest = if input.is_empty() {
                TokenStream::new()
            } else {
                input.parse::<Token![,]>()?;
                input.parse()?
            };
            Ok(Leading { lit, rest })
        }
    }

    if tokens.is_empty() {
        return Err(syn::Error::new(Span::call_site(), EXPECTED));
    }
    let leading: Leading = syn::parse2(tokens)?;
    Ok((leading.lit, leading.rest))
}

/// Strips the invisible groups a `macro_rules!` substitution arrives in.
///
/// A metavariable expands to a `Delimiter::None` group, which syn keeps as
/// `Expr::Group`; its fast path unwraps one only when the substitution is the
/// whole expression being parsed. So `#[pgqueue::job(timeout_ms = $ms,
/// max_attempts = 3)]` reached these helpers as a group and was rejected while
/// `#[pgqueue::job(max_attempts = 3, timeout_ms = $ms)]` compiled — the same
/// value accepted or refused by nothing but its position in the attribute.
fn ungroup(expr: &Expr) -> &Expr {
    let mut expr = expr;
    while let Expr::Group(group) = expr {
        expr = &group.expr;
    }
    expr
}

fn string_value(expr: &Expr) -> syn::Result<String> {
    let expr = ungroup(expr);
    if let Expr::Lit(ExprLit { lit: Lit::Str(s), .. }) = expr {
        return Ok(s.value());
    }
    Err(err(expr, "expected a string literal"))
}

/// The value of an unsuffixed integer literal, widened so that the key which
/// owns it does its own range check.
///
/// Parsing straight into each key's narrow type turned an out-of-range *value*
/// into an out-of-range *literal*, and which message you got depended on which
/// end of the range the value fell off: `priority = 32768` was "integer literal
/// is out of range" while `priority = -32769` named the 16-bit bound, and
/// `max_attempts = 4294967296` was a malformed literal while
/// `max_attempts = 2147483647` named the attempt bound. Every one of those
/// literals is well-formed; it is the value that is not allowed. This is the
/// defect `unsigned_value` below documents for negatives, one bound over.
///
/// `i128` covers every key's range with the sign the user wrote, so only a
/// literal too large for any integer type this crate can parse reaches the
/// remaining message — where it really is the literal that is out of range.
fn int_value(expr: &Expr) -> syn::Result<i128> {
    let expr = ungroup(expr);
    if let Expr::Lit(ExprLit { lit: Lit::Int(int), .. }) = expr {
        if !int.suffix().is_empty() {
            return Err(err(expr, "expected an unsuffixed integer literal"));
        }
        return int.base10_digits().parse().map_err(|_| err(expr, "integer literal is out of range"));
    }
    Err(err(expr, "expected an unsuffixed integer literal"))
}

/// Parses an attribute value for a key that has no negative encoding.
///
/// `syn` folds a leading `-` into the literal only when the literal is the
/// attribute's last token and leaves a unary negation around it otherwise — the
/// same positional hazard `ungroup` and `priority_value` were written to
/// neutralise. `int_value` inherited it: `max_attempts = -1` was rejected as
/// "integer literal is out of range" (the folded literal failed to parse as an
/// unsigned) but `max_attempts = -1, timeout_ms = 5` as "expected an unsuffixed
/// integer literal" (the negation is not a literal at all). One input, two
/// messages, neither of them the reason. Both spellings land here instead.
fn unsigned_value(expr: &Expr) -> syn::Result<i128> {
    let expr = ungroup(expr);
    let negative = match expr {
        Expr::Unary(ExprUnary { op: UnOp::Neg(_), .. }) => true,
        Expr::Lit(ExprLit { lit: Lit::Int(int), .. }) => int.base10_digits().starts_with('-'),
        _ => false,
    };
    if negative {
        return Err(err(expr, "expected a non-negative integer literal"));
    }
    int_value(expr)
}

fn revision_value(expr: &Expr) -> syn::Result<u64> {
    u64::try_from(unsigned_value(expr)?)
        .ok()
        .filter(|revision| *revision <= i64::MAX as u64)
        .ok_or_else(|| err(expr, "revision must fit in PostgreSQL bigint"))
}

fn priority_value(expr: &Expr) -> syn::Result<i16> {
    let expr = ungroup(expr);
    let out_of_range = || err(expr, "priority must fit in a signed 16-bit integer");
    // `-1` parses as a unary negation around a literal.
    if let Expr::Unary(ExprUnary { op: UnOp::Neg(_), expr: inner, .. }) = expr {
        let magnitude = int_value(inner)?;
        if magnitude == i16::MAX as i128 + 1 {
            return Ok(i16::MIN);
        }
        let magnitude = i16::try_from(magnitude).map_err(|_| out_of_range())?;
        return Ok(-magnitude);
    }
    i16::try_from(int_value(expr)?).map_err(|_| out_of_range())
}

/// Parses a millisecond literal for `key`. The upper bound is deliberately
/// *not* checked here: this crate cannot depend on `pgqueue` (dependency
/// cycle), so a copy of the limit would silently drift. Generated code asserts
/// each value against `pgqueue::__private::MAX_DURATION_MS` instead, which
/// fails the build.
///
/// A value too large for the `u64` that limit is typed as cannot be handed to
/// that assertion — but it is over the limit whatever the limit happens to be,
/// so it is refused here with the words the assertion would have used, rather
/// than as the malformed literal it is not.
fn milliseconds_value(key: &str, expr: &Expr) -> syn::Result<u64> {
    u64::try_from(unsigned_value(expr)?)
        .map_err(|_| err(expr, &format!("{key} exceeds pgqueue's maximum supported duration")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn parse(tokens: TokenStream) -> syn::Result<JobAttrs> {
        JobAttrs::parse(tokens, AttrMode::Cron)
    }

    #[test]
    fn test_empty_attrs_are_all_none() {
        let attrs = parse(quote!()).unwrap();
        assert!(attrs.name.is_none());
        assert!(attrs.max_attempts.is_none());
        assert!(attrs.timeout_ms.is_none());
        assert!(attrs.result_ttl_ms.is_none());
        assert!(attrs.failed_ttl_ms.is_none());
        assert!(attrs.retry_delay_ms.is_none());
        assert!(attrs.max_backoff_ms.is_none());
        assert!(attrs.priority.is_none());
        assert!(attrs.revision.is_none());
    }

    #[test]
    fn test_full_attribute_set_parses_every_supported_option() {
        let attrs = parse(quote!(
            name = "custom",
            max_attempts = 3,
            timeout_ms = 30_000,
            result_ttl_ms = 3_600_000,
            failed_ttl_ms = 86_400_000,
            retry_delay_ms = 500,
            max_backoff_ms = 120_000,
            priority = -1,
            revision = 2
        ))
        .unwrap();
        assert_eq!(attrs.name.as_deref(), Some("custom"));
        assert_eq!(attrs.max_attempts, Some(3));
        assert_eq!(attrs.timeout_ms, Some(Some(30_000)));
        assert!(matches!(attrs.result_ttl_ms, Some(ResultTtl::ForMs(3_600_000))));
        assert!(matches!(attrs.failed_ttl_ms, Some(ResultTtl::ForMs(86_400_000))));
        assert_eq!(attrs.retry_delay_ms, Some(500));
        assert_eq!(attrs.max_backoff_ms, Some(120_000));
        assert_eq!(attrs.priority, Some(-1));
        assert_eq!(attrs.revision, Some(2));
    }

    /// A `macro_rules!` substitution arrives as a `Delimiter::None` group, and
    /// syn's fast path unwraps one only when it is the whole expression being
    /// parsed — so a metavariable was accepted in the last attribute position
    /// and rejected in every other. Each value here is written somewhere other
    /// than last.
    #[test]
    fn test_metavariable_groups_parse_in_every_attribute_position() {
        fn grouped(tokens: TokenStream) -> TokenStream {
            TokenStream::from(proc_macro2::TokenTree::Group(proc_macro2::Group::new(
                proc_macro2::Delimiter::None,
                tokens,
            )))
        }
        let name = grouped(quote!("custom"));
        let ms = grouped(quote!(30_000));
        let magnitude = grouped(quote!(7));
        let revision = grouped(quote!(4));
        let attrs = parse(quote!(
            name = #name,
            timeout_ms = #ms,
            priority = -#magnitude,
            revision = #revision,
            max_attempts = 3
        ))
        .unwrap();
        assert_eq!(attrs.name.as_deref(), Some("custom"));
        assert_eq!(attrs.timeout_ms, Some(Some(30_000)));
        assert_eq!(attrs.priority, Some(-7));
        assert_eq!(attrs.revision, Some(4));

        // `$p:expr` binding a negative literal groups the negation itself.
        let negative = grouped(quote!(-9));
        assert_eq!(parse(quote!(priority = #negative, max_attempts = 3)).unwrap().priority, Some(-9));

        // A group that holds no literal is still rejected, with the diagnostic
        // spanning what the caller wrote.
        let text = grouped(quote!(some_ident));
        let error = parse(quote!(timeout_ms = #text, max_attempts = 3)).unwrap_err();
        assert!(error.to_string().contains("unsuffixed integer literal"), "{error}");
    }

    #[test]
    fn test_millisecond_values_are_not_scaled() {
        assert_eq!(parse(quote!(timeout_ms = 500)).unwrap().timeout_ms, Some(Some(500)));
        assert!(matches!(parse(quote!(result_ttl_ms = 500)).unwrap().result_ttl_ms, Some(ResultTtl::ForMs(500))));
    }

    #[test]
    fn test_max_attempts_reserves_shutdown_refund_headroom() {
        assert!(parse(quote!(max_attempts = 2147483646)).is_ok());
        let error = parse(quote!(max_attempts = 2147483647)).unwrap_err();
        assert!(error.to_string().contains("2147483646"), "{error}");
    }

    /// The only literal left that is genuinely out of range is one too large
    /// for the widest integer this crate parses into; every value below that is
    /// range-checked by the key that owns it.
    #[test]
    fn test_integer_attributes_report_overflow_as_out_of_range() {
        let error = parse(quote!(timeout_ms = 1701411834604692317316873037158841057280)).unwrap_err();
        assert_eq!(error.to_string(), "integer literal is out of range");
    }

    /// Parsing straight into each key's own type made an out-of-range *value*
    /// report as an out-of-range *literal*, so the message depended on which
    /// end of the range the value fell off: `priority = -32769` named the
    /// 16-bit bound while `priority = 32768` — just as well-formed a literal —
    /// was refused as malformed. Every key names its own bound instead,
    /// whichever side it is missed on and wherever in the attribute it is
    /// written.
    #[test]
    fn test_out_of_range_values_name_the_bound_of_the_key_they_were_written_for() {
        for (tokens, message) in [
            (quote!(priority = 32768), "priority must fit in a signed 16-bit integer"),
            (quote!(priority = 32768, name = "n"), "priority must fit in a signed 16-bit integer"),
            (quote!(priority = -32769), "priority must fit in a signed 16-bit integer"),
            (quote!(priority = 4294967296), "priority must fit in a signed 16-bit integer"),
            (quote!(max_attempts = 4294967296), "max_attempts must be between 1 and 2147483646"),
            (quote!(max_attempts = 2147483647), "max_attempts must be between 1 and 2147483646"),
            (quote!(max_attempts = 0), "max_attempts must be between 1 and 2147483646"),
            (quote!(revision = 9223372036854775808), "revision must fit in PostgreSQL bigint"),
            (quote!(revision = 18446744073709551616), "revision must fit in PostgreSQL bigint"),
        ] {
            let error = parse(tokens.clone()).expect_err(&format!("should reject: {tokens}"));
            assert_eq!(error.to_string(), message, "for: {tokens}");
        }
    }

    /// A millisecond value the deferred `MAX_DURATION_MS` assertion cannot even
    /// be handed — it does not fit the `u64` that limit is typed as — is over
    /// that limit whatever the limit is, so it is refused in the same words
    /// rather than as a malformed literal.
    #[test]
    fn test_millisecond_values_above_the_deferred_bound_report_the_duration_limit() {
        for key in [
            "timeout_ms",
            "result_ttl_ms",
            "failed_ttl_ms",
            "retry_delay_ms",
            "max_backoff_ms",
        ] {
            let ident = syn::Ident::new(key, Span::call_site());
            let error = parse(quote!(#ident = 18446744073709551616)).unwrap_err();
            assert_eq!(error.to_string(), format!("{key} exceeds pgqueue's maximum supported duration"));
        }
    }

    #[test]
    fn test_revision_is_rejected_for_job_mode_where_it_is_written() {
        let error = JobAttrs::parse(quote!(revision = 1), AttrMode::Job).unwrap_err();
        assert_eq!(error.to_string(), "`revision` is only valid on #[pgqueue::cron]");
        // Cron still accepts it; `tests/macros/fail.stderr` pins the span this
        // error now carries (the `revision` key, not the whole attribute).
        assert_eq!(JobAttrs::parse(quote!(revision = 1), AttrMode::Cron).unwrap().revision, Some(1));
    }

    #[test]
    fn test_unknown_attribute_suggests_only_the_keys_that_mode_accepts() {
        let error = JobAttrs::parse(quote!(bogus = 1), AttrMode::Job).unwrap_err();
        assert!(error.to_string().ends_with("priority"), "{error}");
        assert!(!error.to_string().contains("revision"), "{error}");

        let error = JobAttrs::parse(quote!(bogus = 1), AttrMode::Cron).unwrap_err();
        assert!(error.to_string().ends_with("priority, revision"), "{error}");
    }

    #[test]
    fn test_removed_bare_backoff_points_to_max_backoff_ms() {
        let error = parse(quote!(backoff)).unwrap_err();
        assert_eq!(error.to_string(), "`backoff` is not supported; use `max_backoff_ms = ...`");
    }

    #[test]
    fn test_zero_values_parse_to_expected_values() {
        assert_eq!(parse(quote!(timeout_ms = 0)).unwrap().timeout_ms, Some(None));
        assert!(matches!(parse(quote!(result_ttl_ms = 0)).unwrap().result_ttl_ms, Some(ResultTtl::Delete)));
        assert!(matches!(parse(quote!(failed_ttl_ms = 0)).unwrap().failed_ttl_ms, Some(ResultTtl::Delete)));
        assert_eq!(parse(quote!(priority = 7)).unwrap().priority, Some(7));
        assert_eq!(parse(quote!(priority = -32768)).unwrap().priority, Some(i16::MIN));
        // The same value written anywhere but last arrives as a negation
        // around `32768`, which has no positive `i16` to negate.
        assert_eq!(parse(quote!(priority = -32768, max_attempts = 2)).unwrap().priority, Some(i16::MIN));
    }

    #[test]
    fn test_attributes_reject_invalid_input() {
        for tokens in [
            quote!(bogus = 1),
            quote!(max_attempts = "three"),
            quote!(max_attempts = "3"),
            quote!(max_attempts = 0),
            quote!(max_attempts = 2147483648),
            quote!(timeout = 30_000),
            quote!(ttl = 60_000),
            quote!(ttl_ms = 60_000),
            quote!(backoff_max_ms = 1),
            quote!(timeout_ms = "30000"),
            quote!(timeout_ms = [1]),
            quote!(name = 42),
            quote!(priority = "high"),
            quote!(priority = -32769),
            // A negation `syn` cannot fold into the literal, because the
            // literal is not the attribute's last token: the magnitude is one
            // past what an `i16` holds.
            quote!(priority = -32769, max_attempts = 2),
            quote!(timeout_ms = 30u64),
            quote!(heartbeat = 1_000),
            quote!(heartbeat_ms = 1_000),
            quote!(retry_delay = 500),
            quote!(backoff = 1),
            quote!(backoff),
            quote!(retry_delay_ms = 0, max_backoff_ms = 1),
            quote!(retry_delay_ms = 1, max_backoff_ms = 0),
            quote!(name = ""),
            quote!(max_attempts),
            quote!(timeout_ms = 1, timeout_ms = 2),
            quote!(timeout_ms = 99999999999999999999999),
        ] {
            assert!(parse(tokens.clone()).is_err(), "should reject: {tokens}");
        }
    }

    /// `syn` folds a leading `-` into the literal only when the literal is the
    /// attribute's last token, so a negative on an unsigned key arrived as a
    /// negative literal in one position and a unary negation in the other, and
    /// was refused with a different, equally misleading message each time
    /// ("integer literal is out of range" / "expected an unsuffixed integer
    /// literal"). Both spellings must say the value cannot be negative.
    #[test]
    fn test_attributes_reject_a_negative_value_the_same_way_in_either_position() {
        for key in [
            "max_attempts",
            "timeout_ms",
            "result_ttl_ms",
            "failed_ttl_ms",
            "retry_delay_ms",
            "max_backoff_ms",
        ] {
            let key = syn::Ident::new(key, Span::call_site());
            for tokens in [
                // Last token: `syn` folds the sign into the literal.
                quote!(#key = -1),
                // Anywhere else: a unary negation around the literal.
                quote!(#key = -1, name = "n"),
            ] {
                let message = parse(tokens.clone()).expect_err(&format!("should reject: {tokens}")).to_string();
                assert_eq!(message, "expected a non-negative integer literal", "for: {tokens}");
            }
        }
        for tokens in [quote!(revision = -1), quote!(revision = -1, name = "n")] {
            let message = parse(tokens.clone()).expect_err(&format!("should reject: {tokens}")).to_string();
            assert_eq!(message, "expected a non-negative integer literal", "for: {tokens}");
        }
    }

    /// Durations beyond pgqueue's supported range parse here and are rejected
    /// by the assertion the expansion emits against
    /// `pgqueue::__private::MAX_DURATION_MS`, so the limit is not duplicated.
    #[test]
    fn test_attributes_defer_the_duration_bound_to_generated_code() {
        assert_eq!(parse(quote!(timeout_ms = 3153600000001)).unwrap().timeout_ms, Some(Some(3_153_600_000_001)));
    }

    /// The deferred bound check has to underline the literal that broke it, not
    /// the whole attribute, so every millisecond value is kept with its span.
    /// `tests/macros/fail.stderr` pins the resulting diagnostic.
    #[test]
    fn test_every_millisecond_literal_is_recorded_with_its_span() {
        let attrs =
            parse(quote!(timeout_ms = 1, result_ttl_ms = 2, failed_ttl_ms = 5, retry_delay_ms = 3, max_backoff_ms = 4))
                .unwrap();
        let recorded: Vec<_> = attrs.durations.iter().map(|(key, ms, _)| (*key, *ms)).collect();
        assert_eq!(
            recorded,
            vec![
                ("timeout_ms", 1),
                ("result_ttl_ms", 2),
                ("failed_ttl_ms", 5),
                ("retry_delay_ms", 3),
                ("max_backoff_ms", 4),
            ]
        );
    }

    /// Zero is a sentinel — no timeout, delete the result immediately — rather
    /// than a duration, so it needs no bound check.
    #[test]
    fn test_sentinel_zero_durations_are_not_recorded_for_the_bound_check() {
        let attrs = parse(quote!(timeout_ms = 0, result_ttl_ms = 0, failed_ttl_ms = 0, retry_delay_ms = 0)).unwrap();
        assert!(attrs.durations.is_empty(), "{:?}", attrs.durations);
    }
}
