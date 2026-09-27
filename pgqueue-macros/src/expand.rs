//! The `#[pgqueue::job]` / `#[pgqueue::cron]` expansions, kept as pure token
//! transforms so they can be unit-tested without compiling user code.

use cronexpr::{FallbackTimezoneOption, ParseOptions};
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, format_ident, quote, quote_spanned};
use syn::ext::IdentExt;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit::Visit;
use syn::{AttrStyle, Attribute, FnArg, Ident, ItemFn, Meta, ReturnType, Token, Type};

use crate::attrs::{AttrMode, JobAttrs, ResultTtl, split_leading_str};

/// Which attribute is expanding; drives the signature contract and generated
/// registration marker.
enum Mode {
    /// `#[pgqueue::job]`: first param is the payload, the rest are extractors.
    Job,
    /// `#[pgqueue::cron("...")]`: every param is an extractor; the payload is
    /// fixed to `()` and the schedule is baked into the job type.
    Cron { schedule: String },
}

impl Mode {
    fn attr_name(&self) -> &'static str {
        match self {
            Mode::Job => "#[pgqueue::job]",
            Mode::Cron { .. } => "#[pgqueue::cron]",
        }
    }

    fn attr_mode(&self) -> AttrMode {
        match self {
            Mode::Job => AttrMode::Job,
            Mode::Cron { .. } => AttrMode::Cron,
        }
    }
}

/// Expands `#[pgqueue::job(...)]`.
pub(crate) fn expand_job(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    // `revision` is rejected during parsing, where the key's own span is still
    // available, rather than here against `Span::call_site()`.
    let attrs = JobAttrs::parse(attr, AttrMode::Job)?;
    expand(Mode::Job, attrs, item)
}

/// Expands `#[pgqueue::cron("expr", ...)]`, parsing the cron expression at
/// compile time with the same parser the worker uses at runtime.
pub(crate) fn expand_cron(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let (expr, rest) = split_leading_str(attr)?;
    let schedule = expr.value();
    if schedule.split_ascii_whitespace().count() != 5 {
        return Err(syn::Error::new_spanned(
            &expr,
            format!("invalid cron expression {schedule:?}: expected 5 whitespace-separated fields"),
        ));
    }
    let mut options = ParseOptions::default();
    options.fallback_timezone_option = FallbackTimezoneOption::UTC;
    let crontab = cronexpr::parse_crontab_with(&schedule, options)
        .map_err(|e| syn::Error::new_spanned(&expr, format!("invalid cron expression {schedule:?}: {e}")))?;
    reject_a_schedule_that_never_fires(&expr, &schedule, &crontab)?;
    let attrs = JobAttrs::parse(rest, AttrMode::Cron)?;
    expand(Mode::Cron { schedule }, attrs, item)
}

/// One Gregorian cycle, after which a five-field schedule's calendar repeats
/// exactly. A schedule with no occurrence inside one has none at all.
const CRON_CYCLE_SECONDS: i64 = 146_097 * 86_400;

/// How far each probe advances. `find_next` searches four years from the
/// instant it is given and fails beyond that, so the step stays inside that
/// window and overlaps it.
const CRON_SEARCH_STEP_SECONDS: i64 = 3 * 365 * 86_400;

/// Refuses a schedule that parses but can never occur — `"0 0 30 2 *"`, or a
/// `31` in a thirty-day month.
///
/// Syntax is not the whole contract. Such an expression compiled, and then
/// `JobCronEntry::next_occurrence` answered `Error::Config` at runtime, which
/// the worker's reconciliation classifies as a *permanent* rejection: the cron
/// is disabled for the process's whole life and only `WorkerComponent::Scheduler`
/// says so. A typo in a calendar is exactly what a macro that already parses the
/// expression at compile time should catch at compile time.
///
/// Stepped, not a single `find_next` from a fixed epoch: `cronexpr` supports
/// `#` nth-weekday, and a legitimate schedule can outrun `find_next`'s four-year
/// window — `"0 0 * 2 1#5"` (fifth Monday of February) first fires in 1988, so a
/// one-shot probe from 1970 would reject it. This mirrors `CronSchedule::
/// next_after` in the runtime crate, which is what makes the two agree by
/// construction: a schedule this accepts is one that resolves there.
fn reject_a_schedule_that_never_fires(
    expr: &syn::LitStr,
    schedule: &str,
    crontab: &cronexpr::Crontab,
) -> syn::Result<()> {
    let mut cursor: i64 = 0;
    while cursor <= CRON_CYCLE_SECONDS {
        let fires =
            cronexpr::MakeTimestamp::from_second(cursor).is_ok_and(|timestamp| crontab.find_next(timestamp).is_ok());
        if fires {
            return Ok(());
        }
        cursor += CRON_SEARCH_STEP_SECONDS;
    }
    Err(syn::Error::new_spanned(expr, format!("invalid cron expression {schedule:?}: schedule never fires")))
}

/// Expands the annotated function into:
/// 1. a unit struct named after the function (the job's handle),
/// 2. `::job(args)` (or zero-arg `::job()` for cron) — the typed enqueue
///    constructor,
/// 3. `::call(...)` — a direct invoker preserving the original signature,
/// 4. a `JobType` impl carrying name/config/schedule and the erased handler.
fn expand(mode: Mode, attrs: JobAttrs, item: TokenStream) -> syn::Result<TokenStream> {
    let func: ItemFn = syn::parse2(item)?;
    validate(&mode, &func)?;
    let runtime = runtime_crate_path();

    let vis = &func.vis;
    let ident = &func.sig.ident;
    let name = job_name(&mode, &attrs, ident)?;
    let ItemAttrs { api_attrs, lint_attrs, deprecated } = ItemAttrs::split(&func.attrs);

    let mut types = Vec::new();
    for input in &func.sig.inputs {
        match input {
            FnArg::Typed(pat) => types.push((*pat.ty).clone()),
            FnArg::Receiver(receiver) => {
                return Err(syn::Error::new_spanned(
                    receiver,
                    format!("{} functions cannot take self", mode.attr_name()),
                ));
            }
        }
    }
    // Job mode: first param is the payload, the rest are extractors.
    // Cron mode: every param is an extractor; the payload is `()`.
    let (payload_ty, extractor_tys): (Type, &[Type]) = match mode {
        Mode::Job => (types[0].clone(), &types[1..]),
        Mode::Cron { .. } => (syn::parse_quote!(()), &types[..]),
    };

    let ret_ty: Type = match &func.sig.output {
        ReturnType::Default => syn::parse_quote!(()),
        ReturnType::Type(_, ty) => (**ty).clone(),
    };
    // Both `IntoJobResult` obligations are spanned on the return type the user
    // wrote, not on `Span::call_site()`: returning a bare value from a handler
    // is the common mistake, and with call-site spans its `E0277: the trait
    // bound `u32: IntoJobResult` is not satisfied` underlined `#[pgqueue::job]`
    // instead of the `-> u32` that caused it. The extractor bounds already land
    // on the user's type this way.
    let output_ty = quote_spanned! {ret_ty.span()=>
        <#ret_ty as #runtime::__private::IntoJobResult>::Output
    };
    // The handler's value is bound first so the argument this call reports on
    // is a single token carrying the return type's span; built inline, the
    // argument expression mixes user and generated spans and rustc collapses it
    // back to the attribute.
    //
    // Binding and use share one `Ident`, because a span carries a syntax context
    // as well as a location. A return type substituted from a `macro_rules!`
    // caller through a `tt` or `ident` fragment — neither of which is wrapped in
    // an invisible group, unlike `ty` — puts the caller's context on this span,
    // so emitting the use through `quote_spanned!` while the `let` below stayed
    // on `quote!`'s call site split the two apart: `error[E0425]: cannot find
    // value `__result` in this scope`, naming an identifier the user never
    // wrote. `tests/macros/pass_macro_rules.rs` compiles all three shapes.
    let result_binding = Ident::new("__result", ret_ty.span());
    let encode_result = quote_spanned! {ret_ty.span()=>
        #runtime::__private::encode_result(#result_binding)
    };

    // `call()` forwards positionally with fresh names (original patterns may
    // be `_` or destructurings).
    let call_names: Vec<_> = (0..types.len()).map(|i| format_ident!("__arg{i}", span = Span::call_site())).collect();
    let call_params: Vec<_> = call_names.iter().zip(&types).map(|(name, ty)| quote!(#name: #ty)).collect();

    // The original function, moved verbatim (body, params, output, *and name*)
    // into the anonymous const. Keeping the user's identifier matters: an
    // attribute left on it can derive from the name — `#[tracing::instrument]`
    // takes its span name from the ident, and renaming the function labelled
    // every job's telemetry with the expansion's private placeholder instead of
    // the handler. Inside `const _: () = { ... }` the function only occupies the
    // value namespace of a nested scope, so it cannot clash with the
    // module-level struct of the same name.
    let mut inner = func.clone();
    inner.vis = syn::Visibility::Inherited;
    inner.attrs.retain(|attr| {
        // `syn` hoists a body's *inner* attributes (`#![allow(...)]`, `//!`
        // docs) into `ItemFn::attrs`, and re-emits them inside the braces. They
        // annotate the body the user wrote, not the job, so they stay here —
        // stripping an inner `//!` deleted documentation the user wrote for the
        // function's own body.
        if matches!(attr.style, AttrStyle::Inner(_)) {
            return true;
        }
        let path = attr.path();
        !path.is_ident("doc") && !path.is_ident("deprecated")
    });
    // `#[expect(...)]` is lowered here exactly as `struct_lint_attr` lowers it
    // for the struct and the impls. One written item becomes several, and the
    // lint fires on whichever of them it applies to: an item-level lint
    // (`missing_docs`) on the struct, a body or signature lint here. Every
    // other copy would then report as unfulfilled through no fault of the
    // user's, so no copy may keep the expectation.
    //
    // Suppressing that with an inserted `#[allow(unfulfilled_lint_expectations)]`
    // is not an option: under a crate-level
    // `#![forbid(unfulfilled_lint_expectations)]` the expansion's own `allow`
    // is `error[E0453]`, so writing `#[expect(...)]` on a job broke the build
    // where the equivalent plain function compiled.
    for attr in &mut inner.attrs {
        if matches!(attr.style, AttrStyle::Outer)
            && attr.path().is_ident("expect")
            && let Meta::List(list) = &attr.meta
        {
            let lints = &list.tokens;
            *attr = syn::parse_quote!(#[allow(#lints)]);
        }
    }

    let config_setters = config_setters(&attrs, &runtime);
    // `mut` only when something actually assigns, so the expansion needs no
    // blanket `#[allow(unused_mut)]` — which is itself an error under a crate
    // that writes `#![forbid(unused_mut)]`.
    let config_mut = if config_setters.is_empty() { quote!() } else { quote!(mut) };
    let duration_asserts = duration_bound_assertions(&attrs, &runtime);

    let extractor_names: Vec<_> =
        (0..extractor_tys.len()).map(|i| format_ident!("__ext{i}", span = Span::call_site())).collect();
    let extractions: Vec<_> = extractor_names
        .iter()
        .zip(extractor_tys)
        .map(|(name, ty)| {
            quote! {
                let #name = <#ty as #runtime::FromJobContext>::from_context(&__ctx)?;
            }
        })
        .collect();

    // Cron handlers take no payload, so the erased call skips `__args`.
    //
    // Every binding the expansion introduces is `__`-prefixed, including this
    // one: a bare `args` is a *pattern*, so an in-scope unit struct or const of
    // that name would silently reinterpret it as a path pattern and break the
    // job with a diagnostic that names nothing in the user's source.
    let (job_ctor, invoke, definition_impl) = match &mode {
        Mode::Job => (
            quote! {
                /// Builds a typed enqueue request for this job
                /// (pass it to `Queue::enqueue`).
                #vis fn job(__args: #payload_ty) -> #runtime::JobBuilder<#ident> {
                    #runtime::JobBuilder::new(__args)
                }
            },
            quote!(#ident(__args #(, #extractor_names)*)),
            quote! {
                impl #runtime::JobDefinition for #ident {}
            },
        ),
        Mode::Cron { schedule } => {
            let revision = attrs.revision.unwrap_or(0);
            (
                quote! {
                    /// Builds an enqueue request for a one-off, out-of-schedule
                    /// run of this cron job (pass it to `Queue::enqueue`).
                    #vis fn job() -> #runtime::JobBuilder<#ident> {
                        #runtime::JobBuilder::new(())
                    }
                },
                quote!(#ident(#(#extractor_names),*)),
                quote! {
                    impl #runtime::CronDefinition for #ident {
                        const SCHEDULE: &'static str = #schedule;
                        const CRON_REVISION: u64 = #revision;
                    }
                },
            )
        }
    };
    let decode_args = match &mode {
        Mode::Job => {
            // The payload's `DeserializeOwned` obligation has to land on the
            // payload type, for the same reason both `IntoJobResult`
            // obligations are spanned on the return type above: a payload
            // missing `#[derive(Serialize, Deserialize)]` reported its two
            // `serde` bounds against the type the user wrote and this third one
            // against `#[pgqueue::job]` — one mistake, three errors, pointing at
            // two different places.
            //
            // Naming the type in the turbofish rather than inferring it from a
            // `let __args: #payload_ty` annotation is what moves it. This
            // obligation comes from the callee's *return* type, so rustc reports
            // it on the call expression — whose span starts at the generated
            // crate path and therefore collapsed back to the attribute — unless
            // the argument that carries the bound is written explicitly, where
            // it is the user's own token.
            quote! {
                let __args =
                    #runtime::__private::decode_payload::<#payload_ty>(&__ctx.job().payload)?;
            }
        }
        // The payload is always `()`/null for cron jobs; nothing to decode.
        Mode::Cron { .. } => quote!(),
    };

    // Both associated types carry the user's own span, for the reason the
    // `IntoJobResult` and `DeserializeOwned` obligations above do — and here it
    // is a hard error rather than a lint. A payload or return type less visible
    // than the job puts a private type in the public `JobType` impl, and
    // `error[E0446]: private type `Private` in public interface` then underlined
    // `#[pgqueue::job]` and named nothing the user could act on. It is not
    // suppressible either: the lint form of this is `private_interfaces`, but an
    // associated type in a public trait impl raises the hard error, so
    // `#[allow(private_interfaces)]` does not reach it. Spanned on the parameter
    // and the return type, the diagnostic points at the thing to change.
    let args_assoc = quote_spanned! {payload_ty.span()=> type Args = #payload_ty; };
    let output_assoc = quote_spanned! {ret_ty.span()=> type Output = #output_ty; };

    // Only the generated `impl` blocks name the — possibly `#[deprecated]` —
    // job type, so the allow they need stops there. Spanning the whole
    // anonymous const also covered `#inner`, the user's function body, and
    // silently swallowed every deprecation lint inside every job handler.
    let allow_deprecated = if deprecated { quote!(#[allow(deprecated)]) } else { quote!() };
    // The inherent and JobType impls re-mention the payload, extractor and
    // return types with the *user's* spans, so lints fire on them as if the user
    // had written them. Every impl also names the generated job type. The
    // user's lint control therefore has to reach all three impls. The lowered
    // copies are used for the same reasons as on the struct (see
    // `struct_lint_attr`): `#[forbid(deprecated)]` would collide with the
    // expansion's `#allow_deprecated`, and an `#[expect(...)]` would be
    // unfulfilled on whichever generated item does not emit the lint. The
    // expansion's own allow goes last, so it wins for a deprecated job.
    let impl_attrs = quote! {
        #(#lint_attrs)*
        #allow_deprecated
    };

    // Spanned on the handler body, so the most common real handler mistake —
    // holding a non-`Send` value (an `Rc`, a `MutexGuard`) across an `.await` —
    // underlines that body rather than `#[pgqueue::job]`. At `call_site()` the
    // primary span of "future cannot be sent between threads safely" was the
    // attribute, which is the one diagnostic here that named none of the user's
    // own tokens.
    let erased_future = quote_spanned! {func.block.span()=>
        ::std::boxed::Box::pin(async move {
            #decode_args
            #(#extractions)*
            let #result_binding = #invoke.await;
            #encode_result
        })
    };

    Ok(quote! {
        #(#api_attrs)*
        #[allow(non_camel_case_types)]
        #[derive(::core::clone::Clone, ::core::marker::Copy, ::core::fmt::Debug)]
        #vis struct #ident;

        #(#duration_asserts)*

        const _: () = {
            #inner

            #impl_attrs
            impl #ident {
                #job_ctor

                /// Invokes the underlying handler function directly,
                /// bypassing the queue — useful in unit tests.
                #vis async fn call(#(#call_params),*) -> #ret_ty {
                    #ident(#(#call_names),*).await
                }
            }

            #impl_attrs
            impl #runtime::JobType for #ident {
                #args_assoc
                #output_assoc
                const NAME: &'static str = #name;

                fn config() -> #runtime::JobConfig {
                    let #config_mut __config =
                        <#runtime::JobConfig as ::core::default::Default>::default();
                    #(#config_setters)*
                    __config
                }

                fn erased() -> #runtime::__private::TypeErasedJobHandler {
                    #runtime::__private::TypeErasedJobHandler::new::<Self>(|__ctx| {
                        #erased_future
                    })
                }
            }

            #impl_attrs
            #definition_impl
        };
    })
}

/// How the annotated function's own attributes are routed across the items the
/// expansion produces.
struct ItemAttrs {
    /// Attributes the generated struct carries.
    api_attrs: Vec<TokenStream>,
    /// The lint-control subset of `api_attrs`, which the generated `impl`
    /// blocks carry too. Docs and `#[deprecated]` are deliberately *not* in
    /// here: a second `#[deprecated]` would make the impls' own mentions of the
    /// job type warn, and a doc comment belongs to the one item that is the
    /// job's public face.
    lint_attrs: Vec<TokenStream>,
    /// Whether the function is `#[deprecated]`.
    deprecated: bool,
}

impl ItemAttrs {
    /// Docs and `#[deprecated]` describe the job, so they *move* to the struct.
    /// Lint control is *copied* there and onto the generated impls: the struct
    /// is the item the user's `#[allow(missing_docs)]` was written for —
    /// item-level lints fire on it, not on the hidden function — the impls
    /// re-mention the user's payload, extractor and return types under the
    /// user's own spans, and body and signature lints still have to reach the
    /// function the user actually wrote. Everything else (a
    /// `#[tracing::instrument]`, say) stays on the function alone, where it is
    /// valid.
    ///
    /// `#[cfg]` and `#[cfg_attr]` are deliberately absent: rustc evaluates both
    /// *before* it invokes an attribute macro, so a configured-out item never
    /// reaches this expansion at all — `#[pgqueue::job(bogus_key = 1)]
    /// #[cfg(any())] async fn f(_: ()) {}` compiles clean, because nothing here
    /// ever parsed the attribute. Only a *parameter* keeps its `cfg` into the
    /// macro, and `validate` refuses that outright.
    fn split(attrs: &[Attribute]) -> Self {
        let mut split = ItemAttrs { api_attrs: Vec::new(), lint_attrs: Vec::new(), deprecated: false };
        for attr in attrs {
            // An inner attribute belongs to the function's *body*, and `syn`
            // re-emits it there. Copying one in front of the generated struct
            // re-emits the leading `!` too, which is a hard error ("an inner
            // attribute is not permitted in this context") for every
            // `#![allow]`/`#![warn]`/`#![deny]`/`#![forbid]` and every `//!`
            // comment a handler body happens to carry.
            if matches!(attr.style, AttrStyle::Inner(_)) {
                continue;
            }
            let path = attr.path();
            if path.is_ident("doc") {
                split.api_attrs.push(attr.to_token_stream());
            } else if path.is_ident("deprecated") {
                split.deprecated = true;
                split.api_attrs.push(attr.to_token_stream());
            } else if let Some(copy) = struct_lint_attr(attr) {
                split.api_attrs.push(copy.clone());
                split.lint_attrs.push(copy);
            }
        }
        split
    }
}

/// Whether a `#[cfg_attr(...)]` meta can resolve to a `#[cfg(...)]` — directly
/// in its tail, or through a nested `cfg_attr`. Such an attribute can remove
/// the item it lands on, so it has to be copied onto every generated item
/// exactly as a bare `cfg` is; one that cannot yield a `cfg` leaves the
/// function in place under every configuration and stays where it was written.
/// Anything unparseable answers `false` and is left for rustc to diagnose on
/// the function.
fn cfg_attr_wraps_cfg(meta: &Meta) -> bool {
    if !meta.path().is_ident("cfg_attr") {
        return false;
    }
    let Meta::List(list) = meta else {
        return false;
    };
    let Ok(parts) = list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated) else {
        return false;
    };
    // The first element is the predicate; the rest are the attributes it
    // resolves to.
    parts.iter().skip(1).any(|inner| inner.path().is_ident("cfg") || cfg_attr_wraps_cfg(inner))
}

/// The copy of a lint-control attribute the generated struct and impls get, or
/// `None` when `attr` is not one.
///
/// Two levels are lowered rather than copied verbatim, because each of those is
/// only one part of the split item and the expansion writes lint control on
/// them too:
///
/// * `#[expect(...)]` becomes `#[allow(...)]`, so an expectation the *function*
///   part fulfils does not warn as unfulfilled here.
/// * `#[forbid(...)]` becomes `#[deny(...)]`. `forbid` cannot be overridden
///   later in the same item, and the struct is named after the user's snake_case
///   function, so it must carry `#[allow(non_camel_case_types)]` — a verbatim
///   `#[forbid(non_camel_case_types)]` made that pair `error[E0453]` and broke
///   the job outright. The impls have the same problem with the
///   `#[allow(deprecated)]` a `#[deprecated]` job needs there. `deny` keeps the
///   level the user asked for.
fn struct_lint_attr(attr: &Attribute) -> Option<TokenStream> {
    let path = attr.path();
    for lint_level in ["allow", "warn", "deny"] {
        if path.is_ident(lint_level) {
            return Some(attr.to_token_stream());
        }
    }
    let lowered = if path.is_ident("expect") {
        quote!(allow)
    } else if path.is_ident("forbid") {
        quote!(deny)
    } else {
        return None;
    };
    if let Meta::List(list) = &attr.meta {
        let lints = &list.tokens;
        return Some(quote!(#[#lowered(#lints)]));
    }
    None
}

/// The job's registry and database name.
///
/// An explicit `name = "..."` is validated while the attribute is parsed. A
/// name derived from the function has to clear the same rule here, or an
/// over-long function name compiles and then fails at every `enqueue` — and,
/// for `#[pgqueue::cron]`, at worker startup. The raw-identifier prefix is
/// stripped, so `async fn r#type` is the job `type` rather than `r#type`.
fn job_name(mode: &Mode, attrs: &JobAttrs, ident: &Ident) -> syn::Result<String> {
    if let Some(name) = &attrs.name {
        return Ok(name.clone());
    }
    // An identifier is never empty and cannot contain NUL, so length is the
    // only part of the shared rule that a derived name can break.
    let max = mode.attr_mode().max_name_bytes();
    let name = ident.unraw().to_string();
    if name.len() > max {
        return Err(syn::Error::new_spanned(
            ident,
            format!(
                "job name must be 1..={max} bytes and contain no NUL; this function's \
                 name is {} bytes, so pass a shorter `name = \"...\"`",
                name.len()
            ),
        ));
    }
    Ok(name)
}

fn runtime_crate_path() -> TokenStream {
    match crate_name("pgqueue") {
        Ok(found) => found_crate_path(found),
        Err(_) => quote!(::pgqueue),
    }
}

fn found_crate_path(found: FoundCrate) -> TokenStream {
    match found {
        // Raw, because the rename is the user's to choose and Cargo accepts a
        // reserved keyword as one: `Ident::new("gen", ..)` emits a bare `gen`,
        // so every `::gen::...` path in the expansion failed to parse and the
        // diagnostic pointed at the attribute, with nothing in the user's
        // source named `gen` to blame.
        //
        // The five names below are the ones `Ident::new_raw` rejects outright,
        // and Cargo does accept them as renames. None can name a dependency in
        // a path — `r#self` is not an identifier, `self::` means this module and
        // `_` is not a path segment at all — so the expansion cannot be made to
        // work; it is left non-raw so the user gets an ordinary "expected
        // identifier" error rather than `proc macro panicked`.
        FoundCrate::Name(name) if matches!(name.as_str(), "crate" | "self" | "super" | "Self" | "_") => {
            let ident = Ident::new(&name, Span::call_site());
            quote!(::#ident)
        }
        FoundCrate::Name(name) => {
            let ident = Ident::new_raw(&name, Span::call_site());
            quote!(::#ident)
        }
        // `pgqueue` exposes this self-alias so the same absolute path works in
        // library code, doctests, and package integration tests.
        FoundCrate::Itself => quote!(::pgqueue),
    }
}

fn validate(mode: &Mode, func: &ItemFn) -> syn::Result<()> {
    if func.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            func.sig.fn_token,
            format!("{} functions must be async", mode.attr_name()),
        ));
    }
    // The expansion binds these names as *patterns* — `let __config`, the
    // `|__ctx|` closure parameter, the `__args`/`__ext{i}` call arguments —
    // while also emitting `#vis struct #ident;` at module level. A function
    // named after one therefore puts a unit struct of that name in the value
    // namespace, and every *other* job in the module reinterprets its binding
    // as a path pattern: a pile of E0530 whose primary span is the *neighbour's*
    // attribute, naming nothing the author wrote. `Span::mixed_site()` does not
    // help, because a unit struct in scope converts a mixed-site binding pattern
    // just the same.
    if let Some(reserved) = reserved_expansion_name(&func.sig.ident) {
        return Err(syn::Error::new_spanned(
            &func.sig.ident,
            format!(
                "`{reserved}` is reserved by the {} expansion; rename the function \
                 and keep the stored job name with `name = \"{reserved}\"` if it matters",
                mode.attr_name()
            ),
        ));
    }
    if let syn::Safety::Unsafe(unsafety) = &func.sig.safety {
        return Err(syn::Error::new_spanned(unsafety, format!("{} functions cannot be unsafe", mode.attr_name())));
    }
    // The generated `call()` documents itself as preserving the original
    // signature, and it cannot preserve an ABI: it forwards through a plain
    // `async fn`, so an `extern "C"` handler's calling convention is silently
    // dropped while the user's own signature keeps it — and collects
    // `improper_ctypes_definitions` warnings for a convention nothing here ever
    // uses.
    if let Some(abi) = &func.sig.abi {
        return Err(syn::Error::new_spanned(abi, format!("{} functions cannot declare an ABI", mode.attr_name())));
    }
    if !func.sig.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &func.sig.generics,
            format!("{} functions cannot be generic", mode.attr_name()),
        ));
    }
    // `ToTokens for Generics` emits nothing when `params` is empty, so spanning
    // a where-clause-only signature on the generics would collapse the
    // diagnostic to `Span::call_site()` — an error pointing at the attribute
    // with nothing underlined, unlike every other rule here.
    //
    // The predicates have to be there for the same reason: `ToTokens for
    // WhereClause` emits nothing at all when the list is empty, which collapses
    // the span exactly the same way. A `where` with no predicates also
    // constrains nothing, so there is nothing to refuse — the equally empty
    // `fn f<>(...)` is accepted above — and a wrapper `macro_rules!` splicing an
    // optional bound list writes one on every zero-bound invocation.
    if let Some(where_clause) = &func.sig.generics.where_clause
        && !where_clause.predicates.is_empty()
    {
        return Err(syn::Error::new_spanned(where_clause, format!("{} functions cannot be generic", mode.attr_name())));
    }
    for input in &func.sig.inputs {
        let FnArg::Typed(argument) = input else {
            continue;
        };
        // An attribute macro is handed the item before `cfg` is evaluated, so
        // this signature is always read with the parameter present and the
        // expansion always binds it — while `#inner` keeps the attribute and so
        // loses the parameter in the configuration that strips it. That build
        // then fails with `E0061: this function takes N arguments but N + 1
        // arguments were supplied` pointing at `#[pgqueue::job]`, naming
        // nothing that would lead back to the `cfg`; the other build only
        // appears to work, which is what makes it worth refusing in both. Gate
        // the whole function, or the extractor's contents, instead.
        // Also a `#[cfg_attr]` that resolves to a `cfg`: it removes the
        // parameter exactly as a bare one does, just one evaluation later.
        if let Some(cfg) = argument
            .attrs
            .iter()
            .find(|attribute| attribute.path().is_ident("cfg") || cfg_attr_wraps_cfg(&attribute.meta))
        {
            return Err(syn::Error::new_spanned(
                cfg,
                format!(
                    "{} functions cannot gate a parameter with `#[cfg]`; the attribute \
                     is expanded before `cfg` is evaluated, so the parameter is always \
                     part of the signature it reads",
                    mode.attr_name()
                ),
            ));
        }
        if is_impl_trait(&argument.ty) {
            return Err(syn::Error::new_spanned(
                &argument.ty,
                format!(
                    "{} functions cannot use `impl Trait` in argument position; \
                     use a concrete type",
                    mode.attr_name()
                ),
            ));
        }
        // A borrowed parameter declares no generic parameter when its lifetime
        // is elided, so it slips past the generics check above and fails instead
        // as "missing lifetime in associated type", helpfully suggesting a
        // lifetime on an `impl` block the author cannot see. The payload is
        // deserialized from the job row and extractors are built per attempt, so
        // neither can borrow. Only a top-level reference is refused here: a
        // lifetime *inside* the type can be legitimate, as `Cow<'static, str>`
        // is a valid payload today.
        if let Type::Reference(reference) = &*argument.ty {
            return Err(syn::Error::new_spanned(
                reference,
                format!(
                    "{} functions cannot take a borrowed parameter; the payload is \
                     deserialized from storage and extractors are built per attempt, \
                     so both must be owned",
                    mode.attr_name()
                ),
            ));
        }
    }
    // The return type is reused as an associated type (`JobType::Output`) and
    // as `call()`'s return type, where `impl Trait` is either unstable or
    // outright illegal. Left unchecked it produced a pile of E0658/E0562/E0277
    // pointing into generated code instead of the one clear message argument
    // position gets.
    if let ReturnType::Type(_, ty) = &func.sig.output
        && is_impl_trait(ty)
    {
        return Err(syn::Error::new_spanned(
            ty,
            format!(
                "{} functions cannot use `impl Trait` in return position; \
                 use a concrete type",
                mode.attr_name()
            ),
        ));
    }
    // The same elided-lifetime hole as argument position, and the output has a
    // stricter requirement still: it is stored as JSON and read back, so it must
    // be `DeserializeOwned`.
    if let ReturnType::Type(_, ty) = &func.sig.output
        && let Type::Reference(reference) = &**ty
    {
        return Err(syn::Error::new_spanned(
            reference,
            format!(
                "{} functions cannot return a borrowed type; the output is serialized \
                 into the job row and read back, so it must be owned",
                mode.attr_name()
            ),
        ));
    }
    if matches!(mode, Mode::Job) && func.sig.inputs.is_empty() {
        return Err(syn::Error::new_spanned(
            &func.sig.ident,
            "#[pgqueue::job] functions need a payload as their first parameter; \
             use `_: ()` for jobs without one",
        ));
    }
    if let Some(variadic) = &func.sig.variadic {
        return Err(syn::Error::new_spanned(variadic, format!("{} functions cannot be variadic", mode.attr_name())));
    }
    Ok(())
}

/// The bound name this identifier would collide with in the expansion, if any.
///
/// Raw identifiers are compared unraw-ed: `r#__config` names the same value as
/// `__config` and collides identically.
fn reserved_expansion_name(ident: &syn::Ident) -> Option<String> {
    let raw = ident.to_string();
    let name = raw.strip_prefix("r#").unwrap_or(&raw);
    let numbered = |prefix: &str| {
        name.strip_prefix(prefix).is_some_and(|index| !index.is_empty() && index.bytes().all(|b| b.is_ascii_digit()))
    };
    let collides =
        matches!(name, "__config" | "__args" | "__ctx" | "__result") || numbered("__ext") || numbered("__arg");
    collides.then(|| name.to_string())
}

fn is_impl_trait(ty: &Type) -> bool {
    struct ImplTraitVisitor {
        found: bool,
    }

    impl<'ast> Visit<'ast> for ImplTraitVisitor {
        fn visit_type_impl_trait(&mut self, _node: &'ast syn::TypeImplTrait) {
            self.found = true;
        }
    }

    let mut visitor = ImplTraitVisitor { found: false };
    visitor.visit_type(ty);
    visitor.found
}

/// Emits one compile-time bound check per millisecond attribute against
/// `pgqueue`'s own `MAX_DURATION_MS`, so the limit has a single source of truth
/// even though this crate cannot depend on `pgqueue`.
///
/// Each check is spanned on the literal the user wrote. Built with a plain
/// `quote!`, the whole assertion carried `Span::call_site()`, so the one
/// diagnostic this crate defers to generated code underlined the entire
/// `#[pgqueue::job(...)]` attribute — while every attribute error raised here
/// underlines the offending value.
///
fn duration_bound_assertions(attrs: &JobAttrs, runtime: &TokenStream) -> Vec<TokenStream> {
    attrs
        .durations
        .iter()
        .map(|(field, ms, span)| {
            let message = format!("{field} exceeds pgqueue's maximum supported duration");
            quote_spanned! {*span=>
                const _: () = ::core::assert!(
                    #ms <= #runtime::__private::MAX_DURATION_MS,
                    #message
                );
            }
        })
        .collect()
}

/// The assignments that turn `JobConfig::default()` into this job's config.
///
/// They target `__config`, not `config`: a bare `let mut config` is a *pattern*,
/// so a unit struct or const named `config` anywhere in scope — including one
/// this very macro generates for `#[pgqueue::job] async fn config(...)` — turns it
/// into a path pattern and breaks every job in the module.
fn config_setters(attrs: &JobAttrs, runtime: &TokenStream) -> Vec<TokenStream> {
    let mut setters = Vec::new();
    if let Some(max_attempts) = attrs.max_attempts {
        setters.push(quote!(__config.max_attempts = #max_attempts;));
    }
    if let Some(timeout) = &attrs.timeout_ms {
        setters.push(match timeout {
            Some(ms) => quote! {
                __config.timeout =
                    ::core::option::Option::Some(::core::time::Duration::from_millis(#ms));
            },
            None => quote!(__config.timeout = ::core::option::Option::None;),
        });
    }
    if let Some(ttl) = &attrs.result_ttl_ms {
        setters.push(match ttl {
            ResultTtl::ForMs(ms) => quote! {
                __config.retention =
                    #runtime::JobRetention::For(::core::time::Duration::from_millis(#ms));
            },
            ResultTtl::Delete => {
                quote!(__config.retention = #runtime::JobRetention::DeleteImmediately;)
            }
        });
    }
    if let Some(ttl) = &attrs.failed_ttl_ms {
        setters.push(match ttl {
            ResultTtl::ForMs(ms) => quote! {
                __config.failed_retention =
                    #runtime::JobRetention::For(::core::time::Duration::from_millis(#ms));
            },
            ResultTtl::Delete => {
                quote!(__config.failed_retention = #runtime::JobRetention::DeleteImmediately;)
            }
        });
    }
    if let Some(ms) = attrs.retry_delay_ms {
        setters.push(quote!(__config.retry_delay = ::core::time::Duration::from_millis(#ms);));
    }
    if let Some(ms) = attrs.max_backoff_ms {
        setters.push(quote! {
            __config.backoff = #runtime::JobRetryBackoff::Exponential {
                max: ::core::option::Option::Some(::core::time::Duration::from_millis(#ms)),
            };
        });
    }
    if let Some(priority) = attrs.priority {
        setters.push(quote!(__config.priority = #priority;));
    }
    setters
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn expand_ok(attr: TokenStream, item: TokenStream) -> String {
        expand_job(attr, item).map(|t| t.to_string()).unwrap_or_else(|e| panic!("{e}"))
    }

    fn expand_cron_ok(attr: TokenStream, item: TokenStream) -> String {
        expand_cron(attr, item).map(|t| t.to_string()).unwrap_or_else(|e| panic!("{e}"))
    }

    fn compact(s: &str) -> String {
        s.replace(' ', "")
    }

    #[test]
    fn test_runtime_crate_path_uses_dependency_alias() {
        let path = found_crate_path(FoundCrate::Name("myqueue".to_string()));
        assert_eq!(compact(&path.to_string()), "::r#myqueue");
    }

    /// Cargo accepts a reserved keyword as a dependency rename and the user's
    /// own `r#gen::job` references resolve fine, so only the expansion was
    /// broken: `Ident::new` emits the keyword bare, every `::gen::...` path in
    /// the output failed to parse, and the diagnostic pointed at the attribute
    /// with nothing in the user's source named `gen` to blame.
    #[test]
    fn test_runtime_crate_path_escapes_a_reserved_keyword_alias() {
        for keyword in ["gen", "async", "await", "dyn", "try", "become"] {
            let path = found_crate_path(FoundCrate::Name(keyword.to_string()));
            assert_eq!(compact(&path.to_string()), format!("::r#{keyword}"));
        }
    }

    /// The five names `Ident::new_raw` refuses. Cargo accepts them as renames
    /// too, and `proc_macro_crate` reports them, so escaping unconditionally
    /// turned a compile error into `proc macro panicked`. None of them can name
    /// a dependency in a path either way, so the expansion is left unescaped
    /// and the user gets an ordinary path error instead.
    ///
    /// `_` was the one the guard originally missed: `_ = { package =
    /// "pgqueue", ... }` is a dependency key Cargo accepts and `proc_macro_crate`
    /// passes through verbatim, so the attribute answered `custom attribute
    /// panicked: `_` cannot be a raw identifier` instead of the ordinary
    /// "expected identifier, found reserved identifier `_`".
    #[test]
    fn test_runtime_crate_path_does_not_panic_on_an_unescapable_alias() {
        for keyword in ["crate", "self", "super", "Self", "_"] {
            let path = found_crate_path(FoundCrate::Name(keyword.to_string()));
            assert_eq!(compact(&path.to_string()), format!("::{keyword}"));
        }
    }

    /// Expanding *inside* `pgqueue` — its doctests, its own library code — has
    /// no dependency entry to read a name from, and resolves through the
    /// `extern crate self as pgqueue` alias instead.
    #[test]
    fn test_runtime_crate_path_uses_the_self_alias_inside_pgqueue() {
        let path = found_crate_path(FoundCrate::Itself);
        assert_eq!(compact(&path.to_string()), "::pgqueue");
    }

    #[test]
    fn test_expands_minimal_job() {
        let out = expand_ok(
            quote!(),
            quote! {
                async fn send_email(args: SendEmail) -> anyhow::Result<()> {
                    Ok(())
                }
            },
        );
        let flat = compact(&out);
        assert!(flat.contains("structsend_email;"), "{out}");
        assert!(flat.contains("impl::pgqueue::JobTypeforsend_email"), "{out}");
        assert!(flat.contains("impl::pgqueue::JobDefinitionforsend_email"), "{out}");
        assert!(flat.contains("typeArgs=SendEmail;"), "{out}");
        assert!(
            flat.contains("typeOutput=<anyhow::Result<()>as::pgqueue::__private::IntoJobResult>::Output;"),
            "{out}"
        );
        assert!(flat.contains("constNAME:&'staticstr=\"send_email\";"), "{out}");
        assert!(flat.contains("fnjob(__args:SendEmail)"), "{out}");
        assert!(flat.contains("asyncfncall(__arg0:SendEmail)->anyhow::Result<()>"), "{out}");
        // The hidden handler keeps the name the user wrote (see
        // `test_hidden_handler_keeps_the_user_s_identifier`).
        assert!(flat.contains("asyncfnsend_email"), "{out}");
        // No attrs: config is just the default; no schedule for plain jobs.
        assert!(!flat.contains("__config.max_attempts="), "{out}");
        assert!(!flat.contains("SCHEDULE"), "{out}");
    }

    #[test]
    fn test_expands_extractors_positionally() {
        let out = expand_ok(
            quote!(),
            quote! {
                pub async fn resize(args: Resize, s: JobState<Pool>, ctx: JobContext) -> Result<u32, Error> {
                    Ok(1)
                }
            },
        );
        let flat = compact(&out);
        assert!(flat.contains("pubstructresize;"), "{out}");
        assert!(flat.contains("<JobState<Pool>as::pgqueue::FromJobContext>::from_context(&__ctx)"), "{out}");
        assert!(flat.contains("<JobContextas::pgqueue::FromJobContext>::from_context(&__ctx)"), "{out}");
        assert!(flat.contains("resize(__args,__ext0,__ext1)"), "{out}");
        assert!(flat.contains("pubasyncfncall(__arg0:Resize,__arg1:JobState<Pool>,__arg2:JobContext)"), "{out}");
    }

    #[test]
    fn test_expands_all_config_attrs() {
        let out = expand_ok(
            quote!(
                name = "custom",
                max_attempts = 4,
                timeout_ms = 30_000,
                result_ttl_ms = 3_600_000,
                retry_delay_ms = 500,
                max_backoff_ms = 120_000,
                priority = -1
            ),
            quote! {
                async fn j(_: ()) {}
            },
        );
        let flat = compact(&out);
        assert!(flat.contains("constNAME:&'staticstr=\"custom\";"), "{out}");
        assert!(flat.contains("__config.max_attempts=4u32;"), "{out}");
        assert!(
            flat.contains(
                "__config.timeout=::core::option::Option::Some(::core::time::Duration::from_millis(30000u64));"
            ),
            "{out}"
        );
        assert!(
            flat.contains("::pgqueue::JobRetention::For(::core::time::Duration::from_millis(3600000u64))"),
            "{out}"
        );
        assert!(flat.contains("__config.retry_delay=::core::time::Duration::from_millis(500u64);"), "{out}");
        assert!(flat.contains("::pgqueue::JobRetryBackoff::Exponential{max:::core::option::Option::Some"), "{out}");
        assert!(flat.contains("__config.priority=-1i16;"), "{out}");
        // Unit return type maps through IntoJobResult for ().
        assert!(flat.contains("typeOutput=<()as::pgqueue::__private::IntoJobResult>::Output;"), "{out}");
    }

    #[test]
    fn test_expands_zero_values() {
        let out = expand_ok(
            quote!(timeout_ms = 0),
            quote! {
                async fn j(_: ()) {}
            },
        );
        let flat = compact(&out);
        assert!(flat.contains("__config.timeout=::core::option::Option::None;"), "{out}");
        assert!(!flat.contains("__config.retention="), "{out}");
        assert!(!flat.contains("__config.backoff="), "{out}");

        let out = expand_ok(
            quote!(result_ttl_ms = 0),
            quote!(
                async fn j(_: ()) {}
            ),
        );
        assert!(compact(&out).contains("JobRetention::DeleteImmediately"), "{out}");
    }

    #[test]
    fn test_keeps_doc_comments_on_the_struct() {
        let out = expand_ok(
            quote!(),
            quote! {
                /// Sends the welcome email.
                async fn welcome(_: ()) {}
            },
        );
        assert!(out.contains("Sends the welcome email."), "{out}");
    }

    #[test]
    fn test_moves_deprecation_to_the_generated_job_type() {
        let out = expand_ok(
            quote!(),
            quote! {
                #[deprecated(note = "use the replacement")]
                async fn legacy(_: ()) {}
            },
        );
        let flat = compact(&out);
        assert!(flat.contains("#[deprecated(note=\"usethereplacement\")]"), "{out}");
        assert_eq!(flat.matches("#[deprecated(note=").count(), 1, "{out}");
        // Scoped to the three generated impl blocks, never to the anonymous
        // const that also holds the user's function body.
        assert!(!flat.contains("#[allow(deprecated)]const_:"), "{out}");
        assert!(flat.contains("#[allow(deprecated)]impllegacy{"), "{out}");
        assert!(flat.contains("#[allow(deprecated)]impl::pgqueue::JobTypeforlegacy"), "{out}");
        assert!(flat.contains("#[allow(deprecated)]impl::pgqueue::JobDefinitionforlegacy"), "{out}");
        assert_eq!(flat.matches("#[allow(deprecated)]").count(), 3, "{out}");
    }

    /// A job that is not deprecated must not silently allow the lint, or a
    /// crate migrating off a deprecated API gets no signal inside any handler.
    #[test]
    fn test_omits_the_deprecated_allow_when_the_job_is_not_deprecated() {
        let out = expand_ok(
            quote!(),
            quote! {
                async fn current(_: ()) {}
            },
        );
        assert!(!compact(&out).contains("#[allow(deprecated)]"), "{out}");
    }

    #[test]
    fn test_copies_lint_attributes_onto_every_generated_item() {
        let out = expand_ok(
            quote!(),
            quote! {
                #[allow(missing_docs)]
                #[deny(clippy::pedantic)]
                #[tracing::instrument]
                pub async fn undocumented(_: ()) {}
            },
        );
        let flat = compact(&out);
        // One written item becomes five: the struct (which is what
        // `missing_docs` fires on), the hidden function the user wrote, and the
        // three impls.
        for attr in ["#[allow(missing_docs)]", "#[deny(clippy::pedantic)]"] {
            assert_eq!(flat.matches(attr).count(), 5, "{out}");
        }
        for item in [
            "implundocumented{",
            "impl::pgqueue::JobTypeforundocumented",
            "impl::pgqueue::JobDefinitionforundocumented",
        ] {
            assert!(
                flat.contains(&format!("#[allow(missing_docs)]#[deny(clippy::pedantic)]{item}")),
                "{item} must carry the user's lint attributes: {out}"
            );
        }
        assert!(
            flat.contains("#[allow(missing_docs)]#[deny(clippy::pedantic)]#[allow(non_camel_case_types)]"),
            "{out}"
        );
        assert!(
            flat.contains(
                "#[allow(missing_docs)]#[deny(clippy::pedantic)]\
                 #[tracing::instrument]asyncfnundocumented"
            ),
            "{out}"
        );
        // Anything that is not lint control stays on the function alone: it is
        // not necessarily valid on a struct or an impl.
        assert_eq!(flat.matches("#[tracing::instrument]").count(), 1, "{out}");
        assert!(flat.contains("#[tracing::instrument]asyncfnundocumented"), "{out}");
        // Docs and `#[deprecated]` describe the job, so they reach the struct
        // only: a second `#[deprecated]` would make the impls' own mentions of
        // the job type warn.
        let out = expand_ok(
            quote!(),
            quote! {
                /// Documents the job.
                #[deprecated(note = "use the replacement")]
                #[allow(deprecated)]
                pub async fn legacy(_: OldPayload) {}
            },
        );
        let flat = compact(&out);
        assert_eq!(flat.matches("Documentsthejob.").count(), 1, "{out}");
        assert_eq!(flat.matches("#[deprecated(note=").count(), 1, "{out}");
        // The user's allow reaches all three impls, and the expansion's own allow
        // for the deprecated job type still follows it there.
        assert_eq!(flat.matches("#[allow(deprecated)]#[allow(deprecated)]impl").count(), 3, "{out}");
    }

    /// Both impls re-mention the payload, extractor and return types with the
    /// user's spans, so lints fire on them as if the user had written them —
    /// routing lint control to the struct and the hidden function alone made
    /// `#[allow(deprecated)]` on a job naming a deprecated payload stop
    /// applying to the code the macro derived from that very signature.
    /// `tests/macros/pass_lint_attrs.rs` compiles the scenario.
    #[test]
    fn test_lint_attributes_reach_the_impls_that_name_the_user_s_types() {
        for out in [
            expand_ok(
                quote!(),
                quote!(
                    #[allow(deprecated)]
                    async fn j(_: OldPayload, s: JobState<OldState>) -> Result<OldOutput, Error> {}
                ),
            ),
            expand_cron_ok(
                quote!("* * * * *"),
                quote!(
                    #[allow(deprecated)]
                    async fn c(s: JobState<OldState>) -> Result<OldOutput, Error> {}
                ),
            ),
        ] {
            let flat = compact(&out);
            assert!(flat.contains("#[allow(deprecated)]impl"), "{out}");
            assert!(flat.contains("#[allow(deprecated)]impl::pgqueue::JobTypefor"), "{out}");
            // Struct, hidden function and all three impls.
            assert_eq!(flat.matches("#[allow(deprecated)]").count(), 5, "{out}");
        }
    }

    /// rustc evaluates `#[cfg]` and `#[cfg_attr]` before it invokes an attribute
    /// macro, so a configured-out job never reaches the expansion — there is no
    /// item-level routing to test, and `tests/macros/pass_cfg.rs` pins that both
    /// configurations compile. A *parameter*, though, keeps its `cfg` into the
    /// macro and can vanish from a signature the expansion has already read, so
    /// that one is refused.
    #[test]
    fn test_cfg_gated_parameters_are_refused() {
        for gate in [quote!(#[cfg(any())]), quote!(#[cfg_attr(test, cfg(any()))])] {
            let err = expand_job(
                quote!(),
                quote! {
                    async fn j(_: (), #gate _metrics: u32) {}
                },
            )
            .unwrap_err();
            assert!(err.to_string().contains("cannot gate a parameter with `#[cfg]`"), "{err}");
        }
    }

    /// A `#[cfg_attr(...)]` that does reach the expansion stays on the handler,
    /// where the user wrote it, rather than being copied onto items it was never
    /// meant for.
    ///
    /// rustc itself never delivers one: `take_first_attr` prefers a `cfg`/
    /// `cfg_attr` over a non-builtin attribute whatever the order, so an
    /// item-level `cfg_attr` is always resolved before this macro is invoked —
    /// which is the premise `ItemAttrs::split` records and the reason there is no
    /// routing for it. This calls `expand_job` directly and so pins the fallback
    /// for tokens that arrive some other way; `tests/macros/pass_cfg.rs` pins the
    /// end-to-end behaviour rustc actually produces.
    #[test]
    fn test_cfg_attr_stays_on_the_handler() {
        let out = expand_ok(
            quote!(),
            quote! {
                #[cfg_attr(test, tracing::instrument)]
                pub async fn traced(_: ()) {}
            },
        );
        let flat = compact(&out);
        assert_eq!(flat.matches("#[cfg_attr(test,tracing::instrument)]").count(), 1, "{out}");
    }

    /// `#[expect(...)]` is lowered for every item the expansion writes,
    /// including the hidden handler: one written item becomes several, the
    /// lint fires on only one of them, and every other copy would report as
    /// unfulfilled. Suppressing that with an `#[allow(unfulfilled_lint_expectations)]`
    /// of the expansion's own is itself `error[E0453]` under a crate that
    /// forbids the lint. `#[forbid(...)]` is lowered too, or a
    /// `#[forbid(deprecated)]` job collides with the `#[allow(deprecated)]` a
    /// deprecated job needs on its impls (E0453).
    #[test]
    fn test_lowers_expect_and_forbid_on_the_generated_impls() {
        let out = expand_ok(
            quote!(),
            quote! {
                #[expect(deprecated)]
                #[forbid(unsafe_code)]
                #[deprecated(note = "gone")]
                pub async fn legacy(_: OldPayload) {}
            },
        );
        let flat = compact(&out);
        assert_eq!(flat.matches("#[expect(").count(), 0, "{out}");
        assert_eq!(flat.matches("#[allow(unfulfilled_lint_expectations)]").count(), 0, "{out}");
        assert!(flat.contains("#[allow(deprecated)]#[forbid(unsafe_code)]"), "{out}");
        assert_eq!(flat.matches("#[forbid(unsafe_code)]").count(), 1, "{out}");
        assert_eq!(
            flat.matches("#[allow(deprecated)]#[deny(unsafe_code)]#[allow(deprecated)]impl").count(),
            3,
            "{out}"
        );
    }

    /// `#[tracing::instrument]` is the motivating example for leaving
    /// non-lint attributes on the hidden function, and it derives its span name
    /// from the identifier. Renaming the function to a private placeholder
    /// labelled every job's telemetry `__pgqueue_inner`, losing the handler
    /// name across all of it.
    #[test]
    fn test_hidden_handler_keeps_the_user_s_identifier() {
        let out = expand_ok(
            quote!(),
            quote! {
                #[tracing::instrument]
                async fn send_email(args: SendEmail) -> anyhow::Result<()> {
                    Ok(())
                }
            },
        );
        let flat = compact(&out);
        assert!(
            !flat.contains("__pgqueue_inner"),
            "the placeholder name is what `#[tracing::instrument]` would report: {out}"
        );
        assert!(flat.contains("#[tracing::instrument]asyncfnsend_email(args:SendEmail)"), "{out}");
        assert!(flat.contains("send_email(__args)"), "{out}");
        assert!(flat.contains("send_email(__arg0).await"), "{out}");

        // The same for cron, whose erased call takes extractors only.
        let out = expand_cron_ok(
            quote!("*/5 * * * *"),
            quote! {
                #[tracing::instrument]
                async fn cleanup(ctx: JobContext) {}
            },
        );
        let flat = compact(&out);
        assert!(!flat.contains("__pgqueue_inner"), "{out}");
        assert!(flat.contains("#[tracing::instrument]asyncfncleanup(ctx:JobContext)"), "{out}");
        assert!(flat.contains("cleanup(__ext0)"), "{out}");
    }

    /// Every binding the expansion introduces is `__`-prefixed. `config` and
    /// `args` were not, and both are *patterns*, so an in-scope unit struct or
    /// const of either name reinterpreted them as path patterns — which
    /// `#[pgqueue::job] async fn config(...)` triggers for every *other* job in
    /// the same module, pointing the diagnostic at the attribute. The
    /// `tests/macros/pass_hygiene.rs` case compiles the shadowing scenario.
    #[test]
    fn test_generated_bindings_are_all_double_underscore_prefixed() {
        for out in [
            expand_ok(
                quote!(max_attempts = 2, priority = 1),
                quote!(
                    async fn j(_: u32) {}
                ),
            ),
            expand_cron_ok(
                quote!("* * * * *", max_attempts = 2),
                quote!(
                    async fn c() {}
                ),
            ),
        ] {
            let flat = compact(&out);
            assert!(!flat.contains("letmutconfig="), "{out}");
            assert!(!flat.contains("letconfig="), "{out}");
            assert!(flat.contains("__config"), "{out}");
            assert!(!flat.contains("(args:"), "{out}");
        }
    }

    /// `#[allow(unused_mut)]` is itself an error under `#![forbid(unused_mut)]`,
    /// so `mut` is emitted only when a setter actually assigns.
    #[test]
    fn test_config_binding_is_mutable_only_when_an_attribute_sets_it() {
        let bare = compact(&expand_ok(
            quote!(),
            quote!(
                async fn j(_: ()) {}
            ),
        ));
        assert!(!bare.contains("#[allow(unused_mut)]"), "{bare}");
        assert!(bare.contains("let__config="), "{bare}");

        let configured = compact(&expand_ok(
            quote!(max_attempts = 2),
            quote!(
                async fn j(_: ()) {}
            ),
        ));
        assert!(!configured.contains("#[allow(unused_mut)]"), "{configured}");
        assert!(configured.contains("letmut__config="), "{configured}");
    }

    /// `syn` hoists a body's inner attributes into `ItemFn::attrs`. Splatting
    /// one in front of the generated struct re-emits the leading `!`, which is
    /// "an inner attribute is not permitted in this context" — and the doc
    /// strip *deleted* an inner `//!` outright. Both belong to the body.
    /// `tests/macros/pass_hygiene.rs` compiles the scenario.
    #[test]
    fn test_inner_attributes_stay_on_the_handler_body() {
        let out = expand_ok(
            quote!(),
            quote! {
                /// Outer documentation describes the job.
                pub async fn work(_: ()) -> anyhow::Result<()> {
                    #![allow(unused_variables)]
                    //! Inner documentation describes the body.
                    Ok(())
                }
            },
        );
        let flat = compact(&out);
        let struct_at = flat.find("pubstructwork;").unwrap_or_else(|| panic!("{out}"));
        for inner in [
            "#![allow(unused_variables)]",
            "Innerdocumentationdescribesthebody.",
        ] {
            assert_eq!(flat.matches(inner).count(), 1, "{out}");
            let inner_at = flat.find(inner).unwrap_or_else(|| panic!("{out}"));
            assert!(inner_at > struct_at, "{inner} must stay inside the hidden handler: {out}");
        }
        // The outer doc still moves to the struct, as before.
        let doc_at = flat.find("Outerdocumentationdescribesthejob.").unwrap_or_else(|| panic!("{out}"));
        assert!(doc_at < struct_at, "{out}");
    }

    /// A `#[forbid(...)]` copied verbatim onto the generated struct met the
    /// `#[allow(non_camel_case_types)]` the expansion has to write there —
    /// `error[E0453]: allow(non_camel_case_types) incompatible with previous
    /// forbid` — so the struct's copy is lowered to `deny`, which the user's
    /// intent survives and which a later `allow` may override.
    #[test]
    fn test_lowers_a_forbid_to_a_deny_on_the_generated_struct() {
        let out = expand_ok(
            quote!(),
            quote! {
                #[forbid(non_camel_case_types)]
                pub async fn forbidding(_: ()) {}
            },
        );
        let flat = compact(&out);
        assert!(flat.contains("#[deny(non_camel_case_types)]#[allow(non_camel_case_types)]"), "{out}");
        // The function half keeps the level the user wrote.
        assert_eq!(flat.matches("#[forbid(non_camel_case_types)]").count(), 1, "{out}");
        assert!(flat.contains("#[forbid(non_camel_case_types)]asyncfnforbidding"), "{out}");
    }

    /// A lint level that names no lints is not one the expansion can lower and
    /// copy, so it stays on the function the user wrote it on and rustc reports
    /// it there, rather than being duplicated onto every generated item.
    #[test]
    fn test_leaves_a_lint_level_naming_no_lints_where_it_was_written() {
        let out = expand_ok(
            quote!(),
            quote! {
                #[expect]
                #[forbid = "nonsense"]
                pub async fn odd(_: ()) {}
            },
        );
        let flat = compact(&out);
        assert_eq!(flat.matches("#[expect]").count(), 1, "{out}");
        assert_eq!(flat.matches("#[forbid=\"nonsense\"]").count(), 1, "{out}");
        assert!(flat.contains("#[expect]#[forbid=\"nonsense\"]asyncfnodd"), "{out}");
    }

    #[test]
    fn test_lowers_an_expect_to_an_allow_on_the_generated_struct() {
        let out = expand_ok(
            quote!(),
            quote! {
                #[expect(missing_docs)]
                pub async fn undocumented(_: ()) {}
            },
        );
        let flat = compact(&out);
        // Every item the expansion writes carries a plain allow, so no copy of
        // the expectation is left to report as unfulfilled — and the expansion
        // never has to `allow` a lint the crate may have forbidden.
        assert!(flat.contains("#[allow(missing_docs)]#[allow(non_camel_case_types)]"), "{out}");
        assert!(flat.contains("#[allow(missing_docs)]asyncfnundocumented"), "{out}");
        assert_eq!(flat.matches("#[expect(").count(), 0, "{out}");
        assert_eq!(flat.matches("unfulfilled_lint_expectations").count(), 0, "{out}");
    }

    #[test]
    fn test_derives_the_job_name_from_the_unraw_function_name() {
        let out = expand_ok(
            quote!(),
            quote! {
                async fn r#type(_: ()) {}
            },
        );
        assert!(compact(&out).contains("constNAME:&'staticstr=\"type\";"), "{out}");
    }

    /// A derived name clears the same rule an explicit `name = "..."` does, so
    /// an over-long function name fails the build rather than every `enqueue`.
    #[test]
    fn test_rejects_a_derived_job_name_longer_than_the_column_allows() {
        let long = format_ident!("{}", "n".repeat(256));
        let err = expand_job(quote!(), quote!(async fn #long(_: ()) {})).unwrap_err();
        assert!(err.to_string().contains("256 bytes"), "{err}");
        assert!(err.to_string().contains("job name must be 1..=255 bytes"), "{err}");

        let ok = format_ident!("{}", "n".repeat(255));
        assert!(expand_job(quote!(), quote!(async fn #ok(_: ()) {})).is_ok());
    }

    /// A cron's durable identity is its derived `cron:{name}` dedupe key, which
    /// `JobRequest::validate` caps at 255 bytes — so a 251-byte cron name used
    /// to compile and then fail at `Worker::build()`, the exact runtime failure
    /// the compile-time rule exists to prevent.
    #[test]
    fn test_cron_names_leave_room_for_the_derived_dedupe_key() {
        let boundary = format_ident!("{}", "n".repeat(251));
        let err = expand_cron(quote!("* * * * *"), quote!(async fn #boundary() {})).unwrap_err();
        assert!(err.to_string().contains("251 bytes"), "{err}");
        assert!(err.to_string().contains("job name must be 1..=250 bytes"), "{err}");

        let ok = format_ident!("{}", "n".repeat(250));
        assert!(expand_cron(quote!("* * * * *"), quote!(async fn #ok() {})).is_ok());

        // An explicit name goes through the same bound.
        let long = "n".repeat(251);
        let err = expand_cron(
            quote!("* * * * *", name = #long),
            quote!(
                async fn c() {}
            ),
        )
        .unwrap_err();
        assert_eq!(err.to_string(), "job name must be 1..=250 bytes and contain no NUL");
        // A plain job keeps the full 255, because it derives no dedupe key.
        let job_name = "n".repeat(255);
        assert!(
            expand_job(
                quote!(name = #job_name),
                quote!(
                    async fn j(_: ()) {}
                )
            )
            .is_ok()
        );
    }

    /// `impl Trait` in return position reached the generated `JobType::Output`
    /// and `call()`, where it is unstable or illegal, so the user got a pile of
    /// E0658/E0562/E0277 pointing into generated code instead of the clear
    /// message argument position gets. `tests/macros/fail.stderr` pins the span.
    #[test]
    fn test_rejects_impl_trait_in_return_position() {
        let err = expand_job(
            quote!(),
            quote!(
                async fn j(_: ()) -> impl serde::Serialize {}
            ),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "#[pgqueue::job] functions cannot use `impl Trait` in return position; \
             use a concrete type"
        );

        let err = expand_cron(
            quote!("* * * * *"),
            quote!(
                async fn c() -> impl serde::Serialize {}
            ),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "#[pgqueue::cron] functions cannot use `impl Trait` in return position; \
             use a concrete type"
        );
    }

    #[test]
    fn test_job_config_default_is_absolutely_qualified() {
        let out = expand_ok(
            quote!(max_attempts = 3),
            quote! {
                async fn j(_: ()) {}
            },
        );
        let flat = compact(&out);
        assert!(flat.contains("<::pgqueue::JobConfigas::core::default::Default>::default()"), "{out}");
        assert!(!flat.contains("JobConfig::default()"), "{out}");
    }

    #[test]
    fn test_job_rejects_the_cron_only_revision_key() {
        let err = expand_job(
            quote!(revision = 1),
            quote!(
                async fn j(_: ()) {}
            ),
        )
        .unwrap_err();
        assert_eq!(err.to_string(), "`revision` is only valid on #[pgqueue::cron]");
    }

    #[test]
    fn test_rejects_invalid_functions() {
        let cases: Vec<(TokenStream, &str)> = vec![
            (
                quote!(
                    fn j(_: ()) {}
                ),
                "must be async",
            ),
            (
                quote!(
                    async fn j() {}
                ),
                "need a payload",
            ),
            (
                quote!(
                    async fn j<T>(args: T) {}
                ),
                "cannot be generic",
            ),
            (
                quote!(
                    async unsafe fn j(_: ()) {}
                ),
                "cannot be unsafe",
            ),
            (
                quote! {
                    async fn j(args: u32) where u32: Copy {}
                },
                "cannot be generic",
            ),
            (
                quote!(
                    async fn j(self, args: u32) {}
                ),
                "cannot take self",
            ),
            (
                quote!(
                    async fn j(args: Vec<impl serde::Serialize>) {}
                ),
                "cannot use `impl Trait` in argument position",
            ),
            // `syn` parses a C variadic in any signature; only rustc restricts
            // it to `extern` blocks, and it gets there long after this.
            (
                quote!(
                    async fn j(_: (), _: ...) {}
                ),
                "cannot be variadic",
            ),
        ];
        for (item, expected) in cases {
            let err = expand_job(quote!(), item.clone()).expect_err(&format!("should fail: {item}"));
            assert!(err.to_string().contains(expected), "{item}: {err}");
        }
    }

    /// A `where` with no predicates constrains nothing — the signature is
    /// identical to one without it, and the equally empty `fn j<>(...)` is
    /// accepted — yet it was refused as "generic". Worse, `ToTokens for
    /// WhereClause` emits nothing when the list is empty, so `new_spanned` fell
    /// back to `Span::call_site()` and underlined the attribute: the very
    /// collapse the branch above it was written to avoid. A wrapper
    /// `macro_rules!` splicing an optional bound list writes exactly this on its
    /// zero-bound invocation.
    #[test]
    fn test_accepts_a_where_clause_with_no_predicates() {
        let out = expand_ok(
            quote!(),
            quote! {
                async fn j(args: u32) where {
                    let _ = args;
                }
            },
        );
        assert!(compact(&out).contains("structj;"), "{out}");
        // A `where` that does constrain something is still refused, and still
        // underlines the clause rather than the attribute.
        let err = expand_job(
            quote!(),
            quote! {
                async fn j(args: u32) where u32: Copy {}
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("cannot be generic"), "{err}");
    }

    #[test]
    fn test_attr_errors_propagate() {
        let err = expand_job(
            quote!(bogus = 1),
            quote!(
                async fn j(_: ()) {}
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown attribute"), "{err}");
        let err = expand_job(
            quote!(),
            quote!(
                struct NotAFn;
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("expected"), "{err}");
    }

    #[test]
    fn test_expands_cron_with_extractors_only() {
        let out = expand_cron_ok(
            quote!("*/5 * * * *"),
            quote! {
                pub async fn cleanup(ctx: JobContext, db: JobState<Pool>) -> anyhow::Result<u64> {
                    Ok(0)
                }
            },
        );
        let flat = compact(&out);
        assert!(flat.contains("pubstructcleanup;"), "{out}");
        // Payload is fixed to () and job() takes no arguments.
        assert!(flat.contains("typeArgs=();"), "{out}");
        assert!(flat.contains("pubfnjob()->::pgqueue::JobBuilder<cleanup>"), "{out}");
        assert!(flat.contains("::pgqueue::JobBuilder::new(())"), "{out}");
        // The schedule is baked in.
        assert!(flat.contains("impl::pgqueue::CronDefinitionforcleanup"), "{out}");
        assert!(flat.contains("constSCHEDULE:&'staticstr=\"*/5****\";"), "{out}");
        // Every parameter is an extractor; no payload decode.
        assert!(flat.contains("<JobContextas::pgqueue::FromJobContext>::from_context(&__ctx)"), "{out}");
        assert!(flat.contains("<JobState<Pool>as::pgqueue::FromJobContext>::from_context(&__ctx)"), "{out}");
        assert!(flat.contains("cleanup(__ext0,__ext1)"), "{out}");
        assert!(!flat.contains("decode_payload"), "{out}");
        // call() preserves the original extractor-only signature.
        assert!(flat.contains("pubasyncfncall(__arg0:JobContext,__arg1:JobState<Pool>)"), "{out}");
    }

    #[test]
    fn test_expands_cron_with_no_params_and_config() {
        let out = expand_cron_ok(
            quote!("*/5 * * * *", name = "tidy", max_attempts = 2, timeout_ms = 300_000),
            quote! {
                async fn cleanup() {}
            },
        );
        let flat = compact(&out);
        assert!(flat.contains("constNAME:&'staticstr=\"tidy\";"), "{out}");
        assert!(flat.contains("__config.max_attempts=2u32;"), "{out}");
        assert!(flat.contains("constSCHEDULE:&'staticstr=\"*/5****\";"), "{out}");
        // The handler's value is bound before it is encoded, so the encode's
        // `IntoJobResult` obligation can carry the return type's span.
        assert!(flat.contains("let__result=cleanup().await;"), "{out}");
        assert!(flat.contains("encode_result(__result)"), "{out}");
    }

    #[test]
    fn test_cron_rejects_bad_input() {
        // Missing expression.
        let err = expand_cron(
            quote!(),
            quote!(
                async fn j() {}
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("cron expression"), "{err}");
        // Non-string expression.
        let err = expand_cron(
            quote!(42),
            quote!(
                async fn j() {}
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("cron expression"), "{err}");
        // Invalid expression (validated at compile time).
        let err = expand_cron(
            quote!("99 * * * *"),
            quote!(
                async fn j() {}
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("invalid cron expression"), "{err}");
        // Seconds are intentionally unsupported.
        let err = expand_cron(
            quote!("0 * * * * *"),
            quote!(
                async fn j() {}
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("expected 5"), "{err}");
        // Bad config attr after the expression.
        let err = expand_cron(
            quote!("* * * * *", bogus = 1),
            quote!(
                async fn j() {}
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown attribute"), "{err}");
        // Signature rules still apply.
        let err = expand_cron(
            quote!("* * * * *"),
            quote!(
                fn j() {}
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("#[pgqueue::cron] functions must be async"), "{err}");

        let err = expand_cron(
            quote!("* * * * *"),
            quote!(
                async fn j(state: impl Send) {}
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("cannot use `impl Trait` in argument position"), "{err}");

        let err = expand_cron(
            quote!("* * * * *"),
            quote!(
                async unsafe fn j() {}
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("#[pgqueue::cron] functions cannot be unsafe"), "{err}");
    }
}
