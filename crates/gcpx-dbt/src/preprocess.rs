// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

/// dbt preprocessing pipeline.
///
/// Runs **before** the scanner/macro-expand/resolve phases.
/// Handles text-level transformations that the scanner cannot:
/// - `{% if is_incremental() %}`...`{% endif %}` block removal/retention
/// - `{{ this }}` replacement with backtick-quoted table ref
/// - `{{ var('name') }}` substitution from project-level vars
/// - `dbt_utils.*` built-in macro expansion to BigQuery SQL
use std::collections::BTreeMap;

/// Top-level preprocessor: runs all phases in order.
pub fn preprocess(
    sql: &str,
    vars: &BTreeMap<String, String>,
    table_ref: &str,
    is_incremental: bool,
) -> Result<String, PreprocessError> {
    // Raw regions are set aside before any other phase, so nothing downstream
    // can mistake their contents for a template.
    let protected = protect_raw_regions(sql)?;
    let step1 = handle_incremental_blocks(&protected, is_incremental);
    let step2 = replace_this(&step1, table_ref);
    let step3 = expand_vars(&step2, vars)?;
    let step4 = expand_builtins(&step3);
    Ok(step4)
}

#[derive(Debug, thiserror::Error)]
pub enum PreprocessError {
    #[error("unknown variable '{name}' — available vars: [{available}]")]
    UnknownVar { name: String, available: String },
    #[error("unsupported SQL: {reason}")]
    UnsupportedInput { reason: String },
}

// ── Feature 2: `{% if is_incremental() %}` ──────────────────────────

/// Remove or retain `{% if is_incremental() %}…{% endif %}` blocks.
///
/// Matching is whitespace-tolerant. The predecessor compared against one exact
/// spelling, so `{%if is_incremental()%}` — or any of the other formattings a
/// person naturally writes — passed straight through into the emitted SQL,
/// where BigQuery rejected it with a syntax error pointing at the Jinja rather
/// than at the whitespace. Being strict here buys nothing and costs a
/// genuinely baffling error message.
pub fn handle_incremental_blocks(sql: &str, keep: bool) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut rest = sql;

    loop {
        let Some((before, after_open)) = split_at_tag(rest, TagKind::IfIsIncremental) else {
            result.push_str(rest);
            return result;
        };
        result.push_str(before);

        match split_at_tag(after_open, TagKind::EndIf) {
            Some((body, after_close)) => {
                if keep {
                    result.push_str(body);
                }
                rest = after_close;
            }
            None => {
                // Unclosed block: emit the remainder untouched rather than
                // silently discarding SQL the user wrote.
                result.push_str(&rest[before.len()..]);
                return result;
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TagKind {
    IfIsIncremental,
    EndIf,
    Raw,
    EndRaw,
}

/// Split at the first occurrence of `kind`, returning the text before the tag
/// and the text after it.
fn split_at_tag(input: &str, kind: TagKind) -> Option<(&str, &str)> {
    let mut from = 0;
    while let Some(rel) = input[from..].find("{%") {
        let open = from + rel;
        // Search for the closer *after* the opener. Starting at `open` lets
        // `{%}` match its own `%}`, which yields an inverted slice range and
        // panics — caught by the never-panics property test.
        let body_start = open + 2;
        let close_rel = input[body_start..].find("%}")?;
        let close = body_start + close_rel + 2;
        if tag_matches(&input[body_start..close - 2], kind) {
            return Some((&input[..open], &input[close..]));
        }
        from = close;
    }
    None
}

/// Whether a tag's inner text denotes `kind`, ignoring all whitespace.
///
/// Jinja also allows whitespace-control markers (`{%-` and `-%}`), which are
/// stripped before comparison so `{%- if is_incremental() -%}` is recognised.
fn tag_matches(inner: &str, kind: TagKind) -> bool {
    let inner = inner.trim().trim_start_matches('-').trim_end_matches('-');
    let squashed: String = inner.chars().filter(|c| !c.is_whitespace()).collect();
    match kind {
        TagKind::IfIsIncremental => squashed.eq_ignore_ascii_case("ifis_incremental()"),
        TagKind::EndIf => squashed.eq_ignore_ascii_case("endif"),
        TagKind::Raw => squashed.eq_ignore_ascii_case("raw"),
        TagKind::EndRaw => squashed.eq_ignore_ascii_case("endraw"),
    }
}

// ── Raw regions ─────────────────────────────────────────────────────────────

/// Sentinel that stands in for a raw region while the pipeline runs.
///
/// NUL cannot appear in the SQL the provider accepts, so the marker cannot
/// collide with user content — and input containing one is rejected rather
/// than trusted.
const RAW_SENTINEL_PREFIX: &str = "\u{0}RAW:";
const RAW_SENTINEL_SUFFIX: char = '\u{0}';

/// Replace every `{% raw %}…{% endraw %}` region with an opaque sentinel.
///
/// # Why this exists
///
/// Two renderers see a stack, and both use the same delimiters. The YAML
/// runtime renders the stack file before this provider sees any of it, so dbt
/// syntax written inline there is evaluated by the wrong layer — and the
/// standard Jinja way to say "not for you" is a raw block.
///
/// That leaves this provider needing to honour raw blocks too, for two
/// different reasons:
///
/// - SQL loaded with `fn::readFile` never reaches the YAML renderer, so a raw
///   block in such a file is addressed to *this* layer and means what dbt means
///   by it: the contents are literal.
/// - SQL written inline has already had its markers consumed by the YAML
///   renderer, so anything still present arrived deliberately.
///
/// Either way the contents must survive untouched. Previously they did not:
/// the markers were left in the SQL and shipped to BigQuery, while tags
/// *inside* them were still expanded — the worst of both readings.
///
/// The region is carried through the pipeline base64-encoded, so no later stage
/// can mistake its contents for a template, and restored at the end.
pub fn protect_raw_regions(sql: &str) -> Result<String, PreprocessError> {
    if sql.contains('\u{0}') {
        return Err(PreprocessError::UnsupportedInput {
            reason: "SQL may not contain NUL bytes".to_owned(),
        });
    }
    if !contains_raw_tag(sql) {
        return Ok(sql.to_owned());
    }

    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    loop {
        let Some((before, after_open)) = split_at_tag(rest, TagKind::Raw) else {
            out.push_str(rest);
            return Ok(out);
        };
        out.push_str(before);
        match split_at_tag(after_open, TagKind::EndRaw) {
            Some((body, after_close)) => {
                out.push_str(RAW_SENTINEL_PREFIX);
                out.push_str(&encode_raw(body));
                out.push(RAW_SENTINEL_SUFFIX);
                rest = after_close;
            }
            None => {
                // An unclosed raw block: emit the remainder as-is rather than
                // silently swallowing the query.
                out.push_str(&rest[before.len()..]);
                return Ok(out);
            }
        }
    }
}

/// Put the raw regions back, markers gone and contents exactly as written.
pub fn restore_raw_regions(text: &str) -> String {
    if !text.contains(RAW_SENTINEL_PREFIX) {
        return text.to_owned();
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find(RAW_SENTINEL_PREFIX) {
        out.push_str(&rest[..start]);
        let payload_start = start + RAW_SENTINEL_PREFIX.len();
        match rest[payload_start..].find(RAW_SENTINEL_SUFFIX) {
            Some(len) => {
                let payload = &rest[payload_start..payload_start + len];
                out.push_str(&decode_raw(payload));
                rest = &rest[payload_start + len + RAW_SENTINEL_SUFFIX.len_utf8()..];
            }
            None => {
                out.push_str(&rest[start..]);
                return out;
            }
        }
    }
    out.push_str(rest);
    out
}

/// Whether the entire body is one raw region.
///
/// A model that is *entirely* literal has no reason to be a dbt model, so this
/// is the one shape that can only be a mistake: someone learned the raw idiom
/// for inline SQL and then moved the SQL into a file, where the markers are no
/// longer needed and now suppress the templating they wanted. Detected so the
/// error can say that, instead of BigQuery rejecting unresolved `ref()` text.
pub fn is_entirely_raw(sql: &str) -> bool {
    let trimmed = sql.trim();
    let Some((before, after_open)) = split_at_tag(trimmed, TagKind::Raw) else {
        return false;
    };
    if !before.trim().is_empty() {
        return false;
    }
    match split_at_tag(after_open, TagKind::EndRaw) {
        Some((_, after_close)) => after_close.trim().is_empty(),
        None => false,
    }
}

fn contains_raw_tag(sql: &str) -> bool {
    split_at_tag(sql, TagKind::Raw).is_some()
}

fn encode_raw(body: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD_NO_PAD.encode(body.as_bytes())
}

fn decode_raw(payload: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(payload.as_bytes())
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
        .unwrap_or_default()
}

// ── Feature 2: `{{ this }}` ─────────────────────────────────────────

/// Replace `{{ this }}` with the backtick-quoted table reference.
pub fn replace_this(sql: &str, table_ref: &str) -> String {
    // Handle both `{{ this }}` and `{{this}}` variants.
    let s = sql.replace("{{ this }}", table_ref);
    s.replace("{{this}}", table_ref)
}

// ── Feature 1: `{{ var('...') }}` ───────────────────────────────────

/// Expand `var('name')` calls with project-level variable values.
/// Works both inside `{{ }}` blocks and standalone.
pub fn expand_vars(sql: &str, vars: &BTreeMap<String, String>) -> Result<String, PreprocessError> {
    // Quick check: if no var( pattern exists, skip scanning.
    if !sql.contains("var(") {
        return Ok(sql.to_owned());
    }

    let mut result = String::with_capacity(sql.len());
    let mut remaining = sql;

    loop {
        // Find var(' or var("
        match find_var_call(remaining) {
            Some((prefix_end, name, after_call)) => {
                result.push_str(&remaining[..prefix_end]);

                let value = vars.get(name).ok_or_else(|| {
                    let available = vars
                        .keys()
                        .map(|k| k.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    PreprocessError::UnknownVar {
                        name: name.to_owned(),
                        available,
                    }
                })?;

                result.push_str(value);
                remaining = after_call;
            }
            None => {
                result.push_str(remaining);
                return Ok(result);
            }
        }
    }
}

/// Find the next `var('name')` or `var("name")` call.
/// Returns (position_of_var_start, extracted_name, rest_after_closing_paren).
fn find_var_call(s: &str) -> Option<(usize, &str, &str)> {
    let mut search_from = 0;
    loop {
        let pos = s[search_from..].find("var(")?;
        let abs_pos = search_from + pos;
        let after_paren = &s[abs_pos + 4..];

        // Check for quoted name.
        let (quote, rest) = if let Some(stripped) = after_paren.strip_prefix('\'') {
            ('\'', stripped)
        } else if let Some(stripped) = after_paren.strip_prefix('"') {
            ('"', stripped)
        } else {
            search_from = abs_pos + 4;
            continue;
        };

        // Find closing quote.
        if let Some(end_quote) = rest.find(quote) {
            let name = &rest[..end_quote];
            let after_quote = &rest[end_quote + 1..];
            // Expect closing paren, possibly with whitespace.
            let trimmed = after_quote.trim_start();
            if let Some(after_close) = trimmed.strip_prefix(')') {
                return Some((abs_pos, name, after_close));
            }
        }

        search_from = abs_pos + 4;
        if search_from >= s.len() {
            return None;
        }
    }
}

// ── Feature 3: dbt_utils built-in macros ────────────────────────────

/// Expand dbt_utils.* macro calls to BigQuery SQL.
/// Iterative: max 10 rounds to handle nested calls.
pub fn expand_builtins(sql: &str) -> String {
    let mut current = sql.to_owned();

    for _ in 0..10 {
        let next = expand_builtins_once(&current);
        if next == current {
            return next;
        }
        current = next;
    }

    current
}

fn expand_builtins_once(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut remaining = sql;

    loop {
        match remaining.find("dbt_utils.") {
            Some(pos) => {
                result.push_str(&remaining[..pos]);
                let after_prefix = &remaining[pos + 10..]; // skip "dbt_utils."

                if let Some(expanded) = try_expand_builtin(after_prefix) {
                    result.push_str(&expanded.replacement);
                    remaining = expanded.rest;
                } else {
                    // Not a recognized builtin — emit as-is.
                    result.push_str("dbt_utils.");
                    remaining = after_prefix;
                }
            }
            None => {
                result.push_str(remaining);
                return result;
            }
        }
    }
}

struct BuiltinExpansion<'a> {
    replacement: String,
    rest: &'a str,
}

fn try_expand_builtin(s: &str) -> Option<BuiltinExpansion<'_>> {
    // current_timestamp()
    if let Some(rest) = s.strip_prefix("current_timestamp()") {
        return Some(BuiltinExpansion {
            replacement: "CURRENT_TIMESTAMP()".to_owned(),
            rest,
        });
    }

    // datediff(start, end, part)
    if let Some(rest) = s.strip_prefix("datediff(") {
        let (args, after) = extract_balanced_args(rest)?;
        let parts = split_top_level_args(&args);
        if parts.len() == 3 {
            let start = parts[0].trim();
            let end = parts[1].trim();
            let part = parts[2].trim().trim_matches('\'').trim_matches('"');
            return Some(BuiltinExpansion {
                replacement: format!(
                    "TIMESTAMP_DIFF({}, {}, {})",
                    end,
                    start,
                    part.to_uppercase()
                ),
                rest: after,
            });
        }
    }

    // dateadd(part, amount, expr)
    if let Some(rest) = s.strip_prefix("dateadd(") {
        let (args, after) = extract_balanced_args(rest)?;
        let parts = split_top_level_args(&args);
        if parts.len() == 3 {
            let part = parts[0].trim().trim_matches('\'').trim_matches('"');
            let amount = parts[1].trim();
            let expr = parts[2].trim();
            return Some(BuiltinExpansion {
                replacement: format!(
                    "TIMESTAMP_ADD({}, INTERVAL {} {})",
                    expr,
                    amount,
                    part.to_uppercase()
                ),
                rest: after,
            });
        }
    }

    // generate_surrogate_key(['col1', 'col2'])
    if let Some(rest) = s.strip_prefix("generate_surrogate_key(") {
        let (args, after) = extract_balanced_args(rest)?;
        let inner = args.trim();
        // Parse the list: ['col1', 'col2'] or ['col1']
        let cols = parse_surrogate_key_cols(inner);
        if !cols.is_empty() {
            let concat_parts: Vec<String> = cols
                .iter()
                .map(|c| {
                    format!(
                        "COALESCE(CAST({} AS STRING), '_dbt_utils_surrogate_key_null_')",
                        c
                    )
                })
                .collect();
            let concat_expr = if concat_parts.len() == 1 {
                concat_parts.into_iter().next().unwrap()
            } else {
                format!("CONCAT({})", concat_parts.join(", "))
            };
            return Some(BuiltinExpansion {
                replacement: format!("TO_HEX(MD5({}))", concat_expr),
                rest: after,
            });
        }
    }

    // listagg(measure, delimiter, order_by)
    if let Some(rest) = s.strip_prefix("listagg(") {
        let (args, after) = extract_balanced_args(rest)?;
        let parts = split_top_level_args(&args);
        if parts.len() >= 2 {
            let measure = parts[0].trim();
            let delimiter = parts[1].trim();
            if parts.len() >= 3 {
                let order_by = parts[2].trim();
                return Some(BuiltinExpansion {
                    replacement: format!(
                        "STRING_AGG({}, {} ORDER BY {})",
                        measure, delimiter, order_by
                    ),
                    rest: after,
                });
            }
            return Some(BuiltinExpansion {
                replacement: format!("STRING_AGG({}, {})", measure, delimiter),
                rest: after,
            });
        }
    }

    None
}

/// Extract balanced parenthesized content: finds matching `)` accounting for nesting.
/// Returns (inner_content, rest_after_close_paren).
fn extract_balanced_args(s: &str) -> Option<(String, &str)> {
    let mut depth = 1;
    let mut bracket_depth = 0;
    let bytes = s.as_bytes();
    let mut i = 0;

    while i < bytes.len() && depth > 0 {
        match bytes[i] {
            b'(' => depth += 1,
            b')' if bracket_depth == 0 => depth -= 1,
            b'[' => bracket_depth += 1,
            b']' => {
                if bracket_depth > 0 {
                    bracket_depth -= 1;
                }
            }
            b'\'' => {
                // Skip quoted string.
                i += 1;
                while i < bytes.len() && bytes[i] != b'\'' {
                    if bytes[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        if depth > 0 {
            i += 1;
        }
    }

    if depth == 0 {
        let inner = s[..i].to_owned();
        let rest = &s[i + 1..]; // skip the closing ')'
        Some((inner, rest))
    } else {
        None
    }
}

/// Split args at top-level commas (not inside parens or brackets).
fn split_top_level_args(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0;
    let mut bracket_depth = 0;
    let mut start = 0;
    let bytes = s.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            b'[' => bracket_depth += 1,
            b']' => {
                if bracket_depth > 0 {
                    bracket_depth -= 1;
                }
            }
            b'\'' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'\'' {
                    if bytes[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
            }
            b',' if depth == 0 && bracket_depth == 0 => {
                result.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }

    if start < s.len() {
        result.push(&s[start..]);
    }

    result
}

/// Parse columns from surrogate key list: `['col1', 'col2']` or `['col1.col2']`
fn parse_surrogate_key_cols(s: &str) -> Vec<&str> {
    let trimmed = s.trim();
    let inner = if trimmed.starts_with('[') && trimmed.ends_with(']') {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    };

    inner
        .split(',')
        .map(|c| c.trim().trim_matches('\'').trim_matches('"').trim())
        .filter(|c| !c.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A raw block means "not a template". Previously the markers were shipped
    /// to BigQuery and the tags inside them were expanded anyway.
    #[test]
    fn raw_regions_survive_untouched_and_lose_their_markers() {
        let sql =
            "{% raw %}SELECT {{ ref('x') }} {% if is_incremental() %}W{% endif %}{% endraw %}";
        let out = preprocess(sql, &BTreeMap::new(), "`p.d.t`", false).unwrap();
        let restored = restore_raw_regions(&out);
        assert_eq!(
            restored, "SELECT {{ ref('x') }} {% if is_incremental() %}W{% endif %}",
            "raw contents must come back exactly as written, without markers"
        );
    }

    #[test]
    fn templates_outside_a_raw_region_are_still_rendered() {
        // The point of protecting regions rather than whole files: the rest of
        // the model still behaves like a model.
        let sql = "SELECT 1 {% if is_incremental() %}WHERE live{% endif %} \
                   AND note = '{% raw %}{{ not_a_ref }}{% endraw %}'";
        let rendered =
            restore_raw_regions(&preprocess(sql, &BTreeMap::new(), "`p.d.t`", true).unwrap());
        assert!(rendered.contains("WHERE live"), "{rendered}");
        assert!(rendered.contains("{{ not_a_ref }}"), "{rendered}");
        assert!(!rendered.contains("{% raw"), "{rendered}");
    }

    #[test]
    fn a_variable_inside_a_raw_region_is_not_substituted() {
        let vars: BTreeMap<String, String> = [("d".to_owned(), "'2026-01-01'".to_owned())]
            .into_iter()
            .collect();
        let out = restore_raw_regions(
            &preprocess("{% raw %}{{ var('d') }}{% endraw %}", &vars, "`t`", false).unwrap(),
        );
        assert_eq!(out, "{{ var('d') }}");
    }

    #[test]
    fn an_unknown_variable_inside_a_raw_region_is_not_an_error() {
        // Outside a raw region this is a hard error naming the variable; inside
        // one it is just text.
        let out = preprocess(
            "{% raw %}{{ var('nope') }}{% endraw %}",
            &BTreeMap::new(),
            "`t`",
            false,
        );
        assert!(
            out.is_ok(),
            "raw contents must not be validated as templates"
        );
    }

    #[test]
    fn multiple_raw_regions_are_each_preserved() {
        let sql = "{% raw %}{{ a }}{% endraw %} middle {% raw %}{{ b }}{% endraw %}";
        let out = restore_raw_regions(&preprocess(sql, &BTreeMap::new(), "`t`", false).unwrap());
        assert_eq!(out, "{{ a }} middle {{ b }}");
    }

    #[test]
    fn raw_tags_tolerate_whitespace_and_trim_markers() {
        for (open, close) in [
            ("{% raw %}", "{% endraw %}"),
            ("{%raw%}", "{%endraw%}"),
            ("{%- raw -%}", "{%- endraw -%}"),
            ("{%  raw  %}", "{%  endraw  %}"),
        ] {
            let sql = format!("{open}{{{{ x }}}}{close}");
            let out =
                restore_raw_regions(&preprocess(&sql, &BTreeMap::new(), "`t`", false).unwrap());
            assert_eq!(out, "{{ x }}", "for {open}");
        }
    }

    #[test]
    fn an_unclosed_raw_block_keeps_the_users_sql() {
        let sql = "SELECT 1 {% raw %} trailing";
        let out = preprocess(sql, &BTreeMap::new(), "`t`", false).unwrap();
        assert!(out.contains("trailing"), "dropped user SQL: {out}");
    }

    #[test]
    fn nul_bytes_are_rejected_rather_than_trusted() {
        // The sentinel is NUL-delimited, so input containing one could otherwise
        // forge a region.
        let err = preprocess("SELECT '\u{0}RAW:aGk\u{0}'", &BTreeMap::new(), "`t`", false)
            .expect_err("NUL should be rejected");
        assert!(err.to_string().contains("NUL"), "{err}");
    }

    #[test]
    fn sql_without_raw_regions_is_unaffected() {
        let sql = "SELECT a FROM `p.d.t` WHERE b = 'x'";
        assert_eq!(
            preprocess(sql, &BTreeMap::new(), "`t`", false).unwrap(),
            sql
        );
    }

    /// B6: the matcher accepted exactly one spelling, so every other formatting
    /// leaked Jinja into the emitted SQL and BigQuery rejected it with a syntax
    /// error that pointed at the wrong thing.
    #[test]
    fn regression_b6_incremental_tag_tolerates_whitespace_variants() {
        for open_tag in [
            "{% if is_incremental() %}",
            "{%if is_incremental()%}",
            "{%  if   is_incremental()  %}",
            "{%- if is_incremental() -%}",
            "{% if is_incremental( ) %}",
            "{% IF is_incremental() %}",
        ] {
            let sql = format!("SELECT 1 {open_tag} WHERE ts > x {{% endif %}}");
            let dropped = handle_incremental_blocks(&sql, false);
            assert!(
                !dropped.contains("{%") && !dropped.contains("WHERE"),
                "variant {open_tag:?} was not recognised: {dropped}"
            );
            let kept = handle_incremental_blocks(&sql, true);
            assert!(
                !kept.contains("{%") && kept.contains("WHERE"),
                "variant {open_tag:?} did not retain its body: {kept}"
            );
        }
    }

    #[test]
    fn regression_b6_endif_tolerates_whitespace_variants() {
        for close_tag in ["{% endif %}", "{%endif%}", "{%  endif  %}", "{%- endif -%}"] {
            let sql = format!("A {{% if is_incremental() %}}B{close_tag} C");
            let out = handle_incremental_blocks(&sql, false);
            assert!(
                !out.contains("{%"),
                "close {close_tag:?} unrecognised: {out}"
            );
            assert!(out.contains('A') && out.contains('C') && !out.contains('B'));
        }
    }

    #[test]
    fn degenerate_tag_delimiters_do_not_panic() {
        // `{%}` lets a naive search find the closer overlapping the opener,
        // producing an inverted slice range. Found by the never-panics property.
        for sql in ["{%}", "{%", "%}", "{%}{%}", "{% if is_incremental() {%}"] {
            let _ = handle_incremental_blocks(sql, true);
            let _ = handle_incremental_blocks(sql, false);
        }
    }

    #[test]
    fn unrelated_jinja_tags_are_left_alone() {
        // Only the incremental construct is handled here; anything else must
        // survive for a later stage to deal with.
        let sql = "{% set x = 1 %} SELECT {{ x }}";
        assert_eq!(handle_incremental_blocks(sql, false), sql);
    }

    #[test]
    fn unclosed_block_preserves_user_sql() {
        // Discarding the rest of the query would silently change its meaning.
        let sql = "SELECT 1 {% if is_incremental() %} WHERE ts > x";
        let out = handle_incremental_blocks(sql, false);
        assert!(out.contains("WHERE ts > x"), "dropped user SQL: {out}");
    }

    // ── var expansion ────────────────────────────────────────────────

    #[test]
    fn expand_vars_simple() {
        let mut vars = BTreeMap::new();
        vars.insert("days".to_owned(), "90".to_owned());
        let result = expand_vars("INTERVAL var('days') DAY", &vars).unwrap();
        assert_eq!(result, "INTERVAL 90 DAY");
    }

    #[test]
    fn expand_vars_double_quoted() {
        let mut vars = BTreeMap::new();
        vars.insert("min_orders".to_owned(), "2".to_owned());
        let result = expand_vars("WHERE orders >= var(\"min_orders\")", &vars).unwrap();
        assert_eq!(result, "WHERE orders >= 2");
    }

    #[test]
    fn expand_vars_multiple() {
        let mut vars = BTreeMap::new();
        vars.insert("a".to_owned(), "10".to_owned());
        vars.insert("b".to_owned(), "20".to_owned());
        let result = expand_vars("var('a') + var('b')", &vars).unwrap();
        assert_eq!(result, "10 + 20");
    }

    #[test]
    fn expand_vars_unknown_error() {
        let vars = BTreeMap::new();
        let result = expand_vars("var('missing')", &vars);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unknown variable 'missing'"));
    }

    #[test]
    fn expand_vars_empty_passthrough() {
        let vars = BTreeMap::new();
        let result = expand_vars("SELECT 1", &vars).unwrap();
        assert_eq!(result, "SELECT 1");
    }

    // ── is_incremental blocks ────────────────────────────────────────

    #[test]
    fn handle_incremental_true() {
        let sql = "SELECT * FROM t {% if is_incremental() %}WHERE updated > max{% endif %}";
        let result = handle_incremental_blocks(sql, true);
        assert_eq!(result, "SELECT * FROM t WHERE updated > max");
    }

    #[test]
    fn handle_incremental_false() {
        let sql = "SELECT * FROM t {% if is_incremental() %}WHERE updated > max{% endif %}";
        let result = handle_incremental_blocks(sql, false);
        assert_eq!(result, "SELECT * FROM t ");
    }

    #[test]
    fn handle_incremental_no_block() {
        let sql = "SELECT * FROM t";
        let result = handle_incremental_blocks(sql, true);
        assert_eq!(result, "SELECT * FROM t");
    }

    #[test]
    fn handle_incremental_unclosed() {
        let sql = "SELECT * {% if is_incremental() %}WHERE x";
        let result = handle_incremental_blocks(sql, true);
        assert_eq!(result, "SELECT * {% if is_incremental() %}WHERE x");
    }

    // ── {{ this }} replacement ───────────────────────────────────────

    #[test]
    fn this_replacement() {
        let sql = "SELECT MAX(ts) FROM {{ this }}";
        let result = replace_this(sql, "`p.d.t`");
        assert_eq!(result, "SELECT MAX(ts) FROM `p.d.t`");
    }

    #[test]
    fn this_replacement_tight() {
        let sql = "SELECT MAX(ts) FROM {{this}}";
        let result = replace_this(sql, "`p.d.t`");
        assert_eq!(result, "SELECT MAX(ts) FROM `p.d.t`");
    }

    // ── dbt_utils builtins ──────────────────────────────────────────

    #[test]
    fn dbt_utils_current_timestamp() {
        let result = expand_builtins("dbt_utils.current_timestamp()");
        assert_eq!(result, "CURRENT_TIMESTAMP()");
    }

    #[test]
    fn dbt_utils_datediff() {
        let result = expand_builtins("dbt_utils.datediff('a', 'b', 'day')");
        assert_eq!(result, "TIMESTAMP_DIFF('b', 'a', DAY)");
    }

    #[test]
    fn dbt_utils_datediff_expressions() {
        let result = expand_builtins(
            "dbt_utils.datediff('c.created_at', dbt_utils.current_timestamp(), 'day')",
        );
        assert_eq!(
            result,
            "TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), 'c.created_at', DAY)"
        );
    }

    #[test]
    fn dbt_utils_dateadd() {
        let result = expand_builtins("dbt_utils.dateadd('day', -90, expr)");
        assert_eq!(result, "TIMESTAMP_ADD(expr, INTERVAL -90 DAY)");
    }

    #[test]
    fn dbt_utils_generate_surrogate_key_single() {
        let result = expand_builtins("dbt_utils.generate_surrogate_key(['c.customer_id'])");
        assert_eq!(
            result,
            "TO_HEX(MD5(COALESCE(CAST(c.customer_id AS STRING), '_dbt_utils_surrogate_key_null_')))"
        );
    }

    #[test]
    fn dbt_utils_generate_surrogate_key_multi() {
        let result = expand_builtins("dbt_utils.generate_surrogate_key(['id', 'email'])");
        assert!(result.starts_with("TO_HEX(MD5(CONCAT("));
        assert!(result.contains("CAST(id AS STRING)"));
        assert!(result.contains("CAST(email AS STRING)"));
    }

    #[test]
    fn dbt_utils_listagg() {
        let result = expand_builtins("dbt_utils.listagg(name, ', ', created_at)");
        assert_eq!(result, "STRING_AGG(name, ', ' ORDER BY created_at)");
    }

    #[test]
    fn dbt_utils_listagg_no_order() {
        let result = expand_builtins("dbt_utils.listagg(name, ', ')");
        assert_eq!(result, "STRING_AGG(name, ', ')");
    }

    #[test]
    fn nested_builtins() {
        let result = expand_builtins(
            "dbt_utils.datediff('c.created_at', dbt_utils.current_timestamp(), 'day')",
        );
        assert!(result.contains("CURRENT_TIMESTAMP()"));
        assert!(result.contains("TIMESTAMP_DIFF"));
    }

    #[test]
    fn builtin_in_larger_sql() {
        let sql = "SELECT id, dbt_utils.current_timestamp() AS ts, dbt_utils.datediff('a', 'b', 'day') AS diff FROM t";
        let result = expand_builtins(sql);
        assert!(result.contains("CURRENT_TIMESTAMP()"));
        assert!(result.contains("TIMESTAMP_DIFF"));
        assert!(result.contains("FROM t"));
    }

    // ── Full pipeline ────────────────────────────────────────────────

    #[test]
    fn full_preprocess_pipeline() {
        let mut vars = BTreeMap::new();
        vars.insert("days".to_owned(), "90".to_owned());

        let sql = "SELECT * FROM t \
                    {% if is_incremental() %}WHERE ts > (SELECT MAX(ts) FROM {{ this }}){% endif %}";

        // is_incremental = true
        let result = preprocess(sql, &vars, "`p.d.t`", true).unwrap();
        assert!(result.contains("`p.d.t`"));
        assert!(result.contains("WHERE ts >"));

        // is_incremental = false
        let result = preprocess(sql, &vars, "`p.d.t`", false).unwrap();
        assert!(!result.contains("WHERE ts >"));
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn preprocess_never_panics(sql in "[ -~]{0,500}") {
            let vars = BTreeMap::new();
            let _ = preprocess(&sql, &vars, "`p.d.t`", false);
            let _ = preprocess(&sql, &vars, "`p.d.t`", true);
        }

        #[test]
        fn handle_incremental_blocks_never_panics(sql in "[ -~]{0,500}") {
            let _ = std::hint::black_box(handle_incremental_blocks(&sql, true));
            let _ = std::hint::black_box(handle_incremental_blocks(&sql, false));
        }

        #[test]
        fn replace_this_never_panics(
            sql in "[ -~]{0,200}",
            table_ref in "[ -~]{0,50}",
        ) {
            let _ = std::hint::black_box(replace_this(&sql, &table_ref));
        }

        #[test]
        fn expand_vars_never_panics(sql in "[ -~]{0,300}") {
            let mut vars = BTreeMap::new();
            vars.insert("x".to_owned(), "42".to_owned());
            vars.insert("days".to_owned(), "90".to_owned());
            // Should either succeed or return an error, never panic
            let _ = expand_vars(&sql, &vars);
        }

        #[test]
        fn expand_builtins_never_panics(sql in "[ -~]{0,500}") {
            let _ = std::hint::black_box(expand_builtins(&sql));
        }
    }
}
