// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Grounding an agent in resources the stack already declares.
//!
//! An agent is only as good as what it knows about the data. That knowledge —
//! which tables exist, what the columns mean, how the tables join, what
//! functions are available — is exactly what the BigQuery and dbt resources in
//! this provider already describe. Restating it in the agent's configuration
//! would mean maintaining it twice and letting the two drift.
//!
//! So it is derived instead:
//!
//! - a dbt model's output carries its table reference, which becomes a
//!   datasource;
//! - a model's declared refs describe how models relate, which become schema
//!   relationships;
//! - a table schema's column descriptions become glossary terms;
//! - a BigQuery routine becomes a callable user function.
//!
//! Each of those is also a Pulumi dependency edge, so an agent cannot be
//! published before the data it claims to describe exists.

use crate::types::{GlossaryTerm, TableRef};

/// Parse a `project.dataset.table` reference, with or without backticks.
///
/// Model outputs carry the quoted form because that is what gets embedded in
/// SQL; the API wants the three parts separately.
pub fn parse_table_ref(qualified: &str) -> Option<TableRef<'_>> {
    let trimmed = qualified.trim().trim_matches('`');
    let mut parts = trimmed.split('.');
    let project = parts.next()?;
    let dataset = parts.next()?;
    let table = parts.next()?;
    if parts.next().is_some() || project.is_empty() || dataset.is_empty() || table.is_empty() {
        return None;
    }
    Some(TableRef {
        project,
        dataset,
        table,
    })
}

/// Collect datasource table references from dbt model outputs.
///
/// Ephemeral models are skipped: they are inlined as CTEs and have no table for
/// an agent to query. Pointing an agent at one would produce SQL referencing a
/// table that does not exist.
pub fn tables_from_model_refs<'a, I>(models: I) -> Vec<TableRef<'a>>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut out = Vec::new();
    for (materialization, table_ref) in models {
        if materialization.eq_ignore_ascii_case("ephemeral") {
            continue;
        }
        if let Some(t) = parse_table_ref(table_ref) {
            if !out.contains(&t) {
                out.push(t);
            }
        }
    }
    out.sort();
    out
}

/// Derive glossary terms from column descriptions.
///
/// A description is only worth sending if it says more than the column name
/// already does; an empty one, or one that merely restates the name, adds
/// nothing but tokens.
pub fn glossary_from_columns<'a, I>(columns: I) -> Vec<GlossaryTerm<'a>>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    columns
        .into_iter()
        .filter(|(name, description)| {
            !description.trim().is_empty() && !description.eq_ignore_ascii_case(name)
        })
        .map(|(name, description)| GlossaryTerm {
            term: name,
            description,
            synonyms: Vec::new(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_backticked_and_bare_table_refs() {
        // Model outputs carry the quoted form because it is embedded in SQL.
        for input in ["`p.d.t`", "p.d.t", "  `p.d.t`  "] {
            let t = parse_table_ref(input).expect(input);
            assert_eq!((t.project, t.dataset, t.table), ("p", "d", "t"));
        }
    }

    #[test]
    fn rejects_malformed_table_refs() {
        for input in ["", "p", "p.d", "p.d.t.x", "..", "p..t", ".d.t"] {
            assert!(parse_table_ref(input).is_none(), "accepted {input:?}");
        }
    }

    #[test]
    fn ephemeral_models_are_not_offered_as_datasources() {
        // An ephemeral model is inlined as a CTE and has no table; pointing an
        // agent at one yields SQL against something that does not exist.
        let tables = tables_from_model_refs([
            ("table", "`p.d.mart_revenue`"),
            ("ephemeral", "`p.d.int_joined`"),
            ("view", "`p.d.stg_orders`"),
        ]);
        let names: Vec<_> = tables.iter().map(|t| t.table).collect();
        assert!(names.contains(&"mart_revenue"));
        assert!(names.contains(&"stg_orders"));
        assert!(!names.contains(&"int_joined"));
    }

    #[test]
    fn duplicate_datasources_are_collapsed() {
        let tables = tables_from_model_refs([("table", "`p.d.t`"), ("table", "p.d.t")]);
        assert_eq!(tables.len(), 1);
    }

    #[test]
    fn glossary_skips_descriptions_that_add_nothing() {
        // Sending a description that restates the column name spends tokens to
        // tell the model what it already knows.
        let terms = glossary_from_columns([
            ("arr", "annual recurring revenue"),
            ("user_id", ""),
            ("region", "   "),
            ("status", "status"),
        ]);
        assert_eq!(terms.len(), 1);
        assert_eq!(terms[0].term, "arr");
    }
}
