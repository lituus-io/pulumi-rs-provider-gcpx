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

use crate::types::{GlossaryTerm, SchemaRelationship, TableRef, UserFunction};

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

/// Derive join relationships from a model's declared refs.
///
/// The join column is inferred from the referenced model's name (`stg_users` →
/// `user_id`), which is the dbt convention and right often enough to be worth
/// offering. It is a hint, not a constraint: an explicit relationship declared
/// on the resource always wins.
pub fn relationships_from_refs<'a>(
    model_table: &'a str,
    referenced: &[(&'a str, &'a str)],
) -> Vec<SchemaRelationship<'a>> {
    referenced
        .iter()
        .filter_map(|(name, table)| {
            let key = singular_key(name)?;
            Some(SchemaRelationship {
                left_table: model_table,
                left_column: key.0,
                right_table: table,
                right_column: key.1,
                relationship_type: "many_to_one",
            })
        })
        .collect()
}

/// `stg_users` → (`user_id`, `user_id`). Returns `None` when no convention applies.
fn singular_key(model_name: &str) -> Option<(&'static str, &'static str)> {
    // Deliberately conservative: only the unambiguous, conventional cases, so a
    // wrong guess never ends up in an agent's context.
    let bare = model_name
        .trim_start_matches("stg_")
        .trim_start_matches("dim_")
        .trim_start_matches("fct_")
        .trim_start_matches("int_");
    match bare {
        "users" | "user" => Some(("user_id", "user_id")),
        "customers" | "customer" => Some(("customer_id", "customer_id")),
        "orders" | "order" => Some(("order_id", "order_id")),
        "products" | "product" => Some(("product_id", "product_id")),
        "accounts" | "account" => Some(("account_id", "account_id")),
        _ => None,
    }
}

/// Turn a BigQuery routine into a function the agent may call.
pub fn user_function<'a>(
    name: &'a str,
    description: &'a str,
    signature: &'a str,
) -> UserFunction<'a> {
    UserFunction {
        name,
        description,
        signature,
    }
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

    #[test]
    fn relationships_follow_the_naming_convention_where_it_is_unambiguous() {
        let rels = relationships_from_refs(
            "p.d.fct_orders",
            &[
                ("stg_users", "p.d.stg_users"),
                ("stg_orders", "p.d.stg_orders"),
            ],
        );
        let users = rels
            .iter()
            .find(|r| r.right_table == "p.d.stg_users")
            .unwrap();
        assert_eq!(users.left_column, "user_id");
        assert_eq!(users.relationship_type, "many_to_one");
    }

    #[test]
    fn unrecognised_model_names_produce_no_relationship() {
        // A wrong guess in an agent's context is worse than no guess: it makes
        // the agent join on a column that may not exist.
        let rels = relationships_from_refs("p.d.x", &[("weekly_rollup", "p.d.weekly_rollup")]);
        assert!(rels.is_empty());
    }

    #[test]
    fn conventional_prefixes_are_stripped_before_matching() {
        for name in [
            "stg_users",
            "dim_users",
            "fct_users",
            "int_users",
            "users",
            "user",
        ] {
            assert!(
                singular_key(name).is_some(),
                "{name} should match the convention"
            );
        }
    }
}
