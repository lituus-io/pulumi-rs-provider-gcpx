// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Reading agent resource inputs out of the Pulumi property map.
//!
//! Every borrow points into the incoming `prost_types::Struct`, so parsing a
//! resource allocates only for the collections that hold the borrows.

use std::collections::BTreeMap;

use gcpx_core::prost_util::{
    get_bool, get_list, get_number, get_str, get_string_list, value_as_str,
};
use prost_types::Struct;

use crate::grounding;
use crate::types::*;

type Fields = BTreeMap<String, prost_types::Value>;

fn struct_fields(v: &prost_types::Value) -> Option<&Fields> {
    match &v.kind {
        Some(prost_types::value::Kind::StructValue(s)) => Some(&s.fields),
        _ => None,
    }
}

fn nested<'a>(fields: &'a Fields, key: &str) -> Option<&'a Fields> {
    fields.get(key).and_then(struct_fields)
}

fn labels_of<'a>(fields: &'a Fields, key: &str) -> BTreeMap<&'a str, &'a str> {
    nested(fields, key)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| value_as_str(v).map(|s| (k.as_str(), s)))
                .collect()
        })
        .unwrap_or_default()
}

/// Parse a `DataAgent`'s inputs, including everything derived from other
/// resources in the stack.
pub fn parse_data_agent(props: &Struct) -> Result<DataAgentInputs<'_>, String> {
    let f = &props.fields;
    let project = get_str(f, "project").ok_or("project is required")?;
    let location = get_str(f, "location").unwrap_or("global");
    let agent_id = get_str(f, "agentId").ok_or("agentId is required")?;

    Ok(DataAgentInputs {
        project,
        location,
        agent_id,
        display_name: get_str(f, "displayName"),
        description: get_str(f, "description"),
        labels: labels_of(f, "labels"),
        kms_key: get_str(f, "kmsKey"),
        context: parse_context(f),
        // Publishing is opt-in. Defaulting it on would make every edit live the
        // moment it deploys, with no way to stage a change for review.
        publish: get_bool(f, "publish").unwrap_or(false),
    })
}

fn parse_context(f: &Fields) -> AgentContext<'_> {
    AgentContext {
        system_instruction: get_str(f, "systemInstruction").unwrap_or(""),
        datasources: parse_datasources(f),
        options: AgentOptions {
            chart_rendering: get_str(f, "chartRendering"),
            python_analysis: get_bool(f, "pythonAnalysis"),
            model: get_str(f, "model"),
        },
        example_queries: parse_example_queries(f),
        glossary_terms: parse_glossary(f),
    }
}

/// Datasources come from three places, in precedence order: explicit Looker
/// explores, explicit BigQuery tables, and dbt model outputs.
///
/// Model outputs are the interesting one — they arrive as the `modelOutput` of
/// a `gcpx:dbt/model:Model`, which means the agent's grounding is a dependency
/// edge rather than a hand-copied table name.
fn parse_datasources(f: &Fields) -> Datasources<'_> {
    if let Some(items) = get_list(f, "lookerExplores") {
        let explores: Vec<_> = items
            .iter()
            .filter_map(struct_fields)
            .filter_map(|e| {
                Some(ExploreRef {
                    looker_instance_uri: get_str(e, "lookerInstanceUri")?,
                    lookml_model: get_str(e, "lookmlModel")?,
                    explore: get_str(e, "explore")?,
                })
            })
            .collect();
        if !explores.is_empty() {
            return Datasources::Looker(explores);
        }
    }

    let mut tables: Vec<TableRef<'_>> = Vec::new();

    for qualified in get_string_list(f, "tables") {
        if let Some(t) = grounding::parse_table_ref(qualified) {
            tables.push(t);
        }
    }

    if let Some(items) = get_list(f, "models") {
        let pairs: Vec<(&str, &str)> = items
            .iter()
            .filter_map(struct_fields)
            .filter_map(|m| {
                Some((
                    get_str(m, "materialization").unwrap_or("table"),
                    get_str(m, "tableRef")?,
                ))
            })
            .collect();
        tables.extend(grounding::tables_from_model_refs(pairs));
    }

    tables.sort();
    tables.dedup();
    Datasources::BigQuery(tables)
}

fn parse_example_queries(f: &Fields) -> Vec<ExampleQuery<'_>> {
    get_list(f, "exampleQueries")
        .map(|items| {
            items
                .iter()
                .filter_map(struct_fields)
                .filter_map(|q| {
                    Some(ExampleQuery {
                        question: get_str(q, "question")?,
                        sql: get_str(q, "sql")?,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_glossary(f: &Fields) -> Vec<GlossaryTerm<'_>> {
    get_list(f, "glossaryTerms")
        .map(|items| {
            items
                .iter()
                .filter_map(struct_fields)
                .filter_map(|t| {
                    Some(GlossaryTerm {
                        term: get_str(t, "term")?,
                        description: get_str(t, "description").unwrap_or(""),
                        synonyms: get_string_list(t, "synonyms"),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

pub fn parse_conversation(props: &Struct) -> Result<ConversationInputs<'_>, String> {
    let f = &props.fields;
    Ok(ConversationInputs {
        project: get_str(f, "project").ok_or("project is required")?,
        location: get_str(f, "location").unwrap_or("global"),
        conversation_id: get_str(f, "conversationId").ok_or("conversationId is required")?,
        agents: get_string_list(f, "agents"),
        labels: labels_of(f, "labels"),
    })
}

pub fn parse_iam_policy(props: &Struct) -> Result<IamPolicyInputs<'_>, String> {
    let f = &props.fields;
    let bindings = get_list(f, "bindings")
        .map(|items| {
            items
                .iter()
                .filter_map(struct_fields)
                .filter_map(|b| {
                    Some(IamBinding {
                        role: get_str(b, "role")?,
                        members: get_string_list(b, "members"),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(IamPolicyInputs {
        project: get_str(f, "project").ok_or("project is required")?,
        location: get_str(f, "location").unwrap_or("global"),
        agent_id: get_str(f, "agentId").ok_or("agentId is required")?,
        bindings,
        // Additive by default. An authoritative policy silently revokes grants
        // this stack does not know about, which is not a reasonable default.
        authoritative: get_bool(f, "authoritative").unwrap_or(false),
    })
}

pub fn parse_agent_engine(props: &Struct) -> Result<AgentEngineInputs<'_>, String> {
    let f = &props.fields;
    Ok(AgentEngineInputs {
        project: get_str(f, "project").ok_or("project is required")?,
        location: get_str(f, "location").ok_or("location is required")?,
        engine_id: get_str(f, "engineId"),
        display_name: get_str(f, "displayName").ok_or("displayName is required")?,
        description: get_str(f, "description"),
        pickle_uri: get_str(f, "pickleUri"),
        requirements_uri: get_str(f, "requirementsUri"),
        dependency_files_uri: get_str(f, "dependencyFilesUri"),
        python_version: get_str(f, "pythonVersion"),
        env: labels_of(f, "env"),
        secret_env: labels_of(f, "secretEnv"),
    })
}

pub fn parse_memory(props: &Struct) -> Result<MemoryInputs<'_>, String> {
    let f = &props.fields;
    Ok(MemoryInputs {
        project: get_str(f, "project").ok_or("project is required")?,
        location: get_str(f, "location").ok_or("location is required")?,
        engine_id: get_str(f, "engineId").ok_or("engineId is required")?,
        memory_id: get_str(f, "memoryId"),
        fact: get_str(f, "fact").ok_or("fact is required")?,
        display_name: get_str(f, "displayName"),
        description: get_str(f, "description"),
        scope: labels_of(f, "scope"),
    })
}

pub fn parse_agent_eval(props: &Struct) -> Result<AgentEvalInputs<'_>, String> {
    let f = &props.fields;
    let golden_queries = get_list(f, "goldenQueries")
        .map(|items| {
            items
                .iter()
                .filter_map(struct_fields)
                .filter_map(|q| {
                    Some(GoldenQuery {
                        question: get_str(q, "question")?,
                        must_reference_tables: get_string_list(q, "mustReferenceTables"),
                        expect_sql_matches: get_str(q, "expectSqlMatches"),
                        expect_answer_contains: get_str(q, "expectAnswerContains"),
                        must_not_error: get_bool(q, "mustNotError").unwrap_or(true),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(AgentEvalInputs {
        project: get_str(f, "project").ok_or("project is required")?,
        location: get_str(f, "location").unwrap_or("global"),
        agent: get_str(f, "agent").ok_or("agent is required")?,
        golden_queries,
        fail_on_regression: get_bool(f, "failOnRegression").unwrap_or(true),
        // Bounded so an evaluation cannot open one connection per question and
        // trip the agent's own rate limits.
        max_concurrency: get_number(f, "maxConcurrency")
            .map(|n| (n as usize).clamp(1, 16))
            .unwrap_or(4),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcpx_core::prost_util::{prost_bool, prost_list, prost_number, prost_string, prost_struct};

    fn s(pairs: Vec<(&str, prost_types::Value)>) -> Struct {
        Struct {
            fields: pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect(),
        }
    }

    fn obj(pairs: Vec<(&str, prost_types::Value)>) -> prost_types::Value {
        prost_struct(pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect())
    }

    fn model_output(materialization: &str, table_ref: &str) -> prost_types::Value {
        obj(vec![
            ("materialization", prost_string(materialization)),
            ("tableRef", prost_string(table_ref)),
        ])
    }

    /// Inputs borrow from the property map, so the map has to outlive them.
    /// That is the cost — and the proof — of parsing without allocating.
    fn agent_props(extra: Vec<(&str, prost_types::Value)>) -> Struct {
        let mut pairs = vec![
            ("project", prost_string("p")),
            ("agentId", prost_string("a")),
        ];
        pairs.extend(extra);
        s(pairs)
    }

    #[test]
    fn agent_requires_project_and_id() {
        assert!(parse_data_agent(&s(vec![])).is_err());
        assert!(parse_data_agent(&s(vec![("project", prost_string("p"))])).is_err());
        assert!(parse_data_agent(&s(vec![("agentId", prost_string("a"))])).is_err());
    }

    #[test]
    fn location_defaults_to_global() {
        let props = agent_props(vec![]);
        assert_eq!(parse_data_agent(&props).unwrap().location, "global");
    }

    #[test]
    fn publishing_is_opt_in() {
        // Defaulting this on would make every edit live the instant it deploys,
        // with no way to stage a change for review first.
        let props = agent_props(vec![]);
        assert!(!parse_data_agent(&props).unwrap().publish);

        let props = agent_props(vec![("publish", prost_bool(true))]);
        assert!(parse_data_agent(&props).unwrap().publish);
    }

    #[test]
    fn dbt_model_outputs_become_datasources() {
        // The point of the integration: the agent is grounded on a model the
        // stack declares, not on a table name copied by hand.
        let props = agent_props(vec![(
            "models",
            prost_list(vec![
                model_output("table", "`p.d.mart_revenue`"),
                model_output("ephemeral", "`p.d.int_tmp`"),
            ]),
        )]);
        let a = parse_data_agent(&props).unwrap();
        match &a.context.datasources {
            Datasources::BigQuery(t) => {
                assert_eq!(t.len(), 1, "an ephemeral model has no table to query");
                assert_eq!(t[0].table, "mart_revenue");
            }
            other => panic!("expected BigQuery datasources, got {}", other.kind()),
        }
    }

    #[test]
    fn explicit_tables_and_models_merge_without_duplicates() {
        let props = agent_props(vec![
            ("tables", prost_list(vec![prost_string("p.d.t")])),
            ("models", prost_list(vec![model_output("table", "`p.d.t`")])),
        ]);
        let a = parse_data_agent(&props).unwrap();
        match &a.context.datasources {
            Datasources::BigQuery(t) => assert_eq!(t.len(), 1),
            other => panic!("expected BigQuery datasources, got {}", other.kind()),
        }
    }

    #[test]
    fn looker_explores_take_precedence_over_tables() {
        // The API models datasources as a union, so one has to win; naming
        // Looker explicitly is the more specific statement of intent.
        let props = agent_props(vec![
            ("tables", prost_list(vec![prost_string("p.d.t")])),
            (
                "lookerExplores",
                prost_list(vec![obj(vec![
                    ("lookerInstanceUri", prost_string("https://l")),
                    ("lookmlModel", prost_string("m")),
                    ("explore", prost_string("e")),
                ])]),
            ),
        ]);
        assert_eq!(
            parse_data_agent(&props).unwrap().context.datasources.kind(),
            "looker"
        );
    }

    #[test]
    fn context_fields_round_trip() {
        let props = agent_props(vec![
            ("systemInstruction", prost_string("You are an analyst.")),
            ("pythonAnalysis", prost_bool(true)),
            (
                "glossaryTerms",
                prost_list(vec![obj(vec![
                    ("term", prost_string("ARR")),
                    ("description", prost_string("annual recurring revenue")),
                ])]),
            ),
        ]);
        let a = parse_data_agent(&props).unwrap();
        assert_eq!(a.context.system_instruction, "You are an analyst.");
        assert_eq!(a.context.options.python_analysis, Some(true));
        assert_eq!(a.context.glossary_terms[0].term, "ARR");
    }

    #[test]
    fn iam_policy_is_additive_by_default() {
        // An authoritative policy revokes grants this stack never knew about.
        let props = s(vec![
            ("project", prost_string("p")),
            ("agentId", prost_string("a")),
        ]);
        assert!(!parse_iam_policy(&props).unwrap().authoritative);
    }

    #[test]
    fn iam_bindings_parse_roles_and_members() {
        let props = s(vec![
            ("project", prost_string("p")),
            ("agentId", prost_string("a")),
            (
                "bindings",
                prost_list(vec![obj(vec![
                    (
                        "role",
                        prost_string("roles/geminidataanalytics.dataAgentUser"),
                    ),
                    (
                        "members",
                        prost_list(vec![prost_string("user:a@example.com")]),
                    ),
                ])]),
            ),
        ]);
        let p = parse_iam_policy(&props).unwrap();
        assert_eq!(p.bindings.len(), 1);
        assert_eq!(p.bindings[0].members, vec!["user:a@example.com"]);
    }

    #[test]
    fn eval_concurrency_is_clamped_to_a_sane_range() {
        // Unbounded concurrency would open one connection per question and
        // trip the agent's own rate limits.
        for (given, expected) in [(0.0, 1usize), (4.0, 4), (999.0, 16)] {
            let props = s(vec![
                ("project", prost_string("p")),
                (
                    "agent",
                    prost_string("projects/p/locations/global/dataAgents/a"),
                ),
                ("maxConcurrency", prost_number(given)),
            ]);
            assert_eq!(
                parse_agent_eval(&props).unwrap().max_concurrency,
                expected,
                "for {given}"
            );
        }
    }

    #[test]
    fn eval_defaults_to_failing_the_deploy_on_regression() {
        // A gate that does not gate is decoration.
        let props = s(vec![
            ("project", prost_string("p")),
            ("agent", prost_string("a")),
        ]);
        let e = parse_agent_eval(&props).unwrap();
        assert!(e.fail_on_regression);
        assert_eq!(e.max_concurrency, 4);
    }

    #[test]
    fn malformed_list_entries_are_skipped_not_fatal() {
        // One malformed entry must not reject an otherwise valid stack.
        let props = agent_props(vec![(
            "exampleQueries",
            prost_list(vec![
                obj(vec![("question", prost_string("missing sql"))]),
                obj(vec![
                    ("question", prost_string("q2")),
                    ("sql", prost_string("SELECT 1")),
                ]),
            ]),
        )]);
        let a = parse_data_agent(&props).unwrap();
        assert_eq!(a.context.example_queries.len(), 1);
        assert_eq!(a.context.example_queries[0].question, "q2");
    }

    #[test]
    fn memory_and_engine_require_their_identifiers() {
        assert!(parse_memory(&s(vec![("project", prost_string("p"))])).is_err());
        assert!(parse_agent_engine(&s(vec![("project", prost_string("p"))])).is_err());

        let props = s(vec![
            ("project", prost_string("p")),
            ("location", prost_string("us-central1")),
            ("engineId", prost_string("e")),
            ("fact", prost_string("the user prefers metric units")),
        ]);
        assert_eq!(
            parse_memory(&props).unwrap().fact,
            "the user prefers metric units"
        );
    }
}
