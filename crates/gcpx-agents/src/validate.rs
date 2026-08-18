// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Validation for the agent resources.
//!
//! Checks run before anything is sent to Google, so a malformed stack fails at
//! `pulumi preview` with a message naming the property, rather than at deploy
//! with an API error naming a JSON path.

use gcpx_core::resource::{require_non_empty, CheckFailure};

use crate::types::*;

/// Resource ids are used verbatim in a URL path and a resource name.
const MAX_ID_LEN: usize = 63;

pub fn validate_data_agent(inputs: &DataAgentInputs<'_>) -> Vec<CheckFailure> {
    let mut failures = Vec::new();
    require_non_empty(&mut failures, "project", inputs.project);
    require_non_empty(&mut failures, "agentId", inputs.agent_id);

    if let Some(reason) = invalid_resource_id(inputs.agent_id) {
        failures.push(CheckFailure {
            property: "agentId".into(),
            reason: reason.into(),
        });
    }

    if inputs.context.datasources.is_empty() {
        failures.push(CheckFailure {
            property: "models".into(),
            reason: "an agent needs at least one datasource: set 'models' to one or more \
                     dbt model outputs, 'tables' to BigQuery table references, or \
                     'lookerExplores' to Looker explores"
                .into(),
        });
    }

    if inputs.context.system_instruction.is_empty() {
        failures.push(CheckFailure {
            property: "systemInstruction".into(),
            reason: "systemInstruction tells the agent what it is for; without it the \
                     agent has no framing for the questions it will be asked"
                .into(),
        });
    }

    // Publishing makes the context live for everyone talking to the agent, so
    // the bar for what may be published is higher than for staging.
    if inputs.publish && inputs.context.datasources.is_empty() {
        failures.push(CheckFailure {
            property: "publish".into(),
            reason: "cannot publish an agent with no datasources".into(),
        });
    }

    for (i, q) in inputs.context.example_queries.iter().enumerate() {
        if q.sql.trim().is_empty() {
            failures.push(CheckFailure {
                property: "exampleQueries".into(),
                reason: format!("exampleQueries[{i}] ('{}') has no SQL", q.question).into(),
            });
        }
    }

    if let Some(chart) = inputs.context.options.chart_rendering {
        if !matches!(
            chart.to_ascii_lowercase().as_str(),
            "none" | "svg" | "image"
        ) {
            failures.push(CheckFailure {
                property: "chartRendering".into(),
                reason: format!("chartRendering '{chart}' is not one of: none, svg, image").into(),
            });
        }
    }

    failures
}

pub fn validate_iam_policy(inputs: &IamPolicyInputs<'_>) -> Vec<CheckFailure> {
    let mut failures = Vec::new();
    require_non_empty(&mut failures, "project", inputs.project);
    require_non_empty(&mut failures, "agentId", inputs.agent_id);

    for (i, b) in inputs.bindings.iter().enumerate() {
        if !b.role.starts_with("roles/") && !b.role.starts_with("projects/") {
            failures.push(CheckFailure {
                property: "bindings".into(),
                reason: format!(
                    "bindings[{i}].role '{}' should be a role name such as \
                     'roles/geminidataanalytics.dataAgentUser'",
                    b.role
                )
                .into(),
            });
        }
        if b.members.is_empty() {
            failures.push(CheckFailure {
                property: "bindings".into(),
                reason: format!("bindings[{i}] ('{}') grants the role to nobody", b.role).into(),
            });
        }
        for member in &b.members {
            if !is_valid_iam_member(member) {
                failures.push(CheckFailure {
                    property: "bindings".into(),
                    reason: format!(
                        "bindings[{i}] member '{member}' is missing its type prefix, \
                         e.g. 'user:', 'serviceAccount:', 'group:', or 'domain:'"
                    )
                    .into(),
                });
            }
        }
    }

    failures
}

pub fn validate_agent_engine(inputs: &AgentEngineInputs<'_>) -> Vec<CheckFailure> {
    let mut failures = Vec::new();
    require_non_empty(&mut failures, "project", inputs.project);
    require_non_empty(&mut failures, "displayName", inputs.display_name);

    if inputs.location.eq_ignore_ascii_case("global") {
        failures.push(CheckFailure {
            property: "location".into(),
            reason: "agent engines are regional; use a region such as us-central1 \
                     rather than 'global'"
                .into(),
        });
    }

    for (property, uri) in [
        ("pickleUri", inputs.pickle_uri),
        ("requirementsUri", inputs.requirements_uri),
        ("dependencyFilesUri", inputs.dependency_files_uri),
    ] {
        if let Some(uri) = uri {
            if !uri.starts_with("gs://") {
                failures.push(CheckFailure {
                    property: property.into(),
                    reason: format!("{property} must be a Cloud Storage URI beginning with gs://")
                        .into(),
                });
            }
        }
    }

    for (name, value) in &inputs.secret_env {
        if !value.contains("/secrets/") {
            failures.push(CheckFailure {
                property: "secretEnv".into(),
                reason: format!(
                    "secretEnv['{name}'] should be a Secret Manager resource name such as \
                     'projects/PROJECT/secrets/NAME', not a secret value"
                )
                .into(),
            });
        }
    }

    failures
}

pub fn validate_agent_eval(inputs: &AgentEvalInputs<'_>) -> Vec<CheckFailure> {
    let mut failures = Vec::new();
    require_non_empty(&mut failures, "agent", inputs.agent);

    for (i, q) in inputs.golden_queries.iter().enumerate() {
        if q.question.trim().is_empty() {
            failures.push(CheckFailure {
                property: "goldenQueries".into(),
                reason: format!("goldenQueries[{i}] has no question").into(),
            });
        }
        // A query that asserts nothing passes unconditionally, which makes the
        // gate look green while checking nothing.
        let asserts_something = !q.must_reference_tables.is_empty()
            || q.expect_sql_matches.is_some()
            || q.expect_answer_contains.is_some()
            || q.must_not_error;
        if !asserts_something {
            failures.push(CheckFailure {
                property: "goldenQueries".into(),
                reason: format!(
                    "goldenQueries[{i}] ('{}') asserts nothing, so it would pass no matter \
                     what the agent answers; set mustReferenceTables, expectSqlMatches, \
                     expectAnswerContains, or leave mustNotError enabled",
                    q.question
                )
                .into(),
            });
        }
        if let Some(pattern) = q.expect_sql_matches {
            if let Err(e) = validate_pattern(pattern) {
                failures.push(CheckFailure {
                    property: "goldenQueries".into(),
                    reason: format!("goldenQueries[{i}].expectSqlMatches is invalid: {e}").into(),
                });
            }
        }
    }

    failures
}

/// Merge declared bindings into the live policy, preserving grants this stack
/// does not manage.
///
/// Additive is the default because an authoritative write silently revokes
/// access granted elsewhere — by another stack, by a console click, by an
/// organisation policy — and the person who loses access finds out later.
pub fn merge_bindings<'a>(
    current: &'a IamPolicyMeta,
    declared: &[IamBinding<'a>],
) -> Vec<IamBinding<'a>> {
    let mut merged: Vec<IamBinding<'a>> = Vec::new();

    for (role, members) in &current.bindings {
        merged.push(IamBinding {
            role: role.as_str(),
            members: members.iter().map(String::as_str).collect(),
        });
    }

    for binding in declared {
        match merged.iter_mut().find(|b| b.role == binding.role) {
            Some(existing) => {
                for member in &binding.members {
                    if !existing.members.contains(member) {
                        existing.members.push(member);
                    }
                }
            }
            None => merged.push(binding.clone()),
        }
    }

    merged
}

fn invalid_resource_id(id: &str) -> Option<String> {
    if id.is_empty() {
        return None; // reported separately as "must not be empty"
    }
    if id.len() > MAX_ID_LEN {
        return Some(format!(
            "must be at most {MAX_ID_LEN} characters, got {}",
            id.len()
        ));
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
    {
        return Some(
            "may contain only lowercase letters, digits, hyphens, and underscores".to_owned(),
        );
    }
    if !id.chars().next().is_some_and(|c| c.is_ascii_lowercase()) {
        return Some("must start with a lowercase letter".to_owned());
    }
    None
}

fn is_valid_iam_member(member: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "user:",
        "serviceAccount:",
        "group:",
        "domain:",
        "principal:",
        "principalSet:",
    ];
    member == "allUsers"
        || member == "allAuthenticatedUsers"
        || PREFIXES.iter().any(|p| member.starts_with(p))
}

/// A conservative check that a pattern is usable.
///
/// Full regex compilation would mean a regex dependency for one validation; the
/// failure this catches — unbalanced grouping, which is what a hand-written
/// pattern usually gets wrong — is worth catching at preview rather than at
/// deploy.
fn validate_pattern(pattern: &str) -> Result<(), String> {
    if pattern.is_empty() {
        return Err("pattern is empty".to_owned());
    }
    let mut depth = 0i32;
    let mut escaped = false;
    let mut in_class = false;
    for c in pattern.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        match c {
            '\\' => escaped = true,
            '[' if !in_class => in_class = true,
            ']' if in_class => in_class = false,
            '(' if !in_class => depth += 1,
            ')' if !in_class => {
                depth -= 1;
                if depth < 0 {
                    return Err("unbalanced ')'".to_owned());
                }
            }
            _ => {}
        }
    }
    if escaped {
        return Err("pattern ends with a trailing backslash".to_owned());
    }
    if in_class {
        return Err("unterminated character class '['".to_owned());
    }
    if depth != 0 {
        return Err("unbalanced '('".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn agent(publish: bool, datasources: Datasources<'static>) -> DataAgentInputs<'static> {
        DataAgentInputs {
            project: "p",
            location: "global",
            agent_id: "revenue-analyst",
            display_name: None,
            description: None,
            labels: BTreeMap::new(),
            kms_key: None,
            context: AgentContext {
                system_instruction: "You are an analyst.",
                datasources,
                options: AgentOptions::default(),
                example_queries: vec![],
                glossary_terms: vec![],
            },
            publish,
        }
    }

    fn one_table() -> Datasources<'static> {
        Datasources::BigQuery(vec![TableRef {
            project: "p",
            dataset: "d",
            table: "t",
        }])
    }

    #[test]
    fn a_well_formed_agent_passes() {
        assert!(validate_data_agent(&agent(true, one_table())).is_empty());
    }

    #[test]
    fn an_agent_without_datasources_is_rejected() {
        let failures = validate_data_agent(&agent(false, Datasources::BigQuery(vec![])));
        assert!(failures.iter().any(|f| f.property == "models"));
        // The message has to say how to fix it, not just what is wrong.
        assert!(failures[0].reason.contains("dbt model outputs"));
    }

    #[test]
    fn publishing_without_datasources_is_rejected_twice_over() {
        let failures = validate_data_agent(&agent(true, Datasources::BigQuery(vec![])));
        assert!(failures.iter().any(|f| f.property == "publish"));
    }

    #[test]
    fn missing_system_instruction_is_reported() {
        let mut a = agent(false, one_table());
        a.context.system_instruction = "";
        let failures = validate_data_agent(&a);
        assert!(failures.iter().any(|f| f.property == "systemInstruction"));
    }

    #[test]
    fn agent_ids_must_be_url_safe() {
        for (id, ok) in [
            ("revenue-analyst", true),
            ("agent_1", true),
            ("Revenue", false),
            ("1agent", false),
            ("agent id", false),
            ("agent/id", false),
        ] {
            let mut a = agent(false, one_table());
            a.agent_id = id;
            let failures = validate_data_agent(&a);
            let rejected = failures.iter().any(|f| f.property == "agentId");
            assert_eq!(!rejected, ok, "for id {id:?}");
        }
    }

    #[test]
    fn over_long_agent_ids_are_rejected() {
        let long = "a".repeat(MAX_ID_LEN + 1);
        let mut a = agent(false, one_table());
        a.agent_id = &long;
        assert!(validate_data_agent(&a)
            .iter()
            .any(|f| f.property == "agentId"));
    }

    #[test]
    fn iam_members_need_a_type_prefix() {
        // "alice@example.com" without "user:" is the single most common mistake
        // here, and the API rejects it with a much less helpful message.
        let inputs = IamPolicyInputs {
            project: "p",
            location: "global",
            agent_id: "a",
            bindings: vec![IamBinding {
                role: "roles/geminidataanalytics.dataAgentUser",
                members: vec!["alice@example.com"],
            }],
            authoritative: false,
        };
        let failures = validate_iam_policy(&inputs);
        assert!(failures.iter().any(|f| f.reason.contains("user:")));
    }

    #[test]
    fn well_formed_iam_members_pass() {
        let inputs = IamPolicyInputs {
            project: "p",
            location: "global",
            agent_id: "a",
            bindings: vec![IamBinding {
                role: "roles/geminidataanalytics.dataAgentUser",
                members: vec![
                    "user:alice@example.com",
                    "serviceAccount:sa@p.iam.gserviceaccount.com",
                    "group:team@example.com",
                    "allAuthenticatedUsers",
                ],
            }],
            authoritative: false,
        };
        assert!(validate_iam_policy(&inputs).is_empty());
    }

    #[test]
    fn agent_engines_must_be_regional() {
        let inputs = AgentEngineInputs {
            project: "p",
            location: "global",
            engine_id: None,
            display_name: "agent",
            description: None,
            pickle_uri: None,
            requirements_uri: None,
            dependency_files_uri: None,
            python_version: None,
            env: Default::default(),
            secret_env: Default::default(),
        };
        assert!(validate_agent_engine(&inputs)
            .iter()
            .any(|f| f.property == "location"));
    }

    #[test]
    fn secret_env_must_reference_a_secret_not_hold_one() {
        let inputs = AgentEngineInputs {
            project: "p",
            location: "us-central1",
            engine_id: None,
            display_name: "agent",
            description: None,
            pickle_uri: None,
            requirements_uri: None,
            dependency_files_uri: None,
            python_version: None,
            env: Default::default(),
            secret_env: [("API_KEY", "sk-actual-secret-value")]
                .into_iter()
                .collect(),
        };
        let failures = validate_agent_engine(&inputs);
        assert!(failures.iter().any(|f| f.property == "secretEnv"));
    }

    #[test]
    fn a_golden_query_that_asserts_nothing_is_rejected() {
        // Otherwise the gate reports green while checking nothing.
        let inputs = AgentEvalInputs {
            project: "p",
            location: "global",
            agent: "projects/p/locations/global/dataAgents/a",
            golden_queries: vec![GoldenQuery {
                question: "what is revenue?",
                must_reference_tables: vec![],
                expect_sql_matches: None,
                expect_answer_contains: None,
                must_not_error: false,
            }],
            fail_on_regression: true,
            max_concurrency: 4,
        };
        let failures = validate_agent_eval(&inputs);
        assert!(failures
            .iter()
            .any(|f| f.reason.contains("asserts nothing")));
    }

    #[test]
    fn invalid_sql_patterns_are_caught_at_preview() {
        for bad in ["(unclosed", "unopened)", "[unterminated", "trailing\\"] {
            assert!(validate_pattern(bad).is_err(), "{bad:?} should be rejected");
        }
        for good in ["GROUP BY .*region", r"SELECT \(1\)", "[a-z]+", r"a\\"] {
            assert!(
                validate_pattern(good).is_ok(),
                "{good:?} should be accepted"
            );
        }
    }

    #[test]
    fn merging_preserves_bindings_this_stack_does_not_manage() {
        // The reason additive is the default: an authoritative write revokes
        // access granted elsewhere, and the person who loses it finds out later.
        let current = IamPolicyMeta {
            etag: "e".into(),
            version: 3,
            bindings: vec![(
                "roles/viewer".into(),
                vec!["user:existing@example.com".into()],
            )],
        };
        let declared = [IamBinding {
            role: "roles/geminidataanalytics.dataAgentUser",
            members: vec!["user:new@example.com"],
        }];
        let merged = merge_bindings(&current, &declared);
        assert_eq!(merged.len(), 2);
        assert!(merged.iter().any(|b| b.role == "roles/viewer"));
    }

    #[test]
    fn merging_the_same_role_unions_its_members_without_duplicates() {
        let current = IamPolicyMeta {
            etag: "e".into(),
            version: 3,
            bindings: vec![("roles/viewer".into(), vec!["user:a@example.com".into()])],
        };
        let declared = [IamBinding {
            role: "roles/viewer",
            members: vec!["user:a@example.com", "user:b@example.com"],
        }];
        let merged = merge_bindings(&current, &declared);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].members.len(), 2);
    }
}
