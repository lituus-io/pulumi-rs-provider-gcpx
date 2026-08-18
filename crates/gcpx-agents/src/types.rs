// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Input and output shapes for the agent resources.
//!
//! Inputs borrow from the Pulumi property map (`&'a str`) so parsing a resource
//! costs no allocation; outputs are owned because they cross the gRPC boundary.

use std::collections::BTreeMap;

/// A BigQuery table an agent is allowed to query.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TableRef<'a> {
    pub project: &'a str,
    pub dataset: &'a str,
    pub table: &'a str,
}

impl TableRef<'_> {
    /// `project.dataset.table`, the form the API expects in prose and the form
    /// a user recognises from the console.
    pub fn qualified(&self) -> String {
        format!("{}.{}.{}", self.project, self.dataset, self.table)
    }
}

/// A Looker explore an agent is allowed to query.
#[derive(Debug, Clone)]
pub struct ExploreRef<'a> {
    pub looker_instance_uri: &'a str,
    pub lookml_model: &'a str,
    pub explore: &'a str,
}

/// Where an agent's data comes from.
///
/// The API models this as a union — an agent is grounded on BigQuery *or*
/// Looker, not both — so this is an enum rather than a struct of options, which
/// makes the invalid combination unrepresentable instead of merely rejected.
#[derive(Debug, Clone)]
pub enum Datasources<'a> {
    BigQuery(Vec<TableRef<'a>>),
    Looker(Vec<ExploreRef<'a>>),
}

impl Datasources<'_> {
    pub fn is_empty(&self) -> bool {
        match self {
            Self::BigQuery(t) => t.is_empty(),
            Self::Looker(e) => e.is_empty(),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::BigQuery(_) => "bigquery",
            Self::Looker(_) => "looker",
        }
    }
}

/// A question the agent should already know how to answer, with the SQL that
/// answers it. Improves grounding, and doubles as the corpus for evaluation.
#[derive(Debug, Clone)]
pub struct ExampleQuery<'a> {
    pub question: &'a str,
    pub sql: &'a str,
}

/// A business term whose meaning the agent cannot infer from column names.
#[derive(Debug, Clone)]
pub struct GlossaryTerm<'a> {
    pub term: &'a str,
    pub description: &'a str,
    /// Optional synonyms a user might say instead.
    pub synonyms: Vec<&'a str>,
}

/// Behavioural switches for the agent.
#[derive(Debug, Clone, Default)]
pub struct AgentOptions<'a> {
    /// Whether the agent may render charts, and in what form.
    pub chart_rendering: Option<&'a str>,
    /// Whether the agent may run Python for analysis it cannot express in SQL.
    pub python_analysis: Option<bool>,
    /// Model override; the service default is used when absent.
    pub model: Option<&'a str>,
}

impl AgentOptions<'_> {
    pub fn is_empty(&self) -> bool {
        self.chart_rendering.is_none() && self.python_analysis.is_none() && self.model.is_none()
    }
}

/// Everything that defines how an agent behaves.
#[derive(Debug, Clone)]
pub struct AgentContext<'a> {
    pub system_instruction: &'a str,
    pub datasources: Datasources<'a>,
    pub options: AgentOptions<'a>,
    pub example_queries: Vec<ExampleQuery<'a>>,
    pub glossary_terms: Vec<GlossaryTerm<'a>>,
}

#[derive(Debug, Clone)]
pub struct DataAgentInputs<'a> {
    pub project: &'a str,
    pub location: &'a str,
    pub agent_id: &'a str,
    pub display_name: Option<&'a str>,
    pub description: Option<&'a str>,
    pub labels: BTreeMap<&'a str, &'a str>,
    pub kms_key: Option<&'a str>,
    pub context: AgentContext<'a>,
    /// When false the context is written to staging only, so it can be reviewed
    /// before anyone talks to the agent.
    pub publish: bool,
}

/// Live state of a data agent.
#[derive(Debug, Clone, Default)]
pub struct DataAgentMeta {
    pub name: String,
    pub display_name: String,
    pub description: String,
    pub create_time: String,
    pub update_time: String,
    pub labels: BTreeMap<String, String>,
    /// Set once the agent is scheduled for deletion but still recoverable.
    pub delete_time: String,
    pub published: bool,
}

#[derive(Debug, Clone)]
pub struct ConversationInputs<'a> {
    pub project: &'a str,
    pub location: &'a str,
    pub conversation_id: &'a str,
    /// Fully-qualified agent resource names this conversation may address.
    pub agents: Vec<&'a str>,
    pub labels: BTreeMap<&'a str, &'a str>,
}

#[derive(Debug, Clone, Default)]
pub struct ConversationMeta {
    pub name: String,
    pub agents: Vec<String>,
    pub create_time: String,
    pub last_used_time: String,
}

/// One IAM role and the members holding it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamBinding<'a> {
    pub role: &'a str,
    pub members: Vec<&'a str>,
}

#[derive(Debug, Clone)]
pub struct IamPolicyInputs<'a> {
    pub project: &'a str,
    pub location: &'a str,
    pub agent_id: &'a str,
    pub bindings: Vec<IamBinding<'a>>,
    /// Authoritative replaces the whole policy; additive merges into it.
    ///
    /// The distinction matters: an authoritative binding applied to a policy
    /// someone else also manages will silently revoke their grants.
    pub authoritative: bool,
}

#[derive(Debug, Clone, Default)]
pub struct IamPolicyMeta {
    pub etag: String,
    pub version: i32,
    pub bindings: Vec<(String, Vec<String>)>,
}

#[derive(Debug, Clone)]
pub struct AgentEngineInputs<'a> {
    pub project: &'a str,
    pub location: &'a str,
    pub engine_id: Option<&'a str>,
    pub display_name: &'a str,
    pub description: Option<&'a str>,
    /// GCS URI of the pickled agent object.
    pub pickle_uri: Option<&'a str>,
    pub requirements_uri: Option<&'a str>,
    pub dependency_files_uri: Option<&'a str>,
    pub python_version: Option<&'a str>,
    pub env: BTreeMap<&'a str, &'a str>,
    /// Environment variables sourced from Secret Manager, never from state.
    pub secret_env: BTreeMap<&'a str, &'a str>,
}

#[derive(Debug, Clone, Default)]
pub struct AgentEngineMeta {
    pub name: String,
    pub display_name: String,
    pub description: String,
    pub create_time: String,
    pub update_time: String,
    pub etag: String,
}

#[derive(Debug, Clone)]
pub struct MemoryInputs<'a> {
    pub project: &'a str,
    pub location: &'a str,
    pub engine_id: &'a str,
    pub memory_id: Option<&'a str>,
    pub fact: &'a str,
    pub display_name: Option<&'a str>,
    pub description: Option<&'a str>,
    /// Keys narrowing when this memory applies, e.g. `{"user_id": "u1"}`.
    pub scope: BTreeMap<&'a str, &'a str>,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryMeta {
    pub name: String,
    pub fact: String,
    pub create_time: String,
    pub update_time: String,
}

/// One question the agent must answer acceptably, and what "acceptably" means.
#[derive(Debug, Clone)]
pub struct GoldenQuery<'a> {
    pub question: &'a str,
    /// Tables the generated SQL must reference.
    pub must_reference_tables: Vec<&'a str>,
    /// Regex the generated SQL must match.
    pub expect_sql_matches: Option<&'a str>,
    /// Substring the answer must contain.
    pub expect_answer_contains: Option<&'a str>,
    pub must_not_error: bool,
}

#[derive(Debug, Clone)]
pub struct AgentEvalInputs<'a> {
    pub project: &'a str,
    pub location: &'a str,
    /// Fully-qualified agent resource name.
    pub agent: &'a str,
    pub golden_queries: Vec<GoldenQuery<'a>>,
    /// Whether a failing query fails the deployment.
    pub fail_on_regression: bool,
    /// How many questions to ask at once.
    pub max_concurrency: usize,
}

/// What happened when one golden query was asked.
#[derive(Debug, Clone)]
pub struct GoldenQueryResult {
    pub question: String,
    pub passed: bool,
    pub generated_sql: String,
    pub answer: String,
    /// Why it failed; empty when it passed.
    pub failure_reason: String,
}

#[derive(Debug, Clone, Default)]
pub struct AgentEvalMeta {
    pub total: usize,
    pub passed: usize,
    pub results: Vec<GoldenQueryResult>,
}

impl AgentEvalMeta {
    pub fn all_passed(&self) -> bool {
        self.passed == self.total
    }

    /// A one-line summary suitable for a resource output.
    pub fn summary(&self) -> String {
        format!("{}/{} golden queries passed", self.passed, self.total)
    }

    /// The questions that failed, for the error message when the gate trips.
    pub fn failures(&self) -> Vec<&GoldenQueryResult> {
        self.results.iter().filter(|r| !r.passed).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_ref_renders_the_qualified_form() {
        let t = TableRef {
            project: "p",
            dataset: "d",
            table: "t",
        };
        assert_eq!(t.qualified(), "p.d.t");
    }

    #[test]
    fn datasources_are_exclusive_by_construction() {
        // The API models this as a union; making it an enum means the invalid
        // combination cannot be built, rather than being built and rejected.
        let bq = Datasources::BigQuery(vec![]);
        let looker = Datasources::Looker(vec![]);
        assert_eq!(bq.kind(), "bigquery");
        assert_eq!(looker.kind(), "looker");
        assert!(bq.is_empty() && looker.is_empty());
    }

    #[test]
    fn eval_meta_reports_pass_state_and_failures() {
        let meta = AgentEvalMeta {
            total: 2,
            passed: 1,
            results: vec![
                GoldenQueryResult {
                    question: "a".into(),
                    passed: true,
                    generated_sql: String::new(),
                    answer: String::new(),
                    failure_reason: String::new(),
                },
                GoldenQueryResult {
                    question: "b".into(),
                    passed: false,
                    generated_sql: String::new(),
                    answer: String::new(),
                    failure_reason: "no tables referenced".into(),
                },
            ],
        };
        assert!(!meta.all_passed());
        assert_eq!(meta.summary(), "1/2 golden queries passed");
        assert_eq!(meta.failures().len(), 1);
        assert_eq!(meta.failures()[0].question, "b");
    }

    #[test]
    fn eval_meta_with_no_queries_counts_as_passing() {
        // Nothing asserted is vacuously satisfied; the alternative would fail
        // every deploy that has not written golden queries yet.
        assert!(AgentEvalMeta::default().all_passed());
    }

    #[test]
    fn agent_options_reports_emptiness() {
        assert!(AgentOptions::default().is_empty());
        assert!(!AgentOptions {
            python_analysis: Some(true),
            ..Default::default()
        }
        .is_empty());
    }
}
