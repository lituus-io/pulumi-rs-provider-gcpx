// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Translation from resource inputs into Conversational Analytics API payloads.
//!
//! Kept separate from the handlers so the exact JSON sent to Google is
//! inspectable in a unit test without a network, a client, or a provider. For
//! this provider the request body *is* the product, so it is the thing worth
//! asserting on.

use serde_json::{json, Map, Value};

use crate::types::{AgentContext, DataAgentInputs, Datasources, IamBinding};

/// Build the `DataAgent` body for create and patch.
///
/// `publish` decides whether the context lands in `publishedContext` — which
/// takes effect immediately for anyone talking to the agent — or in
/// `stagingContext`, where it can be reviewed first. Both are written on
/// publish so staging always reflects what is live.
pub fn build_agent_body(inputs: &DataAgentInputs<'_>) -> Value {
    let context = build_context(&inputs.context);

    let mut analytics = Map::new();
    analytics.insert("stagingContext".into(), context.clone());
    if inputs.publish {
        analytics.insert("publishedContext".into(), context);
    }

    let mut body = Map::new();
    if let Some(v) = inputs.display_name {
        body.insert("displayName".into(), json!(v));
    }
    if let Some(v) = inputs.description {
        body.insert("description".into(), json!(v));
    }
    if let Some(v) = inputs.kms_key {
        body.insert("kmsKey".into(), json!(v));
    }
    if !inputs.labels.is_empty() {
        body.insert(
            "labels".into(),
            Value::Object(
                inputs
                    .labels
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), json!(v)))
                    .collect(),
            ),
        );
    }
    body.insert("dataAnalyticsAgent".into(), Value::Object(analytics));
    Value::Object(body)
}

/// The `updateMask` for a patch.
///
/// Sending a body without a mask replaces unset fields with their defaults,
/// which would quietly erase a description or a label set the user still wants.
pub fn agent_update_mask(inputs: &DataAgentInputs<'_>) -> String {
    let mut paths = vec!["dataAnalyticsAgent"];
    if inputs.display_name.is_some() {
        paths.push("displayName");
    }
    if inputs.description.is_some() {
        paths.push("description");
    }
    if !inputs.labels.is_empty() {
        paths.push("labels");
    }
    paths.join(",")
}

pub fn build_context(ctx: &AgentContext<'_>) -> Value {
    let mut out = Map::new();

    if !ctx.system_instruction.is_empty() {
        out.insert("systemInstruction".into(), json!(ctx.system_instruction));
    }
    out.insert(
        "datasourceReferences".into(),
        build_datasource_references(&ctx.datasources),
    );

    if !ctx.options.is_empty() {
        let mut options = Map::new();
        if let Some(chart) = ctx.options.chart_rendering {
            // "none" is expressed by selecting the no-image variant rather than
            // by omitting the field, which would mean "use the default".
            let image = if chart.eq_ignore_ascii_case("none") {
                json!({ "noImage": {} })
            } else {
                json!({ "svg": {} })
            };
            options.insert("chart".into(), json!({ "image": image }));
        }
        if let Some(enabled) = ctx.options.python_analysis {
            options.insert(
                "analysis".into(),
                json!({ "python": { "enabled": enabled } }),
            );
        }
        if let Some(model) = ctx.options.model {
            options.insert("model".into(), json!(model));
        }
        out.insert("options".into(), Value::Object(options));
    }

    if !ctx.example_queries.is_empty() {
        out.insert(
            "exampleQueries".into(),
            Value::Array(
                ctx.example_queries
                    .iter()
                    .map(|q| json!({ "naturalLanguageQuestion": q.question, "sqlQuery": q.sql }))
                    .collect(),
            ),
        );
    }

    if !ctx.glossary_terms.is_empty() {
        out.insert(
            "glossaryTerms".into(),
            Value::Array(
                ctx.glossary_terms
                    .iter()
                    // The field is `displayName`; `term` is rejected. Verified
                    // against the live API, not inferred from the reference.
                    .map(|t| json!({ "displayName": t.term, "description": t.description }))
                    .collect(),
            ),
        );
    }

    Value::Object(out)
}

fn build_datasource_references(sources: &Datasources<'_>) -> Value {
    match sources {
        Datasources::BigQuery(tables) => json!({
            "bq": {
                "tableReferences": tables.iter().map(|t| json!({
                    "projectId": t.project,
                    "datasetId": t.dataset,
                    "tableId": t.table,
                })).collect::<Vec<_>>()
            }
        }),
        Datasources::Looker(explores) => json!({
            "looker": {
                "exploreReferences": explores.iter().map(|e| json!({
                    "lookerInstanceUri": e.looker_instance_uri,
                    "lookmlModel": e.lookml_model,
                    "explore": e.explore,
                })).collect::<Vec<_>>()
            }
        }),
    }
}

/// Build a `setIamPolicy` body.
///
/// `etag` is threaded through from the policy that was read: it is what makes
/// the write a compare-and-swap rather than a last-writer-wins overwrite of
/// whatever someone else granted in between.
pub fn build_iam_policy_body(bindings: &[IamBinding<'_>], etag: &str) -> Value {
    let mut policy = Map::new();
    policy.insert(
        "bindings".into(),
        Value::Array(
            bindings
                .iter()
                .map(|b| json!({ "role": b.role, "members": b.members }))
                .collect(),
        ),
    );
    if !etag.is_empty() {
        policy.insert("etag".into(), json!(etag));
    }
    policy.insert("version".into(), json!(3));
    json!({ "policy": Value::Object(policy) })
}

pub fn build_conversation_body(
    agents: &[&str],
    labels: &std::collections::BTreeMap<&str, &str>,
) -> Value {
    let mut body = Map::new();
    body.insert("agents".into(), json!(agents));
    if !labels.is_empty() {
        body.insert(
            "labels".into(),
            Value::Object(
                labels
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), json!(v)))
                    .collect(),
            ),
        );
    }
    Value::Object(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use std::collections::BTreeMap;

    fn ctx() -> AgentContext<'static> {
        AgentContext {
            system_instruction: "You are an analyst.",
            datasources: Datasources::BigQuery(vec![TableRef {
                project: "p",
                dataset: "d",
                table: "t",
            }]),
            options: AgentOptions::default(),
            example_queries: vec![],
            glossary_terms: vec![],
        }
    }

    fn inputs(publish: bool) -> DataAgentInputs<'static> {
        DataAgentInputs {
            project: "p",
            location: "global",
            agent_id: "a",
            display_name: Some("Analyst"),
            description: None,
            labels: BTreeMap::new(),
            kms_key: None,
            context: ctx(),
            publish,
        }
    }

    #[test]
    fn unpublished_agent_writes_staging_only() {
        // The whole point of the flag: a context under review must not be
        // reachable by anyone talking to the agent.
        let body = build_agent_body(&inputs(false));
        let a = &body["dataAnalyticsAgent"];
        assert!(a.get("stagingContext").is_some());
        assert!(
            a.get("publishedContext").is_none(),
            "unpublished context must not go live"
        );
    }

    #[test]
    fn publishing_writes_both_contexts() {
        // Staging must mirror what is live, or the next unpublished edit would
        // diff against a stale base.
        let body = build_agent_body(&inputs(true));
        let a = &body["dataAnalyticsAgent"];
        assert_eq!(a["stagingContext"], a["publishedContext"]);
    }

    #[test]
    fn bigquery_tables_become_table_references() {
        let body = build_agent_body(&inputs(true));
        let refs = &body["dataAnalyticsAgent"]["stagingContext"]["datasourceReferences"]["bq"]
            ["tableReferences"];
        assert_eq!(refs[0]["projectId"], "p");
        assert_eq!(refs[0]["datasetId"], "d");
        assert_eq!(refs[0]["tableId"], "t");
    }

    #[test]
    fn looker_and_bigquery_are_mutually_exclusive_in_the_body() {
        let mut i = inputs(true);
        i.context.datasources = Datasources::Looker(vec![ExploreRef {
            looker_instance_uri: "https://looker.example.com",
            lookml_model: "m",
            explore: "e",
        }]);
        let body = build_agent_body(&i);
        let ds = &body["dataAnalyticsAgent"]["stagingContext"]["datasourceReferences"];
        assert!(ds.get("looker").is_some());
        assert!(ds.get("bq").is_none(), "union field must carry one variant");
    }

    #[test]
    fn chart_none_selects_the_no_image_variant() {
        // Omitting the field would mean "service default", not "no charts".
        let mut i = inputs(true);
        i.context.options.chart_rendering = Some("none");
        let body = build_agent_body(&i);
        let chart = &body["dataAnalyticsAgent"]["stagingContext"]["options"]["chart"]["image"];
        assert!(chart.get("noImage").is_some());
        assert!(chart.get("svg").is_none());
    }

    #[test]
    fn options_are_omitted_entirely_when_unset() {
        let body = build_agent_body(&inputs(true));
        assert!(body["dataAnalyticsAgent"]["stagingContext"]
            .get("options")
            .is_none());
    }

    /// The exact field names the live API accepts.
    ///
    /// Every one of these was verified against the real service rather than
    /// read off the reference: the first attempt used `term` for a glossary
    /// entry, which the API rejects, and it only surfaced on deploy.
    #[test]
    fn context_uses_the_field_names_the_api_accepts() {
        let mut i = inputs(true);
        i.context.example_queries = vec![ExampleQuery {
            question: "revenue?",
            sql: "SELECT 1",
        }];
        i.context.glossary_terms = vec![GlossaryTerm {
            term: "ARR",
            description: "annual recurring revenue",
            synonyms: vec!["annual revenue"],
        }];
        i.context.options.chart_rendering = Some("svg");
        i.context.options.python_analysis = Some(true);

        let c = &build_agent_body(&i)["dataAnalyticsAgent"]["stagingContext"];
        assert_eq!(
            c["exampleQueries"][0]["naturalLanguageQuestion"],
            "revenue?"
        );
        assert_eq!(c["exampleQueries"][0]["sqlQuery"], "SELECT 1");
        // `displayName`, not `term`: the API rejects `term` outright.
        assert_eq!(c["glossaryTerms"][0]["displayName"], "ARR");
        assert!(
            c["glossaryTerms"][0].get("term").is_none(),
            "`term` is not a field on this message"
        );
        assert_eq!(
            c["glossaryTerms"][0]["description"],
            "annual recurring revenue"
        );
        assert!(c["options"]["chart"]["image"]["svg"].is_object());
        assert_eq!(c["options"]["analysis"]["python"]["enabled"], true);
    }

    /// Two context fields were designed from the reference and then removed:
    /// both message names exist, but no subfield spelling could be found that
    /// the API accepts. Sending an unverified shape fails the deploy, so
    /// nothing is sent until the shape is known.
    #[test]
    fn unverified_context_fields_are_not_sent() {
        let c = &build_agent_body(&inputs(true))["dataAnalyticsAgent"]["stagingContext"];
        assert!(c.get("schemaRelationships").is_none());
        assert!(c.get("userFunctions").is_none());
    }

    #[test]
    fn update_mask_always_covers_the_context() {
        // Without the context path a patch would leave the agent's behaviour
        // unchanged while reporting success.
        let mask = agent_update_mask(&inputs(true));
        assert!(mask.contains("dataAnalyticsAgent"));
        assert!(mask.contains("displayName"));
        assert!(
            !mask.contains("description"),
            "unset fields stay out of the mask"
        );
    }

    #[test]
    fn iam_body_threads_the_etag_for_compare_and_swap() {
        let bindings = [IamBinding {
            role: "roles/geminidataanalytics.dataAgentUser",
            members: vec!["user:a@example.com"],
        }];
        let body = build_iam_policy_body(&bindings, "etag-123");
        assert_eq!(body["policy"]["etag"], "etag-123");
        assert_eq!(
            body["policy"]["bindings"][0]["members"][0],
            "user:a@example.com"
        );
    }

    #[test]
    fn iam_body_omits_an_absent_etag() {
        // A first write has no etag to compare against; sending an empty one
        // would be rejected.
        let body = build_iam_policy_body(&[], "");
        assert!(body["policy"].get("etag").is_none());
    }
}
