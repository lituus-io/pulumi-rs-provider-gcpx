// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! The published Pulumi schema.

/// The schema, embedded at build time.
///
/// Returned as a `&'static str` rather than being read or rebuilt per call:
/// `GetSchema` is on the hot path for validation and preview, which run far
/// more often than deploys.
pub fn schema_json() -> &'static str {
    include_str!("../schema.json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn schema() -> Value {
        serde_json::from_str(schema_json()).expect("schema.json must be valid JSON")
    }

    #[test]
    fn schema_is_valid_json_with_the_expected_package() {
        let s = schema();
        assert_eq!(s["name"], "gcpx", "the package name is the public contract");
        assert_eq!(s["version"], env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn dispatch_and_schema_agree_on_the_resource_set() {
        // A type in one and not the other is either a resource nobody can
        // reach or one nobody can discover.
        let s = schema();
        let declared: std::collections::HashSet<&str> = s["resources"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();

        for token in crate::ALL_RESOURCE_TYPES {
            assert!(
                declared.contains(token),
                "{token} is dispatched but missing from the schema"
            );
        }
        for token in &declared {
            assert!(
                crate::ALL_RESOURCE_TYPES.contains(token),
                "{token} is in the schema but has no dispatch entry"
            );
        }
    }

    #[test]
    fn every_resource_declares_its_required_inputs() {
        let s = schema();
        for (name, resource) in s["resources"].as_object().unwrap() {
            let required = resource
                .get("requiredInputs")
                .and_then(|v| v.as_array())
                .unwrap_or_else(|| panic!("{name} has no requiredInputs"));
            let inputs = resource["inputProperties"].as_object().unwrap();
            for req in required {
                let key = req.as_str().unwrap();
                assert!(
                    inputs.contains_key(key),
                    "{name} requires '{key}' but does not declare it as an input"
                );
            }
        }
    }

    #[test]
    fn every_type_reference_resolves() {
        // A dangling $ref makes the schema unusable for type checking, and the
        // failure surfaces in the user's editor rather than here.
        let s = schema();
        let types: std::collections::HashSet<String> =
            s["types"].as_object().unwrap().keys().cloned().collect();

        fn walk(v: &Value, refs: &mut Vec<String>) {
            match v {
                Value::Object(map) => {
                    for (k, val) in map {
                        if k == "$ref" {
                            if let Some(r) = val.as_str() {
                                refs.push(r.to_owned());
                            }
                        }
                        walk(val, refs);
                    }
                }
                Value::Array(items) => items.iter().for_each(|i| walk(i, refs)),
                _ => {}
            }
        }

        let mut refs = Vec::new();
        walk(&s, &mut refs);
        assert!(
            !refs.is_empty(),
            "expected the schema to use type references"
        );
        for r in refs {
            let Some(name) = r.strip_prefix("#/types/") else {
                continue; // pulumi.json# and similar externals
            };
            assert!(types.contains(name), "dangling type reference: {r}");
        }
    }

    #[test]
    fn the_agent_resources_are_published() {
        let s = schema();
        let resources = s["resources"].as_object().unwrap();
        for token in [
            "gcpx:agent/dataAgent:DataAgent",
            "gcpx:agent/dataAgentIamPolicy:DataAgentIamPolicy",
            "gcpx:agent/conversation:Conversation",
            "gcpx:agent/agentEngine:AgentEngine",
            "gcpx:agent/memory:Memory",
        ] {
            assert!(resources.contains_key(token), "{token} is not published");
        }
    }

    #[test]
    fn the_data_agent_documents_how_to_ground_it() {
        // The grounding wiring is the non-obvious part; a user who misses it
        // ends up naming tables by hand and losing the dependency edge.
        let s = schema();
        let agent = &s["resources"]["gcpx:agent/dataAgent:DataAgent"];
        let models = &agent["inputProperties"]["models"];
        assert!(models["description"]
            .as_str()
            .unwrap()
            .contains("dependency edge"));
        assert!(agent["inputProperties"]["publish"]["description"]
            .as_str()
            .unwrap()
            .contains("staging"));
    }

    #[test]
    fn secret_inputs_warn_against_carrying_values() {
        let s = schema();
        let desc = s["resources"]["gcpx:agent/agentEngine:AgentEngine"]["inputProperties"]
            ["secretEnv"]["description"]
            .as_str()
            .unwrap();
        assert!(desc.contains("never secret values"));
    }

    #[test]
    fn schema_stays_within_the_grpc_message_limit() {
        // The provider raises its own limits, but a schema approaching the cap
        // is worth noticing before a user hits it.
        assert!(
            schema_json().len() < gcpx_core::MAX_GRPC_MESSAGE_BYTES,
            "schema exceeds the negotiated gRPC message size"
        );
    }
}
