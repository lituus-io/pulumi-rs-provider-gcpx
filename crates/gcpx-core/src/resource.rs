// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Cross-resource primitives: validation failures and diff outcomes.

use std::borrow::Cow;

/// A validation failure returned from a resource's `Check`.
///
/// `Cow<'static, str>` keeps static messages allocation-free while still
/// allowing formatted ones.
pub struct CheckFailure {
    pub property: Cow<'static, str>,
    pub reason: Cow<'static, str>,
}

/// Which input keys changed, and whether the change forces replacement.
///
/// Shared by every resource's diff so `build_diff_response` has one input shape.
pub struct DiffResult {
    pub replace_keys: Vec<&'static str>,
    pub update_keys: Vec<&'static str>,
}

impl DiffResult {
    /// Whether anything changed at all.
    pub fn has_changes(&self) -> bool {
        !self.replace_keys.is_empty() || !self.update_keys.is_empty()
    }

    /// Whether any change forces the resource to be recreated rather than
    /// updated in place.
    pub fn needs_replace(&self) -> bool {
        !self.replace_keys.is_empty()
    }
}

/// Validation helper — records a failure when `value` is empty.
pub fn require_non_empty(failures: &mut Vec<CheckFailure>, property: &'static str, value: &str) {
    if value.is_empty() {
        failures.push(CheckFailure {
            property: Cow::Borrowed(property),
            reason: Cow::Owned(format!("{property} must not be empty")),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_keys_means_no_changes() {
        let d = DiffResult {
            replace_keys: vec![],
            update_keys: vec![],
        };
        assert!(!d.has_changes());
        assert!(!d.needs_replace());
    }

    #[test]
    fn update_keys_change_without_replacing() {
        let d = DiffResult {
            replace_keys: vec![],
            update_keys: vec!["description"],
        };
        assert!(d.has_changes());
        assert!(!d.needs_replace());
    }

    #[test]
    fn replace_keys_force_replacement() {
        let d = DiffResult {
            replace_keys: vec!["project"],
            update_keys: vec![],
        };
        assert!(d.has_changes());
        assert!(d.needs_replace());
    }

    #[test]
    fn require_non_empty_records_empty() {
        let mut failures = Vec::new();
        require_non_empty(&mut failures, "project", "");
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].property, "project");
        assert!(failures[0].reason.contains("must not be empty"));
    }

    #[test]
    fn require_non_empty_accepts_value() {
        let mut failures = Vec::new();
        require_non_empty(&mut failures, "project", "proj");
        assert!(failures.is_empty());
    }
}
