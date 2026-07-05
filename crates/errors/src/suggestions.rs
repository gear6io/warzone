//! "Did you mean" helpers, ported from `suggestions.go`. Stdlib only — the
//! Levenshtein distance here is ~15 lines, not worth a `strsim` dependency.

/// Names the kind of value a suggestion refers to (mirrors Go's `Noun*` consts).
pub const NOUN_FIELDS: &str = "fields";
pub const NOUN_KEYS: &str = "keys";
pub const NOUN_REFERENCES: &str = "references";

const TYPO_SUGGESTION_THRESHOLD: f64 = 0.75;
const MAX_VALID_REFERENCES: usize = 20;

/// Returns a "did you mean" correction followed by the valid-references list.
/// `noun` names the kind of value (e.g. `NOUN_FIELDS`).
pub fn new_suggestions_on_levenshtein_distance(
    invalid_input: &str,
    noun: &str,
    valid_inputs: &[String],
) -> Vec<String> {
    let mut suggestions = Vec::with_capacity(2);
    if let Some(m) = closest_levenshtein_match(invalid_input, valid_inputs) {
        suggestions.push(did_you_mean(&m));
    }
    let refs = new_valid_references(noun, valid_inputs);
    if !refs.is_empty() {
        suggestions.push(refs);
    }
    suggestions
}

/// Returns the candidate most similar to `input` that is at least
/// [`TYPO_SUGGESTION_THRESHOLD`] similar, or `None` when nothing is close enough.
pub fn closest_levenshtein_match(input: &str, candidates: &[String]) -> Option<String> {
    let mut best_match: Option<String> = None;
    let mut best_similarity = 0.0;
    for candidate in candidates {
        let sim = similarity(input, candidate);
        if sim > best_similarity && sim >= TYPO_SUGGESTION_THRESHOLD {
            best_similarity = sim;
            best_match = Some(candidate.clone());
        }
    }
    best_match
}

/// Formats a "did you mean" suggestion from a fallible lookup, or `None` when
/// `produce` yields nothing.
pub fn new_suggestions_from_func(produce: impl FnOnce() -> Option<String>) -> Vec<String> {
    match produce() {
        Some(s) if !s.is_empty() => vec![did_you_mean(&s)],
        _ => Vec::new(),
    }
}

/// Formats values as "valid `<noun>` are `a`, `b`", capped at
/// [`MAX_VALID_REFERENCES`] with a "(+N more)" suffix. Empty string when
/// `values` is empty.
pub fn new_valid_references(noun: &str, values: &[String]) -> String {
    if values.is_empty() {
        return String::new();
    }
    let truncated = values.len().saturating_sub(MAX_VALID_REFERENCES);
    let refs = &values[..values.len().min(MAX_VALID_REFERENCES)];
    let quoted: Vec<String> = refs.iter().map(|r| format!("`{r}`")).collect();
    let mut out = format!("valid {noun} are {}", quoted.join(", "));
    if truncated > 0 {
        out.push_str(&format!(" (+{truncated} more)"));
    }
    out
}

fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1: Vec<char> = s1.to_lowercase().chars().collect();
    let s2: Vec<char> = s2.to_lowercase().chars().collect();
    if s1.is_empty() {
        return s2.len();
    }
    if s2.is_empty() {
        return s1.len();
    }
    let mut v0: Vec<usize> = (0..=s2.len()).collect();
    let mut v1 = vec![0usize; s2.len() + 1];
    for i in 0..s1.len() {
        v1[0] = i + 1;
        for j in 0..s2.len() {
            let deletion_cost = v0[j + 1] + 1;
            let insertion_cost = v1[j] + 1;
            let substitution_cost = if s1[i] == s2[j] { v0[j] } else { v0[j] + 1 };
            v1[j + 1] = deletion_cost.min(insertion_cost).min(substitution_cost);
        }
        v0.copy_from_slice(&v1);
    }
    v1[s2.len()]
}

fn similarity(s1: &str, s2: &str) -> f64 {
    let max_len = s1.chars().count().max(s2.chars().count());
    if max_len == 0 {
        return 1.0;
    }
    1.0 - levenshtein_distance(s1, s2) as f64 / max_len as f64
}

fn did_you_mean(m: &str) -> String {
    format!("did you mean: `{m}`")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levenshtein_match_and_no_match() {
        let candidates: Vec<String> = ["field", "filter", "format"].map(String::from).into();
        assert_eq!(
            closest_levenshtein_match("fieled", &candidates),
            Some("field".to_string())
        );
        assert_eq!(closest_levenshtein_match("xyz", &candidates), None);
    }

    #[test]
    fn valid_references_truncates() {
        let values: Vec<String> = (0..25).map(|i| i.to_string()).collect();
        let out = new_valid_references(NOUN_FIELDS, &values);
        assert!(out.contains("(+5 more)"));
    }
}
