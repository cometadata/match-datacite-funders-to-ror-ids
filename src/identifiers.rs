//! Canonical normalization of ROR and Crossref Funder IDs, tolerant of the
//! many forms users assert in DataCite `fundingReferences`.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentifierScheme {
    Ror,
    Fundref,
}

/// Returns `Some("https://ror.org/<id>")` if `s` contains a valid ROR ID
/// (optionally wrapped in URL/host noise and with whitespace/trailing slash).
/// The ID is 9 chars: `0` followed by 8 lowercase alphanumerics.
pub fn normalize_ror(s: &str) -> Option<String> {
    let trimmed = s.trim().trim_end_matches('/');
    let lower = trimmed.to_ascii_lowercase();

    // Strip scheme + host prefixes if present.
    let rest = strip_prefix_case_insensitive(&lower, "https://")
        .or_else(|| strip_prefix_case_insensitive(&lower, "http://"))
        .unwrap_or(&lower);
    let rest = strip_prefix_case_insensitive(rest, "www.").unwrap_or(rest);
    let rest = strip_prefix_case_insensitive(rest, "ror.org/").unwrap_or(rest);

    // After prefix stripping, `rest` should be exactly the 9-char ID.
    if is_valid_ror_id(rest) {
        Some(format!("https://ror.org/{rest}"))
    } else {
        None
    }
}

/// Returns `Some("<bare digits>")` if `s` contains a valid Crossref Funder ID
/// in bare, `10.13039/`-prefixed, or full-DOI-URL form.
pub fn normalize_fundref(s: &str) -> Option<String> {
    let trimmed = s.trim().trim_end_matches('/');

    // Find the Crossref Funder DOI prefix if present; otherwise try bare.
    let digits_candidate = if let Some(pos) = trimmed.find("10.13039/") {
        &trimmed[pos + "10.13039/".len()..]
    } else {
        trimmed
    };

    // Take the leading digit run.
    let digits: String = digits_candidate.chars().take_while(|c| c.is_ascii_digit()).collect();
    if !digits.is_empty() && digits.len() == digits_candidate.trim_end_matches('/').len() {
        // The candidate body was ONLY digits (plus possibly trailing slash stripped above).
        Some(digits)
    } else {
        None
    }
}

/// If the raw value looks unambiguously like one scheme, return it and its
/// canonical form. Ambiguous, empty, or unknown input returns `None`.
/// Used for "trust the value, not the label" type-sniffing in extract.
pub fn sniff_identifier(s: &str) -> Option<(IdentifierScheme, String)> {
    let ror = normalize_ror(s);
    let fundref = normalize_fundref(s);
    match (ror, fundref) {
        (Some(c), None) => Some((IdentifierScheme::Ror, c)),
        (None, Some(c)) => Some((IdentifierScheme::Fundref, c)),
        _ => None,  // both None (unknown) or both Some (shouldn't happen — schemes are disjoint)
    }
}

fn is_valid_ror_id(s: &str) -> bool {
    s.len() == 9
        && s.starts_with('0')
        && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
}

fn strip_prefix_case_insensitive<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() >= prefix.len()
        && s.is_char_boundary(prefix.len())
        && s[..prefix.len()].eq_ignore_ascii_case(prefix)
    {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}
