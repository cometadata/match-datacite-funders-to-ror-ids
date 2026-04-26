use datacite_ror::identifiers::{normalize_ror, normalize_fundref, sniff_identifier, IdentifierScheme};

// ---------- normalize_ror ----------

#[test]
fn ror_bare_id() {
    assert_eq!(normalize_ror("021nxhr62"), Some("https://ror.org/021nxhr62".to_string()));
}

#[test]
fn ror_with_host() {
    assert_eq!(normalize_ror("ror.org/021nxhr62"), Some("https://ror.org/021nxhr62".to_string()));
    assert_eq!(normalize_ror("www.ror.org/021nxhr62"), Some("https://ror.org/021nxhr62".to_string()));
    assert_eq!(normalize_ror("https://ror.org/021nxhr62"), Some("https://ror.org/021nxhr62".to_string()));
    assert_eq!(normalize_ror("http://ror.org/021nxhr62"), Some("https://ror.org/021nxhr62".to_string()));
}

#[test]
fn ror_uppercase_and_trailing_slash() {
    assert_eq!(normalize_ror("https://ROR.org/021NXHR62/"), Some("https://ror.org/021nxhr62".to_string()));
    assert_eq!(normalize_ror("021NXHR62"), Some("https://ror.org/021nxhr62".to_string()));
}

#[test]
fn ror_whitespace_trimmed() {
    assert_eq!(normalize_ror("  021nxhr62  "), Some("https://ror.org/021nxhr62".to_string()));
}

#[test]
fn ror_rejects_non_ror() {
    assert_eq!(normalize_ror(""), None);
    assert_eq!(normalize_ror("National Science Foundation"), None);
    assert_eq!(normalize_ror("10.13039/100000001"), None);
    assert_eq!(normalize_ror("12345"), None);                     // not 9 chars, doesn't start with 0
    assert_eq!(normalize_ror("12345678"), None);                  // not 9 chars
    assert_eq!(normalize_ror("1234567890"), None);                // 10 chars, doesn't start with 0
    assert_eq!(normalize_ror("a21nxhr62"), None);                 // doesn't start with 0
    assert_eq!(normalize_ror("021nxhr6!"), None);                 // invalid char
}

// ---------- normalize_fundref ----------

#[test]
fn fundref_bare_digits() {
    assert_eq!(normalize_fundref("100000001"), Some("100000001".to_string()));
    assert_eq!(normalize_fundref("501100001780"), Some("501100001780".to_string()));
}

#[test]
fn fundref_with_crossref_prefix() {
    assert_eq!(normalize_fundref("10.13039/100000001"), Some("100000001".to_string()));
}

#[test]
fn fundref_with_doi_url() {
    assert_eq!(normalize_fundref("doi.org/10.13039/100000001"), Some("100000001".to_string()));
    assert_eq!(normalize_fundref("https://doi.org/10.13039/100000001"), Some("100000001".to_string()));
    assert_eq!(normalize_fundref("http://dx.doi.org/10.13039/100000001"), Some("100000001".to_string()));
}

#[test]
fn fundref_trailing_slash_and_whitespace() {
    assert_eq!(normalize_fundref("  10.13039/100000001/  "), Some("100000001".to_string()));
}

#[test]
fn fundref_rejects_non_fundref() {
    assert_eq!(normalize_fundref(""), None);
    assert_eq!(normalize_fundref("021nxhr62"), None);
    assert_eq!(normalize_fundref("https://ror.org/021nxhr62"), None);
    assert_eq!(normalize_fundref("National Science Foundation"), None);
    assert_eq!(normalize_fundref("10.1234/something"), None);      // wrong DOI prefix, no digit body
    assert_eq!(normalize_fundref("abc"), None);
}

// ---------- sniff_identifier ----------

#[test]
fn sniff_picks_ror_when_value_is_ror() {
    let (scheme, canonical) = sniff_identifier("021nxhr62").unwrap();
    assert_eq!(scheme, IdentifierScheme::Ror);
    assert_eq!(canonical, "https://ror.org/021nxhr62");
}

#[test]
fn sniff_picks_fundref_when_value_is_fundref() {
    let (scheme, canonical) = sniff_identifier("10.13039/100000001").unwrap();
    assert_eq!(scheme, IdentifierScheme::Fundref);
    assert_eq!(canonical, "100000001");
}

#[test]
fn sniff_returns_none_on_ambiguous_or_unknown() {
    assert!(sniff_identifier("").is_none());
    assert!(sniff_identifier("something else").is_none());
    assert!(sniff_identifier("0000 0001 2345 6789").is_none());   // ISNI; we don't sniff it
}
