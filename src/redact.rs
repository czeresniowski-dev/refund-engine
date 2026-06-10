/// PII redaction for the layer that talks to the card network. Nothing in this
/// path may log a full PAN. We keep the last four and drop everything else of a
/// card-number shape before it reaches the log sink. PCI scope is a cost you
/// pay forever once a secret lands in a log; the cheapest time to prevent it is
/// in the code that makes the call.
///
/// A PAN here is a run of 13 to 19 digits, optionally separated by single
/// spaces or hyphens (the formats card data actually arrives in). We replace
/// the matched run with a masked form that preserves only the last four
/// digits, e.g. `4242 4242 4242 4242` -> `**** **** **** 4242`.
pub fn redact_pan(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;

    while i < bytes.len() {
        // Try to match a PAN-shaped run starting at i.
        if is_pan_start(bytes, i) {
            if let Some((end, digits)) = scan_pan(bytes, i) {
                out.push_str(&mask_keeping_last4(&input[i..end], &digits));
                i = end;
                continue;
            }
        }
        // Not a PAN: copy the byte through. Safe on a char boundary because we
        // only ever skip over ASCII digits/separators above.
        let ch_len = utf8_len(bytes[i]);
        out.push_str(&input[i..i + ch_len]);
        i += ch_len;
    }

    out
}

fn utf8_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b >> 5 == 0b110 {
        2
    } else if b >> 4 == 0b1110 {
        3
    } else {
        4
    }
}

fn is_pan_start(bytes: &[u8], i: usize) -> bool {
    bytes[i].is_ascii_digit()
}

/// Scan a PAN-shaped run from `start`. Returns the exclusive end index and the
/// collected digit string if the run has 13..=19 digits, else None.
fn scan_pan(bytes: &[u8], start: usize) -> Option<(usize, String)> {
    let mut i = start;
    let mut digits = String::new();
    let mut last_was_sep = false;

    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_digit() {
            digits.push(b as char);
            last_was_sep = false;
            i += 1;
        } else if (b == b' ' || b == b'-') && !last_was_sep && !digits.is_empty() {
            // Allow single separators between digit groups, but don't let the
            // run end on a separator.
            last_was_sep = true;
            i += 1;
        } else {
            break;
        }
    }

    // If we stopped on a trailing separator, back it out.
    if last_was_sep {
        i -= 1;
    }

    let len = digits.len();
    if (13..=19).contains(&len) {
        Some((i, digits))
    } else {
        None
    }
}

/// Mask the matched span, preserving its separator layout but replacing every
/// digit except the last four with `*`.
fn mask_keeping_last4(span: &str, digits: &str) -> String {
    let total = digits.len();
    let keep_from = total.saturating_sub(4);
    let mut digit_idx = 0;
    let mut out = String::with_capacity(span.len());
    for ch in span.chars() {
        if ch.is_ascii_digit() {
            if digit_idx >= keep_from {
                out.push(ch);
            } else {
                out.push('*');
            }
            digit_idx += 1;
        } else {
            out.push(ch);
        }
    }
    out
}

/// Convenience for structured logs: return only the last four of a PAN, or the
/// input unchanged if it isn't card-shaped.
pub fn last4(pan: &str) -> Option<String> {
    let digits: String = pan.chars().filter(|c| c.is_ascii_digit()).collect();
    if (13..=19).contains(&digits.len()) {
        Some(digits[digits.len() - 4..].to_string())
    } else {
        None
    }
}
