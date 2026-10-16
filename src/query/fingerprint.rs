//! Stable SQL fingerprints preserve literal spelling and meaningful separators.

use sha2::{Digest, Sha256};
use sqlparser::tokenizer::Token;
use std::collections::HashMap;

pub(super) fn query_id(sql: &str, tokens: &[sqlparser::tokenizer::TokenWithSpan]) -> String {
    let end = tokens
        .iter()
        .rposition(|token| !matches!(&token.token, Token::Whitespace(_) | Token::EOF))
        .map(|index| {
            let mut end = index + 1;
            while end > 0 && matches!(tokens[end - 1].token, Token::SemiColon) {
                end -= 1;
                while end > 0 && matches!(tokens[end - 1].token, Token::Whitespace(_)) {
                    end -= 1;
                }
            }
            end
        })
        .unwrap_or(0);
    let significant = tokens[..end]
        .iter()
        .filter(|token| !matches!(&token.token, Token::Whitespace(_) | Token::EOF))
        .collect::<Vec<_>>();
    let locations = significant
        .iter()
        .flat_map(|token| [token.span.start, token.span.end])
        .collect::<std::collections::HashSet<_>>();
    let mut offsets = HashMap::with_capacity(locations.len());
    let mut line = 1;
    let mut column = 1;
    for (offset, ch) in sql.char_indices() {
        if locations.contains(&sqlparser::tokenizer::Location::new(line, column)) {
            offsets.insert(sqlparser::tokenizer::Location::new(line, column), offset);
        }
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    if locations.contains(&sqlparser::tokenizer::Location::new(line, column)) {
        offsets.insert(sqlparser::tokenizer::Location::new(line, column), sql.len());
    }

    let mut hash = Sha256::new();
    let mut previous_is_string = false;
    let mut previous_end = None;
    for token in significant {
        let start = offsets[&token.span.start];
        if let Some(previous_end) = previous_end {
            let newline = sql[previous_end..start].contains(['\n', '\r']);
            hash.update([
                if previous_is_string && is_string(&token.token) && newline {
                    2
                } else {
                    1
                },
            ]);
        }
        let finish = offsets[&token.span.end];
        let raw = &sql[start..finish];
        let normalized;
        let spelling = match &token.token {
            Token::Word(word) if word.quote_style.is_none() => {
                normalized = raw.to_ascii_lowercase();
                normalized.as_bytes()
            }
            _ => raw.as_bytes(),
        };
        hash.update((spelling.len() as u64).to_be_bytes());
        hash.update(spelling);
        previous_is_string = is_string(&token.token);
        previous_end = Some(finish);
    }
    let digest = hash.finalize();
    let mut result = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write;
        write!(result, "{byte:02x}").expect("writing to a String cannot fail");
    }
    result
}

fn is_string(token: &Token) -> bool {
    matches!(
        token,
        Token::SingleQuotedString(_)
            | Token::TripleSingleQuotedString(_)
            | Token::DollarQuotedString(_)
            | Token::SingleQuotedByteStringLiteral(_)
            | Token::SingleQuotedRawStringLiteral(_)
            | Token::TripleSingleQuotedRawStringLiteral(_)
            | Token::EscapedStringLiteral(_)
            | Token::UnicodeStringLiteral(_)
            | Token::NationalStringLiteral(_)
            | Token::QuoteDelimitedStringLiteral(_)
            | Token::NationalQuoteDelimitedStringLiteral(_)
            | Token::HexStringLiteral(_)
    )
}
