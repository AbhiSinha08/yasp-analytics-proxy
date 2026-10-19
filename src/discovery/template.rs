//! Whole-statement matching with bounded literal-only metadata predicates.
use sqlparser::{
    dialect::Dialect,
    tokenizer::{Token, Tokenizer, Whitespace},
};

/// Capture slots accept bounded strings/IN lists, or an explicit unsigned OID literal.
/// Repeated slots must have identical values across UNION branches.
pub(crate) fn capture<'a>(
    actual: &'a [Token],
    expected: &'a [Token],
) -> Option<std::collections::BTreeMap<&'a str, Vec<&'a str>>> {
    let mut actual = actual.iter().peekable();
    let mut captures = std::collections::BTreeMap::new();
    for expected in expected {
        let marker = match expected {
            Token::SingleQuotedString(marker)
                if matches!(
                    marker.as_str(),
                    "__YASP_SCHEMA__"
                        | "__YASP_TABLE__"
                        | "__YASP_COLUMN__"
                        | "__YASP_SCHEMAS__"
                        | "__YASP_TABLES__"
                        | "__YASP_OID__"
                        | "__YASP_RELATION__"
                ) =>
            {
                marker.as_str()
            }
            _ => {
                if actual.next()? != expected {
                    return None;
                }
                continue;
            }
        };
        let mut values = Vec::new();
        loop {
            // Redshift JDBC wraps prepared string parameters in parentheses
            // even in simple-query mode. Only a literal may appear inside.
            let mut wrappers = 0;
            while actual.peek() == Some(&&Token::LParen) {
                actual.next();
                wrappers += 1;
                if wrappers > 64 {
                    return None;
                }
            }
            let value = match actual.next()? {
                Token::Number(value, false)
                    if marker == "__YASP_OID__"
                        && value.len() <= 10
                        && value.bytes().all(|b| b.is_ascii_digit())
                        && value.parse::<u32>().is_ok() =>
                {
                    value
                }
                Token::SingleQuotedString(value) if marker != "__YASP_OID__" => value,
                _ => return None,
            };
            if value.len() > 1024 || value.contains('\0') || values.len() >= 256 {
                return None;
            }
            values.push(value.as_str());
            for _ in 0..wrappers {
                if actual.next()? != &Token::RParen {
                    return None;
                }
            }
            if matches!(marker, "__YASP_SCHEMAS__" | "__YASP_TABLES__")
                && actual.peek() == Some(&&Token::Comma)
            {
                actual.next();
            } else {
                break;
            }
        }
        if let Some(old) = captures.insert(marker, values.clone())
            && old != values
        {
            return None;
        }
    }
    actual.next().is_none().then_some(captures)
}
pub(crate) fn tokens(sql: &str, dialect: &dyn Dialect) -> Option<Vec<Token>> {
    let mut tokens = Tokenizer::new(dialect, sql).tokenize().ok()?;
    tokens.retain(|t| {
        !matches!(
            t,
            Token::Whitespace(
                Whitespace::Space
                    | Whitespace::Newline
                    | Whitespace::Tab
                    | Whitespace::SingleLineComment { .. }
                    | Whitespace::MultiLineComment(_)
            )
        )
    });
    if matches!(tokens.last(), Some(Token::SemiColon)) {
        tokens.pop();
    }
    if tokens.len() > 4096 {
        return None;
    }
    for token in &mut tokens {
        if let Token::Word(word) = token
            && word.quote_style.is_none()
        {
            word.value.make_ascii_lowercase();
        }
    }
    Some(tokens)
}
