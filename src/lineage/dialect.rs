//! Native naming and extraction rules. New dialects extend this concrete
//! dispatch; shared scopes and constituent unions do not depend on drivers.
use super::PathSegment;
use crate::query::SqlDialect;
use sqlparser::ast::{Expr, Ident, ObjectName, Value};

pub(super) fn ident(dialect: SqlDialect, name: &Ident) -> String {
    if name.quote_style.is_some() {
        return name.value.clone();
    }
    match dialect {
        SqlDialect::Snowflake => name.value.to_ascii_uppercase(),
        SqlDialect::Postgresql | SqlDialect::Redshift => name.value.to_ascii_lowercase(),
    }
}

pub(super) fn name(dialect: SqlDialect, name: &ObjectName) -> Option<Vec<String>> {
    name.0
        .iter()
        .map(|p| p.as_ident().map(|i| ident(dialect, i)))
        .collect()
}

pub(super) fn literal(expr: &Expr) -> Option<PathSegment> {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => Some(PathSegment::Key(s.clone())),
            Value::Number(n, _) => n.parse().ok().map(PathSegment::Index),
            _ => None,
        },
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => match literal(expr)? {
            PathSegment::Index(n) => n.checked_neg().map(PathSegment::Index),
            _ => None,
        },
        _ => None,
    }
}

pub(super) fn string(expr: &Expr) -> Option<&str> {
    if let Expr::Value(v) = expr
        && let Value::SingleQuotedString(s) = &v.value
    {
        Some(s)
    } else {
        None
    }
}

/// PostgreSQL text-array tokens have their own escaping and can address either
/// object keys or array elements. Keep tokens textual instead of guessing shape.
pub(super) fn postgres_path(expr: &Expr) -> Option<Vec<PathSegment>> {
    if let Expr::Array(array) = expr {
        return array
            .elem
            .iter()
            .map(|e| string(e).map(|s| PathSegment::Element { element: s.into() }))
            .collect();
    }
    let text = string(expr)?.strip_prefix('{')?.strip_suffix('}')?;
    if text.is_empty() {
        return Some(Vec::new());
    }
    let mut result = Vec::new();
    let mut chars = text.chars().peekable();
    loop {
        let quoted = chars.peek() == Some(&'"');
        if quoted {
            chars.next();
        }
        let mut token = String::new();
        let mut closed = !quoted;
        while let Some(ch) = chars.next() {
            match ch {
                '\\' => token.push(chars.next()?),
                '"' if quoted => {
                    closed = true;
                    break;
                }
                ',' if !quoted => {
                    closed = true;
                    break;
                }
                _ => token.push(ch),
            }
        }
        if !closed || (!quoted && token.eq_ignore_ascii_case("NULL")) {
            return None;
        }
        result.push(PathSegment::Element { element: token });
        if quoted && chars.peek().is_some() && chars.next() != Some(',') {
            return None;
        }
        if chars.peek().is_none() {
            break;
        }
    }
    Some(result)
}

/// Literal Snowflake GET_PATH syntax. Unsupported/irregular strings stay unknown.
pub(super) fn snowflake_path(text: &str) -> Option<Vec<PathSegment>> {
    let mut chars = text.trim_start_matches('$').chars().peekable();
    let mut result = Vec::new();
    while let Some(ch) = chars.peek().copied() {
        if ch == '.' || ch == ':' {
            chars.next();
            continue;
        }
        if ch == '[' {
            chars.next();
            let quote = chars.peek().copied().filter(|c| *c == '\'' || *c == '"');
            let mut value = String::new();
            if let Some(quote) = quote {
                chars.next();
                loop {
                    let c = chars.next()?;
                    if c == quote {
                        break;
                    }
                    value.push(if c == '\\' { chars.next()? } else { c });
                }
                if chars.next()? != ']' {
                    return None;
                }
                result.push(PathSegment::Key(value));
            } else {
                loop {
                    let c = chars.next()?;
                    if c == ']' {
                        break;
                    }
                    value.push(c);
                }
                result.push(PathSegment::Index(value.parse().ok()?));
            }
        } else if ch == '"' {
            chars.next();
            let mut key = String::new();
            loop {
                let c = chars.next()?;
                if c == '"' {
                    if chars.peek() == Some(&'"') {
                        chars.next();
                        key.push('"');
                    } else {
                        break;
                    }
                } else {
                    key.push(c);
                }
            }
            result.push(PathSegment::Key(key));
        } else {
            let mut key = String::new();
            while let Some(c) = chars.peek().copied() {
                if matches!(c, '.' | ':' | '[') {
                    break;
                }
                if !(c.is_alphanumeric() || c == '_') {
                    return None;
                }
                key.push(c);
                chars.next();
            }
            if key.is_empty() {
                return None;
            }
            result.push(PathSegment::Key(key));
        }
    }
    Some(result)
}

pub(super) fn function(dialect: SqlDialect, name: &ObjectName) -> Option<String> {
    let parts = self::name(dialect, name)?;
    match parts.as_slice() {
        [name] => Some(name.to_ascii_lowercase()),
        [schema, name] if dialect == SqlDialect::Postgresql && schema == "pg_catalog" => {
            Some(name.to_ascii_lowercase())
        }
        _ => None,
    }
}

pub(super) fn nested_type(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "json" | "jsonb" | "variant" | "object" | "super"
    )
}

/// Native date-part identifiers are grammar arguments, not column references.
pub(super) fn date_part_argument(dialect: SqlDialect, function: Option<&str>) -> bool {
    matches!(dialect, SqlDialect::Snowflake | SqlDialect::Redshift)
        && matches!(
            function,
            Some(
                "dateadd"
                    | "datediff"
                    | "date_part"
                    | "datepart"
                    | "timestampadd"
                    | "timestampdiff"
                    | "timeadd"
                    | "timediff"
            )
        )
}

/// Ordered-set aggregates take their data input from WITHIN GROUP.
pub(super) fn ordered_set(function: Option<&str>) -> bool {
    matches!(
        function,
        Some("percentile_cont" | "percentile_disc" | "mode")
    )
}

/// SQL wildcard ILIKE uses only % and _ as wildcards, not a regular expression.
pub(super) fn ilike(value: &str, pattern: &str) -> bool {
    let value: Vec<_> = value.to_lowercase().chars().collect();
    let pattern: Vec<_> = pattern.to_lowercase().chars().collect();
    let (mut i, mut j, mut star, mut retry) = (0, 0, None, 0);
    while i < value.len() {
        if j < pattern.len() && (pattern[j] == '_' || pattern[j] == value[i]) {
            i += 1;
            j += 1;
        } else if j < pattern.len() && pattern[j] == '%' {
            star = Some(j);
            j += 1;
            retry = i;
        } else if let Some(s) = star {
            retry += 1;
            i = retry;
            j = s + 1;
        } else {
            return false;
        }
    }
    while j < pattern.len() && pattern[j] == '%' {
        j += 1;
    }
    j == pattern.len()
}
