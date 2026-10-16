//! Bounded PostgreSQL frontend setup independent of a pooled source connection.

use super::catalog::name;
use sqlparser::{
    ast::{
        ContextModifier, Expr, Set, Statement, TransactionAccessMode, TransactionIsolationLevel,
        TransactionMode, Value,
    },
    dialect::PostgreSqlDialect,
    tokenizer::{Token, Tokenizer},
};

/// These values keep pooled text responses stable across clients and cache hits.
pub(crate) fn fixed_setting(name: &str) -> Option<&'static str> {
    match name.to_ascii_lowercase().as_str() {
        "client_encoding" => Some("UTF8"),
        "standard_conforming_strings" => Some("on"),
        "datestyle" => Some("ISO, MDY"),
        "timezone" => Some("UTC"),
        "intervalstyle" => Some("postgres"),
        "bytea_output" => Some("hex"),
        "extra_float_digits" => Some("3"),
        "default_transaction_read_only" | "transaction_read_only" => Some("on"),
        "default_transaction_isolation" | "transaction_isolation" => Some("read committed"),
        _ => None,
    }
}

pub(crate) struct SessionSettings {
    pub search_path: String,
    pub application_name: String,
}

impl Default for SessionSettings {
    fn default() -> Self {
        Self {
            search_path: "\"$user\", public".into(),
            application_name: String::new(),
        }
    }
}

pub(crate) enum SetupResponse {
    Set {
        parameter: Option<(&'static str, String)>,
    },
    Show {
        name: String,
        value: String,
    },
}

impl SessionSettings {
    pub fn set_search_path(&mut self, value: &str) -> Result<(), &'static str> {
        if value.len() > 1024 || value.contains('\0') || !valid_search_path(value) {
            return Err("unsupported or oversized metadata search path");
        }
        self.search_path = value.into();
        Ok(())
    }
    /// Only bounded literal assignments; rejected commands cannot alter pool state.
    pub fn handle(&mut self, statement: &Statement) -> Result<Option<SetupResponse>, &'static str> {
        let mut assignment = None;
        match statement {
            Statement::Set(Set::SingleAssignment {
                scope,
                hivevar: false,
                variable,
                values,
            }) if !matches!(scope, Some(ContextModifier::Local)) => {
                let Some((None, setting)) = name(variable) else {
                    return Err("unsupported session setting");
                };
                let values = values
                    .iter()
                    .map(|value| {
                        if setting == "search_path" {
                            schema_name(value)
                        } else {
                            literal(value)
                        }
                    })
                    .collect::<Option<Vec<_>>>()
                    .ok_or("session settings require literal values")?;
                assignment = Some((setting, values.join(", ")));
            }
            Statement::Set(Set::SetTimeZone {
                local: false,
                value,
            }) => {
                assignment = Some((
                    "timezone".into(),
                    literal(value).ok_or("session settings require literal values")?,
                ));
            }
            Statement::Set(Set::SetNames {
                charset_name,
                collation_name: None,
            }) => {
                assignment = Some(("client_encoding".into(), charset_name.value.clone()));
            }
            Statement::Set(Set::SetTransaction {
                session: true,
                snapshot: None,
                modes,
            }) if !modes.is_empty()
                && modes.iter().all(|mode| {
                    matches!(
                        mode,
                        TransactionMode::AccessMode(TransactionAccessMode::ReadOnly)
                            | TransactionMode::IsolationLevel(
                                TransactionIsolationLevel::ReadCommitted
                            )
                    )
                }) =>
            {
                return Ok(Some(SetupResponse::Set { parameter: None }));
            }
            Statement::Set(_) => return Err("unsupported session setting"),
            Statement::ShowVariable { variable } => {
                let setting = variable
                    .iter()
                    .map(|i| i.value.to_ascii_lowercase())
                    .collect::<Vec<_>>()
                    .join(" ");
                let value = match setting.as_str() {
                    "search_path" => Some(self.search_path.as_str()),
                    "application_name" => Some(self.application_name.as_str()),
                    _ => fixed_setting(&setting),
                };
                if let Some(value) = value {
                    let column = match setting.as_str() {
                        "datestyle" => "DateStyle",
                        "timezone" => "TimeZone",
                        "intervalstyle" => "IntervalStyle",
                        _ => &setting,
                    };
                    return Ok(Some(SetupResponse::Show {
                        name: column.into(),
                        value: value.into(),
                    }));
                }
            }
            _ => {}
        }
        if let Some((setting, value)) = assignment {
            if value.len() > 1024 || value.contains('\0') {
                return Err("session setting exceeds its bound");
            }
            let parameter = match setting.as_str() {
                "application_name" if value.len() <= 63 => {
                    self.application_name = value.clone();
                    Some(("application_name", value))
                }
                "search_path" => {
                    self.set_search_path(&value)?;
                    None
                }
                _ => {
                    let Some(fixed) = fixed_setting(&setting) else {
                        return Err("unsupported session setting");
                    };
                    let equivalent = value.eq_ignore_ascii_case(fixed)
                        || (fixed == "on"
                            && matches!(value.to_ascii_lowercase().as_str(), "true" | "1"))
                        || (setting == "client_encoding" && value.eq_ignore_ascii_case("UTF-8"));
                    if !equivalent {
                        return Err("changing protected session settings is unsupported");
                    }
                    None
                }
            };
            return Ok(Some(SetupResponse::Set { parameter }));
        }
        Ok(None)
    }
}

fn valid_search_path(value: &str) -> bool {
    let Ok(tokens) = Tokenizer::new(&PostgreSqlDialect {}, value).tokenize() else {
        return false;
    };
    let tokens = tokens
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_) | Token::EOF))
        .collect::<Vec<_>>();
    if tokens.is_empty() || tokens.len() > 31 || tokens.len() % 2 == 0 {
        return false;
    }
    tokens.iter().enumerate().all(|(position, token)| {
        if position % 2 == 1 {
            return matches!(token, Token::Comma);
        }
        match token {
            Token::Word(word) if word.quote_style.is_none() || word.quote_style == Some('"') => {
                let catalog = if word.quote_style.is_some() {
                    word.value == "pg_catalog"
                } else {
                    word.value.eq_ignore_ascii_case("pg_catalog")
                };
                // Otherwise an application operator can override a native one.
                !catalog || position == 0
            }
            Token::Placeholder(word) => word == "$user",
            _ => false,
        }
    })
}

fn schema_name(expression: &Expr) -> Option<String> {
    let value = match expression {
        Expr::Identifier(identifier) if identifier.quote_style.is_none() => {
            identifier.value.to_ascii_lowercase()
        }
        Expr::Identifier(identifier) => identifier.value.clone(),
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(value) => value.clone(),
            _ => return None,
        },
        _ => return None,
    };
    if value.is_empty() {
        return None;
    }
    let simple = value
        .bytes()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == b'_')
        && value
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_' || c == b'$');
    Some(if simple {
        value
    } else {
        format!("\"{}\"", value.replace('"', "\"\""))
    })
}

fn literal(expression: &Expr) -> Option<String> {
    match expression {
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(s) | Value::Number(s, _) => Some(s.clone()),
            Value::Boolean(value) => Some(value.to_string()),
            _ => None,
        },
        Expr::Identifier(identifier) => Some(if identifier.quote_style.is_some() {
            format!("\"{}\"", identifier.value.replace('"', "\"\""))
        } else {
            identifier.value.clone()
        }),
        _ => None,
    }
}
