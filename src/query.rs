//! Dialect-aware query syntax and read-only inspection. Original SQL and the
//! immutable parsed tree stay together through context, routing, and execution.

mod fingerprint;
mod read_only;

use fingerprint::query_id;
use read_only::ReadOnly;
use sqlparser::{
    ast::Visit,
    dialect::{Dialect, PostgreSqlDialect, RedshiftSqlDialect, SnowflakeDialect},
    parser::Parser,
    tokenizer::{Token, TokenWithSpan, Tokenizer},
};
use thiserror::Error;

pub use sqlparser::ast::Statement;

const MAX_SQL_BYTES: usize = 1024 * 1024;
// A nesting limit alone does not bound left-associative trees or their drop stack.
const MAX_SQL_TOKENS: usize = 4096;

/// The input grammar, independent of the selected source and its credentials.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlDialect {
    #[default]
    Postgresql,
    Redshift,
    Snowflake,
}

impl SqlDialect {
    fn parser(self) -> &'static dyn Dialect {
        match self {
            Self::Postgresql => &PostgreSqlDialect {},
            Self::Redshift => &RedshiftSqlDialect {},
            Self::Snowflake => &SnowflakeDialect {},
        }
    }
}

/// Input grammar and native-query classification never grant discovery access.
#[derive(Clone, Copy, Debug)]
pub struct InputOptions {
    pub dialect: SqlDialect,
    pub native: bool,
    pub(crate) native_after_compatibility: Option<SqlDialect>,
}

impl InputOptions {
    pub fn frontend(dialect: SqlDialect) -> Self {
        Self {
            dialect,
            native: false,
            native_after_compatibility: None,
        }
    }

    pub fn native(dialect: SqlDialect) -> Self {
        Self {
            dialect,
            native: true,
            native_after_compatibility: None,
        }
    }

    /// An explicit host default for unmarked data SQL. Recognized frontend
    /// setup/catalog requests still use their compatibility grammar and handlers.
    pub fn native_after_compatibility(frontend: SqlDialect, native: SqlDialect) -> Self {
        Self {
            dialect: frontend,
            native: false,
            native_after_compatibility: Some(native),
        }
    }
}

/// Original SQL and its single statement. Access is immutable so the
/// inspected tree cannot diverge from the SQL sent to the database.
pub struct ParsedQuery {
    sql: String,
    statement: Statement,
    query_id: String,
    options: InputOptions,
    serialized_ast: std::sync::OnceLock<serde_json::Value>,
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("empty query")]
    Empty,
    #[error("one SQL statement per query is required")]
    MultipleStatements,
    #[error("SQL exceeds the configured byte limit")]
    TooLarge,
    #[error("SQL exceeds the 4096 significant-token limit")]
    TooComplex,
    #[error("unsupported or invalid SQL syntax for the input dialect")]
    Syntax,
    #[error("query is outside the supported read-only scope")]
    Unsupported,
}

impl ParsedQuery {
    pub fn parse(sql: &str) -> Result<Self, QueryError> {
        Self::parse_with_limit(sql, MAX_SQL_BYTES)
    }

    pub(crate) fn parse_with_limit(sql: &str, max_sql_bytes: usize) -> Result<Self, QueryError> {
        Self::parse_with_options(
            sql,
            max_sql_bytes,
            InputOptions::frontend(SqlDialect::Postgresql),
        )
    }

    pub fn parse_with_options(
        sql: &str,
        max_sql_bytes: usize,
        options: InputOptions,
    ) -> Result<Self, QueryError> {
        let dialect = options.dialect.parser();
        let tokens = bounded_tokens(sql, max_sql_bytes, options.dialect)?;
        // sqlparser does not decode PostgreSQL Unicode identifier escapes.
        // Reject that syntax rather than inspecting a different function name.
        if tokens.windows(3).any(|tokens| {
            matches!(&tokens[0].token, Token::Word(word)
                if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("u"))
                && tokens[0].span.end == tokens[1].span.start
                && matches!(tokens[1].token, Token::Ampersand)
                && tokens[1].span.end == tokens[2].span.start
                && matches!(&tokens[2].token, Token::Word(word) if word.quote_style == Some('"'))
        }) {
            return Err(QueryError::Unsupported);
        }
        let query_id = query_id(sql, &tokens);
        let mut statements = Parser::new(dialect)
            .with_recursion_limit(64)
            .with_tokens_with_locations(tokens)
            .parse_statements()
            .map_err(|_| QueryError::Syntax)?;
        match statements.len() {
            0 => return Err(QueryError::Empty),
            1 => {}
            _ => return Err(QueryError::MultipleStatements),
        }
        Ok(Self {
            sql: sql.to_owned(),
            statement: statements.remove(0),
            query_id,
            options,
            serialized_ast: std::sync::OnceLock::new(),
        })
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn input_options(&self) -> InputOptions {
        self.options
    }

    /// Stable lowercase SHA-256 identifier for this query's normalized tokens.
    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    /// Rust extensions can borrow this same tree. A future SQL rewrite must
    /// construct a new query and validate its effective SQL before execution.
    pub fn statement(&self) -> &Statement {
        &self.statement
    }

    /// The sqlparser 0.63 Serde shape, constructed once for plain JSON hooks.
    /// Parser upgrades may change this representation; Rust uses `statement()`.
    pub fn serialized_ast(&self) -> &serde_json::Value {
        self.serialized_ast.get_or_init(|| {
            serde_json::to_value(&self.statement)
                .expect("sqlparser AST contains only JSON-serializable values")
        })
    }

    pub fn validate_read_only(&self) -> Result<(), QueryError> {
        if self.statement.visit(&mut ReadOnly).is_break() {
            Err(QueryError::Unsupported)
        } else {
            Ok(())
        }
    }
}

fn bounded_tokens(
    sql: &str,
    max_sql_bytes: usize,
    dialect: SqlDialect,
) -> Result<Vec<TokenWithSpan>, QueryError> {
    if sql.len() > max_sql_bytes {
        return Err(QueryError::TooLarge);
    }
    let tokens = Tokenizer::new(dialect.parser(), sql)
        .tokenize_with_location()
        .map_err(|_| QueryError::Syntax)?;
    if tokens
        .iter()
        .filter(|token| !matches!(token.token, Token::Whitespace(_) | Token::EOF))
        .count()
        > MAX_SQL_TOKENS
    {
        return Err(QueryError::TooComplex);
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests;
