//! Dialect-aware query syntax and read-only inspection. Effective SQL and the
//! immutable parsed tree stay together through routing, caching, and execution.

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

/// SQL and its single statement. Access is immutable so the
/// inspected tree cannot diverge from the SQL sent to the database.
pub struct ParsedQuery {
    sql: String,
    statement: Statement,
    query_id: std::sync::OnceLock<String>,
    dialect: SqlDialect,
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
        Self::parse_with_dialect(sql, max_sql_bytes, SqlDialect::Postgresql)
    }

    pub fn parse_with_dialect(
        sql: &str,
        max_sql_bytes: usize,
        dialect: SqlDialect,
    ) -> Result<Self, QueryError> {
        let parser_dialect = dialect.parser();
        let tokens = bounded_tokens(sql, max_sql_bytes, dialect)?;
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
        let mut statements = Parser::new(parser_dialect)
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
            query_id: std::sync::OnceLock::new(),
            dialect,
            serialized_ast: std::sync::OnceLock::new(),
        })
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn dialect(&self) -> SqlDialect {
        self.dialect
    }

    /// Stable lowercase SHA-256 identifier for this query's normalized tokens.
    pub fn query_id(&self) -> &str {
        self.query_id.get_or_init(|| {
            // This immutable SQL has already passed tokenization and all bounds.
            let tokens = bounded_tokens(&self.sql, self.sql.len(), self.dialect)
                .expect("parsed SQL remains valid for fingerprinting");
            query_id(&self.sql, &tokens)
        })
    }

    /// Rust extensions borrow this tree. Replacements construct a new query so
    /// the inspected AST always describes the SQL sent to the backend.
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
