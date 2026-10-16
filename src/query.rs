//! PostgreSQL query syntax and read-only inspection. Original SQL and the
//! immutable parsed tree stay together through context, routing, and execution.

mod fingerprint;
mod read_only;

use fingerprint::query_id;
use read_only::ReadOnly;
use sqlparser::{
    ast::Visit,
    dialect::PostgreSqlDialect,
    parser::Parser,
    tokenizer::{Token, TokenWithSpan, Tokenizer},
};
use thiserror::Error;

pub use sqlparser::ast::Statement;

const MAX_SQL_BYTES: usize = 1024 * 1024;
// A nesting limit alone does not bound left-associative trees or their drop stack.
const MAX_SQL_TOKENS: usize = 4096;

/// Original SQL and its single PostgreSQL statement. Access is immutable so the
/// inspected tree cannot diverge from the SQL sent to the database.
pub struct ParsedQuery {
    sql: String,
    statement: Statement,
    query_id: String,
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
    #[error("unsupported or invalid PostgreSQL syntax")]
    Syntax,
    #[error("query is outside the supported read-only scope")]
    Unsupported,
}

impl ParsedQuery {
    /// A generated metadata statement has its own effective SQL and query ID.
    pub(crate) fn from_statement(statement: Statement) -> Result<Self, QueryError> {
        let sql = statement.to_string();
        let tokens = bounded_tokens(&sql, MAX_SQL_BYTES)?;
        let query_id = query_id(&sql, &tokens);
        Ok(Self {
            sql,
            statement,
            query_id,
        })
    }

    pub fn parse(sql: &str) -> Result<Self, QueryError> {
        Self::parse_with_limit(sql, MAX_SQL_BYTES)
    }

    pub(crate) fn parse_with_limit(sql: &str, max_sql_bytes: usize) -> Result<Self, QueryError> {
        let dialect = PostgreSqlDialect {};
        let tokens = bounded_tokens(sql, max_sql_bytes)?;
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
        let mut statements = Parser::new(&dialect)
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
        })
    }

    pub fn sql(&self) -> &str {
        &self.sql
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

    pub fn validate_read_only(&self) -> Result<(), QueryError> {
        if self.statement.visit(&mut ReadOnly).is_break() {
            Err(QueryError::Unsupported)
        } else {
            Ok(())
        }
    }
}

fn bounded_tokens(sql: &str, max_sql_bytes: usize) -> Result<Vec<TokenWithSpan>, QueryError> {
    if sql.len() > max_sql_bytes {
        return Err(QueryError::TooLarge);
    }
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, sql)
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
