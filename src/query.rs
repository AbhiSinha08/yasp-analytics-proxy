//! PostgreSQL query syntax and read-only inspection. The SQL and its parsed tree
//! share one lifetime; consumers inspect the tree without reparsing the SQL.

use sha2::{Digest, Sha256};
use sqlparser::{
    ast::{
        Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Query, Select, Value, Visit, Visitor,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
    tokenizer::{Token, Tokenizer},
};
use std::{collections::HashMap, ops::ControlFlow};
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
        if sql.len() > MAX_SQL_BYTES {
            return Err(QueryError::TooLarge);
        }
        let tokens = Tokenizer::new(&PostgreSqlDialect {}, &sql)
            .tokenize_with_location()
            .map_err(|_| QueryError::Syntax)?;
        if tokens
            .iter()
            .filter(|t| !matches!(t.token, Token::Whitespace(_) | Token::EOF))
            .count()
            > MAX_SQL_TOKENS
        {
            return Err(QueryError::TooComplex);
        }
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
        if sql.len() > max_sql_bytes {
            return Err(QueryError::TooLarge);
        }
        let dialect = PostgreSqlDialect {};
        let tokens = Tokenizer::new(&dialect, sql)
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

fn query_id(sql: &str, tokens: &[sqlparser::tokenizer::TokenWithSpan]) -> String {
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

struct ReadOnly;

impl Visitor for ReadOnly {
    type Break = ();

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<()> {
        if matches!(
            statement,
            Statement::Query(_) | Statement::ShowVariable { .. }
        ) {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if query.locks.is_empty()
            && query.pipe_operators.is_empty()
            && query.settings.is_none()
            && query.format_clause.is_none()
            && query.for_clause.is_none()
        {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<()> {
        if select.into.is_none() {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if let Expr::Function(function) = expr
            && function
                .name
                .0
                .last()
                .and_then(|part| part.as_ident())
                .is_some_and(|name| name.value.eq_ignore_ascii_case("set_config"))
        {
            // Session settings can change serialization or execution bounds
            // during a read. Only this harmless, resettable label is allowed.
            let allowed = matches!(&function.args, FunctionArguments::List(list)
                    if matches!(list.args.first(), Some(FunctionArg::Unnamed(
                        FunctionArgExpr::Expr(Expr::Value(value))))
                        if matches!(&value.value, Value::SingleQuotedString(name)
                            if name.eq_ignore_ascii_case("application_name"))));
            if !allowed {
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inspect_the_preserved_tree_including_nested_writes() {
        for sql in [
            "/* context */ WITH r AS (SELECT 1 AS n) SELECT n FROM r",
            "SELECT '; DELETE' AS value UNION ALL SELECT 'safe'",
            "SHOW ALL",
            "SHOW TIME ZONE",
            "SELECT pg_catalog.set_config('application_name', 'label', false)",
            "SELECT u & \"column\" FROM bits",
        ] {
            let parsed = ParsedQuery::parse(sql).unwrap();
            parsed.validate_read_only().unwrap();
            assert_eq!(parsed.sql(), sql);
        }
        for sql in [
            "WITH r AS (DELETE FROM users RETURNING *) SELECT * FROM r",
            "WITH r AS (UPDATE users SET email = 'x' RETURNING *) SELECT * FROM r",
            "SELECT * FROM (SELECT * INTO stolen FROM users) s",
            "SELECT * FROM users FOR UPDATE",
            "SET ROLE local",
            "BEGIN",
            "SELECT set_config('DateStyle', 'SQL, DMY', false)",
            "SELECT pg_catalog.\"set_config\"('client_encoding', 'LATIN1', false)",
            "SELECT SET_CONFIG(lower('application_name'), 'label', false)",
            r#"SELECT U&"set\005fconfig"('DateStyle', 'SQL, DMY', false)"#,
        ] {
            assert!(
                !ParsedQuery::parse(sql).is_ok_and(|q| q.validate_read_only().is_ok()),
                "{sql}"
            );
        }
        assert!(matches!(
            ParsedQuery::parse("SELECT 1; SELECT 2"),
            Err(QueryError::MultipleStatements)
        ));
    }

    #[test]
    fn query_id_normalizes_only_safe_formatting() {
        let id = |sql| ParsedQuery::parse(sql).unwrap().query_id().to_owned();
        assert_eq!(
            id("SELECT a FROM t;"),
            id(" select  a\nfrom t ; -- comment\n")
        );
        assert_eq!(id("SELECT a FROM t"), id("SELECT a /* comment */ FROM t"));
        assert_eq!(
            id("SELECT '雪' /* note */ FROM t;"),
            id(" select '雪'\r\n/* 雪 */ from t ;")
        );
        assert_ne!(id("SELECT 'A'"), id("SELECT 'a'"));
        assert_ne!(id("SELECT 'é'"), id("SELECT 'É'"));
        assert_ne!(id("SELECT \"Column\""), id("SELECT \"column\""));
        assert_ne!(id(r"SELECT E'it\'s'"), id("SELECT E'it''s'"));
        assert_ne!(id("SELECT ab"), id("SELECT a b"));
        let hash = |sql| {
            let dialect = PostgreSqlDialect {};
            let tokens = Tokenizer::new(&dialect, sql)
                .tokenize_with_location()
                .unwrap();
            query_id(sql, &tokens)
        };
        assert_ne!(hash("SELECT 'a'\n'b'"), hash("SELECT 'a' 'b'"));
        assert_ne!(hash("SELECT 'a'\r\n'b'"), hash("SELECT 'a' 'b'"));
        assert!(
            id("SELECT 1")
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        );
        assert_eq!(id("SELECT 1").len(), 64);
    }

    #[test]
    fn reject_flat_trees_before_they_can_overflow_on_inspection_or_drop() {
        for sql in [
            format!("SELECT {}", vec!["1"; 100_000].join("+")),
            vec!["SELECT 1"; 50_000].join(" UNION ALL "),
        ] {
            assert!(matches!(
                ParsedQuery::parse(&sql),
                Err(QueryError::TooComplex)
            ));
        }
        let sql = format!("SELECT {}", vec!["1"; 2048].join("+"));
        let query = ParsedQuery::parse(&sql).unwrap();
        query.validate_read_only().unwrap();
        drop(query);
        assert!(
            ParsedQuery::parse(&format!("SELECT {}1{}", "(".repeat(100), ")".repeat(100))).is_err()
        );
    }
}
