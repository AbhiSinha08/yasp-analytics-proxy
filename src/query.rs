//! PostgreSQL query syntax and read-only inspection. The SQL and its parsed tree
//! share one lifetime; consumers inspect the tree without reparsing the SQL.

use sqlparser::{
    ast::{
        Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Query, Select, Value, Visit, Visitor,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
    tokenizer::{Token, Tokenizer},
};
use std::ops::ControlFlow;
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
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("empty query")]
    Empty,
    #[error("one SQL statement per query is required")]
    MultipleStatements,
    #[error("SQL exceeds the 1 MiB limit")]
    TooLarge,
    #[error("SQL exceeds the 4096 significant-token limit")]
    TooComplex,
    #[error("unsupported or invalid PostgreSQL syntax")]
    Syntax,
    #[error("query is outside the supported read-only scope")]
    Unsupported,
}

impl ParsedQuery {
    pub fn parse(sql: &str) -> Result<Self, QueryError> {
        if sql.len() > MAX_SQL_BYTES {
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
        })
    }

    pub fn sql(&self) -> &str {
        &self.sql
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
