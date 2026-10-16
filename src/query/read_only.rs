//! Inspect the complete PostgreSQL tree before source execution.
//! Database roles and read-only transactions still enforce database access.

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Query, Select, Statement, Value, Visitor,
};
use std::ops::ControlFlow;

pub(super) struct ReadOnly;

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
