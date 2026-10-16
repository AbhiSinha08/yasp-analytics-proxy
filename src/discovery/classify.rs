//! Recognize metadata-only PostgreSQL statements over the complete parsed tree.
//! Unknown expressions and application data retain ordinary routing.

use super::{
    catalog::{catalog_relation, helper, name, safe_type},
    session::fixed_setting,
};
use crate::query::ParsedQuery;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Query, SelectItem,
    SetExpr, Statement, TableFactor, Value, Visit, Visitor,
};
use std::ops::ControlFlow;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiscoveryKind {
    Catalog,
    Fingerprint,
}

/// Conservative classification over the complete immutable frontend statement.
/// Unknown shapes and helpers continue through ordinary application routing.
pub fn classify(query: &ParsedQuery) -> Option<DiscoveryKind> {
    if query.validate_read_only().is_err() {
        return None;
    }
    if let Statement::ShowVariable { variable } = query.statement() {
        let name = variable
            .iter()
            .map(|i| i.value.to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(" ");
        return matches!(name.as_str(), "server_version" | "server_version_num")
            .then_some(DiscoveryKind::Fingerprint);
    }
    let mut visitor = Metadata {
        catalog: false,
        fingerprint: false,
        role_identity: false,
    };
    if query.statement().visit(&mut visitor).is_break() {
        return None;
    }
    if visitor.catalog {
        Some(DiscoveryKind::Catalog)
    } else if visitor.fingerprint
        && (!visitor.role_identity || fingerprint_projection(query.statement()))
    {
        Some(DiscoveryKind::Fingerprint)
    } else {
        None
    }
}

// Clients probe schema and login in separate result columns. Computed identities
// and standalone role reads retain application routing and its selected role.
fn fingerprint_projection(statement: &Statement) -> bool {
    let Statement::Query(query) = statement else {
        return false;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select.from.is_empty()
        && select.projection.iter().all(|item| {
            let (SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. }) = item else {
                return false;
            };
            matches!(expr, Expr::Function(f) if name(&f.name).is_some_and(|(_, n)| matches!(n.as_str(),
                "version" | "current_database" | "current_schema" | "current_schemas" | "current_setting"
                    | "current_user" | "session_user" | "current_role" | "user")))
        })
}

struct Metadata {
    catalog: bool,
    fingerprint: bool,
    role_identity: bool,
}

impl Visitor for Metadata {
    type Break = ();
    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        // CTE aliases can shadow native catalogs; these use ordinary routing.
        if query.with.is_some() {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }
    fn pre_visit_table_factor(&mut self, table: &TableFactor) -> ControlFlow<()> {
        match table {
            TableFactor::Table {
                name,
                args: None,
                sample: None,
                version: None,
                ..
            } => {
                if catalog_relation(name).is_none() {
                    return ControlFlow::Break(());
                }
                self.catalog = true;
            }
            TableFactor::Table {
                name,
                args: Some(_),
                ..
            }
            | TableFactor::Function { name, .. } => {
                if !helper(name) {
                    return ControlFlow::Break(());
                }
                // Only allowlisted helpers reach this point; the prefix marks
                // metadata output, never permission to call arbitrary pg_* code.
                self.catalog |= self::name(name).is_some_and(|(_, n)| n.starts_with("pg_"));
            }
            TableFactor::Derived { sample: None, .. }
            | TableFactor::NestedJoin { .. }
            | TableFactor::UNNEST { .. } => {}
            _ => return ControlFlow::Break(()),
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<()> {
        match expression {
            // sqlparser represents PostgreSQL ARRAY(SELECT ...) as a function.
            // Its subquery is still visited and must contain metadata only.
            Expr::Function(function)
                if function.name.0.len() == 1
                    && function.name.0[0].as_ident().is_some_and(|i| {
                        i.quote_style.is_none() && i.value.eq_ignore_ascii_case("array")
                    })
                    && matches!(function.args, FunctionArguments::Subquery(_))
                    && matches!(function.parameters, FunctionArguments::None) => {}
            Expr::Function(function) => {
                // PostgreSQL role keywords have no call syntax and cannot be
                // redirected to an application function with the same name.
                if function.name.0.len() == 1
                    && function.name.0[0]
                        .as_ident()
                        .is_some_and(|i| i.quote_style.is_none())
                    && matches!(function.args, FunctionArguments::None)
                    && matches!(function.parameters, FunctionArguments::None)
                    && name(&function.name).is_some_and(|(_, n)| {
                        matches!(
                            n.as_str(),
                            "current_user" | "session_user" | "current_role" | "user"
                        )
                    })
                {
                    self.role_identity = true;
                    return ControlFlow::Continue(());
                }
                if !helper(&function.name) {
                    return ControlFlow::Break(());
                }
                let (_, function_name) = name(&function.name).unwrap();
                if function_name == "current_setting" {
                    let valid = matches!(&function.args, FunctionArguments::List(list)
                        if matches!(list.args.first(), Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v))))
                            if matches!(&v.value, Value::SingleQuotedString(s) if fixed_setting(s).is_some() || matches!(s.as_str(), "server_version" | "server_version_num" | "search_path" | "application_name"))));
                    if !valid {
                        return ControlFlow::Break(());
                    }
                }
                self.fingerprint |= matches!(
                    function_name.as_str(),
                    "version"
                        | "current_database"
                        | "current_schema"
                        | "current_schemas"
                        | "current_setting"
                );
                self.catalog |= function_name.starts_with("pg_")
                    || matches!(
                        function_name.as_str(),
                        "obj_description" | "col_description"
                    );
            }
            Expr::Cast { data_type, .. } => {
                // A user-defined type can run arbitrary input/conversion functions.
                if !safe_type(data_type) {
                    return ControlFlow::Break(());
                }
            }
            // psql emits explicit native regex operators for object-name filters.
            // Arbitrary qualified operators may execute application functions.
            Expr::BinaryOp {
                op: BinaryOperator::PGCustomBinaryOperator(parts),
                ..
            } if matches!(parts.as_slice(), [schema, operator]
                if schema == "pg_catalog" && matches!(operator.as_str(), "~" | "~*" | "!~" | "!~*")) =>
                {}
            Expr::BinaryOp {
                op: BinaryOperator::Custom(_) | BinaryOperator::PGCustomBinaryOperator(_),
                ..
            } => return ControlFlow::Break(()),
            Expr::TypedString(typed) if !safe_type(&typed.data_type) => {
                return ControlFlow::Break(());
            }
            Expr::Convert {
                data_type: Some(data_type),
                ..
            } if !safe_type(data_type) => return ControlFlow::Break(()),
            Expr::Lambda(_) => return ControlFlow::Break(()),
            _ => {}
        }
        ControlFlow::Continue(())
    }
}
