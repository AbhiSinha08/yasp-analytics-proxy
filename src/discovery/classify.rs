//! Recognize metadata-only PostgreSQL statements over the complete parsed tree.
//! Unknown expressions and application data retain ordinary routing.

use super::{
    catalog::{catalog_relation, helper, name, safe_type},
    session::fixed_setting,
};
use crate::query::ParsedQuery;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName, Query,
    SelectItem, SetExpr, Statement, TableFactor, Value, Visit, Visitor,
};
use std::{collections::VecDeque, ops::ControlFlow};

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
        ctes: Vec::new(),
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
            matches!(expr, Expr::Identifier(i) if i.quote_style.is_none()
                && matches!(i.value.to_ascii_lowercase().as_str(), "current_schema" | "current_role"))
                || matches!(expr, Expr::Function(f) if name(&f.name).is_some_and(|(_, n)| matches!(n.as_str(),
                "version" | "current_catalog" | "current_database" | "current_schema" | "current_schemas" | "current_setting"
                    | "current_user" | "session_user" | "current_role" | "user")))
        })
}

struct Metadata {
    catalog: bool,
    fingerprint: bool,
    role_identity: bool,
    ctes: Vec<CteScope>,
}

#[derive(Default)]
struct CteScope {
    visible: Vec<String>,
    pending: VecDeque<String>,
}

impl Metadata {
    // Function calls in SELECT and FROM must share the same allowlist and
    // setting restrictions, regardless of the parser's table-function shape.
    fn function(&mut self, object: &ObjectName, args: &[FunctionArg]) -> ControlFlow<()> {
        if !helper(object) {
            return ControlFlow::Break(());
        }
        let (_, function) = name(object).unwrap();
        if function == "current_setting" {
            let valid = matches!(args.first(), Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v))))
                if matches!(&v.value, Value::SingleQuotedString(s) if fixed_setting(s).is_some()
                    || matches!(s.to_ascii_lowercase().as_str(), "server_version" | "server_version_num" | "search_path" | "application_name")));
            if !valid {
                return ControlFlow::Break(());
            }
        }
        self.fingerprint |= matches!(
            function.as_str(),
            "version"
                | "current_database"
                | "current_schema"
                | "current_schemas"
                | "current_setting"
        );
        // These prefixes only apply after explicit helper allowlisting above.
        self.catalog |= function.starts_with("pg_")
            || function.starts_with("has_")
            || matches!(
                function.as_str(),
                "format_type" | "obj_description" | "col_description" | "shobj_description"
            );
        ControlFlow::Continue(())
    }
}

impl Visitor for Metadata {
    type Break = ();
    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        // TABLE stores relation names as strings, outside the visitor's table
        // checks. Only the set-expression shapes inspected below use discovery.
        let mut bodies = vec![query.body.as_ref()];
        while let Some(body) = bodies.pop() {
            match body {
                SetExpr::SetOperation { left, right, .. } => {
                    bodies.extend([left.as_ref(), right.as_ref()]);
                }
                SetExpr::Select(_) | SetExpr::Query(_) | SetExpr::Values(_) => {}
                _ => return ControlFlow::Break(()),
            }
        }
        let mut scope = CteScope::default();
        if let Some(with) = &query.with {
            if with.recursive {
                return ControlFlow::Break(());
            }
            for cte in &with.cte_tables {
                let object = ObjectName::from(vec![cte.alias.name.clone()]);
                // Native-name qualification must never rewrite a CTE reference.
                if catalog_relation(&object).is_some() || cte.from.is_some() {
                    return ControlFlow::Break(());
                }
                scope.pending.push_back(name(&object).unwrap().1);
            }
        }
        self.ctes.push(scope);
        ControlFlow::Continue(())
    }
    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.ctes.pop();
        // sqlparser visits WITH definitions in order before the query body.
        // Nonrecursive CTEs become visible only after their definition is checked.
        if let Some(parent) = self.ctes.last_mut()
            && let Some(alias) = parent.pending.pop_front()
        {
            parent.visible.push(alias);
        }
        ControlFlow::Continue(())
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
                if self::name(name).is_some_and(|(schema, relation)| {
                    schema.is_none()
                        && self
                            .ctes
                            .iter()
                            .any(|scope| scope.visible.contains(&relation))
                }) {
                    return ControlFlow::Continue(());
                }
                if catalog_relation(name).is_none() {
                    return ControlFlow::Break(());
                }
                self.catalog = true;
            }
            TableFactor::Table {
                name,
                args: Some(args),
                ..
            } => self.function(name, &args.args)?,
            TableFactor::Function { name, args, .. } => self.function(name, args)?,
            TableFactor::Derived { sample: None, .. }
            | TableFactor::NestedJoin { .. }
            | TableFactor::UNNEST { .. } => {}
            _ => return ControlFlow::Break(()),
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<()> {
        match expression {
            // sqlparser preserves these PostgreSQL keywords as identifiers.
            Expr::Identifier(i)
                if i.quote_style.is_none() && i.value.eq_ignore_ascii_case("current_schema") =>
            {
                self.fingerprint = true;
            }
            Expr::Identifier(i)
                if i.quote_style.is_none() && i.value.eq_ignore_ascii_case("current_role") =>
            {
                self.role_identity = true;
            }
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
                // PostgreSQL identity keywords have no call syntax and cannot be
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
                            "current_user"
                                | "session_user"
                                | "current_role"
                                | "user"
                                | "current_catalog"
                                | "current_schema"
                        )
                    })
                {
                    let (_, keyword) = name(&function.name).unwrap();
                    if matches!(keyword.as_str(), "current_catalog" | "current_schema") {
                        self.fingerprint = true;
                    } else {
                        self.role_identity = true;
                    }
                    return ControlFlow::Continue(());
                }
                let args = match &function.args {
                    FunctionArguments::List(list) => list.args.as_slice(),
                    _ => &[],
                };
                self.function(&function.name, args)?;
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
