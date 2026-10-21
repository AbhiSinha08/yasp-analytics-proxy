//! Value-expression and native extraction traversal.
use super::*;

impl Analyzer<'_> {
    pub(super) fn expr(
        &mut self,
        expr: &Expr,
        scope: &Scope,
        ctes: &Ctes,
        outer: &[&Scope],
    ) -> Inputs {
        if self.depth >= 64 || !self.tick() {
            return Inputs::unknown("analysis limit");
        }
        self.depth += 1;
        let result = self.expression(expr, scope, ctes, outer);
        self.depth -= 1;
        if result.weight() > self.max_bytes {
            Inputs::unknown("analysis limit")
        } else {
            result
        }
    }

    fn expression(&mut self, expr: &Expr, scope: &Scope, ctes: &Ctes, outer: &[&Scope]) -> Inputs {
        match expr {
            Expr::Identifier(i) => self.lookup(std::slice::from_ref(i), scope, outer),
            Expr::CompoundIdentifier(parts) => self.lookup(parts, scope, outer),
            Expr::Value(_) | Expr::TypedString(_) => Inputs::default(),
            Expr::Nested(expr) | Expr::Cast { expr, .. } => self.expr(expr, scope, ctes, outer),
            Expr::Case {
                conditions,
                else_result,
                ..
            } => {
                let mut result = Inputs::default();
                for when in conditions {
                    result.union(self.expr(&when.result, scope, ctes, outer), self.max_bytes);
                }
                if let Some(expr) = else_result {
                    result.union(self.expr(expr, scope, ctes, outer), self.max_bytes);
                }
                result
            }
            Expr::Subquery(query) => {
                let mut parents = outer.to_vec();
                parents.push(scope);
                let relation = self.query(query, ctes, &parents);
                let mut result = Inputs::default();
                for column in relation.columns {
                    result.union(column.inputs, self.max_bytes);
                }
                if relation.unknown {
                    result.union(Inputs::unknown("subquery output"), self.max_bytes);
                }
                result
            }
            Expr::InSubquery { expr, subquery, .. } => {
                let mut result = self.expr(expr, scope, ctes, outer);
                let mut parents = outer.to_vec();
                parents.push(scope);
                let relation = self.query(subquery, ctes, &parents);
                for column in relation.columns {
                    result.union(column.inputs, self.max_bytes);
                }
                if relation.unknown {
                    result.union(Inputs::unknown("subquery output"), self.max_bytes);
                }
                result
            }
            Expr::Exists { .. } => Inputs::default(),
            Expr::JsonAccess { value, path } => {
                let mut inputs = self.expr(value, scope, ctes, outer);
                for part in &path.path {
                    inputs = match part {
                        JsonPathElem::Dot { key, .. } => {
                            inputs.path(vec![PathSegment::Key(key.clone())], self.max_bytes)
                        }
                        JsonPathElem::Bracket { key } | JsonPathElem::ColonBracket { key } => {
                            self.extract(inputs, key, false, scope, ctes, outer)
                        }
                    };
                }
                inputs
            }
            Expr::BinaryOp { left, op, right }
                if matches!(
                    op,
                    BinaryOperator::Arrow
                        | BinaryOperator::LongArrow
                        | BinaryOperator::HashArrow
                        | BinaryOperator::HashLongArrow
                ) =>
            {
                let inputs = self.expr(left, scope, ctes, outer);
                self.extract(
                    inputs,
                    right,
                    matches!(
                        op,
                        BinaryOperator::HashArrow | BinaryOperator::HashLongArrow
                    ),
                    scope,
                    ctes,
                    outer,
                )
            }
            Expr::CompoundFieldAccess { root, access_chain } => {
                let mut inputs = self.expr(root, scope, ctes, outer);
                for access in access_chain {
                    inputs = match access {
                        AccessExpr::Dot(Expr::Identifier(i)) => {
                            inputs.path(vec![PathSegment::Key(i.value.clone())], self.max_bytes)
                        }
                        AccessExpr::Subscript(Subscript::Index { index }) => {
                            self.extract(inputs, index, false, scope, ctes, outer)
                        }
                        _ => {
                            inputs.union(Inputs::unknown("field access"), self.max_bytes);
                            inputs
                        }
                    };
                }
                inputs
            }
            Expr::Function(function) => self.function(function, scope, ctes, outer),
            Expr::Wildcard(_) => self.star_inputs(
                None,
                &WildcardAdditionalOptions::default(),
                scope,
                ctes,
                outer,
            ),
            Expr::QualifiedWildcard(name, _) => self.star_inputs(
                Some(name),
                &WildcardAdditionalOptions::default(),
                scope,
                ctes,
                outer,
            ),
            Expr::Lambda(_) => Inputs::unknown("lambda scope"),
            _ => {
                // Visit immediate expression children only semantically. Nested
                // CASE/functions/subqueries re-enter their own rules, not a global
                // identifier scan that would include filters or branch predicates.
                let mut children = Children {
                    analyzer: self,
                    scope,
                    ctes,
                    outer,
                    root: true,
                    skip: 0,
                    children: 0,
                    result: Inputs::default(),
                };
                if expr.visit(&mut children).is_break() {
                    children.result.union(
                        Inputs::unknown("analysis limit"),
                        children.analyzer.max_bytes,
                    );
                }
                if children.children == 0 {
                    children.result.union(
                        Inputs::unknown("unsupported expression"),
                        children.analyzer.max_bytes,
                    );
                }
                children.result
            }
        }
    }

    fn extract(
        &mut self,
        inputs: Inputs,
        key: &Expr,
        text_path: bool,
        scope: &Scope,
        ctes: &Ctes,
        outer: &[&Scope],
    ) -> Inputs {
        let path = if text_path {
            dialect::postgres_path(key)
        } else {
            dialect::literal(key).map(|s| vec![s])
        };
        if let Some(path) = path {
            inputs.path(path, self.max_bytes)
        } else {
            let mut result =
                inputs.path(vec![PathSegment::Unknown { unknown: true }], self.max_bytes);
            result.union(self.expr(key, scope, ctes, outer), self.max_bytes);
            result
        }
    }

    fn star_inputs(
        &mut self,
        qualifier: Option<&ObjectName>,
        options: &WildcardAdditionalOptions,
        scope: &Scope,
        ctes: &Ctes,
        outer: &[&Scope],
    ) -> Inputs {
        let qualifier = qualifier.and_then(|n| dialect::name(self.dialect, n));
        let relation = self.star(qualifier.as_deref(), options, scope, ctes, outer);
        let mut result = Inputs::default();
        for column in relation.columns {
            result.union(column.inputs, self.max_bytes);
        }
        if relation.unknown {
            result.union(Inputs::unknown("wildcard"), self.max_bytes);
        }
        result
    }

    fn arg(&mut self, arg: &FunctionArg, scope: &Scope, ctes: &Ctes, outer: &[&Scope]) -> Inputs {
        let arg = match arg {
            FunctionArg::Unnamed(a)
            | FunctionArg::Named { arg: a, .. }
            | FunctionArg::ExprNamed { arg: a, .. } => a,
        };
        match arg {
            FunctionArgExpr::Expr(expr) => self.expr(expr, scope, ctes, outer),
            FunctionArgExpr::Wildcard => self.star_inputs(
                None,
                &WildcardAdditionalOptions::default(),
                scope,
                ctes,
                outer,
            ),
            FunctionArgExpr::QualifiedWildcard(name) => self.star_inputs(
                Some(name),
                &WildcardAdditionalOptions::default(),
                scope,
                ctes,
                outer,
            ),
            FunctionArgExpr::WildcardWithOptions(options) => {
                self.star_inputs(None, options, scope, ctes, outer)
            }
        }
    }

    fn function(
        &mut self,
        function: &Function,
        scope: &Scope,
        ctes: &Ctes,
        outer: &[&Scope],
    ) -> Inputs {
        let name = dialect::function(self.dialect, &function.name);
        if let FunctionArguments::List(list) = &function.args {
            if name.as_deref() == Some("count")
                && list.duplicate_treatment.is_none()
                && matches!(
                    list.args.as_slice(),
                    [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
                )
            {
                return Inputs::default();
            }
            if self.dialect == SqlDialect::Snowflake
                && matches!(name.as_deref(), Some("get" | "get_path"))
            {
                let positional: Vec<_> = list
                    .args
                    .iter()
                    .filter_map(|a| match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                        _ => None,
                    })
                    .collect();
                if let [value, key] = positional.as_slice() {
                    let inputs = self.expr(value, scope, ctes, outer);
                    if name.as_deref() == Some("get_path") {
                        if let Some(path) = dialect::string(key).and_then(dialect::snowflake_path) {
                            return inputs.path(path, self.max_bytes);
                        }
                        let mut result = inputs
                            .path(vec![PathSegment::Unknown { unknown: true }], self.max_bytes);
                        result.union(self.expr(key, scope, ctes, outer), self.max_bytes);
                        return result;
                    }
                    return self.extract(inputs, key, false, scope, ctes, outer);
                }
            }
        }
        let mut result = Inputs::default();
        for arguments in [&function.parameters, &function.args] {
            match arguments {
                FunctionArguments::List(list) => {
                    for (index, arg) in list.args.iter().enumerate() {
                        if index == 0 && dialect::date_part_argument(self.dialect, name.as_deref())
                        {
                            continue;
                        }
                        result.union(self.arg(arg, scope, ctes, outer), self.max_bytes);
                    }
                }
                FunctionArguments::Subquery(query) => {
                    let mut parents = outer.to_vec();
                    parents.push(scope);
                    let relation = self.query(query, ctes, &parents);
                    for column in relation.columns {
                        result.union(column.inputs, self.max_bytes);
                    }
                    if relation.unknown {
                        result.union(Inputs::unknown("function subquery"), self.max_bytes);
                    }
                }
                FunctionArguments::None => {}
            }
        }
        if dialect::ordered_set(name.as_deref()) {
            for order in &function.within_group {
                result.union(self.expr(&order.expr, scope, ctes, outer), self.max_bytes);
            }
        } else if !function.within_group.is_empty() && result.sources.is_empty() {
            result.union(Inputs::unknown("ordered-set input"), self.max_bytes);
        }
        // FILTER, ordinary argument ordering and OVER are row/order controls.
        result
    }

    pub(super) fn generator(
        &mut self,
        name: &ObjectName,
        args: &[FunctionArg],
        scope: &Scope,
        ctes: &Ctes,
        outer: &[&Scope],
    ) -> Relation {
        let name = dialect::function(self.dialect, name);
        let mut inputs = Inputs::default();
        let mut direct = false;
        for (i, arg) in args.iter().enumerate() {
            let value = self.arg(arg, scope, ctes, outer);
            if i == 0
                || matches!(arg, FunctionArg::Named { name, .. } if name.value.eq_ignore_ascii_case("input"))
            {
                direct = value.direct;
            }
            inputs.union(value, self.max_bytes);
        }
        let known = matches!(
            (self.dialect, name.as_deref()),
            (
                SqlDialect::Postgresql,
                Some(
                    "jsonb_array_elements"
                        | "json_array_elements"
                        | "jsonb_array_elements_text"
                        | "json_array_elements_text",
                ),
            ) | (SqlDialect::Snowflake, Some("flatten"))
        );
        if !known {
            inputs.union(Inputs::unknown("table function"), self.max_bytes);
            return Relation {
                columns: vec![Column {
                    name: None,
                    inputs,
                    nested: false,
                }],
                unknown: true,
            };
        }
        if self.dialect == SqlDialect::Snowflake {
            // Only INPUT and a literal PATH establish VALUE's origin. Recursive
            // traversal and unhandled options must remain visibly uncertain.
            for arg in args {
                if let FunctionArg::Named {
                    name,
                    arg: FunctionArgExpr::Expr(expr),
                    ..
                } = arg
                {
                    if name.value.eq_ignore_ascii_case("path") {
                        if let Some(path) = dialect::string(expr).and_then(dialect::snowflake_path)
                        {
                            if direct {
                                for source in &mut inputs.sources {
                                    if let LineageSource::Column { path: p, .. } = source {
                                        p.extend(path.clone());
                                    }
                                }
                            }
                        } else {
                            inputs.union(Inputs::unknown("flatten path"), self.max_bytes);
                            direct = false;
                        }
                    } else if !name.value.eq_ignore_ascii_case("input")
                        && !matches!(expr, Expr::Value(_))
                    {
                        inputs.union(Inputs::unknown("flatten option"), self.max_bytes);
                        direct = false;
                    } else if name.value.eq_ignore_ascii_case("recursive")
                        && !matches!(expr, Expr::Value(v) if v.value == Value::Boolean(false))
                    {
                        inputs.union(Inputs::unknown("recursive flatten"), self.max_bytes);
                        direct = false;
                    }
                }
            }
        }
        if direct {
            for source in &mut inputs.sources {
                if let LineageSource::Column { path, .. } = source {
                    path.push(PathSegment::Any { any: true });
                }
            }
        }
        inputs.direct = direct;
        let mut columns = vec![Column {
            name: Some(
                if self.dialect == SqlDialect::Snowflake {
                    "VALUE"
                } else {
                    "value"
                }
                .into(),
            ),
            inputs: inputs.clone(),
            nested: true,
        }];
        if self.dialect == SqlDialect::Snowflake {
            for name in ["SEQ", "KEY", "PATH", "INDEX", "THIS"] {
                let mut inputs = inputs.clone();
                inputs.union(Inputs::unknown(format!("FLATTEN.{name}")), self.max_bytes);
                columns.push(Column {
                    name: Some(name.into()),
                    inputs,
                    nested: false,
                });
            }
            // Snowflake's declared FLATTEN output order.
            columns.sort_by_key(|c| {
                ["SEQ", "KEY", "PATH", "INDEX", "VALUE", "THIS"]
                    .iter()
                    .position(|n| c.name.as_deref() == Some(n))
                    .unwrap_or(6)
            });
        }
        Relation {
            columns,
            unknown: false,
        }
    }
}

struct Children<'a, 'schema> {
    analyzer: &'a mut Analyzer<'schema>,
    scope: &'a Scope,
    ctes: &'a Ctes,
    outer: &'a [&'a Scope],
    root: bool,
    skip: usize,
    children: usize,
    result: Inputs,
}

impl Visitor for Children<'_, '_> {
    type Break = ();
    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if self.skip >= 64 || !self.analyzer.tick() {
            return ControlFlow::Break(());
        }
        if self.root {
            self.root = false;
        } else if self.skip == 0 {
            self.children += 1;
            let inputs = self.analyzer.expr(expr, self.scope, self.ctes, self.outer);
            self.result.union(inputs, self.analyzer.max_bytes);
            self.skip = 1;
        } else {
            self.skip += 1;
        }
        ControlFlow::Continue(())
    }
    fn post_visit_expr(&mut self, _: &Expr) -> ControlFlow<()> {
        self.skip = self.skip.saturating_sub(1);
        ControlFlow::Continue(())
    }
}
