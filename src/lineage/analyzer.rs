mod expressions;

use super::{AstDataContext, ColumnLineage, LineageSource, PathSegment, dialect};
use crate::{
    catalog::Snapshot,
    query::{ParsedQuery, SqlDialect},
};
use sqlparser::ast::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::ControlFlow,
};

#[derive(Clone, Default)]
struct Inputs {
    sources: Vec<LineageSource>,
    // Only direct references/extractions can extend a physical JSON path.
    direct: bool,
}

impl Inputs {
    fn weight(&self) -> usize {
        self.sources.iter().map(source_weight).sum()
    }
    fn unknown(reference: impl Into<String>) -> Self {
        Self {
            sources: vec![LineageSource::Unknown {
                reference: reference.into(),
            }],
            direct: false,
        }
    }
    fn union(&mut self, other: Self, max_bytes: usize) {
        let mut weight = self.weight();
        for source in other.sources {
            if !self.sources.contains(&source) {
                weight = weight.saturating_add(source_weight(&source));
                if weight > max_bytes {
                    let unknown = LineageSource::Unknown {
                        reference: "analysis limit".into(),
                    };
                    if !self.sources.contains(&unknown) {
                        self.sources.push(unknown);
                    }
                    break;
                }
                self.sources.push(source);
            }
        }
        self.direct = false;
    }
    fn path(mut self, path: Vec<PathSegment>, max_bytes: usize) -> Self {
        if self.direct {
            let added = path.iter().map(path_weight).sum::<usize>();
            if self
                .weight()
                .saturating_add(added.saturating_mul(self.sources.len()))
                > max_bytes
            {
                self.union(Self::unknown("analysis limit"), max_bytes);
                return self;
            }
            for source in &mut self.sources {
                if let LineageSource::Column { path: existing, .. } = source {
                    existing.extend(path.clone());
                }
            }
        }
        self
    }
}

fn path_weight(path: &PathSegment) -> usize {
    32 + match path {
        PathSegment::Key(key) | PathSegment::Element { element: key } => key.len(),
        _ => 0,
    }
}

fn column_weight(column: &crate::catalog::ColumnId) -> usize {
    let relation = &column.relation;
    128 + relation.database.len() + relation.schema.len() + relation.name.len() + column.name.len()
}

fn source_weight(source: &LineageSource) -> usize {
    match source {
        LineageSource::Column { column, path } => {
            column_weight(column) + path.iter().map(path_weight).sum::<usize>()
        }
        LineageSource::Unknown { reference } => 64 + reference.len(),
    }
}

#[derive(Clone)]
struct Column {
    name: Option<String>,
    inputs: Inputs,
    nested: bool,
}

impl Column {
    fn weight(&self) -> usize {
        64 + self.name.as_ref().map_or(0, String::len) + self.inputs.weight()
    }
}

#[derive(Clone, Default)]
struct Relation {
    columns: Vec<Column>,
    unknown: bool,
}

impl Relation {
    fn weight(&self) -> usize {
        self.columns.iter().map(Column::weight).sum()
    }
}

#[derive(Clone)]
struct Binding {
    names: Vec<Vec<String>>,
    relation: Relation,
}

#[derive(Clone, Default)]
struct Scope {
    bindings: Vec<Binding>,
    visible: Relation,
    // Redshift aliases are lookup candidates, never extra columns in SELECT *.
    aliases: Vec<Column>,
}

impl Scope {
    fn bound(&mut self, max_bytes: usize) {
        let weight = self.visible.weight()
            + self.aliases.iter().map(Column::weight).sum::<usize>()
            + self
                .bindings
                .iter()
                .map(|b| b.relation.weight())
                .sum::<usize>();
        if weight > max_bytes {
            self.bindings.clear();
            self.aliases.clear();
            self.visible = Relation {
                unknown: true,
                ..Relation::default()
            };
        }
    }
}

type Ctes = BTreeMap<String, Relation>;

pub(super) fn analyze(
    query: &ParsedQuery,
    snapshot: Option<&Snapshot>,
    max_bytes: usize,
) -> AstDataContext {
    let mut analyzer = Analyzer {
        dialect: query.input_options().dialect,
        snapshot,
        remaining: (max_bytes / 8).clamp(64, 65_536),
        max_columns: (max_bytes / 64).clamp(1, 4096),
        max_bytes,
        depth: 0,
    };
    let relation = match query.statement() {
        Statement::Query(query) => analyzer.query(query, &Ctes::new(), &[]),
        _ => Relation {
            unknown: true,
            ..Relation::default()
        },
    };
    AstDataContext {
        columns: relation
            .columns
            .into_iter()
            .enumerate()
            .map(|(ordinal, c)| ColumnLineage {
                ordinal,
                name: c.name,
                sources: c.inputs.sources,
            })
            .collect(),
        unknown_outputs: relation.unknown,
    }
}

struct Analyzer<'a> {
    dialect: SqlDialect,
    snapshot: Option<&'a Snapshot>,
    remaining: usize,
    max_columns: usize,
    max_bytes: usize,
    depth: usize,
}

impl Analyzer<'_> {
    fn tick(&mut self) -> bool {
        if self.remaining == 0 {
            return false;
        }
        self.remaining -= 1;
        true
    }
    fn ident(&self, ident: &Ident) -> String {
        dialect::ident(self.dialect, ident)
    }

    fn query(&mut self, query: &Query, inherited: &Ctes, outer: &[&Scope]) -> Relation {
        if self.depth >= 64 || !self.tick() {
            return Relation {
                unknown: true,
                ..Relation::default()
            };
        }
        self.depth += 1;
        let mut ctes = inherited.clone();
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                let name = self.ident(&cte.alias.name);
                if with.recursive {
                    ctes.insert(
                        name,
                        Relation {
                            unknown: true,
                            ..Relation::default()
                        },
                    );
                } else {
                    let mut relation = self.query(&cte.query, &ctes, outer);
                    self.alias_columns(&mut relation, &cte.alias);
                    ctes.insert(name, relation);
                }
                if ctes.values().map(Relation::weight).sum::<usize>() > self.max_bytes {
                    self.depth -= 1;
                    return Relation {
                        unknown: true,
                        ..Relation::default()
                    };
                }
            }
        }
        let result = self.set(&query.body, &ctes, outer);
        self.depth -= 1;
        result
    }

    fn set(&mut self, set: &SetExpr, ctes: &Ctes, outer: &[&Scope]) -> Relation {
        if self.depth >= 64 {
            return Relation {
                unknown: true,
                ..Relation::default()
            };
        }
        self.depth += 1;
        let result = self.set_body(set, ctes, outer);
        self.depth -= 1;
        result
    }

    fn set_body(&mut self, set: &SetExpr, ctes: &Ctes, outer: &[&Scope]) -> Relation {
        if !self.tick() {
            return Relation {
                unknown: true,
                ..Relation::default()
            };
        }
        match set {
            SetExpr::Select(select) => self.select(select, ctes, outer),
            SetExpr::Query(query) => self.query(query, ctes, outer),
            SetExpr::SetOperation {
                left,
                op,
                right,
                set_quantifier,
                ..
            } => {
                let mut left = self.set(left, ctes, outer);
                let right = self.set(right, ctes, outer);
                let supported = *op == SetOperator::Union
                    && matches!(
                        set_quantifier,
                        SetQuantifier::All | SetQuantifier::Distinct | SetQuantifier::None
                    );
                if left.unknown || right.unknown || left.columns.len() != right.columns.len() {
                    left.unknown = true;
                }
                for (i, column) in left.columns.iter_mut().enumerate() {
                    if let Some(other) = right.columns.get(i) {
                        column.inputs.union(other.inputs.clone(), self.max_bytes);
                    }
                    if !supported {
                        column
                            .inputs
                            .union(Inputs::unknown("set operation"), self.max_bytes);
                    }
                }
                left
            }
            SetExpr::Values(values) => {
                let scope = Scope::default();
                let mut result = Relation::default();
                for row in &values.rows {
                    for (i, expr) in row.iter().take(self.max_columns).enumerate() {
                        let inputs = self.expr(expr, &scope, ctes, outer);
                        if let Some(c) = result.columns.get_mut(i) {
                            c.inputs.union(inputs, self.max_bytes);
                        } else {
                            result.columns.push(Column {
                                name: None,
                                inputs,
                                nested: false,
                            });
                        }
                    }
                    result.unknown |= row.len() > self.max_columns;
                }
                result
            }
            _ => Relation {
                unknown: true,
                ..Relation::default()
            },
        }
    }

    fn select(&mut self, select: &Select, ctes: &Ctes, outer: &[&Scope]) -> Relation {
        let mut scope = Scope::default();
        for table in &select.from {
            self.joined(table, &mut scope, ctes, outer);
        }
        let mut output = Relation::default();
        let mut output_weight = 0;
        for item in &select.projection {
            if !self.tick() || output.columns.len() >= self.max_columns {
                output.unknown = true;
                break;
            }
            let previous_len = output.columns.len();
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    let name = match item {
                        SelectItem::ExprWithAlias { alias, .. } => Some(self.ident(alias)),
                        _ => match expr {
                            Expr::Identifier(i) => Some(self.ident(i)),
                            Expr::CompoundIdentifier(parts) => parts.last().map(|i| self.ident(i)),
                            _ => None,
                        },
                    };
                    let inputs = self.expr(expr, &scope, ctes, outer);
                    output.columns.push(Column {
                        name,
                        nested: inputs.direct,
                        inputs,
                    });
                }
                SelectItem::Wildcard(options) => {
                    let expanded = self.star(None, options, &scope, ctes, outer);
                    if expanded.unknown {
                        output.unknown = true;
                        break;
                    }
                    output.columns.extend(expanded.columns);
                }
                SelectItem::QualifiedWildcard(kind, options) => {
                    let name = match kind {
                        SelectItemQualifiedWildcardKind::ObjectName(name) => {
                            dialect::name(self.dialect, name)
                        }
                        _ => None,
                    };
                    let expanded = self.star(name.as_deref(), options, &scope, ctes, outer);
                    if expanded.unknown || name.is_none() {
                        output.unknown = true;
                        break;
                    }
                    output.columns.extend(expanded.columns);
                }
                SelectItem::ExprWithAliases { .. } => {
                    output.unknown = true;
                    break;
                }
            }
            let added = output
                .columns
                .iter()
                .skip(previous_len)
                .map(Column::weight)
                .sum::<usize>();
            output_weight += added;
            if output_weight > self.max_bytes {
                output.columns.clear();
                output.unknown = true;
                break;
            }
            if self.dialect == SqlDialect::Redshift {
                scope
                    .aliases
                    .extend(output.columns[previous_len..].iter().cloned());
                scope.bound(self.max_bytes);
            }
            if output.columns.len() > self.max_columns {
                output.columns.truncate(self.max_columns);
                output.unknown = true;
            }
        }
        output
    }

    fn alias_columns(&self, relation: &mut Relation, alias: &TableAlias) {
        for (column, alias) in relation.columns.iter_mut().zip(&alias.columns) {
            column.name = Some(self.ident(&alias.name));
        }
        if alias.columns.len() > relation.columns.len() {
            relation.unknown = true;
        }
    }

    fn factor(
        &mut self,
        factor: &TableFactor,
        scope: &Scope,
        ctes: &Ctes,
        outer: &[&Scope],
    ) -> Binding {
        if self.depth >= 64 || !self.tick() {
            return Binding {
                names: Vec::new(),
                relation: Relation {
                    unknown: true,
                    ..Relation::default()
                },
            };
        }
        self.depth += 1;
        let result = self.factor_body(factor, scope, ctes, outer);
        self.depth -= 1;
        result
    }

    fn factor_body(
        &mut self,
        factor: &TableFactor,
        scope: &Scope,
        ctes: &Ctes,
        outer: &[&Scope],
    ) -> Binding {
        let (mut relation, alias, names) = match factor {
            TableFactor::Table {
                name,
                alias,
                args: None,
                json_path,
                ..
            } => {
                let parts = dialect::name(self.dialect, name).unwrap_or_default();
                let relation = if parts.len() == 1 && ctes.contains_key(&parts[0]) {
                    ctes[&parts[0]].clone()
                } else {
                    self.table(&parts)
                };
                let names = (0..parts.len()).map(|i| parts[i..].to_vec()).collect();
                let relation = if json_path.is_some() {
                    Relation {
                        unknown: true,
                        ..relation
                    }
                } else {
                    relation
                };
                (relation, alias.as_ref(), names)
            }
            TableFactor::Derived {
                subquery,
                alias,
                lateral,
                ..
            } => {
                let mut parents = outer.to_vec();
                if *lateral {
                    parents.push(scope);
                }
                (
                    self.query(subquery, ctes, &parents),
                    alias.as_ref(),
                    Vec::new(),
                )
            }
            TableFactor::Function {
                name,
                args,
                alias,
                with_ordinality,
                ..
            } => (
                {
                    let mut relation = self.generator(name, args, scope, ctes, outer);
                    relation.unknown |= *with_ordinality;
                    relation
                },
                alias.as_ref(),
                Vec::new(),
            ),
            TableFactor::Table {
                name,
                args: Some(args),
                alias,
                with_ordinality,
                ..
            } => (
                {
                    let mut relation = self.generator(name, &args.args, scope, ctes, outer);
                    relation.unknown |= *with_ordinality;
                    relation
                },
                alias.as_ref(),
                Vec::new(),
            ),
            TableFactor::TableFunction {
                expr: Expr::Function(function),
                alias,
            } => {
                let args = match &function.args {
                    FunctionArguments::List(list) => list.args.as_slice(),
                    _ => &[],
                };
                (
                    self.generator(&function.name, args, scope, ctes, outer),
                    alias.as_ref(),
                    Vec::new(),
                )
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                let mut nested = Scope::default();
                self.joined(table_with_joins, &mut nested, ctes, outer);
                (nested.visible, alias.as_ref(), Vec::new())
            }
            _ => (
                Relation {
                    unknown: true,
                    ..Relation::default()
                },
                None,
                Vec::new(),
            ),
        };
        let names = if let Some(alias) = alias {
            self.alias_columns(&mut relation, alias);
            vec![vec![self.ident(&alias.name)]]
        } else {
            names
        };
        Binding { names, relation }
    }

    fn table(&mut self, parts: &[String]) -> Relation {
        let Some(snapshot) = self.snapshot else {
            return Relation {
                unknown: true,
                ..Relation::default()
            };
        };
        if parts.is_empty() {
            return Relation {
                unknown: true,
                ..Relation::default()
            };
        }
        let matches: Vec<_> = snapshot
            .relations
            .values()
            .filter(|r| {
                let path = [&r.id.database, &r.id.schema, &r.id.name];
                parts.len() <= path.len()
                    && parts
                        .iter()
                        .zip(&path[path.len() - parts.len()..])
                        .all(|(a, b)| a == *b)
            })
            .take(2)
            .collect();
        if matches.len() != 1 {
            return Relation {
                unknown: true,
                ..Relation::default()
            };
        }
        let relation = matches[0];
        let mut columns = Vec::new();
        let mut weight = 0;
        for column in relation.columns.iter().take(self.max_columns) {
            weight += column_weight(&column.id) + 64 + column.id.name.len();
            if !self.tick() || weight > self.max_bytes {
                break;
            }
            columns.push(Column {
                name: Some(column.id.name.clone()),
                nested: dialect::nested_type(&column.native_type.name),
                inputs: Inputs {
                    sources: vec![LineageSource::Column {
                        column: column.id.clone(),
                        path: Vec::new(),
                    }],
                    direct: true,
                },
            });
        }
        let unknown = columns.len() != relation.columns.len();
        Relation { columns, unknown }
    }

    fn joined(&mut self, table: &TableWithJoins, scope: &mut Scope, ctes: &Ctes, outer: &[&Scope]) {
        let binding = self.factor(&table.relation, scope, ctes, outer);
        scope
            .visible
            .columns
            .extend(binding.relation.columns.clone());
        scope.visible.unknown |= binding.relation.unknown;
        scope.bindings.push(binding);
        scope.bound(self.max_bytes);
        for join in &table.joins {
            if !self.tick() {
                scope.visible.unknown = true;
                break;
            }
            let right = self.factor(&join.relation, scope, ctes, outer);
            let constraint = match &join.join_operator {
                JoinOperator::Join(c)
                | JoinOperator::Inner(c)
                | JoinOperator::Left(c)
                | JoinOperator::LeftOuter(c)
                | JoinOperator::Right(c)
                | JoinOperator::RightOuter(c)
                | JoinOperator::FullOuter(c)
                | JoinOperator::CrossJoin(c) => Some(c),
                _ => None,
            };
            let using: Vec<String> = match constraint {
                Some(JoinConstraint::Using(names)) => names
                    .iter()
                    .filter_map(|n| dialect::name(self.dialect, n).and_then(|p| p.last().cloned()))
                    .collect(),
                Some(JoinConstraint::Natural) => scope
                    .visible
                    .columns
                    .iter()
                    .filter_map(|c| c.name.as_ref())
                    .filter(|n| {
                        right
                            .relation
                            .columns
                            .iter()
                            .any(|c| c.name.as_ref() == Some(n))
                    })
                    .cloned()
                    .collect(),
                _ => Vec::new(),
            };
            let mut merged = Vec::new();
            for name in &using {
                let left: Vec<_> = scope
                    .visible
                    .columns
                    .iter()
                    .filter(|c| c.name.as_ref() == Some(name))
                    .collect();
                let rhs: Vec<_> = right
                    .relation
                    .columns
                    .iter()
                    .filter(|c| c.name.as_ref() == Some(name))
                    .collect();
                let inputs = match (left.as_slice(), rhs.as_slice()) {
                    ([left], [rhs]) => match &join.join_operator {
                        JoinOperator::Right(_) | JoinOperator::RightOuter(_) => rhs.inputs.clone(),
                        JoinOperator::FullOuter(_) => {
                            let mut v = left.inputs.clone();
                            v.union(rhs.inputs.clone(), self.max_bytes);
                            v
                        }
                        _ => left.inputs.clone(),
                    },
                    _ => Inputs::unknown(name),
                };
                merged.push(Column {
                    name: Some(name.clone()),
                    inputs,
                    nested: false,
                });
            }
            merged.extend(
                scope
                    .visible
                    .columns
                    .iter()
                    .filter(|c| !c.name.as_ref().is_some_and(|n| using.contains(n)))
                    .cloned(),
            );
            merged.extend(
                right
                    .relation
                    .columns
                    .iter()
                    .filter(|c| !c.name.as_ref().is_some_and(|n| using.contains(n)))
                    .cloned(),
            );
            scope.visible.columns = merged;
            scope.visible.unknown |= right.relation.unknown || constraint.is_none();
            scope.bindings.push(right);
            scope.bound(self.max_bytes);
        }
    }

    fn lookup(&self, parts: &[Ident], scope: &Scope, outer: &[&Scope]) -> Inputs {
        let names: Vec<_> = parts.iter().map(|i| self.ident(i)).collect();
        for current in std::iter::once(scope).chain(outer.iter().rev().copied()) {
            if names.len() == 1 {
                let matches: Vec<_> = current
                    .visible
                    .columns
                    .iter()
                    .filter(|c| c.name.as_ref() == names.first())
                    .collect();
                match matches.as_slice() {
                    [column] => {
                        let mut inputs = column.inputs.clone();
                        if current.visible.unknown {
                            inputs.union(Inputs::unknown(parts[0].to_string()), self.max_bytes);
                        }
                        return inputs;
                    }
                    [] if !current.visible.unknown => {
                        let aliases: Vec<_> = current
                            .aliases
                            .iter()
                            .filter(|c| c.name.as_ref() == names.first())
                            .collect();
                        match aliases.as_slice() {
                            [column] => return column.inputs.clone(),
                            [] => continue,
                            _ => return Inputs::unknown("ambiguous target alias"),
                        }
                    }
                    _ => {
                        return Inputs::unknown(
                            parts
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join("."),
                        );
                    }
                }
            } else {
                // Longest matching qualifier wins; remaining components may be SUPER keys.
                for split in (1..names.len()).rev() {
                    let bindings: Vec<_> = current
                        .bindings
                        .iter()
                        .filter(|b| b.names.iter().any(|n| n == &names[..split]))
                        .collect();
                    if bindings.is_empty() {
                        continue;
                    }
                    if bindings.len() != 1 {
                        return Inputs::unknown("ambiguous qualifier");
                    }
                    let binding = bindings[0];
                    let columns: Vec<_> = binding
                        .relation
                        .columns
                        .iter()
                        .filter(|c| c.name.as_ref() == Some(&names[split]))
                        .collect();
                    if let [column] = columns.as_slice() {
                        if split + 1 == names.len() {
                            return column.inputs.clone();
                        }
                        if column.nested && self.dialect != SqlDialect::Postgresql {
                            return column.inputs.clone().path(
                                parts[split + 1..]
                                    .iter()
                                    .map(|i| PathSegment::Key(i.value.clone()))
                                    .collect(),
                                self.max_bytes,
                            );
                        }
                    }
                    let mut inputs = Inputs::default();
                    for column in binding.relation.columns.iter().filter(|c| c.name.is_none()) {
                        inputs.union(column.inputs.clone(), self.max_bytes);
                    }
                    inputs.union(
                        Inputs::unknown(
                            parts
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join("."),
                        ),
                        self.max_bytes,
                    );
                    return inputs;
                }
                if self.dialect != SqlDialect::Postgresql {
                    let columns: Vec<_> = current
                        .visible
                        .columns
                        .iter()
                        .filter(|c| c.name.as_ref() == names.first() && c.nested)
                        .collect();
                    if let [column] = columns.as_slice() {
                        return column.inputs.clone().path(
                            parts[1..]
                                .iter()
                                .map(|i| PathSegment::Key(i.value.clone()))
                                .collect(),
                            self.max_bytes,
                        );
                    }
                }
                if current.visible.unknown {
                    break;
                }
            }
        }
        Inputs::unknown(
            parts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("."),
        )
    }

    fn star(
        &mut self,
        qualifier: Option<&[String]>,
        options: &WildcardAdditionalOptions,
        scope: &Scope,
        ctes: &Ctes,
        outer: &[&Scope],
    ) -> Relation {
        let mut relation = if let Some(qualifier) = qualifier {
            let bindings: Vec<_> = scope
                .bindings
                .iter()
                .filter(|b| b.names.iter().any(|n| n == qualifier))
                .collect();
            match bindings.as_slice() {
                [binding] => binding.relation.clone(),
                _ => Relation {
                    unknown: true,
                    ..Relation::default()
                },
            }
        } else {
            scope.visible.clone()
        };
        if let Some(ilike) = &options.opt_ilike {
            relation.columns.retain(|c| {
                c.name
                    .as_ref()
                    .is_some_and(|n| dialect::ilike(n, &ilike.pattern))
            });
        }
        let mut excluded = BTreeSet::new();
        if let Some(exclude) = &options.opt_exclude {
            let names = match exclude {
                ExcludeSelectItem::Single(n) => std::slice::from_ref(n),
                ExcludeSelectItem::Multiple(ns) => ns,
            };
            for name in names {
                if let Some(parts) = dialect::name(self.dialect, name)
                    && parts.len() == 1
                {
                    excluded.insert(parts[0].clone());
                } else {
                    relation.unknown = true;
                }
            }
        }
        if let Some(except) = &options.opt_except {
            excluded.insert(self.ident(&except.first_element));
            for name in &except.additional_elements {
                excluded.insert(self.ident(name));
            }
        }
        relation
            .columns
            .retain(|c| !c.name.as_ref().is_some_and(|n| excluded.contains(n)));
        if let Some(replace) = &options.opt_replace {
            for replacement in &replace.items {
                let name = self.ident(&replacement.column_name);
                let inputs = self.expr(&replacement.expr, scope, ctes, outer);
                let mut matched = false;
                for column in &mut relation.columns {
                    if column.name.as_ref() == Some(&name) {
                        column.inputs = inputs.clone();
                        column.nested = false;
                        matched = true;
                    }
                }
                relation.unknown |= !matched;
            }
        }
        if let Some(rename) = &options.opt_rename {
            let names = match rename {
                RenameSelectItem::Single(n) => std::slice::from_ref(n),
                RenameSelectItem::Multiple(ns) => ns,
            };
            for rename in names {
                let name = self.ident(&rename.ident);
                let mut matched = false;
                for column in &mut relation.columns {
                    if column.name.as_ref() == Some(&name) {
                        column.name = Some(self.ident(&rename.alias));
                        matched = true;
                    }
                }
                relation.unknown |= !matched;
            }
        }
        if options.opt_alias.is_some() {
            relation.unknown = true;
        }
        relation
    }
}
