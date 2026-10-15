//! PostgreSQL client discovery recognition and bounded frontend setup settings.
//! Recognition grants metadata execution only, never access to application rows.

use crate::query::ParsedQuery;
use sqlparser::ast::{
    BinaryOperator, ContextModifier, DataType, Expr, FunctionArg, FunctionArgExpr,
    FunctionArguments, ObjectName, Query, SelectItem, Set, SetExpr, Statement, TableFactor,
    TransactionAccessMode, TransactionIsolationLevel, TransactionMode, Value, Visit, Visitor,
};
use sqlparser::{
    dialect::PostgreSqlDialect,
    tokenizer::{Token, Tokenizer},
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

pub(crate) fn name(object: &ObjectName) -> Option<(Option<String>, String)> {
    let parts = object
        .0
        .iter()
        .map(|p| {
            let i = p.as_ident()?;
            Some(if i.quote_style.is_some() {
                i.value.clone()
            } else {
                i.value.to_ascii_lowercase()
            })
        })
        .collect::<Option<Vec<_>>>()?;
    match parts.as_slice() {
        [name] => Some((None, name.clone())),
        [schema, name] => Some((Some(schema.clone()), name.clone())),
        _ => None,
    }
}

pub(crate) fn catalog_relation(object: &ObjectName) -> Option<&'static str> {
    let (schema, table) = name(object)?;
    if schema.as_deref() == Some("information_schema") {
        return Some("information_schema");
    }
    if schema.is_some() && schema.as_deref() != Some("pg_catalog") {
        return None;
    }
    matches!(
        table.as_str(),
        "pg_database"
            | "pg_namespace"
            | "pg_class"
            | "pg_attribute"
            | "pg_type"
            | "pg_enum"
            | "pg_constraint"
            | "pg_index"
            | "pg_indexes"
            | "pg_am"
            | "pg_description"
            | "pg_attrdef"
            | "pg_depend"
            | "pg_proc"
            | "pg_roles"
            | "pg_user"
            | "pg_tables"
            | "pg_views"
            | "pg_matviews"
            | "pg_sequences"
            | "pg_sequence"
            | "pg_settings"
            | "pg_collation"
            | "pg_partitioned_table"
            | "pg_inherits"
            | "pg_trigger"
            | "pg_rewrite"
            | "pg_tablespace"
            | "pg_policy"
            | "pg_statistic_ext"
            | "pg_publication"
            | "pg_publication_namespace"
            | "pg_publication_rel"
            | "pg_shdescription"
            | "pg_auth_members"
            | "pg_extension"
            | "pg_operator"
            | "pg_opclass"
            | "pg_opfamily"
            | "pg_amop"
            | "pg_amproc"
            | "pg_cast"
            | "pg_range"
            | "pg_language"
            | "pg_aggregate"
    )
    .then_some("pg_catalog")
}

pub(crate) fn helper(object: &ObjectName) -> bool {
    let Some((schema, function)) = name(object) else {
        return false;
    };
    if schema.as_deref() == Some("information_schema") {
        return matches!(
            function.as_str(),
            "_pg_expandarray"
                | "_pg_char_max_length"
                | "_pg_numeric_precision"
                | "_pg_numeric_scale"
                | "_pg_datetime_precision"
        );
    }
    if schema.is_some() && schema.as_deref() != Some("pg_catalog") {
        return false;
    }
    matches!(
        function.as_str(),
        "version"
            | "current_database"
            | "current_schema"
            | "current_schemas"
            | "current_setting"
            | "format_type"
            | "obj_description"
            | "col_description"
            | "shobj_description"
            | "pg_get_expr"
            | "pg_get_indexdef"
            | "pg_get_constraintdef"
            | "pg_get_viewdef"
            | "pg_get_functiondef"
            | "pg_get_function_arguments"
            | "pg_get_function_identity_arguments"
            | "pg_get_function_result"
            | "pg_get_triggerdef"
            | "pg_get_keywords"
            | "pg_get_partkeydef"
            | "pg_get_partition_constraintdef"
            | "pg_get_ruledef"
            | "pg_get_serial_sequence"
            | "pg_get_userbyid"
            | "pg_encoding_to_char"
            | "pg_get_statisticsobjdef_columns"
            | "pg_partition_ancestors"
            | "pg_relation_is_publishable"
            | "pg_table_is_visible"
            | "pg_type_is_visible"
            | "pg_function_is_visible"
            | "has_schema_privilege"
            | "has_table_privilege"
            | "has_any_column_privilege"
            | "has_column_privilege"
            | "has_database_privilege"
            | "has_function_privilege"
            | "has_sequence_privilege"
            | "pg_has_role"
            | "pg_relation_size"
            | "pg_total_relation_size"
            | "pg_indexes_size"
            | "pg_table_size"
            | "pg_size_pretty"
            | "pg_stat_get_numscans"
            | "pg_tablespace_location"
            | "quote_ident"
            | "quote_literal"
            | "quote_nullable"
            | "lower"
            | "upper"
            | "length"
            | "char_length"
            | "substring"
            | "replace"
            | "concat"
            | "concat_ws"
            | "array_to_string"
            | "array_length"
            | "array_upper"
            | "array_lower"
            | "array_position"
            | "unnest"
            | "generate_series"
            | "generate_subscripts"
            | "count"
            | "min"
            | "max"
            | "sum"
            | "string_agg"
            | "array_agg"
            | "coalesce"
            | "nullif"
            | "greatest"
            | "least"
    )
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

fn safe_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Custom(object, modifiers) => {
            modifiers.is_empty()
                && name(object).is_some_and(|(schema, ty)| {
                    schema.is_none_or(|s| s == "pg_catalog")
                        && matches!(
                            ty.as_str(),
                            "regclass"
                                | "regtype"
                                | "regnamespace"
                                | "regproc"
                                | "regprocedure"
                                | "oid"
                                | "name"
                                | "int2"
                                | "int4"
                                | "int8"
                                | "text"
                                | "bool"
                                | "varchar"
                                | "bpchar"
                                | "float4"
                                | "float8"
                                | "numeric"
                                | "date"
                                | "timestamp"
                                | "timestamptz"
                                | "interval"
                                | "json"
                                | "jsonb"
                        )
                })
        }
        DataType::Array(array) => match array {
            sqlparser::ast::ArrayElemTypeDef::SquareBracket(ty, _)
            | sqlparser::ast::ArrayElemTypeDef::AngleBracket(ty)
            | sqlparser::ast::ArrayElemTypeDef::Parenthesis(ty) => safe_type(ty),
            _ => false,
        },
        _ => true,
    }
}

/// These values keep pooled text responses stable across clients and cache hits.
pub(crate) fn fixed_setting(name: &str) -> Option<&'static str> {
    match name.to_ascii_lowercase().as_str() {
        "client_encoding" => Some("UTF8"),
        "standard_conforming_strings" => Some("on"),
        "datestyle" => Some("ISO, MDY"),
        "timezone" => Some("UTC"),
        "intervalstyle" => Some("postgres"),
        "bytea_output" => Some("hex"),
        "extra_float_digits" => Some("3"),
        "default_transaction_read_only" | "transaction_read_only" => Some("on"),
        "default_transaction_isolation" | "transaction_isolation" => Some("read committed"),
        _ => None,
    }
}

pub(crate) struct SessionSettings {
    pub search_path: String,
    pub application_name: String,
}

impl Default for SessionSettings {
    fn default() -> Self {
        Self {
            search_path: "\"$user\", public".into(),
            application_name: String::new(),
        }
    }
}

pub(crate) enum SetupResponse {
    Set {
        parameter: Option<(&'static str, String)>,
    },
    Show {
        name: String,
        value: String,
    },
}

impl SessionSettings {
    pub fn set_search_path(&mut self, value: &str) -> Result<(), &'static str> {
        if value.len() > 1024 || value.contains('\0') || !valid_search_path(value) {
            return Err("unsupported or oversized metadata search path");
        }
        self.search_path = value.into();
        Ok(())
    }
    /// Only bounded literal assignments; rejected commands cannot alter pool state.
    pub fn handle(&mut self, statement: &Statement) -> Result<Option<SetupResponse>, &'static str> {
        let mut assignment = None;
        match statement {
            Statement::Set(Set::SingleAssignment {
                scope,
                hivevar: false,
                variable,
                values,
            }) if !matches!(scope, Some(ContextModifier::Local)) => {
                let Some((None, setting)) = name(variable) else {
                    return Err("unsupported session setting");
                };
                let values = values
                    .iter()
                    .map(|value| {
                        if setting == "search_path" {
                            schema_name(value)
                        } else {
                            literal(value)
                        }
                    })
                    .collect::<Option<Vec<_>>>()
                    .ok_or("session settings require literal values")?;
                assignment = Some((setting, values.join(", ")));
            }
            Statement::Set(Set::SetTimeZone {
                local: false,
                value,
            }) => {
                assignment = Some((
                    "timezone".into(),
                    literal(value).ok_or("session settings require literal values")?,
                ));
            }
            Statement::Set(Set::SetNames {
                charset_name,
                collation_name: None,
            }) => {
                assignment = Some(("client_encoding".into(), charset_name.value.clone()));
            }
            Statement::Set(Set::SetTransaction {
                session: true,
                snapshot: None,
                modes,
            }) if !modes.is_empty()
                && modes.iter().all(|mode| {
                    matches!(
                        mode,
                        TransactionMode::AccessMode(TransactionAccessMode::ReadOnly)
                            | TransactionMode::IsolationLevel(
                                TransactionIsolationLevel::ReadCommitted
                            )
                    )
                }) =>
            {
                return Ok(Some(SetupResponse::Set { parameter: None }));
            }
            Statement::Set(_) => return Err("unsupported session setting"),
            Statement::ShowVariable { variable } => {
                let setting = variable
                    .iter()
                    .map(|i| i.value.to_ascii_lowercase())
                    .collect::<Vec<_>>()
                    .join(" ");
                let value = match setting.as_str() {
                    "search_path" => Some(self.search_path.as_str()),
                    "application_name" => Some(self.application_name.as_str()),
                    _ => fixed_setting(&setting),
                };
                if let Some(value) = value {
                    let column = match setting.as_str() {
                        "datestyle" => "DateStyle",
                        "timezone" => "TimeZone",
                        "intervalstyle" => "IntervalStyle",
                        _ => &setting,
                    };
                    return Ok(Some(SetupResponse::Show {
                        name: column.into(),
                        value: value.into(),
                    }));
                }
            }
            _ => {}
        }
        if let Some((setting, value)) = assignment {
            if value.len() > 1024 || value.contains('\0') {
                return Err("session setting exceeds its bound");
            }
            let parameter = match setting.as_str() {
                "application_name" if value.len() <= 63 => {
                    self.application_name = value.clone();
                    Some(("application_name", value))
                }
                "search_path" => {
                    self.set_search_path(&value)?;
                    None
                }
                _ => {
                    let Some(fixed) = fixed_setting(&setting) else {
                        return Err("unsupported session setting");
                    };
                    let equivalent = value.eq_ignore_ascii_case(fixed)
                        || (fixed == "on"
                            && matches!(value.to_ascii_lowercase().as_str(), "true" | "1"))
                        || (setting == "client_encoding" && value.eq_ignore_ascii_case("UTF-8"));
                    if !equivalent {
                        return Err("changing protected session settings is unsupported");
                    }
                    None
                }
            };
            return Ok(Some(SetupResponse::Set { parameter }));
        }
        Ok(None)
    }
}

fn valid_search_path(value: &str) -> bool {
    let Ok(tokens) = Tokenizer::new(&PostgreSqlDialect {}, value).tokenize() else {
        return false;
    };
    let tokens = tokens
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_) | Token::EOF))
        .collect::<Vec<_>>();
    if tokens.is_empty() || tokens.len() > 31 || tokens.len() % 2 == 0 {
        return false;
    }
    tokens.iter().enumerate().all(|(position, token)| {
        if position % 2 == 1 {
            return matches!(token, Token::Comma);
        }
        match token {
            Token::Word(word) if word.quote_style.is_none() || word.quote_style == Some('"') => {
                let catalog = if word.quote_style.is_some() {
                    word.value == "pg_catalog"
                } else {
                    word.value.eq_ignore_ascii_case("pg_catalog")
                };
                // Otherwise an application operator can override a native one.
                !catalog || position == 0
            }
            Token::Placeholder(word) => word == "$user",
            _ => false,
        }
    })
}

fn schema_name(expression: &Expr) -> Option<String> {
    let value = match expression {
        Expr::Identifier(identifier) if identifier.quote_style.is_none() => {
            identifier.value.to_ascii_lowercase()
        }
        Expr::Identifier(identifier) => identifier.value.clone(),
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(value) => value.clone(),
            _ => return None,
        },
        _ => return None,
    };
    if value.is_empty() {
        return None;
    }
    let simple = value
        .bytes()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == b'_')
        && value
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_' || c == b'$');
    Some(if simple {
        value
    } else {
        format!("\"{}\"", value.replace('"', "\"\""))
    })
}

fn literal(expression: &Expr) -> Option<String> {
    match expression {
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(s) | Value::Number(s, _) => Some(s.clone()),
            Value::Boolean(value) => Some(value.to_string()),
            _ => None,
        },
        Expr::Identifier(identifier) => Some(if identifier.quote_style.is_some() {
            format!("\"{}\"", identifier.value.replace('"', "\"\""))
        } else {
            identifier.value.clone()
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn psql_discovery_queries_use_the_catalog_route() {
        let queries: Vec<String> =
            serde_json::from_str(include_str!("../tests/fixtures/psql_discovery.json")).unwrap();
        let missed: Vec<_> = queries
            .iter()
            .enumerate()
            .filter(|(_, sql)| {
                classify(&ParsedQuery::parse(sql).unwrap()) != Some(DiscoveryKind::Catalog)
            })
            .map(|(i, _)| i + 1)
            .collect();
        assert!(
            missed.is_empty(),
            "psql discovery queries missed: {missed:?}"
        );
    }

    #[test]
    fn dbeaver_discovery_queries_bypass_ordinary_routing() {
        let queries: Vec<String> =
            serde_json::from_str(include_str!("../tests/fixtures/dbeaver_discovery.json")).unwrap();
        let missed: Vec<_> = queries
            .iter()
            .enumerate()
            .filter(|(_, sql)| classify(&ParsedQuery::parse(sql).unwrap()).is_none())
            .map(|(i, _)| i + 1)
            .collect();
        assert!(
            missed.is_empty(),
            "DBeaver discovery queries missed: {missed:?}"
        );
    }

    #[test]
    fn discovery_classification_does_not_grant_application_access() {
        for sql in [
            "SELECT datname FROM pg_database WHERE datistemplate=false ORDER BY datname",
            "SELECT nspname FROM pg_catalog.pg_namespace",
            "SELECT c.column_name, a.atttypid::regtype FROM information_schema.columns c JOIN pg_attribute a ON a.attname=c.column_name",
            "SELECT pg_catalog.format_type(a.atttypid,a.atttypmod), pg_get_expr(d.adbin,d.adrelid) FROM pg_attribute a LEFT JOIN pg_attrdef d ON a.attrelid=d.adrelid",
            "SELECT c.relname FROM pg_class c WHERE EXISTS(SELECT 1 FROM pg_namespace n WHERE n.oid=c.relnamespace)",
            "SELECT * FROM pg_constraint c LEFT JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(n,pos) ON true",
            "SHOW server_version",
            "SELECT relname FROM pg_class WHERE has_table_privilege(current_user,oid,'SELECT')",
            "SELECT rolname FROM pg_roles WHERE rolname=session_user",
            "SELECT version()",
            "SELECT current_setting('server_version_num')",
            "SELECT * FROM information_schema.sql_features",
            "SELECT * FROM information_schema.view_table_usage v JOIN information_schema.constraint_table_usage c ON c.table_name=v.table_name",
            "SELECT * FROM information_schema.columns c WHERE EXISTS (SELECT 1 FROM information_schema.column_domain_usage d WHERE d.column_name=c.column_name)",
            "SELECT current_schema() AS schema, session_user AS login",
            "SELECT current_database(),current_schema(),current_user,session_user",
            "SELECT version(),session_user",
            "SELECT word FROM pg_get_keywords() WHERE catcode='R' ORDER BY word",
            "SELECT array_agg(k.word) FROM (SELECT word FROM pg_catalog.pg_get_keywords()) k",
            "SELECT pg_get_partkeydef(c.oid),pg_get_partition_constraintdef(c.oid) FROM pg_class c",
            "SELECT pg_get_ruledef(r.oid) FROM pg_rewrite r",
            "SELECT pg_get_serial_sequence('public.users','id')",
            "SELECT c.opcname,f.opfname FROM pg_opclass c JOIN pg_opfamily f ON f.oid=c.opcfamily",
            "SELECT o.oprname,a.amopstrategy FROM pg_operator o JOIN pg_amop a ON a.amopopr=o.oid",
            "SELECT p.amproc FROM pg_amproc p",
            "SELECT c.castsource,r.rngtypid FROM pg_cast c JOIN pg_range r ON r.rngsubtype=c.castsource",
            "SELECT p.proname,l.lanname FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang JOIN pg_aggregate a ON a.aggfnoid=p.oid",
        ] {
            assert!(
                classify(&ParsedQuery::parse(sql).unwrap()).is_some(),
                "{sql}"
            );
        }
        for sql in [
            "SELECT * FROM public.users LIMIT 10000",
            "SELECT * FROM users LIMIT 0",
            "SELECT c.relname,u.email FROM pg_class c CROSS JOIN public.users u",
            "SELECT * FROM pg_class WHERE EXISTS(SELECT 1 FROM public.users)",
            "SELECT public.secret() FROM pg_class",
            "SELECT \"current_user\"() FROM pg_class",
            "SELECT pg_sleep(1) FROM pg_class",
            "SELECT set_config('application_name','x',false) FROM pg_class",
            "SELECT 'x'::public.evil FROM pg_class",
            "SELECT 'x'::evil[] FROM pg_class",
            "SELECT * FROM public.pg_class",
            "SELECT * FROM \"PG_CLASS\"",
            "SELECT 'pg_catalog.pg_class'",
            "WITH pg_class AS (SELECT * FROM users) SELECT * FROM pg_class",
            "SELECT current_setting('role')",
            "SELECT pg_terminate_backend(1) FROM pg_class",
            "SELECT current_database() || ':' || current_user",
            "SELECT * FROM pg_class UNION SELECT * FROM users",
            "SELECT relname FROM pg_class WHERE relname OPERATOR(public.~) 'users'",
            "SELECT relname OPERATOR(pg_catalog.||) 'users' FROM pg_class",
            "SELECT relname FROM pg_class WHERE relname OPERATOR(~) 'users'",
            "SELECT pg_get_userbyid(c.relowner), u.email FROM pg_class c JOIN public.users u ON true",
            "SELECT pg_table_size(c.oid) FROM pg_class c WHERE EXISTS (SELECT 1 FROM public.users)",
            "SELECT * FROM pg_policy WHERE EXISTS (SELECT 1 FROM public.users)",
            "SELECT * FROM pg_statistic_ext_data",
            "SELECT * FROM pg_statistic",
            "SELECT * FROM pg_stats",
            "SELECT * FROM pg_publication p JOIN public.users u ON true",
            "SELECT * FROM pg_trigger t, public.pg_partition_ancestors(t.tgrelid) a",
            "SELECT * FROM pg_trigger t, pg_catalog.pg_partition_ancestors(public.secret()) a",
            "SELECT ARRAY(SELECT email FROM public.users) FROM pg_policy",
            "SELECT ARRAY(SELECT public.secret() FROM pg_roles) FROM pg_policy",
            "SELECT public.\"array\"(rolname) FROM pg_roles",
            "SELECT * FROM information_schema.sql_features f JOIN public.users u ON true",
            "SELECT * FROM information_schema.view_table_usage WHERE EXISTS (SELECT 1 FROM public.users)",
            "SELECT public.secret() FROM information_schema.sql_features",
            "SELECT current_user",
            "SELECT session_user",
            "SELECT current_schema(), (SELECT email FROM public.users LIMIT 1), session_user",
            "SELECT word,email FROM pg_catalog.pg_get_keywords() k CROSS JOIN public.users",
            "SELECT pg_get_keywords(), ARRAY(SELECT email FROM public.users)",
            "SELECT public.pg_get_keywords()",
            "SELECT * FROM public.pg_extension",
            "SELECT pg_get_partkeydef(public.secret()) FROM pg_class",
            "SELECT pg_stat_get_activity(NULL) FROM pg_extension",
            "SELECT pg_read_file('/etc/passwd') FROM pg_extension",
            "SELECT pg_read_binary_file('/etc/passwd') FROM pg_opclass",
            "SELECT lo_get(1234) FROM pg_extension",
            "SELECT currval(pg_get_serial_sequence('public.users','id'))",
            "SELECT query FROM pg_stat_activity",
            "SELECT * FROM pg_largeobject",
            "SELECT * FROM pg_authid",
        ] {
            let query = ParsedQuery::parse(sql)
                .unwrap_or_else(|error| panic!("cannot parse regression SQL {sql}: {error}"));
            assert!(classify(&query).is_none(), "{sql}");
        }
    }
}
