//! Native catalog names, helpers, and casts accepted by metadata recognition.
//! These allowlists are shared with the PostgreSQL metadata rewriter.

use sqlparser::ast::{DataType, ObjectName};

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
            | "json_build_object"
            | "coalesce"
            | "nullif"
            | "greatest"
            | "least"
    )
}

pub(super) fn safe_type(data_type: &DataType) -> bool {
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
