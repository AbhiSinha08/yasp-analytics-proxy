//! Compiled application example: rename qualified tables with explicit aliases.
use futures::future::BoxFuture;
use sqlparser::ast::{ObjectName, TableFactor, VisitMut, VisitorMut};
use std::{ops::ControlFlow, sync::Arc};
use yasp::hooks::{QueryModificationError, QueryModificationRequest};

pub struct TableRename {
    pub from: ObjectName,
    pub to: ObjectName,
}

pub fn rename_table<'a>(
    request: QueryModificationRequest<'a>,
    mapping: Arc<TableRename>,
) -> BoxFuture<'a, Result<Option<String>, QueryModificationError>> {
    Box::pin(async move {
        // Qualified names avoid mistaking a CTE for a physical table. Explicit
        // aliases keep existing column references valid after the table is renamed.
        if mapping.from.0.len() < 2 {
            return Err(QueryModificationError::Failed);
        }
        struct Rename<'a> {
            mapping: &'a TableRename,
            changed: bool,
        }
        impl VisitorMut for Rename<'_> {
            type Break = QueryModificationError;
            fn post_visit_table_factor(
                &mut self,
                table: &mut TableFactor,
            ) -> ControlFlow<Self::Break> {
                if let TableFactor::Table {
                    name,
                    alias,
                    args: None,
                    ..
                } = table
                    && *name == self.mapping.from
                {
                    if alias.is_none() {
                        return ControlFlow::Break(QueryModificationError::Failed);
                    }
                    *name = self.mapping.to.clone();
                    self.changed = true;
                }
                ControlFlow::Continue(())
            }
        }
        let mut statement = request.statement.clone();
        let mut visitor = Rename {
            mapping: &mapping,
            changed: false,
        };
        if let ControlFlow::Break(error) = statement.visit(&mut visitor) {
            return Err(error);
        }
        Ok(visitor.changed.then(|| statement.to_string()))
    })
}
