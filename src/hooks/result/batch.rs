//! Editable result batches that preserve column metadata and source row order.
//!
//! A row keeps its original wire bytes and indexes into them. Edits are stored
//! separately, so reading one field does not require decoding every column.
//! Reservations hold byte-budget permits for buffers and copies until ownership
//! ends. Filtering removes rows; editing cannot add rows or change column types.

use super::ResultHookError;
use bytes::Bytes;
use std::{mem::size_of, ops::Range, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Native identifiers are descriptive; only adapter-recognized text is writable.
#[derive(Debug, Clone)]
pub struct ResultColumn {
    pub ordinal: usize,
    pub name: String,
    pub data_type: String,
    pub(crate) text: bool,
    pub(crate) max_chars: Option<usize>,
}
impl ResultColumn {
    pub fn is_text(&self) -> bool {
        self.text
    }
}

/// Owns the batch's share of the aggregate memory budget. Dropping the semaphore
/// permit returns its bytes, including when a callback fails or is cancelled.
pub(crate) struct Reservation {
    budget: Arc<Semaphore>,
    permit: Option<OwnedSemaphorePermit>,
    pub bytes: usize,
}
impl Reservation {
    pub fn new(budget: Arc<Semaphore>) -> Self {
        Self {
            budget,
            permit: None,
            bytes: 0,
        }
    }
    pub fn absorb(&mut self, mut other: Self) -> Result<(), ResultHookError> {
        if !Arc::ptr_eq(&self.budget, &other.budget) {
            return Err(ResultHookError::InvalidOutput);
        }
        if let Some(permit) = other.permit.take() {
            if let Some(held) = &mut self.permit {
                held.merge(permit);
            } else {
                self.permit = Some(permit);
            }
        }
        self.bytes += other.bytes;
        Ok(())
    }
    pub fn charge(&mut self, bytes: usize) -> Result<(), ResultHookError> {
        if bytes == 0 {
            return Ok(());
        }
        let count = u32::try_from(bytes).map_err(|_| ResultHookError::Limit)?;
        let permit = self
            .budget
            .clone()
            .try_acquire_many_owned(count)
            .map_err(|_| ResultHookError::Limit)?;
        if let Some(held) = &mut self.permit {
            held.merge(permit);
        } else {
            self.permit = Some(permit);
        }
        self.bytes += bytes;
        Ok(())
    }
}

struct Row {
    raw: crate::wire::ResultRow,
    fields: Vec<Option<Range<usize>>>,
    edits: Vec<(usize, Option<Bytes>)>,
}
impl Row {
    fn value(&self, column: usize) -> Option<&[u8]> {
        self.edits
            .iter()
            .find(|(index, _)| *index == column)
            .map_or_else(
                || {
                    self.fields[column]
                        .as_ref()
                        .map(|r| self.raw.value(r.clone()))
                },
                |(_, value)| value.as_deref(),
            )
    }
}

/// Complete rows in their original order. Mutation methods cannot change schema.
pub struct ResultBatch {
    columns: Arc<[ResultColumn]>,
    rows: Vec<Row>,
    reservation: Reservation,
    pub(crate) input_bytes: usize,
}
impl ResultBatch {
    pub(crate) fn new(columns: Arc<[ResultColumn]>, budget: Arc<Semaphore>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
            reservation: Reservation::new(budget),
            input_bytes: 0,
        }
    }
    pub fn columns(&self) -> &[ResultColumn] {
        &self.columns
    }
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    pub fn column_index(&self, name: &str) -> Result<usize, ResultHookError> {
        let mut matches = self.columns.iter().filter(|c| c.name == name);
        let column = matches.next().ok_or(ResultHookError::Column)?;
        if matches.next().is_some() {
            return Err(ResultHookError::Column);
        }
        Ok(column.ordinal)
    }
    pub fn value(&self, row: usize, column: usize) -> Result<Option<&[u8]>, ResultHookError> {
        let row = self
            .rows
            .get(row)
            .filter(|_| column < self.columns.len())
            .ok_or(ResultHookError::InvalidOutput)?;
        Ok(row.value(column))
    }
    pub fn text(&self, row: usize, column: usize) -> Result<Option<&str>, ResultHookError> {
        self.value(row, column)?
            .map(std::str::from_utf8)
            .transpose()
            .map_err(|_| ResultHookError::Type)
    }
    pub fn set_text(
        &mut self,
        row: usize,
        column: usize,
        value: &str,
    ) -> Result<(), ResultHookError> {
        let metadata = self
            .columns
            .get(column)
            .ok_or(ResultHookError::InvalidOutput)?;
        if !metadata.text
            || value.contains('\0')
            || metadata
                .max_chars
                .is_some_and(|limit| value.chars().count() > limit)
        {
            return Err(ResultHookError::Type);
        }
        self.reservation.charge(value.len())?;
        self.edit(row, column, Some(Bytes::copy_from_slice(value.as_bytes())))
    }
    pub fn set_null(&mut self, row: usize, column: usize) -> Result<(), ResultHookError> {
        self.edit(row, column, None)
    }
    fn edit(
        &mut self,
        row: usize,
        column: usize,
        value: Option<Bytes>,
    ) -> Result<(), ResultHookError> {
        let row = self
            .rows
            .get_mut(row)
            .filter(|_| column < self.columns.len())
            .ok_or(ResultHookError::InvalidOutput)?;
        if let Some((_, old)) = row.edits.iter_mut().find(|(index, _)| *index == column) {
            *old = value;
        } else {
            if row.edits.len() == row.edits.capacity() {
                self.reservation
                    .charge(size_of::<(usize, Option<Bytes>)>())?;
                row.edits.reserve_exact(1);
            }
            row.edits.push((column, value));
        }
        Ok(())
    }
    /// The mask addresses current rows. Repeated filtering keeps their order.
    pub fn retain_rows(&mut self, keep: &[bool]) -> Result<(), ResultHookError> {
        if keep.len() != self.rows.len() {
            return Err(ResultHookError::InvalidOutput);
        }
        let mut index = 0;
        self.rows.retain(|_| {
            let retain = keep[index];
            index += 1;
            retain
        });
        Ok(())
    }
    #[cfg(feature = "python")]
    pub(crate) fn charge_external(&mut self, bytes: usize) -> Result<(), ResultHookError> {
        self.reservation.charge(bytes)
    }
    #[cfg(feature = "python")]
    pub(crate) fn reserved_bytes(&self) -> usize {
        self.reservation.bytes
    }
    pub(crate) fn push(
        &mut self,
        raw: crate::wire::ResultRow,
        input: Option<Reservation>,
    ) -> Result<(), ResultHookError> {
        if raw.column_count() != self.columns.len() {
            return Err(ResultHookError::InvalidOutput);
        }
        if let Some(input) = input {
            self.reservation.absorb(input)?;
        } else {
            self.reservation.charge(raw.capacity())?;
        }
        self.reservation
            .charge(self.columns.len() * size_of::<Option<Range<usize>>>())?;
        if self.rows.len() == self.rows.capacity() {
            let grow = self.rows.capacity().clamp(1, 256);
            self.reservation.charge(grow * size_of::<Row>())?;
            self.rows.reserve_exact(grow);
        }
        self.input_bytes += raw.len();
        let fields = raw.fields()?;
        self.rows.push(Row {
            raw,
            fields,
            edits: Vec::new(),
        });
        Ok(())
    }
    /// Validate the whole chunk before its first row is sent.
    pub(crate) fn finish(
        mut self,
        max_frame: usize,
        max_payload: Option<usize>,
    ) -> Result<(Vec<crate::wire::Response>, Reservation), ResultHookError> {
        self.reservation
            .charge(self.rows.len() * size_of::<crate::wire::Response>())?;
        let mut payload = 0usize;
        for row in &self.rows {
            let size = 7
                + (0..self.columns.len())
                    .map(|i| 4 + row.value(i).map_or(0, <[u8]>::len))
                    .sum::<usize>();
            payload = payload.checked_add(size).ok_or(ResultHookError::Limit)?;
            if size > max_frame
                || size > i32::MAX as usize
                || max_payload.is_some_and(|limit| payload > limit)
            {
                return Err(ResultHookError::Limit);
            }
            // Reserve wire-buffer copies even when native row storage is reused.
            self.reservation.charge(size.saturating_mul(2))?;
            if !row.edits.is_empty() {
                self.reservation.charge(size)?;
            }
        }
        let output = self
            .rows
            .into_iter()
            .map(|row| {
                if row.edits.is_empty() {
                    row.raw.into_response()
                } else {
                    crate::wire::result_row(
                        (0..self.columns.len()).map(|i| row.value(i)),
                        self.columns.len(),
                    )
                }
            })
            .collect();
        Ok((output, self.reservation))
    }
}
