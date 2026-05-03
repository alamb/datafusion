// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::marker::PhantomData;
use std::mem::size_of;

use datafusion_common::{Result, exec_datafusion_err, internal_err};

use arrow::array::{
    Array, ArrayAccessor, ArrayBuilder, ArrayDataBuilder, BinaryArray, ByteView,
    GenericStringArray, LargeStringArray, OffsetSizeTrait, StringArray, StringViewArray,
    StringViewBuilder, make_view,
};
use arrow::buffer::{Buffer, MutableBuffer, NullBuffer, ScalarBuffer};
use arrow::datatypes::DataType;

/// Builder used by `concat`/`concat_ws` to assemble a [`StringArray`] one row
/// at a time from multiple input columns.
///
/// Each row is written via repeated `write` calls (one per input fragment)
/// followed by a single `append_offset` to commit the row.  The output null
/// buffer is computed in bulk by the caller and supplied to `finish`, avoiding
/// per-row NULL handling work.
///
/// For the common "produce one `&str` per row" pattern, prefer
/// `GenericStringArrayBuilder` instead.
pub(crate) struct ConcatStringBuilder {
    offsets_buffer: MutableBuffer,
    value_buffer: MutableBuffer,
    /// If true, a safety check is required during the `finish` call
    tainted: bool,
}

impl ConcatStringBuilder {
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        let capacity = item_capacity
            .checked_add(1)
            .map(|i| i.saturating_mul(size_of::<i32>()))
            .expect("capacity integer overflow");

        let mut offsets_buffer = MutableBuffer::with_capacity(capacity);
        // SAFETY: the first offset value is definitely not going to exceed the bounds.
        unsafe { offsets_buffer.push_unchecked(0_i32) };
        Self {
            offsets_buffer,
            value_buffer: MutableBuffer::with_capacity(data_capacity),
            tainted: false,
        }
    }

    pub fn write<const CHECK_VALID: bool>(
        &mut self,
        column: &ColumnarValueRef,
        i: usize,
    ) {
        match column {
            ColumnarValueRef::Scalar(s) => {
                self.value_buffer.extend_from_slice(s);
                self.tainted = true;
            }
            ColumnarValueRef::NullableArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer
                        .extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableLargeStringArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer
                        .extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableStringViewArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer
                        .extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableBinaryArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer.extend_from_slice(array.value(i));
                }
                self.tainted = true;
            }
            ColumnarValueRef::NonNullableArray(array) => {
                self.value_buffer
                    .extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableLargeStringArray(array) => {
                self.value_buffer
                    .extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableStringViewArray(array) => {
                self.value_buffer
                    .extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableBinaryArray(array) => {
                self.value_buffer.extend_from_slice(array.value(i));
                self.tainted = true;
            }
        }
    }

    pub fn append_offset(&mut self) -> Result<()> {
        let next_offset: i32 = self
            .value_buffer
            .len()
            .try_into()
            .map_err(|_| exec_datafusion_err!("byte array offset overflow"))?;
        self.offsets_buffer.push(next_offset);
        Ok(())
    }

    /// Finalize the builder into a concrete [`StringArray`].
    ///
    /// # Errors
    ///
    /// Returns an error when:
    ///
    /// - the provided `null_buffer` is not the same length as the `offsets_buffer`.
    pub fn finish(self, null_buffer: Option<NullBuffer>) -> Result<StringArray> {
        let row_count = self.offsets_buffer.len() / size_of::<i32>() - 1;
        if let Some(ref null_buffer) = null_buffer
            && null_buffer.len() != row_count
        {
            return internal_err!(
                "Null buffer and offsets buffer must be the same length"
            );
        }
        let array_builder = ArrayDataBuilder::new(DataType::Utf8)
            .len(row_count)
            .add_buffer(self.offsets_buffer.into())
            .add_buffer(self.value_buffer.into())
            .nulls(null_buffer);
        if self.tainted {
            // Raw binary arrays with possible invalid utf-8 were used,
            // so let ArrayDataBuilder perform validation
            let array_data = array_builder.build()?;
            Ok(StringArray::from(array_data))
        } else {
            // SAFETY: all data that was appended was valid UTF8 and the values
            // and offsets were created correctly
            let array_data = unsafe { array_builder.build_unchecked() };
            Ok(StringArray::from(array_data))
        }
    }
}

/// Builder used by `concat`/`concat_ws` to assemble a [`StringViewArray`] one
/// row at a time from multiple input columns.
///
/// Each row is written via repeated `write` calls (one per input
/// fragment) followed by a single `append_offset` to commit the row
/// as a single string view. The output null buffer is supplied by the caller
/// at `finish` time, avoiding per-row NULL handling work.
///
/// For the common "produce one `&str` per row" pattern, prefer
/// [`StringViewArrayBuilder`] instead.
pub(crate) struct ConcatStringViewBuilder {
    views: Vec<u128>,
    data: Vec<u8>,
    block: Vec<u8>,
    /// If true, a safety check is required during the `append_offset` call
    tainted: bool,
}

impl ConcatStringViewBuilder {
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        Self {
            views: Vec::with_capacity(item_capacity),
            data: Vec::with_capacity(data_capacity),
            block: vec![],
            tainted: false,
        }
    }

    pub fn write<const CHECK_VALID: bool>(
        &mut self,
        column: &ColumnarValueRef,
        i: usize,
    ) {
        match column {
            ColumnarValueRef::Scalar(s) => {
                self.block.extend_from_slice(s);
                self.tainted = true;
            }
            ColumnarValueRef::NullableArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.block.extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableLargeStringArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.block.extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableStringViewArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.block.extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableBinaryArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.block.extend_from_slice(array.value(i));
                }
                self.tainted = true;
            }
            ColumnarValueRef::NonNullableArray(array) => {
                self.block.extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableLargeStringArray(array) => {
                self.block.extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableStringViewArray(array) => {
                self.block.extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableBinaryArray(array) => {
                self.block.extend_from_slice(array.value(i));
                self.tainted = true;
            }
        }
    }

    /// Finalizes the current row by converting the accumulated data into a
    /// StringView and appending it to the views buffer.
    pub fn append_offset(&mut self) -> Result<()> {
        if self.tainted {
            std::str::from_utf8(&self.block)
                .map_err(|_| exec_datafusion_err!("invalid UTF-8 in binary literal"))?;
        }

        let v = &self.block;
        if v.len() > 12 {
            let offset: u32 = self
                .data
                .len()
                .try_into()
                .map_err(|_| exec_datafusion_err!("byte array offset overflow"))?;
            self.data.extend_from_slice(v);
            self.views.push(make_view(v, 0, offset));
        } else {
            self.views.push(make_view(v, 0, 0));
        }

        self.block.clear();
        self.tainted = false;
        Ok(())
    }

    /// Finalize the builder into a concrete [`StringViewArray`].
    ///
    /// # Errors
    ///
    /// Returns an error when:
    ///
    /// - the provided `null_buffer` length does not match the row count.
    pub fn finish(self, null_buffer: Option<NullBuffer>) -> Result<StringViewArray> {
        if let Some(ref nulls) = null_buffer
            && nulls.len() != self.views.len()
        {
            return internal_err!(
                "Null buffer length ({}) must match row count ({})",
                nulls.len(),
                self.views.len()
            );
        }

        let buffers: Vec<Buffer> = if self.data.is_empty() {
            vec![]
        } else {
            vec![Buffer::from(self.data)]
        };

        // SAFETY: views were constructed with correct lengths, offsets, and
        // prefixes. UTF-8 validity was checked in append_offset() for any row
        // where tainted data (e.g., binary literals) was appended.
        let array = unsafe {
            StringViewArray::new_unchecked(
                ScalarBuffer::from(self.views),
                buffers,
                null_buffer,
            )
        };
        Ok(array)
    }
}

/// Builder used by `concat`/`concat_ws` to assemble a [`LargeStringArray`] one
/// row at a time from multiple input columns. See [`ConcatStringBuilder`] for
/// details on the row-composition contract.
///
/// For the common "produce one `&str` per row" pattern, prefer
/// `GenericStringArrayBuilder` instead.
pub(crate) struct ConcatLargeStringBuilder {
    offsets_buffer: MutableBuffer,
    value_buffer: MutableBuffer,
    /// If true, a safety check is required during the `finish` call
    tainted: bool,
}

impl ConcatLargeStringBuilder {
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        let capacity = item_capacity
            .checked_add(1)
            .map(|i| i.saturating_mul(size_of::<i64>()))
            .expect("capacity integer overflow");

        let mut offsets_buffer = MutableBuffer::with_capacity(capacity);
        // SAFETY: the first offset value is definitely not going to exceed the bounds.
        unsafe { offsets_buffer.push_unchecked(0_i64) };
        Self {
            offsets_buffer,
            value_buffer: MutableBuffer::with_capacity(data_capacity),
            tainted: false,
        }
    }

    pub fn write<const CHECK_VALID: bool>(
        &mut self,
        column: &ColumnarValueRef,
        i: usize,
    ) {
        match column {
            ColumnarValueRef::Scalar(s) => {
                self.value_buffer.extend_from_slice(s);
                self.tainted = true;
            }
            ColumnarValueRef::NullableArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer
                        .extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableLargeStringArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer
                        .extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableStringViewArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer
                        .extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableBinaryArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer.extend_from_slice(array.value(i));
                }
                self.tainted = true;
            }
            ColumnarValueRef::NonNullableArray(array) => {
                self.value_buffer
                    .extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableLargeStringArray(array) => {
                self.value_buffer
                    .extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableStringViewArray(array) => {
                self.value_buffer
                    .extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableBinaryArray(array) => {
                self.value_buffer.extend_from_slice(array.value(i));
                self.tainted = true;
            }
        }
    }

    pub fn append_offset(&mut self) -> Result<()> {
        let next_offset: i64 = self
            .value_buffer
            .len()
            .try_into()
            .map_err(|_| exec_datafusion_err!("byte array offset overflow"))?;
        self.offsets_buffer.push(next_offset);
        Ok(())
    }

    /// Finalize the builder into a concrete [`LargeStringArray`].
    ///
    /// # Errors
    ///
    /// Returns an error when:
    ///
    /// - the provided `null_buffer` is not the same length as the `offsets_buffer`.
    pub fn finish(self, null_buffer: Option<NullBuffer>) -> Result<LargeStringArray> {
        let row_count = self.offsets_buffer.len() / size_of::<i64>() - 1;
        if let Some(ref null_buffer) = null_buffer
            && null_buffer.len() != row_count
        {
            return internal_err!(
                "Null buffer and offsets buffer must be the same length"
            );
        }
        let array_builder = ArrayDataBuilder::new(DataType::LargeUtf8)
            .len(row_count)
            .add_buffer(self.offsets_buffer.into())
            .add_buffer(self.value_buffer.into())
            .nulls(null_buffer);
        if self.tainted {
            // Raw binary arrays with possible invalid utf-8 were used,
            // so let ArrayDataBuilder perform validation
            let array_data = array_builder.build()?;
            Ok(LargeStringArray::from(array_data))
        } else {
            // SAFETY: all data that was appended was valid Large UTF8 and the values
            // and offsets were created correctly
            let array_data = unsafe { array_builder.build_unchecked() };
            Ok(LargeStringArray::from(array_data))
        }
    }
}

// ----------------------------------------------------------------------------
// Bulk-nulls builders
//
// These builders are similar to Arrow's `GenericStringBuilder` and
// `StringViewBuilder`, except that callers must pass the NULL bitmap to
// `finish()`, rather than maintaining it iteratively (per-row). For callers
// that can compute the NULL bitmap in bulk (which is true of many
// string-related UDFs), this can be significantly more efficient.
//
// For a row known to be null, call `append_placeholder` to advance the row
// count without touching the value buffer; the caller MUST ensure that the
// corresponding bit is cleared (0 = null) in the null buffer passed to
// `finish`.
// ----------------------------------------------------------------------------

/// Builder for a [`GenericStringArray<O>`] that defers null tracking to
/// `finish`. Instantiate with `O = i32` for [`StringArray`] (Utf8) or
/// `O = i64` for [`LargeStringArray`] (LargeUtf8).
pub(crate) struct GenericStringArrayBuilder<O: OffsetSizeTrait> {
    offsets_buffer: MutableBuffer,
    value_buffer: MutableBuffer,
    placeholder_count: usize,
    _phantom: PhantomData<O>,
}

impl<O: OffsetSizeTrait> GenericStringArrayBuilder<O> {
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        let capacity = item_capacity
            .checked_add(1)
            .map(|i| i.saturating_mul(size_of::<O>()))
            .expect("capacity integer overflow");

        let mut offsets_buffer = MutableBuffer::with_capacity(capacity);
        offsets_buffer.push(O::usize_as(0));
        Self {
            offsets_buffer,
            value_buffer: MutableBuffer::with_capacity(data_capacity),
            placeholder_count: 0,
            _phantom: PhantomData,
        }
    }

    /// Append `value` as the next row.
    ///
    /// # Panics
    ///
    /// Panics if the cumulative byte length exceeds `O::MAX`.
    pub fn append_value(&mut self, value: &str) {
        self.value_buffer.extend_from_slice(value.as_bytes());
        let next_offset =
            O::from_usize(self.value_buffer.len()).expect("byte array offset overflow");
        self.offsets_buffer.push(next_offset);
    }

    /// Append an empty placeholder row. The corresponding slot must be masked
    /// as null by the null buffer passed to `finish`.
    pub fn append_placeholder(&mut self) {
        let next_offset =
            O::from_usize(self.value_buffer.len()).expect("byte array offset overflow");
        self.offsets_buffer.push(next_offset);
        self.placeholder_count += 1;
    }

    /// Finalize into a [`GenericStringArray<O>`] using the caller-supplied
    /// null buffer.
    ///
    /// # Errors
    ///
    /// Returns an error when `null_buffer.len()` does not match the number of
    /// appended rows.
    pub fn finish(
        self,
        null_buffer: Option<NullBuffer>,
    ) -> Result<GenericStringArray<O>> {
        let row_count = self.offsets_buffer.len() / size_of::<O>() - 1;
        if let Some(ref n) = null_buffer
            && n.len() != row_count
        {
            return internal_err!(
                "Null buffer length ({}) must match row count ({row_count})",
                n.len()
            );
        }
        let null_count = null_buffer.as_ref().map_or(0, |n| n.null_count());
        debug_assert!(
            null_count >= self.placeholder_count,
            "{} placeholder rows but null buffer has {null_count} nulls",
            self.placeholder_count,
        );
        let array_data = ArrayDataBuilder::new(GenericStringArray::<O>::DATA_TYPE)
            .len(row_count)
            .add_buffer(self.offsets_buffer.into())
            .add_buffer(self.value_buffer.into())
            .nulls(null_buffer);
        // SAFETY: every appended value came from a `&str`, so the value
        // buffer is valid UTF-8 and offsets are monotonically non-decreasing.
        let array_data = unsafe { array_data.build_unchecked() };
        Ok(GenericStringArray::<O>::from(array_data))
    }
}

/// Builder for a [`StringViewArray`] that defers null tracking to `finish`.
///
/// This optimizes the special case for String functions where an input NULL -->
/// an output NULL.
///
/// This wrapper delegates all string-view layout details to
/// [`StringViewBuilder`], but  intentionally does not expose null-appending
/// APIs: callers append either a real value or an empty placeholder row, then
/// pass the final null buffer to `finish`.
pub(crate) struct StringViewArrayBuilder {
    inner: StringViewBuilder,
    placeholder_count: usize,
}

impl StringViewArrayBuilder {
    pub fn with_capacity(item_capacity: usize) -> Self {
        Self {
            inner: StringViewBuilder::with_capacity(item_capacity),
            placeholder_count: 0,
        }
    }

    /// Append `value` as the next row.
    ///
    /// # Panics
    ///
    /// Panics if Arrow's [`StringViewBuilder`] cannot encode the value.
    #[inline]
    pub fn append_value(&mut self, value: &str) {
        self.inner.append_value(value);
    }

    /// Append an empty placeholder row. The corresponding slot must be
    /// masked as null by the null buffer passed to `finish`.
    #[inline]
    pub fn append_placeholder(&mut self) {
        // Use an empty non-null value only to advance Arrow's builder and
        // produce a valid zero-length view. The caller-supplied null buffer in
        // `finish` replaces Arrow's internal all-valid null state.
        self.inner.append_value("");
        self.placeholder_count += 1;
    }

    /// Finalize into a [`StringViewArray`] using the caller-supplied null
    /// buffer.
    ///
    /// # Errors
    ///
    /// Returns an error when `null_buffer.len()` does not match the number of
    /// appended rows.
    pub fn finish(mut self, null_buffer: Option<NullBuffer>) -> Result<StringViewArray> {
        let row_count = self.inner.len();
        if let Some(ref n) = null_buffer
            && n.len() != row_count
        {
            return internal_err!(
                "Null buffer length ({}) must match row count ({})",
                n.len(),
                row_count
            );
        }
        let null_count = null_buffer.as_ref().map_or(0, |n| n.null_count());
        debug_assert!(
            null_count >= self.placeholder_count,
            "{} placeholder rows but null buffer has {null_count} nulls",
            self.placeholder_count,
        );

        let (views, buffers, _) = self.inner.finish().into_parts();
        // SAFETY: `views` and `buffers` come directly from Arrow's
        // `StringViewBuilder::finish`, so the only changed component is the
        // null buffer. Its length was checked above to match the row count.
        let array =
            unsafe { StringViewArray::new_unchecked(views, buffers, null_buffer) };
        Ok(array)
    }
}

/// Append a new view to the views buffer with the given substr.
///
/// Callers are responsible for their own null tracking.
///
/// # Safety
///
/// original_view must be a valid view (the format described on
/// [`GenericByteViewArray`](arrow::array::GenericByteViewArray).
///
/// # Arguments
/// - views_buffer: The buffer to append the new view to
/// - original_view: The original view value
/// - substr: The substring to append. Must be a valid substring of the original view
/// - start_offset: The start offset of the substring in the view
///
/// LLVM is apparently overly eager to inline this function into some hot loops,
/// which bloats them and regresses performance, so we disable inlining for now.
#[inline(never)]
pub(crate) fn append_view(
    views_buffer: &mut Vec<u128>,
    original_view: &u128,
    substr: &str,
    start_offset: u32,
) {
    let substr_len = substr.len();
    let sub_view = if substr_len > 12 {
        let view = ByteView::from(*original_view);
        make_view(
            substr.as_bytes(),
            view.buffer_index,
            view.offset + start_offset,
        )
    } else {
        make_view(substr.as_bytes(), 0, 0)
    };
    views_buffer.push(sub_view);
}

#[derive(Debug)]
pub(crate) enum ColumnarValueRef<'a> {
    Scalar(&'a [u8]),
    NullableArray(&'a StringArray),
    NonNullableArray(&'a StringArray),
    NullableLargeStringArray(&'a LargeStringArray),
    NonNullableLargeStringArray(&'a LargeStringArray),
    NullableStringViewArray(&'a StringViewArray),
    NonNullableStringViewArray(&'a StringViewArray),
    NullableBinaryArray(&'a BinaryArray),
    NonNullableBinaryArray(&'a BinaryArray),
}

impl ColumnarValueRef<'_> {
    #[inline]
    pub fn is_valid(&self, i: usize) -> bool {
        match &self {
            Self::Scalar(_)
            | Self::NonNullableArray(_)
            | Self::NonNullableLargeStringArray(_)
            | Self::NonNullableStringViewArray(_)
            | Self::NonNullableBinaryArray(_) => true,
            Self::NullableArray(array) => array.is_valid(i),
            Self::NullableStringViewArray(array) => array.is_valid(i),
            Self::NullableLargeStringArray(array) => array.is_valid(i),
            Self::NullableBinaryArray(array) => array.is_valid(i),
        }
    }

    #[inline]
    pub fn nulls(&self) -> Option<NullBuffer> {
        match &self {
            Self::Scalar(_)
            | Self::NonNullableArray(_)
            | Self::NonNullableStringViewArray(_)
            | Self::NonNullableLargeStringArray(_)
            | Self::NonNullableBinaryArray(_) => None,
            Self::NullableArray(array) => array.nulls().cloned(),
            Self::NullableStringViewArray(array) => array.nulls().cloned(),
            Self::NullableLargeStringArray(array) => array.nulls().cloned(),
            Self::NullableBinaryArray(array) => array.nulls().cloned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "capacity integer overflow")]
    fn test_overflow_concat_string_builder() {
        let _builder = ConcatStringBuilder::with_capacity(usize::MAX, usize::MAX);
    }

    #[test]
    #[should_panic(expected = "capacity integer overflow")]
    fn test_overflow_concat_large_string_builder() {
        let _builder = ConcatLargeStringBuilder::with_capacity(usize::MAX, usize::MAX);
    }

    #[test]
    fn string_array_builder_empty() {
        let builder = GenericStringArrayBuilder::<i32>::with_capacity(0, 0);
        let array = builder.finish(None).unwrap();
        assert_eq!(array.len(), 0);
    }

    #[test]
    fn string_array_builder_no_nulls() {
        let mut builder = GenericStringArrayBuilder::<i32>::with_capacity(3, 16);
        builder.append_value("foo");
        builder.append_value("");
        builder.append_value("hello world");
        let array = builder.finish(None).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.value(0), "foo");
        assert_eq!(array.value(1), "");
        assert_eq!(array.value(2), "hello world");
        assert_eq!(array.null_count(), 0);
    }

    #[test]
    fn string_array_builder_with_nulls() {
        let mut builder = GenericStringArrayBuilder::<i32>::with_capacity(3, 8);
        builder.append_value("a");
        builder.append_placeholder();
        builder.append_value("c");
        let nulls = NullBuffer::from(vec![true, false, true]);
        let array = builder.finish(Some(nulls)).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.value(0), "a");
        assert!(array.is_null(1));
        assert_eq!(array.value(2), "c");
    }

    #[test]
    fn string_array_builder_null_buffer_length_mismatch() {
        let mut builder = GenericStringArrayBuilder::<i32>::with_capacity(2, 4);
        builder.append_value("a");
        builder.append_value("b");
        let nulls = NullBuffer::from(vec![true, false, true]);
        assert!(builder.finish(Some(nulls)).is_err());
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "placeholder rows")]
    fn string_array_builder_placeholder_without_null_mask() {
        let mut builder = GenericStringArrayBuilder::<i32>::with_capacity(2, 4);
        builder.append_value("a");
        builder.append_placeholder();
        // Slot 1 is a placeholder but the null buffer doesn't mark it null.
        let nulls = NullBuffer::from(vec![true, true]);
        let _ = builder.finish(Some(nulls));
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "placeholder rows")]
    fn string_array_builder_placeholder_with_none_null_buffer() {
        let mut builder = GenericStringArrayBuilder::<i32>::with_capacity(1, 4);
        builder.append_placeholder();
        let _ = builder.finish(None);
    }

    #[test]
    fn string_array_builder_all_placeholders() {
        let mut builder = GenericStringArrayBuilder::<i32>::with_capacity(3, 0);
        builder.append_placeholder();
        builder.append_placeholder();
        builder.append_placeholder();
        let nulls = NullBuffer::from(vec![false, false, false]);
        let array = builder.finish(Some(nulls)).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.null_count(), 3);
        assert!((0..3).all(|i| array.is_null(i)));
    }

    #[test]
    fn large_string_array_builder_with_nulls() {
        let mut builder = GenericStringArrayBuilder::<i64>::with_capacity(3, 8);
        builder.append_value("a");
        builder.append_placeholder();
        builder.append_value("c");
        let nulls = NullBuffer::from(vec![true, false, true]);
        let array = builder.finish(Some(nulls)).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.value(0), "a");
        assert!(array.is_null(1));
        assert_eq!(array.value(2), "c");
    }

    #[test]
    fn string_view_array_builder_empty() {
        let builder = StringViewArrayBuilder::with_capacity(0);
        let array = builder.finish(None).unwrap();
        assert_eq!(array.len(), 0);
    }

    #[test]
    fn string_view_array_builder_inline_and_buffer() {
        let mut builder = StringViewArrayBuilder::with_capacity(3);
        builder.append_value("short"); // ≤ 12 bytes, inline
        builder.append_value("a string longer than twelve bytes");
        builder.append_value("");
        let array = builder.finish(None).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.value(0), "short");
        assert_eq!(array.value(1), "a string longer than twelve bytes");
        assert_eq!(array.value(2), "");
    }

    #[test]
    fn string_view_array_builder_with_nulls() {
        let mut builder = StringViewArrayBuilder::with_capacity(4);
        builder.append_value("a string longer than twelve bytes");
        builder.append_placeholder();
        builder.append_value("short");
        builder.append_placeholder();
        let nulls = NullBuffer::from(vec![true, false, true, false]);
        let array = builder.finish(Some(nulls)).unwrap();
        assert_eq!(array.len(), 4);
        assert_eq!(array.value(0), "a string longer than twelve bytes");
        assert!(array.is_null(1));
        assert_eq!(array.value(2), "short");
        assert!(array.is_null(3));
    }

    #[test]
    fn string_view_array_builder_all_placeholders() {
        let mut builder = StringViewArrayBuilder::with_capacity(3);
        builder.append_placeholder();
        builder.append_placeholder();
        builder.append_placeholder();
        let nulls = NullBuffer::from(vec![false, false, false]);
        let array = builder.finish(Some(nulls)).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.null_count(), 3);
        assert!((0..3).all(|i| array.is_null(i)));
    }

    #[test]
    fn string_view_array_builder_null_buffer_length_mismatch() {
        let mut builder = StringViewArrayBuilder::with_capacity(2);
        builder.append_value("a");
        builder.append_value("b");
        let nulls = NullBuffer::from(vec![true, false, true]);
        assert!(builder.finish(Some(nulls)).is_err());
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "placeholder rows")]
    fn string_view_array_builder_placeholder_without_null_mask() {
        let mut builder = StringViewArrayBuilder::with_capacity(2);
        builder.append_value("a");
        builder.append_placeholder();
        let nulls = NullBuffer::from(vec![true, true]);
        let _ = builder.finish(Some(nulls));
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "placeholder rows")]
    fn string_view_array_builder_placeholder_with_none_null_buffer() {
        let mut builder = StringViewArrayBuilder::with_capacity(1);
        builder.append_placeholder();
        let _ = builder.finish(None);
    }

    #[test]
    fn string_view_array_builder_flushes_full_blocks() {
        // Each value is 300 bytes. The first data block is 2 × STARTING_BLOCK_SIZE
        // = 16 KiB, so ~50 values saturate it and the rest spill into additional
        // blocks.
        let value = "x".repeat(300);
        let mut builder = StringViewArrayBuilder::with_capacity(100);
        for _ in 0..100 {
            builder.append_value(&value);
        }
        let array = builder.finish(None).unwrap();
        assert_eq!(array.len(), 100);
        assert!(
            array.data_buffers().len() > 1,
            "expected multiple data buffers, got {}",
            array.data_buffers().len()
        );
        for i in 0..100 {
            assert_eq!(array.value(i), value);
        }
    }
}
