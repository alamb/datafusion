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

//! Vectorized [`accumulate`] and [`accumulate_nullable`] functions.
//!
//! These functions are designed to be the performance critical inner
//! loops of accumlators and thus there are multiple versions, to be
//! invoked depending on the input.
//!
//! There are typically 4 potential combinations of input values that
//! accumulators need to special case for performance,
//!
//! With / Without filter
//! With / Without nulls
//!
//! If there are filters present, the accumulator typically needs to
//! to track if it has seen *any* value for that group (as some values
//! may be filtered out). Without a filter, the accumulator is only
//! invoked for groups that actually had a value to accumulate so they
//! do not need to track if they have seen values for a particular
//! group.
//!
//! If the input has nulls, then the accumulator must also potentially
//! handle each input null value specially (e.g. for `SUM` to mark the
//! corresponding sum as null)

use arrow_array::{Array, ArrowNumericType, BooleanArray, PrimitiveArray};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};

/// This structure is used to update the accumulator state per row for
/// a `PrimitiveArray<T>`, and track if values or nulls have been seen
/// for each group. Since it is the inner loop for many
/// GroupsAccumulators, the  performance is critical.
///
#[derive(Debug)]
pub struct NullState {
    /// If we have seen a null input value for `group_index`
    null_inputs: Option<BooleanBufferBuilder>,

    /// If there has been a filter value, has it seen any non-filtered
    /// input values for `group_index`?
    seen_values: Option<BooleanBufferBuilder>,
}

impl NullState {
    pub fn new() -> Self {
        Self {
            null_inputs: None,
            seen_values: None,
        }
    }

    /// Invokes `value_fn(group_index, value)` for each non null, non
    /// filtered value, while tracking which groups have seen null
    /// inputs and which groups have seen any inputs
    //
    /// # Arguments:
    ///
    /// * `values`: the input arguments to the accumulator
    /// * `group_indices`:  To which groups do the rows in `values` belong, (aka group_index)
    /// * `opt_filter`: if present, only rows for which is Some(true) are included
    /// * `value_fn`: function invoked for  (group_index, value) where value is non null
    ///
    /// `F`: Invoked for each input row like `value_fn(group_index,
    /// value)` for each non null, non filtered value.
    ///
    /// # Example
    ///
    /// ```text
    ///  ┌─────────┐   ┌─────────┐   ┌ ─ ─ ─ ─ ┐
    ///  │ ┌─────┐ │   │ ┌─────┐ │     ┌─────┐
    ///  │ │  2  │ │   │ │ 200 │ │   │ │  t  │ │
    ///  │ ├─────┤ │   │ ├─────┤ │     ├─────┤
    ///  │ │  2  │ │   │ │ 100 │ │   │ │  f  │ │
    ///  │ ├─────┤ │   │ ├─────┤ │     ├─────┤
    ///  │ │  0  │ │   │ │ 200 │ │   │ │  t  │ │
    ///  │ ├─────┤ │   │ ├─────┤ │     ├─────┤
    ///  │ │  1  │ │   │ │ 200 │ │   │ │NULL │ │
    ///  │ ├─────┤ │   │ ├─────┤ │     ├─────┤
    ///  │ │  0  │ │   │ │ 300 │ │   │ │  t  │ │
    ///  │ └─────┘ │   │ └─────┘ │     └─────┘
    ///  └─────────┘   └─────────┘   └ ─ ─ ─ ─ ┘
    ///
    /// group_indices   values        opt_filter
    /// ```
    ///
    /// In the example above, `value_fn` is invoked for each (group_index,
    /// value) pair where `opt_filter[i]` is true
    ///
    /// ```text
    /// value_fn(2, 200)
    /// value_fn(0, 200)
    /// value_fn(0, 300)
    /// ```
    ///
    /// It also sets
    ///
    /// 1. `self.seen_values[group_index]` to true for all rows that had a value if there is a filter
    ///
    /// 2. `self.null_inputs[group_index]` to true for all rows that had a null in input
    pub fn accumulate<T, F>(
        &mut self,
        group_indices: &[usize],
        values: &PrimitiveArray<T>,
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
        mut value_fn: F,
    ) where
        T: ArrowNumericType + Send,
        F: FnMut(usize, T::Native) + Send,
    {
        let data: &[T::Native] = values.values();
        assert_eq!(data.len(), group_indices.len());

        match (values.nulls(), opt_filter) {
            (Some(nulls), None) if nulls.null_count() > 0 => {
                // All groups start as valid (true), and are set to
                // null if we see a null in the input)
                let null_inputs =
                    initialize_builder(&mut self.null_inputs, total_num_groups, true);

                // This is based on (ahem, COPY/PASTA) arrow::compute::aggregate::sum
                // iterate over in chunks of 64 bits for more efficient null checking
                let data: &[T::Native] = values.values();
                assert_eq!(data.len(), group_indices.len());
                let group_indices_chunks = group_indices.chunks_exact(64);
                let data_chunks = data.chunks_exact(64);
                let bit_chunks = nulls.inner().bit_chunks();

                let group_indices_remainder = group_indices_chunks.remainder();
                let data_remainder = data_chunks.remainder();

                group_indices_chunks
                    .zip(data_chunks)
                    .zip(bit_chunks.iter())
                    .for_each(|((group_index_chunk, data_chunk), mask)| {
                        // index_mask has value 1 << i in the loop
                        let mut index_mask = 1;
                        group_index_chunk.iter().zip(data_chunk.iter()).for_each(
                            |(&group_index, &new_value)| {
                                // valid bit was set, real vale
                                let is_valid = (mask & index_mask) != 0;
                                value_fn(group_index, new_value);
                                if !is_valid {
                                    // input null means this group is now null
                                    null_inputs.set_bit(group_index, false);
                                }
                                index_mask <<= 1;
                            },
                        )
                    });

                // handle any remaining bits (after the intial 64)
                let remainder_bits = bit_chunks.remainder_bits();
                group_indices_remainder
                    .iter()
                    .zip(data_remainder.iter())
                    .enumerate()
                    .for_each(|(i, (&group_index, &new_value))| {
                        let is_valid = remainder_bits & (1 << i) != 0;
                        value_fn(group_index, new_value);
                        if !is_valid {
                            // input null means this group is now null
                            null_inputs.set_bit(group_index, false);
                        }
                    });
            }
            // no filter, no nulls
            (_, None) => {
                // if we have previously seen nulls, ensure the null
                // buffer is big enough (start everything at valid)
                if self.null_inputs.is_some() {
                    initialize_builder(&mut self.null_inputs, total_num_groups, true);
                }
                let iter = group_indices.iter().zip(data.iter());
                for (&group_index, &new_value) in iter {
                    value_fn(group_index, new_value)
                }
            }
            // no nulls, but a filter
            (None, Some(filter)) => {
                assert_eq!(filter.len(), group_indices.len());

                // default seen to false (we fill it in as we go)
                let seen_values =
                    initialize_builder(&mut self.seen_values, total_num_groups, false);
                // The performance with a filter could be improved by
                // iterating over the filter in chunks, rather than a single
                // iterator. TODO file a ticket
                let iter = group_indices.iter().zip(data.iter());
                let iter = iter.zip(filter.iter());
                for ((&group_index, &new_value), filter_value) in iter {
                    if let Some(true) = filter_value {
                        value_fn(group_index, new_value);
                        // remember we have seen a value for this index
                        seen_values.set_bit(group_index, true);
                    }
                }
            }
            // both null values and filters
            (
                Some(_value_nulls /* nulls obtained via values.iters() */),
                Some(filter),
            ) => {
                let null_inputs =
                    initialize_builder(&mut self.null_inputs, total_num_groups, true);
                let seen_values =
                    initialize_builder(&mut self.seen_values, total_num_groups, false);

                assert_eq!(filter.len(), values.len());
                assert_eq!(filter.len(), group_indices.len());
                // The performance with a filter could be improved by
                // iterating over the filter in chunks, rather than using
                // iterators. TODO file a ticket
                filter
                    .iter()
                    .zip(group_indices.iter())
                    .zip(values.iter())
                    .for_each(|((filter_value, group_index), new_value)| {
                        if let Some(true) = filter_value {
                            if let Some(new_value) = new_value {
                                value_fn(*group_index, new_value)
                            } else {
                                // input null means this group is now null
                                null_inputs.set_bit(*group_index, false);
                            }
                            // remember we have seen a value for this index
                            seen_values.set_bit(*group_index, true);
                        }
                    })
            }
        }
    }

    /// Creates the final NullBuffer representing which group_indices have
    /// null values (if they saw a null input, or because they never saw any values)
    ///
    /// resets the internal state to empty
    ///
    /// nulls (validity) set false for any group that saw a null
    /// seen_values (validtity) set true for any group that saw a value
    pub fn build(&mut self) -> Option<NullBuffer> {
        let nulls = self
            .null_inputs
            .as_mut()
            .map(|null_inputs| NullBuffer::new(null_inputs.finish()))
            .and_then(|nulls| {
                if nulls.null_count() > 0 {
                    Some(nulls)
                } else {
                    None
                }
            });

        // if we had filters, some groups may never have seen a group
        // so they are only non-null if we have seen values
        let seen_values = self
            .seen_values
            .as_mut()
            .map(|seen_values| NullBuffer::new(seen_values.finish()));

        match (nulls, seen_values) {
            (None, None) => None,
            (Some(nulls), None) => Some(nulls),
            (None, Some(seen_values)) => Some(seen_values),
            (Some(seen_values), Some(nulls)) => {
                NullBuffer::union(Some(&seen_values), Some(&nulls))
            }
        }
    }
}

/// This function is used to update the accumulator state per row,
/// for a `PrimitiveArray<T>` with no nulls. It is the inner loop for
/// many GroupsAccumulators and thus performance critical.
///
/// # Arguments:
///
/// * `values`: the input arguments to the accumulator
/// * `group_indices`:  To which groups do the rows in `values` belong, group id)
/// * `opt_filter`: if present, invoke value_fn if opt_filter[i] is true
/// * `value_fn`: function invoked for each (group_index, value) pair.
///
/// `F`: Invoked for each input row like `value_fn(group_index, value)
///
/// # Example
///
/// ```text
///  ┌─────────┐   ┌─────────┐   ┌ ─ ─ ─ ─ ┐
///  │ ┌─────┐ │   │ ┌─────┐ │     ┌─────┐
///  │ │  2  │ │   │ │ 200 │ │   │ │  t  │ │
///  │ ├─────┤ │   │ ├─────┤ │     ├─────┤
///  │ │  2  │ │   │ │ 100 │ │   │ │  f  │ │
///  │ ├─────┤ │   │ ├─────┤ │     ├─────┤
///  │ │  0  │ │   │ │ 200 │ │   │ │  t  │ │
///  │ ├─────┤ │   │ ├─────┤ │     ├─────┤
///  │ │  1  │ │   │ │ 200 │ │   │ │NULL │ │
///  │ ├─────┤ │   │ ├─────┤ │     ├─────┤
///  │ │  0  │ │   │ │ 300 │ │   │ │  t  │ │
///  │ └─────┘ │   │ └─────┘ │     └─────┘
///  └─────────┘   └─────────┘   └ ─ ─ ─ ─ ┘
///
/// group_indices   values        opt_filter
/// ```
///
/// In the example above, `value_fn` is invoked for each (group_index,
/// value) pair where `opt_filter[i]` is true
///
/// ```text
/// value_fn(2, 200)
/// value_fn(0, 200)
/// value_fn(0, 300)
/// ```
///
pub fn accumulate_all<T, F>(
    group_indices: &[usize],
    values: &PrimitiveArray<T>,
    opt_filter: Option<&BooleanArray>,
    mut value_fn: F,
) where
    T: ArrowNumericType + Send,
    F: FnMut(usize, T::Native) + Send,
{
    // Given performance is critical, assert if the wrong flavor is called
    assert_eq!(
        values.null_count(), 0,
        "Called accumulate_all with nullable array (call accumulate_all_nullable instead)"
    );

    let data: &[T::Native] = values.values();
    assert_eq!(data.len(), group_indices.len());

    let iter = group_indices.iter().zip(data.iter());

    // handle filter values with a specialized loop
    if let Some(filter) = opt_filter {
        assert_eq!(filter.len(), group_indices.len());
        // The performance with a filter could be improved by
        // iterating over the filter in chunks, rather than a single
        // iterator. TODO file a ticket
        let iter = iter.zip(filter.iter());
        for ((&group_index, &new_value), filter_value) in iter {
            if let Some(true) = filter_value {
                value_fn(group_index, new_value)
            }
        }
    } else {
        for (&group_index, &new_value) in iter {
            value_fn(group_index, new_value)
        }
    }
}

/// This function is called to update the accumulator state per row
/// when the value is not needed (e.g. COUNT)
///
/// `F`: Invoked like `value_fn(group_index).
pub fn accumulate_indices<F>(
    group_indices: &[usize],
    opt_filter: Option<&BooleanArray>,
    mut index_fn: F,
) where
    F: FnMut(usize) + Send,
{
    let iter = group_indices.iter();
    // handle filter values with a specialized loop
    if let Some(filter) = opt_filter {
        assert_eq!(filter.len(), group_indices.len());
        // The performance with a filter could be improved by
        // iterating over the filter in chunks, rather than a single
        // iterator. TODO file a ticket
        let iter = iter.zip(filter.iter());
        for (&group_index, filter_value) in iter {
            if let Some(true) = filter_value {
                index_fn(group_index)
            }
        }
    } else {
        for &group_index in iter {
            index_fn(group_index)
        }
    }
}

/// This function is called to update the accumulator state per row,
/// for a `PrimitiveArray<T>` that can have nulls. See
/// [`accumulate_all`] for more detail and example
///
/// `F`: Invoked like `value_fn(group_index, value, is_valid).
///
/// NOTE the parameter is true when the value is VALID (not when it is
/// NULL).
pub fn accumulate_all_nullable<T, F>(
    group_indices: &[usize],
    values: &PrimitiveArray<T>,
    opt_filter: Option<&BooleanArray>,
    mut value_fn: F,
) where
    T: ArrowNumericType + Send,
    F: FnMut(usize, T::Native, bool) + Send,
{
    // Given performance is critical, assert if the wrong flavor is called
    let valids = values
        .nulls()
        .expect("Called accumulate_all_nullable with non-nullable array (call accumulate_all instead)");

    if let Some(filter) = opt_filter {
        assert_eq!(filter.len(), values.len());
        assert_eq!(filter.len(), group_indices.len());
        // The performance with a filter could be improved by
        // iterating over the filter in chunks, rather than using
        // iterators. TODO file a ticket
        filter
            .iter()
            .zip(group_indices.iter())
            .zip(values.iter())
            .for_each(|((filter_value, group_index), new_value)| {
                // did value[i] pass the filter?
                if let Some(true) = filter_value {
                    // Is value[i] valid?
                    match new_value {
                        Some(new_value) => value_fn(*group_index, new_value, true),
                        None => value_fn(*group_index, Default::default(), false),
                    }
                }
            })
    } else {
        // This is based on (ahem, COPY/PASTA) arrow::compute::aggregate::sum
        // iterate over in chunks of 64 bits for more efficient null checking
        let data: &[T::Native] = values.values();
        assert_eq!(data.len(), group_indices.len());
        let group_indices_chunks = group_indices.chunks_exact(64);
        let data_chunks = data.chunks_exact(64);
        let bit_chunks = valids.inner().bit_chunks();

        let group_indices_remainder = group_indices_chunks.remainder();
        let data_remainder = data_chunks.remainder();

        group_indices_chunks
            .zip(data_chunks)
            .zip(bit_chunks.iter())
            .for_each(|((group_index_chunk, data_chunk), mask)| {
                // index_mask has value 1 << i in the loop
                let mut index_mask = 1;
                group_index_chunk.iter().zip(data_chunk.iter()).for_each(
                    |(&group_index, &new_value)| {
                        // valid bit was set, real vale
                        let is_valid = (mask & index_mask) != 0;
                        value_fn(group_index, new_value, is_valid);
                        index_mask <<= 1;
                    },
                )
            });

        // handle any remaining bits (after the intial 64)
        let remainder_bits = bit_chunks.remainder_bits();
        group_indices_remainder
            .iter()
            .zip(data_remainder.iter())
            .enumerate()
            .for_each(|(i, (&group_index, &new_value))| {
                let is_valid = remainder_bits & (1 << i) != 0;
                value_fn(group_index, new_value, is_valid)
            });
    }
}

pub fn accumulate_indices_nullable<F>(
    group_indices: &[usize],
    array: &dyn Array,
    opt_filter: Option<&BooleanArray>,
    mut index_fn: F,
) where
    F: FnMut(usize) + Send,
{
    // Given performance is critical, assert if the wrong flavor is called
    let valids = array
        .nulls()
        .expect("Called accumulate_all_nullable with non-nullable array (call accumulate_all instead)");

    if let Some(filter) = opt_filter {
        assert_eq!(filter.len(), group_indices.len());
        // The performance with a filter could be improved by
        // iterating over the filter in chunks, rather than using
        // iterators. TODO file a ticket
        filter.iter().zip(group_indices.iter()).for_each(
            |(filter_value, &group_index)| {
                // did value[i] pass the filter?
                if let Some(true) = filter_value {
                    // Is value[i] valid?
                    index_fn(group_index)
                }
            },
        )
    } else {
        // This is based on (ahem, COPY/PASTA) arrow::compute::aggregate::sum
        // iterate over in chunks of 64 bits for more efficient null checking
        let group_indices_chunks = group_indices.chunks_exact(64);
        let bit_chunks = valids.inner().bit_chunks();

        let group_indices_remainder = group_indices_chunks.remainder();

        group_indices_chunks.zip(bit_chunks.iter()).for_each(
            |(group_index_chunk, mask)| {
                // index_mask has value 1 << i in the loop
                let mut index_mask = 1;
                group_index_chunk.iter().for_each(|&group_index| {
                    // valid bit was set, real vale
                    let is_valid = (mask & index_mask) != 0;
                    if is_valid {
                        index_fn(group_index);
                    }
                    index_mask <<= 1;
                })
            },
        );

        // handle any remaining bits (after the intial 64)
        let remainder_bits = bit_chunks.remainder_bits();
        group_indices_remainder
            .iter()
            .enumerate()
            .for_each(|(i, &group_index)| {
                let is_valid = remainder_bits & (1 << i) != 0;
                if is_valid {
                    index_fn(group_index)
                }
            });
    }
}

/// Enures that `builder` contains a `BooleanBufferBuilder with at
/// least `total_num_groups`.
///
/// All new entries are initialized to `default_value`
fn initialize_builder(
    builder: &mut Option<BooleanBufferBuilder>,
    total_num_groups: usize,
    default_value: bool,
) -> &mut BooleanBufferBuilder {
    if builder.is_none() {
        *builder = Some(BooleanBufferBuilder::new(total_num_groups));
    }
    let builder = builder.as_mut().unwrap();

    if builder.len() < total_num_groups {
        let new_groups = total_num_groups - builder.len();
        builder.append_n(new_groups, default_value);
    }
    builder
}

#[cfg(test)]
mod test {
    use super::*;

    use arrow_array::UInt32Array;
    use rand::{rngs::ThreadRng, Rng};

    #[test]
    fn accumulate_no_filter() {
        Fixture::new().accumulate_all_test()
    }

    #[test]
    fn accumulate_with_filter() {
        Fixture::new()
            .with_filter(|group_index, _value, _value_opt| {
                if group_index < 20 {
                    None
                } else if group_index < 40 {
                    Some(false)
                } else {
                    Some(true)
                }
            })
            .accumulate_all_test();
    }

    #[test]
    #[should_panic(
        expected = "assertion failed: `(left == right)`\n  left: `34`,\n right: `0`: Called accumulate_all with nullable array (call accumulate_all_nullable instead)"
    )]
    fn accumulate_with_nullable_panics() {
        let fixture = Fixture::new();
        // call with an array that has nulls should panic
        accumulate_all(
            &fixture.group_indices,
            &fixture.values_with_nulls_array(),
            fixture.opt_filter(),
            |_, _| {},
        );
    }

    #[test]
    fn accumulate_nullable_no_filter() {
        Fixture::new().accumulate_all_nullable_test()
    }

    #[test]
    fn accumulate_nullable_with_filter() {
        Fixture::new()
            .with_filter(|group_index, _value, _value_opt| {
                if group_index < 20 {
                    None
                } else if group_index < 40 {
                    Some(false)
                } else {
                    Some(true)
                }
            })
            .accumulate_all_nullable_test();
    }

    #[test]
    #[should_panic(
        expected = "Called accumulate_all_nullable with non-nullable array (call accumulate_all instead)"
    )]
    fn accumulate_nullable_with_non_nullable_panics() {
        let fixture = Fixture::new();
        // call with an array that has nulls should panic
        accumulate_all_nullable(
            &fixture.group_indices,
            &fixture.values_array(),
            fixture.opt_filter(),
            |_, _, _| {},
        );
    }

    #[test]
    fn accumulate_fuzz() {
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            Fixture::new_random(&mut rng).accumulate_all_test();
        }
    }

    #[test]
    fn accumulate_nullable_fuzz() {
        let mut rng = rand::thread_rng();
        let mut nullable_called = false;
        for _ in 0..100 {
            let fixture = Fixture::new_random(&mut rng);
            // sometimes the random generator will create an array
            // with no nulls so avoid panic'ing in tests
            if fixture.values_with_nulls.iter().any(|v| v.is_none()) {
                nullable_called = true;
                fixture.accumulate_all_nullable_test();
            } else {
                fixture.accumulate_all_test();
            }
            assert!(nullable_called);
        }
    }

    // todo accumulate testing with fuzz

    /// Values for testing (there are enough values to exercise the 64 bit chunks
    struct Fixture {
        /// 100..0
        group_indices: Vec<usize>,

        /// 10, 20, ... 1010
        values: Vec<u32>,

        /// same as values, but every third is null:
        /// None, Some(20), Some(30), None ...
        values_with_nulls: Vec<Option<u32>>,

        /// Optional filter (defaults to None)
        opt_filter: Option<BooleanArray>,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                group_indices: (0..100).collect(),
                values: (0..100).map(|i| (i + 1) * 10).collect(),
                values_with_nulls: (0..100)
                    .map(|i| if i % 3 == 0 { None } else { Some((i + 1) * 10) })
                    .collect(),
                opt_filter: None,
            }
        }

        /// Applies `f(group_index, value, value_with_null)` for all
        /// values in this fixture and set `opt_filter` to the result
        fn with_filter<F>(mut self, mut f: F) -> Self
        where
            F: FnMut(usize, u32, Option<u32>) -> Option<bool>,
        {
            let filter: BooleanArray = self
                .group_indices
                .iter()
                .zip(self.values.iter())
                .zip(self.values_with_nulls.iter())
                .map(|((&group_index, &value), &value_with_null)| {
                    f(group_index, value, value_with_null)
                })
                .collect();

            self.opt_filter = Some(filter);
            self
        }

        fn new_random(rng: &mut ThreadRng) -> Self {
            let num_groups: usize = rng.gen_range(0..1000);
            let group_indices: Vec<usize> = (0..num_groups).map(|_| rng.gen()).collect();

            let values: Vec<u32> = (0..num_groups).map(|_| rng.gen()).collect();

            // with 30 percent probability, add a filter
            let opt_filter = if 0.3 < rng.gen_range(0.0..1.0) {
                // 10% chance of false
                // 10% change of null
                // 80% chance of true
                let filter: BooleanArray = (0..num_groups)
                    .map(|_| {
                        let filter_value = rng.gen_range(0.0..1.0);
                        if filter_value < 0.1 {
                            Some(false)
                        } else if filter_value < 0.2 {
                            None
                        } else {
                            Some(true)
                        }
                    })
                    .collect();
                Some(filter)
            } else {
                None
            };

            // random values with random number and location of nulls
            // random null percentage
            let null_pct: f32 = rng.gen_range(0.0..1.0);
            let values_with_nulls: Vec<Option<u32>> = (0..num_groups)
                .map(|_| {
                    let is_null = null_pct < rng.gen_range(0.0..1.0);
                    if is_null {
                        None
                    } else {
                        Some(rng.gen())
                    }
                })
                .collect();

            Self {
                group_indices,
                values,
                values_with_nulls,
                opt_filter,
            }
        }

        /// returns `Self::values` an Array
        fn values_array(&self) -> UInt32Array {
            UInt32Array::from(self.values.clone())
        }

        /// returns `Self::values_with_nulls` as an Array
        fn values_with_nulls_array(&self) -> UInt32Array {
            UInt32Array::from(self.values_with_nulls.clone())
        }

        fn opt_filter(&self) -> Option<&BooleanArray> {
            self.opt_filter.as_ref()
        }

        // Calls `accumulate_all` with group_indices, values, and
        // opt_filter and ensures it calls the right values
        fn accumulate_all_test(&self) {
            let mut accumulated = vec![];
            accumulate_all(
                &self.group_indices,
                &self.values_array(),
                self.opt_filter(),
                |group_index, value| accumulated.push((group_index, value)),
            );

            // check_values[i] is true if the value[i] should have been included in the output
            let check_values = match self.opt_filter.as_ref() {
                Some(filter) => filter.into_iter().collect::<Vec<_>>(),
                None => vec![Some(true); self.values.len()],
            };

            // Should have only checked indexes where the filter was true
            let mut check_idx = 0;
            for (i, check_value) in check_values.iter().enumerate() {
                if let Some(true) = check_value {
                    let (group_index, value) = &accumulated[check_idx];
                    check_idx += 1;
                    assert_eq!(*group_index, self.group_indices[i]);
                    assert_eq!(*value, self.values[i]);
                }
            }
        }

        // Calls `accumulate_all_nullable` with group_indices, values,
        // and opt_filter and ensures it calls the right values
        fn accumulate_all_nullable_test(&self) {
            let mut accumulated = vec![];

            accumulate_all_nullable(
                &self.group_indices,
                &self.values_with_nulls_array(),
                self.opt_filter(),
                |group_index, value, is_valid| {
                    let value = if is_valid { Some(value) } else { None };
                    accumulated.push((group_index, value));
                },
            );

            // check_values[i] is true if the value[i] should have been included in the output
            let check_values = match self.opt_filter.as_ref() {
                Some(filter) => filter.into_iter().collect::<Vec<_>>(),
                None => vec![Some(true); self.values.len()],
            };

            // Should have see all indexes and values in order
            let mut check_idx = 0;
            for (i, check_value) in check_values.iter().enumerate() {
                if let Some(true) = check_value {
                    let (group_index, value) = &accumulated[check_idx];
                    check_idx += 1;

                    assert_eq!(*group_index, self.group_indices[i]);
                    assert_eq!(*value, self.values_with_nulls[i]);
                }
            }
        }
    }
}
