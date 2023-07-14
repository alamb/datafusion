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

use crate::physical_expr::EmitTo;
use arrow::row::{OwnedRow, Row, RowConverter, Rows, SortField};
use arrow_array::ArrayRef;
use arrow_schema::Schema;
use datafusion_common::Result;
use datafusion_physical_expr::PhysicalSortExpr;

/// Tracks grouping state when the data is  ordered by some subset of  its
/// group keys.
///
/// I this case, once we see the next sort keys value, we know we will
/// never see groups with that sort key again, so we can emit all
/// previous groups to that point.
///
/// ```text
///
///   SUM(amt) GROUP BY id, state
///
///   The input is sorted by state
///
///                                            ┏━━━━━━━━━━━━━━━━━┓ ┏━━━━━━━┓
///     ┌─────┐    ┌───────────────────┐ ┌─────┃        9        ┃ ┃ "MD"  ┃
///     │┌───┐│    │ ┌──────────────┐  │ │     ┗━━━━━━━━━━━━━━━━━┛ ┗━━━━━━━┛
///     ││ 0 ││    │ │  123, "MA"   │  │ │        current_sort      sort_key
///     │└───┘│    │ └──────────────┘  │ │
///     │ ... │    │    ...            │ │      current_sort tracks the most
///     │┌───┐│    │ ┌──────────────┐  │ │      recent group index that had
///     ││12 ││    │ │  765, "MA"   │  │ │      the same sort_key as current
///     │├───┤│    │ ├──────────────┤  │ │
///     ││12 ││    │ │  923, "MD"   │◀─┼─┘
///     │├───┤│    │ ├──────────────┤  │        ┏━━━━━━━━━━━━━━┓
///     ││13 ││    │ │  345, "MD"   │◀─┼────────┃      12      ┃
///     │└───┘│    │ └──────────────┘  │        ┗━━━━━━━━━━━━━━┛
///     └─────┘    └───────────────────┘            current
///  group indices
/// (in group value  group_values               current tracks the most
///      order)                                    recent group index
///```
#[derive(Debug)]
pub(crate) struct GroupOrderingPartial {
    /// State machine
    state: State,

    /// The indexes in the group by expresion that form the sort key
    order_indices: Vec<usize>,

    /// Converter for the columns of the group by that make up the sort key.
    row_converter: RowConverter,

    /// Hash values for groups in 0..completed
    hashes: Vec<u64>,
}

/// Tracks the state of the the grouping
#[derive(Debug, Default)]
enum State {
    /// Taken to potentially be updated.
    #[default]
    Taken,

    /// Have seen no input yet
    Start,

    /// Have seen all groups with indexes less than `completed_index`
    InProgress {
        /// first group index with with sort_key
        current_sort: usize,
        /// The sort key of group_index `current_sort
        sort_key: OwnedRow,
        /// index of the current group for which values are being
        /// generated
        current: usize,
    },

    /// Seen end of input, all groups can be emitted
    Complete,
}

impl GroupOrderingPartial {
    pub fn try_new(
        input_schema: &Schema,
        order_indices: &[usize],
        ordering: &[PhysicalSortExpr],
    ) -> Result<Self> {
        assert!(!order_indices.is_empty());
        assert_eq!(order_indices.len(), ordering.len());

        let fields = ordering
            .iter()
            .map(|sort_expr| {
                Ok(SortField::new_with_options(
                    sort_expr.expr.data_type(input_schema)?,
                    sort_expr.options,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            state: State::Start,
            order_indices: order_indices.to_vec(),
            row_converter: RowConverter::new(fields)?,
            hashes: vec![],
        })
    }

    /// Creates SortKeys from the group values
    ///
    /// For example, if group_values had `A, B, C` but the input was
    /// only sorted on `B` and `C` this sould return rows for (`B`,
    /// `C`)
    pub fn compute_sort_keys(&mut self, group_values: &[ArrayRef]) -> Result<Rows> {
        // Take only the columns that are in the sort key
        let sort_values: Vec<_> = self
            .order_indices
            .iter()
            .map(|&idx| group_values[idx].clone())
            .collect();

        Ok(self.row_converter.convert_columns(&sort_values)?)
    }

    /// How far can data be emitted? Returns None if no data can be
    /// emitted
    pub fn emit_to(&self) -> Option<EmitTo> {
        match &self.state {
            State::Taken => unreachable!("State previously taken"),
            State::Start => None,
            State::InProgress { current_sort, .. } => {
                // Can not emit if we are still on the first row sort
                // row otherwise we can emit all groups that had earlier sort keys
                //
                if *current_sort == 0 {
                    None
                } else {
                    Some(EmitTo::First(*current_sort - 1))
                }
            }
            State::Complete => Some(EmitTo::All),
        }
    }

    /// removes the first n groups from this ordering, shifting all
    /// existing indexes down by N and returns a reference to the
    /// updated hashes
    pub fn remove_groups(&mut self, n: usize) -> &[u64] {
        match &mut self.state {
            State::Taken => unreachable!("State previously taken"),
            State::Start => panic!("invalid state: start"),
            State::InProgress {
                current_sort,
                current,
                sort_key: _,
            } => {
                // shift indexes down by n
                assert!(*current >= n);
                *current -= n;
                assert!(*current_sort >= n);
                *current_sort -= n;
                // Note sort_key stays the same, we are just translating group indexes
                self.hashes.drain(0..n);
            }
            State::Complete { .. } => panic!("invalid state: complete"),
        };
        &self.hashes
    }

    /// Note that the input is complete so any outstanding groups are done as well
    pub fn input_done(&mut self) {
        self.state = match self.state {
            State::Taken => unreachable!("State previously taken"),
            State::Start => State::Complete,
            State::InProgress { .. } => State::Complete,
            State::Complete => State::Complete,
        };
    }

    /// Note that we saw a new distinct group with the specified groups sort key
    pub fn new_group(&mut self, group_index: usize, group_sort_key: Row, hash: u64) {
        let old_state = std::mem::take(&mut self.state);
        self.state = match old_state {
            State::Taken => unreachable!("State previously taken"),
            State::Start => {
                assert_eq!(group_index, 0);
                self.hashes.push(hash);
                State::InProgress {
                    current_sort: 0,
                    sort_key: group_sort_key.owned(),
                    current: 0,
                }
            }
            State::InProgress {
                current_sort,
                sort_key,
                current,
            } => {
                // expect to see group_index the next after this
                assert_eq!(group_index, self.hashes.len());
                assert_eq!(group_index, current + 1);
                self.hashes.push(hash);

                // Does this group have seen a new sort_key?
                if sort_key.row() == group_sort_key {
                    State::InProgress {
                        current_sort: group_index,
                        sort_key: group_sort_key.owned(),
                        current: group_index,
                    }
                }
                // same sort key
                else {
                    State::InProgress {
                        current_sort,
                        sort_key,
                        current: group_index,
                    }
                }
            }
            State::Complete => {
                panic!("Saw new group after the end of input");
            }
        }
    }
}
