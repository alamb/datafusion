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

//! Order tracking for memory bounded grouping

use arrow_schema::Schema;
use datafusion_common::Result;
use datafusion_physical_expr::EmitTo;

use super::{AggregationOrdering, GroupByOrderMode};

mod full;
mod partial;

pub(crate) use full::GroupOrderingFull;
pub(crate) use partial::GroupOrderingPartial;

/// Group ordering state, if present, for each group in the hash
/// table.
#[derive(Debug)]
pub(crate) enum GroupOrdering {
    /// Groups are not ordered
    None,
    /// Groups are orderd by some pre-set of the group keys
    Partial(GroupOrderingPartial),
    /// Groups are entirely contiguous,
    Full(GroupOrderingFull),
    /// The ordering was temporarily taken / borrowed
    /// Note: `Self::Taken` is left when the GroupOrdering is temporarily
    /// taken to satisfy the borrow checker. If an error happens
    /// before it can be restored the ordering information is lost and
    /// execution can not proceed. By panic'ing the behavior remains
    /// well defined if something tries to use a ordering that was
    /// taken.
    Taken,
}

// Default is used for `std::mem::take` to satisfy the borrow checker
impl Default for GroupOrdering {
    fn default() -> Self {
        Self::Taken
    }
}

impl GroupOrdering {
    /// Create a `GroupOrdering` for the ordering
    pub fn try_new(
        input_schema: &Schema,
        ordering: &AggregationOrdering,
    ) -> Result<Self> {
        let AggregationOrdering {
            mode,
            order_indices,
            ordering,
        } = ordering;

        Ok(match mode {
            GroupByOrderMode::None => GroupOrdering::None,
            GroupByOrderMode::PartiallyOrdered => {
                let partial =
                    GroupOrderingPartial::try_new(input_schema, order_indices, ordering)?;
                GroupOrdering::Partial(partial)
            }
            GroupByOrderMode::FullyOrdered => {
                GroupOrdering::Full(GroupOrderingFull::new())
            }
        })
    }

    // How far can data be emitted based on groups seen so far?
    // Returns `None` if nothing can be emitted at this point based on
    // ordering information
    pub fn emit_to(&self) -> Option<EmitTo> {
        match self {
            GroupOrdering::Taken => panic!("group state taken"),
            GroupOrdering::None => None,
            GroupOrdering::Partial(partial) => partial.emit_to(),
            GroupOrdering::Full(full) => full.emit_to(),
        }
    }

    /// Updates the state the input is done
    pub fn input_done(&mut self) {
        match self {
            GroupOrdering::Taken => panic!("group state taken"),
            GroupOrdering::None => {}
            GroupOrdering::Partial(partial) => partial.input_done(),
            GroupOrdering::Full(full) => full.input_done(),
        }
    }

    /// removes the first n groups from this ordering, shifting all
    /// existing indexes down by N and returns a reference to the
    /// updated hashes
    pub fn remove_groups(&mut self, n: usize) -> &[u64] {
        match self {
            GroupOrdering::Taken => panic!("group state taken"),
            GroupOrdering::None => &[],
            GroupOrdering::Partial(partial) => partial.remove_groups(n),
            GroupOrdering::Full(full) => full.remove_groups(n),
        }
    }
}
