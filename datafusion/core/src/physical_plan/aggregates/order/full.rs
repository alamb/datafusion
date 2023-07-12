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

/// Tracks grouping state when the data is ordered entirely by its
/// group keys
///
/// When the group values are sorted, as soon as we see group `n+1` we
/// know we will never see any rows for group `n again and thus they
/// can be emitted.
///
/// ```text
///  SUM(amt) GROUP BY id
///
///  The input is sorted by id
///
///
///      ┌─────┐   ┌──────────────────┐
///      │┌───┐│   │ ┌──────────────┐ │         ┏━━━━━━━━━━━━━━┓
///      ││ 0 ││   │ │     123      │ │   ┌─────┃      13      ┃
///      │└───┘│   │ └──────────────┘ │   │     ┗━━━━━━━━━━━━━━┛
///      │ ... │   │    ...           │   │
///      │┌───┐│   │ ┌──────────────┐ │   │         current
///      ││12 ││   │ │     234      │ │   │
///      │├───┤│   │ ├──────────────┤ │   │
///      ││12 ││   │ │     234      │ │   │
///      │├───┤│   │ ├──────────────┤ │   │
///      ││13 ││   │ │     456      │◀┼───┘
///      │└───┘│   │ └──────────────┘ │
///      └─────┘   └──────────────────┘
///
///  group indices    group_values        current tracks the most
/// (in group value                          recent group index
///      order)
/// ```
///
/// In the above diagram the current group is `13` groups `0..12` can
/// be emitted. Group `13` can not be emitted because it may have more
/// values in the next batch.
#[derive(Debug)]
pub(crate) struct GroupOrderingFull {
    state: State,
    /// Hash values for groups in 0..completed
    hashes: Vec<u64>,
}

#[derive(Debug)]
enum State {
    /// Have seen no input yet
    Start,

    /// Have seen all groups with indexes less than `completed_index`
    InProgress {
        /// index of the current group for which values are being
        /// generated (can emit current - 1)
        current: usize,
    },

    /// Seen end of input, all groups can be emitted
    Complete,
}

impl GroupOrderingFull {
    pub fn new() -> Self {
        Self {
            state: State::Start,
            hashes: vec![],
        }
    }

    // How far can data be emitted? Returns None if no data can be
    // emitted
    pub fn emit_to(&self) -> Option<EmitTo> {
        match &self.state {
            State::Start => None,
            State::InProgress { current, .. } => {
                // Can not emit if we are still on the first row,
                // otherwise emit all rows prior to the current group
                if *current == 0 {
                    None
                } else {
                    Some(EmitTo::First(*current - 1))
                }
            }
            State::Complete { .. } => Some(EmitTo::All),
        }
    }

    /// removes the first n groups from this ordering, shifting all
    /// existing indexes down by N and returns a reference to the
    /// updated hashes
    pub fn remove_groups(&mut self, n: usize) -> &[u64] {
        println!("remove_groups n:{n}, self: {self:?}");
        match &mut self.state {
            State::Start => panic!("invalid state: start"),
            State::InProgress { current } => {
                // shift down by n
                assert!(*current >= n);
                *current = *current - n;
                self.hashes.drain(0..n);
            }
            State::Complete { .. } => panic!("invalid state: complete"),
        };
        &self.hashes
    }

    /// Note that the input is complete so any outstanding groups are done as well
    pub fn input_done(&mut self) {
        println!("input done");
        self.state = match self.state {
            State::Start => State::Complete,
            State::InProgress { .. } => State::Complete,
            State::Complete => State::Complete,
        };
    }

    /// Note that we saw a new distinct group
    pub fn new_group(&mut self, group_index: usize, hash: u64) {
        println!("new group: group_index: {group_index}");
        self.state = match self.state {
            State::Start => {
                assert_eq!(group_index, 0);
                self.hashes.push(hash);
                State::InProgress {
                    current: group_index,
                }
            }
            State::InProgress { current } => {
                // expect to see group_index the next after this
                assert_eq!(group_index, self.hashes.len());
                assert_eq!(group_index, current + 1);
                self.hashes.push(hash);
                State::InProgress {
                    current: group_index,
                }
            }
            State::Complete { .. } => {
                panic!("Saw new group after input was complete");
            }
        };
    }
}
