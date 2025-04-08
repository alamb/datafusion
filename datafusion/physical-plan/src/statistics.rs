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

//! Defines the cross join plan for loading the left side of the cross join
//! and producing batches in parallel for the right partitions

use datafusion_common::Statistics;
use std::ops::Index;
use std::sync::Arc;

/// Represents statistics data grouped by partition.
///
/// This structure maintains a collection of statistics, one for each partition
/// of a distributed dataset, allowing access to statistics by partition index.
#[derive(Debug, Clone)]
pub struct PartitionedStatistics {
    inner: Vec<Arc<Statistics>>,
}

impl PartitionedStatistics {
    pub fn new(statistics: Vec<Arc<Statistics>>) -> Self {
        Self { inner: statistics }
    }

    pub fn statistics(&self, partition_idx: usize) -> &Statistics {
        &self.inner[partition_idx]
    }

    pub fn get_statistics(&self, partition_idx: usize) -> Option<&Statistics> {
        self.inner.get(partition_idx).map(|arc| arc.as_ref())
    }

    pub fn iter(&self) -> impl Iterator<Item = &Statistics> {
        self.inner.iter().map(|arc| arc.as_ref())
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl Index<usize> for PartitionedStatistics {
    type Output = Statistics;

    fn index(&self, partition_idx: usize) -> &Self::Output {
        self.statistics(partition_idx)
    }
}
