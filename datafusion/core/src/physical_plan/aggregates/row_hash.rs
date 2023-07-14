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

//! Hash aggregation

use datafusion_physical_expr::{
    AggregateExpr, EmitTo, GroupsAccumulator, GroupsAccumulatorAdapter,
};
use log::debug;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::vec;

use ahash::RandomState;
use arrow::row::{RowConverter, Rows, SortField};
use datafusion_physical_expr::hash_utils::create_hashes;
use futures::ready;
use futures::stream::{Stream, StreamExt};

use crate::physical_plan::aggregates::{
    evaluate_group_by, evaluate_many, evaluate_optional, group_schema, AggregateMode,
    PhysicalGroupBy,
};
use crate::physical_plan::metrics::{BaselineMetrics, RecordOutput};
use crate::physical_plan::{aggregates, PhysicalExpr};
use crate::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use arrow::array::*;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion_common::Result;
use datafusion_execution::memory_pool::proxy::{RawTableAllocExt, VecAllocExt};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::TaskContext;
use hashbrown::raw::RawTable;

#[derive(Debug, Clone)]
/// This object tracks the aggregation phase (input/output)
pub(crate) enum ExecutionState {
    ReadingInput,
    /// When producing output, the remaining rows to output are stored
    /// here and are sliced off as needed in batch_size chunks
    ProducingOutput(RecordBatch),
    Done,
}

use super::order::GroupOrdering;
use super::AggregateExec;

/// Hash based Grouping Aggregator
///
/// # Design Goals
///
/// This structure is designed so that updating the aggregates can be
/// vectorized (done in a tight loop) without allocations. The
/// accumulator state is *not* managed by this operator (e.g in the
/// hash table) and instead is delegated to the individual
/// accumulators which have type specialized inner loops that perform
/// the aggregation.
///
/// # Architecture
///
/// ```text
///
/// stores "group       stores group values,       internally stores aggregate
///    indexes"          in arrow_row format         values, for all groups
///
/// ┌─────────────┐      ┌────────────┐    ┌──────────────┐       ┌──────────────┐
/// │   ┌─────┐   │      │ ┌────────┐ │    │┌────────────┐│       │┌────────────┐│
/// │   │  5  │   │ ┌────┼▶│  "A"   │ │    ││accumulator ││       ││accumulator ││
/// │   ├─────┤   │ │    │ ├────────┤ │    ││     0      ││       ││     N      ││
/// │   │  9  │   │ │    │ │  "Z"   │ │    ││ ┌────────┐ ││       ││ ┌────────┐ ││
/// │   └─────┘   │ │    │ └────────┘ │    ││ │ state  │ ││       ││ │ state  │ ││
/// │     ...     │ │    │            │    ││ │┌─────┐ │ ││  ...  ││ │┌─────┐ │ ││
/// │   ┌─────┐   │ │    │    ...     │    ││ │├─────┤ │ ││       ││ │├─────┤ │ ││
/// │   │  1  │───┼─┘    │            │    ││ │└─────┘ │ ││       ││ │└─────┘ │ ││
/// │   ├─────┤   │      │            │    ││ │        │ ││       ││ │        │ ││
/// │   │ 13  │───┼─┐    │ ┌────────┐ │    ││ │  ...   │ ││       ││ │  ...   │ ││
/// │   └─────┘   │ └────┼▶│  "Q"   │ │    ││ │        │ ││       ││ │        │ ││
/// └─────────────┘      │ └────────┘ │    ││ │┌─────┐ │ ││       ││ │┌─────┐ │ ││
///                      │            │    ││ │└─────┘ │ ││       ││ │└─────┘ │ ││
///                      └────────────┘    ││ └────────┘ ││       ││ └────────┘ ││
///                                        │└────────────┘│       │└────────────┘│
///                                        └──────────────┘       └──────────────┘
///
///       map            group_values                   accumulators
///  (Hash Table)
///
///  ```
///
/// For example, given a query like `COUNT(x), SUM(y) ... GROUP BY z`,
/// [`group_values`] will store the distinct values of `z`. There will
/// be one accumulator for `COUNT(x)`, specialized for the data type
/// of `x` and one accumulator for `SUM(y)`, specialized for the data
/// type of `y`.
///
/// # Description
///
/// The hash table does not store any aggregate state inline. It only
/// stores "group indices", one for each (distinct) group value. The
/// accumulators manage the in-progress aggregate state for each
/// group, and the group values themselves are stored in
/// [`group_values`] at the corresponding group index.
///
/// The accumulator state (e.g partial sums) is managed by and stored
/// by a [`GroupsAccumulator`] accumulator. There is one accumulator
/// per aggregate expression (COUNT, AVG, etc) in the
/// stream. Internally, each `GroupsAccumulator` manages the state for
/// multiple groups, and is passed `group_indexes` during update. Note
/// The accumulator state is not managed by this operator (e.g in the
/// hash table).
///
/// [`group_values`]: Self::group_values
pub(crate) struct GroupedHashAggregateStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    mode: AggregateMode,

    /// Accumulators, one for each `AggregateExpr` in the query
    ///
    /// For example, if the query has aggregates, `SUM(x)`,
    /// `COUNT(y)`, there will be two accumulators, each one
    /// specialized for that particular aggregate and its input types
    accumulators: Vec<Box<dyn GroupsAccumulator>>,

    /// Arguments to pass to each accumulator.
    ///
    /// The arguments in `accumulator[i]` is passed `aggregate_arguments[i]`
    ///
    /// The argument to each accumulator is itself a `Vec` because
    /// some aggregates such as `CORR` can accept more than one
    /// argument.
    aggregate_arguments: Vec<Vec<Arc<dyn PhysicalExpr>>>,

    /// Optional filter expression to evaluate, one for each for
    /// accumulator. If present, only those rows for which the filter
    /// evaluate to true should be included in the aggregate results.
    ///
    /// For example, for an aggregate like `SUM(x FILTER x > 100)`,
    /// the filter expression is  `x > 100`.
    filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,

    /// Converter for the group values
    row_converter: RowConverter,

    /// GROUP BY expressions
    group_by: PhysicalGroupBy,

    /// The memory reservation for this grouping
    reservation: MemoryReservation,

    /// Logically maps group values to a group_index in
    /// [`Self::group_values`] and in each accumulator
    ///
    /// Uses the raw API of hashbrown to avoid actually storing the
    /// keys (group values) in the table
    ///
    /// keys: u64 hashes of the GroupValue
    /// values: (hash, group_index)
    map: RawTable<(u64, usize)>,

    /// The actual group by values, stored in arrow [`Row`] format.
    /// `group_values[i]` holds the group value for group_index `i`.
    ///
    /// The row format is used to compare group keys quickly and store
    /// them efficiently in memory. Quick comparison is especially
    /// important for multi-column group keys.
    ///
    /// [`Row`]: arrow::row::Row
    group_values: Rows,

    /// scratch space for the current input [`RecordBatch`] being
    /// processed. The reason this is a field is so it can be reused
    /// for all input batches, avoiding the need to reallocate Vecs on
    /// each input.
    scratch_space: ScratchSpace,

    /// Tracks if this stream is generating input or output
    exec_state: ExecutionState,

    /// Execution metrics
    baseline_metrics: BaselineMetrics,

    /// Random state for creating hashes
    random_state: RandomState,

    /// max rows in output RecordBatches
    batch_size: usize,

    /// Optional ordering information, that might allow the hash table
    /// to emitted prior to seeing more input
    group_ordering: GroupOrdering,

    /// Have we seen the end of the input
    input_done: bool,
}

impl GroupedHashAggregateStream {
    /// Create a new GroupedHashAggregateStream
    ///
    pub fn new(
        agg: &AggregateExec,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug!("Creating GroupedHashAggregateStream");
        let agg_schema = Arc::clone(&agg.schema);
        let agg_group_by = agg.group_by.clone();
        let agg_filter_expr = agg.filter_expr.clone();

        let batch_size = context.session_config().batch_size();
        let input = agg.input.execute(partition, Arc::clone(&context))?;
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        let timer = baseline_metrics.elapsed_compute().timer();

        let aggregate_exprs = agg.aggr_expr.clone();

        // arguments for each aggregate, one vec of expressions per
        // aggregate
        let aggregate_arguments = aggregates::aggregate_expressions(
            &agg.aggr_expr,
            &agg.mode,
            agg_group_by.expr.len(),
        )?;

        let filter_expressions = match agg.mode {
            AggregateMode::Partial | AggregateMode::Single => agg_filter_expr,
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                vec![None; agg.aggr_expr.len()]
            }
        };

        // Instantiate the accumulators
        let accumulators: Vec<_> = aggregate_exprs
            .iter()
            .map(create_group_accumulator)
            .collect::<Result<_>>()?;

        let group_schema = group_schema(&agg_schema, agg_group_by.expr.len());
        let row_converter = RowConverter::new(
            group_schema
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;

        let name = format!("GroupedHashAggregateStream[{partition}]");
        let reservation = MemoryConsumer::new(name).register(context.memory_pool());
        let map = RawTable::with_capacity(0);
        let group_values = row_converter.empty_rows(0, 0);

        let group_ordering =
            if let Some(aggregation_ordering) = agg.aggregation_ordering.as_ref() {
                GroupOrdering::try_new(&group_schema, aggregation_ordering)?
            } else {
                GroupOrdering::None
            };

        timer.done();

        let exec_state = ExecutionState::ReadingInput;

        Ok(GroupedHashAggregateStream {
            schema: agg_schema,
            input,
            mode: agg.mode,
            accumulators,
            aggregate_arguments,
            filter_expressions,
            row_converter,
            group_by: agg_group_by,
            reservation,
            map,
            group_values,
            scratch_space: ScratchSpace::new(),
            exec_state,
            baseline_metrics,
            random_state: Default::default(),
            batch_size,
            group_ordering,
            input_done: false,
        })
    }
}

/// Create an accumulator for `agg_expr` -- a [`GroupsAccumulator`] if
/// that is supported by the aggregate, or a
/// [`GroupsAccumulatorAdapter`] if not.
fn create_group_accumulator(
    agg_expr: &Arc<dyn AggregateExpr>,
) -> Result<Box<dyn GroupsAccumulator>> {
    if agg_expr.groups_accumulator_supported() {
        agg_expr.create_groups_accumulator()
    } else {
        // Note in the log when the slow path is used
        debug!(
            "Creating GroupsAccumulatorAdapter for {}: {agg_expr:?}",
            agg_expr.name()
        );
        let agg_expr_captured = agg_expr.clone();
        let factory = move || agg_expr_captured.create_accumulator();
        Ok(Box::new(GroupsAccumulatorAdapter::new(factory)))
    }
}

/// Extracts a successful Ok(_) or returns Poll::Ready(Some(Err(e))) with errors
macro_rules! extract_ok {
    ($RES: expr) => {{
        match $RES {
            Ok(v) => v,
            Err(e) => return Poll::Ready(Some(Err(e))),
        }
    }};
}

impl Stream for GroupedHashAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        loop {
            let exec_state = self.exec_state.clone();
            match exec_state {
                ExecutionState::ReadingInput => {
                    match ready!(self.input.poll_next_unpin(cx)) {
                        // new batch to aggregate
                        Some(Ok(batch)) => {
                            let timer = elapsed_compute.timer();
                            // Do the grouping
                            let allocated = self.group_aggregate_batch(batch);

                            // Register allocated memory AFTER we
                            // actually used it, to simplify the whole
                            // accounting and we are OK with
                            // overshooting a bit.
                            //
                            // Also this means we either store the
                            // whole record batch or not.
                            extract_ok!(allocated.and_then(|allocated| {
                                self.reservation.try_grow(allocated)
                            }));

                            // If we can begin emitting rows, do so,
                            // otherwise keep consuming input
                            let to_emit = if self.input_done {
                                Some(EmitTo::All)
                            } else {
                                self.group_ordering.emit_to()
                            };

                            if let Some(to_emit) = to_emit {
                                let batch =
                                    extract_ok!(self.create_batch_from_map(to_emit));
                                self.exec_state = ExecutionState::ProducingOutput(batch);
                            }
                            timer.done();
                        }
                        Some(Err(e)) => {
                            // inner had error, return to caller
                            return Poll::Ready(Some(Err(e)));
                        }
                        None => {
                            // inner is done, emit all rows and switch to producing output
                            self.input_done = true;
                            self.group_ordering.input_done();
                            let timer = elapsed_compute.timer();
                            let batch =
                                extract_ok!(self.create_batch_from_map(EmitTo::All));
                            self.exec_state = ExecutionState::ProducingOutput(batch);
                            timer.done();
                        }
                    }
                }

                ExecutionState::ProducingOutput(batch) => {
                    // slice off a part of the batch, if needed
                    let output_batch = if batch.num_rows() <= self.batch_size {
                        if self.input_done {
                            self.exec_state = ExecutionState::Done;
                        } else {
                            self.exec_state = ExecutionState::ReadingInput
                        }
                        batch
                    } else {
                        // output first batch_size rows
                        let num_remaining = batch.num_rows() - self.batch_size;
                        let remaining = batch.slice(self.batch_size, num_remaining);
                        self.exec_state = ExecutionState::ProducingOutput(remaining);
                        batch.slice(0, self.batch_size)
                    };
                    return Poll::Ready(Some(Ok(
                        output_batch.record_output(&self.baseline_metrics)
                    )));
                }

                ExecutionState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for GroupedHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl GroupedHashAggregateStream {
    /// Calculates the group indices for each input row of
    /// `group_values`.
    ///
    /// At the return of this function,
    /// `self.scratch_space.current_group_indices` has the same number
    /// of entries as each array in `group_values` and holds the
    /// correct group_index for that row.
    ///
    /// This is one of the core hot loops in the algorithm
    fn update_group_state(
        &mut self,
        group_values: &[ArrayRef],
        allocated: &mut usize,
    ) -> Result<()> {
        // track memory used
        let group_values_size_pre = self.group_values.size();
        let scratch_size_pre = self.scratch_space.size();

        // take ownership of the group ordering to satisfy the borrow
        // checker, leaving a "Taken" item in its place
        let mut group_ordering = std::mem::take(&mut self.group_ordering);

        match &mut group_ordering {
            // Tried to run after error. See comments on GroupOrdering::Taken
            GroupOrdering::Taken => panic!("order state was taken"),
            GroupOrdering::None => {
                self.update_group_state_inner(group_values, allocated, |_, _| {})?;
            }
            // ordering columns are a subset of the groups and we
            // need to make a new Rows with just the ordered
            // columns to determine when the next order by key has
            // been emitted
            GroupOrdering::Partial(group_ordering_partial) => {
                // compute the sort key values for each group
                let sort_keys = group_ordering_partial.compute_sort_keys(group_values)?;

                self.update_group_state_inner(
                    group_values,
                    allocated,
                    |group_index, hash| {
                        group_ordering_partial.new_group(
                            group_index,
                            sort_keys.row(group_index),
                            hash,
                        );
                    },
                )?;
            }
            GroupOrdering::Full(group_ordering_full) => {
                self.update_group_state_inner(
                    group_values,
                    allocated,
                    |group_index, hash| {
                        group_ordering_full.new_group(group_index, hash);
                    },
                )?;
            }
        }

        // put the updated group ordering back
        self.group_ordering = group_ordering;

        // account for memory growth in scratch space
        *allocated += self.scratch_space.size();
        *allocated -= scratch_size_pre; // subtract after adding to avoid underflow

        // account for any memory increase used to store group_values
        *allocated += self.group_values.size();
        *allocated -= group_values_size_pre; // subtract after adding to avoid underflow

        Ok(())
    }

    /// See comments on [`Self::update_group_state`]
    ///
    /// Invokes new_group_fn(group_idx, hash, group_row) whenever a new
    /// group is seen (aka inserted into the hash table)
    ///
    /// `group_idx` is the new group's index
    ///
    /// hash: the hash value for this group's row
    ///
    /// `group_row` group by values for the group, in `Row` format
    ///
    /// Note this function is templated so that the dispatch cost of
    /// figuring out what to do is not done in the inner loop
    fn update_group_state_inner<F>(
        &mut self,
        group_values: &[ArrayRef],
        allocated: &mut usize,
        mut updated_fn: F,
    ) -> Result<()>
    where
        F: FnMut(usize, u64),
    {
        // Convert the group keys into the row format
        // Avoid reallocation when https://github.com/apache/arrow-rs/issues/4479 is available
        let group_rows = self.row_converter.convert_columns(group_values)?;
        let n_rows = group_rows.num_rows();

        // tracks to which group each of the input rows belongs
        let group_indices = &mut self.scratch_space.current_group_indices;
        group_indices.clear();

        // 1.1 Calculate the group keys for the group values
        let batch_hashes = &mut self.scratch_space.hashes_buffer;
        batch_hashes.clear();
        batch_hashes.resize(n_rows, 0);
        create_hashes(group_values, &self.random_state, batch_hashes)?;

        for (row, &hash) in batch_hashes.iter().enumerate() {
            let entry = self.map.get_mut(hash, |(_hash, group_idx)| {
                // verify that a group that we are inserting with hash is
                // actually the same key value as the group in
                // existing_idx  (aka group_values @ row)
                group_rows.row(row) == self.group_values.row(*group_idx)
            });

            let group_idx = match entry {
                // Existing group_index for this group value
                Some((_hash, group_idx)) => *group_idx,
                //  1.2 Need to create new entry for the group
                None => {
                    // Assign a new group_index, and save group_value
                    let group_idx = self.group_values.num_rows();
                    // Do any special update
                    (updated_fn)(group_idx, hash);
                    self.group_values.push(group_rows.row(row));

                    // for hasher function, use precomputed hash value
                    self.map.insert_accounted(
                        (hash, group_idx),
                        |(hash, _group_index)| *hash,
                        allocated,
                    );
                    group_idx
                }
            };
            group_indices.push(group_idx);
        }
        Ok(())
    }

    /// Perform group-by aggregation for the given [`RecordBatch`].
    ///
    /// If successful, returns the additional amount of memory, in
    /// bytes, that were allocated during this process.
    fn group_aggregate_batch(&mut self, batch: RecordBatch) -> Result<usize> {
        // Evaluate the grouping expressions
        let group_by_values = evaluate_group_by(&self.group_by, &batch)?;

        // Keep track of memory allocated:
        let mut allocated = 0usize;

        // Evaluate the aggregation expressions.
        let input_values = evaluate_many(&self.aggregate_arguments, &batch)?;

        // Evaluate the filter expressions, if any, against the inputs
        let filter_values = evaluate_optional(&self.filter_expressions, &batch)?;

        let row_converter_size_pre = self.row_converter.size();

        for group_values in &group_by_values {
            // calculate the group indices for each input row
            self.update_group_state(group_values, &mut allocated)?;
            let group_indices = &self.scratch_space.current_group_indices;

            // Gather the inputs to call the actual accumulator
            let t = self
                .accumulators
                .iter_mut()
                .zip(input_values.iter())
                .zip(filter_values.iter());

            let total_num_groups = self.group_values.num_rows();

            for ((acc, values), opt_filter) in t {
                let acc_size_pre = acc.size();
                let opt_filter = opt_filter.as_ref().map(|filter| filter.as_boolean());

                // Call the appropriate method on each aggregator with
                // the entire input row and the relevant group indexes
                match self.mode {
                    AggregateMode::Partial | AggregateMode::Single => {
                        acc.update_batch(
                            values,
                            group_indices,
                            opt_filter,
                            total_num_groups,
                        )?;
                    }
                    AggregateMode::FinalPartitioned | AggregateMode::Final => {
                        // if aggregation is over intermediate states,
                        // use merge
                        acc.merge_batch(
                            values,
                            group_indices,
                            opt_filter,
                            total_num_groups,
                        )?;
                    }
                }

                allocated += acc.size();
                allocated -= acc_size_pre;
            }
        }
        allocated += self.row_converter.size();
        allocated -= row_converter_size_pre;

        Ok(allocated)
    }

    /// Create an output RecordBatch with the group keys and
    /// accumulator states/values specified in emit_to
    fn create_batch_from_map(&mut self, emit_to: EmitTo) -> Result<RecordBatch> {
        if self.group_values.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(self.schema()));
        }

        let output = self.build_output(emit_to)?;
        self.remove_emitted(emit_to)?;
        let batch = RecordBatch::try_new(self.schema(), output)?;
        Ok(batch)
    }

    /// Creates the output:
    ///
    /// (group value, group value 2, ... agg value 1, agg value 2, ...)
    fn build_output(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        // First output rows are the groups
        let mut output: Vec<ArrayRef> = match emit_to {
            EmitTo::All => {
                let groups_rows = self.group_values.iter();
                self.row_converter.convert_rows(groups_rows)?
            }
            EmitTo::First(n) => {
                let groups_rows = self.group_values.iter().take(n);
                self.row_converter.convert_rows(groups_rows)?
            }
        };

        // Next output each aggregate value, from the accumulators
        for acc in self.accumulators.iter_mut() {
            use AggregateMode::*;
            match self.mode {
                Partial => output.extend(acc.state(emit_to)?),
                Final | FinalPartitioned | Single => output.push(acc.evaluate(emit_to)?),
            }
        }

        Ok(output)
    }

    /// Removes the first `n` groups, and adjust all
    /// group_indices appropriately
    fn remove_emitted(&mut self, emit_to: EmitTo) -> Result<()> {
        match emit_to {
            EmitTo::All => {
                // Eventually when we allow early dumping of the hash
                // table (TODO write ticket) we can clear out all the
                // state (hash table, and groups)
                //self.map.clear();
                Ok(())
            }
            EmitTo::First(n) => {
                // Clear out first n group keys
                // TODO file some ticket in arrow-rs to make this more efficent?
                let mut new_group_values = self.row_converter.empty_rows(0, 0);
                for row in self.group_values.iter().skip(n) {
                    new_group_values.push(row);
                }
                std::mem::swap(&mut new_group_values, &mut self.group_values);

                // rebuild hash table (TODO we could just remove the
                // entries for each group that was emitted rather than
                // rebuilding the whole thing

                let hashes = self.group_ordering.remove_groups(n);
                assert_eq!(hashes.len(), self.group_values.num_rows());
                self.map.clear();
                for (idx, &hash) in hashes.iter().enumerate() {
                    self.map.insert(hash, (hash, idx), |(hash, _)| *hash);
                }
                Ok(())
            }
        }
    }
}

/// Holds structures used for the current input [`RecordBatch`] being
/// processed. Reused across batches here to avoid reallocations
#[derive(Debug, Default)]
struct ScratchSpace {
    /// scratch space for the current input [`RecordBatch`] being
    /// processed. Reused across batches here to avoid reallocations
    current_group_indices: Vec<usize>,
    // buffer to be reused to store hashes
    hashes_buffer: Vec<u64>,
}

impl ScratchSpace {
    fn new() -> Self {
        Default::default()
    }

    /// Return the amount of memory alocated by this structure in bytes
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.current_group_indices.allocated_size()
            + self.hashes_buffer.allocated_size()
    }
}
