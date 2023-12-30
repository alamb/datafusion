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

use crate::command::OutputFormat;
use crate::print_format::PrintFormat;
use crate::print_options::{MaxRows, PrintOptions};
use arrow::array::RecordBatch;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_common::Result;
use futures::StreamExt;
use std::time::Instant;

/// Prints RecordBatches to stdout based on PrintOptions
pub struct Printer {
    start_time: Instant,
    print_options: PrintOptions,
    streaming: bool,
}

impl Printer {
    /// create a new `Printer`, that can print a stream of RecordBatches and the
    /// total execution time since creation.
    pub fn new(options: PrintOptions) -> Self {
        Self {
            start_time: Instant::now(),
            print_options: options,
            streaming: false,
        }
    }

    /// Ignore any max rows settings and print all rows
    pub fn print_all_rows(&mut self) {
        self.print_options.maxrows = MaxRows::Unlimited;
    }

    /// Note that the stream may "not end" (aka is streaming) in the sense that
    /// it may never actually end, but generates output incrementally
    pub fn is_streaming(&mut self, streaming: bool) {
        self.streaming = streaming;
    }

    /// Prints a `RecordBatchStream` according to PrintOptions
    pub async fn print(self, mut stream: SendableRecordBatchStream) -> Result<()> {
        let Self {
            start_time,
            print_options: PrintOptions {
                format,
                quiet,
                maxrows,
            },
            streaming,
        } = self;

        let formatter = match format {
            //PrintFormat::Csv => Box::new(CsvFormatter::new(',')),
            //PrintFormat::Tsv => Box::new(CsvFormatter::new('\t')),

            //  if `is_streaming` is true, the plan may never actually
            //  end, but generates output incrementally) --> NDJson.
            PrintFormat::Automatic if streaming => Box::new(NdJsonFormatter::new()),
            // Otherwise buffer // all the records and print them as a table with nicely formatted output
            PrintFormat::Automatic => Box::new(TableFormatter::new(maxrows)),
            _ => todo!()
        };

        let mut output_state = OutputState::new(
            start_time,
            quiet,
            formatter,
        );

        while let Some(batch) = stream.next().await {
            output_state.print(batch?)?;
        }

        output_state.done()
    }
}

/// State for printing
struct OutputState {
    start_time: Instant,
    quiet: bool,
    /// Total rows that have been printed
    row_count: usize,
    formatter: Box<dyn OutputFormatter>,
}

impl OutputState {
    fn new(
        start_time: Instant,
        quiet: bool,
        formatter: Box<dyn OutputFormatter>,
    ) -> Self {

        Self {
            start_time,
            quiet,
            row_count: 0,
            formatter,
        }

    }

    /// Print an individual batch
    pub fn print(&mut self, batch: RecordBatch) -> Result<()> {
        self.row_count += batch.num_rows();
        todo!();
        //self.print_options.format.print_batches()
    }

    /// Complete printing
    pub fn done(mut self) -> Result<()> {

        // print the timing string if not in quiet mode
        if self.quiet {
            return Ok(());
        }

        let row_count = self.row_count;
        let row_word = if row_count == 1 { "row" } else { "rows" };
        let nrows_shown_msg = self.formatter.nrows_shown_msg();
        let elapsed = self.start_time.elapsed().as_secs_f64();

        println!(
            "{row_count} {row_word} in set{nrows_shown_msg}. Query took {elapsed:.3} seconds.\n",
        );
        Ok(())

    }
}

/// Per format state, if any
trait OutputFormatter {
    /// Print a batch
    fn print_batch(&mut self, batch: RecordBatch) -> Result<()>;

    /// Return a string describing the number of rows shown (if any)
    /// This is used to show how many rows were skipped due to maxrows.
    ///
    /// Returns "" by default
    fn nrows_shown_msg(&self) -> String { String::new() }

    /// Complete printing
    fn done(&mut self) -> Result<()>;
}
