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
}

impl Printer {
    /// create a new `Printer`, that will print the total execution time since
    /// creation.
    pub fn new(options: PrintOptions) -> Self {
        Self {
            start_time: Instant::now(),
            print_options: options,
        }
    }

    /// Ignore any max rows settings and print all rows
    pub fn print_all_rows(&mut self) {
        self.print_options.maxrows = MaxRows::Unlimited;
    }

    /// If the printer is `OutputFormat::Automatic` resolves the format based on
    /// on the details of the plan
    ///
    /// if `is_streaming` is true, the plan is streaming (may never actually
    /// end, but generates output incrementatlly) --> NDJson. Otherwise buffer
    /// all the records and print them as a table with nicely formatted output
    pub fn resolve_output_format(&mut self, is_streaming: bool) {
        if self.print_options.format == PrintFormat::Automatic {
            if is_streaming {
                self.print_options.format = PrintFormat::Json;
            } else {
                self.print_options.format = PrintFormat::Table;
            }
        }
    }

    /// Prints a `RecordBatchStream` according to PrintOptions
    pub async fn print(self, mut stream: SendableRecordBatchStream) -> Result<()> {
        let Self {
            start_time,
            print_options,
        } = self;

        let mut output_state = OutputState::new(print_options);

        while let Some(batch) = stream.next().await {
            output_state.print(batch?)?;
        }

        output_state.done()
    }
}

/// State for printing
struct OutputState {
    print_options: PrintOptions,
}

impl OutputState {
    fn new(print_options: PrintOptions) -> Self {
        Self { print_options }
    }

    /// Print an individual batch
    pub fn print(&mut self, batch: RecordBatch) -> Result<()> {
        todo!();
        //self.print_options.format.print_batches()
    }

    /// Complete printing
    pub fn done(mut self) -> Result<()> {
        todo!();
        //self.print_options.format.done()
    }
}

/// Per format state, if any
enum PrintFormatState {}
