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

//! Serde code for logical plans and expressions.

pub mod bytes;
pub mod common;
pub mod generated;
pub mod logical_plan;
pub mod physical_plan;

pub use generated::datafusion as protobuf;

#[cfg(doctest)]
doc_comment::doctest!("../README.md", readme_example_test);


#[cfg(test)]
mod test {
    use datafusion::{sql::{sqlparser::{parser::Parser, dialect::GenericDialect, ast::Statement}}, prelude::SessionContext};

    use crate::bytes::{logical_plan_to_bytes, logical_plan_from_bytes};


    /// Ensures that the plans for the TPCH queries can be correctly serialized
    #[tokio::test]
    async fn  tpch_roundtrip() {
        // Read in the entire SQL as a string
        let tpch_data = std::fs::read_to_string("./testdata/tpch.sql").unwrap();

        println!("Parsing tpch data...");

        let statements = Parser::new(&GenericDialect{})
            .try_with_sql(&tpch_data)
            .expect("Error tokenizing")
            .parse_statements()
            .expect("Error parsing");

        let ctx = SessionContext::new();

        // For each statement, run it against a context.
        for statement in statements {
            // Special case the querie to serde them
            match statement {
                Statement::Query(query) => {
                    let query = query.to_string();

                    // Ensure the plan is the same after serialization
                    let plan = ctx.sql(&query).await.unwrap();
                    let plan = plan.into_optimized_plan().unwrap();
                    let bytes = logical_plan_to_bytes(&plan).unwrap();
                    let plan2 = logical_plan_from_bytes(&bytes, &ctx).unwrap();
                    let plan_formatted = format!("{}", plan.display_indent());
                    let plan2_formatted = format!("{}", plan2.display_indent());
                    assert_eq!(plan_formatted, plan2_formatted);

                },
                statement => {
                    let statement = statement.to_string();
                    println!("Running {statement}...");
                    ctx.sql(&statement.to_string()).await.expect("error during execution");
                }
            }

        }


    }

}
