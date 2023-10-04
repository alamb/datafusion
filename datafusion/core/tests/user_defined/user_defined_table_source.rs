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

//! Tests for [`TableSource`]



use async_trait::async_trait;
use std::any::Any;
use std::sync::Arc;
use arrow::compute::filter_record_batch;
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::prelude::SessionContext;
use datafusion_common::{assert_batches_eq, Result};
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::create_physical_expr;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::memory::MemoryExec;


#[tokio::test]
// User defined source that supports filter pushdown
async fn filter_pushdown() -> Result<()> {
    let ctx = SessionContext::new();

    let a = Int32Array::from(vec![10,20,30,20,60,30,70,10]);
    let b = Int32Array::from(vec![1,2,3,2,6,3,7,1]);
    let batch = RecordBatch::try_from_iter(vec![
        ("a", Arc::new(a) as _),
        ("b", Arc::new(b) as _),
    ]).unwrap();

    let source = Arc::new(FilteringTableSource::new(batch)) as _;
    ctx.register_table("t", source).unwrap();

    let results = ctx.sql("select t.a from t where t.b > 5")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let expected = vec!["foo"];

    assert_batches_eq!(expected, &results);
    Ok(())
}

/// A table source that evaluates filters
pub struct FilteringTableSource {
    batch: RecordBatch,
}

impl FilteringTableSource {
    fn new(batch: RecordBatch) -> Self {
        Self {
            batch
        }
    }
}

#[async_trait]
impl TableProvider for FilteringTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(&self, state: &SessionState, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {
        //
/*        let filters = filters.iter()
            .map(|filter| create_physical_expr()
            )
        filter_record_batch
  */
        let partitions = vec![vec![self.batch.clone()]];
        let exec = Arc::new(MemoryExec::try_new(&partitions, self.schema().clone(), projection.cloned())?);
        Ok(exec)
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        // supports all filters
        let support: Vec<_> = filters.iter().map(|_| TableProviderFilterPushDown::Exact).collect();
        Ok(support)
    }
}

struct UserExec {
    batch: RecordBatch,
}