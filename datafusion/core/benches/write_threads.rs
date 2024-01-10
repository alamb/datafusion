use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use criterion::{BatchSize, BenchmarkId};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaBuilder, SchemaRef};
use datafusion::catalog::TableReference;
use datafusion::common::Result;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::Arc;
use tokio::runtime::Builder;

fn register_csv(
    ctx: &SessionContext,
    table_name: &str,
    schema: SchemaRef,
    table_path: impl AsRef<str>,
) -> Result<()> {
    let file_format = CsvFormat::default()
        .with_has_header(false)
        .with_delimiter(b'|');

    let options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".tbl")
        .with_target_partitions(ctx.copied_config().batch_size());
    let table_path = ListingTableUrl::parse(table_path)?;

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        .with_schema(schema);
    let table = ListingTable::try_new(config)?;
    ctx.register_table(
        TableReference::Bare {
            table: table_name.into(),
        },
        Arc::new(table),
    )?;
    Ok(())
}

async fn execute_query(ctx: SessionContext, sql: String) {
    ctx.sql(&sql).await.unwrap().collect().await.unwrap();
}

fn get_tpch_table_schema(table: &str) -> Schema {
    match table {
        "orders" => Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_orderpriority", DataType::Utf8, false),
            Field::new("o_clerk", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int32, false),
            Field::new("o_comment", DataType::Utf8, false),
        ]),

        "lineitem" => Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Decimal128(15, 2), false),
            Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
            Field::new("l_discount", DataType::Decimal128(15, 2), false),
            Field::new("l_tax", DataType::Decimal128(15, 2), false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Date32, false),
            Field::new("l_commitdate", DataType::Date32, false),
            Field::new("l_receiptdate", DataType::Date32, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, false),
        ]),
        _ => unimplemented!("Table: {}", table),
    }
}

pub fn get_tbl_tpch_table_schema(table: &str) -> Schema {
    let mut schema = SchemaBuilder::from(get_tpch_table_schema(table).fields);
    schema.push(Field::new("__placeholder", DataType::Utf8, true));
    schema.finish()
}

fn delete_file(path: &str) {
    std::fs::remove_file(path).expect("Failed to delete file");
}

fn from_elem_reading(c: &mut Criterion) {
    let batch_sizes = [100, 1000, 3000];
    let target_partitions = [1, 4];
    let worker_threads = [1, 4];
    let mut group = c.benchmark_group("parameter group");
    for batch_size in batch_sizes.iter() {
        for target_partition in target_partitions {
            for worker_thread in worker_threads {
                group.bench_with_input(BenchmarkId::new(
                    format!("sink_bs{}_tp{}", batch_size, target_partition),
                    worker_thread,
                ), &(batch_size, target_partition, worker_thread), |b, &(batch_size, target_partition, worker_thread)| {
                    let rt = Builder::new_multi_thread()
                        .worker_threads(worker_thread)
                        .build()
                        .unwrap();
                    b.to_async(rt).iter_batched(
                        || {
                            let csv_file = tempfile::Builder::new()
                                .prefix("foo")
                                .suffix(".csv")
                                .tempfile()
                                .unwrap();
                            let path = csv_file.path().to_str().unwrap().to_string();
                            let config = SessionConfig::new()
                                .with_coalesce_batches(false)
                                .with_batch_size(*batch_size)
                                .with_target_partitions(target_partition);
                            let ctx = SessionContext::new_with_config(config);
                            let lineitem = Arc::new(get_tbl_tpch_table_schema("lineitem"));
                            register_csv(&ctx, "lineitem", lineitem, "/Users/andrewlamb/Downloads/tpch_sf0.1/lineitem.tbl").unwrap();
                            let orders = Arc::new(get_tbl_tpch_table_schema("orders"));
                            register_csv(&ctx, "orders", orders, "/Users/andrewlamb/Downloads/tpch_sf0.1/orders.tbl").unwrap();
                            let sql = format!(
                                "COPY (SELECT * FROM
                                lineitem, orders
                                where
                                l_orderkey = o_orderkey)
                                to '{path}' (format csv);"
                            );
                            (path, ctx, sql)
                        },
                        |(path, clone_ctx, sql)| async move {
                            execute_query(clone_ctx, sql).await;
                            delete_file(&path);
                        },
                        BatchSize::LargeInput,
                    );
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, from_elem_reading);
criterion_main!(benches);