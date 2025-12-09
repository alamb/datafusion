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

use std::str::FromStr;

use arrow::compute::kernels::cast_utils::IntervalUnit;
use datafusion_common::{internal_err, tree_node::Transformed, Result, ScalarValue};
use datafusion_expr::{
    and, expr::ScalarFunction, lit, or, simplify::SimplifyInfo, BinaryExpr, Expr,
    Operator, ScalarUDFImpl,
};
use datafusion_expr_common::interval_arithmetic::Interval;

/// Rewrites a binary expression using its "preimage"
///
/// Specifically it rewrites expressions of the form `<expr> OP x` (e.g. `<expr> =
/// x`) where `<expr>` is known to have a pre-image (aka the entire single
/// range for which it is valid)
///
/// This rewrite is described in the [ClickHouse Paper] and is particularly
/// useful for simplifying expressions `date_part` or equivalent functions. The
/// idea is that if you have an expression like `date_part(YEAR, k) = 2024` and you
/// can find a [preimage] for `date_part(YEAR, k)`, which is the range of dates
/// covering the entire year of 2024. Thus, you can rewrite the expression to `k
/// >= '2024-01-01' AND k < '2025-01-01' which is often more optimizable.
///
/// [ClickHouse Paper]:  https://www.vldb.org/pvldb/vol17/p3731-schulze.pdf
/// [preimage]: https://en.wikipedia.org/wiki/Image_(mathematics)#Inverse_image
///
pub(super) fn rewrite_with_preimage(
    info: &dyn SimplifyInfo,
    preimage_interval: Interval,
    op: Operator,
    expr: Box<Expr>,
) -> Result<Transformed<Expr>> {
    let (lower, upper) = preimage_interval.into_bounds();
    let (lower, upper) = (lit(lower), lit(upper));

    let rewritten_expr = match op {
        // <expr> < x   ==>  <expr> < upper
        // <expr> >= x  ==>  <expr> >= lower
        Operator::Lt | Operator::GtEq => Expr::BinaryExpr(BinaryExpr {
            left: expr,
            op,
            right: Box::new(lower),
        }),
        // <expr> > x   ==>  <expr> >= lower
        Operator::Gt => Expr::BinaryExpr(BinaryExpr {
            left: expr,
            op: Operator::GtEq,
            right: Box::new(upper),
        }),
        Operator::LtEq => Expr::BinaryExpr(BinaryExpr {
            left: expr,
            op: Operator::Lt,
            right: Box::new(upper),
        }),
        Operator::Eq => and(
            Expr::BinaryExpr(BinaryExpr {
                left: expr.clone(),
                op: Operator::GtEq,
                right: Box::new(lower),
            }),
            Expr::BinaryExpr(BinaryExpr {
                left: expr,
                op: Operator::Lt,
                right: Box::new(upper),
            }),
        ),
        Operator::NotEq => or(
            Expr::BinaryExpr(BinaryExpr {
                left: expr.clone(),
                op: Operator::Lt,
                right: Box::new(lower),
            }),
            Expr::BinaryExpr(BinaryExpr {
                left: expr,
                op: Operator::GtEq,
                right: Box::new(upper),
            }),
        ),
        Operator::IsDistinctFrom => or(
            or(
                Expr::BinaryExpr(BinaryExpr {
                    left: expr.clone(),
                    op: Operator::Lt,
                    right: Box::new(lower.clone()),
                }),
                Expr::BinaryExpr(BinaryExpr {
                    left: expr.clone(),
                    op: Operator::GtEq,
                    right: Box::new(upper),
                }),
            ),
            or(
                and(expr.clone().is_null(), lower.clone().is_not_null()),
                and(expr.is_not_null(), lower.is_null()),
            ),
        ),
        Operator::IsNotDistinctFrom => or(
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(expr.clone().is_null()),
                op: Operator::And,
                right: Box::new(lower.clone().is_null()),
            }),
            and(
                Expr::BinaryExpr(BinaryExpr {
                    left: expr.clone(),
                    op: Operator::GtEq,
                    right: Box::new(lower.clone()),
                }),
                Expr::BinaryExpr(BinaryExpr {
                    left: expr,
                    op: Operator::Lt,
                    right: Box::new(upper),
                }),
            ),
        ),
        _ => return internal_err!("Expect comparison operators"),
    };
    Ok(Transformed::yes(rewritten_expr))
}

#[cfg(test)]
mod tests {
    use crate::simplify_expressions::ExprSimplifier;
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use datafusion_common::{DFSchema, DFSchemaRef, ScalarValue};
    use datafusion_expr::expr_fn::col;
    use datafusion_expr::or;
    use datafusion_expr::{
        and, execution_props::ExecutionProps, lit, simplify::SimplifyContext, Expr,
    };

    use std::{collections::HashMap, sync::Arc};

    #[test]
    fn test_preimage_date_part_date32_eq() {
        let schema = expr_test_schema();
        // date_part(c1, DatePart::Year) = 2024 -> c1 >= 2024-01-01 AND c1 < 2025-01-01
        let expr_lt = expr_fn::date_part(lit("year"), col("date32")).eq(lit(2024i32));
        let expected = and(
            col("date32").gt_eq(lit(ScalarValue::Date32(Some(19723)))),
            col("date32").lt(lit(ScalarValue::Date32(Some(20089)))),
        );
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_date64_not_eq() {
        let schema = expr_test_schema();
        // date_part(c1, DatePart::Year) <> 2024 -> c1 < 2024-01-01 AND c1 >= 2025-01-01
        let expr_lt = expr_fn::date_part(lit("year"), col("date64")).not_eq(lit(2024i32));
        let expected = or(
            col("date64").lt(lit(ScalarValue::Date64(Some(19723 * 86_400_000)))),
            col("date64").gt_eq(lit(ScalarValue::Date64(Some(20089 * 86_400_000)))),
        );
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_timestamp_nano_lt() {
        let schema = expr_test_schema();
        let expr_lt =
            expr_fn::date_part(lit("year"), col("ts_nano_none")).lt(lit(2024i32));
        let expected = col("ts_nano_none").lt(lit(ScalarValue::TimestampNanosecond(
            Some(19723 * 86_400_000_000_000),
            None,
        )));
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_timestamp_nano_utc_gt() {
        let schema = expr_test_schema();
        let expr_lt =
            expr_fn::date_part(lit("year"), col("ts_nano_utc")).gt(lit(2024i32));
        let expected = col("ts_nano_utc").gt_eq(lit(ScalarValue::TimestampNanosecond(
            Some(20089 * 86_400_000_000_000),
            None,
        )));
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_timestamp_sec_est_gt_eq() {
        let schema = expr_test_schema();
        let expr_lt =
            expr_fn::date_part(lit("year"), col("ts_sec_est")).gt_eq(lit(2024i32));
        let expected = col("ts_sec_est").gt_eq(lit(ScalarValue::TimestampSecond(
            Some(19723 * 86_400),
            None,
        )));
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_timestamp_sec_est_lt_eq() {
        let schema = expr_test_schema();
        let expr_lt =
            expr_fn::date_part(lit("year"), col("ts_mic_pt")).lt_eq(lit(2024i32));
        let expected = col("ts_mic_pt").lt(lit(ScalarValue::TimestampMicrosecond(
            Some(20089 * 86_400_000_000),
            None,
        )));
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_timestamp_nano_lt_swap() {
        let schema = expr_test_schema();
        let expr_lt =
            lit(2024i32).gt(expr_fn::date_part(lit("year"), col("ts_nano_none")));
        let expected = col("ts_nano_none").lt(lit(ScalarValue::TimestampNanosecond(
            Some(19723 * 86_400_000_000_000),
            None,
        )));
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    // Should not simplify
    fn test_preimage_date_part_not_year_date32_eq() {
        let schema = expr_test_schema();
        // date_part(c1, DatePart::Year) = 2024 -> c1 >= 2024-01-01 AND c1 < 2025-01-01
        let expr_lt = expr_fn::date_part(lit("month"), col("date32")).eq(lit(1i32));
        let expected = expr_fn::date_part(lit("month"), col("date32")).eq(lit(1i32));
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    fn optimize_test(expr: Expr, schema: &DFSchemaRef) -> Expr {
        let props = ExecutionProps::new();
        let simplifier = ExprSimplifier::new(
            SimplifyContext::new(&props).with_schema(Arc::clone(schema)),
        );

        simplifier.simplify(expr).unwrap()
    }

    fn expr_test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::from_unqualified_fields(
                vec![
                    Field::new("date32", DataType::Date32, false),
                    Field::new("date64", DataType::Date64, false),
                    Field::new("ts_nano_none", timestamp_nano_none_type(), false),
                    Field::new("ts_nano_utc", timestamp_nano_utc_type(), false),
                    Field::new("ts_sec_est", timestamp_sec_est_type(), false),
                    Field::new("ts_mic_pt", timestamp_mic_pt_type(), false),
                ]
                .into(),
                HashMap::new(),
            )
            .unwrap(),
        )
    }

    fn timestamp_nano_none_type() -> DataType {
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    }

    // this is the type that now() returns
    fn timestamp_nano_utc_type() -> DataType {
        let utc = Some("+0:00".into());
        DataType::Timestamp(TimeUnit::Nanosecond, utc)
    }

    fn timestamp_sec_est_type() -> DataType {
        let est = Some("-5:00".into());
        DataType::Timestamp(TimeUnit::Second, est)
    }

    fn timestamp_mic_pt_type() -> DataType {
        let pt = Some("-8::00".into());
        DataType::Timestamp(TimeUnit::Microsecond, pt)
    }
}
