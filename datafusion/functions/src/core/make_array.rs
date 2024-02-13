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

//! make_array function
use arrow::{
    array::{Array, ArrayRef, BinaryArray, OffsetSizeTrait, StringArray},
    datatypes::DataType,
};
use base64::{engine::general_purpose, Engine as _};
use datafusion_common::ScalarValue;
use datafusion_common::{
    cast::{as_generic_binary_array, as_generic_string_array},
    internal_err, not_impl_err, plan_err,
};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use std::sync::Arc;
use std::{fmt, str::FromStr};

use datafusion_expr::TypeSignature::*;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;

#[derive(Debug)]
pub(super) struct MakeArray {
    signature: Signature,
}

impl MakeArray {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8, Utf8]),
                    Exact(vec![LargeUtf8, Utf8]),
                    Exact(vec![Binary, Utf8]),
                    Exact(vec![LargeBinary, Utf8]),
                ],
                Volatility::Immutable,
            )
        }
    }
}

impl ScalarUDFImpl for MakeArray {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "encode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;

        Ok(match arg_types[0] {
            Utf8 => Utf8,
            LargeUtf8 => LargeUtf8,
            Binary => Utf8,
            LargeBinary => LargeUtf8,
            Null => Null,
            _ => {
                return plan_err!("The encode function can only accept utf8 or binary.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        todo!()
    }
}
