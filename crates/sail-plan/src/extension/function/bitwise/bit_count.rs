use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int32Array};
use datafusion::arrow::datatypes::{DataType, Int16Type, Int32Type, Int64Type, Int8Type};
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::functions_utils::make_scalar_function;

#[derive(Debug)]
pub struct BitCount {
    signature: Signature,
}

impl Default for BitCount {
    fn default() -> Self {
        Self::new()
    }
}

impl BitCount {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BitCount {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bit_count"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(bit_count_inner, vec![])(&args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return exec_err!(
                "`bit_count` function requires 1 argument, got {}",
                arg_types.len()
            );
        }
        match arg_types[0] {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Binary => Ok(vec![arg_types[0].clone()]),
            _ => exec_err!("The `bit_count` function can only accept integer or binary."),
        }
    }
}

fn bit_count_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("bit_count expects one argument");
    }
    let array = &args[0];
    let result: Int32Array = match array.data_type() {
        DataType::Int8 => array
            .as_primitive::<Int8Type>()
            .unary(|v| v.count_ones() as i32),
        DataType::Int16 => array
            .as_primitive::<Int16Type>()
            .unary(|v| v.count_ones() as i32),
        DataType::Int32 => array
            .as_primitive::<Int32Type>()
            .unary(|v| v.count_ones() as i32),
        DataType::Int64 => array
            .as_primitive::<Int64Type>()
            .unary(|v| v.count_ones() as i32),
        DataType::Binary => array
            .as_binary::<i32>()
            .iter()
            .map(|val| val.map(|bytes| bytes.iter().map(|b| b.count_ones()).sum::<u32>() as i32))
            .collect(),
        other => return exec_err!("bit_count does not support type: {other:?}"),
    };
    Ok(Arc::new(result))
}
