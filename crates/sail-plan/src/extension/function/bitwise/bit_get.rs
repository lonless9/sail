use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, GenericBinaryArray, Int8Builder, PrimitiveArray,
};
use datafusion::arrow::datatypes::{DataType, Int16Type, Int32Type, Int64Type, Int8Type};
use datafusion::common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::functions_utils::make_scalar_function;

#[derive(Debug)]
pub struct BitGet {
    signature: Signature,
}

impl Default for BitGet {
    fn default() -> Self {
        Self::new()
    }
}

impl BitGet {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BitGet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bit_get"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!("bit_get expects two arguments");
        }

        let expr_type = &arg_types[0];
        if !matches!(
            expr_type,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Binary
        ) {
            return exec_err!("bit_get does not support type for first argument: {expr_type:?}");
        }

        let pos_type = &arg_types[1];
        if !pos_type.is_integer() {
            return exec_err!("bit_get position must be an integer, but got {pos_type:?}");
        }

        // Coerce position to Int32 as that's what the array implementation expects.
        Ok(vec![expr_type.clone(), DataType::Int32])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(bit_get_inner, vec![])(&args)
    }
}

fn bit_get_int(n: i64, pos: i64) -> i8 {
    if pos >= 0 && pos < 64 {
        ((n >> pos) & 1) as i8
    } else {
        0
    }
}

fn bit_get_binary(bytes: &[u8], pos: i64) -> i8 {
    if pos < 0 {
        return 0;
    }
    let pos = pos as usize;
    let byte_index = pos / 8;
    if byte_index >= bytes.len() {
        return 0;
    }

    let bit_index = 7 - (pos % 8);
    ((bytes[byte_index] >> bit_index) & 1) as i8
}

fn bit_get_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("bit_get expects two arguments");
    }

    let expr_arr = &args[0];
    let pos_arr = &args[1];

    let len = expr_arr.len();
    let mut builder = Int8Builder::with_capacity(len);
    let pos_arr_i32 = pos_arr.as_primitive::<Int32Type>();

    for i in 0..len {
        if expr_arr.is_null(i) || pos_arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let pos = pos_arr_i32.value(i) as i64;
        let val = match expr_arr.data_type() {
            DataType::Int8 => bit_get_int(expr_arr.as_primitive::<Int8Type>().value(i) as i64, pos),
            DataType::Int16 => {
                bit_get_int(expr_arr.as_primitive::<Int16Type>().value(i) as i64, pos)
            }
            DataType::Int32 => {
                bit_get_int(expr_arr.as_primitive::<Int32Type>().value(i) as i64, pos)
            }
            DataType::Int64 => bit_get_int(expr_arr.as_primitive::<Int64Type>().value(i), pos),
            DataType::Binary => bit_get_binary(expr_arr.as_binary::<i32>().value(i), pos),
            other => return exec_err!("bit_get does not support type: {other:?}"),
        };
        builder.append_value(val);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}
