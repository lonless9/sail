use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use datafusion::arrow::datatypes::{DataType, Int16Type, Int32Type, Int64Type, Int8Type};
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::functions_utils::make_scalar_function;

#[derive(Debug)]
pub struct ShiftRightUnsigned {
    signature: Signature,
}

impl Default for ShiftRightUnsigned {
    fn default() -> Self {
        Self::new()
    }
}

impl ShiftRightUnsigned {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ShiftRightUnsigned {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "shiftrightunsigned"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Int8 | DataType::Int16 => Ok(DataType::Int32),
            DataType::Int32 => Ok(DataType::Int32),
            DataType::Int64 => Ok(DataType::Int64),
            other => {
                exec_err!("shiftrightunsigned does not support type for first argument: {other:?}")
            }
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!("shiftrightunsigned expects two arguments");
        }

        let expr_type = &arg_types[0];
        if !matches!(
            expr_type,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        ) {
            return exec_err!(
                "shiftrightunsigned does not support type for first argument: {expr_type:?}"
            );
        }

        let shift_amount_type = &arg_types[1];
        if !shift_amount_type.is_integer() {
            return exec_err!(
                "shiftrightunsigned shift amount must be an integer, but got {shift_amount_type:?}"
            );
        }

        Ok(vec![expr_type.clone(), DataType::Int32])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(shift_right_unsigned_inner, vec![])(&args)
    }
}

fn shift_right_unsigned_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("shiftrightunsigned expects two arguments");
    }

    let expr_arr = &args[0];
    let shift_amount_arr = args[1].as_primitive::<Int32Type>();

    match expr_arr.data_type() {
        DataType::Int8 => {
            let array = expr_arr.as_primitive::<Int8Type>();
            let result: PrimitiveArray<Int32Type> = array
                .iter()
                .zip(shift_amount_arr.iter())
                .map(|(val, shift)| match (val, shift) {
                    (Some(val), Some(shift)) => Some(((val as u32) >> shift) as i32),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result) as ArrayRef)
        }
        DataType::Int16 => {
            let array = expr_arr.as_primitive::<Int16Type>();
            let result: PrimitiveArray<Int32Type> = array
                .iter()
                .zip(shift_amount_arr.iter())
                .map(|(val, shift)| match (val, shift) {
                    (Some(val), Some(shift)) => Some(((val as u32) >> shift) as i32),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result) as ArrayRef)
        }
        DataType::Int32 => {
            let array = expr_arr.as_primitive::<Int32Type>();
            let result: PrimitiveArray<Int32Type> = array
                .iter()
                .zip(shift_amount_arr.iter())
                .map(|(val, shift)| match (val, shift) {
                    (Some(val), Some(shift)) => Some(((val as u32) >> shift) as i32),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result) as ArrayRef)
        }
        DataType::Int64 => {
            let array = expr_arr.as_primitive::<Int64Type>();
            let result: PrimitiveArray<Int64Type> = array
                .iter()
                .zip(shift_amount_arr.iter())
                .map(|(val, shift)| match (val, shift) {
                    (Some(val), Some(shift)) => Some(((val as u64) >> shift) as i64),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result) as ArrayRef)
        }
        other => exec_err!("shiftrightunsigned does not support type: {other:?}"),
    }
}
