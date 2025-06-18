use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use datafusion::arrow::datatypes::{DataType, Int16Type, Int32Type, Int64Type, Int8Type};
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::functions_utils::make_scalar_function;

#[derive(Debug)]
pub struct BitNot {
    signature: Signature,
}

impl Default for BitNot {
    fn default() -> Self {
        Self::new()
    }
}

impl BitNot {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BitNot {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "~"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return exec_err!("`~` function requires 1 argument, got {}", arg_types.len());
        }
        let expr_type = &arg_types[0];
        if !expr_type.is_integer() {
            return exec_err!("`~` function does not support type for argument: {expr_type:?}");
        }
        Ok(vec![expr_type.clone()])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(bit_not_inner, vec![])(&args)
    }
}

fn bit_not_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("`~` expects one argument");
    }

    let array = &args[0];

    Ok(match array.data_type() {
        DataType::Int8 => Arc::new(
            array
                .as_primitive::<Int8Type>()
                .iter()
                .map(|v| v.map(|v| !v))
                .collect::<PrimitiveArray<Int8Type>>(),
        ) as ArrayRef,
        DataType::Int16 => Arc::new(
            array
                .as_primitive::<Int16Type>()
                .iter()
                .map(|v| v.map(|v| !v))
                .collect::<PrimitiveArray<Int16Type>>(),
        ) as ArrayRef,
        DataType::Int32 => Arc::new(
            array
                .as_primitive::<Int32Type>()
                .iter()
                .map(|v| v.map(|v| !v))
                .collect::<PrimitiveArray<Int32Type>>(),
        ) as ArrayRef,
        DataType::Int64 => Arc::new(
            array
                .as_primitive::<Int64Type>()
                .iter()
                .map(|v| v.map(|v| !v))
                .collect::<PrimitiveArray<Int64Type>>(),
        ) as ArrayRef,
        other => return exec_err!("`~` does not support type: {other:?}"),
    })
}
