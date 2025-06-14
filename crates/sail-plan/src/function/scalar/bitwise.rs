use datafusion_expr::Operator;

use crate::extension::function::bitwise::bit_count::BitCount;
use crate::extension::function::bitwise::bit_get::BitGet;
use crate::function::common::ScalarFunction;

pub(super) fn list_built_in_bitwise_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("&", F::binary_op(Operator::BitwiseAnd)),
        ("^", F::binary_op(Operator::BitwiseXor)),
        ("bit_count", F::udf(BitCount::new())),
        ("bit_get", F::udf(BitGet::new())),
        ("getbit", F::udf(BitGet::new())),
        // "shiftleft" is defined in math functions
        ("shiftright", F::binary_op(Operator::BitwiseShiftRight)),
        ("shiftrightunsigned", F::unknown("shiftrightunsigned")),
        ("|", F::binary_op(Operator::BitwiseOr)),
        ("~", F::unknown("~")),
    ]
}
