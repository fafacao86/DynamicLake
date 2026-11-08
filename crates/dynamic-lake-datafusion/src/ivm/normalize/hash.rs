use std::fmt::Write as _;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, lit,
};
use datafusion::prelude::SessionContext;
use sha1::{Digest, Sha1};

use super::RowIdDomain;

pub const HASH_FUNCTION_NAME: &str = "_ivm_sha1";

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IvmSha1 {
    signature: Signature,
}

impl Default for IvmSha1 {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IvmSha1 {
    fn name(&self) -> &str {
        HASH_FUNCTION_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> DataFusionResult<FieldRef> {
        Ok(Arc::new(Field::new(
            HASH_FUNCTION_NAME,
            DataType::Utf8,
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if args.args.is_empty() {
            return Err(DataFusionError::Plan(
                "_ivm_sha1 expects a domain label".to_string(),
            ));
        }
        let domains = string_rows(&args.args[0], args.number_rows)?;
        let mut inputs = Vec::with_capacity(args.args.len().saturating_sub(1));
        for input in args.args.iter().skip(1) {
            inputs.push(string_rows(input, args.number_rows)?);
        }

        let mut output = StringBuilder::with_capacity(args.number_rows, args.number_rows * 40);
        for row in 0..args.number_rows {
            let domain = domains[scalar_index(domains.len(), row)]
                .as_deref()
                .ok_or_else(|| DataFusionError::Plan("_ivm_sha1 domain must not be null".into()))?;
            let values = inputs
                .iter()
                .map(|values| values[scalar_index(values.len(), row)].as_deref())
                .collect::<Vec<_>>();
            output.append_value(sha1_row(domain, &values));
        }
        Ok(ColumnarValue::Array(Arc::new(output.finish()) as ArrayRef))
    }
}

fn scalar_index(length: usize, row: usize) -> usize {
    if length == 1 { 0 } else { row }
}

fn string_rows(value: &ColumnarValue, number_rows: usize) -> DataFusionResult<Vec<Option<String>>> {
    let array = value.clone().into_array(number_rows)?;
    let array = cast(&array, &DataType::Utf8)?;
    let array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Plan("_ivm_sha1 arguments must be castable to string".to_string())
        })?;
    Ok((0..array.len())
        .map(|row| array.is_valid(row).then(|| array.value(row).to_string()))
        .collect())
}

fn sha1_row(domain: &str, values: &[Option<&str>]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(b"dynamic-lake-ivm-row-id-v1");
    update_hashed_field(&mut hasher, domain.as_bytes());
    hasher.update(values.len().to_be_bytes());
    for value in values {
        match value {
            None => hasher.update([0_u8]),
            Some(value) => {
                hasher.update([1_u8]);
                update_hashed_field(&mut hasher, value.as_bytes());
            }
        }
    }
    hex_encode(&hasher.finalize())
}

fn update_hashed_field(hasher: &mut Sha1, value: &[u8]) {
    hasher.update(value.len().to_be_bytes());
    hasher.update(value);
}

fn hex_encode(value: &[u8]) -> String {
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output
}

pub fn ivm_sha1() -> ScalarUDF {
    ScalarUDF::from(IvmSha1::default())
}

pub fn ivm_sha1_expr(domain: RowIdDomain, inputs: impl IntoIterator<Item = Expr>) -> Expr {
    let mut args = vec![lit(domain.label())];
    args.extend(inputs);
    ivm_sha1().call(args)
}

pub fn register_ivm_functions(context: &SessionContext) {
    context.register_udf(ivm_sha1());
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::StringArray;

    use super::*;

    #[test]
    fn null_and_empty_hashes_differ() {
        let func = IvmSha1::default();
        let domain = ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("join")])));
        let input = ColumnarValue::Array(Arc::new(StringArray::from(vec![Some(""), None])));
        let output = func
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![domain, input],
                arg_fields: Vec::new(),
                number_rows: 2,
                return_field: Arc::new(Field::new("hash", DataType::Utf8, false)),
                config_options: Arc::new(Default::default()),
            })
            .expect("hash invocation");
        let ColumnarValue::Array(output) = output else {
            panic!("hash must return an array");
        };
        let output = output
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 output");
        assert_eq!(output.value(0).len(), 40);
        assert_ne!(output.value(0), output.value(1));
    }

    #[test]
    fn sha1_matches_known_vector() {
        assert_eq!(
            hex_encode(&Sha1::digest(b"abc")),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
    }
}
