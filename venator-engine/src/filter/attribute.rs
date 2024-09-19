use std::str::FromStr;

use crate::models::{Value, ValueOperator};

#[derive(Debug, Clone)]
pub enum ValueComparison<T> {
    None,
    Compare(ValueOperator, T),
    All,
}

impl<T> ValueComparison<T> {
    fn compare(&self, lhs: &T) -> bool
    where
        T: PartialOrd,
    {
        match self {
            ValueComparison::None => false,
            ValueComparison::Compare(op, rhs) => op.compare(lhs, rhs),
            ValueComparison::All => true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ValueFilter {
    pub f64s: ValueComparison<f64>,
    pub i64s: ValueComparison<i64>,
    pub u64s: ValueComparison<u64>,
    pub i128s: ValueComparison<i128>,
    pub u128s: ValueComparison<u128>,
    pub bools: ValueComparison<bool>,
    pub strings: ValueComparison<String>,
}

impl ValueFilter {
    pub fn from_input(operator: ValueOperator, value: &str) -> ValueFilter {
        // TODO: check if value has wildcards
        // TODO: check if value is a regex

        let strings = ValueComparison::Compare(operator, value.to_owned());

        let f64s = if let Ok(f64_value) = f64::from_str(value) {
            ValueComparison::Compare(operator, f64_value)
        } else {
            ValueComparison::None
        };

        let i128s = if let Ok(i128_value) = i128::from_str(value) {
            ValueComparison::Compare(operator, i128_value)
        } else if let ValueComparison::Compare(_, f64_value) = f64s {
            if f64_value.is_nan() {
                ValueComparison::None
            } else if f64_value > i128::MAX as f64 {
                match operator {
                    ValueOperator::Gt => ValueComparison::None,
                    ValueOperator::Gte => ValueComparison::None,
                    ValueOperator::Eq => ValueComparison::None,
                    ValueOperator::Lt => ValueComparison::All,
                    ValueOperator::Lte => ValueComparison::All,
                }
            } else if f64_value < i128::MIN as f64 {
                match operator {
                    ValueOperator::Gt => ValueComparison::All,
                    ValueOperator::Gte => ValueComparison::All,
                    ValueOperator::Eq => ValueComparison::None,
                    ValueOperator::Lt => ValueComparison::None,
                    ValueOperator::Lte => ValueComparison::None,
                }
            } else {
                let maybe_i128_value = match operator {
                    ValueOperator::Gt => Some(f64_value.floor() as i128),
                    ValueOperator::Gte => Some(f64_value.ceil() as i128),
                    ValueOperator::Eq => {
                        if f64_value.round() == f64_value {
                            Some(f64_value as i128)
                        } else {
                            None
                        }
                    }
                    ValueOperator::Lt => Some(f64_value.ceil() as i128),
                    ValueOperator::Lte => Some(f64_value.floor() as i128),
                };

                if let Some(i128_value) = maybe_i128_value {
                    ValueComparison::Compare(operator, i128_value)
                } else {
                    ValueComparison::None
                }
            }
        } else {
            ValueComparison::None
        };

        let u128s = if let Ok(u128_value) = u128::from_str(value) {
            ValueComparison::Compare(operator, u128_value)
        } else if let ValueComparison::Compare(_, f64_value) = f64s {
            if f64_value.is_nan() {
                ValueComparison::None
            } else if f64_value > u128::MAX as f64 {
                match operator {
                    ValueOperator::Gt => ValueComparison::None,
                    ValueOperator::Gte => ValueComparison::None,
                    ValueOperator::Eq => ValueComparison::None,
                    ValueOperator::Lt => ValueComparison::All,
                    ValueOperator::Lte => ValueComparison::All,
                }
            } else if f64_value < u128::MIN as f64 {
                match operator {
                    ValueOperator::Gt => ValueComparison::All,
                    ValueOperator::Gte => ValueComparison::All,
                    ValueOperator::Eq => ValueComparison::None,
                    ValueOperator::Lt => ValueComparison::None,
                    ValueOperator::Lte => ValueComparison::None,
                }
            } else {
                let maybe_u128_value = match operator {
                    ValueOperator::Gt => Some(f64_value.floor() as u128),
                    ValueOperator::Gte => Some(f64_value.ceil() as u128),
                    ValueOperator::Eq => {
                        if f64_value.round() == f64_value {
                            Some(f64_value as u128)
                        } else {
                            None
                        }
                    }
                    ValueOperator::Lt => Some(f64_value.ceil() as u128),
                    ValueOperator::Lte => Some(f64_value.floor() as u128),
                };

                if let Some(u128_value) = maybe_u128_value {
                    ValueComparison::Compare(operator, u128_value)
                } else {
                    ValueComparison::None
                }
            }
        } else {
            ValueComparison::None
        };

        let i64s = if let Ok(i64_value) = i64::from_str(value) {
            ValueComparison::Compare(operator, i64_value)
        } else if let ValueComparison::Compare(_, f64_value) = f64s {
            if f64_value.is_nan() {
                ValueComparison::None
            } else if f64_value > i64::MAX as f64 {
                match operator {
                    ValueOperator::Gt => ValueComparison::None,
                    ValueOperator::Gte => ValueComparison::None,
                    ValueOperator::Eq => ValueComparison::None,
                    ValueOperator::Lt => ValueComparison::All,
                    ValueOperator::Lte => ValueComparison::All,
                }
            } else if f64_value < i64::MIN as f64 {
                match operator {
                    ValueOperator::Gt => ValueComparison::All,
                    ValueOperator::Gte => ValueComparison::All,
                    ValueOperator::Eq => ValueComparison::None,
                    ValueOperator::Lt => ValueComparison::None,
                    ValueOperator::Lte => ValueComparison::None,
                }
            } else {
                let maybe_i64_value = match operator {
                    ValueOperator::Gt => Some(f64_value.floor() as i64),
                    ValueOperator::Gte => Some(f64_value.ceil() as i64),
                    ValueOperator::Eq => {
                        if f64_value.round() == f64_value {
                            Some(f64_value as i64)
                        } else {
                            None
                        }
                    }
                    ValueOperator::Lt => Some(f64_value.ceil() as i64),
                    ValueOperator::Lte => Some(f64_value.floor() as i64),
                };

                if let Some(i64_value) = maybe_i64_value {
                    ValueComparison::Compare(operator, i64_value)
                } else {
                    ValueComparison::None
                }
            }
        } else {
            ValueComparison::None
        };

        let u64s = if let Ok(u64_value) = u64::from_str(value) {
            ValueComparison::Compare(operator, u64_value)
        } else if let ValueComparison::Compare(_, f64_value) = f64s {
            if f64_value.is_nan() {
                ValueComparison::None
            } else if f64_value > u64::MAX as f64 {
                match operator {
                    ValueOperator::Gt => ValueComparison::None,
                    ValueOperator::Gte => ValueComparison::None,
                    ValueOperator::Eq => ValueComparison::None,
                    ValueOperator::Lt => ValueComparison::All,
                    ValueOperator::Lte => ValueComparison::All,
                }
            } else if f64_value < u64::MIN as f64 {
                match operator {
                    ValueOperator::Gt => ValueComparison::All,
                    ValueOperator::Gte => ValueComparison::All,
                    ValueOperator::Eq => ValueComparison::None,
                    ValueOperator::Lt => ValueComparison::None,
                    ValueOperator::Lte => ValueComparison::None,
                }
            } else {
                let maybe_u64_value = match operator {
                    ValueOperator::Gt => Some(f64_value.floor() as u64),
                    ValueOperator::Gte => Some(f64_value.ceil() as u64),
                    ValueOperator::Eq => {
                        if f64_value.round() == f64_value {
                            Some(f64_value as u64)
                        } else {
                            None
                        }
                    }
                    ValueOperator::Lt => Some(f64_value.ceil() as u64),
                    ValueOperator::Lte => Some(f64_value.floor() as u64),
                };

                if let Some(u64_value) = maybe_u64_value {
                    ValueComparison::Compare(operator, u64_value)
                } else {
                    ValueComparison::None
                }
            }
        } else {
            ValueComparison::None
        };

        let bools = if value == "true" {
            ValueComparison::Compare(operator, true)
        } else if value == "false" {
            ValueComparison::Compare(operator, false)
        } else {
            ValueComparison::None
        };

        ValueFilter {
            f64s,
            i64s,
            u64s,
            i128s,
            u128s,
            bools,
            strings,
        }
    }

    pub fn matches(&self, value: &Value) -> bool {
        match value {
            Value::F64(value) => self.f64s.compare(value),
            Value::I64(value) => self.i64s.compare(value),
            Value::U64(value) => self.u64s.compare(value),
            Value::I128(value) => self.i128s.compare(value),
            Value::U128(value) => self.u128s.compare(value),
            Value::Bool(value) => self.bools.compare(value),
            Value::Str(value) => self.strings.compare(value),
        }
    }
}
