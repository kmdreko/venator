use std::str::FromStr;

use regex::Regex;
use wildcard::{Wildcard, WildcardBuilder};

use crate::models::{Value, ValueOperator};

use super::InputError;

#[derive(Clone)]
pub(crate) enum ValueStringComparison {
    // TODO: apparently we never create a non-matching string comparison,
    // consider removing this
    #[allow(unused)]
    None,
    Compare(ValueOperator, String),
    Wildcard(Wildcard<'static, u8>),
    Regex(Regex),
    All,
}

impl ValueStringComparison {
    pub fn matches(&self, lhs: &str) -> bool {
        match self {
            ValueStringComparison::None => false,
            ValueStringComparison::Compare(op, rhs) => op.compare(lhs, rhs),
            ValueStringComparison::Wildcard(wildcard) => wildcard.is_match(lhs.as_bytes()),
            ValueStringComparison::Regex(regex) => regex.is_match(lhs),
            ValueStringComparison::All => true,
        }
    }

    pub fn matches_opt(&self, lhs: Option<&str>) -> bool {
        match self {
            ValueStringComparison::None => false,
            ValueStringComparison::Compare(op, rhs) => match lhs {
                Some(lhs) => op.compare(lhs, rhs),
                None => false,
            },
            ValueStringComparison::Wildcard(wildcard) => match lhs {
                Some(lhs) => wildcard.is_match(lhs.as_bytes()),
                None => false,
            },
            ValueStringComparison::Regex(regex) => match lhs {
                Some(lhs) => regex.is_match(lhs),
                None => false,
            },
            ValueStringComparison::All => true,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ValueComparison<T> {
    None,
    Compare(ValueOperator, T),
    All,
}

impl<T> ValueComparison<T> {
    pub(crate) fn matches(&self, lhs: &T) -> bool
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

#[derive(Clone)]
pub(crate) struct ValueFilter {
    pub nulls: bool,
    pub f64s: ValueComparison<f64>,
    pub i64s: ValueComparison<i64>,
    pub u64s: ValueComparison<u64>,
    pub i128s: ValueComparison<i128>,
    pub u128s: ValueComparison<u128>,
    pub bools: ValueComparison<bool>,
    pub strings: ValueStringComparison,
    pub bytes: bool,
    pub arrays: bool,
    pub objects: bool,
}

impl ValueFilter {
    pub fn from_input(operator: ValueOperator, value: &str) -> ValueFilter {
        let strings = ValueStringComparison::Compare(operator, value.to_owned());

        let nulls = value == "null";

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
            nulls,
            f64s,
            i64s,
            u64s,
            i128s,
            u128s,
            bools,
            strings,
            bytes: false,
            arrays: false,
            objects: false,
        }
    }

    pub fn from_wildcard(wildcard: String) -> Result<ValueFilter, InputError> {
        if wildcard == "*" {
            return Ok(ValueFilter {
                nulls: true,
                f64s: ValueComparison::All,
                i64s: ValueComparison::All,
                u64s: ValueComparison::All,
                i128s: ValueComparison::All,
                u128s: ValueComparison::All,
                bools: ValueComparison::All,
                strings: ValueStringComparison::All,
                bytes: true,
                arrays: true,
                objects: true,
            });
        }

        let wildcard = WildcardBuilder::from_owned(wildcard.into_bytes())
            .without_one_metasymbol()
            .build()
            .map_err(|_| InputError::InvalidWildcardValue)?;

        Ok(ValueFilter {
            nulls: false,
            f64s: ValueComparison::None,
            i64s: ValueComparison::None,
            u64s: ValueComparison::None,
            i128s: ValueComparison::None,
            u128s: ValueComparison::None,
            bools: ValueComparison::None,
            strings: ValueStringComparison::Wildcard(wildcard),
            bytes: false,
            arrays: false,
            objects: false,
        })
    }

    pub fn from_regex(regex: String) -> Result<ValueFilter, InputError> {
        let regex = Regex::new(&regex).map_err(|_| InputError::InvalidWildcardValue)?;

        Ok(ValueFilter {
            nulls: false,
            f64s: ValueComparison::None,
            i64s: ValueComparison::None,
            u64s: ValueComparison::None,
            i128s: ValueComparison::None,
            u128s: ValueComparison::None,
            bools: ValueComparison::None,
            strings: ValueStringComparison::Regex(regex),
            bytes: false,
            arrays: false,
            objects: false,
        })
    }

    pub fn matches(&self, value: &Value) -> bool {
        match value {
            Value::Null => self.nulls,
            Value::F64(value) => self.f64s.matches(value),
            Value::I64(value) => self.i64s.matches(value),
            Value::U64(value) => self.u64s.matches(value),
            Value::I128(value) => self.i128s.matches(value),
            Value::U128(value) => self.u128s.matches(value),
            Value::Bool(value) => self.bools.matches(value),
            Value::Str(value) => self.strings.matches(value),
            Value::Bytes(_) => self.bytes,
            Value::Array(_) => self.arrays,
            Value::Object(_) => self.objects,
        }
    }
}
