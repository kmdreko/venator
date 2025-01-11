//! Types and functions for handling filters.
//!
//! This filtering module covers two steps: parsing and validating. Parsing a
//! filter from a string is done via [`FilterPredicate::parse`]. If the string
//! was parsed successfully, the returned set of filter predicates can either
//! be:
//! - validated by calling [`validate_event_filter`] or [`validate_span_filter`]
//! - put in a [`Query`] to be passed to the engine query methods

use std::error::Error as StdError;
use std::fmt::{Display, Error as FmtError, Formatter};

use serde::Deserialize;

use crate::models::{Timestamp, ValueOperator};

mod event_filter;
mod input;
mod span_filter;
mod util;
mod value;

pub use event_filter::*;
pub use input::*;
pub use span_filter::*;
pub(crate) use util::*;
pub(crate) use value::*;

#[derive(Clone)]
pub enum FallibleFilterPredicate {
    // Not(Box<FilterPredicate>),
    Single(FilterPredicateSingle),
    And(Vec<Result<FallibleFilterPredicate, (InputError, String)>>),
    Or(Vec<Result<FallibleFilterPredicate, (InputError, String)>>),
}

#[derive(Debug, Deserialize)]
pub struct Query {
    pub filter: Vec<FilterPredicate>,
    pub order: Order,
    pub limit: usize,
    pub start: Timestamp,
    pub end: Timestamp,
    // when paginating, this is the last key of the previous call
    pub previous: Option<Timestamp>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum InputError {
    InvalidLevelValue,
    InvalidLevelOperator,
    InvalidNameValue,
    InvalidNameOperator,
    InvalidAttributeValue,
    InvalidInherentProperty,
    InvalidDurationValue,
    MissingDurationOperator,
    InvalidDurationOperator,
    InvalidCreatedValue,
    InvalidClosedValue,
    InvalidParentValue,
    InvalidParentOperator,
    InvalidTraceValue,
    InvalidTraceOperator,
    InvalidConnectedValue,
    InvalidDisconnectedValue,
    InvalidWildcardValue,
    InvalidRegexValue,
    InvalidFileOperator,
    InvalidFileValue,
}

impl Display for InputError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            InputError::InvalidLevelValue => write!(f, "invalid #level value"),
            InputError::InvalidLevelOperator => write!(f, "invalid #level operator"),
            InputError::InvalidNameValue => write!(f, "invalid #name value"),
            InputError::InvalidNameOperator => write!(f, "invalid #name operator"),
            InputError::InvalidAttributeValue => write!(f, "invalid #attribute value"),
            InputError::InvalidInherentProperty => write!(f, "invalid '#' Property"),
            InputError::InvalidDurationValue => write!(f, "invalid #duration value"),
            InputError::MissingDurationOperator => write!(f, "missing #duration operator"),
            InputError::InvalidDurationOperator => write!(f, "invalid #duration operator"),
            InputError::InvalidCreatedValue => write!(f, "invalid #created value"),
            InputError::InvalidClosedValue => write!(f, "invalid #closed value"),
            InputError::InvalidParentValue => write!(f, "invalid #parent value"),
            InputError::InvalidParentOperator => write!(f, "invalid #parent operator"),
            InputError::InvalidTraceValue => write!(f, "invalid #trace value"),
            InputError::InvalidTraceOperator => write!(f, "invalid #trace operator"),
            InputError::InvalidConnectedValue => write!(f, "invalid #connected value"),
            InputError::InvalidDisconnectedValue => write!(f, "invalid #disconnected value"),
            InputError::InvalidWildcardValue => write!(f, "invalid wildcard syntax"),
            InputError::InvalidRegexValue => write!(f, "invalid regex syntax"),
            InputError::InvalidFileOperator => write!(f, "invalid #file operator"),
            InputError::InvalidFileValue => write!(f, "invalid #file value"),
        }
    }
}

impl StdError for InputError {}

pub(crate) struct FileFilter {
    name: ValueStringComparison,
    line: Option<u32>,
}

impl FileFilter {
    fn matches(&self, file_name: Option<&str>, file_line: Option<u32>) -> bool {
        let Some(file_name) = file_name else {
            return false; // entities without a filename cannot match a #file
        };

        if !self.name.matches(file_name) {
            return false;
        }

        match self.line {
            Some(line) => Some(line) == file_line,
            None => true,
        }
    }
}

fn validate_value_predicate(
    value: &ValuePredicate,
    comparison_validator: impl Fn(&ValueOperator, &str) -> Result<(), InputError> + Clone,
    wildcard_validator: impl Fn(&str) -> Result<(), InputError> + Clone,
    regex_validator: impl Fn(&str) -> Result<(), InputError> + Clone,
) -> Result<(), InputError> {
    match value {
        ValuePredicate::Not(predicate) => validate_value_predicate(
            predicate,
            comparison_validator,
            wildcard_validator,
            regex_validator,
        ),
        ValuePredicate::Comparison(op, value) => comparison_validator(op, value),
        ValuePredicate::Wildcard(wildcard) => wildcard_validator(wildcard),
        ValuePredicate::Regex(regex) => regex_validator(regex),
        ValuePredicate::And(predicates) => predicates.iter().try_for_each(|p| {
            validate_value_predicate(
                p,
                comparison_validator.clone(),
                wildcard_validator.clone(),
                regex_validator.clone(),
            )
        }),
        ValuePredicate::Or(predicates) => predicates.iter().try_for_each(|p| {
            validate_value_predicate(
                p,
                comparison_validator.clone(),
                wildcard_validator.clone(),
                regex_validator.clone(),
            )
        }),
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Order {
    Asc,
    Desc,
}

#[cfg(test)]
mod tests {
    // #[test]
    // fn parse_level_into_filter() {
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:TRACE").unwrap(),
    //         BasicEventFilter::Level(0),
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:DEBUG").unwrap(),
    //         BasicEventFilter::Level(1),
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:INFO").unwrap(),
    //         BasicEventFilter::Level(2),
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:WARN").unwrap(),
    //         BasicEventFilter::Level(3),
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:ERROR").unwrap(),
    //         BasicEventFilter::Level(4),
    //     );
    // }

    // #[test]
    // fn parse_level_plus_into_filter() {
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:TRACE+").unwrap(),
    //         BasicEventFilter::Or(vec![
    //             BasicEventFilter::Level(0),
    //             BasicEventFilter::Level(1),
    //             BasicEventFilter::Level(2),
    //             BasicEventFilter::Level(3),
    //             BasicEventFilter::Level(4),
    //         ])
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:DEBUG+").unwrap(),
    //         BasicEventFilter::Or(vec![
    //             BasicEventFilter::Level(1),
    //             BasicEventFilter::Level(2),
    //             BasicEventFilter::Level(3),
    //             BasicEventFilter::Level(4),
    //         ])
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:INFO+").unwrap(),
    //         BasicEventFilter::Or(vec![
    //             BasicEventFilter::Level(2),
    //             BasicEventFilter::Level(3),
    //             BasicEventFilter::Level(4),
    //         ])
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:WARN+").unwrap(),
    //         BasicEventFilter::Or(vec![BasicEventFilter::Level(3), BasicEventFilter::Level(4),])
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:ERROR+").unwrap(),
    //         BasicEventFilter::Level(4)
    //     );
    // }

    // #[test]
    // fn parse_attribute_into_filter() {
    //     assert_eq!(
    //         BasicEventFilter::from_str("@attr1:A").unwrap(),
    //         BasicEventFilter::Attribute("attr1".into(), "A".into()),
    //     );
    // }

    // #[test]
    // fn parse_multiple_into_filter() {
    //     assert_eq!(
    //         BasicEventFilter::from_str("@attr1:A @attr2:B").unwrap(),
    //         BasicEventFilter::And(vec![
    //             BasicEventFilter::Attribute("attr1".into(), "A".into()),
    //             BasicEventFilter::Attribute("attr2".into(), "B".into()),
    //         ])
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:ERROR @attr2:B").unwrap(),
    //         BasicEventFilter::And(vec![
    //             BasicEventFilter::Level(4),
    //             BasicEventFilter::Attribute("attr2".into(), "B".into()),
    //         ])
    //     );
    //     assert_eq!(
    //         BasicEventFilter::from_str("#level:INFO+ @attr2:B").unwrap(),
    //         BasicEventFilter::And(vec![
    //             BasicEventFilter::Or(vec![
    //                 BasicEventFilter::Level(2),
    //                 BasicEventFilter::Level(3),
    //                 BasicEventFilter::Level(4),
    //             ]),
    //             BasicEventFilter::Attribute("attr2".into(), "B".into()),
    //         ])
    //     );
    // }

    // #[test]
    // fn parse_duration_into_filter() {
    //     assert_eq!(
    //         BasicSpanFilter::from_str("#duration:>1000000").unwrap(),
    //         BasicSpanFilter::Duration(DurationFilter::Gt(1000000.try_into().unwrap()))
    //     );
    //     assert_eq!(
    //         BasicSpanFilter::from_str("#duration:<1000000").unwrap(),
    //         BasicSpanFilter::Duration(DurationFilter::Lt(1000000.try_into().unwrap()))
    //     );
    // }
}
