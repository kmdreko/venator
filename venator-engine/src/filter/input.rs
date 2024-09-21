use std::fmt::{Display, Error as FmtError, Formatter};

use serde::{Deserialize, Serialize};

use crate::models::ValueOperator;

#[derive(Debug)]
pub struct SyntaxError;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum FilterPropertyKind {
    Inherent,
    Attribute,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "value_kind", content = "value", rename_all = "camelCase")]
pub enum ValuePredicate {
    Not(Box<ValuePredicate>),
    Comparison(ValueOperator, String),
    // Regex(String),
    // Wildcard(String),
    And(Vec<ValuePredicate>),
    Or(Vec<ValuePredicate>),
}

impl Display for ValuePredicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match &self {
            ValuePredicate::Not(inner) => write!(f, "!{inner}"),
            ValuePredicate::Comparison(value_operator, value) => match value_operator {
                ValueOperator::Gt => {
                    if needs_escapes(value) {
                        write!(f, ">{value:?}")
                    } else {
                        write!(f, ">{value}")
                    }
                }
                ValueOperator::Gte => {
                    if needs_escapes(value) {
                        write!(f, ">={value:?}")
                    } else {
                        write!(f, ">={value}")
                    }
                }
                ValueOperator::Eq => {
                    if needs_escapes(value) {
                        write!(f, "{value:?}")
                    } else {
                        write!(f, "{value}")
                    }
                }
                ValueOperator::Lt => {
                    if needs_escapes(value) {
                        write!(f, "<{value:?}")
                    } else {
                        write!(f, "<{value}")
                    }
                }
                ValueOperator::Lte => {
                    if needs_escapes(value) {
                        write!(f, "<={value:?}")
                    } else {
                        write!(f, "<={value}")
                    }
                }
            },
            ValuePredicate::And(inners) => {
                write!(f, "({}", inners[0])?;
                for inner in &inners[1..] {
                    write!(f, " AND {}", inner)?;
                }
                write!(f, ")")
            }
            ValuePredicate::Or(inners) => {
                write!(f, "({}", inners[0])?;
                for inner in &inners[1..] {
                    write!(f, " OR {}", inner)?;
                }
                write!(f, ")")
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterPredicate {
    pub property_kind: Option<FilterPropertyKind>,
    pub property: String,
    #[serde(flatten)]
    pub value: ValuePredicate,
}

impl FilterPredicate {
    pub fn parse(input: &str) -> Result<Vec<FilterPredicate>, SyntaxError> {
        parsers::predicates(input)
            .map(|(_, predicates)| predicates)
            .map_err(|_| SyntaxError)
    }
}

impl Display for FilterPredicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self.property_kind {
            Some(FilterPropertyKind::Inherent) => write!(f, "#")?,
            Some(FilterPropertyKind::Attribute) => write!(f, "@")?,
            None => {}
        }

        write!(f, "{}: {}", self.property, self.value)?;

        Ok(())
    }
}

fn needs_escapes(s: &str) -> bool {
    s.contains(['"', '\\', '#', '@', ':', '<', '>', '=', '!'])
        || s.contains(|c: char| c.is_whitespace())
}

mod parsers {
    use super::*;

    use nom::branch::alt;
    use nom::bytes::complete::{escaped, tag, take_while, take_while1};
    use nom::character::complete::{char, none_of, one_of};
    use nom::combinator::{cut, eof, map, opt};
    use nom::multi::{many0_count, separated_list0};
    use nom::sequence::delimited;
    use nom::IResult;

    fn whitespace(input: &str) -> IResult<&str, &str> {
        take_while(|c: char| c.is_whitespace())(input)
    }

    fn expect_whitespace(input: &str) -> IResult<&str, &str> {
        take_while1(|c: char| c.is_whitespace())(input)
    }

    fn unquoted_name(input: &str) -> IResult<&str, &str> {
        take_while(|c: char| c.is_alphabetic() || c == '.' || c == '_')(input)
    }

    fn inherent_name(input: &str) -> IResult<&str, &str> {
        let (input, _) = char('#')(input)?;
        let (input, attr_name) = cut(unquoted_name)(input)?;
        Ok((input, attr_name))
    }

    fn attribute_name(input: &str) -> IResult<&str, &str> {
        let (input, _) = char('@')(input)?;
        let (input, attr_name) = cut(unquoted_name)(input)?;
        Ok((input, attr_name))
    }

    fn undecorated_name(input: &str) -> IResult<&str, &str> {
        take_while(|c: char| c.is_alphabetic() || c == '.')(input)
    }

    fn property(input: &str) -> IResult<&str, (Option<FilterPropertyKind>, &str)> {
        use FilterPropertyKind::*;

        alt((
            map(inherent_name, |name| (Some(Inherent), name)),
            map(attribute_name, |name| (Some(Attribute), name)),
            map(undecorated_name, |name| (None, name)),
        ))(input)
    }

    fn not(input: &str) -> IResult<&str, &str> {
        let (input, _) = whitespace(input)?;
        tag("!")(input)
    }

    fn value(input: &str) -> IResult<&str, (bool, Option<ValueOperator>, String)> {
        let (input, not_count) = many0_count(not)(input)?;
        let (input, op) = opt(alt((
            map(tag(">="), |_| ValueOperator::Gte),
            map(tag(">"), |_| ValueOperator::Gt),
            map(tag("<="), |_| ValueOperator::Lte),
            map(tag("<"), |_| ValueOperator::Lt),
        )))(input)?;
        let (input, value) = alt((
            map(quoted_value, |v| v.map(unescape).unwrap_or_default()),
            map(unquoted_value, |v| v.to_owned()),
        ))(input)?;

        Ok((input, (not_count % 2 == 1, op, value)))
    }

    fn escaped_value(input: &str) -> IResult<&str, &str> {
        escaped(none_of("\\\""), '\\', one_of("\"\\"))(input)
    }

    fn quoted_value(input: &str) -> IResult<&str, Option<&str>> {
        delimited(char('\"'), opt(escaped_value), char('\"'))(input)
    }

    fn unquoted_value(input: &str) -> IResult<&str, &str> {
        take_while1(|c: char| !c.is_whitespace() && c != '"' && c != '@' && c != '#' && c != ':')(
            input,
        )
    }

    fn predicate(input: &str) -> IResult<&str, FilterPredicate> {
        let (input, (kind, property)) = property(input)?;
        let (input, _) = whitespace(input)?;
        let (input, _) = char(':')(input)?;
        let (input, _) = whitespace(input)?;
        let (input, (not, op, value)) = value(input)?;

        let value = ValuePredicate::Comparison(op.unwrap_or(ValueOperator::Eq), value);
        let value = if not {
            ValuePredicate::Not(Box::new(value))
        } else {
            value
        };

        let predicate = FilterPredicate {
            property_kind: kind,
            property: property.to_owned(),
            value,
        };

        Ok((input, predicate))
    }

    pub fn predicates(input: &str) -> IResult<&str, Vec<FilterPredicate>> {
        let (input, _) = whitespace(input)?;
        let (input, list) = separated_list0(expect_whitespace, predicate)(input)?;
        let (input, _) = whitespace(input)?;
        let (input, _) = eof(input)?;

        Ok((input, list))
    }

    fn unescape(input: &str) -> String {
        let mut input = input.to_owned();
        let mut escaped = false;
        input.retain(|c| match (escaped, c) {
            (true, _) => {
                escaped = false;
                true
            }
            (false, '\\') => {
                escaped = true;
                false
            }
            (false, _) => true,
        });

        input
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_property_kind() {
        // assert_eq!(
        //     FilterPredicate::parse("prop:value").unwrap(),
        //     vec![FilterPredicate::new_unknown("prop", "value")],
        // );
        // assert_eq!(
        //     FilterPredicate::parse("#prop:value").unwrap(),
        //     vec![FilterPredicate::new_inherent("prop", "value")],
        // );
        // assert_eq!(
        //     FilterPredicate::parse("@prop:value").unwrap(),
        //     vec![FilterPredicate::new_attribute("prop", "value")],
        // );
    }

    #[test]
    fn parse_extra_whitespace() {
        // assert_eq!(
        //     FilterPredicate::parse("prop :value").unwrap(),
        //     vec![FilterPredicate::new_unknown("prop", "value")],
        // );
        // assert_eq!(
        //     FilterPredicate::parse("prop: value").unwrap(),
        //     vec![FilterPredicate::new_unknown("prop", "value")],
        // );
        // assert_eq!(
        //     FilterPredicate::parse("prop : value").unwrap(),
        //     vec![FilterPredicate::new_unknown("prop", "value")],
        // );
    }

    #[test]
    fn parse_quoted_values() {
        // assert_eq!(
        //     FilterPredicate::parse("prop: \"value\"").unwrap(),
        //     vec![FilterPredicate::new_unknown("prop", "value")],
        // );
        // assert_eq!(
        //     FilterPredicate::parse("prop: \" value \"").unwrap(),
        //     vec![FilterPredicate::new_unknown("prop", " value ")],
        // );
        // assert_eq!(
        //     FilterPredicate::parse("prop: \"va\\\\lue\\\"\"").unwrap(),
        //     vec![FilterPredicate::new_unknown("prop", "va\\lue\"")],
        // );
        // assert_eq!(
        //     FilterPredicate::parse("prop: \"\"").unwrap(),
        //     vec![FilterPredicate::new_unknown("prop", "")],
        // );
    }

    #[test]
    fn parse_multiple() {
        assert!(FilterPredicate::parse("prop:aprop:b").is_err());
        assert!(FilterPredicate::parse("prop:a prop:b").is_ok());
        assert!(FilterPredicate::parse("prop:a@prop:b").is_err());
        assert!(FilterPredicate::parse("prop:a @prop:b").is_ok());
        assert!(FilterPredicate::parse("prop:a#prop:b").is_err());
        assert!(FilterPredicate::parse("prop:a #prop:b").is_ok());
        assert!(FilterPredicate::parse("  prop:a #prop:b  ").is_ok());
        assert!(FilterPredicate::parse("prop: !4 #prop: >10 @prop: <=20").is_ok());
    }
}
