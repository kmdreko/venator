use std::fmt::{Display, Error as FmtError, Formatter};

use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct SyntaxError;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum FilterPropertyKind {
    Inherent,
    Attribute,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum FilterValueOperator {
    Gt,
    Gte,
    Eq,
    Neq,
    Lt,
    Lte,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterPredicate {
    pub property_kind: Option<FilterPropertyKind>,
    pub property: String,
    pub value_operator: Option<FilterValueOperator>,
    pub value: String,
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

        write!(f, "{}: ", self.property)?;

        match self.value_operator {
            Some(FilterValueOperator::Gt) => write!(f, ">")?,
            Some(FilterValueOperator::Gte) => write!(f, ">=")?,
            Some(FilterValueOperator::Eq) => write!(f, "=")?,
            Some(FilterValueOperator::Neq) => write!(f, "!")?,
            Some(FilterValueOperator::Lt) => write!(f, "<")?,
            Some(FilterValueOperator::Lte) => write!(f, "<=")?,
            None => {}
        }

        if needs_escapes(&self.value) {
            write!(f, "{:?}", self.value)?;
        } else {
            write!(f, "{}", self.value)?;
        }

        Ok(())
    }
}

fn needs_escapes(s: &str) -> bool {
    s.contains(['"', '\\', '#', '@', ':', '<', '>', '=', '!'])
        || s.contains(|c: char| c.is_whitespace())
}

impl FilterPredicate {
    pub fn new_unknown(property: impl Into<String>, value: impl Into<String>) -> FilterPredicate {
        FilterPredicate {
            property_kind: None,
            property: property.into(),
            value_operator: None,
            value: value.into(),
        }
    }

    pub fn new_inherent(property: impl Into<String>, value: impl Into<String>) -> FilterPredicate {
        FilterPredicate {
            property_kind: Some(FilterPropertyKind::Inherent),
            property: property.into(),
            value_operator: None,
            value: value.into(),
        }
    }

    pub fn new_attribute(
        property: impl Into<String>,
        value: impl Into<String>,
    ) -> FilterPredicate {
        FilterPredicate {
            property_kind: Some(FilterPropertyKind::Attribute),
            property: property.into(),
            value_operator: None,
            value: value.into(),
        }
    }

    pub fn with_operator(self, op: FilterValueOperator) -> FilterPredicate {
        FilterPredicate {
            value_operator: Some(op),
            ..self
        }
    }
}

mod parsers {
    use super::*;

    use nom::branch::alt;
    use nom::bytes::complete::{escaped, tag, take_while, take_while1};
    use nom::character::complete::{char, none_of, one_of};
    use nom::combinator::{cut, eof, map, opt};
    use nom::multi::separated_list0;
    use nom::sequence::delimited;
    use nom::IResult;

    fn whitespace(input: &str) -> IResult<&str, &str> {
        take_while(|c: char| c.is_whitespace())(input)
    }

    fn expect_whitespace(input: &str) -> IResult<&str, &str> {
        take_while1(|c: char| c.is_whitespace())(input)
    }

    fn unquoted_name(input: &str) -> IResult<&str, &str> {
        take_while(|c: char| c.is_alphabetic() || c == '.')(input)
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

    fn value(input: &str) -> IResult<&str, (Option<FilterValueOperator>, String)> {
        let (input, op) = opt(alt((
            map(tag(">="), |_| FilterValueOperator::Gte),
            map(tag(">"), |_| FilterValueOperator::Gt),
            map(tag("="), |_| FilterValueOperator::Eq),
            map(tag("!"), |_| FilterValueOperator::Neq),
            map(tag("<="), |_| FilterValueOperator::Lte),
            map(tag("<"), |_| FilterValueOperator::Lt),
        )))(input)?;
        let (input, value) = alt((
            map(quoted_value, |v| v.map(unescape).unwrap_or_default()),
            map(unquoted_value, |v| v.to_owned()),
        ))(input)?;

        Ok((input, (op, value)))
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
        let (input, (op, value)) = value(input)?;

        let predicate = FilterPredicate {
            property_kind: kind,
            property: property.to_owned(),
            value_operator: op,
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
        assert_eq!(
            FilterPredicate::parse("prop:value").unwrap(),
            vec![FilterPredicate::new_unknown("prop", "value")],
        );
        assert_eq!(
            FilterPredicate::parse("#prop:value").unwrap(),
            vec![FilterPredicate::new_inherent("prop", "value")],
        );
        assert_eq!(
            FilterPredicate::parse("@prop:value").unwrap(),
            vec![FilterPredicate::new_attribute("prop", "value")],
        );
    }

    #[test]
    fn parse_extra_whitespace() {
        assert_eq!(
            FilterPredicate::parse("prop :value").unwrap(),
            vec![FilterPredicate::new_unknown("prop", "value")],
        );
        assert_eq!(
            FilterPredicate::parse("prop: value").unwrap(),
            vec![FilterPredicate::new_unknown("prop", "value")],
        );
        assert_eq!(
            FilterPredicate::parse("prop : value").unwrap(),
            vec![FilterPredicate::new_unknown("prop", "value")],
        );
    }

    #[test]
    fn parse_quoted_values() {
        assert_eq!(
            FilterPredicate::parse("prop: \"value\"").unwrap(),
            vec![FilterPredicate::new_unknown("prop", "value")],
        );
        assert_eq!(
            FilterPredicate::parse("prop: \" value \"").unwrap(),
            vec![FilterPredicate::new_unknown("prop", " value ")],
        );
        assert_eq!(
            FilterPredicate::parse("prop: \"va\\\\lue\\\"\"").unwrap(),
            vec![FilterPredicate::new_unknown("prop", "va\\lue\"")],
        );
        assert_eq!(
            FilterPredicate::parse("prop: \"\"").unwrap(),
            vec![FilterPredicate::new_unknown("prop", "")],
        );
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
