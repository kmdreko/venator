use std::fmt::{Display, Error as FmtError, Formatter};

use serde::{Deserialize, Serialize};

use crate::models::ValueOperator;

#[derive(Debug)]
pub struct SyntaxError;

impl Display for SyntaxError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        write!(f, "syntax error")
    }
}

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
    Wildcard(String),
    Regex(String),
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
            ValuePredicate::Wildcard(wildcard) => {
                if needs_escapes(wildcard) {
                    write!(f, "\"{}\"", escape_wildcard(wildcard))
                } else {
                    write!(f, "{wildcard}")
                }
            }
            ValuePredicate::Regex(regex) => {
                write!(f, "/{regex}/")
            }
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

#[derive(Debug, Clone, Deserialize)]
#[serde(
    tag = "predicate_kind",
    rename_all = "camelCase",
    content = "predicate"
)]
pub enum FilterPredicate {
    // Not(Box<FilterPredicate>),
    Single(FilterPredicateSingle),
    And(Vec<FilterPredicate>),
    Or(Vec<FilterPredicate>),
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
        match self {
            FilterPredicate::Single(single) => write!(f, "{single}"),
            FilterPredicate::And(inners) => {
                write!(f, "({}", inners[0])?;
                for inner in &inners[1..] {
                    write!(f, " AND {}", inner)?;
                }
                write!(f, ")")
            }
            FilterPredicate::Or(inners) => {
                write!(f, "({}", inners[0])?;
                for inner in &inners[1..] {
                    write!(f, " OR {}", inner)?;
                }
                write!(f, ")")
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FilterPredicateSingle {
    pub property_kind: Option<FilterPropertyKind>,
    pub property: String,
    #[serde(flatten)]
    pub value: ValuePredicate,
}

impl Display for FilterPredicateSingle {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self.property_kind {
            Some(FilterPropertyKind::Inherent) => write!(f, "#")?,
            Some(FilterPropertyKind::Attribute) => write!(f, "@")?,
            None => {}
        }

        if name_needs_escapes(&self.property) {
            write!(f, "{:?}", self.property)?;
        } else {
            write!(f, "{}", self.property)?;
        }

        write!(f, ": {}", self.value)?;

        Ok(())
    }
}

fn needs_escapes(s: &str) -> bool {
    s.contains(['"', '\\', '/', '#', '@', ':', '<', '>', '=', '!'])
        || s.contains(|c: char| c.is_whitespace())
        || s.is_empty()
}

fn escape_wildcard(s: &str) -> String {
    s.replace('\"', "\\\"")
}

fn name_needs_escapes(s: &str) -> bool {
    s.contains(|c: char| !c.is_alphabetic() && c != '.' && c != '_') || s.is_empty()
}

mod parsers {
    use super::*;

    use nom::branch::alt;
    use nom::bytes::complete::{escaped, tag, take_while, take_while1};
    use nom::character::complete::{char, none_of, one_of};
    use nom::combinator::{cut, eof, map, map_res, opt};
    use nom::multi::{many0, many0_count, separated_list0};
    use nom::sequence::delimited;
    use nom::{IResult, Parser};

    enum GroupSeparator {
        And,
        Or,
    }

    fn whitespace(input: &str) -> IResult<&str, &str> {
        take_while(|c: char| c.is_whitespace()).parse(input)
    }

    fn expect_whitespace(input: &str) -> IResult<&str, &str> {
        take_while1(|c: char| c.is_whitespace()).parse(input)
    }

    fn escaped_name(input: &str) -> IResult<&str, &str> {
        escaped(none_of("\\\""), '\\', one_of("\"\\*")).parse(input)
    }

    fn quoted_name(input: &str) -> IResult<&str, Option<&str>> {
        delimited(char('\"'), opt(escaped_name), char('\"')).parse(input)
    }

    fn unquoted_name(input: &str) -> IResult<&str, &str> {
        take_while(|c: char| c.is_alphabetic() || c == '.' || c == '_').parse(input)
    }

    fn name(input: &str) -> IResult<&str, &str> {
        alt((map(quoted_name, |res| res.unwrap_or("")), unquoted_name)).parse(input)
    }

    fn inherent_name(input: &str) -> IResult<&str, &str> {
        let (input, _) = char('#').parse(input)?;
        let (input, attr_name) = cut(name).parse(input)?;
        Ok((input, attr_name))
    }

    fn attribute_name(input: &str) -> IResult<&str, &str> {
        let (input, _) = char('@').parse(input)?;
        let (input, attr_name) = cut(name).parse(input)?;
        Ok((input, attr_name))
    }

    fn property(input: &str) -> IResult<&str, (Option<FilterPropertyKind>, &str)> {
        use FilterPropertyKind::*;

        alt((
            map(inherent_name, |name| (Some(Inherent), name)),
            map(attribute_name, |name| (Some(Attribute), name)),
            map(name, |name| (None, name)),
        ))
        .parse(input)
    }

    fn not(input: &str) -> IResult<&str, &str> {
        let (input, _) = whitespace(input)?;
        tag("!").parse(input)
    }

    fn bare_value(input: &str) -> IResult<&str, ValuePredicate> {
        let orig = input;

        let (input, op) = opt(alt((
            map(tag(">="), |_| ValueOperator::Gte),
            map(tag(">"), |_| ValueOperator::Gt),
            map(tag("<="), |_| ValueOperator::Lte),
            map(tag("<"), |_| ValueOperator::Lt),
        )))
        .parse(input)?;
        let (input, value) = alt((
            map_res(quoted_value, |v| {
                let Some(v) = v else {
                    return Ok(ValuePredicate::Comparison(
                        op.unwrap_or(ValueOperator::Eq),
                        String::new(),
                    ));
                };

                if v.contains('*') {
                    if op.is_some() {
                        Err(nom::error::Error::new(orig, nom::error::ErrorKind::Fail))
                    } else {
                        Ok(ValuePredicate::Wildcard(unescape_wildcard(v)))
                    }
                } else {
                    Ok(ValuePredicate::Comparison(
                        op.unwrap_or(ValueOperator::Eq),
                        unescape(v),
                    ))
                }
            }),
            map_res(unquoted_value, |v| {
                if v.contains('*') {
                    if op.is_some() {
                        Err(nom::error::Error::new(orig, nom::error::ErrorKind::Fail))
                    } else {
                        Ok(ValuePredicate::Wildcard(v.to_owned()))
                    }
                } else {
                    Ok(ValuePredicate::Comparison(
                        op.unwrap_or(ValueOperator::Eq),
                        v.to_owned(),
                    ))
                }
            }),
        ))
        .parse(input)?;

        Ok((input, value))
    }

    fn group_list(input: &str) -> IResult<&str, ValuePredicate> {
        let (input, _) = whitespace(input)?;
        let (input, first) = value(input)?;
        let (input, list) = many0((
            whitespace,
            alt((
                map(tag("AND"), |_| GroupSeparator::And),
                map(tag("OR"), |_| GroupSeparator::Or),
            )),
            whitespace,
            value,
        ))
        .parse(input)?;
        let (input, _) = whitespace(input)?;

        if list.is_empty() {
            return Ok((input, first));
        }

        // TODO: I'm sure this can be done better, but the clean solution isn't
        // coming to me at the moment

        let (mut separators, mut values) = list
            .into_iter()
            .map(|(_, sep, _, value)| (sep, value))
            .collect::<(Vec<_>, Vec<_>)>();

        values.insert(0, first);

        let mut i = 0;
        loop {
            if let GroupSeparator::And = separators[i] {
                let lhs = values.remove(i);
                let rhs = values.remove(i);

                let pred = match (lhs, rhs) {
                    (ValuePredicate::And(mut lhs_ands), ValuePredicate::And(rhs_ands)) => {
                        lhs_ands.extend(rhs_ands);
                        ValuePredicate::And(lhs_ands)
                    }
                    (ValuePredicate::And(mut lhs_ands), rhs) => {
                        lhs_ands.push(rhs);
                        ValuePredicate::And(lhs_ands)
                    }
                    (lhs, ValuePredicate::And(mut rhs_ands)) => {
                        rhs_ands.insert(0, lhs);
                        ValuePredicate::And(rhs_ands)
                    }
                    (lhs, rhs) => ValuePredicate::And(vec![lhs, rhs]),
                };

                values.insert(i, pred);
                separators.remove(i);

                if i == separators.len() {
                    break;
                }
            } else if i == separators.len() - 1 {
                break;
            } else {
                i += 1;
            }
        }

        if values.len() == 1 {
            return Ok((input, values.pop().unwrap()));
        }

        Ok((input, ValuePredicate::Or(values)))
    }

    fn grouped_value(input: &str) -> IResult<&str, ValuePredicate> {
        delimited(char('('), group_list, char(')')).parse(input)
    }

    fn regex_inner(input: &str) -> IResult<&str, &str> {
        escaped(none_of("\\/"), '\\', one_of("/")).parse(input)
    }

    fn regex_value(input: &str) -> IResult<&str, ValuePredicate> {
        let (input, regex) = delimited(char('/'), opt(regex_inner), char('/')).parse(input)?;

        let regex = regex.unwrap_or_default().to_owned();

        Ok((input, ValuePredicate::Regex(regex)))
    }

    fn value(input: &str) -> IResult<&str, ValuePredicate> {
        let (input, not_count) = many0_count(not).parse(input)?;
        let (input, value) = alt((grouped_value, regex_value, bare_value)).parse(input)?;

        let value = if not_count % 2 == 1 {
            ValuePredicate::Not(Box::new(value))
        } else {
            value
        };

        Ok((input, value))
    }

    fn escaped_value(input: &str) -> IResult<&str, &str> {
        escaped(none_of("\\\""), '\\', one_of("\"\\*")).parse(input)
    }

    fn quoted_value(input: &str) -> IResult<&str, Option<&str>> {
        delimited(char('\"'), opt(escaped_value), char('\"')).parse(input)
    }

    fn unquoted_value(input: &str) -> IResult<&str, &str> {
        take_while1(|c: char| {
            !c.is_whitespace()
                && c != '"'
                && c != '@'
                && c != '#'
                && c != ':'
                && c != '('
                && c != ')'
        })
        .parse(input)
    }

    fn predicate_list(input: &str) -> IResult<&str, FilterPredicate> {
        let (input, _) = whitespace(input)?;
        let (input, first) = predicate(input)?;
        let (input, list) = many0((
            whitespace,
            alt((
                map(tag("AND"), |_| GroupSeparator::And),
                map(tag("OR"), |_| GroupSeparator::Or),
            )),
            whitespace,
            predicate,
        ))
        .parse(input)?;
        let (input, _) = whitespace(input)?;

        if list.is_empty() {
            return Ok((input, first));
        }

        // TODO: I'm sure this can be done better, but the clean solution isn't
        // coming to me at the moment

        let (mut separators, mut values) = list
            .into_iter()
            .map(|(_, sep, _, value)| (sep, value))
            .collect::<(Vec<_>, Vec<_>)>();

        values.insert(0, first);

        let mut i = 0;
        loop {
            if let GroupSeparator::And = separators[i] {
                let lhs = values.remove(i);
                let rhs = values.remove(i);

                let pred = match (lhs, rhs) {
                    (FilterPredicate::And(mut lhs_ands), FilterPredicate::And(rhs_ands)) => {
                        lhs_ands.extend(rhs_ands);
                        FilterPredicate::And(lhs_ands)
                    }
                    (FilterPredicate::And(mut lhs_ands), rhs) => {
                        lhs_ands.push(rhs);
                        FilterPredicate::And(lhs_ands)
                    }
                    (lhs, FilterPredicate::And(mut rhs_ands)) => {
                        rhs_ands.insert(0, lhs);
                        FilterPredicate::And(rhs_ands)
                    }
                    (lhs, rhs) => FilterPredicate::And(vec![lhs, rhs]),
                };

                values.insert(i, pred);
                separators.remove(i);

                if i == separators.len() {
                    break;
                }
            } else if i == separators.len() - 1 {
                break;
            } else {
                i += 1;
            }
        }

        if values.len() == 1 {
            return Ok((input, values.pop().unwrap()));
        }

        Ok((input, FilterPredicate::Or(values)))
    }

    fn predicate_grouped(input: &str) -> IResult<&str, FilterPredicate> {
        delimited(char('('), predicate_list, char(')')).parse(input)
    }

    fn predicate_single(input: &str) -> IResult<&str, FilterPredicate> {
        let (input, (kind, property)) = property(input)?;
        let (input, _) = whitespace(input)?;
        let (input, _) = char(':').parse(input)?;
        let (input, _) = whitespace(input)?;
        let (input, value) = value(input)?;

        let predicate = FilterPredicate::Single(FilterPredicateSingle {
            property_kind: kind,
            property: property.to_owned(),
            value,
        });

        Ok((input, predicate))
    }

    fn predicate(input: &str) -> IResult<&str, FilterPredicate> {
        alt((predicate_grouped, predicate_single)).parse(input)
    }

    pub fn predicates(input: &str) -> IResult<&str, Vec<FilterPredicate>> {
        let (input, _) = whitespace(input)?;
        let (input, list) = separated_list0(expect_whitespace, predicate).parse(input)?;
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

    fn unescape_wildcard(input: &str) -> String {
        input.replace("\\\"", "\"")
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
