use std::iter::Rev;

use either::Either;

use crate::filter::Order;
use crate::util::{AdvanceUntil, IndexIterator, SetIntersectionIterator, SetUnionIterator};

#[derive(Debug)]
pub(crate) enum CompoundIndexIterator<'a, T: Clone + Ord> {
    Single(IndexIterator<'a, T>),
    And(SetIntersectionIterator<CompoundIndexIterator<'a, T>>),
    Or(SetUnionIterator<CompoundIndexIterator<'a, T>>),
}

impl<'a, T: Clone + Ord> CompoundIndexIterator<'a, T> {
    pub(crate) fn with_order(self, order: Order) -> Either<Self, Rev<Self>> {
        match order {
            Order::Asc => Either::Left(self),
            Order::Desc => Either::Right(self.rev()),
        }
    }
}

impl<'a, T: Clone + Ord> AdvanceUntil for CompoundIndexIterator<'a, T> {
    fn advance_front_until_equals(&mut self, entry: &T) -> bool {
        match self {
            CompoundIndexIterator::Single(iter) => iter.advance_front_until_equals(entry),
            CompoundIndexIterator::And(iter) => iter.advance_front_until_equals(entry),
            CompoundIndexIterator::Or(iter) => iter.advance_front_until_equals(entry),
        }
    }

    fn advance_back_until_equals(&mut self, entry: &T) -> bool {
        match self {
            CompoundIndexIterator::Single(iter) => iter.advance_back_until_equals(entry),
            CompoundIndexIterator::And(iter) => iter.advance_back_until_equals(entry),
            CompoundIndexIterator::Or(iter) => iter.advance_back_until_equals(entry),
        }
    }
}

impl<'a, T: Clone + Ord> Iterator for CompoundIndexIterator<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        match self {
            CompoundIndexIterator::Single(iter) => iter.next(),
            CompoundIndexIterator::And(iter) => iter.next(),
            CompoundIndexIterator::Or(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            CompoundIndexIterator::Single(iter) => iter.size_hint(),
            CompoundIndexIterator::And(iter) => iter.size_hint(),
            CompoundIndexIterator::Or(iter) => iter.size_hint(),
        }
    }
}

impl<'a, T: Clone + Ord> DoubleEndedIterator for CompoundIndexIterator<'a, T> {
    fn next_back(&mut self) -> Option<T> {
        match self {
            CompoundIndexIterator::Single(iter) => iter.next_back(),
            CompoundIndexIterator::And(iter) => iter.next_back(),
            CompoundIndexIterator::Or(iter) => iter.next_back(),
        }
    }
}
