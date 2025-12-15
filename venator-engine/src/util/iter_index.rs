use std::borrow::Cow;
use std::fmt::{Debug, Error as FmtError, Formatter};

use crate::util::{AdvanceUntil, BoundSearch};

pub(crate) struct IndexIterator<'a, T: Clone> {
    // TODO: this can probably be more efficient with iterators and #![feature(iter_advance_by)]
    index: Cow<'a, [T]>,
    filter: Option<Box<dyn FnMut(&T) -> bool + 'a>>,
}

impl<'a, T> IndexIterator<'a, T>
where
    T: Clone,
{
    pub(crate) fn new(
        index: impl Into<Cow<'a, [T]>>,
        filter: Option<Box<dyn FnMut(&T) -> bool + 'a>>,
    ) -> IndexIterator<'a, T> {
        IndexIterator {
            index: index.into(),
            filter,
        }
    }
}

impl<'a, T: Clone + Ord> AdvanceUntil for IndexIterator<'a, T> {
    fn advance_front_until_equals(&mut self, item: &T) -> bool {
        let idx = self.index.lower_bound(item);
        match &mut self.index {
            Cow::Borrowed(index) => {
                *index = &index[idx..];
            }
            Cow::Owned(index) => {
                index.drain(..idx);
            }
        }

        self.index.first().is_some_and(|first| first == item)
            && self.filter.as_mut().is_none_or(|f| f(item))
    }

    fn advance_back_until_equals(&mut self, item: &T) -> bool {
        let idx = self.index.upper_bound(item);
        match &mut self.index {
            Cow::Borrowed(index) => {
                *index = &index[..idx];
            }
            Cow::Owned(index) => {
                index.truncate(idx);
            }
        }

        self.index.last().is_some_and(|last| last == item)
            && self.filter.as_mut().is_none_or(|f| f(item))
    }
}

impl<'a, T: Clone> Iterator for IndexIterator<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        for (i, t) in self.index.iter().enumerate() {
            if let Some(matches) = &mut self.filter {
                if !matches(t) {
                    continue; // go to the next item
                }
            }

            let t = t.clone();

            match &mut self.index {
                Cow::Borrowed(index) => {
                    *index = &index[i + 1..];
                }
                Cow::Owned(index) => {
                    index.drain(..i + 1);
                }
            }

            return Some(t);
        }

        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.filter {
            Some(_) => (0, Some(self.index.len())),
            None => (self.index.len(), Some(self.index.len())),
        }
    }
}

impl<'a, T: Clone + Ord> DoubleEndedIterator for IndexIterator<'a, T> {
    fn next_back(&mut self) -> Option<T> {
        for (i, t) in self.index.iter().enumerate().rev() {
            if let Some(matches) = &mut self.filter {
                if !matches(t) {
                    continue; // go to the next item
                }
            }

            let t = t.clone();

            match &mut self.index {
                Cow::Borrowed(index) => {
                    *index = &index[..i];
                }
                Cow::Owned(index) => {
                    index.truncate(i);
                }
            }

            return Some(t);
        }

        None
    }
}

#[rustfmt::skip]
impl<T: Debug + Clone> Debug for IndexIterator<'_, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        struct IndexSample<'a, T> {
            index: &'a [T],
        }

        impl<T: Debug> Debug for IndexSample<'_, T> {
            fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
                write!(f, "{} items ", self.index.len())?;

                match self.index {
                    [] => write!(f, "[]")?,
                    [x] => write!(f, "[{x:?}]")?,
                    [x, y] => write!(f, "[{x:?}, {y:?}]")?,
                    [x, .., y] => write!(f, "[{x:?}, .., {y:?}]")?,
                }

                Ok(())
            }
        }

        struct FilterSample<'a, 'b, T> {
            filter: &'b Option<Box<dyn FnMut(&T) -> bool + 'a>>,
        }

        impl<T> Debug for FilterSample<'_, '_, T> {
            fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
                match self.filter {
                    Some(_) => write!(f, "Some(...)"),
                    None => write!(f, "None"),
                }
            }
        }

        f.debug_struct("IndexIterator")
            .field("index", &IndexSample { index: &*self.index })
            .field("filter", &FilterSample { filter: &self.filter })
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_iterator_with_filter() {
        let mut iter =
            IndexIterator::new(&[0, 1, 2, 3, 4, 5, 6, 7, 8], Some(Box::new(|i| i % 2 == 0)));

        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.next(), Some(4));
        assert_eq!(iter.next(), Some(6));
        assert_eq!(iter.next(), Some(8));
        assert_eq!(iter.next(), None);

        let mut iter = IndexIterator::new(
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8],
            Some(Box::new(|i| i % 2 == 0)),
        );

        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.next(), Some(4));
        assert_eq!(iter.next(), Some(6));
        assert_eq!(iter.next(), Some(8));
        assert_eq!(iter.next(), None);
    }
}
