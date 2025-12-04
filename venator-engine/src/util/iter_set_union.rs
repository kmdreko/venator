use std::fmt::Debug;

use crate::util::{AdvanceUntil, DoubleEndedPeekable};

pub struct SetUnionIterator<I: Iterator> {
    iters: Box<[DoubleEndedPeekable<I>]>,
    heap_front: Option<Box<[usize]>>,
    heap_back: Option<Box<[usize]>>,
    distinct: bool,
}

impl<I: Iterator> SetUnionIterator<I> {
    pub fn new<II: IntoIterator<Item: IntoIterator<IntoIter = I>>>(
        iters: II,
        distinct: bool, // indicates that the iterators do not overlap
    ) -> SetUnionIterator<I> {
        SetUnionIterator {
            iters: iters
                .into_iter()
                .map(|ii| DoubleEndedPeekable::new(ii.into_iter()))
                .collect(),
            heap_front: None,
            heap_back: None,
            distinct,
        }
    }
}

impl<I: Iterator<Item: Ord>> SetUnionIterator<I> {
    fn cmp_front(iters: &mut [DoubleEndedPeekable<I>], a: usize, b: usize) -> bool {
        let [a_iter, b_iter] = unsafe { iters.get_disjoint_unchecked_mut([a, b]) };

        match (a_iter.peek_front(), b_iter.peek_front()) {
            (Some(a_item), Some(b_item)) => a_item < b_item,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => false,
        }
    }

    fn pop_front(&mut self) -> Option<I::Item> {
        let mut heap = unsafe { self.heap_front.as_mut().unwrap_unchecked() };

        let front_idx = *heap.first()?;
        let front_had_back = self.iters[front_idx].next_back.is_some();
        let front_item = self.iters[front_idx].next()?;
        let front_has_back = self.iters[front_idx].next_back.is_some();

        heapify_one(&mut heap, 0, &mut |a, b| {
            Self::cmp_front(&mut self.iters, *a, *b)
        });

        // If the reverse heap is built and we took the last item, we could have
        // unbalanced it. This is fixable by finding `front_idx` and heapifying
        // that position, but right now we just clear it since it can be rebuilt
        // if needed again.
        if self.heap_back.is_some() && front_had_back && !front_has_back {
            self.heap_back = None;
        }

        Some(front_item)
    }

    /// Returns a reference to the next value to be yielded from `next()`.
    ///
    /// This is not meant for public usage since it assumes the front heap is
    /// already built.
    fn peek_front(&mut self) -> Option<&I::Item> {
        let heap = unsafe { self.heap_front.as_mut().unwrap_unchecked() };
        let front_idx = *heap.first()?;

        self.iters[front_idx].peek_front()
    }
}

impl<I: DoubleEndedIterator<Item: Ord>> SetUnionIterator<I> {
    fn cmp_back(iters: &mut [DoubleEndedPeekable<I>], a: usize, b: usize) -> bool {
        let [a_iter, b_iter] = unsafe { iters.get_disjoint_unchecked_mut([a, b]) };

        match (a_iter.peek_back(), b_iter.peek_back()) {
            (Some(a_item), Some(b_item)) => a_item > b_item,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => false,
        }
    }

    fn pop_back(&mut self) -> Option<I::Item> {
        let mut heap = unsafe { self.heap_back.as_mut().unwrap_unchecked() };

        let back_idx = *heap.first()?;
        let back_had_front = self.iters[back_idx].next_front.is_some();
        let back_item = self.iters[back_idx].next_back()?;
        let back_has_front = self.iters[back_idx].next_front.is_some();

        heapify_one(&mut heap, 0, &mut |a, b| {
            Self::cmp_back(&mut self.iters, *a, *b)
        });

        // If the reverse heap is built and we took the last item, we could have
        // unbalanced it. This is fixable by finding `back_idx` and heapifying
        // that position, but right now we just clear it since it can be rebuilt
        // if needed again.
        if self.heap_front.is_some() && back_had_front && !back_has_front {
            self.heap_front = None;
        }

        Some(back_item)
    }

    /// Returns a reference to the next value to be yielded from `next_back()`.
    ///
    /// This is not meant for public usage since it assumes the back heap is
    /// already built.
    fn peek_back(&mut self) -> Option<&I::Item> {
        let heap = unsafe { self.heap_back.as_mut().unwrap_unchecked() };
        let back_idx = *heap.first()?;

        self.iters[back_idx].peek_back()
    }
}

impl<I: Iterator<Item: Ord> + AdvanceUntil> AdvanceUntil for SetUnionIterator<I> {
    fn advance_front_until_equals(&mut self, item: &I::Item) -> bool {
        // we could try to re-heapify these, but in the current code if this
        // method is called then it is unlikely we will call `next()` again
        if let Some(_) = &mut self.heap_front {
            self.heap_front = None;
        }

        if let Some(_) = &mut self.heap_back {
            self.heap_back = None;
        }

        self.iters
            .iter_mut()
            .any(|iter| iter.advance_front_until_equals(item))
    }

    fn advance_back_until_equals(&mut self, item: &I::Item) -> bool {
        // we could try to re-heapify these, but in the current code if this
        // method is called then it is unlikely we will call `next_back()` again
        if let Some(_) = &mut self.heap_front {
            self.heap_front = None;
        }

        if let Some(_) = &mut self.heap_back {
            self.heap_back = None;
        }

        self.iters
            .iter_mut()
            .any(|iter| iter.advance_back_until_equals(item))
    }
}

impl<I: Iterator<Item: Ord>> Iterator for SetUnionIterator<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let _ = self.heap_front.get_or_insert_with(|| {
            let mut items = (0..self.iters.len()).collect::<Box<[_]>>();

            heapify(&mut items, &mut |a, b| {
                Self::cmp_front(&mut self.iters, *a, *b)
            });
            items
        });

        let item = self.pop_front()?;

        while self.peek_front().is_some_and(|i| *i == item) {
            let _ = self.pop_front();
        }

        Some(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.distinct {
            // With distinct filters OR'd together, the minimums and maximums
            // can simply be aggregated.
            self.iters
                .iter()
                .fold((0, Some(0)), |(a_min, a_max), filter| {
                    let (min, max) = filter.size_hint();
                    (
                        a_min + min,
                        Option::zip(a_max, max).map(|(a_max, max)| a_max + max),
                    )
                })
        } else {
            // With non-distinct filters OR'd together, the potential min is the
            // largest minimum and potential max is the sum of maximums.
            self.iters
                .iter()
                .fold((0, Some(0)), |(a_min, a_max), filter| {
                    let (min, max) = filter.size_hint();
                    (
                        usize::max(a_min, min),
                        Option::zip(a_max, max).map(|(a_max, max)| a_max + max),
                    )
                })
        }
    }
}

impl<I: DoubleEndedIterator<Item: Ord>> DoubleEndedIterator for SetUnionIterator<I> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let _ = self.heap_back.get_or_insert_with(|| {
            let mut items = (0..self.iters.len()).collect::<Box<[_]>>();

            heapify(&mut items, &mut |a, b| {
                Self::cmp_back(&mut self.iters, *a, *b)
            });
            items
        });

        let item = self.pop_back()?;

        while self.peek_back().is_some_and(|i| *i == item) {
            let _ = self.pop_back();
        }

        Some(item)
    }
}

impl<I: Iterator<Item: Debug> + Debug> Debug for SetUnionIterator<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SetUnionIterator")
            .field("iters", &self.iters)
            .finish()
    }
}

fn heapify_one<T, F: FnMut(&T, &T) -> bool>(arr: &mut [T], i: usize, f: &mut F) {
    let mut curr = i;

    loop {
        let mut min = curr;
        let l = 2 * curr + 1;
        let r = 2 * curr + 2;

        if l < arr.len() && f(&arr[l], &arr[min]) {
            min = l;
        }

        if r < arr.len() && f(&arr[r], &arr[min]) {
            min = r;
        }

        if min != curr {
            arr.swap(curr, min);
            curr = min;
        } else {
            break;
        }
    }
}

fn heapify<T, F: FnMut(&T, &T) -> bool>(arr: &mut [T], f: &mut F) {
    for i in (0..arr.len() / 2).rev() {
        heapify_one(arr, i, f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn union_iterator() {
        let mut iter =
            SetUnionIterator::new([vec![1, 2, 4, 5], vec![1, 5, 6, 7], vec![1, 2, 6]], false);

        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.next(), Some(4));
        assert_eq!(iter.next(), Some(5));
        assert_eq!(iter.next(), Some(6));
        assert_eq!(iter.next(), Some(7));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);

        let mut iter =
            SetUnionIterator::new([vec![1, 2, 4, 5], vec![1, 5, 6, 7], vec![1, 2, 6]], false);

        assert_eq!(iter.next_back(), Some(7));
        assert_eq!(iter.next_back(), Some(6));
        assert_eq!(iter.next_back(), Some(5));
        assert_eq!(iter.next_back(), Some(4));
        assert_eq!(iter.next_back(), Some(2));
        assert_eq!(iter.next_back(), Some(1));
        assert_eq!(iter.next_back(), None);
        assert_eq!(iter.next_back(), None);

        let mut iter =
            SetUnionIterator::new([vec![1, 2, 4, 5], vec![1, 5, 6, 7], vec![1, 2, 6]], false);

        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.next_back(), Some(7));
        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.next_back(), Some(6));
        assert_eq!(iter.next(), Some(4));
        assert_eq!(iter.next_back(), Some(5));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next_back(), None);
    }
}
