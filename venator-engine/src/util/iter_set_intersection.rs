use crate::util::AdvanceUntil;

#[derive(Debug)]
pub struct SetIntersectionIterator<I> {
    iters: Box<[I]>,
}

impl<I: Iterator> SetIntersectionIterator<I> {
    pub fn new<II: IntoIterator<Item = I>>(iters: II) -> SetIntersectionIterator<I> {
        SetIntersectionIterator {
            iters: iters.into_iter().collect(),
        }
    }
}

impl<I: AdvanceUntil<Item: Eq>> AdvanceUntil for SetIntersectionIterator<I> {
    fn advance_front_until_equals(&mut self, entry: &I::Item) -> bool {
        self.iters
            .iter_mut()
            .all(|iter| iter.advance_front_until_equals(entry))
    }

    fn advance_back_until_equals(&mut self, entry: &I::Item) -> bool {
        self.iters
            .iter_mut()
            .all(|iter| iter.advance_back_until_equals(entry))
    }
}

impl<I: AdvanceUntil<Item: Eq>> Iterator for SetIntersectionIterator<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        'outer: loop {
            let item = self.iters.first_mut()?.next()?;

            for iter in &mut self.iters[1..] {
                if !iter.advance_front_until_equals(&item) {
                    continue 'outer;
                }
            }

            return Some(item);
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // With multiple filters AND-ed together, the potential min
        // is zero (where none agree) and potential max is the
        // smallest maximum.
        let max = self
            .iters
            .iter()
            .filter_map(|iter| iter.size_hint().1)
            .min();

        (0, max)
    }
}

impl<I: AdvanceUntil<Item: Eq>> DoubleEndedIterator for SetIntersectionIterator<I> {
    fn next_back(&mut self) -> Option<Self::Item> {
        'outer: loop {
            let item = self.iters.first_mut()?.next_back()?;

            for iter in &mut self.iters[1..] {
                if !iter.advance_back_until_equals(&item) {
                    continue 'outer;
                }
            }

            return Some(item);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::util::IndexIterator;

    use super::*;

    #[test]
    fn verify_if_second_matches() {
        let mut iter = SetIntersectionIterator::new([
            IndexIterator::new(&[1, 2, 3], None),
            IndexIterator::new(&[2], None), // next item after first matches
        ]);

        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.next(), None);
    }
}
