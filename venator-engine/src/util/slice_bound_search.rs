use std::cmp::Ordering;

pub(crate) trait BoundSearch<T> {
    // This finds the first index of an item that is not less than the provided
    // item. This works via a binary-search algorithm.
    //
    // NOTE: The result is only meaningful if the input is sorted.
    fn lower_bound(&self, item: &T) -> usize;

    // This finds the first index of an item that is greater than the provided
    // item. This works via a binary-search algorithm.
    //
    // NOTE: The result is only meaningful if the input is sorted.
    fn upper_bound(&self, item: &T) -> usize;

    // This finds the first index of an item that is not less than the provided
    // item. This works via a binary-expansion-search algorithm, i.e. it checks
    // indexes geometrically starting from the beginning and then uses binary
    // -search within those bounds. This method is good if the item is expected
    // near the beginning.
    //
    // NOTE: The result is only meaningful if the input is sorted.
    fn lower_bound_via_expansion(&self, item: &T) -> usize;

    // This finds the first index of an item that is greater than the provided
    // item. This works via a binary-expansion-search algorithm, i.e. it checks
    // indexes geometrically starting from the end and then uses binary-search
    // within those bounds. This method is good if the item is expected near the
    // end.
    //
    // NOTE: The result is only meaningful if the input is sorted.
    fn upper_bound_via_expansion(&self, item: &T) -> usize;
}

impl<T: Ord> BoundSearch<T> for [T] {
    fn lower_bound(&self, item: &T) -> usize {
        self.binary_search_by(|current_item| match current_item.cmp(item) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => Ordering::Greater,
            Ordering::Less => Ordering::Less,
        })
        .unwrap_or_else(|idx| idx)
    }

    fn upper_bound(&self, item: &T) -> usize {
        self.binary_search_by(|current_item| match current_item.cmp(item) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => Ordering::Less,
            Ordering::Less => Ordering::Less,
        })
        .unwrap_or_else(|idx| idx)
    }

    fn lower_bound_via_expansion(&self, item: &T) -> usize {
        let len = self.len();
        for (start, mut end) in std::iter::successors(Some((0, 1)), |&(_, j)| Some((j, j * 2))) {
            if end >= len {
                end = len
            } else if &self[end] < item {
                continue;
            }

            return self[start..end].lower_bound(item) + start;
        }

        unreachable!()
    }

    fn upper_bound_via_expansion(&self, item: &T) -> usize {
        let len = self.len();
        for (start, mut end) in std::iter::successors(Some((0, 1)), |&(_, j)| Some((j, j * 2))) {
            if end >= len {
                end = len
            } else if &self[len - end] > item {
                continue;
            }

            return self[len - end..len - start].upper_bound(item) + (len - end);
        }

        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_on_empty_slice() {
        assert_eq!([].lower_bound(&0), 0);
        assert_eq!([].upper_bound(&0), 0);
        assert_eq!([].lower_bound_via_expansion(&0), 0);
        assert_eq!([].upper_bound_via_expansion(&0), 0);
    }

    #[test]
    fn bounds_on_single_slice() {
        assert_eq!([1].lower_bound(&0), 0);
        assert_eq!([1].upper_bound(&0), 0);
        assert_eq!([1].lower_bound_via_expansion(&0), 0);
        assert_eq!([1].upper_bound_via_expansion(&0), 0);

        assert_eq!([1].lower_bound(&1), 0);
        assert_eq!([1].upper_bound(&1), 1);
        assert_eq!([1].lower_bound_via_expansion(&1), 0);
        assert_eq!([1].upper_bound_via_expansion(&1), 1);

        assert_eq!([1].lower_bound(&2), 1);
        assert_eq!([1].upper_bound(&2), 1);
        assert_eq!([1].lower_bound_via_expansion(&2), 1);
        assert_eq!([1].upper_bound_via_expansion(&2), 1);
    }

    #[test]
    fn bounds_for_duplicate_item() {
        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound(&-1), 0);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound(&-1), 0);
        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound_via_expansion(&-1), 0);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound_via_expansion(&-1), 0);

        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound(&0), 0);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound(&0), 2);
        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound_via_expansion(&0), 0);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound_via_expansion(&0), 2);

        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound(&1), 2);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound(&1), 4);
        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound_via_expansion(&1), 2);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound_via_expansion(&1), 4);

        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound(&2), 4);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound(&2), 6);
        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound_via_expansion(&2), 4);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound_via_expansion(&2), 6);

        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound(&3), 6);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound(&3), 6);
        assert_eq!([0, 0, 1, 1, 2, 2].lower_bound_via_expansion(&3), 6);
        assert_eq!([0, 0, 1, 1, 2, 2].upper_bound_via_expansion(&3), 6);
    }

    #[test]
    fn bounds_for_missing_item() {
        assert_eq!([0, 0, 2, 2].lower_bound(&1), 2);
        assert_eq!([0, 0, 2, 2].upper_bound(&1), 2);
        assert_eq!([0, 0, 2, 2].lower_bound_via_expansion(&1), 2);
        assert_eq!([0, 0, 2, 2].upper_bound_via_expansion(&1), 2);
    }
}
