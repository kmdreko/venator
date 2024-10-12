use std::cmp::Ordering;

use crate::filter::BoundSearch;

pub trait IndexExt<T> {
    /// This is intended to remove elements in an efficient way for sorted
    /// `self` and `list`.
    fn remove_list_sorted(&mut self, list: &[T]);
}

impl<T: Ord> IndexExt<T> for Vec<T> {
    fn remove_list_sorted(&mut self, list: &[T]) {
        let mut i = 0;
        let mut j = 0;

        while let Some((ii, jj)) = find_next_match(&self[i..], &list[j..]) {
            // TODO: this can be done more efficiently with unsafe shenanigans -
            // as it is, this is O(n^2) when it could be O(n)
            self.remove(i + ii);

            i += ii;
            j += jj + 1;
        }
    }
}

// Returns the indexes from the respective lists of the first element that is
// found in both. This assumes both lists are sorted.
fn find_next_match<T: Ord>(a: &[T], b: &[T]) -> Option<(usize, usize)> {
    if a.is_empty() || b.is_empty() {
        return None;
    }

    let mut i = 0;
    let mut j = 0;

    loop {
        match Ord::cmp(&a[i], &b[j]) {
            Ordering::Equal => return Some((i, j)),
            Ordering::Less => {
                i = a[i..].lower_bound_via_expansion(&b[j]);
                if i == a.len() {
                    return None;
                }
            }
            Ordering::Greater => {
                j = b[j..].lower_bound_via_expansion(&a[i]);
                if j == b.len() {
                    return None;
                }
            }
        }
    }
}
