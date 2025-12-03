use crate::util::AdvanceUntil;

#[derive(Debug)]
pub(crate) struct DoubleEndedPeekable<I: Iterator> {
    iter: I,
    pub(super) next_front: Option<I::Item>,
    pub(super) next_back: Option<I::Item>,
}

impl<I: Iterator> DoubleEndedPeekable<I> {
    pub(crate) fn new(iter: I) -> DoubleEndedPeekable<I> {
        DoubleEndedPeekable {
            iter,
            next_front: None,
            next_back: None,
        }
    }

    pub(crate) fn peek_front(&mut self) -> Option<&I::Item> {
        if let Some(ref item) = self.next_front {
            return Some(item);
        }

        if let Some(item) = self.iter.next() {
            self.next_front = Some(item);
            return self.next_front.as_ref();
        }

        self.next_back.as_ref()
    }
}

impl<I: DoubleEndedIterator> DoubleEndedPeekable<I> {
    pub(crate) fn peek_back(&mut self) -> Option<&I::Item> {
        if let Some(ref item) = self.next_back {
            return Some(item);
        }

        if let Some(item) = self.iter.next_back() {
            self.next_back = Some(item);
            return self.next_back.as_ref();
        }

        self.next_front.as_ref()
    }
}

impl<I: Iterator> Iterator for DoubleEndedPeekable<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.next_front.take() {
            return Some(item);
        }

        if let Some(item) = self.iter.next() {
            return Some(item);
        }

        self.next_back.take()
    }
}

impl<I: DoubleEndedIterator> DoubleEndedIterator for DoubleEndedPeekable<I> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.next_back.take() {
            return Some(item);
        }

        if let Some(item) = self.iter.next_back() {
            return Some(item);
        }

        self.next_front.take()
    }
}

impl<I: AdvanceUntil<Item: Ord>> AdvanceUntil for DoubleEndedPeekable<I> {
    fn advance_front_until_equals(&mut self, item: &Self::Item) -> bool {
        match &self.next_front {
            Some(front) if front == item => return true,
            Some(front) if front > item => return false,
            Some(_front) => {
                self.next_front = None;
            }
            None => {}
        }

        if self.iter.advance_front_until_equals(item) {
            return true;
        }

        match &self.next_back {
            Some(back) if back == item => return true,
            Some(back) if back > item => return false,
            Some(_back) => {
                self.next_back = None;
            }
            None => {}
        }

        false
    }

    fn advance_back_until_equals(&mut self, item: &Self::Item) -> bool {
        match &self.next_back {
            Some(back) if back == item => return true,
            Some(back) if back < item => return false,
            Some(_back) => {
                self.next_back = None;
            }
            None => {}
        }

        if self.iter.advance_back_until_equals(item) {
            return true;
        }

        match &self.next_front {
            Some(front) if front == item => return true,
            Some(front) if front < item => return false,
            Some(_front) => {
                self.next_front = None;
            }
            None => {}
        }

        false
    }
}
