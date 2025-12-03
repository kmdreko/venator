/// This trait essentially requires an iterator to be sorted and peekable.
pub trait AdvanceUntil: DoubleEndedIterator {
    /// This will advance the iterator forwards to where the given item is
    /// expected and then yields whether it exists.
    ///
    /// This is an optimization function and implementations should ensure that
    /// processing stops immediately when it is known whether `item` exists or
    /// cannot exist in the iterator.
    fn advance_front_until_equals(&mut self, item: &Self::Item) -> bool;

    /// This will advance the iterator backwards to where the given item is
    /// expected and then yields whether it exists.
    ///
    /// This is an optimization function and implementations should ensure that
    /// processing stops immediately when it is known whether `item` exists or
    /// cannot exist in the iterator.
    fn advance_back_until_equals(&mut self, item: &Self::Item) -> bool;
}
