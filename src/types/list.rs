use std::collections::VecDeque;
use std::mem;

use bytes::Bytes;

use crate::encoding::{LIST_MAX_LISTPACK_ENTRIES, LIST_MAX_LISTPACK_VALUE};

/// Internal encoding for a List value.
///
/// REDIS: Small lists use a listpack (contiguous memory). Large lists use a
/// quicklist — a doubly-linked list of listpack nodes. We simplify quicklist
/// to a plain VecDeque<Bytes>: it captures the O(1) head/tail semantics
/// Redis needs for LPUSH/RPUSH/LPOP/RPOP without reimplementing the node
/// management of quicklist.c.
///
/// The two variants are kept distinct so callers can observe (and tests can
/// assert on) which encoding is active, mirroring OBJECT ENCODING in Redis.
#[derive(Debug, Clone)]
enum Encoding {
    Listpack(VecDeque<Bytes>),
    Linked(VecDeque<Bytes>),
}

#[derive(Debug, Clone)]
pub(crate) struct List(Encoding);

impl List {
    pub(crate) fn new() -> Self {
        Self(Encoding::Listpack(VecDeque::new()))
    }

    /// Push to the front of the list (LPUSH). Promotes if needed.
    pub(crate) fn push_front(&mut self, value: Bytes) {
        if self.is_listpack() && value.len() > LIST_MAX_LISTPACK_VALUE {
            self.promote();
        }
        self.deque_mut().push_front(value);
        if self.is_listpack() && self.len() > LIST_MAX_LISTPACK_ENTRIES {
            self.promote();
        }
    }

    /// Push to the back of the list (RPUSH). Promotes if needed.
    pub(crate) fn push_back(&mut self, value: Bytes) {
        if self.is_listpack() && value.len() > LIST_MAX_LISTPACK_VALUE {
            self.promote();
        }
        self.deque_mut().push_back(value);
        if self.is_listpack() && self.len() > LIST_MAX_LISTPACK_ENTRIES {
            self.promote();
        }
    }

    /// Remove and return the front element (LPOP).
    pub(crate) fn pop_front(&mut self) -> Option<Bytes> {
        self.deque_mut().pop_front()
    }

    /// Remove and return the back element (RPOP).
    pub(crate) fn pop_back(&mut self) -> Option<Bytes> {
        self.deque_mut().pop_back()
    }

    /// Return elements in the range [start, stop] (inclusive), supporting
    /// negative indices. Out-of-bounds indices are clamped.
    ///
    /// REDIS: LRANGE — see t_list.c listTypeGetRange().
    /// Negative index: -1 = last element, -2 = second to last, etc.
    pub(crate) fn range(&self, start: i64, stop: i64) -> Vec<Bytes> {
        let len = self.len() as i64;
        if len == 0 {
            return vec![];
        }

        let start = (if start < 0 { len + start } else { start }).max(0) as usize;
        let stop = (if stop < 0 { len + stop } else { stop }).min(len - 1);

        if stop < 0 || start as i64 > stop {
            return vec![];
        }
        let stop = stop as usize;

        self.deque()
            .iter()
            .skip(start)
            .take(stop - start + 1)
            .cloned()
            .collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.deque().len()
    }

    /// Rough size estimate used for maxmemory enforcement.
    ///
    /// REDIS: quicklists carry more structural overhead than small listpacks.
    /// We keep the estimate simple but make promotion visible to memory policy.
    pub(crate) fn estimated_size(&self) -> usize {
        match &self.0 {
            Encoding::Listpack(deque) => 16 + deque.iter().map(|v| v.len() + 8).sum::<usize>(),
            Encoding::Linked(deque) => 32 + deque.iter().map(|v| v.len() + 32).sum::<usize>(),
        }
    }

    /// Iterate over elements front-to-back regardless of encoding.
    ///
    /// REDIS: Used by RDB serialization to dump all list elements.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &Bytes> {
        self.deque().iter()
    }

    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // Used in tests and will serve OBJECT ENCODING when that command is added.
    #[allow(dead_code)]
    pub(crate) fn encoding_name(&self) -> &'static str {
        match &self.0 {
            Encoding::Listpack(_) => "listpack",
            Encoding::Linked(_) => "quicklist",
        }
    }

    fn is_listpack(&self) -> bool {
        matches!(&self.0, Encoding::Listpack(_))
    }

    fn deque(&self) -> &VecDeque<Bytes> {
        match &self.0 {
            Encoding::Listpack(d) | Encoding::Linked(d) => d,
        }
    }

    fn deque_mut(&mut self) -> &mut VecDeque<Bytes> {
        match &mut self.0 {
            Encoding::Listpack(d) | Encoding::Linked(d) => d,
        }
    }

    /// Listpack → Linked (quicklist). Data moves without copying.
    fn promote(&mut self) {
        // REDIS: listTypeConvert() in t_list.c.
        let old = mem::replace(&mut self.0, Encoding::Linked(VecDeque::new()));
        if let Encoding::Listpack(deque) = old {
            self.0 = Encoding::Linked(deque);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    // --- basic push/pop ---

    #[test]
    fn new_list_uses_listpack() {
        assert_eq!(List::new().encoding_name(), "listpack");
    }

    #[test]
    fn push_front_and_pop_front() {
        let mut l = List::new();
        l.push_front(b("a"));
        l.push_front(b("b"));
        assert_eq!(l.pop_front(), Some(b("b")));
        assert_eq!(l.pop_front(), Some(b("a")));
        assert_eq!(l.pop_front(), None);
    }

    #[test]
    fn push_back_and_pop_back() {
        let mut l = List::new();
        l.push_back(b("a"));
        l.push_back(b("b"));
        assert_eq!(l.pop_back(), Some(b("b")));
        assert_eq!(l.pop_back(), Some(b("a")));
    }

    #[test]
    fn lpush_ordering_matches_redis() {
        // REDIS: LPUSH k a b c results in [c, b, a] — each value pushed to front.
        let mut l = List::new();
        for v in [b("a"), b("b"), b("c")] {
            l.push_front(v);
        }
        assert_eq!(l.range(0, -1), vec![b("c"), b("b"), b("a")]);
    }

    #[test]
    fn rpush_ordering_matches_redis() {
        let mut l = List::new();
        for v in [b("a"), b("b"), b("c")] {
            l.push_back(v);
        }
        assert_eq!(l.range(0, -1), vec![b("a"), b("b"), b("c")]);
    }

    // --- range ---

    #[test]
    fn range_full_list() {
        let mut l = List::new();
        l.push_back(b("x"));
        l.push_back(b("y"));
        l.push_back(b("z"));
        assert_eq!(l.range(0, -1), vec![b("x"), b("y"), b("z")]);
    }

    #[test]
    fn range_positive_indices() {
        let mut l = List::new();
        for v in ["a", "b", "c", "d"] {
            l.push_back(Bytes::from(v));
        }
        assert_eq!(l.range(1, 2), vec![b("b"), b("c")]);
    }

    #[test]
    fn range_negative_indices() {
        let mut l = List::new();
        for v in ["a", "b", "c", "d"] {
            l.push_back(Bytes::from(v));
        }
        // -2 to -1 = last two elements
        assert_eq!(l.range(-2, -1), vec![b("c"), b("d")]);
    }

    #[test]
    fn range_out_of_bounds_is_clamped() {
        let mut l = List::new();
        l.push_back(b("a"));
        l.push_back(b("b"));
        assert_eq!(l.range(0, 100), vec![b("a"), b("b")]);
    }

    #[test]
    fn range_empty_list_returns_empty() {
        assert_eq!(List::new().range(0, -1), Vec::<Bytes>::new());
    }

    #[test]
    fn range_start_after_stop_returns_empty() {
        let mut l = List::new();
        l.push_back(b("a"));
        assert_eq!(l.range(5, 2), Vec::<Bytes>::new());
    }

    // --- len ---

    #[test]
    fn len_is_correct() {
        let mut l = List::new();
        assert_eq!(l.len(), 0);
        l.push_back(b("a"));
        l.push_back(b("b"));
        assert_eq!(l.len(), 2);
        l.pop_front();
        assert_eq!(l.len(), 1);
    }

    // --- promotion: entry count ---

    #[test]
    fn promotes_to_quicklist_when_count_exceeds_threshold() {
        let mut l = List::new();
        for i in 0..=LIST_MAX_LISTPACK_ENTRIES {
            l.push_back(Bytes::from(format!("v{i}")));
        }
        assert_eq!(l.encoding_name(), "quicklist");
    }

    #[test]
    fn stays_listpack_at_exactly_threshold() {
        let mut l = List::new();
        for i in 0..LIST_MAX_LISTPACK_ENTRIES {
            l.push_back(Bytes::from(format!("v{i}")));
        }
        assert_eq!(l.encoding_name(), "listpack");
    }

    // --- promotion: value size ---

    #[test]
    fn promotes_when_value_exceeds_size_threshold() {
        let mut l = List::new();
        l.push_back(Bytes::from(vec![b'x'; LIST_MAX_LISTPACK_VALUE + 1]));
        assert_eq!(l.encoding_name(), "quicklist");
    }

    // --- operations work after promotion ---

    #[test]
    fn push_pop_range_work_after_promotion() {
        let mut l = List::new();
        l.push_back(Bytes::from(vec![b'x'; LIST_MAX_LISTPACK_VALUE + 1]));
        l.push_back(b("after"));
        assert_eq!(l.len(), 2);
        assert_eq!(l.pop_back(), Some(b("after")));
    }

    #[test]
    fn promotion_changes_estimated_size() {
        let mut l = List::new();
        l.push_back(b("small"));
        let before = l.estimated_size();

        l.push_back(Bytes::from(vec![b'x'; LIST_MAX_LISTPACK_VALUE + 1]));
        assert_eq!(l.encoding_name(), "quicklist");
        assert!(l.estimated_size() > before);
    }
}
