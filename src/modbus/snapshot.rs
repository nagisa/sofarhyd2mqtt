use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Default)]
pub struct Snapshot {
    words: BTreeMap<u16, u16>,
    available: bool,
    last_success: Option<Instant>,
}

impl Snapshot {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, address: u16) -> Option<u16> {
        self.words.get(&address).copied()
    }

    pub fn words(&self, start: u16, count: u16) -> Option<Vec<u16>> {
        (0..count).map(|i| self.get(start.checked_add(i)?)).collect()
    }

    pub fn available(&self) -> bool {
        self.available
    }

    pub(crate) fn expires_at(&self, after: Duration) -> Option<Instant> {
        self.available.then_some(self.last_success? + after)
    }

    pub(crate) fn set(&mut self, address: u16, word: u16) -> bool {
        self.words.insert(address, word) != Some(word)
    }

    pub(crate) fn set_many(&mut self, start: u16, words: &[u16]) -> bool {
        let mut changed = false;
        for (address, &word) in (start..=u16::MAX).zip(words) {
            changed |= self.set(address, word);
        }
        changed
    }

    pub(crate) fn record_success(&mut self, at: Instant) -> bool {
        self.last_success = Some(at);
        self.set_available(true)
    }

    pub(crate) fn set_available(&mut self, available: bool) -> bool {
        std::mem::replace(&mut self.available, available) != available
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn words_are_only_complete_if_all_present() {
        let mut s = Snapshot::new();
        assert!(s.set(10, 1));
        assert!(s.set(11, 2));
        assert!(s.set(13, 4));
        assert!(!s.set(10, 1));
        assert_eq!(s.words(10, 2), Some(vec![1, 2]));
        assert_eq!(s.words(10, 4), None);
        assert_eq!(s.words(11, 1), Some(vec![2]));
        assert_eq!(s.words(65535, 2), None);
    }

    #[test]
    fn many_words_are_set_at_once() {
        let mut s = Snapshot::new();
        assert!(s.set_many(100, &[1, 2, 3]));
        assert!(!s.set_many(100, &[1, 2, 3]));
        assert!(s.set_many(101, &[2, 9]));
        assert_eq!(s.words(100, 3), Some(vec![1, 2, 9]));
        assert!(!s.set_many(100, &[]));
        assert!(s.set_many(65535, &[5, 6]));
        assert_eq!(s.get(65535), Some(5));
        assert_eq!(s.words(65535, 2), None);
    }

    #[test]
    fn availability_follows_successes() {
        let mut s = Snapshot::new();
        let (after, now) = (Duration::from_secs(60), Instant::now());
        assert!(!s.available() && s.expires_at(after).is_none());
        assert!(s.record_success(now));
        assert!(!s.record_success(now));
        assert_eq!(s.expires_at(after), Some(now + after));
        assert!(s.set_available(false));
        assert_eq!(s.expires_at(after), None);
    }
}
