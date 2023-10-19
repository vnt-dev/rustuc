use std::hash::Hash;
use std::sync::atomic::{AtomicPtr, Ordering};

pub(crate) struct Node<K, V> {
    pub(crate) hash: usize,
    pub(crate) key: *const K,
    pub(crate) val: *mut V,
    pub(crate) next: AtomicPtr<Node<K, V>>,
    pub(crate) prev: AtomicPtr<Node<K, V>>,
}

impl<K, V> PartialEq<Self> for Node<K, V>
where
    K: Eq,
{
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.key == other.key
    }
}

impl<K, V> Eq for Node<K, V> where K: Eq {}

impl<K, V> Node<K, V>
where
    K: Hash + Eq,
{
    pub(crate) fn into_box(self) -> *mut Node<K, V> {
        Box::into_raw(Box::new(self))
    }
    pub(crate) fn new(hash: usize, key: *const K, val: *mut V) -> Node<K, V> {
        Self {
            hash,
            key,
            val,
            next: AtomicPtr::default(),
            prev: AtomicPtr::default(),
        }
    }
    pub(crate) fn new_next(
        hash: usize,
        key: *const K,
        val: *mut V,
        next: *mut Node<K, V>,
    ) -> Node<K, V> {
        Self {
            hash,
            key,
            val,
            next: AtomicPtr::new(next),
            prev: AtomicPtr::default(),
        }
    }
    pub(crate) unsafe fn find(&self, h: usize, key: &K) -> Option<*mut V> {
        let mut e = self;
        loop {
            if e.hash == h && &*e.key == key {
                return Some(e.val);
            }
            let p = e.next.load(Ordering::Relaxed);
            if p.is_null() {
                return None;
            }
            e = &*p;
        }
    }
}
