use std::hash::Hash;
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crossbeam_epoch::{Atomic, Guard};

pub(crate) struct Node<K, V> {
    pub(crate) hash: usize,
    pub(crate) key: Arc<K>,
    pub(crate) val: Atomic<Arc<V>>,
    pub(crate) next: Atomic<Arc<Node<K, V>>>,
    pub(crate) prev: Atomic<Arc<Node<K, V>>>,
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
    pub(crate) fn new(hash: usize, key: Arc<K>, val: Arc<V>) -> Node<K, V> {
        Self {
            hash,
            key,
            val: Atomic::new(val),
            next: Atomic::null(),
            prev: Atomic::null(),
        }
    }
    pub(crate) fn new_next(
        hash: usize,
        key: Arc<K>,
        val: Arc<V>,
        next: Arc<Node<K, V>>,
    ) -> Node<K, V> {
        Self {
            hash,
            key,
            val: Atomic::new(val),
            next: Atomic::new(next),
            prev: Atomic::null(),
        }
    }
    pub(crate) fn find(self: &Arc<Node<K, V>>, h: usize, key: &K, guard: &Guard) -> Option<Arc<V>> {
        let mut e = self;
        loop {
            if e.hash == h && e.key.deref() == key {
                unsafe {
                    return Some(e.val.load(Ordering::Acquire, guard).deref().clone());
                }
            }
            let p = e.next.load(Ordering::Acquire, guard);
            if p.is_null() {
                return None;
            }
            unsafe { e = p.deref() }
        }
    }
}
