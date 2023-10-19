use std::hash::Hash;
use std::sync::atomic::Ordering;

use crate::concurrent_hash_map::base::{BaseNode, NodeEnums};

pub(crate) struct ForwardingNode<K, V> {
    pub(crate) next_table: *const Box<[BaseNode<K, V>]>,
}
impl<K, V> Clone for ForwardingNode<K, V> {
    fn clone(&self) -> Self {
        Self {
            next_table: self.next_table,
        }
    }
}
impl<K, V> Copy for ForwardingNode<K, V> {}

impl<K, V> ForwardingNode<K, V>
where
    K: Hash + Eq,
{
    pub(crate) fn new(next_table: *const Box<[BaseNode<K, V>]>) -> ForwardingNode<K, V> {
        Self { next_table }
    }
    pub(crate) unsafe fn find<'a>(&self, h: usize, key: &K) -> Option<*mut V> {
        let mut tab = &*self.next_table;
        loop {
            let n = tab.len();
            let e = tab[(n - 1) & h].node.load(Ordering::Acquire);
            unsafe {
                match e.as_ref() {
                    None => {
                        return None;
                    }
                    Some(e) => match e {
                        NodeEnums::Node(e) => return e.find(h, key),
                        NodeEnums::ForwardingNode(e) => {
                            tab = &*e.next_table;
                            continue;
                        }
                        NodeEnums::TreeBin(e) => return e.find(h, key),
                    },
                }
            }
        }
    }
}
