use crate::concurrent_hash_map::base::{BaseNode, NodeEnums};
use crossbeam_epoch::Guard;
use std::hash::Hash;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub(crate) struct ForwardingNode<K, V> {
    next_table: Arc<Vec<BaseNode<K, V>>>,
}
impl<K, V> Clone for ForwardingNode<K, V> {
    fn clone(&self) -> Self {
        Self {
            next_table: self.next_table.clone(),
        }
    }
}

impl<K, V> ForwardingNode<K, V>
where
    K: Hash + Eq,
{
    pub(crate) fn new(next_table: Arc<Vec<BaseNode<K, V>>>) -> ForwardingNode<K, V> {
        Self { next_table }
    }
    pub(crate) fn find<'a>(&self, h: usize, key: &K, guard: &'a Guard) -> Option<Arc<V>> {
        let mut tab = &self.next_table;
        loop {
            let n = tab.len();
            let e = tab[(n - 1) & h].node.load(Ordering::Acquire, guard);
            unsafe {
                match e.as_ref() {
                    None => {
                        return None;
                    }
                    Some(e) => match e {
                        NodeEnums::Node(e) => return e.find(h, key, guard),
                        NodeEnums::ForwardingNode(e) => {
                            tab = &e.next_table;
                            continue;
                        }
                        NodeEnums::TreeBin(e) => return e.find(h, key,guard),
                    },
                }
            }
        }
    }
}
