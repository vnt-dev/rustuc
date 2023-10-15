use std::fmt::Debug;
use std::sync::Arc;

pub trait Map<K, V> {
    fn size(&self) -> usize;
    // fn is_empty(&self) ->bool;
    // fn contains_key(&self,key:K)->bool;
    // fn contains_value(&self,value:V)->bool;
    fn get(&self, key: &K) -> Option<Arc<V>>;
    fn insert(&self, key: K, value: V) -> Option<Arc<V>>;
    // fn remove(&self,key:&K)->;
    // fn clear(&self);
}

// pub struct Value<V>{
//     guard:Guard
// }
