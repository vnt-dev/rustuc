use crate::ebr::collector::Guard;
use std::ops::Deref;

pub trait Map<K, V> {
    fn size(&self) -> usize;
    // fn is_empty(&self) ->bool;
    // fn contains_key(&self,key:K)->bool;
    // fn contains_value(&self,value:V)->bool;
    fn get(&self, key: &K) -> Option<Value<V>>;
    fn insert(&self, key: K, value: V) -> Option<Value<V>>;
    // fn remove(&self,key:&K)->;
    // fn clear(&self);
}
pub struct Value<'a, V> {
    guard: Guard<'a>,
    val: *mut V,
    is_drop: bool,
}
impl<'a, V> Value<'a, V> {
    pub(crate) fn new(guard: Guard<'a>, val: *mut V) -> Value<'a, V> {
        Self {
            guard,
            val,
            is_drop: false,
        }
    }
    pub(crate) fn new_drop(guard: Guard<'a>, val: *mut V) -> Value<'a, V> {
        Self {
            guard,
            val,
            is_drop: true,
        }
    }
}
impl<'a, V> Drop for Value<'a, V> {
    fn drop(&mut self) {
        if self.is_drop {
            self.guard.defer_destroy(self.val);
        }
    }
}
impl<'a, V> Deref for Value<'a, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.val }
    }
}
