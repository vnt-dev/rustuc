use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use std::hash::Hash;
use std::ops::Deref;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::Arc;
use std::thread::Thread;
use std::{ptr, thread};

use crate::concurrent_hash_map::node::Node;

pub(crate) struct TreeNode<K, V> {
    pub(crate) node: Arc<Node<K, V>>,
    pub(crate) parent: *mut TreeNode<K, V>,
    pub(crate) left: *mut TreeNode<K, V>,
    pub(crate) right: *mut TreeNode<K, V>,
    pub(crate) red: bool,
}

impl<K, V> Drop for TreeNode<K, V> {
    fn drop(&mut self) {
        unsafe {
            if !self.left.is_null() {
                drop(Box::from_raw(self.left));
            }
            if !self.right.is_null() {
                drop(Box::from_raw(self.right));
            }
        }
    }
}

impl<K, V> TreeNode<K, V>
    where
        K: Hash + Eq,
{
    pub(crate) fn new(node: Arc<Node<K, V>>) -> TreeNode<K, V> {
        Self {
            node,
            parent: ptr::null_mut(),
            left: ptr::null_mut(),
            right: ptr::null_mut(),
            red: false,
        }
    }
    pub(crate) fn new_next(node: Arc<Node<K, V>>,next:*mut TreeNode<K, V>,parent:*mut TreeNode<K, V>) -> TreeNode<K, V> {
        Self {
            node,
            parent,
            left: ptr::null_mut(),
            right: next,
            red: false,
        }
    }
    /// 此操作和更新树结构互斥
    pub(crate) unsafe fn find_tree_node(&self, h: usize, key: &K) -> Option<&TreeNode<K, V>> {
        let mut p = self;
        loop {
            let pl = p.left;
            let pr = p.right;
            let ph = p.node.hash;
            if ph > h {
                if pl.is_null() {
                    return None;
                }
                p = &*pl;
            } else if ph < h {
                if pr.is_null() {
                    return None;
                }
                p = &*pr;
            } else if p.node.key.deref() == key {
                return Some(p);
            } else if !pl.is_null() {
                p = &*pl
            }  else {
                return None;
            }
        }
    }
}

// values for lockState
const WRITER: isize = 1;
// set while holding write lock
const WAITER: isize = 2;
// set when waiting for write lock
const READER: isize = 4; // increment value for setting read lock

pub(crate) struct TreeBin<K, V> {
    pub(crate) root: *mut TreeNode<K, V>,
    pub(crate) first: Atomic<Arc<Node<K, V>>>,
    waiter: Atomic<Thread>,
    lock_state: AtomicIsize,
}

impl<K, V> Drop for TreeBin<K, V> {
    fn drop(&mut self) {
        unsafe {
            if !self.root.is_null() {
                drop(Box::from_raw(self.root));
            }
        }
    }
}

impl<K, V> TreeBin<K, V> {
    /// Creates bin with initial set of nodes headed by b.
    pub(crate) unsafe fn new(b: *mut TreeNode<K, V>) -> TreeBin<K, V> {
        let first = Atomic::new(((&*b).node).clone());
        let mut r = ptr::null_mut::<TreeNode<K, V>>();
        let mut x = b;
        loop {
            let next = (*x).right;
            (*x).left = ptr::null_mut();
            (*x).right = ptr::null_mut();
            if r.is_null() {
                (*x).parent = ptr::null_mut();
                (*x).red = false;
                r = x;
            } else {
                let h = (*x).node.hash;
                let mut p = r;
                loop {
                    let ph = (*p).node.hash;
                    let xp = p;
                    if ph >= h {
                        p = (*p).left;
                    } else {
                        p = (*p).right;
                    }

                    if p.is_null() {
                        (*x).parent = xp;
                        if ph >= h {
                            (*xp).left = x;
                        } else {
                            (*xp).right = x;
                        }
                        r = Self::balance_insertion(r, x);
                        break;
                    }
                }
            }
            if next.is_null() {
                break;
            }
            x = next;
        }
        Self {
            root: r,
            first,
            waiter: Default::default(),
            lock_state: Default::default(),
        }
    }
    unsafe fn balance_insertion(
        mut root: *mut TreeNode<K, V>,
        mut x: *mut TreeNode<K, V>,
    ) -> *mut TreeNode<K, V> {
        (*x).red = true;
        let mut xpp = ptr::null_mut::<TreeNode<K, V>>();
        loop {
            let mut xp = (*x).parent;
            if xp.is_null() {
                (*x).red = false;
                return x;
            }
            if !(*xp).red || {
                xpp = (*xp).parent;
                xpp.is_null()
            } {
                return root;
            }
            let xppl = (*xpp).left;
            if xp == xppl {
                let xppr = (*xpp).right;
                if !xppr.is_null() && (*xppr).red {
                    (*xppr).red = false;
                    (*xp).red = false;
                    (*xpp).red = true;
                    x = xpp;
                } else {
                    if x == (*xp).right {
                        x = xp;
                        root = Self::rotate_left(root, x);
                        xp = (*x).parent;
                        xpp = if xp.is_null() { xp } else { (*xp).parent };
                    }
                    if !xp.is_null() {
                        (*xp).red = false;
                        if !xpp.is_null() {
                            (*xpp).red = true;
                            root = Self::rotate_right(root, xpp);
                        }
                    }
                }
            } else {
                if !xppl.is_null() && (*xppl).red {
                    (*xppl).red = false;
                    (*xp).red = false;
                    (*xpp).red = true;
                    x = xpp;
                } else {
                    if x == (*xp).left {
                        x = xp;
                        root = Self::rotate_right(root, x);
                        xp = (*x).parent;
                        xpp = if xp.is_null() { xp } else { (*xp).parent };
                    }
                    if !xp.is_null() {
                        (*xp).red = false;
                        if !xpp.is_null() {
                            (*xpp).red = true;
                            root = Self::rotate_left(root, xpp);
                        }
                    }
                }
            }
        }
    }
    /// Red-black tree methods, all adapted from CLR
    unsafe fn rotate_left(
        mut root: *mut TreeNode<K, V>,
        mut p: *mut TreeNode<K, V>,
    ) -> *mut TreeNode<K, V> {
        if !p.is_null() {
            let r = (*p).right;
            if !r.is_null() {
                (*r).right = (*r).left;
                let rl = (*r).right;
                if !rl.is_null() {
                    (*rl).parent = p;
                }
                (*r).parent = (*p).parent;
                let pp = (*r).parent;
                if pp.is_null() {
                    root = r;
                    (*root).red = false;
                } else if (*pp).left == p {
                    (*pp).left = r;
                } else {
                    (*pp).right = r;
                }
                (*r).left = p;
                (*p).parent = r;
            }
        }
        root
    }
    unsafe fn rotate_right(
        mut root: *mut TreeNode<K, V>,
        mut p: *mut TreeNode<K, V>,
    ) -> *mut TreeNode<K, V> {
        if !p.is_null() {
            let l = (*p).left;
            if !l.is_null() {
                (*p).left = (*l).right;
                let lr = (*p).left;
                if !lr.is_null() {
                    (*lr).parent = p;
                }
                (*l).parent = (*p).parent;
                let pp = (*l).parent;
                if pp.is_null() {
                    root = l;
                    (*root).red = false;
                } else if (*pp).right == p {
                    (*pp).right = l;
                } else {
                    (*pp).left = l;
                }
                (*l).right = p;
                (*p).parent = l;
            }
        }
        root
    }
    pub(crate) fn contended_lock(&self, guard: &Guard) {
        let mut waiting = false;
        let lock_state = &self.lock_state;
        let waiter = &self.waiter;
        loop {
            let s = lock_state.load(Ordering::Acquire);
            if s & !WAITER == 0 {
                if lock_state
                    .compare_exchange(s, WRITER, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    if waiting {
                        let shared = waiter.swap(Shared::null(), Ordering::Relaxed, guard);
                        if !shared.is_null() {
                            unsafe {
                                guard.defer_destroy(shared);
                            }
                        }
                    }
                    return;
                }
            } else if (s & WAITER) == 0 {
                if lock_state
                    .compare_exchange(s, s | WAITER, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    waiting = true;
                    let shared =
                        waiter.swap(Owned::new(thread::current()), Ordering::Relaxed, guard);
                    if !shared.is_null() {
                        unsafe {
                            guard.defer_destroy(shared);
                        }
                    }
                }
            } else if waiting {
                thread::park();
            }
        }
    }
    /// Acquires write lock for tree restructuring.
    pub(crate) fn lock_root(&self, guard: &Guard) {
        if self
            .lock_state
            .compare_exchange(0, WRITER, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            self.contended_lock(guard)
        }
    }
    /// Releases write lock for tree restructuring.
    pub(crate) fn unlock_root(&self) {
        self.lock_state.store(0, Ordering::Release);
    }
}

impl<K, V> TreeBin<K, V> where
    K: Hash + Eq, {
    pub(crate) unsafe fn find(&self, h: usize, key: &K, guard: &Guard) -> Option<Arc<V>> {
        let e = self.first.load(Ordering::Acquire,guard);
        let lock_state = &self.lock_state;
        while let Some(e) = e.as_ref(){
            let s = lock_state.load(Ordering::Acquire);
            if s&(WAITER|WRITER)==0{
                todo!("java.util.concurrent.ConcurrentHashMap.TreeBin#find")
            }
        }
        None
    }
    pub(crate) unsafe fn put_tree_val(&mut self, h: usize, key: K, value: V, guard: &Guard) -> Option<&Node<K, V>> {
        let root = self.root;
        let mut p = root;
        loop {
            let pd = &(*p).node;
            let ph = pd.hash;
            let xp = p;
            p = if ph > h {
                (*p).left
            } else if ph < h {
                (*p).right
            } else if pd.key.deref() == &key {
                return Some(pd);
            }else{
                (*p).left
            };
            if p.is_null(){
                let f = self.first.load(Ordering::Acquire,guard).deref();
                let x = Arc::new(Node::new_next(h,Arc::new(key),Arc::new(value),f.clone()));
                // Old nodes do not need to be recycled
                self.first.store(Owned::new(x.clone()), Ordering::Release);
                f.prev.store(Owned::new(f.clone()),Ordering::Release);
                let x = Box::into_raw(Box::new( TreeNode::new_next(x,root,xp)));
                if ph >= h{
                    (*xp).left = x;
                }else{
                    (*xp).right = x;
                }
                if !(*xp).red {
                    (*x).red = true;
                }else{
                    self.lock_root(guard);
                    self.root = Self::balance_insertion(root,x);
                    self.unlock_root();
                }
                return None;
            }
        }
    }
}

#[cfg(test)]
mod test_tree {
    use crate::concurrent_hash_map::node::Node;
    use crate::concurrent_hash_map::tree::{TreeBin, TreeNode};
    use std::sync::Arc;

    #[test]
    fn new_tree_bin() {
        let node = Node::new(1, Arc::new(1), Arc::new(1));
        let tree_node = TreeNode::new(Arc::new(node));
        let bin = unsafe { TreeBin::new(tree_node) };
    }
}
