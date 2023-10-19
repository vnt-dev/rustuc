use std::any::Any;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::atomic::{AtomicIsize, AtomicPtr, Ordering};
use std::sync::Arc;
use std::thread::Thread;
use std::{cmp, ptr, thread};

use crate::concurrent_hash_map::node::Node;
use crate::ebr::collector::Guard;

pub(crate) struct TreeNode<K, V> {
    pub(crate) node: *mut Node<K, V>,
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
    pub(crate) fn into_box(self) -> *mut TreeNode<K, V> {
        Box::into_raw(Box::new(self))
    }
    pub(crate) fn new(node: *mut Node<K, V>) -> TreeNode<K, V> {
        Self {
            node,
            parent: ptr::null_mut(),
            left: ptr::null_mut(),
            right: ptr::null_mut(),
            red: false,
        }
    }
    pub(crate) fn new_right(node: *mut Node<K, V>, right: *mut TreeNode<K, V>) -> TreeNode<K, V> {
        Self {
            node,
            parent: ptr::null_mut(),
            left: ptr::null_mut(),
            right,
            red: false,
        }
    }
    pub(crate) fn new_parent(node: *mut Node<K, V>, parent: *mut TreeNode<K, V>) -> TreeNode<K, V> {
        Self {
            node,
            parent,
            left: ptr::null_mut(),
            right: ptr::null_mut(),
            red: false,
        }
    }
    /// Returns the TreeNode (or null if not found) for the given key starting at given root.
    pub(crate) unsafe fn find_tree_node(&self, h: usize, key: &K) -> Option<&TreeNode<K, V>> {
        let mut p = self;
        loop {
            let pl = p.left;
            let pr = p.right;
            let ph = (&*p.node).hash;
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
            } else if &*(&*p.node).key == key {
                return Some(p);
            } else if pl.is_null() {
                if pr.is_null() {
                    return None;
                }
                p = &*pr
            } else if pr.is_null() {
                p = &*pl
            } else {
                if let Some(q) = (*pr).find_tree_node(h, key) {
                    return Some(q);
                }
                p = &*pl
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
    pub(crate) first: AtomicPtr<Node<K, V>>,
    waiter: AtomicPtr<Thread>,
    lock_state: AtomicIsize,
}

impl<K, V> Drop for TreeBin<K, V> {
    fn drop(&mut self) {
        unsafe {
            // first会复用，不需要回收
            // waiter在释放锁后会回收
            if !self.root.is_null() {
                drop(Box::from_raw(self.root));
            }
        }
    }
}

impl<K, V> TreeBin<K, V> {
    /// Creates bin with initial set of nodes headed by b.
    pub(crate) unsafe fn new(b: *mut TreeNode<K, V>) -> TreeBin<K, V> {
        let first = AtomicPtr::new((&*b).node);
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
                let h = (*(*x).node).hash;
                let mut p = r;
                loop {
                    let ph = (*(*p).node).hash;
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
    unsafe fn balance_deletion(
        mut root: *mut TreeNode<K, V>,
        mut x: *mut TreeNode<K, V>,
    ) -> *mut TreeNode<K, V> {
        loop {
            if x.is_null() || x == root {
                return root;
            }
            let mut xp = (*x).parent;
            if xp.is_null() {
                (*x).red = false;
                return x;
            }
            if (*x).red {
                (*x).red = false;
                return root;
            }
            let mut xpl = (*xp).left;
            if xpl == x {
                let mut xpr = (*xp).right;
                if !xpr.is_null() && (*xpr).red {
                    (*xpr).red = false;
                    (*xp).red = true;
                    root = Self::rotate_left(root, xp);
                    xp = (*x).parent;
                    xpr = if xp.is_null() { xp } else { (*xp).right };
                }
                if xpr.is_null() {
                    x = xp;
                } else {
                    let sl = (*xpr).left;
                    let mut sr = (*xpr).right;
                    if (sr.is_null() || !(*sr).red) && (sl.is_null() || !(*sl).red) {
                        (*xpr).red = true;
                        x = xp;
                    } else {
                        if sr.is_null() || !(*sr).red {
                            if !sl.is_null() {
                                (*sl).red = false;
                            }
                            (*xpr).red = true;
                            root = Self::rotate_right(root, xpr);
                            xp = (*x).parent;
                            xpr = if xp.is_null() { xp } else { (*xp).right };
                        }
                        if !xpr.is_null() {
                            (*xpr).red = if xp.is_null() { false } else { (*xp).red };
                            sr = (*xpr).right;
                            if !sr.is_null() {
                                (*sr).red = false;
                            }
                        }
                        if !xp.is_null() {
                            (*xp).red = false;
                            root = Self::rotate_left(root, xp);
                        }
                        x = root;
                    }
                }
            } else {
                // symmetric
                if !xpl.is_null() && (*xpl).red {
                    (*xpl).red = false;
                    (*xp).red = true;
                    root = Self::rotate_right(root, xp);
                    xp = (*x).parent;
                    xpl = if xp.is_null() { xp } else { (*xp).left };
                }
                if xpl.is_null() {
                    x = xp;
                } else {
                    let mut sl = (*xpl).left;
                    let sr = (*xpl).right;
                    if (sl.is_null() || !(*sl).red) && (sr.is_null() || !(*sr).red) {
                        (*xpl).red = true;
                        x = xp;
                    } else {
                        if sl.is_null() || !(*sl).red {
                            if !sr.is_null() {
                                (*sr).red = false;
                            }
                            (*xpl).red = true;
                            root = Self::rotate_left(root, xpl);
                            xp = (*x).parent;
                            xpl = if xp.is_null() { xp } else { (*xp).left };
                        }
                        if !xpl.is_null() {
                            (*xpl).red = if xp.is_null() { false } else { (*xp).red };
                            sl = (*xpl).left;
                            if !sl.is_null() {
                                (*sl).red = false;
                            }
                        }
                        if !xp.is_null() {
                            (*xp).red = false;
                            root = Self::rotate_right(root, xp);
                        }
                        x = root;
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
                        let shared = waiter.swap(ptr::null_mut(), Ordering::Relaxed);
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
                    let shared = waiter.swap(
                        Box::into_raw(Box::new(thread::current())),
                        Ordering::Relaxed,
                    );
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

impl<K, V> TreeBin<K, V>
where
    K: Hash + Eq,
{
    /// Returns matching node or null if none. Tries to search using tree comparisons from root,
    /// but continues linear search when lock not available.
    pub(crate) unsafe fn find(&self, h: usize, key: &K) -> Option<*mut V> {
        let mut e_shared = self.first.load(Ordering::Acquire);
        let lock_state = &self.lock_state;
        while let Some(e) = e_shared.as_ref() {
            let s = lock_state.load(Ordering::Acquire);
            if s & (WAITER | WRITER) != 0 {
                if e.hash == h && &*e.key == key {
                    return Some(e.val);
                }
                e_shared = e.next.load(Ordering::Acquire);
            } else if lock_state
                .compare_exchange(s, s + READER, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                let root = self.root;
                // not null
                let p = if !root.is_null() {
                    if let Some(t) = (*root).find_tree_node(h, key) {
                        Some((*t.node).val)
                    } else {
                        None
                    }
                } else {
                    None
                };
                if lock_state.fetch_add(-READER, Ordering::AcqRel) == (READER | WAITER) {
                    let w = self.waiter.load(Ordering::Acquire);
                    if let Some(w) = w.as_ref() {
                        w.unpark();
                    }
                }
                return p;
            }
        }
        None
    }
    /// Finds or adds a node.
    /// Returns:
    /// null if added
    pub(crate) unsafe fn put_tree_val(
        &mut self,
        h: usize,
        key: *const K,
        value: *mut V,
        guard: &Guard,
    ) -> Option<&mut Node<K, V>> {
        let root = self.root;
        let mut p = root;
        let mut searched = false;
        loop {
            let pd = &mut *(*p).node;
            let ph = pd.hash;
            let xp = p;
            p = if ph > h {
                (*p).left
            } else if ph < h {
                (*p).right
            } else if &*pd.key == &*key {
                return Some(pd);
            } else {
                if searched {
                    searched = true;
                    let ch = (*p).left;
                    if !ch.is_null() {
                        if let Some(q) = (*ch).find_tree_node(h, &*key) {
                            return Some(&mut *q.node);
                        }
                    }
                    let ch = (*p).right;
                    if !ch.is_null() {
                        if let Some(q) = (*ch).find_tree_node(h, &*key) {
                            return Some(&mut *q.node);
                        }
                    }
                }
                (*p).left
            };
            if p.is_null() {
                let f = self.first.load(Ordering::Acquire);
                let x = Node::new_next(h, key, value, f).into_box();
                let shared = self.first.swap(x, Ordering::AcqRel);
                if !shared.is_null() {
                    guard.defer_destroy(shared);
                }
                let shared = (*f).prev.swap(x, Ordering::AcqRel);
                if !shared.is_null() {
                    guard.defer_destroy(shared);
                }
                let x = TreeNode::new_parent(x, xp).into_box();
                if ph >= h {
                    (*xp).left = x;
                } else {
                    (*xp).right = x;
                }
                if !(*xp).red {
                    (*x).red = true;
                } else {
                    self.lock_root(guard);
                    self.root = Self::balance_insertion(root, x);
                    self.unlock_root();
                }
                return None;
            }
        }
    }
    /// Removes the given node, that must be present before this call.
    /// This is messier than typical red-black deletion code because we cannot swap the contents
    /// of an interior node with a leaf successor that is pinned by "next" pointers that are
    /// accessible independently of lock. So instead we swap the tree linkages.
    /// Returns:
    /// true if now too small, so should be untreeified
    pub(crate) unsafe fn remove_tree_node(
        &mut self,
        p: *mut TreeNode<K, V>,
        guard: &Guard,
    ) -> bool {
        let null = ptr::null_mut();
        let next = (*(*p).node).next.load(Ordering::Acquire);
        let prev = (*(*p).node).prev.load(Ordering::Acquire); // unlink traversal pointers
        let (shared, is_null) = if let Some(prev) = prev.as_ref() {
            (prev.next.swap(next, Ordering::AcqRel), false)
        } else {
            let is_null = next.is_null();
            (self.first.swap(next, Ordering::AcqRel), is_null)
        };
        if !shared.is_null() {
            guard.defer_destroy(shared);
        }
        if is_null {
            return true;
        }
        if let Some(next) = next.as_ref() {
            let shared = next.prev.swap(prev, Ordering::AcqRel);
            if !shared.is_null() {
                guard.defer_destroy(shared);
            }
        }
        let mut r = self.root;
        let replacement;
        if r.is_null() || (*r).right.is_null() {
            // too small
            return true;
        }
        let rl = (*r).left;
        if rl.is_null() || (*rl).left.is_null() {
            // too small
            return true;
        }
        self.lock_root(guard);
        {
            let pl = (*p).left;
            let pr = (*p).right;
            if !pl.is_null() && !pr.is_null() {
                let mut s = pr;
                let mut sl;
                while {
                    sl = (*s).left;
                    !sl.is_null()
                } {
                    s = sl;
                }
                // swap colors
                let c = (*s).red;
                (*s).red = (*p).red;
                (*p).red = c;
                let sr = (*s).right;
                let pp = (*p).parent;
                if s == pr {
                    // p was s's direct parent
                    (*p).parent = s;
                    (*s).right = p;
                } else {
                    let sp = (*s).parent;
                    (*p).parent = sp;
                    if !sp.is_null() {
                        if s == (*sp).left {
                            (*sp).left = p;
                        } else {
                            (*sp).right = p;
                        }
                    }
                    (*s).right = pr;
                    if !pr.is_null() {
                        (*pr).parent = s;
                    }
                }
                (*p).left = ptr::null_mut();
                (*p).right = sr;
                if !sr.is_null() {
                    (*sr).parent = p;
                }
                (*s).left = pl;
                if !pl.is_null() {
                    (*pl).parent = s;
                }
                (*s).parent = pp;
                if pp.is_null() {
                    r = s;
                } else if p == (*pp).left {
                    (*pp).left = s;
                } else {
                    (*pp).right = s;
                }
                if !sr.is_null() {
                    replacement = sr;
                } else {
                    replacement = p;
                }
            } else if !pl.is_null() {
                replacement = pl;
            } else if !pr.is_null() {
                replacement = pr;
            } else {
                replacement = p;
            }
            if replacement != p {
                let pp = (*p).parent;
                (*replacement).parent = pp;
                if pp.is_null() {
                    r = replacement;
                } else if p == (*pp).left {
                    (*pp).left = replacement;
                } else {
                    (*pp).right = replacement;
                }
                (*p).parent = null;
                (*p).right = null;
                (*p).left = null;
            }
            self.root = if (*p).red {
                r
            } else {
                Self::balance_deletion(r, replacement)
            };
            if p == replacement {
                let pp = (*p).parent;
                if !pp.is_null() {
                    if p == (*pp).left {
                        (*pp).left = null;
                    } else if p == (*pp).right {
                        (*pp).right = null;
                        (*p).parent = null;
                    }
                }
            }
        }
        self.unlock_root();
        false
    }
}
