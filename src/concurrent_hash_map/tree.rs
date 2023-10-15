use std::hash::Hash;
use std::ptr;
use std::sync::Arc;

use crate::concurrent_hash_map::node::Node;

pub(crate) struct TreeNode<K, V> {
    pub(crate) node: Arc<Node<K, V>>,
    pub(crate) parent: *mut TreeNode<K, V>,
    pub(crate) left: *mut TreeNode<K, V>,
    pub(crate) right: *mut TreeNode<K, V>,
    pub(crate) prev: *mut TreeNode<K, V>,
    pub(crate) next: *mut TreeNode<K, V>,
    pub(crate) red: bool,
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
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
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
            } else if &p.node.key == key {
                return Some(p);
            } else if !pl.is_null() {
                p = &*pl
            } else if !pr.is_null() {
                p = &*pr
            } else {
                return None;
            }
        }
    }
}

pub(crate) struct TreeBin<K, V> {
    pub(crate) root: *const TreeNode<K, V>,
    pub(crate) first: *const TreeNode<K, V>,
}

impl<K, V> TreeBin<K, V>
where
    K: Hash + Eq,
{
    /// Creates bin with initial set of nodes headed by b.
    pub(crate) unsafe fn new(b: TreeNode<K, V>) -> TreeBin<K, V> {
        let b = Box::into_raw(Box::new(b));
        let first = b;
        let mut r = ptr::null_mut::<TreeNode<K, V>>();
        let mut x = b;
        loop {
            let next = (*x).next;
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
        Self { root: r, first }
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
    pub(crate) fn find(&self, h: usize, key: &K) -> Option<Arc<V>> {
        todo!()
    }
}

#[cfg(test)]
mod test_tree {
    use crate::concurrent_hash_map::node::Node;
    use crate::concurrent_hash_map::tree::{TreeBin, TreeNode};
    use std::sync::Arc;

    #[test]
    fn new_tree_bin() {
        let node = Node::new(1, 1, Arc::new(1));
        let tree_node = TreeNode::new(Arc::new(node));
        let bin = unsafe { TreeBin::new(tree_node) };
    }
}
