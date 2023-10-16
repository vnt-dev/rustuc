use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::hint::spin_loop;
use std::ops::Deref;
use std::sync::atomic::{AtomicIsize, AtomicPtr, Ordering};
use std::sync::{Arc, Once};
use std::{panic, ptr, thread};

use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use parking_lot::Mutex;

use crate::concurrent_hash_map::forwarding::ForwardingNode;
use crate::concurrent_hash_map::map::Map;
use crate::concurrent_hash_map::node::Node;
use crate::concurrent_hash_map::tree::{TreeBin, TreeNode};

pub(crate) struct BaseNode<K, V> {
    lock: Mutex<bool>,
    pub(crate) node: Atomic<NodeEnums<K, V>>,
}

pub(crate) enum NodeEnums<K, V> {
    Node(Arc<Node<K, V>>),
    ForwardingNode(ForwardingNode<K, V>),
    TreeBin(TreeBin<K, V>),
}

impl<K, V> NodeEnums<K, V> {
    fn is_moved(&self) -> bool {
        match self {
            NodeEnums::ForwardingNode(_) => true,
            _ => false,
        }
    }
}

impl<K, V> BaseNode<K, V>
where
    K: Hash + Eq,
{
    fn new() -> BaseNode<K, V> {
        Self {
            lock: Mutex::new(false),
            node: Atomic::null(),
        }
    }
}

/// The largest possible table capacity.
/// This value must be exactly 1<<30 to stay within Java array allocation and indexing
/// bounds for power of two table sizes, and is further required because the top
/// two bits of 32bit hash fields are used for control purposes.
const MAXIMUM_CAPACITY: usize = 1 << (isize::BITS - 2);
/// The default initial table capacity. Must be a power of 2 (i.e., at least 1) and at most
/// MAXIMUM_CAPACITY.
const DEFAULT_CAPACITY: usize = 16;
/// The largest possible (non-power of two) array size. Needed by toArray and related methods.
const MAX_ARRAY_SIZE: isize = isize::MAX;
/// The load factor for this table.
/// Overrides of this value in constructors affect only the initial table capacity.
/// The actual floating point value isn't normally used -- it is simpler to use expressions such
/// as n - (n >>> 2) for the associated resizing threshold.
const LOAD_FACTOR: f32 = 0.75;
/// The bin count threshold for using a tree rather than list for a bin.
/// Bins are converted to trees when adding an element to a bin with at least this many nodes.
/// The value must be greater than 2, and should be at least 8 to mesh with assumptions in tree
/// removal about conversion back to plain bins upon shrinkage.
const TREEIFY_THRESHOLD: usize = 8;
/// The bin count threshold for untreeifying a (split) bin during a resize operation.
/// Should be less than TREEIFY_THRESHOLD, and at most 6 to mesh with shrinkage detection under removal.
const UNTREEIFY_THRESHOLD: usize = 6;
/// The smallest table capacity for which bins may be treeified.
/// (Otherwise the table is resized if too many nodes in a bin.) The value should be at least
/// 4 * TREEIFY_THRESHOLD to avoid conflicts between resizing and treeification thresholds.
const MIN_TREEIFY_CAPACITY: usize = 64;
/// Minimum number of rebinnings per transfer step. Ranges are subdivided to allow multiple
/// resizer threads. This value serves as a lower bound to avoid resizers encountering excessive
/// memory contention. The value should be at least DEFAULT_CAPACITY.
const MIN_TRANSFER_STRIDE: isize = 16;
/// The number of bits used for generation stamp in sizeCtl. Must be at least 6 for 32bit arrays.
const RESIZE_STAMP_BITS: isize = 16;
/// The maximum number of threads that can help resize. Must fit in 32 - RESIZE_STAMP_BITS bits.
const MAX_RESIZERS: isize = (1 << (usize::BITS as isize - RESIZE_STAMP_BITS)) - 1;
/// The bit shift for recording size stamp in sizeCtl.
const RESIZE_STAMP_SHIFT: isize = isize::BITS as isize - RESIZE_STAMP_BITS;

/// Encodings for Node hash fields. See above for explanation.
/// hash for forwarding nodes
const MOVED: usize = -1isize as usize;
/// hash for roots of trees
const TREEBIN: usize = -2isize as usize;
/// hash for transient reservations
const RESERVED: usize = -3isize as usize;
/// usable bits of normal node hash
const HASH_BITS: usize = isize::MAX as usize;
/// Number of CPUS, to place bounds on some sizings
static mut NCPU: usize = 0;
const INIT: Once = Once::new();

pub struct ConcurrentHashMap<K, V, S = RandomState> {
    hash_builder: S,
    // The array of bins. Lazily initialized upon first insertion. Size is always a power of two.
    // Accessed directly by iterators.
    table: Atomic<Arc<Vec<BaseNode<K, V>>>>,
    // The next table to use; non-null only while resizing.
    next_table: Atomic<Arc<Vec<BaseNode<K, V>>>>,
    // Base counter value, used mainly when there is no contention,
    // but also as a fallback during table initialization races. Updated via CAS.
    base_count: AtomicIsize,
    // Table initialization and resizing control. When negative, the table is being initialized or resized: -1 for
    // initialization, else -(1 + the number of active resizing threads). Otherwise, when table is null,
    // holds the initial table size to use upon creation, or 0 for default. After initialization,
    // holds the next element count value upon which to resize the table.
    size_ctl: AtomicIsize,
    // The next table index (plus one) to split while resizing.
    transfer_index: AtomicIsize,
    // Spinlock (locked via CAS) used when resizing and/or creating CounterCells.
    cells_busy: AtomicIsize,
    // Table of counter cells. When non-null, size is a power of 2.
    counter_cells: AtomicPtr<Vec<AtomicIsize>>,
}

impl<K, V> ConcurrentHashMap<K, V>
where
    K: Hash + Eq + Send + 'static,
    V: Send + 'static,
{
    pub fn len(&self)->usize{
        unsafe {
            if let Some(option) = self.table.load(Ordering::Acquire, &crossbeam_epoch::pin()).as_ref() {
                option.len()
            } else {
                0
            }
        }
    }
    pub fn new() -> ConcurrentHashMap<K, V> {
        INIT.call_once(|| unsafe {
            let n = thread::available_parallelism()
                .map(|v| v.get())
                .unwrap_or(1);
            if n == 0 {
                NCPU = 1;
            } else {
                NCPU = n;
            }
        });
        Self {
            hash_builder: RandomState::new(),
            table: Default::default(),
            next_table: Default::default(),
            base_count: Default::default(),
            size_ctl: Default::default(),
            transfer_index: Default::default(),
            cells_busy: Default::default(),
            counter_cells: Default::default(),
        }
    }
}

impl<K, V> Map<K, V> for ConcurrentHashMap<K, V>
where
    K: Hash + Eq + Send + 'static,
    V: Send + 'static,
{
    fn size(&self) -> usize {
        let n = self.sum_count();
        if n < 0 {
            0
        } else {
            n as usize
        }
    }

    fn get(&self, key: &K) -> Option<Arc<V>> {
        let h = self.spread(key);
        let guard = &crossbeam_epoch::pin();
        let tab = self.table.load(Ordering::Acquire, guard);
        if tab.is_null() {
            return None;
        }
        let tab = unsafe { tab.deref() };
        let n = tab.len();
        let eb = &tab[(n - 1) & h];
        let mut e_node_share = eb.node.load(Ordering::Acquire, guard);
        if e_node_share.is_null() {
            return None;
        }
        unsafe {
            if let Some(e) = e_node_share.as_ref() {
                return match e {
                    NodeEnums::Node(e) => e.find(h, key, guard),
                    NodeEnums::ForwardingNode(e) => e.find(h, key, guard),
                    NodeEnums::TreeBin(e) => e.find(h, key, guard),
                };
            }
            return None;
        }
    }
    fn insert(&self, key: K, value: V) -> Option<Arc<V>> {
        unsafe { self.put_val(key, value, false) }
    }
}

impl<K, V> ConcurrentHashMap<K, V>
where
    K: Hash + Eq + Send + 'static,
    V: Send + 'static,
{
    fn init_table<'a>(&self, guard: &'a Guard) -> Shared<'a, Arc<Vec<BaseNode<K, V>>>> {
        loop {
            let shared = self.table.load(Ordering::Acquire, guard);
            if shared.is_null() {
                let sc = self.size_ctl.load(Ordering::Acquire);
                if sc < 0 {
                    spin_loop();
                } else if self
                    .size_ctl
                    .compare_exchange(sc, -1, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    let shared = self.table.load(Ordering::Acquire, guard);
                    if shared.is_null() {
                        let n = if sc > 0 {
                            sc as usize
                        } else {
                            DEFAULT_CAPACITY
                        };
                        match Self::new_tab(n) {
                            Ok(v) => {
                                self.table.store(v, Ordering::Release);
                                self.size_ctl
                                    .store((n - (n >> 2)) as isize, Ordering::Release);
                                return self.table.load(Ordering::Acquire, guard);
                            }
                            Err(e) => {
                                self.size_ctl.store(sc, Ordering::Release);
                                panic::resume_unwind(e);
                            }
                        }
                    }
                }
            } else {
                return shared;
            }
        }
    }
    /// Adds to count, and if table is too small and not already resizing,
    /// initiates transfer. If already resizing, helps perform transfer if work is available.
    /// Rechecks occupancy after a transfer to see if another resize is already needed because
    /// resizings are lagging additions.
    /// Params:
    ///  x    – the count to add
    /// check – if <0, don't check resize, if <= 1 only check if uncontended
    unsafe fn add_count(&self, x: isize, check: isize, guard: &Guard) {
        let mut s = 0;
        let cc = self.counter_cells.load(Ordering::Acquire);
        let h = self.hash_builder.hash_one(thread::current().id()) as usize;
        if cc.is_null() {
            let b = self.base_count.load(Ordering::Acquire);
            s = b + x;
            if self
                .base_count
                .compare_exchange(b, s, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                self.full_add_count(x, h);
                return;
            }
        } else {
            let cc = unsafe { &*cc };
            let m = cc.len() - 1;
            let a = &cc[h & m];
            a.fetch_add(x, Ordering::Release);
            if check <= 1 {
                return;
            }
            s = self.sum_count();
        }
        if check >= 0 {
            loop {
                let sc = self.size_ctl.load(Ordering::Acquire);
                if s < sc {
                    break;
                }
                let tab = self.table.load(Ordering::Acquire, guard);
                if let Some(tab) = tab.as_ref() {
                    let n = tab.len();
                    if n >= MAXIMUM_CAPACITY {
                        break;
                    }
                    let rs = resize_stamp(n as isize);
                    if sc < 0 {
                        if (sc >> RESIZE_STAMP_SHIFT) != rs
                            || sc == rs + 1
                            || sc == rs + MAX_RESIZERS
                        {
                            break;
                        }
                        let nt = self.next_table.load(Ordering::Acquire, guard);
                        if let Some(nt) = nt.as_ref() {
                            if self.transfer_index.load(Ordering::Acquire) <= 0 {
                                break;
                            }
                            if self
                                .size_ctl
                                .compare_exchange(sc, sc + 1, Ordering::AcqRel, Ordering::Relaxed)
                                .is_ok()
                            {
                                self.transfer(tab, Some(nt.clone()), guard)
                            }
                        } else {
                            break;
                        }
                    }else if self.size_ctl.compare_exchange(sc,(rs<<RESIZE_STAMP_SHIFT)+2,Ordering::AcqRel,Ordering::Relaxed).is_ok(){
                        self.transfer(tab, None, guard)
                    }
                } else {
                    break;
                }
                s = self.sum_count();

            }
        }
    }
    /// counter_cells 简化为大小固定的数组，避免内存回收的问题
    fn full_add_count(&self, x: isize, h: usize) {
        let counter_cells = &self.counter_cells;
        let cells_busy = &self.cells_busy;
        let cc = counter_cells.load(Ordering::Acquire);
        if !cc.is_null() {
            let cc = unsafe { &*cc };
            let n = cc.len();
            let a = &cc[(n - 1) & h];
            a.fetch_add(x, Ordering::Release);
        } else if cells_busy.load(Ordering::Acquire) == 0
            && cells_busy
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        {
            //锁定再次校验
            let rs = panic::catch_unwind(|| {
                let cc = counter_cells.load(Ordering::Acquire);
                if cc.is_null() {
                    let n = table_size_for(unsafe { NCPU });
                    let mut rs = Vec::with_capacity(n);
                    rs.push(AtomicIsize::new(x));
                    for _ in 1..n {
                        rs.push(AtomicIsize::new(0));
                    }
                    counter_cells.store(Box::into_raw(Box::new(rs)), Ordering::Release);
                } else {
                    let cc = unsafe { &*cc };
                    let n = cc.len();
                    let a = &cc[(n - 1) & h];
                    a.fetch_add(x, Ordering::Release);
                }
            });
            cells_busy.store(0, Ordering::Release);
            if let Err(e) = rs {
                panic::resume_unwind(e);
            }
        } else {
            //前面都失败了这里直接添加，不再循环了
            self.base_count.fetch_add(x, Ordering::Release);
        }
    }
    fn sum_count(&self) -> isize {
        unsafe {
            let cc = self.counter_cells.load(Ordering::Acquire);
            let mut sum = self.base_count.load(Ordering::Acquire);
            if !cc.is_null() {
                for x in &*cc {
                    sum += x.load(Ordering::Acquire);
                }
            }
            sum
        }
    }

    unsafe fn put_val(&self, key: K, value: V, only_if_absent: bool) -> Option<Arc<V>> {
        let hash = self.spread(&key);
        let value = Arc::new(value);
        let key = Arc::new(key);
        let mut node_option: Option<Owned<NodeEnums<K, V>>> = None;
        let guard = &crossbeam_epoch::pin();
        let mut bin_count = 0;
        let old: Option<Arc<V>> = 'a: loop {
            let mut shared = self.table.load(Ordering::Acquire, guard);
            if shared.is_null() {
                shared = self.init_table(guard);
            }
            let table = unsafe { shared.deref() };
            let n = table.len();
            let i = (n - 1) & hash;
            let f = &table[i];
            let f_node_atomic = &f.node;
            let mut f_node_share = f_node_atomic.load(Ordering::Acquire, guard);
            //节点为空则cas替换
            if f_node_share.is_null() {
                let node = node_option.unwrap_or_else(|| {
                    Owned::new(NodeEnums::Node(Arc::new(Node::new(
                        hash,
                        key.clone(),
                        value.clone(),
                    ))))
                });
                match f_node_atomic.compare_exchange(
                    f_node_share,
                    node,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    guard,
                ) {
                    Ok(_) => {
                        break None;
                    }
                    Err(e) => {
                        node_option = Some(e.new);
                        f_node_share = e.current;
                    }
                }
            }
            let f_node = unsafe { f_node_share.deref_mut() };
            if let NodeEnums::ForwardingNode(f_move) = f_node {
                self.help_transfer(table, &f_move.next_table, guard);
            } else {
                let mutex_guard = f.lock.lock();
                if f_node_atomic.load(Ordering::Acquire, guard) == f_node_share {
                    match f_node {
                        NodeEnums::Node(link_node) => {
                            bin_count = 1;
                            let mut e = link_node.deref();
                            loop {
                                if e.hash == hash && e.key == key {
                                    let old = e.val.load(Ordering::Acquire, guard).deref().clone();
                                    if !only_if_absent {
                                        let shared =
                                            e.val.swap(Owned::new(value), Ordering::AcqRel, guard);
                                        guard.defer_destroy(shared);
                                    }
                                    break 'a Some(old);
                                }
                                if let Some(next) = e.next.load(Ordering::Acquire, guard).as_ref() {
                                    e = next;
                                } else {
                                    e.next.store(
                                        Owned::new(Arc::new(Node::new(hash, key, value))),
                                        Ordering::Release,
                                    );
                                    drop(mutex_guard);
                                    if bin_count >= TREEIFY_THRESHOLD {
                                        self.treeify_bin(table, i, guard);
                                    }
                                    break 'a None;
                                }
                                bin_count += 1;
                            }
                        }
                        NodeEnums::TreeBin(f) => {
                            bin_count = 2;
                            if let Some(p) = f.put_tree_val(hash, &key, &value, guard) {
                                let old = p.val.load(Ordering::Acquire, guard).deref().clone();
                                if !only_if_absent {
                                    let shared =
                                        p.val.swap(Owned::new(value), Ordering::AcqRel, guard);
                                    guard.defer_destroy(shared);
                                }
                                break 'a Some(old);
                            }
                            break 'a None;
                        }
                        _ => {}
                    }
                }
                drop(mutex_guard);
            }
        };
        match old {
            None => {
                self.add_count(1, bin_count as isize, guard);
                None
            }
            Some(v) => Some(v),
        }
    }
    /// Replaces all linked nodes in bin at given index unless table is
    /// too small, in which case resizes instead.
    unsafe fn treeify_bin(&self, tab: &Vec<BaseNode<K, V>>, index: usize, guard: &Guard) {
        let n = tab.len();
        if n < MIN_TREEIFY_CAPACITY {
            self.try_presize(n << 1, guard);
        } else {
            let tab_at = &tab[index];
            let b_shared = tab_at.node.load(Ordering::Acquire, guard);
            if let Some(b) = b_shared.as_ref() {
                if let NodeEnums::Node(b) = b {
                    let mutex_guard = tab_at.lock.lock();
                    if b_shared == tab_at.node.load(Ordering::Acquire, guard) {
                        let mut e = b;
                        let hd = Box::into_raw(Box::new(TreeNode::new(e.clone())));
                        let mut tail = hd;
                        // Reuse existing linked lists
                        while let Some(pd) = e.next.load(Ordering::Acquire, guard).as_ref() {
                            // Do not maintain 'prev' when using linked lists
                            pd.prev.store(Owned::new(e.clone()), Ordering::Release);
                            let p = Box::into_raw(Box::new(TreeNode::new(pd.clone())));
                            // Use 'right' as' next '
                            (*tail).right = p;
                            tail = p;
                            e = pd;
                        }
                        let shared = tab_at.node.swap(
                            Owned::new(NodeEnums::TreeBin(TreeBin::new(hd))),
                            Ordering::AcqRel,
                            guard,
                        );
                        if !shared.is_null() {
                            guard.defer_destroy(shared);
                        }
                    }
                    drop(mutex_guard);
                }
            }
        }
    }
    /// Spreads (XORs) higher bits of hash to lower and also forces top bit to 0. Because the table uses
    /// power-of-two masking, sets of hashes that vary only in bits above the current mask will always
    /// collide. (Among known examples are sets of Float keys holding consecutive whole numbers in small
    /// tables.) So we apply a transform that spreads the impact of higher bits downward. There is a
    /// tradeoff between speed, utility, and quality of bit-spreading. Because many common sets of hashes
    /// are already reasonably distributed (so don't benefit from spreading), and because we use trees to
    /// handle large sets of collisions in bins, we just XOR some shifted bits in the cheapest possible way
    /// to reduce systematic lossage, as well as to incorporate impact of the highest bits that would
    /// otherwise never be used in index calculations because of table bounds.
    fn spread(&self, key: &K) -> usize {
        //todo 测试
        1
        // let hash = self.hash_builder.hash_one(key);
        // HASH_BITS & (hash ^ (hash >> 32)) as usize
    }
    /// Tries to presize table to accommodate the given number of elements.
    /// Params:
    ///  size – number of elements (doesn't need to be perfectly accurate)
    unsafe fn try_presize(&self, size: usize, guard: &Guard) {
        let c = if size >= (MAXIMUM_CAPACITY >> 1) {
            MAXIMUM_CAPACITY
        } else {
            table_size_for(size + (size >> 1) + 1)
        };
        let mut sc;
        let size_ctl = &self.size_ctl;
        let table = &self.table;
        while {
            sc = size_ctl.load(Ordering::Acquire);
            sc >= 0
        } {
            let tab = table.load(Ordering::Acquire, guard);
            if tab.is_null() {
                let n = if sc as usize > c { sc as usize } else { c };
                if size_ctl
                    .compare_exchange(sc, -1, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    if table.load(Ordering::Acquire, guard) == tab {
                        match Self::new_tab(n) {
                            Ok(tab) => {
                                table.store(tab, Ordering::Release);
                                sc = (n - (n >> 2)) as isize;
                                size_ctl.store(sc, Ordering::Release);
                                break;
                            }
                            Err(e) => {
                                size_ctl.store(sc, Ordering::Release);
                                panic::resume_unwind(e);
                            }
                        }
                    }
                }
            } else if c <= sc as usize {
                break;
            } else {
                let tab = tab.deref();
                let n = tab.len();
                if n > MAXIMUM_CAPACITY {
                    break;
                }
                let rs = resize_stamp(n as isize);
                if sc < 0 {
                    if (sc >> RESIZE_STAMP_SHIFT) != rs || sc == rs + 1 || sc == rs + MAX_RESIZERS {
                        let nt = self.next_table.load(Ordering::Acquire, guard);
                        if let Some(nt) = nt.as_ref() {
                            if self.transfer_index.load(Ordering::Acquire) <= 0 {
                                break;
                            }
                            if size_ctl
                                .compare_exchange(sc, sc + 1, Ordering::AcqRel, Ordering::Relaxed)
                                .is_ok()
                            {
                                self.transfer(tab, Some(nt.clone()), guard);
                            }
                        } else {
                            break;
                        }
                    }
                } else if size_ctl
                    .compare_exchange(
                        sc,
                        (rs << RESIZE_STAMP_SHIFT) + 2,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    self.transfer(tab, None, guard);
                    return;
                }
            }
        }
    }
    /// Helps transfer if a resize is in progress.
    unsafe fn help_transfer(
        &self,
        tab: &Vec<BaseNode<K, V>>,
        next_tab: &Arc<Vec<BaseNode<K, V>>>,
        guard: &Guard,
    ) {
        let rs = resize_stamp(tab.len() as isize);
        loop {
            let sc = self.size_ctl.load(Ordering::Acquire);
            if (sc >> RESIZE_STAMP_SHIFT) != rs
                || sc == rs + 1
                || sc == rs + MAX_RESIZERS
                || self.transfer_index.load(Ordering::Acquire) <= 0
            {
                return;
            }
            if self
                .size_ctl
                .compare_exchange(sc, sc + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.transfer(tab, Some(next_tab.clone()), guard);
                return;
            }
        }
    }
    /// Moves and/or copies the nodes in each bin to new table. See above for explanation.
    unsafe fn transfer(
        &self,
        tab: &Vec<BaseNode<K, V>>,
        next_tab: Option<Arc<Vec<BaseNode<K, V>>>>,
        guard: &Guard,
    ) {
        let n = tab.len();
        let mut stride = if NCPU > 1 { (n >> 3) / NCPU } else { n } as isize;
        if stride < MIN_TRANSFER_STRIDE {
            stride = MIN_TRANSFER_STRIDE; // subdivide range
        }
        let size_ctl = &self.size_ctl;
        let next_table = &self.next_table;
        let next_tab = match next_tab {
            None => {
                // initiating
                match panic::catch_unwind(|| {
                    let n = n << 1;
                    let mut tab: Vec<BaseNode<K, V>> = Vec::with_capacity(n );
                    tab.resize_with(n, || BaseNode::new());
                    let tab = Arc::new(tab);
                    (Owned::new(tab.clone()), tab)
                }) {
                    Ok((nt, next_tab)) => {
                        next_table.store(nt, Ordering::Release);
                        next_tab
                    }
                    Err(e) => {
                        // try to cope with OOME
                        size_ctl.store(isize::MAX, Ordering::Release);
                        panic::resume_unwind(e);
                    }
                }
            }
            Some(next_tab) => next_tab,
        };
        let nextn = next_tab.len() as isize;
        let fwd = ForwardingNode::new(next_tab.clone());
        let mut advance = true;
        let mut finishing = false; // to ensure sweep before committing nextTab
        let mut i = 0;
        let mut bound = 0;
        let transfer_index = &self.transfer_index;
        let n = n as isize;
        loop {
            while advance {
                i -= 1;
                if i >= bound || finishing {
                    advance = false;
                    break;
                }
                let next_index = transfer_index.load(Ordering::Acquire);
                if next_index <= 0 {
                    i = -1;
                    advance = false;
                    break;
                }
                let next_bound = if next_index > stride {
                    next_index - stride
                } else {
                    0
                };
                if transfer_index
                    .compare_exchange(next_index, next_bound, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    bound = next_bound;
                    i = next_index - 1;
                    advance = false;
                    break;
                }
            }
            if i < 0 || i >= n || i + n >= nextn {
                if finishing {
                    let shared = next_table.swap(Shared::null(), Ordering::Release, guard);
                    if !shared.is_null() {
                        guard.defer_destroy(shared);
                    }
                    let shared = self
                        .table
                        .swap(Owned::new(next_tab), Ordering::Release, guard);
                    if !shared.is_null() {
                        guard.defer_destroy(shared);
                    }
                    size_ctl.store((n << 1) - (n >> 1), Ordering::Release);
                    return;
                }
                let sc = size_ctl.fetch_add(-1, Ordering::AcqRel);
                if sc - 2 != resize_stamp(n) << RESIZE_STAMP_SHIFT {
                    return;
                }
                advance = true;
                finishing = true;
                i = n; // recheck before commit
                continue;
            }
            let tab_at = &tab[i as usize];
            let tab_at_node = &tab_at.node;
            let f_shared = tab_at_node.load(Ordering::Acquire, guard);
            if let Some(f) = f_shared.as_ref() {
                if f.is_moved() {
                    advance = true; // already processed
                    continue;
                }
                let n = n as usize;
                let mutex_guard = tab_at.lock.lock();
                if tab_at_node.load(Ordering::Acquire, guard) == f_shared {
                    match f {
                        NodeEnums::Node(f) => {
                            let fh = f.hash;
                            let mut run_bit = fh & n;
                            let mut last_run = f;
                            let mut p_shared = f.next.load(Ordering::Acquire, guard);
                            while let Some(p) = p_shared.as_ref() {
                                let b = p.hash & n;
                                if b != run_bit {
                                    run_bit = b;
                                    last_run = p;
                                }
                                p_shared = p.next.load(Ordering::Acquire, guard);
                            }
                            let (mut ln, mut hn) = if run_bit == 0 {
                                (Some(last_run.clone()), Option::<Arc<Node<K, V>>>::None)
                            } else {
                                (None, Some(last_run.clone()))
                            };
                            let mut p = f;
                            while p != last_run {
                                let ph = p.hash;
                                let pk = p.key.clone();
                                let pv = p.val.load(Ordering::Acquire, guard).deref().clone();
                                if (ph & n) == 0 {
                                    if let Some(ln_node) = ln {
                                        ln = Some(Arc::new(Node::new_next(ph, pk, pv, ln_node)));
                                    } else {
                                        ln = Some(Arc::new(Node::new(ph, pk, pv)));
                                    }
                                } else {
                                    hn = if let Some(hn_node) = hn {
                                        Some(Arc::new(Node::new_next(ph, pk, pv, hn_node)))
                                    } else {
                                        Some(Arc::new(Node::new(ph, pk, pv)))
                                    };
                                }
                                match p.next.load(Ordering::Acquire, guard).as_ref() {
                                    None => {
                                        break;
                                    }
                                    Some(tmp) => {
                                        p = tmp;
                                    }
                                }
                            }
                            if let Some(ln) = ln {
                                next_tab[i as usize]
                                    .node
                                    .store(Owned::new(NodeEnums::Node(ln)), Ordering::Release);
                            }
                            if let Some(hn) = hn {
                                next_tab[i as usize + n]
                                    .node
                                    .store(Owned::new(NodeEnums::Node(hn)), Ordering::Release);
                            }
                            let shared_old = tab_at.node.swap(
                                Owned::new(NodeEnums::ForwardingNode(fwd.clone())),
                                Ordering::AcqRel,
                                guard,
                            );
                            guard.defer_destroy(shared_old);
                            advance = true;
                        }
                        NodeEnums::TreeBin(t) => {
                            let mut lc = 0;
                            let mut hc = 0;
                            let mut e_shared = t.first.load(Ordering::Acquire, guard);
                            let mut lo = ptr::null_mut::<TreeNode<K, V>>();
                            let mut lo_tail = ptr::null_mut::<TreeNode<K, V>>();
                            let mut hi = ptr::null_mut::<TreeNode<K, V>>();
                            let mut hi_tail = ptr::null_mut::<TreeNode<K, V>>();
                            while let Some(e) = e_shared.as_ref() {
                                let h = e.hash;
                                let ek = e.key.clone();
                                let ev = e.val.load(Ordering::Acquire, guard).deref().clone();
                                let p = Box::into_raw(Box::new(TreeNode::new(Arc::new(
                                    Node::new(h, ek, ev),
                                ))));
                                if (h & n) == 0 {
                                    (*p).node.prev.store(
                                        Owned::new((*lo_tail).node.clone()),
                                        Ordering::Release,
                                    );
                                    if lo_tail.is_null() {
                                        lo = p;
                                    } else {
                                        (*lo_tail).node.next.store(
                                            Owned::new((*p).node.clone()),
                                            Ordering::Release,
                                        );
                                        (*lo_tail).right = p;
                                    }
                                    lo_tail = p;
                                    lc += 1;
                                } else {
                                    (*p).node.prev.store(
                                        Owned::new((*hi_tail).node.clone()),
                                        Ordering::Release,
                                    );
                                    if hi_tail.is_null() {
                                        hi = p;
                                    } else {
                                        (*hi_tail).node.next.store(
                                            Owned::new((*p).node.clone()),
                                            Ordering::Release,
                                        );
                                        (*hi_tail).right = p;
                                    }
                                    hi_tail = p;
                                    hc += 1;
                                }
                                e_shared = e.next.load(Ordering::Acquire, guard);
                            }
                            let ln = if lc < UNTREEIFY_THRESHOLD {
                                if lo.is_null() {
                                    None
                                } else {
                                    Some(Owned::new(NodeEnums::Node(Self::untreeify(&*lo))))
                                }
                            } else {
                                if lo.is_null() {
                                    None
                                } else {
                                    Some(Owned::new(NodeEnums::TreeBin(TreeBin::new(lo))))
                                }
                            };
                            let hn = if hi.is_null() {
                                None
                            } else if hc < UNTREEIFY_THRESHOLD {
                                Some(Owned::new(NodeEnums::Node(Self::untreeify(&*hi))))
                            } else {
                                Some(Owned::new(NodeEnums::TreeBin(TreeBin::new(hi))))
                            };
                            if let Some(ln) = ln {
                                next_tab[i as usize].node.store(ln, Ordering::AcqRel);
                            }
                            if let Some(hn) = hn {
                                next_tab[i as usize + n].node.store(hn, Ordering::AcqRel);
                            }

                            let shared = tab_at.node.swap(
                                Owned::new(NodeEnums::ForwardingNode(fwd.clone())),
                                Ordering::AcqRel,
                                guard,
                            );
                            guard.defer_destroy(shared);
                            advance = true;
                        }
                        _ => {}
                    }
                }
                drop(mutex_guard);
            } else {
                advance = tab_at_node
                    .compare_exchange(
                        f_shared,
                        Owned::new(NodeEnums::ForwardingNode(fwd.clone())),
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                        guard,
                    )
                    .is_ok();
            }
        }
    }
    /// Returns a list on non-TreeNodes replacing those in given list.
    #[inline]
    fn untreeify(b: &TreeNode<K, V>) -> Arc<Node<K, V>> {
        b.node.clone()
    }
    fn new_tab(n: usize) -> thread::Result<Owned<Arc<Vec<BaseNode<K, V>>>>> {
        panic::catch_unwind(|| {
            let mut tab: Vec<BaseNode<K, V>> = Vec::with_capacity(n);
            tab.resize_with(n, || BaseNode::new());
            let tab = Arc::new(tab);
            Owned::new(tab)
        })
    }
}

/// Returns a power of two table size for the given desired capacity. See Hackers Delight, sec 3.2
fn table_size_for(c: usize) -> usize {
    let mut n = c - 1;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    if n >= MAXIMUM_CAPACITY {
        MAXIMUM_CAPACITY
    } else {
        n + 1
    }
}

/// Returns the stamp bits for resizing a table of size n. Must be negative when shifted left by
/// RESIZE_STAMP_SHIFT.
fn resize_stamp(n: isize) -> isize {
    number_of_leading_zeros(n) | (1 << (RESIZE_STAMP_BITS - 1))
}

/// Returns the number of zero bits preceding the highest-order
/// ("leftmost") one-bit in the two's complement binary representation
/// of the specified int value. Returns 32 if the
/// specified value has no one-bits in its two's complement representation,
/// in other words if it is equal to zero.
///
/// Note that this method is closely related to the logarithm base 2. For all positive int values x:
/// floor(log2(x)) = 31 - number_of_leading_zeros(x)
/// ceil(log2(x)) = 32 - number_of_leading_zeros(x - 1)
/// Params:
/// i – the value whose number of leading zeros is to be computed
/// Returns:
/// the number of zero bits preceding the highest-order ("leftmost") one-bit in the two's complement
/// binary representation of the specified int value, or 32 if the value is equal to zero.
fn number_of_leading_zeros(mut i: isize) -> isize {
    // HD, Figure 5-6
    if i == 0 {
        return 32;
    }
    let mut n = 1;
    if i >> 16 == 0 {
        n += 16;
        i <<= 16;
    }
    if i >> 24 == 0 {
        n += 8;
        i <<= 8;
    }
    if i >> 28 == 0 {
        n += 4;
        i <<= 4;
    }
    if i >> 30 == 0 {
        n += 2;
        i <<= 2;
    }
    n -= i >> 31;
    return n;
}
