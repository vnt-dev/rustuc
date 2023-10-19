use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::{mem, ptr, thread};

thread_local! {
    static THREAD_ID:u64 = unsafe { mem::transmute(thread::current().id()) };
}
const LEN: usize = 1 << 12;
const RETIRE_LEN: usize = 1 << 8;
const HASH_BITS: usize = LEN - 1;

pub struct Collector {
    active_array: Vec<AtomicBool>,
    epoch_array: Vec<AtomicUsize>,
    global_epoch: AtomicUsize,
    retire_list: Vec<AtomicPtr<Collectible>>,
    state: AtomicBool,
    hasher: RandomState,
}
/// Number of words a piece of `Data` can hold.
///
/// Three words should be enough for the majority of cases. For example, you can fit inside it the
/// function pointer together with a fat pointer representing an object that needs to be destroyed.
const DATA_WORDS: usize = 3;

/// Some space to keep a `FnOnce()` object on the stack.
type Data = [usize; DATA_WORDS];
struct Collectible {
    call: unsafe fn(*mut u8),
    data: MaybeUninit<Data>,
    next: AtomicPtr<Collectible>,
}
impl Collectible {
    /// Constructs a new `Deferred` from a `FnOnce()`.
    pub(crate) fn new<F: FnOnce()>(f: F) -> Self {
        let size = mem::size_of::<F>();
        let align = mem::align_of::<F>();

        unsafe {
            if size <= mem::size_of::<Data>() && align <= mem::align_of::<Data>() {
                let mut data = MaybeUninit::<Data>::uninit();
                ptr::write(data.as_mut_ptr().cast::<F>(), f);

                unsafe fn call<F: FnOnce()>(raw: *mut u8) {
                    let f: F = ptr::read(raw.cast::<F>());
                    f();
                }

                Self {
                    call: call::<F>,
                    data,
                    next: Default::default(),
                }
            } else {
                let b: Box<F> = Box::new(f);
                let mut data = MaybeUninit::<Data>::uninit();
                ptr::write(data.as_mut_ptr().cast::<Box<F>>(), b);

                unsafe fn call<F: FnOnce()>(raw: *mut u8) {
                    // It's safe to cast `raw` from `*mut u8` to `*mut Box<F>`, because `raw` is
                    // originally derived from `*mut Box<F>`.
                    let b: Box<F> = ptr::read(raw.cast::<Box<F>>());
                    (*b)();
                }

                Self {
                    call: call::<F>,
                    data,
                    next: Default::default(),
                }
            }
        }
    }

    /// Calls the function.
    #[inline]
    pub(crate) fn call(mut self) {
        let call = self.call;
        unsafe { call(self.data.as_mut_ptr().cast::<u8>()) };
    }
}

impl Collector {
    pub fn new() -> Self {
        let mut active_array = Vec::with_capacity(LEN);
        let mut epoch_array = Vec::with_capacity(LEN);
        let mut retire_list = Vec::with_capacity(RETIRE_LEN);
        for _ in 0..LEN {
            active_array.push(AtomicBool::new(false));
            epoch_array.push(AtomicUsize::new(0));
        }
        for _ in 0..RETIRE_LEN {
            retire_list.push(AtomicPtr::default());
        }
        Self {
            active_array,
            epoch_array,
            global_epoch: Default::default(),
            retire_list,
            state: Default::default(),
            hasher: RandomState::new(),
        }
    }
    #[inline]
    pub fn pin(&self) -> Guard {
        let thread_id: u64 = THREAD_ID.with(|f| *f);
        let mut index = HASH_BITS & thread_id as usize;
        while self.active_array[index]
            .compare_exchange_weak(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            index = (index + 1) & HASH_BITS;
        }

        let global_epoch = self.global_epoch.load(Ordering::Relaxed);
        self.epoch_array[index].store(global_epoch, Ordering::Release);
        Guard {
            collector: &self,
            epoch: global_epoch,
            index,
            hash: self.hasher.hash_one(thread_id) as usize,
        }
    }

    pub fn try_gc(&self) {
        if self
            .state
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            let mut e = self.global_epoch.load(Ordering::Acquire);
            let epoch_array = &self.epoch_array;
            let active_array = &self.active_array;
            let mut update = true;
            for index in 0..LEN {
                if active_array[index].load(Ordering::Acquire)
                    && epoch_array[index].load(Ordering::Acquire) != e
                {
                    update = false;
                    break;
                }
            }
            if update {
                e = (e + 1) & (RETIRE_LEN - 1);
                self.global_epoch.store(e, Ordering::Release);
            }
            unsafe {
                self.free((e + 1) & (RETIRE_LEN - 1));
            }
            self.state.store(false, Ordering::Release);
        }
    }
    unsafe fn free(&self, epoch: usize) {
        let retire = &self.retire_list[epoch];
        let mut p = retire.swap(ptr::null_mut(), Ordering::Acquire);
        while !p.is_null() {
            let c = Box::from_raw(p);
            p = c.next.load(Ordering::Acquire);
            c.call();
        }
    }
}

pub struct Guard<'a> {
    collector: &'a Collector,
    epoch: usize,
    index: usize,
    hash: usize,
}

impl<'a> Drop for Guard<'a> {
    fn drop(&mut self) {
        let index = self.index;
        self.collector.epoch_array[index].store(usize::MAX, Ordering::Relaxed);
        self.collector.active_array[index].store(false, Ordering::Release);
        self.collector.try_gc();
    }
}

impl<'a> Guard<'a> {
    pub fn unpin(self) {
        drop(self);
    }
    pub fn defer_destroy<T>(&self, p: *mut T) {
        unsafe { self.defer_unchecked(move || drop(Box::from_raw(p))) }
    }
    pub unsafe fn defer_unchecked<F>(&self, f: F)
    where
        F: FnOnce(),
    {
        let f = Box::from_raw(Box::into_raw(Box::new(f)));
        let c = Collectible::new(f);
        let c_p = Box::into_raw(Box::new(c));
        let next = &(*c_p).next;
        let epoch = self.epoch;
        let hash = epoch.wrapping_add(self.hash) & (RETIRE_LEN - 1);
        let hd = if hash == epoch.wrapping_sub(1) || hash == epoch.wrapping_add(1) {
            &self.collector.retire_list[epoch]
        } else {
            &self.collector.retire_list[hash]
        };
        loop {
            let p = hd.load(Ordering::Relaxed);
            if !p.is_null() {
                next.store(p, Ordering::Relaxed);
            }
            match hd.compare_exchange(p, c_p, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(_) => {}
            }
        }
    }
}
