use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::{ptr, thread};

const LEN: usize = 1 << 12;
const RETIRE_LEN: usize = 1 << 8;
const HASH_BITS: usize = LEN - 1;

pub struct Collector {
    active_array: Vec<AtomicBool>,
    epoch_array: Vec<AtomicUsize>,
    global_epoch: AtomicUsize,
    retire_list: Vec<AtomicPtr<Collectible>>,
    state: AtomicBool,
}

struct Collectible {
    f: Box<dyn FnOnce()>,
    next: AtomicPtr<Collectible>,
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
        }
    }
    pub fn pin(&self) -> Guard {
        let id: u64 = unsafe { std::mem::transmute(thread::current().id()) };
        let mut index = HASH_BITS & id as usize;
        let active_array = &self.active_array;
        while active_array[index]
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            index = (index + 1) & HASH_BITS;
        }

        let global_epoch = self.global_epoch.load(Ordering::Relaxed);
        self.epoch_array[index].store(global_epoch, Ordering::Relaxed);
        Guard {
            collector: &self,
            epoch: global_epoch,
            index,
        }
    }

    pub fn try_gc(&self) {
        if self
            .state
            .compare_exchange_weak(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            let e = self.global_epoch.load(Ordering::Acquire);
            let epoch_array = &self.epoch_array;
            let active_array = &self.active_array;
            for index in 0..LEN {
                if active_array[index].load(Ordering::Acquire)
                    && epoch_array[index].load(Ordering::Acquire) != e
                {
                    let epoch2 = (e - 1) & (RETIRE_LEN - 1);
                    unsafe {
                        self.free(e, epoch2);
                    }
                    return;
                }
            }
            let epoch2 = (e + 1) & (RETIRE_LEN - 1);
            self.global_epoch.store(epoch2, Ordering::Release);
            unsafe {
                self.free(e, epoch2);
            }
            self.state.store(false, Ordering::Release);
        }
    }
    unsafe fn free(&self, epoch1: usize, epoch2: usize) {
        let retire_list = &self.retire_list;
        for (index, retire) in retire_list.iter().enumerate() {
            if index != epoch1 && index != epoch2 {
                let mut p = retire.swap(ptr::null_mut(), Ordering::Acquire);
                while !p.is_null() {
                    let c = Box::from_raw(p);
                    let f = c.f;
                    f();
                    p = c.next.load(Ordering::Acquire);
                }
            }
        }
    }
}

pub struct Guard<'a> {
    collector: &'a Collector,
    epoch: usize,
    index: usize,
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
    pub fn defer_destroy<T: 'static>(&mut self, p: *mut T) {
        unsafe { self.defer_unchecked(move || drop(Box::from_raw(p))) }
    }
    pub unsafe fn defer_unchecked<F>(&mut self, f: F)
    where
        F: FnOnce() + 'static,
    {
        let c = Collectible {
            f: Box::new(f),
            next: Default::default(),
        };
        let c_p = Box::into_raw(Box::new(c));
        let next = &(*c_p).next;
        let epoch = self.epoch;
        let hd = &self.collector.retire_list[epoch];
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
