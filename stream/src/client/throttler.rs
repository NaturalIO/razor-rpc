use std::cell::Cell;

use crossfire::waitgroup::{WaitGroup, WaitGroupGuard};

pub struct Throttler {
    wg: WaitGroup<()>,
    thresholds: Cell<usize>,
}

unsafe impl Sync for Throttler {}

impl Throttler {
    pub fn new(thresholds: usize) -> Self {
        Throttler { wg: WaitGroup::new((), thresholds), thresholds: Cell::new(thresholds) }
    }

    #[inline(always)]
    pub fn nearly_full(&self) -> bool {
        self.wg.get_left() + 1 > self.thresholds.get()
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.wg.get_left() >= self.thresholds.get()
    }

    #[inline(always)]
    pub async fn throttle(&self) {
        let target = self.thresholds.get();
        if target > 0 {
            return self.wg.wait_async().await;
        }
    }

    #[inline(always)]
    pub fn add_task(&self) -> WaitGroupGuard<()> {
        self.wg.add_guard()
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn set_thresholds(&mut self, thresholds: usize) {
        self.thresholds.set(thresholds);
        self.wg.set_threshold(thresholds);
    }

    #[inline(always)]
    pub fn get_inflight_count(&self) -> usize {
        self.wg.get_left()
    }
}
