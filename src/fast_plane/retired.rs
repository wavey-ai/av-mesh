//! Values that a class thread may still hold when another thread replaces
//! them (stall follow-up to wave 9).
//!
//! The class threads read their snapshots (lease children, the edge's
//! clients, the parent feeds, the demand reports) from an `ArcSwap` without
//! a lock. When a rebuild replaces one, `ArcSwap` turns every guard still
//! held into a counted reference; if the rebuild's thread then let go of
//! the old value, the class thread's guard would be the last reference, and
//! the real-time thread would free the value (a `HashMap`, its entries) into
//! another thread's allocator arena. A replacing thread hands the old value
//! here instead: it is kept while anyone else holds it, and freed by a later
//! replacement on that thread once no one does.

use std::sync::{Arc, Mutex};

pub struct Retired<T>(Mutex<Vec<Arc<T>>>);

impl<T> Default for Retired<T> {
    fn default() -> Self {
        Self(Mutex::new(Vec::new()))
    }
}

impl<T> std::fmt::Debug for Retired<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Retired")
    }
}

impl<T> Retired<T> {
    /// Keep `old` while others hold it; free what no one else holds any
    /// more, here. Called by the thread that replaced `old`.
    pub fn keep(&self, old: Arc<T>) {
        let mut kept = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        kept.push(old);
        kept.retain(|value| Arc::strong_count(value) > 1);
    }

    /// Values kept now.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_is_kept_while_a_reader_holds_it_and_freed_by_the_next_keep() {
        let retired = Retired::default();
        let swap = arc_swap::ArcSwap::from_pointee(1_u32);
        let guard = swap.load();
        // The replacement turns the reader's guard into a counted reference.
        retired.keep(swap.swap(Arc::new(2)));
        assert_eq!(retired.len(), 1, "the reader holds the old value");
        drop(guard);
        retired.keep(swap.swap(Arc::new(3)));
        // The first is freed here, the second (no reader) at once.
        assert_eq!(retired.len(), 0);
    }
}
