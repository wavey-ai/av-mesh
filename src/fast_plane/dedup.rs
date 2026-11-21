//! First-arrival deduplication of RLS1 symbols on the fast plane.
//!
//! Audio comes in full from two parents, so most of its symbols arrive
//! twice. The first copy is forwarded and teed; the second is dropped here.
//! The table is allocated once: 4-way buckets, the oldest entry of a full
//! bucket replaced. An entry that is replaced early costs at most one extra
//! forward of a late duplicate, which the child's receiver absorbs as it
//! does today.

use std::hash::{BuildHasher, Hasher};

const WAYS: usize = 4;

/// The identity of one symbol: the object (tenant, stream, track, epoch,
/// group, object, version) and the packet sequence of its RQD2 datagram.
/// The epoch is part of it, so a contributor restart that counts parts from
/// zero again is never mistaken for a duplicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SymbolKey {
    /// A hash of the strings of the object key and of its block id.
    pub names: u64,
    pub epoch: u64,
    pub object: u64,
    pub packet_sequence: u32,
}

#[derive(Debug, Clone, Copy, Default)]
struct Slot {
    key: SymbolKey,
    stamp: u64,
}

/// A fixed-size table of the symbols seen.
pub struct FirstArrival {
    slots: Box<[Slot]>,
    mask: usize,
    clock: u64,
    hasher: std::collections::hash_map::RandomState,
}

impl FirstArrival {
    /// A table of about `capacity` symbols (rounded up to a power of two).
    pub fn new(capacity: usize) -> Self {
        let buckets = (capacity / WAYS).max(1).next_power_of_two();
        Self {
            slots: vec![Slot::default(); buckets * WAYS].into_boxed_slice(),
            mask: buckets - 1,
            clock: 0,
            hasher: std::collections::hash_map::RandomState::new(),
        }
    }

    /// The key of a symbol.
    #[allow(clippy::too_many_arguments)]
    pub fn key(
        &self,
        tenant: &str,
        stream: &str,
        track: &str,
        group: u64,
        version: u32,
        block_id: u32,
        epoch: u64,
        object: u64,
        packet_sequence: u32,
    ) -> SymbolKey {
        let mut hasher = self.hasher.build_hasher();
        hasher.write(tenant.as_bytes());
        hasher.write_u8(0xff);
        hasher.write(stream.as_bytes());
        hasher.write_u8(0xff);
        hasher.write(track.as_bytes());
        hasher.write_u64(group);
        hasher.write_u32(version);
        hasher.write_u32(block_id);
        SymbolKey {
            // Zero marks an empty slot.
            names: hasher.finish() | 1,
            epoch,
            object,
            packet_sequence,
        }
    }

    /// Record a symbol. `true` the first time it is seen.
    pub fn first(&mut self, key: SymbolKey) -> bool {
        let mixed = key
            .names
            .wrapping_add(key.epoch.rotate_left(17))
            .wrapping_add(key.object.wrapping_mul(0x9e37_79b9_7f4a_7c15))
            .wrapping_add(u64::from(key.packet_sequence).wrapping_mul(0xc2b2_ae3d_27d4_eb4f));
        let bucket = ((mixed ^ (mixed >> 29)) as usize & self.mask) * WAYS;
        self.clock += 1;
        let mut oldest = bucket;
        for index in bucket..bucket + WAYS {
            let slot = &self.slots[index];
            if slot.key == key {
                return false;
            }
            if slot.stamp < self.slots[oldest].stamp {
                oldest = index;
            }
        }
        self.slots[oldest] = Slot {
            key,
            stamp: self.clock,
        };
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(table: &FirstArrival, epoch: u64, object: u64, sequence: u32) -> SymbolKey {
        table.key("t", "4001", "audio", 0, 1, 9, epoch, object, sequence)
    }

    #[test]
    fn a_duplicate_is_seen_once_and_a_new_epoch_is_new() {
        let mut table = FirstArrival::new(1_024);
        assert!(table.first(key(&table, 1, 10, 100)));
        assert!(!table.first(key(&table, 1, 10, 100)));
        assert!(table.first(key(&table, 1, 10, 101)));
        assert!(table.first(key(&table, 1, 11, 100)));
        // A contributor restart counts parts from zero in a new epoch.
        assert!(table.first(key(&table, 2, 10, 100)));
        assert!(!table.first(key(&table, 2, 10, 100)));
        let other_stream = table.key("t", "4002", "audio", 0, 1, 9, 1, 10, 100);
        assert!(table.first(other_stream));
    }

    #[test]
    fn a_recent_symbol_survives_a_full_table_of_older_ones() {
        let mut table = FirstArrival::new(65_536);
        for sequence in 0..65_536_u32 {
            assert!(table.first(key(&table, 1, u64::from(sequence / 8), sequence)));
        }
        // The last few thousand are still remembered.
        let mut remembered = 0;
        for sequence in 60_000..65_536_u32 {
            if !table.first(key(&table, 1, u64::from(sequence / 8), sequence)) {
                remembered += 1;
            }
        }
        assert!(remembered > 5_000, "{remembered}");
    }
}
