//! First-arrival deduplication of RLS1 symbols on the fast plane.
//!
//! Audio comes in full from two parents, so most of its symbols arrive
//! twice. The first copy is forwarded and teed; the second is dropped here.
//! The table is allocated once: 4-way buckets, the oldest entry of a full
//! bucket replaced. An entry that is replaced early costs at most one extra
//! forward of a late duplicate, which the child's receiver absorbs as it
//! does today.
//!
//! The key holds no hash of the datagram: two contributors publish the same
//! parts under the same keys, and whether their bytes are the same is the
//! thing to measure. So an entry also keeps a digest of the first copy's
//! bytes, when it arrived and on which lane, and a second copy is reported
//! with whether its bytes are equal and how much later it came. Every
//! duplicate in the mesh is meant to be a verbatim copy: one that differs
//! is a fault of the senders.

use std::hash::{BuildHasher, Hasher};
use xxhash_rust::xxh3::xxh3_64;

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

/// What the table says of a symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arrival {
    /// The first copy seen: it goes on.
    First,
    /// A further copy of a symbol already seen: it is dropped.
    Duplicate {
        /// Its digest is the first copy's: the same bytes.
        equal: bool,
        /// How much later than the first copy it arrived. Zero when its
        /// arrival time is not the later one (its socket was read second),
        /// or when it is more than 35 minutes later (the times kept are 32
        /// bits of microseconds).
        lag_us: u32,
        /// The lane that the first copy came on.
        first_lane: u8,
    },
}

/// The digest of a datagram that the table compares copies by: the low 32
/// bits of XXH3-64 over all of its bytes.
#[must_use]
pub fn digest(datagram: &[u8]) -> u32 {
    xxh3_64(datagram) as u32
}

/// One symbol seen. The key's fields lie beside the rest, not in a
/// `SymbolKey` of their own, so that an entry is 48 bytes.
#[derive(Debug, Clone, Copy, Default)]
struct Slot {
    names: u64,
    epoch: u64,
    object: u64,
    stamp: u64,
    packet_sequence: u32,
    /// Of the first copy's bytes; zero where the bytes are not compared.
    digest: u32,
    /// The low 32 bits of the first copy's arrival time in microseconds.
    rx_us: u32,
    /// The lane that the first copy came on.
    lane: u8,
}

impl Slot {
    fn holds(&self, key: &SymbolKey) -> bool {
        self.names == key.names
            && self.epoch == key.epoch
            && self.object == key.object
            && self.packet_sequence == key.packet_sequence
    }
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

    /// Record a symbol that arrived at `rx_us` on `lane`, with the `digest`
    /// of its datagram (zero where the bytes are not compared: every copy
    /// is then equal). The first time it is `First`; after that it is a
    /// `Duplicate`, compared with the first copy, which stays the one kept.
    pub fn first(&mut self, key: SymbolKey, digest: u32, rx_us: u64, lane: u8) -> Arrival {
        let mixed = key
            .names
            .wrapping_add(key.epoch.rotate_left(17))
            .wrapping_add(key.object.wrapping_mul(0x9e37_79b9_7f4a_7c15))
            .wrapping_add(u64::from(key.packet_sequence).wrapping_mul(0xc2b2_ae3d_27d4_eb4f));
        let bucket = ((mixed ^ (mixed >> 29)) as usize & self.mask) * WAYS;
        let rx_us = rx_us as u32;
        self.clock += 1;
        let mut oldest = bucket;
        for index in bucket..bucket + WAYS {
            let slot = &self.slots[index];
            if slot.holds(&key) {
                let lag = rx_us.wrapping_sub(slot.rx_us) as i32;
                return Arrival::Duplicate {
                    equal: slot.digest == digest,
                    lag_us: lag.max(0) as u32,
                    first_lane: slot.lane,
                };
            }
            if slot.stamp < self.slots[oldest].stamp {
                oldest = index;
            }
        }
        self.slots[oldest] = Slot {
            names: key.names,
            epoch: key.epoch,
            object: key.object,
            stamp: self.clock,
            packet_sequence: key.packet_sequence,
            digest,
            rx_us,
            lane,
        };
        Arrival::First
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(table: &FirstArrival, epoch: u64, object: u64, sequence: u32) -> SymbolKey {
        table.key("t", "4001", "audio", 0, 1, 9, epoch, object, sequence)
    }

    /// A copy with the first copy's bytes, however late.
    fn is_equal_duplicate(arrival: Arrival) -> bool {
        matches!(arrival, Arrival::Duplicate { equal: true, .. })
    }

    #[test]
    fn a_duplicate_is_seen_once_and_a_new_epoch_is_new() {
        let mut table = FirstArrival::new(1_024);
        assert_eq!(table.first(key(&table, 1, 10, 100), 7, 0, 0), Arrival::First);
        assert!(is_equal_duplicate(table.first(key(&table, 1, 10, 100), 7, 0, 1)));
        assert_eq!(table.first(key(&table, 1, 10, 101), 7, 0, 0), Arrival::First);
        assert_eq!(table.first(key(&table, 1, 11, 100), 7, 0, 0), Arrival::First);
        // A contributor restart counts parts from zero in a new epoch.
        assert_eq!(table.first(key(&table, 2, 10, 100), 7, 0, 0), Arrival::First);
        assert!(is_equal_duplicate(table.first(key(&table, 2, 10, 100), 7, 0, 1)));
        let other_stream = table.key("t", "4002", "audio", 0, 1, 9, 1, 10, 100);
        assert_eq!(table.first(other_stream, 7, 0, 0), Arrival::First);
        // Another object version is another symbol: two contributors whose
        // bytes may differ are never merged.
        let other_version = table.key("t", "4001", "audio", 0, 2, 9, 1, 10, 100);
        assert_eq!(table.first(other_version, 7, 0, 0), Arrival::First);
    }

    #[test]
    fn a_recent_symbol_survives_a_full_table_of_older_ones() {
        let mut table = FirstArrival::new(65_536);
        for sequence in 0..65_536_u32 {
            let key = key(&table, 1, u64::from(sequence / 8), sequence);
            assert_eq!(table.first(key, sequence, 0, 0), Arrival::First);
        }
        // The last few thousand are still remembered, with their digests.
        let mut remembered = 0;
        for sequence in 60_000..65_536_u32 {
            let key = key(&table, 1, u64::from(sequence / 8), sequence);
            match table.first(key, sequence, 0, 0) {
                Arrival::First => {}
                Arrival::Duplicate { equal, .. } => {
                    assert!(equal, "symbol {sequence}");
                    remembered += 1;
                }
            }
        }
        assert!(remembered > 5_000, "{remembered}");
    }

    #[test]
    fn a_duplicate_is_compared_with_the_first_copy() {
        let mut table = FirstArrival::new(1_024);
        let datagram = b"RLC1 one part of one millisecond".to_vec();
        let mut other = datagram.clone();
        *other.last_mut().unwrap() ^= 1;
        assert_ne!(digest(&datagram), digest(&other));
        assert_eq!(digest(&datagram), digest(&datagram.clone()));

        let symbol = key(&table, 1, 10, 100);
        assert_eq!(table.first(symbol, digest(&datagram), 1_000, 0), Arrival::First);
        // The same bytes from the other lane.
        assert_eq!(
            table.first(symbol, digest(&datagram), 1_040, 1),
            Arrival::Duplicate {
                equal: true,
                lag_us: 40,
                first_lane: 0
            }
        );
        // One byte differs: not equal, and the first copy stays the one
        // that later copies are compared with.
        assert_eq!(
            table.first(symbol, digest(&other), 1_050, 1),
            Arrival::Duplicate {
                equal: false,
                lag_us: 50,
                first_lane: 0
            }
        );
        assert_eq!(
            table.first(symbol, digest(&datagram), 1_060, 1),
            Arrival::Duplicate {
                equal: true,
                lag_us: 60,
                first_lane: 0
            }
        );
        // Where bytes are not compared (video) the digest is zero and every
        // copy is equal.
        let video = key(&table, 1, 11, 7);
        assert_eq!(table.first(video, 0, 2_000, 1), Arrival::First);
        assert_eq!(
            table.first(video, 0, 2_500, 0),
            Arrival::Duplicate {
                equal: true,
                lag_us: 500,
                first_lane: 1
            }
        );
    }

    #[test]
    fn the_lag_is_the_later_arrival_less_the_first() {
        let mut table = FirstArrival::new(1_024);
        let lag = |arrival| match arrival {
            Arrival::Duplicate { lag_us, .. } => lag_us,
            Arrival::First => panic!("not a duplicate"),
        };
        // Unix time in microseconds: only its low 32 bits are kept.
        let now = 1_763_924_000_123_456_u64;
        let first = key(&table, 1, 1, 1);
        assert_eq!(table.first(first, 1, now, 0), Arrival::First);
        assert_eq!(lag(table.first(first, 1, now + 310, 1)), 310);
        assert_eq!(lag(table.first(first, 1, now + 20_000, 1)), 20_000);
        // The other socket was read second, but the kernel stamped its
        // copy earlier: no lag.
        assert_eq!(lag(table.first(first, 1, now - 25, 1)), 0);
        // Across a wrap of the 32 bits.
        let wrap = (now | 0xffff_ffff) - 10;
        let second = key(&table, 1, 2, 2);
        assert_eq!(table.first(second, 1, wrap, 0), Arrival::First);
        assert_eq!(lag(table.first(second, 1, wrap + 100, 1)), 100);
        // A copy more than 35 minutes late has no lag to show.
        assert_eq!(lag(table.first(second, 1, wrap + 2_200_000_000, 1)), 0);
    }

    #[test]
    fn an_entry_is_48_bytes_and_the_table_is_allocated_once() {
        assert_eq!(std::mem::size_of::<Slot>(), 48);
        // The class threads' table: 65,536 symbols, 3 MB.
        let table = FirstArrival::new(65_536);
        assert_eq!(table.slots.len(), 65_536);
        assert_eq!(std::mem::size_of_val(&*table.slots), 3_145_728);
        // A capacity that is not a power of two is rounded up; the least
        // table is one bucket.
        assert_eq!(FirstArrival::new(1_000).slots.len(), 1_024);
        assert_eq!(FirstArrival::new(0).slots.len(), WAYS);
    }
}
