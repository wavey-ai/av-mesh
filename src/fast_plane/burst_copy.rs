//! Burst copies on the long-haul lanes (low-latency wave 5, W2).
//!
//! A long-haul path that loses datagrams in bursts (the referee's netem
//! `gemodel 1% 20%`: bursts of five datagrams on average, longer ones not
//! rare) takes a part's source symbol and the repair symbols that follow it
//! back to back in one burst: the part is lost at the next forwarder, and a
//! later part's repair cannot help it (RaptorQ repairs one object). Its hole
//! then held the stream's watermark.
//!
//! The audio thread of a forwarder therefore sends each audio source symbol
//! that it forwards to a compiled child (the long-haul forwarders of the
//! contributor's region, the distributors of a relay) a second time,
//! `--relay-burst-copy-ms` later: the same bytes, from the same socket.
//! A burst shorter than that delay cannot take both copies. The child needs
//! nothing new: its first-arrival table drops the copy of a symbol that
//! came, and takes the copy of one that a burst took. The copy costs the
//! audio sources once more on the long-haul lanes (2.5 ms Opus about 0.3
//! Mbit/s, 1 ms PCM about 3.6 Mbit/s per child), and a part that a burst
//! took arrives the delay late instead of never. The lease children (the
//! edges of the region, on its own network) get no copy.
//!
//! The copies wait in a ring of preallocated buffers: the thread allocates
//! nothing for them, and sends the copies that are due after each batch it
//! drains, and on its own when its wait for the sockets ends (the wait is no
//! longer than the next copy's due time). A full ring drops the oldest copy
//! and counts it.

use std::collections::VecDeque;
use std::net::SocketAddr;

/// The default of `--relay-burst-copy-ms`: longer than a burst of 20
/// datagrams on a lane of 2.5 ms Opus (about one burst in a hundred at
/// `gemodel 1% 20%`), short beside the repair horizon.
pub const DEFAULT_BURST_COPY_MS: u64 = 20;
/// Copies waiting at most: 20 ms of 1 ms PCM to two children is 40, so
/// this is many streams' worth.
pub const CAPACITY: usize = 4_096;
/// The largest datagram copied (RelaySession's limit is 1,472 bytes).
pub const MAX_COPY_BYTES: usize = 1_500;

/// One copy: its bytes, where it goes and from which socket group, and
/// when.
struct Copy {
    due_us: u64,
    group: usize,
    target: SocketAddr,
    len: usize,
    buffer: Vec<u8>,
}

/// The copies that a class thread has to send, oldest first.
pub struct BurstCopies {
    delay_us: u64,
    waiting: VecDeque<Copy>,
    /// Buffers not in use, allocated once.
    free: Vec<Vec<u8>>,
    /// Copies dropped because the ring was full, or the datagram too large.
    pub dropped: u64,
}

impl BurstCopies {
    /// Copies sent `delay_us` after their original; `None` for no copies.
    pub fn new(delay_us: u64) -> Option<Self> {
        (delay_us > 0).then(|| Self {
            delay_us,
            waiting: VecDeque::with_capacity(CAPACITY),
            free: (0..CAPACITY)
                .map(|_| Vec::with_capacity(MAX_COPY_BYTES))
                .collect(),
            dropped: 0,
        })
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.waiting.len()
    }

    pub fn is_empty(&self) -> bool {
        self.waiting.is_empty()
    }

    /// Keep a copy of `datagram`, sent at `sent_us`, for `target` (from
    /// the socket group `group`).
    pub fn push(&mut self, datagram: &[u8], group: usize, target: SocketAddr, sent_us: u64) {
        if datagram.len() > MAX_COPY_BYTES {
            self.dropped += 1;
            return;
        }
        let mut buffer = match self.free.pop() {
            Some(buffer) => buffer,
            None => {
                // Full: the oldest copy makes room.
                self.dropped += 1;
                match self.waiting.pop_front() {
                    Some(oldest) => oldest.buffer,
                    None => return,
                }
            }
        };
        buffer.clear();
        buffer.extend_from_slice(datagram);
        self.waiting.push_back(Copy {
            due_us: sent_us.saturating_add(self.delay_us),
            group,
            target,
            len: datagram.len(),
            buffer,
        });
    }

    /// How long until the oldest copy is due, in whole milliseconds rounded
    /// up (0: now); `None` with no copy waiting.
    pub fn wait_ms(&self, now_us: u64) -> Option<i32> {
        let due_us = self.waiting.front()?.due_us;
        let wait_us = due_us.saturating_sub(now_us);
        Some(i32::try_from(wait_us.div_ceil(1_000)).unwrap_or(i32::MAX))
    }

    /// The copies due at `now_us`, oldest first, as `send` takes them: it is
    /// given the run of due copies of one socket group at a time and returns
    /// how many of them it sent. A copy that the socket refused is dropped
    /// (counted): it is late already, and the next one is due. Returns the
    /// copies sent.
    pub fn send_due(
        &mut self,
        now_us: u64,
        mut send: impl FnMut(usize, &[(&[u8], SocketAddr)]) -> usize,
    ) -> u64 {
        const RUN: usize = 32;
        let unspecified = SocketAddr::from(([0, 0, 0, 0], 0));
        let mut sent_total = 0_u64;
        loop {
            let Some(first) = self.waiting.front() else {
                return sent_total;
            };
            if first.due_us > now_us {
                return sent_total;
            }
            let group = first.group;
            // A run of due copies of one group, on the stack.
            let mut run: [(&[u8], SocketAddr); RUN] = [(&[], unspecified); RUN];
            let mut count = 0;
            for copy in self.waiting.iter().take(RUN) {
                if copy.due_us > now_us || copy.group != group {
                    break;
                }
                run[count] = (&copy.buffer[..copy.len], copy.target);
                count += 1;
            }
            let sent = send(group, &run[..count]).min(count);
            sent_total += sent as u64;
            self.dropped += (count - sent) as u64;
            for _ in 0..count {
                if let Some(copy) = self.waiting.pop_front() {
                    self.free.push(copy.buffer);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn no_delay_means_no_copies() {
        assert!(BurstCopies::new(0).is_none());
    }

    #[test]
    fn a_copy_leaves_its_delay_after_the_original_byte_for_byte() {
        let mut copies = BurstCopies::new(20_000).unwrap();
        copies.push(b"source-1", 0, addr(1), 1_000_000);
        copies.push(b"source-2", 0, addr(2), 1_000_500);
        assert_eq!(copies.wait_ms(1_000_000), Some(20));
        assert_eq!(copies.wait_ms(1_019_999), Some(1));
        let mut sent = Vec::new();
        let none = copies.send_due(1_019_999, |group, run| {
            sent.extend(run.iter().map(|(bytes, to)| (group, bytes.to_vec(), *to)));
            run.len()
        });
        assert_eq!(none, 0);
        assert!(sent.is_empty());
        assert_eq!(
            copies.send_due(1_020_000, |group, run| {
                sent.extend(run.iter().map(|(bytes, to)| (group, bytes.to_vec(), *to)));
                run.len()
            }),
            1
        );
        assert_eq!(sent, vec![(0, b"source-1".to_vec(), addr(1))]);
        assert_eq!(copies.wait_ms(1_020_000), Some(1));
        assert_eq!(
            copies.send_due(1_030_000, |_, run| run.len()),
            1,
            "the second copy"
        );
        assert!(copies.is_empty());
        assert_eq!(copies.wait_ms(1_030_000), None);
        assert_eq!(copies.dropped, 0);
    }

    #[test]
    fn due_copies_go_in_runs_of_one_socket_group() {
        let mut copies = BurstCopies::new(1_000).unwrap();
        for (index, group) in [0, 0, 1, 1, 0].into_iter().enumerate() {
            copies.push(&[index as u8], group, addr(index as u16), 0);
        }
        let mut runs = Vec::new();
        assert_eq!(
            copies.send_due(5_000, |group, run| {
                runs.push((
                    group,
                    run.iter().map(|(bytes, _)| bytes[0]).collect::<Vec<_>>(),
                ));
                run.len()
            }),
            5
        );
        assert_eq!(runs, vec![(0, vec![0, 1]), (1, vec![2, 3]), (0, vec![4])]);
    }

    #[test]
    fn a_refused_copy_is_dropped_and_counted() {
        let mut copies = BurstCopies::new(1_000).unwrap();
        for index in 0..3_u8 {
            copies.push(&[index], 0, addr(1), 0);
        }
        assert_eq!(copies.send_due(1_000, |_, _| 1), 1);
        assert_eq!(copies.dropped, 2);
        assert!(copies.is_empty());
    }

    #[test]
    fn a_full_ring_drops_its_oldest_copy_and_allocates_nothing() {
        let mut copies = BurstCopies::new(1_000).unwrap();
        for index in 0..CAPACITY + 3 {
            copies.push(&(index as u32).to_be_bytes(), 0, addr(1), index as u64);
        }
        assert_eq!(copies.len(), CAPACITY);
        assert_eq!(copies.dropped, 3);
        let mut first = None;
        copies.send_due(u64::MAX, |_, run| {
            first.get_or_insert_with(|| run[0].0.to_vec());
            run.len()
        });
        assert_eq!(first, Some(3_u32.to_be_bytes().to_vec()));
        // Every buffer is back, with its capacity.
        assert_eq!(copies.free.len(), CAPACITY);
        assert!(copies
            .free
            .iter()
            .all(|buffer| buffer.capacity() >= MAX_COPY_BYTES));
        copies.push(&[0; MAX_COPY_BYTES + 1], 0, addr(1), 0);
        assert_eq!(copies.dropped, 4);
    }
}
