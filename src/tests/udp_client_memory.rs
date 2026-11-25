//! The UDP client's memory (udp-client memory round, 25 Nov 2026).
//!
//! The referee's 4K probe (`needletail-udp-client --arrivals --write-parts`)
//! kept every part's bytes to write them at the end of the run, and every
//! part's arrival, account entry and timing samples: 7.6 GB at 240 Mbit/s
//! after 300 s, and the kernel killed it on every viewer host. These tests
//! run what the client does with each part it takes (the part ledger, the
//! timing samples, the arrivals line and the bytes handed to the writer
//! thread, `av_mesh::client_record`) far faster than real time, for a
//! simulated 10 minutes of 4K at 320 Mbit/s and an hour of 1 ms audio, and
//! read the allocator's live bytes (`crate::alloc_count`) every simulated
//! 10 s: they must stay under 300 MB and flat. Each runs alone in a child
//! process of this test binary, so that the live bytes are its own.

use crate::alloc_count;
use av_mesh::client_compact::STALE_PARTS;
use av_mesh::client_record::{
    BoundedSamples, Mark, PartLedger, PartsWriter, ORDER_LIMITS, QUEUE_BYTES,
};
use bytes::Bytes;
use std::io::{self, Write};
use std::process::Command;
use std::time::Duration;

const MB: u64 = 1 << 20;
const CHILD: &str = "NEEDLETAIL_UDP_MEMORY_CHILD";

/// Run the ignored test `name` of this module alone in a child process and
/// return what it printed; it fails if the child does.
fn in_child(name: &str) -> String {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            &format!("tests::udp_client_memory::{name}"),
            "--exact",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, "1")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    println!("{stdout}{}", String::from_utf8_lossy(&output.stderr));
    assert!(
        output.status.success(),
        "{name} failed in its child process"
    );
    assert!(stdout.contains("1 passed"), "{name} did not run");
    stdout
}

fn live() -> u64 {
    alloc_count::live_bytes_by_class().iter().sum()
}

/// A disk that stalls: each write between `from` and `to` (counted in
/// writes) takes `stall`.
struct SlowDisk {
    writes: u64,
    from: u64,
    to: u64,
    stall: Duration,
    bytes: u64,
}

impl Write for SlowDisk {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writes += 1;
        if (self.from..self.to).contains(&self.writes) {
            std::thread::sleep(self.stall);
        }
        self.bytes += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The client's per-part work: what `needletail-udp-client` does with a
/// part it takes (and with each datagram's kernel time).
struct Client {
    ledger: PartLedger,
    publish_ms: BoundedSamples,
    available_ms: BoundedSamples,
    host_ms: BoundedSamples,
    writer: PartsWriter,
    duplicates: u64,
}

impl Client {
    fn new<A: Write + Send + 'static, P: Write + Send + 'static>(
        arrivals: Option<A>,
        parts: Option<P>,
    ) -> Self {
        Self {
            ledger: PartLedger::new(STALE_PARTS),
            publish_ms: BoundedSamples::new(),
            available_ms: BoundedSamples::new(),
            host_ms: BoundedSamples::new(),
            writer: PartsWriter::spawn(arrivals, parts, ORDER_LIMITS, QUEUE_BYTES).unwrap(),
            duplicates: 0,
        }
    }

    fn take(&mut self, part: u64, payload: Bytes, datagrams: u64, at_us: u64) {
        for datagram in 0..datagrams {
            self.host_ms.push((datagram % 97) as f64 / 1_000.0);
        }
        match self.ledger.receive(part) {
            Mark::New => {}
            Mark::Duplicate | Mark::BeyondWindow => {
                self.duplicates += 1;
                return;
            }
        }
        let published = at_us - 40_000 - part % 3_000;
        self.publish_ms
            .push(at_us.saturating_sub(published) as f64 / 1_000.0);
        self.available_ms.push((part % 1_700) as f64 / 1_000.0);
        let line = format!(
            "{part},{at_us},{},{published},{},{},lon-edge-1,fast\n",
            payload.len(),
            at_us - 2_000,
            at_us - 15
        );
        self.writer.send(part, Some(line), Some(payload));
    }

    fn held(&self) -> usize {
        self.ledger.held_bytes()
            + self.publish_ms.held_bytes()
            + self.available_ms.held_bytes()
            + self.host_ms.held_bytes()
            + self.writer.queued_bytes()
    }
}

/// A part of a 4K stream at `rate` bytes a second, 24 fps: a key frame of
/// four times the others every 48 parts (1.6 MB and 6.3 MB at 320 Mbit/s).
fn frame_bytes(rate: u64, sequence: u64) -> usize {
    let p = rate * 2 / 51;
    (if sequence.is_multiple_of(48) {
        4 * p
    } else {
        p
    }) as usize
}

#[test]
fn a_4k_viewer_at_320_mbits_holds_flat_memory() {
    in_child("four_k_viewer_run");
}

#[test]
fn a_1ms_audio_listener_for_an_hour_holds_flat_memory() {
    in_child("audio_listener_run");
}

/// Ten simulated minutes of 4K at 320 Mbit/s (14,400 parts, 24 GB): a part
/// in 300 never comes (the parts after it wait for it up to the writer's
/// byte bound), a part in 50 comes after the next one, every part comes
/// again from the other leg, and the disk stalls for a while (the queue
/// fills, parts are dropped from the files and counted, and the receive
/// side never waits).
#[test]
#[ignore = "run alone in a child process by a_4k_viewer_at_320_mbits_holds_flat_memory"]
fn four_k_viewer_run() {
    assert!(std::env::var_os(CHILD).is_some(), "run by its parent test");
    const FPS: u64 = 24;
    const SECONDS: u64 = 600;
    let rate = 320_000_000 / 8;
    let disk = SlowDisk {
        writes: 0,
        from: 6_000,
        to: 6_100,
        stall: Duration::from_millis(5),
        bytes: 0,
    };
    let before = live();
    let mut client = Client::new(Some(io::sink()), Some(disk));
    let mut old_retained = 0_u64;
    let mut curve = Vec::new();
    let mut at_us = 1_764_000_000_000_000_u64;
    let mut swapped: Option<(u64, Bytes)> = None;
    println!("  sim s   live MB   client-held MB   old path MB");
    for part in 0..FPS * SECONDS {
        at_us += 1_000_000 / FPS;
        let bytes = frame_bytes(rate, part);
        if part % 300 == 7 {
            continue;
        }
        let payload = Bytes::from(vec![0_u8; bytes]);
        // What the code before kept of this part to the end of the run.
        old_retained += bytes as u64 + 56 + 3 * 8 + bytes as u64 / 1_400 * 8;
        let datagrams = bytes as u64 / 1_400;
        if part % 50 == 3 {
            swapped = Some((part, payload));
            continue;
        }
        // Far faster than real time (40 MB/s), but not so fast that a
        // disk at full speed falls behind.
        std::thread::sleep(Duration::from_micros(200));
        client.take(part, payload.clone(), datagrams, at_us);
        if let Some((late, late_payload)) = swapped.take() {
            client.take(late, late_payload, datagrams, at_us);
        }
        // The other leg's copy.
        client.take(part, payload, 0, at_us + 300);
        if (part + 1) % (10 * FPS) == 0 {
            // Let the writer catch up a little, as real time would.
            std::thread::sleep(Duration::from_millis(2));
            let now = live().saturating_sub(before);
            curve.push(now);
            println!(
                "  {:5}  {:8}  {:15}  {:12}",
                (part + 1) / FPS,
                now / MB,
                client.held() as u64 / MB,
                old_retained / MB
            );
        }
    }
    let writer = client.writer.finish().unwrap();
    println!("writer: {writer:?}");
    println!(
        "peak live {} MB; old path at the end {} MB",
        curve.iter().max().unwrap() / MB,
        old_retained / MB
    );
    assert!(client.ledger.received() > 14_000);
    assert!(client.duplicates > 14_000);
    assert!(
        !client.host_ms.is_exact(),
        "the datagram samples were folded"
    );
    // Bounded: the writer's queue and its order window, and little else,
    // whatever the run's length (the code before held all 23 GB).
    let peak = *curve.iter().max().unwrap();
    assert!(peak < 300 * MB, "peak live {} MB", peak / MB);
    let bound = (QUEUE_BYTES + ORDER_LIMITS.max_bytes + 3 * frame_bytes(rate, 0)) as u64 + 16 * MB;
    assert!(
        curve.iter().all(|live| *live <= bound),
        "live bytes past the writer's bounds ({} MB)",
        bound / MB
    );
    let after = live().saturating_sub(before);
    println!("live after the run: {} MB", after / MB);
    assert!(
        writer.output.held_peak_bytes <= (ORDER_LIMITS.max_bytes + frame_bytes(rate, 0)) as u64
    );
    assert!(writer.queued_peak_bytes <= (QUEUE_BYTES + frame_bytes(rate, 0)) as u64);
    // The stalled disk cost parts in the files, counted, not memory.
    assert!(writer.queue_full > 0, "the stalled disk dropped nothing");
    assert!(writer.output.written + writer.queue_full + writer.output.late > 14_000);
    assert!(old_retained > 20_000 * MB);
}

/// An hour of 1 ms audio (3.6 million parts of 240 bytes): a part in 1,000
/// lost and NACKed 20 parts later, a gap in 5,000, every part twice.
#[test]
#[ignore = "run alone in a child process by a_1ms_audio_listener_for_an_hour_holds_flat_memory"]
fn audio_listener_run() {
    assert!(std::env::var_os(CHILD).is_some(), "run by its parent test");
    const PARTS: u64 = 3_600_000;
    let before = live();
    let mut client = Client::new(Some(io::sink()), Some(io::sink()));
    let mut old_retained = 0_u64;
    let mut curve = Vec::new();
    let mut at_us = 1_764_000_000_000_000_u64;
    let payload = Bytes::from(vec![7_u8; 240]);
    println!("  sim s   live MB   client-held MB   old path MB");
    for part in 0..PARTS {
        at_us += 1_000;
        if part % 5_000 == 11 {
            client.ledger.gap(part);
            continue;
        }
        old_retained += 240 + 56 + 3 * 8 + 8 + 48;
        if part % 1_000 == 5 {
            continue;
        }
        if part % 1_000 == 25 {
            client.take(part - 20, payload.clone(), 1, at_us);
        }
        client.take(part, payload.clone(), 1, at_us);
        client.take(part, payload.clone(), 1, at_us + 200);
        if (part + 1) % 10_000 == 0 {
            if (part + 1) % 300_000 == 0 {
                std::thread::sleep(Duration::from_millis(2));
            }
            let now = live().saturating_sub(before);
            curve.push(now);
            if (part + 1) % 300_000 == 0 {
                println!(
                    "  {:5}  {:8}  {:15}  {:12}",
                    (part + 1) / 1_000,
                    now / MB,
                    client.held() as u64 / MB,
                    old_retained / MB
                );
            }
        }
    }
    let writer = client.writer.finish().unwrap();
    println!("writer: {writer:?}");
    let account = client.ledger.account();
    assert_eq!(account.missing, 0, "every hole was filled");
    assert_eq!(client.ledger.gaps(), PARTS / 5_000);
    assert_eq!(writer.output.late, 0);
    let peak = *curve.iter().max().unwrap();
    println!(
        "peak live {} MB; old path at the end {} MB",
        peak / MB,
        old_retained / MB
    );
    assert!(peak < 64 * MB, "peak live {} MB", peak / MB);
    let first: u64 = *curve[..60].iter().max().unwrap();
    let last: u64 = *curve[curve.len() - 60..].iter().max().unwrap();
    assert!(
        last <= first + 8 * MB,
        "live bytes grew: {} MB in the first 10 min, {} MB in the last",
        first / MB,
        last / MB
    );
    assert!(old_retained > 1_000 * MB);
}
