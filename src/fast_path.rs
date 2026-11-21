//! The media fast path of a node: batched sends, busy polling on the
//! sockets that carry the media, kernel buffer limits, and an optional media
//! thread pinned to one processor.
//!
//! Everything here is a setting with the faster behaviour as its default,
//! except the pinned thread, which is off unless `--media-cpu` names a CPU,
//! and busy polling, which is off unless `--busy-poll-us` asks for it (see
//! [`DEFAULT_BUSY_POLL_US`] for why).
//! What the host allowed is recorded in a [`FastPathSnapshot`] that the node
//! reports beside its mesh transport settings, so a lab run can see whether
//! busy polling or the pinning actually took effect.

use serde::Serialize;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::net::UdpSocket;

/// The default busy-poll budget for sockets that carry media, in µs: off.
///
/// The per-socket budget (SO_BUSY_POLL) is used inside a receive call that
/// finds the queue empty, and only once for a non-blocking socket. The
/// runtime receives after epoll has reported the socket readable, so the
/// queue is not empty and the wait for the next datagram does not poll;
/// only the last receive of each drain, which finds the queue empty, costs
/// one extra NAPI poll. The wait itself busy-polls only with the host's
/// `net.core.busy_poll` (reported as `host_busy_poll_us`), which the
/// deploy does not set. So the budget is a setting for a lab A/B, off until
/// a measurement shows a gain.
pub const DEFAULT_BUSY_POLL_US: u32 = 0;

/// Messages handed to the kernel in one `sendmmsg` call. A relay rarely has
/// more children than this; longer batches are sent in several calls.
pub const MAX_BATCH_MESSAGES: usize = 32;

/// What the node asked of the host and what it got.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct FastPathSnapshot {
    /// Datagrams to several children that share a source socket leave in one
    /// `sendmmsg` call. This is the setting; see
    /// `relay_forward_batch_effective` for whether any socket is shared.
    pub relay_forward_batch: bool,
    /// Whether the setting does anything on this node: it is on and at
    /// least one source socket of the relay forwarder serves more than one
    /// child. A relay whose children each have a bind of their own (the
    /// Linode deploy gives every carrier link its own, ECMP-hunted, source
    /// port) sends one datagram per call as before, and reports false.
    pub relay_forward_batch_effective: bool,
    /// Source sockets of the relay forwarder that serve more than one child.
    pub relay_forward_shared_sockets: usize,
    /// Source sockets of the relay forwarder.
    pub relay_forward_sockets: usize,
    /// The busy-poll budget asked for, in µs. Zero is off.
    pub busy_poll_us: u32,
    /// Sockets that took the busy-poll budget.
    pub busy_poll_applied: Vec<String>,
    /// Sockets where the kernel refused it. Raising SO_BUSY_POLL above the
    /// host's `net.core.busy_read` needs CAP_NET_ADMIN, so an unprivileged
    /// node lands here and runs as before.
    pub busy_poll_denied: Vec<String>,
    /// Other failures, with the reason.
    pub busy_poll_failed: Vec<String>,
    /// The host's `net.core.busy_poll`, which governs busy polling inside
    /// epoll, the way the runtime waits for sockets. When it is zero only
    /// the receive calls themselves poll.
    pub host_busy_poll_us: Option<u32>,
    /// The CPU asked for with `--media-cpu`.
    pub media_cpu: Option<usize>,
    /// Whether the media thread is pinned to it.
    pub media_cpu_pinned: bool,
    /// Why the pinning failed, if it did.
    pub media_cpu_error: Option<String>,
}

/// The node's record of its fast path, filled in while it starts.
#[derive(Debug, Clone, Default)]
pub struct FastPath {
    busy_poll_us: u32,
    snapshot: Arc<Mutex<FastPathSnapshot>>,
}

impl FastPath {
    pub fn new(busy_poll_us: u32, relay_forward_batch: bool) -> Self {
        let snapshot = FastPathSnapshot {
            relay_forward_batch,
            busy_poll_us,
            host_busy_poll_us: read_host_busy_poll_us(),
            ..FastPathSnapshot::default()
        };
        Self {
            busy_poll_us,
            snapshot: Arc::new(Mutex::new(snapshot)),
        }
    }

    pub fn snapshot(&self) -> FastPathSnapshot {
        self.lock().clone()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FastPathSnapshot> {
        self.snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Ask for busy polling on a socket that receives media and record the
    /// outcome under `purpose`.
    pub fn tune_receive_socket(&self, socket: &impl std::os::fd::AsFd, purpose: &str) {
        let outcome = apply_busy_poll(socket, self.busy_poll_us);
        self.record_busy_poll(purpose, outcome);
    }

    /// Ask for busy polling on the socket of this process bound to `local`,
    /// for sockets another crate owns (the cache mesh).
    pub fn tune_receive_socket_at(&self, local: SocketAddr, purpose: &str) {
        if self.busy_poll_us == 0 {
            self.record_busy_poll(purpose, BusyPollOutcome::Disabled);
            return;
        }
        let outcome = match find_udp_socket(local) {
            Some(fd) => {
                // SAFETY: the descriptor stays open for the call. It belongs
                // to a socket that lives as long as the node.
                let borrowed = unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) };
                apply_busy_poll(&borrowed, self.busy_poll_us)
            }
            None => BusyPollOutcome::Failed(format!(
                "no UDP socket of this process is bound to {local}"
            )),
        };
        self.record_busy_poll(purpose, outcome);
    }

    fn record_busy_poll(&self, purpose: &str, outcome: BusyPollOutcome) {
        let mut snapshot = self.lock();
        match outcome {
            BusyPollOutcome::Disabled => {}
            BusyPollOutcome::Applied => {
                tracing::info!(
                    purpose,
                    busy_poll_us = self.busy_poll_us,
                    "SO_BUSY_POLL applied"
                );
                snapshot.busy_poll_applied.push(purpose.to_owned());
            }
            BusyPollOutcome::Denied => {
                tracing::warn!(
                    purpose,
                    busy_poll_us = self.busy_poll_us,
                    "SO_BUSY_POLL needs CAP_NET_ADMIN above net.core.busy_read; continuing without it"
                );
                snapshot.busy_poll_denied.push(purpose.to_owned());
            }
            BusyPollOutcome::Failed(reason) => {
                tracing::warn!(
                    purpose,
                    reason,
                    "SO_BUSY_POLL not applied; continuing without it"
                );
                snapshot
                    .busy_poll_failed
                    .push(format!("{purpose}: {reason}"));
            }
        }
    }

    /// Record the source sockets of the relay forwarder: how many there are
    /// and how many serve more than one child, which is where a batched
    /// forward takes effect.
    pub fn record_relay_forward_sockets(&self, sockets: usize, shared_sockets: usize) {
        let mut snapshot = self.lock();
        snapshot.relay_forward_sockets = sockets;
        snapshot.relay_forward_shared_sockets = shared_sockets;
        snapshot.relay_forward_batch_effective = snapshot.relay_forward_batch && shared_sockets > 0;
    }

    pub fn record_media_cpu(&self, cpu: usize, pinned: Result<(), String>) {
        let mut snapshot = self.lock();
        snapshot.media_cpu = Some(cpu);
        match pinned {
            Ok(()) => {
                snapshot.media_cpu_pinned = true;
                snapshot.media_cpu_error = None;
            }
            Err(error) => {
                snapshot.media_cpu_pinned = false;
                snapshot.media_cpu_error = Some(error);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusyPollOutcome {
    /// The budget is zero; nothing was asked of the kernel.
    Disabled,
    Applied,
    /// EPERM: the process may not raise the budget.
    Denied,
    Failed(String),
}

/// Set SO_BUSY_POLL on a socket. A budget of zero leaves the socket alone.
pub fn apply_busy_poll(socket: &impl std::os::fd::AsFd, busy_poll_us: u32) -> BusyPollOutcome {
    if busy_poll_us == 0 {
        return BusyPollOutcome::Disabled;
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let value = libc::c_int::try_from(busy_poll_us).unwrap_or(libc::c_int::MAX);
        // SAFETY: a plain setsockopt with a c_int value on a live descriptor.
        let result = unsafe {
            libc::setsockopt(
                socket.as_fd().as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_BUSY_POLL,
                std::ptr::from_ref(&value).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if result == 0 {
            return BusyPollOutcome::Applied;
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EPERM) {
            BusyPollOutcome::Denied
        } else {
            BusyPollOutcome::Failed(error.to_string())
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = socket;
        BusyPollOutcome::Failed("SO_BUSY_POLL is Linux only".to_owned())
    }
}

/// Read back SO_BUSY_POLL.
#[cfg(all(test, target_os = "linux"))]
pub fn busy_poll_of(socket: &impl std::os::fd::AsFd) -> io::Result<u32> {
    use std::os::fd::AsRawFd;
    let mut value: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: getsockopt into a c_int of the stated length.
    let result = unsafe {
        libc::getsockopt(
            socket.as_fd().as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_BUSY_POLL,
            std::ptr::from_mut(&mut value).cast(),
            &mut len,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(u32::try_from(value).unwrap_or(0))
}

fn read_host_busy_poll_us() -> Option<u32> {
    std::fs::read_to_string("/proc/sys/net/core/busy_poll")
        .ok()
        .and_then(|value| value.trim().parse().ok())
}

/// Raise a socket buffer past `net.core.rmem_max` / `wmem_max` with the
/// FORCE options when the plain request was capped. Needs CAP_NET_ADMIN; a
/// refusal leaves the buffer as it was. Returns whether both buffers now
/// meet the request.
pub fn force_socket_buffers(socket: &impl std::os::fd::AsFd, requested_bytes: usize) -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let Ok(value) = libc::c_int::try_from(requested_bytes) else {
            return false;
        };
        let fd = socket.as_fd().as_raw_fd();
        let mut all = true;
        for option in [libc::SO_RCVBUFFORCE, libc::SO_SNDBUFFORCE] {
            // SAFETY: a plain setsockopt with a c_int value on a live descriptor.
            let result = unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    option,
                    std::ptr::from_ref(&value).cast(),
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                )
            };
            all &= result == 0;
        }
        all
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (socket, requested_bytes);
        false
    }
}

/// Find the descriptor of the UDP socket of this process bound to `local`.
/// The cache mesh binds its own socket inside the playlists crate; this is
/// how the node reaches it to set an option without changing that crate.
pub fn find_udp_socket(local: SocketAddr) -> Option<std::os::fd::RawFd> {
    #[cfg(target_os = "linux")]
    {
        let entries = std::fs::read_dir("/proc/self/fd").ok()?;
        for entry in entries.flatten() {
            let Some(fd) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<std::os::fd::RawFd>().ok())
            else {
                continue;
            };
            let Ok(target) = std::fs::read_link(entry.path()) else {
                continue;
            };
            if !target.to_string_lossy().starts_with("socket:") {
                continue;
            }
            // SAFETY: the descriptor is open (it was listed a moment ago) and
            // only borrowed for two queries.
            let borrowed = unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) };
            let socket = socket2::SockRef::from(&borrowed);
            if socket.r#type().ok() != Some(socket2::Type::DGRAM) {
                continue;
            }
            if socket
                .local_addr()
                .ok()
                .and_then(|address| address.as_socket())
                == Some(local)
            {
                return Some(fd);
            }
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = local;
        None
    }
}

/// Hand several datagrams to the kernel in one call on a socket the runtime
/// owns, without waiting. Returns how many left, from the front of the list;
/// `WouldBlock` when the socket was full before the first one.
///
/// On Linux this is one `sendmmsg` for up to [`MAX_BATCH_MESSAGES`] messages
/// per call; elsewhere it is a loop of non-blocking sends.
pub fn try_send_batch<'a>(
    socket: &UdpSocket,
    count: usize,
    message: impl Fn(usize) -> (&'a [u8], SocketAddr),
) -> io::Result<usize> {
    let mut sent = 0;
    while sent < count {
        let chunk = (count - sent).min(MAX_BATCH_MESSAGES);
        let result = socket.try_io(tokio::io::Interest::WRITABLE, || {
            send_chunk(socket, chunk, |index| message(sent + index))
        });
        match result {
            Ok(0) => break,
            Ok(n) => {
                sent += n;
                if n < chunk {
                    // The kernel stopped early: the socket is full or the
                    // next message failed. The caller decides what to do with
                    // the rest.
                    break;
                }
            }
            Err(error) if sent == 0 => return Err(error),
            Err(_) => break,
        }
    }
    Ok(sent)
}

#[cfg(target_os = "linux")]
fn send_chunk<'a>(
    socket: &UdpSocket,
    count: usize,
    message: impl Fn(usize) -> (&'a [u8], SocketAddr),
) -> io::Result<usize> {
    use std::os::fd::AsRawFd;
    debug_assert!(count <= MAX_BATCH_MESSAGES);
    // SAFETY: zeroed sockaddr_storage, iovec and mmsghdr are valid values.
    let mut addresses: [libc::sockaddr_storage; MAX_BATCH_MESSAGES] = unsafe { std::mem::zeroed() };
    let mut iovecs: [libc::iovec; MAX_BATCH_MESSAGES] = unsafe { std::mem::zeroed() };
    let mut headers: [libc::mmsghdr; MAX_BATCH_MESSAGES] = unsafe { std::mem::zeroed() };
    for index in 0..count {
        let (datagram, target) = message(index);
        let address = socket2::SockAddr::from(target);
        // SAFETY: SockAddr holds at most a sockaddr_storage; `len` bytes are
        // copied into a sockaddr_storage of its own.
        unsafe {
            std::ptr::copy_nonoverlapping(
                address.as_ptr().cast::<u8>(),
                std::ptr::from_mut(&mut addresses[index]).cast::<u8>(),
                address.len() as usize,
            );
        }
        iovecs[index] = libc::iovec {
            iov_base: datagram.as_ptr().cast_mut().cast(),
            iov_len: datagram.len(),
        };
        headers[index].msg_hdr.msg_name = std::ptr::from_mut(&mut addresses[index]).cast();
        headers[index].msg_hdr.msg_namelen = address.len();
        headers[index].msg_hdr.msg_iov = std::ptr::from_mut(&mut iovecs[index]);
        headers[index].msg_hdr.msg_iovlen = 1;
    }
    // SAFETY: every header points at an address and one iovec that live
    // until the call returns; the datagrams outlive the call.
    let result = unsafe {
        libc::sendmmsg(
            socket.as_raw_fd(),
            headers.as_mut_ptr(),
            count as libc::c_uint,
            libc::MSG_DONTWAIT,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    for (index, header) in headers.iter().enumerate().take(result as usize) {
        // A datagram socket sends a whole datagram or fails it.
        let (datagram, _) = message(index);
        if header.msg_len as usize != datagram.len() {
            return Ok(index);
        }
    }
    Ok(result as usize)
}

#[cfg(not(target_os = "linux"))]
fn send_chunk<'a>(
    socket: &UdpSocket,
    count: usize,
    message: impl Fn(usize) -> (&'a [u8], SocketAddr),
) -> io::Result<usize> {
    let mut sent = 0;
    for index in 0..count {
        let (datagram, target) = message(index);
        match socket.try_send_to(datagram, target) {
            Ok(_) => sent += 1,
            Err(error) if sent == 0 => return Err(error),
            Err(_) => break,
        }
    }
    Ok(sent)
}

/// Pin the calling thread to one CPU.
pub fn pin_current_thread(cpu: usize) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        if cpu >= libc::CPU_SETSIZE as usize {
            return Err(format!("CPU {cpu} is beyond CPU_SETSIZE"));
        }
        // SAFETY: a zeroed cpu_set_t is empty; CPU_SET writes inside it.
        let result = unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(cpu, &mut set);
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set)
        };
        if result == 0 {
            Ok(())
        } else {
            Err(format!(
                "sched_setaffinity to CPU {cpu}: {}",
                io::Error::last_os_error()
            ))
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(format!("pinning to CPU {cpu} is Linux only"))
    }
}

/// A runtime of one thread for the media: the relay ingest and the cache
/// mesh run here, pinned to one CPU when the host allows it, apart from the
/// readers and the rest of the node.
pub struct MediaRuntime {
    handle: tokio::runtime::Handle,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl MediaRuntime {
    /// Start the media thread and pin it to `cpu`. The thread runs even when
    /// the pinning fails; the second value says why it did.
    pub fn start(cpu: usize) -> io::Result<(Self, Result<(), String>)> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .thread_name("needletail-media-blocking")
            .build()?;
        let handle = runtime.handle().clone();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let (pinned_tx, pinned_rx) = std::sync::mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("needletail-media".to_owned())
            .spawn(move || {
                let _ = pinned_tx.send(pin_current_thread(cpu));
                runtime.block_on(async {
                    let _ = stop_rx.await;
                });
                runtime.shutdown_background();
            })?;
        let pinned = pinned_rx
            .recv()
            .unwrap_or_else(|_| Err("the media thread ended before it started".to_owned()));
        Ok((
            Self {
                handle,
                stop: Some(stop_tx),
                thread: Some(thread),
            },
            pinned,
        ))
    }

    pub fn handle(&self) -> &tokio::runtime::Handle {
        &self.handle
    }
}

impl Drop for MediaRuntime {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Run a future on the media runtime when there is one, else where the
/// caller is. Sockets bound inside it are then driven by the media thread.
pub async fn on_media<F>(
    media: Option<&tokio::runtime::Handle>,
    future: F,
) -> anyhow::Result<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match media {
        Some(handle) => handle
            .spawn(future)
            .await
            .map_err(|error| anyhow::anyhow!("the media runtime task ended: {error}")),
        None => Ok(future.await),
    }
}

/// Spawn a task on the media runtime when there is one, else on the
/// caller's runtime.
pub fn spawn_media<F>(
    media: Option<&tokio::runtime::Handle>,
    future: F,
) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match media {
        Some(handle) => handle.spawn(future),
        None => tokio::spawn(future),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn one_batch_reaches_every_target_in_order() {
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut receivers = Vec::new();
        for _ in 0..3 {
            receivers.push(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        }
        let targets: Vec<SocketAddr> = receivers.iter().map(|r| r.local_addr().unwrap()).collect();
        // Wait until the runtime knows the socket is writable.
        sender.writable().await.unwrap();
        let sent = try_send_batch(&sender, targets.len(), |index| {
            (b"same".as_slice(), targets[index])
        })
        .unwrap();
        assert_eq!(sent, 3);
        for receiver in &receivers {
            let mut buf = [0_u8; 16];
            let (len, from) =
                tokio::time::timeout(Duration::from_secs(1), receiver.recv_from(&mut buf))
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(&buf[..len], b"same");
            assert_eq!(from, sender.local_addr().unwrap());
        }

        // Many datagrams to one target, more than one call holds.
        let datagrams: Vec<Vec<u8>> = (0..(MAX_BATCH_MESSAGES * 2 + 5))
            .map(|index| format!("datagram-{index:03}").into_bytes())
            .collect();
        let target = targets[0];
        sender.writable().await.unwrap();
        let sent = try_send_batch(&sender, datagrams.len(), |index| {
            (datagrams[index].as_slice(), target)
        })
        .unwrap();
        assert_eq!(sent, datagrams.len());
        for expected in &datagrams {
            let mut buf = [0_u8; 32];
            let (len, _) =
                tokio::time::timeout(Duration::from_secs(1), receivers[0].recv_from(&mut buf))
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(&buf[..len], expected.as_slice());
        }
    }

    #[test]
    fn busy_poll_is_applied_or_refused_and_zero_leaves_the_socket_alone() {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        assert_eq!(apply_busy_poll(&socket, 0), BusyPollOutcome::Disabled);
        match apply_busy_poll(&socket, 50) {
            BusyPollOutcome::Applied => assert_eq!(busy_poll_of(&socket).unwrap(), 50),
            // Unprivileged: the kernel refuses and the socket is unchanged.
            BusyPollOutcome::Denied => assert_eq!(busy_poll_of(&socket).unwrap(), 0),
            other => panic!("unexpected busy-poll outcome {other:?}"),
        }

        let fast_path = FastPath::new(50, true);
        fast_path.tune_receive_socket(&socket, "test socket");
        let snapshot = fast_path.snapshot();
        assert!(snapshot.relay_forward_batch);
        // On, but no shared source socket: inert.
        assert!(!snapshot.relay_forward_batch_effective);
        fast_path.record_relay_forward_sockets(3, 0);
        assert!(!fast_path.snapshot().relay_forward_batch_effective);
        fast_path.record_relay_forward_sockets(2, 1);
        assert!(fast_path.snapshot().relay_forward_batch_effective);
        // Off: never effective.
        let unbatched = FastPath::new(50, false);
        unbatched.record_relay_forward_sockets(2, 1);
        assert!(!unbatched.snapshot().relay_forward_batch_effective);
        let snapshot = fast_path.snapshot();
        assert_eq!(snapshot.busy_poll_us, 50);
        assert_eq!(
            snapshot.busy_poll_applied.len() + snapshot.busy_poll_denied.len(),
            1,
            "{snapshot:?}"
        );

        let off = FastPath::new(0, false);
        off.tune_receive_socket(&socket, "test socket");
        off.tune_receive_socket_at(socket.local_addr().unwrap(), "test socket");
        let snapshot = off.snapshot();
        assert!(snapshot.busy_poll_applied.is_empty());
        assert!(snapshot.busy_poll_denied.is_empty());
        assert!(snapshot.busy_poll_failed.is_empty());
    }

    #[test]
    fn a_socket_owned_elsewhere_is_found_by_its_address() {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let local = socket.local_addr().unwrap();
        let fd = find_udp_socket(local).expect("the socket is found");
        assert_eq!(fd, std::os::fd::AsRawFd::as_raw_fd(&socket));
        // An address nothing is bound to any more is not found.
        let unused = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let unused_addr = unused.local_addr().unwrap();
        drop(unused);
        assert_eq!(find_udp_socket(unused_addr), None);

        let fast_path = FastPath::new(50, true);
        fast_path.tune_receive_socket_at(local, "cache mesh");
        fast_path.tune_receive_socket_at(unused_addr, "missing");
        let snapshot = fast_path.snapshot();
        assert_eq!(
            snapshot.busy_poll_applied.len() + snapshot.busy_poll_denied.len(),
            1,
            "{snapshot:?}"
        );
        assert_eq!(snapshot.busy_poll_failed.len(), 1);
        assert!(snapshot.busy_poll_failed[0].starts_with("missing:"));
    }

    #[tokio::test]
    async fn the_media_runtime_runs_its_tasks_on_the_pinned_cpu() {
        // The first CPU this process may use (a container may not have 0).
        // SAFETY: sched_getaffinity fills a cpu_set_t of the stated size.
        let allowed = unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            assert_eq!(
                libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set),
                0
            );
            (0..libc::CPU_SETSIZE as usize)
                .find(|cpu| libc::CPU_ISSET(*cpu, &set))
                .unwrap()
        };
        let (media, pinned) = MediaRuntime::start(allowed).unwrap();
        pinned.unwrap();
        let cpu = on_media(Some(media.handle()), async {
            // SAFETY: sched_getcpu has no preconditions.
            unsafe { libc::sched_getcpu() }
        })
        .await
        .unwrap();
        assert_eq!(cpu, allowed as i32);
        let name = spawn_media(Some(media.handle()), async {
            std::thread::current().name().map(str::to_owned)
        })
        .await
        .unwrap();
        assert_eq!(name.as_deref(), Some("needletail-media"));
        // A socket bound on the media runtime works from it.
        let echoed = on_media(Some(media.handle()), async {
            let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            a.send_to(b"media", b.local_addr().unwrap()).await.unwrap();
            let mut buf = [0_u8; 8];
            let (len, _) = b.recv_from(&mut buf).await.unwrap();
            buf[..len].to_vec()
        })
        .await
        .unwrap();
        assert_eq!(echoed, b"media");
        drop(media);

        // Without a media runtime the work runs where the caller is.
        assert_eq!(on_media(None, async { 7 }).await.unwrap(), 7);

        // An impossible CPU is reported and the thread still runs.
        let (media, pinned) = MediaRuntime::start(libc::CPU_SETSIZE as usize + 1).unwrap();
        assert!(pinned.is_err());
        assert_eq!(
            on_media(Some(media.handle()), async { 1 }).await.unwrap(),
            1
        );

        let fast_path = FastPath::new(0, true);
        fast_path.record_media_cpu(3, Err("no".to_owned()));
        let snapshot = fast_path.snapshot();
        assert_eq!(snapshot.media_cpu, Some(3));
        assert!(!snapshot.media_cpu_pinned);
    }
}
