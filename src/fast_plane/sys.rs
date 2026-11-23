//! System calls of the fast plane: batched receive with kernel timestamps
//! and drop counts, batched non-blocking send on raw descriptors, socket
//! pairs on one address, and thread scheduling.
//!
//! The class threads are plain OS threads that own their sockets, so these
//! work on raw descriptors and never touch the tokio reactor.

use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};

/// Messages one receive call takes.
pub const RECV_BATCH: usize = 64;
/// Bytes per receive slot: a RelaySession datagram is at most 1,472 bytes;
/// a legacy or AEP1 datagram larger than a slot is counted as truncated.
pub const RECV_SLOT_BYTES: usize = 16 * 1024;
/// Messages one send call takes.
pub const SEND_BATCH: usize = 32;
/// SO_PRIORITY of audio sockets: band 0 of the default prio qdisc priomap
/// (TC_PRIO_INTERACTIVE), so a 4K burst never sits ahead of audio in the
/// host's queue.
pub const AUDIO_SO_PRIORITY: i32 = 6;

#[cfg(target_os = "linux")]
fn setsockopt_int(
    fd: RawFd,
    level: libc::c_int,
    option: libc::c_int,
    value: libc::c_int,
) -> io::Result<()> {
    // SAFETY: a plain setsockopt with a c_int value on a live descriptor.
    let result = unsafe {
        libc::setsockopt(
            fd,
            level,
            option,
            std::ptr::from_ref(&value).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Bind a UDP socket with SO_REUSEPORT, so that a second socket of another
/// class can share its address (and the children's peer binding of it).
/// Non-blocking.
pub fn bind_reuseport(bind: SocketAddr) -> io::Result<std::net::UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(bind),
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    #[cfg(target_os = "linux")]
    setsockopt_int(socket.as_raw_fd(), libc::SOL_SOCKET, libc::SO_REUSEPORT, 1)?;
    socket.set_nonblocking(true)?;
    socket.bind(&bind.into())?;
    Ok(socket.into())
}

/// Bind a plain non-blocking UDP receive socket.
pub fn bind_plain(bind: SocketAddr) -> io::Result<std::net::UdpSocket> {
    let socket = std::net::UdpSocket::bind(bind)?;
    socket.set_nonblocking(true)?;
    Ok(socket)
}

/// Set SO_PRIORITY. 0..=6 needs no privilege.
pub fn set_priority(socket: &impl AsRawFd, priority: i32) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        setsockopt_int(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PRIORITY,
            priority,
        )
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (socket, priority);
        Ok(())
    }
}

/// Set SO_BUSY_POLL: a receive on the socket, and a poll() of it when the
/// host's net.core.busy_poll is above zero, polls the device queue for up to
/// `micros` before it sleeps. A value above net.core.busy_read needs
/// CAP_NET_ADMIN (EPERM without it).
pub fn set_busy_poll(socket: &impl AsRawFd, micros: u32) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        setsockopt_int(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_BUSY_POLL,
            libc::c_int::try_from(micros).unwrap_or(libc::c_int::MAX),
        )
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (socket, micros);
        Ok(())
    }
}

/// Ask for a kernel receive timestamp (SO_TIMESTAMPNS) and the socket's
/// drop counter (SO_RXQ_OVFL) on every received message.
pub fn enable_receive_metadata(socket: &impl AsRawFd) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        let fd = socket.as_raw_fd();
        setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_TIMESTAMPNS, 1)?;
        setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_RXQ_OVFL, 1)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = socket;
        Ok(())
    }
}

/// One received message of a [`RecvBatch`].
#[derive(Debug, Clone, Copy)]
pub struct Received<'a> {
    pub data: &'a [u8],
    pub peer: Option<SocketAddr>,
    /// Kernel receive time, CLOCK_REALTIME µs.
    pub kernel_us: Option<u64>,
    /// The socket's cumulative drop count when the message was queued.
    pub dropped: Option<u32>,
    /// The datagram was larger than a slot.
    pub truncated: bool,
}

/// Preallocated buffers for `recvmmsg`.
pub struct RecvBatch {
    data: Vec<u8>,
    #[cfg(target_os = "linux")]
    addresses: Vec<libc::sockaddr_storage>,
    #[cfg(target_os = "linux")]
    iovecs: Vec<libc::iovec>,
    #[cfg(target_os = "linux")]
    headers: Vec<libc::mmsghdr>,
    control: Vec<[u64; 16]>,
    count: usize,
}

// SAFETY: the raw pointers in the headers point only into the batch's own
// heap buffers, which move with it.
unsafe impl Send for RecvBatch {}

impl Default for RecvBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl RecvBatch {
    pub fn new() -> Self {
        Self {
            data: vec![0_u8; RECV_BATCH * RECV_SLOT_BYTES],
            #[cfg(target_os = "linux")]
            // SAFETY: all-zero sockaddr_storage, iovec and mmsghdr are valid.
            addresses: vec![unsafe { std::mem::zeroed() }; RECV_BATCH],
            #[cfg(target_os = "linux")]
            iovecs: vec![unsafe { std::mem::zeroed() }; RECV_BATCH],
            #[cfg(target_os = "linux")]
            headers: vec![unsafe { std::mem::zeroed() }; RECV_BATCH],
            control: vec![[0_u64; 16]; RECV_BATCH],
            count: 0,
        }
    }

    /// Take what the socket has queued, up to [`RECV_BATCH`] messages,
    /// without waiting. `Ok(0)` when nothing was queued.
    pub fn recv(&mut self, socket: &impl AsRawFd) -> io::Result<usize> {
        self.count = 0;
        #[cfg(target_os = "linux")]
        {
            for index in 0..RECV_BATCH {
                self.iovecs[index] = libc::iovec {
                    iov_base: self.data[index * RECV_SLOT_BYTES..].as_mut_ptr().cast(),
                    iov_len: RECV_SLOT_BYTES,
                };
                let header = &mut self.headers[index].msg_hdr;
                header.msg_name = std::ptr::from_mut(&mut self.addresses[index]).cast();
                header.msg_namelen =
                    std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
                header.msg_iov = std::ptr::from_mut(&mut self.iovecs[index]);
                header.msg_iovlen = 1;
                header.msg_control = self.control[index].as_mut_ptr().cast();
                header.msg_controllen = std::mem::size_of::<[u64; 16]>() as _;
                header.msg_flags = 0;
                self.headers[index].msg_len = 0;
            }
            // SAFETY: every header points at buffers of this batch that live
            // for the call.
            let result = unsafe {
                libc::recvmmsg(
                    socket.as_raw_fd(),
                    self.headers.as_mut_ptr(),
                    RECV_BATCH as libc::c_uint,
                    libc::MSG_DONTWAIT,
                    std::ptr::null_mut(),
                )
            };
            if result < 0 {
                let error = io::Error::last_os_error();
                return if error.kind() == io::ErrorKind::WouldBlock {
                    Ok(0)
                } else {
                    Err(error)
                };
            }
            self.count = result as usize;
            Ok(self.count)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = socket;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "recvmmsg is Linux only",
            ))
        }
    }

    pub const fn len(&self) -> usize {
        self.count
    }

    /// The bytes of message `index` of the last receive, without its
    /// metadata.
    pub fn data(&self, index: usize) -> &[u8] {
        #[cfg(target_os = "linux")]
        {
            let len = (self.headers[index].msg_len as usize).min(RECV_SLOT_BYTES);
            &self.data[index * RECV_SLOT_BYTES..index * RECV_SLOT_BYTES + len]
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = index;
            &self.data[..0]
        }
    }

    /// The message `index` of the last receive.
    pub fn get(&self, index: usize) -> Received<'_> {
        #[cfg(target_os = "linux")]
        {
            debug_assert!(index < self.count);
            let header = &self.headers[index];
            let len = (header.msg_len as usize).min(RECV_SLOT_BYTES);
            let data = &self.data[index * RECV_SLOT_BYTES..index * RECV_SLOT_BYTES + len];
            let peer = sockaddr_to_std(&self.addresses[index], header.msg_hdr.msg_namelen);
            let mut kernel_us = None;
            let mut dropped = None;
            // SAFETY: the kernel filled msg_control up to msg_controllen with
            // well-formed cmsg headers; CMSG_* only walk inside it.
            unsafe {
                let msg = &header.msg_hdr;
                let mut cmsg = libc::CMSG_FIRSTHDR(msg);
                while !cmsg.is_null() {
                    let level = (*cmsg).cmsg_level;
                    let kind = (*cmsg).cmsg_type;
                    if level == libc::SOL_SOCKET && kind == libc::SCM_TIMESTAMPNS {
                        let time = std::ptr::read_unaligned(
                            libc::CMSG_DATA(cmsg).cast::<libc::timespec>(),
                        );
                        kernel_us = Some(
                            (time.tv_sec as u64)
                                .saturating_mul(1_000_000)
                                .saturating_add(time.tv_nsec as u64 / 1_000),
                        );
                    } else if level == libc::SOL_SOCKET && kind == libc::SO_RXQ_OVFL {
                        dropped = Some(std::ptr::read_unaligned(
                            libc::CMSG_DATA(cmsg).cast::<u32>(),
                        ));
                    }
                    cmsg = libc::CMSG_NXTHDR(msg, cmsg);
                }
            }
            Received {
                data,
                peer,
                kernel_us,
                dropped,
                truncated: header.msg_hdr.msg_flags & libc::MSG_TRUNC != 0,
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = index;
            Received {
                data: &self.data[..0],
                peer: None,
                kernel_us: None,
                dropped: None,
                truncated: false,
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn sockaddr_to_std(storage: &libc::sockaddr_storage, len: libc::socklen_t) -> Option<SocketAddr> {
    // SAFETY: the kernel wrote an address of `len` bytes; SockAddr copies it.
    let address = unsafe {
        socket2::SockAddr::try_init(|target, target_len| {
            std::ptr::copy_nonoverlapping(
                std::ptr::from_ref(storage).cast::<u8>(),
                target.cast::<u8>(),
                len as usize,
            );
            *target_len = len;
            Ok(())
        })
    }
    .ok()?
    .1;
    address.as_socket()
}

/// What became of one message of the rest of a batch (`send_rest`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestOutcome {
    Sent,
    /// The socket was full: refused, never waited for.
    WouldBlock,
    /// Another error, for this message.
    Error,
}

/// The messages `from..count` of a batch that `send_batch` did not take,
/// each tried once on its own with `try_one`, until the socket refuses one
/// as full: the socket's send buffer is shared, so every later message
/// would be refused too, and the rest are refused without a syscall each.
/// An error of one message (its destination) does not stop the others.
/// `outcome` is told of each message.
pub fn send_rest(
    from: usize,
    count: usize,
    mut try_one: impl FnMut(usize) -> io::Result<usize>,
    mut outcome: impl FnMut(usize, RestOutcome),
) {
    let mut full = false;
    for index in from..count {
        if full {
            outcome(index, RestOutcome::WouldBlock);
            continue;
        }
        match try_one(index) {
            Ok(1) => outcome(index, RestOutcome::Sent),
            Err(error) if error.kind() != io::ErrorKind::WouldBlock => {
                outcome(index, RestOutcome::Error);
            }
            _ => {
                full = true;
                outcome(index, RestOutcome::WouldBlock);
            }
        }
    }
}

/// Hand messages to the kernel in `sendmmsg` calls of up to [`SEND_BATCH`],
/// without waiting. Returns how many left, from the front.
pub fn send_batch<'a>(
    socket: RawFd,
    count: usize,
    message: impl Fn(usize) -> (&'a [u8], SocketAddr),
) -> io::Result<usize> {
    let mut sent = 0;
    while sent < count {
        let chunk = (count - sent).min(SEND_BATCH);
        match send_chunk(socket, chunk, |index| message(sent + index)) {
            Ok(0) => break,
            Ok(n) => {
                sent += n;
                if n < chunk {
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
    socket: RawFd,
    count: usize,
    message: impl Fn(usize) -> (&'a [u8], SocketAddr),
) -> io::Result<usize> {
    debug_assert!(count <= SEND_BATCH);
    // SAFETY: zeroed sockaddr_storage, iovec and mmsghdr are valid values.
    let mut addresses: [libc::sockaddr_storage; SEND_BATCH] = unsafe { std::mem::zeroed() };
    let mut iovecs: [libc::iovec; SEND_BATCH] = unsafe { std::mem::zeroed() };
    let mut headers: [libc::mmsghdr; SEND_BATCH] = unsafe { std::mem::zeroed() };
    for index in 0..count {
        let (datagram, target) = message(index);
        let address = socket2::SockAddr::from(target);
        // SAFETY: SockAddr holds at most a sockaddr_storage.
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
            socket,
            headers.as_mut_ptr(),
            count as libc::c_uint,
            libc::MSG_DONTWAIT,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(result as usize)
}

#[cfg(not(target_os = "linux"))]
fn send_chunk<'a>(
    socket: RawFd,
    count: usize,
    message: impl Fn(usize) -> (&'a [u8], SocketAddr),
) -> io::Result<usize> {
    let _ = (socket, count, message);
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "sendmmsg is Linux only",
    ))
}

/// Datagrams of one UDP GSO message at most (the kernel's UDP_MAX_SEGMENTS
/// is 64 on older hosts).
pub const GSO_MAX_SEGMENTS: usize = 64;
/// Payload bytes of one UDP GSO message at most: an IPv4 datagram's 65,507
/// less a margin, so that the kernel never refuses one for its length.
pub const GSO_MAX_BYTES: usize = 63 * 1024;
/// Datagrams handed to the kernel in one GSO `sendmmsg` call at most.
const GSO_CALL_DATAGRAMS: usize = 256;

/// What [`send_gso_batch`] handed to the kernel.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GsoSent {
    /// Datagrams, in order from the first.
    pub datagrams: usize,
    /// `sendmmsg` calls made.
    pub calls: usize,
    /// GSO messages (runs of datagrams the kernel cuts into packets).
    pub messages: usize,
}

/// The runs of a batch that one UDP GSO message each can carry: consecutive
/// datagrams to one target, every one but the last exactly as long as the
/// first, at most [`GSO_MAX_SEGMENTS`] datagrams and [`GSO_MAX_BYTES`]
/// bytes. Returns `(first, count)` of each run, in order, covering
/// `0..count` exactly.
pub fn gso_runs(
    count: usize,
    datagram: impl Fn(usize) -> (usize, SocketAddr),
) -> Vec<(usize, usize)> {
    let mut runs = Vec::new();
    gso_runs_into(&mut runs, count, datagram, |_| GSO_MAX_SEGMENTS);
    runs
}

/// [`gso_runs`] into `runs` (cleared first), with a run that starts at
/// datagram `first` at most `max_segments(first)` datagrams (and never more
/// than [`GSO_MAX_SEGMENTS`]): a paced sender cuts a client's datagrams
/// into runs that each leave at their own time.
pub fn gso_runs_into(
    runs: &mut Vec<(usize, usize)>,
    count: usize,
    datagram: impl Fn(usize) -> (usize, SocketAddr),
    max_segments: impl Fn(usize) -> usize,
) {
    runs.clear();
    let mut index = 0;
    while index < count {
        let (size, target) = datagram(index);
        let mut length = 1;
        let mut bytes = size;
        let limit = max_segments(index).clamp(1, GSO_MAX_SEGMENTS);
        // A run continues while the next datagram goes to the same target
        // and has the run's size; a shorter one may end it.
        if size > 0 {
            while index + length < count && length < limit {
                let (next_size, next_target) = datagram(index + length);
                if next_target != target
                    || next_size == 0
                    || next_size > size
                    || bytes + next_size > GSO_MAX_BYTES
                {
                    break;
                }
                length += 1;
                bytes += next_size;
                if next_size < size {
                    break;
                }
            }
        }
        runs.push((index, length));
        index += length;
    }
}

/// Whether a GSO send failed because the host cannot do UDP GSO on this
/// socket (no UDP_SEGMENT, no checksum offload on the device): the caller
/// sends without it from then on.
pub fn gso_unsupported(error: &io::Error) -> bool {
    #[cfg(target_os = "linux")]
    {
        matches!(
            error.raw_os_error(),
            Some(libc::EIO | libc::EINVAL | libc::EOPNOTSUPP | libc::ENOPROTOOPT)
        )
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = error;
        true
    }
}

/// [`send_batch`] with UDP GSO: each run of [`gso_runs`] is one message
/// whose datagrams the kernel cuts apart (`UDP_SEGMENT`), so a 4K part to
/// one child costs one pass through the stack instead of one per symbol.
/// The receiver sees exactly the datagrams of a loop of single sends, in
/// order. Non-blocking: returns what the kernel took, a whole number of
/// runs from the first; an error only when it took none of the first call.
pub fn send_gso_batch<'a>(
    socket: RawFd,
    count: usize,
    message: impl Fn(usize) -> (&'a [u8], SocketAddr),
) -> io::Result<GsoSent> {
    let runs = gso_runs(count, |index| {
        let (data, target) = message(index);
        (data.len(), target)
    });
    send_gso_runs(socket, &runs, message, None)
}

/// Send `runs` (from [`gso_runs_into`]) of the messages, each run one UDP
/// GSO message, as [`send_gso_batch`] does. With `departures`, run `i`
/// carries an earliest departure time (`SCM_TXTIME`, CLOCK_MONOTONIC ns,
/// [`monotonic_ns`]) of `departures[i]`: on a socket with [`enable_txtime`],
/// an `fq` qdisc holds the run until then (other qdiscs send it at once).
pub fn send_gso_runs<'a>(
    socket: RawFd,
    runs: &[(usize, usize)],
    message: impl Fn(usize) -> (&'a [u8], SocketAddr),
    departures: Option<&[u64]>,
) -> io::Result<GsoSent> {
    debug_assert!(departures.is_none_or(|departures| departures.len() == runs.len()));
    let mut sent = GsoSent::default();
    let mut next_run = 0;
    while next_run < runs.len() {
        // The runs of one call: at most SEND_BATCH messages and
        // GSO_CALL_DATAGRAMS datagrams.
        let mut end = next_run;
        let mut datagrams = 0;
        while end < runs.len()
            && end - next_run < SEND_BATCH
            && (datagrams == 0 || datagrams + runs[end].1 <= GSO_CALL_DATAGRAMS)
        {
            datagrams += runs[end].1;
            end += 1;
        }
        let call = &runs[next_run..end];
        let call_departures = departures.map(|departures| &departures[next_run..end]);
        match send_gso_chunk(socket, call, &message, call_departures) {
            Ok(taken) => {
                sent.calls += 1;
                sent.messages += taken;
                sent.datagrams += call[..taken].iter().map(|run| run.1).sum::<usize>();
                next_run += taken;
                if taken < call.len() {
                    break;
                }
            }
            Err(error) if sent.calls == 0 => return Err(error),
            Err(_) => break,
        }
    }
    Ok(sent)
}

#[cfg(target_os = "linux")]
fn send_gso_chunk<'a>(
    socket: RawFd,
    runs: &[(usize, usize)],
    message: &impl Fn(usize) -> (&'a [u8], SocketAddr),
    departures: Option<&[u64]>,
) -> io::Result<usize> {
    debug_assert!(runs.len() <= SEND_BATCH);
    // A cmsghdr with a u16 payload (UDP_SEGMENT) and one with a u64
    // (SCM_TXTIME), aligned as the kernel reads them.
    const CONTROL: usize = 64;
    #[repr(C, align(8))]
    #[derive(Clone, Copy)]
    struct Control([u8; CONTROL]);
    // SAFETY: zeroed sockaddr_storage, iovec, mmsghdr and byte arrays are
    // valid values.
    let mut addresses: [libc::sockaddr_storage; SEND_BATCH] = unsafe { std::mem::zeroed() };
    let mut iovecs: [libc::iovec; GSO_CALL_DATAGRAMS] = unsafe { std::mem::zeroed() };
    let mut headers: [libc::mmsghdr; SEND_BATCH] = unsafe { std::mem::zeroed() };
    let mut controls = [Control([0; CONTROL]); SEND_BATCH];
    // SAFETY: CMSG_SPACE is a pure size computation.
    let segment_space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<u16>() as u32) } as usize;
    // SAFETY: as above.
    let txtime_space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<u64>() as u32) } as usize;
    debug_assert!(segment_space + txtime_space <= CONTROL);
    let mut iov = 0;
    for (slot, &(first, length)) in runs.iter().enumerate() {
        let (head, target) = message(first);
        let address = socket2::SockAddr::from(target);
        // SAFETY: SockAddr holds at most a sockaddr_storage.
        unsafe {
            std::ptr::copy_nonoverlapping(
                address.as_ptr().cast::<u8>(),
                std::ptr::from_mut(&mut addresses[slot]).cast::<u8>(),
                address.len() as usize,
            );
        }
        let iov_first = iov;
        for index in first..first + length {
            let (data, _) = message(index);
            iovecs[iov] = libc::iovec {
                iov_base: data.as_ptr().cast_mut().cast(),
                iov_len: data.len(),
            };
            iov += 1;
        }
        let header = &mut headers[slot].msg_hdr;
        header.msg_name = std::ptr::from_mut(&mut addresses[slot]).cast();
        header.msg_namelen = address.len();
        header.msg_iov = std::ptr::from_mut(&mut iovecs[iov_first]);
        header.msg_iovlen = length as _;
        let departure = departures.map(|departures| departures[slot]);
        let control = usize::from(length > 1) * segment_space
            + usize::from(departure.is_some()) * txtime_space;
        if control == 0 {
            continue;
        }
        header.msg_control = controls[slot].0.as_mut_ptr().cast();
        header.msg_controllen = control as _;
        // SAFETY: the control buffer is aligned and as long as the headers
        // written, each with its data, so CMSG_FIRSTHDR/CMSG_NXTHDR stay
        // inside it.
        unsafe {
            let mut cmsg = libc::CMSG_FIRSTHDR(header);
            if length > 1 {
                let segment = u16::try_from(head.len()).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "GSO segment over 64 KiB")
                })?;
                (*cmsg).cmsg_level = libc::SOL_UDP;
                (*cmsg).cmsg_type = libc::UDP_SEGMENT;
                (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<u16>() as u32) as _;
                std::ptr::write_unaligned(libc::CMSG_DATA(cmsg).cast::<u16>(), segment);
                cmsg = libc::CMSG_NXTHDR(header, cmsg);
            }
            if let Some(departure) = departure {
                (*cmsg).cmsg_level = libc::SOL_SOCKET;
                (*cmsg).cmsg_type = libc::SCM_TXTIME;
                (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<u64>() as u32) as _;
                std::ptr::write_unaligned(libc::CMSG_DATA(cmsg).cast::<u64>(), departure);
            }
        }
    }
    // SAFETY: every header points at an address, its iovecs and its control
    // buffer, all of which live until the call returns; the datagrams
    // outlive the call.
    let result = unsafe {
        libc::sendmmsg(
            socket,
            headers.as_mut_ptr(),
            runs.len() as libc::c_uint,
            libc::MSG_DONTWAIT,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(result as usize)
}

#[cfg(not(target_os = "linux"))]
fn send_gso_chunk<'a>(
    socket: RawFd,
    runs: &[(usize, usize)],
    message: &impl Fn(usize) -> (&'a [u8], SocketAddr),
    departures: Option<&[u64]>,
) -> io::Result<usize> {
    let _ = (socket, runs, message, departures);
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "UDP GSO is Linux only",
    ))
}

/// Let the socket's messages carry an earliest departure time
/// (`SO_TXTIME`, CLOCK_MONOTONIC, no deadline mode, no error reports). An
/// `fq` qdisc honours it; CLOCK_MONOTONIC needs no privilege.
pub fn enable_txtime(socket: RawFd) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        #[repr(C)]
        struct SockTxtime {
            clockid: libc::clockid_t,
            flags: u32,
        }
        let value = SockTxtime {
            clockid: libc::CLOCK_MONOTONIC,
            flags: 0,
        };
        // SAFETY: a setsockopt of a sock_txtime on a live descriptor.
        let result = unsafe {
            libc::setsockopt(
                socket,
                libc::SOL_SOCKET,
                libc::SO_TXTIME,
                std::ptr::from_ref(&value).cast(),
                std::mem::size_of::<SockTxtime>() as libc::socklen_t,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = socket;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "SO_TXTIME is Linux only",
        ))
    }
}

/// CLOCK_MONOTONIC now, in nanoseconds: the clock of `SCM_TXTIME`.
pub fn monotonic_ns() -> u64 {
    #[cfg(target_os = "linux")]
    {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: clock_gettime writes one timespec.
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) };
        (time.tv_sec as u64)
            .saturating_mul(1_000_000_000)
            .saturating_add(time.tv_nsec as u64)
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

/// Wait until one of the descriptors is readable, or the timeout. Returns
/// a bit set of the readable ones.
/// Wait up to `timeout_ms` for room in `fd`'s send buffer; `true` when
/// there is.
pub fn wait_writable(fd: RawFd, timeout_ms: i32) -> bool {
    #[cfg(target_os = "linux")]
    {
        let mut poll = libc::pollfd {
            fd,
            events: libc::POLLOUT,
            revents: 0,
        };
        // SAFETY: one initialised pollfd.
        let result = unsafe { libc::poll(&mut poll, 1, timeout_ms) };
        result > 0 && poll.revents & libc::POLLOUT != 0
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (fd, timeout_ms);
        false
    }
}

pub fn wait_readable(fds: &[RawFd], timeout_ms: i32) -> io::Result<u64> {
    #[cfg(target_os = "linux")]
    {
        let mut polls = [libc::pollfd {
            fd: -1,
            events: 0,
            revents: 0,
        }; 8];
        let count = fds.len().min(polls.len());
        for (poll, fd) in polls.iter_mut().zip(fds) {
            poll.fd = *fd;
            poll.events = libc::POLLIN;
        }
        // SAFETY: `count` initialised pollfd entries.
        let result = unsafe { libc::poll(polls.as_mut_ptr(), count as libc::nfds_t, timeout_ms) };
        if result < 0 {
            let error = io::Error::last_os_error();
            return if error.kind() == io::ErrorKind::Interrupted {
                Ok(0)
            } else {
                Err(error)
            };
        }
        let mut ready = 0_u64;
        for (index, poll) in polls.iter().take(count).enumerate() {
            if poll.revents != 0 {
                ready |= 1 << index;
            }
        }
        Ok(ready)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (fds, timeout_ms);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "poll is Linux only here",
        ))
    }
}

/// Run the calling thread under SCHED_FIFO at `priority`.
pub fn set_fifo(priority: i32) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        let param = libc::sched_param {
            sched_priority: priority,
        };
        // SAFETY: pid 0 is the calling thread; the parameter is valid.
        let result = unsafe { libc::sched_setscheduler(0, libc::SCHED_FIFO, &param) };
        if result == 0 {
            Ok(())
        } else {
            Err(format!(
                "SCHED_FIFO {priority}: {}",
                io::Error::last_os_error()
            ))
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(format!("SCHED_FIFO {priority} is Linux only"))
    }
}

/// `--relay-class-fallback-nice` (wave 8): when the audio class thread is
/// refused SCHED_FIFO (no CAP_SYS_NICE, as in the local lab), every other
/// thread of the process is made this much nicer, so that the ingest thread
/// still wins the CPU over the fan-out, the runtime and the repair, as it
/// does at real-time priority on a deployed node. 0: off.
static FALLBACK_NICE: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);

pub fn set_fallback_nice(nice: i32) {
    FALLBACK_NICE.store(nice.clamp(0, 19), std::sync::atomic::Ordering::Relaxed);
}

pub fn fallback_nice() -> i32 {
    FALLBACK_NICE.load(std::sync::atomic::Ordering::Relaxed)
}

/// The audio class thread was refused SCHED_FIFO: make every other thread
/// of the process [`fallback_nice`] nicer than the process (the class
/// threads, named `needletail-fast-*`, excepted). Threads started later
/// inherit the nice value of the thread that starts them. Returns the
/// threads niced.
pub fn nice_other_threads() -> usize {
    let step = fallback_nice();
    if step == 0 {
        return 0;
    }
    #[cfg(target_os = "linux")]
    {
        // SAFETY: gettid has no preconditions.
        let me = unsafe { libc::syscall(libc::SYS_gettid) } as i64;
        let nice = (process_nice() + step).min(19);
        let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
            return 0;
        };
        let mut niced = 0;
        for task in tasks.flatten() {
            let Some(tid) = task.file_name().to_str().and_then(|tid| tid.parse::<i64>().ok())
            else {
                continue;
            };
            if tid == me {
                continue;
            }
            let name = std::fs::read_to_string(task.path().join("comm")).unwrap_or_default();
            if name.starts_with("needletail-fast") {
                continue;
            }
            // SAFETY: setpriority on a thread of this process.
            if unsafe { libc::setpriority(libc::PRIO_PROCESS, tid as libc::id_t, nice) } == 0 {
                niced += 1;
            }
        }
        niced
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

/// Run the calling thread as an ordinary one: SCHED_OTHER on the CPUs of
/// the process's main thread, at the process's nice value. A thread
/// inherits the policy, the priority and the CPUs of the thread that
/// started it: a helper started by a SCHED_FIFO class thread pinned to one
/// CPU would otherwise share that CPU with it at its priority, and hold it.
///
/// The CPUs come first and the nice value last, and a nice value the
/// kernel refuses is not an error: a niced process without CAP_SYS_NICE
/// may not lower a thread's nice value, and a refusal there must not leave
/// the thread pinned to the class thread's CPU (wave 7 review). The nice
/// value asked for is the process's own, which raising never needs a
/// privilege for.
pub fn set_ordinary() -> Result<(), String> {
    set_ordinary_at(None)
}

/// [`set_ordinary`], at the nice value `nice` if given (wave 8: the fan-out
/// threads, above the runtime's threads): where the kernel refuses it (no
/// CAP_SYS_NICE for a value below the thread's), the thread keeps the nice
/// value it inherited from the class thread that started it, which is
/// still above the threads that `--relay-class-fallback-nice` made nicer.
pub fn set_ordinary_at(nice: Option<i32>) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: a zeroed cpu_set_t is valid; sched_getaffinity writes at
        // most its size; getpid is the main thread's id.
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            let size = std::mem::size_of::<libc::cpu_set_t>();
            if libc::sched_getaffinity(libc::getpid(), size, &mut set) != 0
                || libc::sched_setaffinity(0, size, &set) != 0
            {
                return Err(format!("CPUs of the process: {}", io::Error::last_os_error()));
            }
        }
        let param = libc::sched_param { sched_priority: 0 };
        // SAFETY: pid 0 is the calling thread; the parameter is valid.
        if unsafe { libc::sched_setscheduler(0, libc::SCHED_OTHER, &param) } != 0 {
            return Err(format!("SCHED_OTHER: {}", io::Error::last_os_error()));
        }
        let wanted = nice.unwrap_or_else(process_nice);
        if let Err(error) = set_nice(wanted) {
            tracing::debug!(error, "an ordinary thread keeps its nice value");
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = nice;
        Ok(())
    }
}

/// The nice value of the process's main thread (0 if it cannot be read).
#[cfg(target_os = "linux")]
fn process_nice() -> i32 {
    // SAFETY: errno is reset before the call, as getpriority's -1 is also
    // a valid nice value; getpid has no preconditions.
    unsafe {
        *libc::__errno_location() = 0;
        let nice = libc::getpriority(libc::PRIO_PROCESS, libc::getpid() as libc::id_t);
        if nice == -1 && *libc::__errno_location() != 0 {
            0
        } else {
            nice
        }
    }
}

/// Set the nice value of the calling thread.
pub fn set_nice(nice: i32) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: gettid has no preconditions; setpriority on our own tid.
        let result = unsafe {
            let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
            libc::setpriority(libc::PRIO_PROCESS, tid, nice)
        };
        if result == 0 {
            Ok(())
        } else {
            Err(format!("nice {nice}: {}", io::Error::last_os_error()))
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(format!("nice {nice} is Linux only"))
    }
}

/// Whether the fast plane's system calls exist on this platform.
pub const fn supported() -> bool {
    cfg!(target_os = "linux")
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    /// Once the socket refuses a message as full, the rest of the batch
    /// costs no syscall each; an error of one destination does not stop
    /// the others.
    #[test]
    fn busy_poll_is_set_or_refused_for_want_of_the_privilege() {
        let socket = bind_plain("127.0.0.1:0".parse().unwrap()).unwrap();
        match set_busy_poll(&socket, 1) {
            Ok(()) => {
                let mut value: libc::c_int = 0;
                let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
                // SAFETY: a plain getsockopt of a c_int on a live descriptor.
                let result = unsafe {
                    libc::getsockopt(
                        socket.as_raw_fd(),
                        libc::SOL_SOCKET,
                        libc::SO_BUSY_POLL,
                        std::ptr::from_mut(&mut value).cast(),
                        &mut len,
                    )
                };
                assert_eq!(result, 0);
                assert_eq!(value, 1);
            }
            Err(error) => assert_eq!(error.raw_os_error(), Some(libc::EPERM)),
        }
    }

    /// A helper of a pinned thread runs on the process's CPUs again.
    #[test]
    fn an_ordinary_thread_runs_on_the_cpus_of_the_process() {
        let count = |set: &libc::cpu_set_t| unsafe { libc::CPU_COUNT(set) };
        let affinity = |tid: libc::pid_t| unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::sched_getaffinity(tid, std::mem::size_of::<libc::cpu_set_t>(), &mut set);
            set
        };
        let process = count(&affinity(unsafe { libc::getpid() }));
        std::thread::spawn(move || {
            crate::fast_path::pin_current_thread(0).unwrap();
            assert_eq!(count(&affinity(0)), 1);
            set_ordinary().unwrap();
            assert_eq!(count(&affinity(0)), process);
            let policy = unsafe { libc::sched_getscheduler(0) };
            assert_eq!(policy, libc::SCHED_OTHER);
            // At the process's nice value, which needs no privilege (the
            // test passes under `nice -n 10` too).
            assert_eq!(super::process_nice(), unsafe {
                libc::getpriority(libc::PRIO_PROCESS, 0)
            });
        })
        .join()
        .unwrap();
    }

    #[test]
    fn a_socket_with_room_is_writable_at_once() {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let started = std::time::Instant::now();
        assert!(wait_writable(socket.as_raw_fd(), 1_000));
        assert!(started.elapsed() < std::time::Duration::from_millis(500));
    }

    #[test]
    fn the_rest_of_a_batch_stops_at_the_first_full_socket() {
        let mut tries = Vec::new();
        let mut outcomes = Vec::new();
        send_rest(
            10,
            1_600,
            |index| {
                tries.push(index);
                match index {
                    10 => Ok(1),
                    11 => Err(io::Error::from(io::ErrorKind::ConnectionRefused)),
                    _ => Err(io::Error::from(io::ErrorKind::WouldBlock)),
                }
            },
            |index, outcome| outcomes.push((index, outcome)),
        );
        assert_eq!(
            tries,
            vec![10, 11, 12],
            "one syscall after the socket is full"
        );
        assert_eq!(outcomes.len(), 1_590);
        assert_eq!(outcomes[0], (10, RestOutcome::Sent));
        assert_eq!(outcomes[1], (11, RestOutcome::Error));
        assert!(outcomes[2..]
            .iter()
            .all(|(_, outcome)| *outcome == RestOutcome::WouldBlock));
    }

    #[test]
    fn a_batch_receives_what_a_batch_sends_with_peer_and_kernel_time() {
        let receiver = bind_plain("127.0.0.1:0".parse().unwrap()).unwrap();
        enable_receive_metadata(&receiver).unwrap();
        let target = receiver.local_addr().unwrap();
        let sender = bind_reuseport("127.0.0.1:0".parse().unwrap()).unwrap();
        let twin = bind_reuseport(sender.local_addr().unwrap()).unwrap();
        assert_eq!(twin.local_addr().unwrap(), sender.local_addr().unwrap());
        set_priority(&sender, AUDIO_SO_PRIORITY).unwrap();
        let payloads: Vec<Vec<u8>> = (0..40_u8)
            .map(|index| vec![index; 100 + usize::from(index)])
            .collect();
        let sent = send_batch(sender.as_raw_fd(), payloads.len(), |index| {
            (&payloads[index][..], target)
        })
        .unwrap();
        assert_eq!(sent, 40);
        // The twin sends from the same address.
        send_batch(twin.as_raw_fd(), 1, |_| (&b"twin"[..], target)).unwrap();
        assert_ne!(wait_readable(&[receiver.as_raw_fd()], 1_000).unwrap(), 0);
        let mut batch = RecvBatch::new();
        let mut got = Vec::new();
        for _ in 0..20 {
            let n = batch.recv(&receiver).unwrap();
            for index in 0..n {
                let message = batch.get(index);
                assert_eq!(message.peer, Some(sender.local_addr().unwrap()));
                assert!(message.kernel_us.is_some());
                assert!(!message.truncated);
                got.push(message.data.to_vec());
            }
            if got.len() == 41 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(got.len(), 41);
        assert_eq!(&got[..40], &payloads[..]);
        assert_eq!(got[40], b"twin");
    }

    #[test]
    fn gso_runs_join_equal_datagrams_to_one_target_within_the_limits() {
        let a: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let b: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        // Ten symbols to a, a short last one, then two to b and one to a.
        let mut datagrams: Vec<(usize, SocketAddr)> = vec![(1_200, a); 10];
        datagrams.push((300, a));
        datagrams.push((1_200, a));
        datagrams.extend([(1_200, b), (1_200, b), (1_200, a)]);
        let runs = gso_runs(datagrams.len(), |index| datagrams[index]);
        assert_eq!(runs, vec![(0, 11), (11, 1), (12, 2), (14, 1)]);
        // A longer datagram starts a new run.
        let grow = [(500, a), (500, a), (900, a)];
        assert_eq!(gso_runs(3, |index| grow[index]), vec![(0, 2), (2, 1)]);
        // Segments and bytes are bounded.
        let many = vec![(1_000, a); 200];
        let runs = gso_runs(many.len(), |index| many[index]);
        assert!(runs.iter().all(|run| run.1 <= GSO_MAX_SEGMENTS));
        assert!(runs.iter().all(|run| run.1 * 1_000 <= GSO_MAX_BYTES));
        assert_eq!(runs.iter().map(|run| run.1).sum::<usize>(), 200);
        let big = vec![(9_000, a); 20];
        let runs = gso_runs(big.len(), |index| big[index]);
        assert!(runs.iter().all(|run| run.1 * 9_000 <= GSO_MAX_BYTES));
        assert_eq!(gso_runs(0, |index| many[index]), Vec::new());
    }

    #[test]
    fn gso_runs_are_cut_to_the_segments_each_run_may_carry() {
        let a: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let b: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        // 40 symbols to a, then 10 to b: a's runs carry at most 16, b's 3.
        let mut datagrams: Vec<(usize, SocketAddr)> = vec![(1_100, a); 40];
        datagrams.extend([(1_100, b); 10]);
        let mut runs = Vec::new();
        gso_runs_into(
            &mut runs,
            datagrams.len(),
            |index| datagrams[index],
            |first| if datagrams[first].1 == a { 16 } else { 3 },
        );
        assert_eq!(
            runs,
            vec![
                (0, 16),
                (16, 16),
                (32, 8),
                (40, 3),
                (43, 3),
                (46, 3),
                (49, 1)
            ]
        );
        // A limit of 0 or above the kernel's is held to 1..=64.
        gso_runs_into(&mut runs, 3, |index| datagrams[index], |_| 0);
        assert_eq!(runs, vec![(0, 1), (1, 1), (2, 1)]);
        gso_runs_into(&mut runs, 40, |index| datagrams[index], |_| 1_000);
        assert_eq!(runs, vec![(0, 40)]);
    }

    /// Runs with an earliest departure each (SO_TXTIME): the receiver sees
    /// the same datagrams in order (on loopback without fq they leave at
    /// once; `tests::video_throughput` measures the spacing under fq).
    #[test]
    fn runs_with_departures_arrive_as_the_same_datagrams_in_order() {
        let receiver = bind_plain("127.0.0.1:0".parse().unwrap()).unwrap();
        socket2::SockRef::from(&receiver)
            .set_recv_buffer_size(4 << 20)
            .unwrap();
        let target = receiver.local_addr().unwrap();
        let sender = bind_plain("127.0.0.1:0".parse().unwrap()).unwrap();
        enable_txtime(sender.as_raw_fd()).unwrap();
        let payloads: Vec<Vec<u8>> = (0..50_u32)
            .map(|index| vec![index as u8; if index == 49 { 200 } else { 1_100 }])
            .collect();
        let mut runs = Vec::new();
        gso_runs_into(
            &mut runs,
            payloads.len(),
            |index| (payloads[index].len(), target),
            |_| 8,
        );
        let now = monotonic_ns();
        let departures: Vec<u64> = (0..runs.len() as u64)
            .map(|run| now + run * 1_000)
            .collect();
        let sent = match send_gso_runs(
            sender.as_raw_fd(),
            &runs,
            |index| (&payloads[index][..], target),
            Some(&departures),
        ) {
            Ok(sent) => sent,
            Err(error) if gso_unsupported(&error) => {
                eprintln!("UDP GSO is not available here: {error}");
                return;
            }
            Err(error) => panic!("paced GSO send failed: {error}"),
        };
        assert_eq!(sent.datagrams, payloads.len());
        assert_eq!(sent.messages, runs.len());
        let mut got = Vec::new();
        let mut batch = RecvBatch::new();
        for _ in 0..100 {
            let _ = wait_readable(&[receiver.as_raw_fd()], 100);
            let n = batch.recv(&receiver).unwrap_or(0);
            got.extend((0..n).map(|index| batch.data(index).to_vec()));
            if got.len() == payloads.len() {
                break;
            }
        }
        assert_eq!(got, payloads);
        assert!(monotonic_ns() >= now);
    }

    #[test]
    fn a_gso_batch_arrives_as_the_same_datagrams_in_order() {
        let receivers: Vec<_> = (0..2)
            .map(|_| {
                let socket = bind_plain("127.0.0.1:0".parse().unwrap()).unwrap();
                socket2::SockRef::from(&socket)
                    .set_recv_buffer_size(4 << 20)
                    .unwrap();
                socket
            })
            .collect();
        let targets: Vec<SocketAddr> = receivers
            .iter()
            .map(|socket| socket.local_addr().unwrap())
            .collect();
        let sender = bind_plain("127.0.0.1:0".parse().unwrap()).unwrap();
        // A 4K part of 300 symbols and a short last one, interleaved per
        // datagram over two children as a plan of one socket would be.
        let payloads: Vec<Vec<u8>> = (0..301_u32)
            .map(|index| {
                let size = if index == 300 { 333 } else { 1_200 };
                (0..size)
                    .map(|byte| (index as usize * 7 + byte) as u8)
                    .collect()
            })
            .collect();
        let mut plan = Vec::new();
        for target in &targets {
            for payload in &payloads {
                plan.push((&payload[..], *target));
            }
        }
        let sent = match send_gso_batch(sender.as_raw_fd(), plan.len(), |index| plan[index]) {
            Ok(sent) => sent,
            Err(error) if gso_unsupported(&error) => {
                eprintln!("UDP GSO is not available here: {error}");
                return;
            }
            Err(error) => panic!("GSO send failed: {error}"),
        };
        assert_eq!(sent.datagrams, plan.len());
        // 301 datagrams per child in runs of at most 52 (63 KiB of 1,200
        // bytes), against 602 sends without GSO.
        assert!(sent.messages <= 2 * 6, "{sent:?}");
        assert!(sent.calls <= 4, "{sent:?}");
        for receiver in &receivers {
            let mut got = Vec::new();
            let mut batch = RecvBatch::new();
            for _ in 0..200 {
                let _ = wait_readable(&[receiver.as_raw_fd()], 100);
                let n = batch.recv(receiver).unwrap_or(0);
                for index in 0..n {
                    let message = batch.get(index);
                    assert_eq!(message.peer, Some(sender.local_addr().unwrap()));
                    got.push(message.data.to_vec());
                }
                if got.len() == payloads.len() {
                    break;
                }
            }
            assert_eq!(got.len(), payloads.len());
            assert_eq!(got, payloads);
        }
    }
}
