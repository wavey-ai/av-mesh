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

/// Wait until one of the descriptors is readable, or the timeout. Returns
/// a bit set of the readable ones.
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
}
