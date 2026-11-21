//! The fast plane: RelaySession datagrams forwarded on arrival by one OS
//! thread per traffic class, with the decode and commit behind them.
//!
//! At a relay or distributor each RLS1 receive port is a pair: audio streams
//! arrive on the port and video streams on the port plus the class offset.
//! Each class has a thread that owns its sockets and, per drained batch,
//! reads the RLS1 header in place, admits the symbol against its parent's
//! session, drops the second copy of a symbol, hands every child its copy in
//! one non-blocking `sendmmsg` per source socket, and only then pushes the
//! datagram onto the class's tee: a bounded channel to a tokio task that
//! runs the unchanged receive path (deadline, payload hash, assembly) and
//! commits the object to the node's caches. Nothing on the thread waits,
//! decodes or hashes the payload, so a 4K object never holds an audio symbol
//! back, and a stall of the runtime never stops forwarding.

pub mod dedup;
pub mod relay;
pub mod sys;
pub mod view;

use dashmap::DashMap;
use playlists::mesh::StreamClass;
use std::sync::Arc;

/// The traffic class of a stream: where its datagrams travel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TrafficClass {
    Audio = 0,
    Video = 1,
}

impl TrafficClass {
    pub const ALL: [Self; 2] = [Self::Audio, Self::Video];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Audio => "audio",
            Self::Video => "video",
        }
    }

    pub const fn index(self) -> usize {
        self as usize
    }
}

/// The streams this node knows to be video: those that arrived on a video
/// port, carried a video object kind, or came over the cache mesh's class
/// socket. Every other stream is audio. Shared by the relay tee, the cache
/// mesh's classifier and the edge's client sockets.
#[derive(Clone, Default)]
pub struct StreamClasses(Arc<DashMap<u64, ()>>);

impl StreamClasses {
    pub fn mark_video(&self, stream_id: u64) {
        if !self.0.contains_key(&stream_id) {
            self.0.insert(stream_id, ());
        }
    }

    pub fn class(&self, stream_id: u64) -> TrafficClass {
        if self.0.contains_key(&stream_id) {
            TrafficClass::Video
        } else {
            TrafficClass::Audio
        }
    }

    pub fn mesh_class(&self, stream_id: u64) -> StreamClass {
        match self.class(stream_id) {
            TrafficClass::Audio => StreamClass::Audio,
            TrafficClass::Video => StreamClass::Video,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn video_streams(&self) -> usize {
        self.0.len()
    }
}

/// The address of the video twin of an audio address.
pub fn class_addr(
    addr: std::net::SocketAddr,
    class: TrafficClass,
    offset: u16,
) -> anyhow::Result<std::net::SocketAddr> {
    match class {
        TrafficClass::Audio => Ok(addr),
        TrafficClass::Video => {
            if addr.port() == 0 {
                return Ok(addr);
            }
            let port = addr.port().checked_add(offset).ok_or_else(|| {
                anyhow::anyhow!("class port offset {offset} takes {addr} past port 65535")
            })?;
            let mut video = addr;
            video.set_port(port);
            Ok(video)
        }
    }
}

/// The receive observer of the cache mesh's class socket: every part that
/// arrives there is of a video playlist, so its stream is marked video
/// before the datagram goes on to `inner` (the edge's cut-through).
pub struct ClassMarker {
    classes: StreamClasses,
    inner: Option<Arc<dyn playlists::mesh::CacheMeshReceiveObserver>>,
}

impl ClassMarker {
    pub fn new(
        classes: StreamClasses,
        inner: Option<Arc<dyn playlists::mesh::CacheMeshReceiveObserver>>,
    ) -> Self {
        Self { classes, inner }
    }
}

impl playlists::mesh::CacheMeshReceiveObserver for ClassMarker {
    fn datagram(&self, peer: std::net::SocketAddr, datagram: &[u8]) {
        if let Some(inner) = &self.inner {
            inner.datagram(peer, datagram);
        }
    }

    fn decoded(&self, peer: std::net::SocketAddr, datagram: &[u8], frame: &[u8]) {
        if let Some((stream_id, _)) = playlists::mesh::chunk_frame_slot(frame) {
            self.classes.mark_video(stream_id);
        }
        if let Some(inner) = &self.inner {
            inner.decoded(peer, datagram, frame);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stream_is_audio_until_marked_video() {
        let classes = StreamClasses::default();
        assert_eq!(classes.class(4001), TrafficClass::Audio);
        classes.mark_video(1);
        classes.mark_video(1);
        assert_eq!(classes.class(1), TrafficClass::Video);
        assert_eq!(classes.mesh_class(1), StreamClass::Video);
        assert_eq!(classes.mesh_class(4001), StreamClass::Audio);
        assert_eq!(classes.video_streams(), 1);
    }

    #[test]
    fn the_video_twin_is_at_the_offset_and_never_past_the_last_port() {
        let addr: std::net::SocketAddr = "10.0.0.1:22001".parse().unwrap();
        assert_eq!(class_addr(addr, TrafficClass::Audio, 1000).unwrap(), addr);
        assert_eq!(
            class_addr(addr, TrafficClass::Video, 1000).unwrap(),
            "10.0.0.1:23001".parse().unwrap()
        );
        assert!(class_addr("10.0.0.1:65000".parse().unwrap(), TrafficClass::Video, 1000).is_err());
    }
}
