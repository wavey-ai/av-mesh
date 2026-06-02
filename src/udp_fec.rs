use bytes::Bytes;
use raptorq_datagram_fec::{DatagramFecDecoder, DATAGRAM_MAGIC};
pub use raptorq_datagram_fec::{
    SequenceStats, UdpFecSender, DEFAULT_REPAIR_SYMBOLS, DEFAULT_SOURCE_SYMBOLS,
    DEFAULT_SYMBOL_SIZE, HEADER_LEN,
};
use raptorq_fec_transport::{split_stream_id_prefix, FecDatagramDecoder};
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedUdpFecPayload {
    pub stream_id: Option<u64>,
    pub payload: Bytes,
}

pub struct UdpFecReceiver {
    decoders: HashMap<SocketAddr, DatagramFecDecoder>,
    stream_decoders: HashMap<(SocketAddr, u64), FecDatagramDecoder>,
}

impl UdpFecReceiver {
    pub fn new() -> Self {
        Self {
            decoders: HashMap::new(),
            stream_decoders: HashMap::new(),
        }
    }

    pub fn push(&mut self, peer: SocketAddr, datagram: &[u8]) -> Option<Bytes> {
        self.push_payload(peer, datagram)
            .and_then(|decoded| decoded.stream_id.is_none().then_some(decoded.payload))
    }

    pub fn push_payload(
        &mut self,
        peer: SocketAddr,
        datagram: &[u8],
    ) -> Option<DecodedUdpFecPayload> {
        if let Some((stream_id, payload)) = split_stream_id_prefix(datagram) {
            if payload.starts_with(&DATAGRAM_MAGIC) {
                let decoder = self
                    .stream_decoders
                    .entry((peer, stream_id))
                    .or_insert_with(|| {
                        FecDatagramDecoder::webtransport_with_stream_prefix(stream_id)
                    });
                return decoder
                    .push_datagram(datagram)
                    .ok()
                    .flatten()
                    .map(|payload| DecodedUdpFecPayload {
                        stream_id: Some(stream_id),
                        payload: Bytes::from(payload),
                    });
            }
        }

        let decoder = self.decoders.entry(peer).or_default();
        decoder
            .push_datagram(datagram)
            .ok()
            .flatten()
            .map(|payload| DecodedUdpFecPayload {
                stream_id: None,
                payload: Bytes::from(payload),
            })
    }

    pub fn sequence_stats(&self, peer: SocketAddr) -> Option<SequenceStats> {
        self.decoders
            .get(&peer)
            .map(DatagramFecDecoder::sequence_stats)
    }
}

impl Default for UdpFecReceiver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UdpSocket;

    #[tokio::test]
    async fn udp_fec_sender_receiver_roundtrip() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let mut sender = UdpFecSender::new(addr)
            .await
            .unwrap()
            .with_repair_symbols(1)
            .with_symbol_size(DEFAULT_SYMBOL_SIZE);
        let mut receiver = UdpFecReceiver::new();
        let payload = Bytes::from_static(b"fec-protected-media");

        sender.send(&payload).await.unwrap();

        let mut buf = vec![0u8; 65_536];
        loop {
            let (len, peer) = socket.recv_from(&mut buf).await.unwrap();
            if let Some(decoded) = receiver.push(peer, &buf[..len]) {
                assert_eq!(decoded, payload);
                let stats = receiver.sequence_stats(peer).unwrap();
                assert!(stats.received > 0);
                break;
            }
        }
    }
}
