use bytes::Bytes;
use raptorq_datagram_fec::DatagramFecDecoder;
pub use raptorq_datagram_fec::{
    SequenceStats, UdpFecSender, DEFAULT_REPAIR_SYMBOLS, DEFAULT_SOURCE_SYMBOLS,
    DEFAULT_SYMBOL_SIZE, HEADER_LEN,
};
use std::collections::HashMap;
use std::net::SocketAddr;

pub struct UdpFecReceiver {
    decoders: HashMap<SocketAddr, DatagramFecDecoder>,
}

impl UdpFecReceiver {
    pub fn new() -> Self {
        Self {
            decoders: HashMap::new(),
        }
    }

    pub fn push(&mut self, peer: SocketAddr, datagram: &[u8]) -> Option<Bytes> {
        let decoder = self.decoders.entry(peer).or_default();
        decoder
            .push_datagram(datagram)
            .ok()
            .flatten()
            .map(Bytes::from)
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
