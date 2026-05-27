use bytes::Bytes;
use raptorq::{Decoder, Encoder, EncodingPacket, ObjectTransmissionInformation};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;

pub const HEADER_LEN: usize = 12;
pub const DEFAULT_SYMBOL_SIZE: u16 = 1316;
pub const DEFAULT_SOURCE_SYMBOLS: u16 = 4;
pub const DEFAULT_REPAIR_SYMBOLS: u32 = 1;

#[derive(Debug, Clone, Copy)]
struct WireHeader {
    block_id: u32,
    transfer_length: u32,
    source_symbols: u16,
    symbol_size: u16,
}

impl WireHeader {
    fn encode(&self, buf: &mut [u8]) {
        buf[0..4].copy_from_slice(&self.block_id.to_le_bytes());
        buf[4..8].copy_from_slice(&self.transfer_length.to_le_bytes());
        buf[8..10].copy_from_slice(&self.source_symbols.to_le_bytes());
        buf[10..12].copy_from_slice(&self.symbol_size.to_le_bytes());
    }

    fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < HEADER_LEN {
            return None;
        }
        Some(Self {
            block_id: u32::from_le_bytes(buf[0..4].try_into().ok()?),
            transfer_length: u32::from_le_bytes(buf[4..8].try_into().ok()?),
            source_symbols: u16::from_le_bytes(buf[8..10].try_into().ok()?),
            symbol_size: u16::from_le_bytes(buf[10..12].try_into().ok()?),
        })
    }

    fn oti(&self) -> ObjectTransmissionInformation {
        ObjectTransmissionInformation::with_defaults(self.transfer_length as u64, self.symbol_size)
    }
}

pub struct UdpFecSender {
    socket: Arc<UdpSocket>,
    target: SocketAddr,
    block_id: u32,
    source_symbols: u16,
    repair_symbols: u32,
    symbol_size: u16,
}

impl UdpFecSender {
    pub async fn new(target: SocketAddr) -> std::io::Result<Self> {
        let bind_addr: SocketAddr = if target.is_ipv6() {
            "[::]:0".parse().unwrap()
        } else {
            "0.0.0.0:0".parse().unwrap()
        };
        let socket = UdpSocket::bind(bind_addr).await?;
        Ok(Self {
            socket: Arc::new(socket),
            target,
            block_id: 0,
            source_symbols: DEFAULT_SOURCE_SYMBOLS,
            repair_symbols: DEFAULT_REPAIR_SYMBOLS,
            symbol_size: DEFAULT_SYMBOL_SIZE,
        })
    }

    pub fn with_repair_symbols(mut self, repair_symbols: u32) -> Self {
        self.repair_symbols = repair_symbols;
        self
    }

    pub fn with_symbol_size(mut self, symbol_size: u16) -> Self {
        self.symbol_size = symbol_size;
        self
    }

    pub async fn send(&mut self, data: &[u8]) -> std::io::Result<()> {
        let encoder = Encoder::with_defaults(data, self.symbol_size);
        let packets = encoder.get_encoded_packets(self.repair_symbols);
        let header = WireHeader {
            block_id: self.block_id,
            transfer_length: data.len() as u32,
            source_symbols: self.source_symbols,
            symbol_size: self.symbol_size,
        };

        let mut datagram = Vec::with_capacity(HEADER_LEN + self.symbol_size as usize + 64);
        for packet in packets {
            let serialized = packet.serialize();
            datagram.clear();
            datagram.resize(HEADER_LEN, 0);
            header.encode(&mut datagram[..HEADER_LEN]);
            datagram.extend_from_slice(&serialized);
            self.socket.send_to(&datagram, self.target).await?;
        }

        self.block_id = self.block_id.wrapping_add(1);
        Ok(())
    }
}

struct BlockState {
    decoder: Decoder,
}

pub struct UdpFecReceiver {
    blocks: HashMap<(SocketAddr, u32), BlockState>,
    completed: HashSet<(SocketAddr, u32)>,
}

impl UdpFecReceiver {
    pub fn new() -> Self {
        Self {
            blocks: HashMap::new(),
            completed: HashSet::new(),
        }
    }

    pub fn push(&mut self, peer: SocketAddr, datagram: &[u8]) -> Option<Bytes> {
        let header = WireHeader::decode(datagram)?;
        if self.completed.contains(&(peer, header.block_id)) {
            return None;
        }
        let packet = EncodingPacket::deserialize(&datagram[HEADER_LEN..]);
        let key = (peer, header.block_id);
        let state = self.blocks.entry(key).or_insert_with(|| BlockState {
            decoder: Decoder::new(header.oti()),
        });

        let decoded = state.decoder.decode(packet)?;
        self.blocks.remove(&key);
        self.completed.insert(key);
        self.prune(peer, header.block_id);
        Some(Bytes::from(decoded))
    }

    fn prune(&mut self, peer: SocketAddr, current_block_id: u32) {
        if current_block_id < 32 {
            return;
        }
        let cutoff = current_block_id.wrapping_sub(32);
        self.blocks
            .retain(|(candidate_peer, block_id), _| *candidate_peer != peer || *block_id >= cutoff);
        self.completed
            .retain(|(candidate_peer, block_id)| *candidate_peer != peer || *block_id >= cutoff);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                break;
            }
        }
    }
}
