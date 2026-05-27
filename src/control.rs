use anyhow::{anyhow, Result};
use bytes::Bytes;
use message_packetizer::{SignableMessage, SignedMessageDemuxer, SignedMessageEnvelope};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshControlMessage {
    pub node_id: String,
    pub region: String,
    pub event: MeshControlEvent,
}

impl SignableMessage for MeshControlMessage {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MeshControlEvent {
    NodeStarted { mesh_addr: String },
    StreamAvailable { stream_id: u64 },
}

pub fn packetize_control_message(message: &MeshControlMessage) -> Result<Vec<Bytes>> {
    let content = serde_json::to_vec(message)?;
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let envelope = SignedMessageEnvelope {
        sequence: timestamp,
        content,
        timestamp,
        signature: Vec::new(),
    };
    Ok(envelope.to_packets())
}

pub fn reassemble_unsigned_control_packets(packets: &[Bytes]) -> Result<MeshControlMessage> {
    let mut demuxer = SignedMessageDemuxer::new();
    let mut messages = Vec::new();

    for packet in packets {
        let result = demuxer.process_packet(packet);
        if let Some(error) = result.errors.into_iter().next() {
            return Err(anyhow!(error.to_string()));
        }
        messages.extend(result.messages);
    }

    let envelope = messages
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("control packets did not complete a message"))?;
    Ok(serde_json::from_slice(&envelope.content)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_messages_use_message_packetizer_packets() {
        let message = MeshControlMessage {
            node_id: "uk-1".into(),
            region: "uk".into(),
            event: MeshControlEvent::NodeStarted {
                mesh_addr: "127.0.0.1:9101".into(),
            },
        };

        let packets = packetize_control_message(&message).unwrap();
        assert!(!packets.is_empty());

        let decoded = reassemble_unsigned_control_packets(&packets).unwrap();
        assert_eq!(decoded, message);
    }
}
