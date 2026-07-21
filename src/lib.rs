mod connection_action;
pub mod ingress;
mod listener;
mod state;

use access_unit::AccessUnit;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RtmpStreamInfo {
    pub key: String,
    pub id: u64,
}

#[derive(Debug, Clone)]
pub enum RtmpIngestEvent {
    AccessUnit {
        stream: RtmpStreamInfo,
        access_unit: AccessUnit,
    },
    End {
        stream: RtmpStreamInfo,
    },
}

pub const PSI_STREAM_H264: u8 = 0x1b;
pub const PSI_STREAM_AAC: u8 = 0x0f;
