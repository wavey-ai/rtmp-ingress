use bytes::{Bytes, BytesMut};
use std::error::Error;
use ts::AccessUnit;

enum FrameType {
    FrameKey = 1,
    FrameInter = 2,
}

const VIDEO_H264: u8 = 7;

#[derive(Debug, Clone, Copy, PartialEq)]
enum VideoType {
    SeqHead = 0,
    Nalu = 1,
    Eos = 2,
}

impl VideoType {
    fn from_u8(value: u8) -> Option<VideoType> {
        match value {
            0 => Some(VideoType::SeqHead),
            1 => Some(VideoType::Nalu),
            2 => Some(VideoType::Eos),
            _ => None,
        }
    }
}

pub fn extract_au(packet: Bytes, timestamp: i64) -> Result<AccessUnit, Box<dyn Error>> {
    let codec = packet.get(0).ok_or("Packet is empty")? & 0x0F;

    if codec != VIDEO_H264 {
        return Err("Unsupported codec".into());
    }

    match VideoType::from_u8(packet[1]) {
        Some(VideoType::SeqHead) => {
            let pts = (((packet[2] as u32) << 16) | ((packet[3] as u32) << 8) | (packet[4] as u32))
                as u64;

            let avcc_start = 5;
            let nalu_length = avcc_start + 5;

            // Parsing SPS
            let sps_length_pos = nalu_length + 1;
            let sps_length =
                ((packet[sps_length_pos] as u16) << 8) | (packet[sps_length_pos + 1] as u16);
            let sps_start = sps_length_pos + 2;
            let sps_end = sps_start + sps_length as usize;

            let sps_data = packet.slice(sps_start..sps_end);

            // Parsing PPS
            let pps_length_pos = sps_end + 1;
            let pps_length =
                ((packet[pps_length_pos] as u16) << 8) | (packet[pps_length_pos + 1] as u16);
            let pps_start = pps_length_pos + 2;
            let pps_end = pps_start + pps_length as usize;

            let pps_data = packet.slice(pps_start..pps_end);

            let mut data = BytesMut::new();
            data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
            data.extend_from_slice(&sps_data);
            data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
            data.extend_from_slice(&pps_data);

            Ok(AccessUnit {
                avc: true,
                key: false,
                pts,
                dts: pts,
                data: data.freeze(),
            })
        }
        Some(VideoType::Nalu) | Some(VideoType::Eos) => {
            let cts = (((packet[2] as u32) << 16) | ((packet[3] as u32) << 8) | (packet[4] as u32))
                as u64;

            let pts = cts + timestamp as u64;
            let dts = timestamp as u64;

            Ok(AccessUnit {
                avc: true,
                key: false,
                pts,
                dts,
                data: packet.slice(5..),
            })
        }
        _ => return Err("Unsupported or unknown video message type".into()),
    }
}
