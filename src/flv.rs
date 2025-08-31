use access_unit::AccessUnit;
use bytes::{Bytes, BytesMut};
use std::error::Error;

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

pub fn extract_au(
    packet: Bytes,
    timestamp: i64,
    sps_pps: Option<&Bytes>,
) -> Result<(AccessUnit, bool), Box<dyn Error>> {
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

            Ok((
                AccessUnit {
                    stream_type: access_unit::PSI_STREAM_H264,
                    key: false,
                    pts,
                    dts: pts,
                    data: data.freeze(),
                    id: 0,
                },
                true,
            ))
        }
        Some(VideoType::Nalu) | Some(VideoType::Eos) => {
            let cts = (((packet[2] as u32) << 16) | ((packet[3] as u32) << 8) | (packet[4] as u32))
                as u64;

            let pts = cts + timestamp as u64;
            let dts = timestamp as u64;

            let nalus: BytesMut = lp_to_nal_start_code(packet.slice(5..));
            let mut data = BytesMut::new();
            if let Some(sps_pps_data) = sps_pps {
                data.extend_from_slice(&sps_pps_data);
            }
            data.extend_from_slice(&nalus);

            Ok((
                AccessUnit {
                    stream_type: access_unit::PSI_STREAM_H264, 
                    key: false,
                    pts,
                    dts,
                    data: data.freeze(),
                    id: 100,
                },
                false,
            ))
        }
        _ => return Err("Unsupported or unknown video message type".into()),
    }
}

pub fn lp_to_nal_start_code(flv_data: Bytes) -> BytesMut {
    let mut nal_units = BytesMut::new();
    let mut offset: usize = 0;

    while offset < flv_data.len() {
        if offset + 4 > flv_data.len() {
            break;
        }

        // Extract the NALU length (first 4 bytes) as Bytes and convert to [u8; 4]
        let nalu_length_bytes = flv_data.slice(offset..offset + 4);
        let nalu_length =
            u32::from_be_bytes(nalu_length_bytes.as_ref().try_into().unwrap()) as usize;
        offset += 4;

        if offset + nalu_length > flv_data.len() {
            break;
        }

        // Append the NAL start code
        nal_units.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);

        // Append the NAL unit data
        nal_units.extend_from_slice(&flv_data.slice(offset..offset + nalu_length));
        offset += nalu_length;
    }

    nal_units
}
