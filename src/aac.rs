use bytes::{Bytes, BytesMut};
use ts::aac::extract_aac_data;

pub fn ensure_adts_header(data: Bytes, channels: u8, sample_rate: u32) -> Bytes {
    // Assume that the first byte might contain the ASC if `extract_aac_data` finds no ADTS header
    if extract_aac_data(&data).is_none() {
        // Assuming data[0] is present and is the first byte of ASC
        // Parse the profile from the ASC
        let audio_object_type = data[0] >> 3; // First 5 bits contain the audio object type
        let profile = match audio_object_type {
            1 => 0x66, // AAC-LC
            2 => 0x67, // HE-AAC v1
            5 => 0x68, // HE-AAC v2
            _ => 0x66, // Default to AAC-LC if unknown
        };

        let header = create_adts_header(profile, channels, sample_rate, data.len() - 2, false);
        let mut payload = BytesMut::from(&header[..]);
        payload.extend_from_slice(&data[2..]); // Skip the first two bytes if they are part of ASC

        return payload.freeze();
    }

    return data;
}

fn create_adts_header(
    codec_id: u8,
    channels: u8,
    sample_rate: u32,
    aac_frame_length: usize,
    has_crc: bool,
) -> Vec<u8> {
    let profile_object_type = match codec_id {
        0x66 => 1, // AAC LC (internally set as `1`, should directly be `01` in bits)
        0x67 => 2, // AAC HEV1
        0x68 => 3, // AAC HEV2
        _ => 1,    // Default to AAC LC
    };

    let sample_rate_index = sample_rate_index(sample_rate);
    let channel_config = channels.min(7);
    let header_length = if has_crc { 9 } else { 7 };
    let frame_length = aac_frame_length + header_length;

    let mut header = Vec::with_capacity(header_length);
    let protection_absent = if has_crc { 0 } else { 1 };

    header.push(0xFF);
    header.push(0xF0 | protection_absent);

    let profile_and_sampling =
        (profile_object_type << 6) | (sample_rate_index << 2) | (channel_config >> 2);
    header.push(profile_and_sampling);

    let frame_length_high = ((frame_length >> 11) & 0x03) as u8;
    let frame_length_mid = ((frame_length >> 3) & 0xFF) as u8;
    header.push((channel_config & 3) << 6 | frame_length_high);
    header.push(frame_length_mid);

    let frame_length_low = ((frame_length & 0x07) << 5) | 0x1F;
    header.push(frame_length_low as u8);
    header.push(0xFC);

    if has_crc {
        header.extend_from_slice(&[0x00, 0x00]);
    }

    header
}

fn sample_rate_index(sample_rate: u32) -> u8 {
    match sample_rate {
        96000 => 0x0,
        88200 => 0x1,
        64000 => 0x2,
        48000 => 0x3,
        44100 => 0x4,
        32000 => 0x5,
        24000 => 0x6,
        22050 => 0x7,
        16000 => 0x8,
        12000 => 0x9,
        11025 => 0xA,
        8000 => 0xB,
        7350 => 0xC,
        _ => 0xF, // Invalid sample rate
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mse_fmp4::aac::{AdtsHeader, ChannelConfiguration, SamplingFrequency};

    #[test]
    fn test_adts_header_parsing() {
        let data = vec![0u8; 200]; // Dummy AAC frame data
        let channels = 2u8;
        let sample_rate = 44100u32;
        let adts_payload = create_adts_header(0x66, channels, sample_rate, data.len(), false);
        let mut full_payload = adts_payload.clone();
        full_payload.extend_from_slice(&data);

        let adts = AdtsHeader::read_from(&full_payload[..]).unwrap();
        assert_eq!(adts.frame_len, 207);
        assert_eq!(adts.sampling_frequency, SamplingFrequency::Hz44100);
        assert_eq!(
            adts.channel_configuration,
            ChannelConfiguration::TwoChannels
        );
        assert_eq!(adts.profile, mse_fmp4::aac::AacProfile::Lc);
    }
}
