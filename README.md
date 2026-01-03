# rtmp-ingress

RTMP/RTMPS ingest server for Rust with optional TLS support.

## Features

- RTMP ingest with video/audio parsing
- Optional TLS support (RTMPS) via `tls` feature
- Integration with `upload-response` service via `upload-response` feature
- FLV demuxing and AccessUnit extraction
- H.264 video and AAC audio support

## Usage

```rust
use rtmp_ingress::upload::RtmpUploadIngest;

// Plain RTMP
let ingest = RtmpUploadIngest::new(service);
let shutdown = ingest.start(addr).await?;

// RTMPS (TLS) - requires "tls" feature
let ingest = RtmpUploadIngest::new(service);
let shutdown = ingest.start_tls(addr, cert_pem, key_pem).await?;
```

## Features

- `upload-response` - Integration with upload-response shared memory cache
- `tls` - RTMPS (TLS) support via rustls

## Acknowledgements

This crate uses [rml_rtmp](https://github.com/wavey-ai/rust-media-libs) for RTMP protocol handling, which is a fork of [KallDrexx/rust-media-libs](https://github.com/KallDrexx/rust-media-libs).

### rust-media-libs

The original rust-media-libs was created by [Matthew Shapiro (KallDrexx)](https://github.com/KallDrexx) and provides:

- **rml_amf0** - AMF0 serialization/deserialization
- **rml_rtmp** - High and low level RTMP protocol APIs

The original work is dual-licensed under MIT and Apache-2.0.

### Code Attribution

| Component | Source | License |
|-----------|--------|---------|
| RTMP handshake | rust-media-libs | MIT/Apache-2.0 |
| RTMP chunk parsing | rust-media-libs | MIT/Apache-2.0 |
| RTMP session management | rust-media-libs | MIT/Apache-2.0 |
| AMF0 encoding/decoding | rust-media-libs | MIT/Apache-2.0 |
| FLV parsing (`flv.rs`) | This project | - |
| TLS integration | This project | - |
| upload-response integration | This project | - |

## License

See the LICENSE file for details.
