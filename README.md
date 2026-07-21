# RTMP ingress

`rtmp-ingress` receives RTMP publish sessions and emits typed media access units.

The listener validates stream keys with `wavey-gatekeeper`. It parses H.264 video and AAC audio from the RTMP session.

## Add the crate

Add the dependency to your `Cargo.toml` file:

```toml
[dependencies]
rtmp-ingress = "0.1.0"
```

## Start the listener

Call `start_rtmp_listener` with a gatekeeper key and a socket address. The function returns media events and shutdown controls.

```rust
use rtmp_ingress::ingress::start_rtmp_listener;

let (_up, _finished, shutdown, mut events) =
    start_rtmp_listener(gatekeeper_key, "0.0.0.0:1935".parse()?).await?;
```

## Acknowledgements

This crate uses [rml_rtmp](https://github.com/wavey-ai/rust-media-libs) for the RTMP protocol. This project is a fork of [rust-media-libs](https://github.com/KallDrexx/rust-media-libs).

### rust-media-libs

Matthew Shapiro created the original `rust-media-libs` project. The project provides these crates:

- `rml_amf0` for AMF0 serialization and deserialization.
- `rml_rtmp` for high-level and low-level RTMP protocol APIs.

The original project uses the MIT and Apache-2.0 licenses.

### Code Attribution

| Component | Source | License |
|-----------|--------|---------|
| RTMP handshake | rust-media-libs | MIT/Apache-2.0 |
| RTMP chunk parsing | rust-media-libs | MIT/Apache-2.0 |
| RTMP session management | rust-media-libs | MIT/Apache-2.0 |
| AMF0 encoding/decoding | rust-media-libs | MIT/Apache-2.0 |
| FLV parsing (`flv.rs`) | This project | - |
| TLS integration | This project | - |

## License

This crate uses the MIT license. Refer to [LICENSE](LICENSE) for the license text.

The upstream `rust-media-libs` project uses the MIT and Apache-2.0 licenses.
