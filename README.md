# RTMP ingress

`rtmp-ingress` receives RTMP publish sessions and emits typed media access units.
It parses H.264 video and AAC audio from FLV messages.

The listener validates stream keys with `wavey-gatekeeper`.
Applications receive each media unit through a typed Rust event.

## Add the crate

Add the dependency to `Cargo.toml`:

```toml
[dependencies]
rtmp-ingress = "0.1.1"
```

Enable `upload-response` to connect RTMP sessions to `av-upload-response`.
Enable `tls` to accept RTMPS sessions through rustls.

```toml
[dependencies]
rtmp-ingress = { version = "0.1.1", features = ["tls"] }
```

## Start the event listener

Call `start_rtmp_listener` with a gatekeeper key and socket address.
The function returns the media events and shutdown controls.

```rust
use rtmp_ingress::ingress::start_rtmp_listener;

let (_up, _finished, shutdown, mut events) =
    start_rtmp_listener(gatekeeper_key, "0.0.0.0:1935".parse()?).await?;
```

## Start the upload service

Use `RtmpUploadIngest` with an `UploadResponseService` instance.

```rust
use rtmp_ingress::upload::RtmpUploadIngest;

let ingest = RtmpUploadIngest::new(service);
let shutdown = ingest.start("0.0.0.0:1935".parse()?).await?;
```

The `tls` feature adds `start_tls` for RTMPS.
It also enables the required `upload-response` feature.

## Acknowledgments

This crate uses the Wavey fork of `rml_rtmp` for RTMP protocol processing.
Matthew Shapiro created the original `rust-media-libs` project.

The upstream project provides `rml_amf0` and `rml_rtmp`.
The upstream code uses the MIT and Apache-2.0 licenses.

## License

This crate uses the MIT license.
See [LICENSE](LICENSE) for the license text.
