<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" height=170 srcset="docs/images/icon-dark.svg">
    <source media="(prefers-color-scheme: light)" height=170 srcset="docs/images/icon-light.svg">
    <img alt="venator logo" height=170 src="docs/images/icon-light.svg">
  </picture>
</p>

Venator is a application for recording, viewing, and filtering logs and spans
from programs instrumented with the Rust tracing crate or using OpenTelemetry.
It is purpose-built for rapid local development.

## Installation

### With Cargo:

Compiling and installing `venator` source with Cargo requires Rust 1.76 or newer:

```
cargo install venator-app
```

### With Pre-built Binaries:

TBD

## Usage

### Using Rust Tracing:

In your instrumented program:

```toml
[dependencies]
venator = "0.2.0"
```

```rust
use venator::Venator;

Venator::default().install();
```

See the [documentation](https://docs.rs/venator/latest/venator/) for more.

### Using OpenTelemetry:

Configure your program's OpenTelemetry SDK to export logs and traces to
`127.0.0.1:8362` (the default for Venator) and to use GRPC or HTTP with binary
encoding.

## Screenshots:

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/screenshot-events-dark.png">
  <source media="(prefers-color-scheme: light)" srcset="docs/images/screenshot-events-light.png">
  <img alt="screenshots of events screen" src="docs/images/screenshot-events-light.png">
</picture>
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/screenshot-spans-dark.png">
  <source media="(prefers-color-scheme: light)" srcset="docs/images/screenshot-spans-light.png">
  <img alt="screenshots of spans screen" src="docs/images/screenshot-spans-light.png">
</picture>
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/screenshot-traces-dark.png">
  <source media="(prefers-color-scheme: light)" srcset="docs/images/screenshot-traces-light.png">
  <img alt="screenshots of trace screen" src="docs/images/screenshot-traces-light.png">
</picture>
