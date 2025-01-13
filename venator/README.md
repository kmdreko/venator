The Venator Rust library provides a tracing layer that will export logs and
spans to the [Venator app](https://crates.io/crates/venator-app).

## Usage

```toml
[dependencies]
venator = "1.0.0"
```

Install it as the global subscriber:

```rust
use venator::Venator;

// minimal
Venator::default().install();
```

```rust
use venator::Venator;

// configured
Venator::builder()
    .with_host("localhost:8362")
    .with_attribute("service", "my_app")
    .with_attribute("environment.name", "internal")
    .with_attribute("environment.dev", true)
    .build()
    .install();
```

Or use it as a [`Layer`](https://docs.rs/tracing-subscriber/0.3.19/tracing_subscriber/layer/trait.Layer.html):

```rust
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use venator::Venator;

tracing_subscriber::registry()
    .with(Venator::default())
    .with(tracing_subscriber::fmt::Layer::default())
    .with(tracing_subscriber::EnvFilter::from_default_env())
    .init()
```
