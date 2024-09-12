The Venator library provides a tracing layer that will export logs and spans to the Venator app.

This is currently in an "alpha" state; bugs, quirks, and missing functionality are to be expected. Bug reports and feature requests are welcome.

## Usage

```toml
[dependencies]
tracing = "0.1.40"
tracing-subscriber = "0.3.18"
venator = "0.1.0"
```

```rust
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use venator::Venator;

tracing_subscriber::registry()
    .with(Venator::builder()
    	.with_host("localhost:8362")         // optional, this is the default
    	.with_attribute("service", "my_app") // provide any top-level attributes
    	.build()
    )
    .init();
```