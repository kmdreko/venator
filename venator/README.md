The Venator library provides a tracing layer that will export logs and spans to
the Venator app.

This is currently in a "beta" state; bugs and quirks are to be expected but
functionality should be complete. Bug reports and future feature requests are
welcome.

## Usage

```toml
[dependencies]
venator = "0.2.0"
```

```rust
use venator::Venator;

Venator::builder()
    .with_host("localhost:8362")
    .with_attribute("service", "my_app")
    .with_attribute("environment", "dev")
    .build()
    .install();
```
