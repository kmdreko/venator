#![doc = include_str!("../README.md")]

use std::cell::Cell;
use std::collections::BTreeMap;
use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tracing::span::{Attributes, Id, Record};
use tracing::{debug, error, Event, Subscriber, Value};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

mod fields;
mod ids;
mod messaging;

use fields::OwnedValue;
use ids::VenatorId;
use messaging::{Handshake, Message};

/// This is a builder for configuring a [`Venator`] layer. Use [`.build()`](VenatorBuilder::build)
/// to finalize.
pub struct VenatorBuilder {
    host: Option<String>,
    fields: BTreeMap<String, OwnedValue>,
}

impl VenatorBuilder {
    /// This will set the host address of the `Venator` layer used to connect to
    /// the Venator app.
    ///
    /// Setting the host again will overwrite the previous value. The default is
    /// `"localhost:8362"` which is the default for the Venator app.
    ///
    /// # Examples
    ///
    /// ```
    /// # use venator::Venator;
    /// let venator_layer = Venator::builder()
    ///     .with_host("localhost:8362")
    ///     .build()
    ///     .install();
    /// ```
    pub fn with_host<H: Into<String>>(mut self, host: H) -> VenatorBuilder {
        self.host = Some(host.into());
        self
    }

    /// This will add an attribute to the `Venator` layer. These will be
    /// provided to the Venator app and all events and spans will have these
    /// root attributes.
    ///
    /// Providing an attribute with the same name as another will overwrite the
    /// previous value. Note that some values (like `None`) don't record a
    /// "value" and will not set or overwrite that attribute.
    ///
    /// # Examples
    ///
    /// ```
    /// # use venator::Venator;
    /// let venator_layer = Venator::builder()
    ///     .with_attribute("service", "my_app")
    ///     .with_attribute("service.version", 5)
    ///     .with_attribute("environment", "dev")
    ///     .with_attribute("environment.debug", true)
    ///     .build()
    ///     .install();
    /// ```
    pub fn with_attribute<A: Into<String>, V: Value>(
        mut self,
        attribute: A,
        value: V,
    ) -> VenatorBuilder {
        if let Some(value) = OwnedValue::from_tracing(value) {
            self.fields.insert(attribute.into(), value);
        }
        self
    }

    /// This will build the `Venator` layer. It will need to be added to another
    /// subscriber via `.with()` or installed globally with [`.install()`](Venator::install)
    /// to be useful.
    ///
    /// # Examples
    ///
    /// ```
    /// # use venator::Venator;
    /// # use tracing_subscriber::layer::SubscriberExt;
    /// # use tracing_subscriber::util::SubscriberInitExt;
    /// let venator_layer = Venator::builder()
    ///     .with_host("localhost:8362")
    ///     .with_attribute("service", "my_app")
    ///     .with_attribute("environment", "dev")
    ///     .build();
    ///
    /// tracing_subscriber::registry()
    ///     .with(venator_layer)
    ///     .with(tracing_subscriber::fmt::Layer::default())
    ///     .with(tracing_subscriber::EnvFilter::from_default_env())
    ///     .init();
    /// ```
    pub fn build(self) -> Venator {
        let connection = Connection::new(self.host, self.fields);

        Venator {
            connection: Mutex::new(connection),
        }
    }
}

/// This is the layer that will connect and send event and span data to the
/// Venator app.
///
/// You can configure it with [`.builder()`](Venator::builder) or just use the
/// `default()`.
pub struct Venator {
    connection: Mutex<Connection>,
}

impl Venator {
    /// This creates a builder for a `Venator` layer for configuring the host
    /// and/or attributes.
    pub fn builder() -> VenatorBuilder {
        VenatorBuilder {
            host: None,
            fields: BTreeMap::new(),
        }
    }

    /// This will set a default `Venator` layer as the global subscriber.
    ///
    /// # Panics
    ///
    /// This call will panic if there is already a global subscriber configured.
    ///
    /// # Examples
    ///
    /// ```
    /// # use venator::Venator;
    /// Venator::default().install();
    /// ```
    pub fn install(self) {
        use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
        tracing_subscriber::registry().with(self).init();
    }

    fn send(&self, message: &Message) {
        // this persists the space used for encoding the message in a thread
        // local to reduce per-call allocation costs
        thread_local! { static SCRATCH: Cell<Vec<u8>> = const { Cell::new(Vec::new()) }; }

        let mut buffer = SCRATCH.with(|b| b.take());

        if let Err(err) = messaging::encode(&mut buffer, &message) {
            error!(parent: None, "failed to encode message: {err:?}");
            return;
        };

        self.connection.lock().unwrap().send(&buffer);

        SCRATCH.with(|b| b.set(buffer));
    }
}

impl Default for Venator {
    fn default() -> Self {
        Venator::builder().build()
    }
}

impl<S> Layer<S> for Venator
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let vid = ids::generate();
        ctx.span(id).unwrap().extensions_mut().insert(vid);

        self.send(&Message::from_new_span(attrs, &vid, &ctx));
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let vid = ctx
            .span(id)
            .unwrap()
            .extensions()
            .get::<VenatorId>()
            .copied()
            .unwrap();

        self.send(&Message::from_record(&vid, values));
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        let vid = ctx
            .span(id)
            .unwrap()
            .extensions()
            .get::<VenatorId>()
            .copied()
            .unwrap();

        self.send(&Message::from_enter(&vid));
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        let vid = ctx
            .span(id)
            .unwrap()
            .extensions()
            .get::<VenatorId>()
            .copied()
            .unwrap();

        self.send(&Message::from_exit(&vid));
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let vid = ctx
            .span(&id)
            .unwrap()
            .extensions()
            .get::<VenatorId>()
            .copied()
            .unwrap();

        self.send(&Message::from_close(&vid));
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if event.metadata().target() == "venator" {
            return;
        }

        self.send(&Message::from_event(event, &ctx));
    }

    fn on_follows_from(&self, span: &Id, follows: &Id, ctx: Context<'_, S>) {
        let vid = ctx
            .span(span)
            .unwrap()
            .extensions()
            .get::<VenatorId>()
            .copied()
            .unwrap();

        let follows_vid = ctx
            .span(follows)
            .unwrap()
            .extensions()
            .get::<VenatorId>()
            .copied()
            .unwrap();

        self.send(&Message::from_follows(&vid, &follows_vid));
    }

    fn on_id_change(&self, _old: &Id, _new: &Id, _ctx: Context<'_, S>) {
        // we do not handle this because we generate our own ids
    }
}

struct Connection {
    host: Option<String>,
    fields: BTreeMap<String, OwnedValue>,
    stream: Option<TcpStream>,
    last_connect_attempt: Instant,
}

impl Connection {
    fn new(host: Option<String>, fields: BTreeMap<String, OwnedValue>) -> Connection {
        Connection {
            host,
            fields,
            stream: None,
            last_connect_attempt: Instant::now() - Duration::from_secs(10),
        }
    }

    fn connect(&mut self) {
        self.last_connect_attempt = Instant::now();

        let host = self.host.as_deref().unwrap_or("localhost:8362");

        let mut addrs = match host.to_socket_addrs() {
            Ok(addrs) => addrs,
            Err(err) => {
                error!(parent: None, "failed to connect: {err:?}");
                return;
            }
        };

        let addr = match addrs.next() {
            Some(addr) => addr,
            None => {
                error!(parent: None, "failed to connect: could not resolve to any addresses");
                return;
            }
        };

        let connect_result = TcpStream::connect_timeout(&addr, Duration::from_millis(100));
        let mut stream = match connect_result {
            Ok(stream) => stream,
            Err(err) => {
                error!(parent: None, "failed to connect: {err:?}");
                return;
            }
        };

        debug!(parent: None, "connected");

        let handshake = Handshake {
            fields: self.fields.clone(),
        };

        let mut buffer = vec![];

        if let Err(err) = messaging::encode(&mut buffer, &handshake) {
            error!(parent: None, "failed to encode handshake: {err:?}");
            return;
        };

        if let Err(err) = stream.write_all(&buffer) {
            error!(parent: None, "failed to send handshake: {err:?}");
            return;
        }

        self.stream = Some(stream);
    }

    fn send(&mut self, payload: &[u8]) {
        if let Some(ref mut stream) = self.stream {
            let result = stream.write_all(payload);

            if let Err(err) = result {
                error!(parent: None, "failed to send payload: {err:?}");

                self.stream = None;
            }
        } else if self.last_connect_attempt.elapsed() >= Duration::from_secs(5) {
            self.connect();
            self.send(payload);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use tracing::Level;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    #[test]
    fn what_is_name_on_event_metadata() {
        tracing_subscriber::registry::Registry::default()
            .with(Venator::builder().build())
            .init();

        let span = tracing::span!(Level::WARN, "testsdadsad");
        let _entered = span.entered();
        tracing::info!("heehoo peanut!");
        drop(_entered);
        std::thread::sleep(Duration::from_millis(200));
    }
}
