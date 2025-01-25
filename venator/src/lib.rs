#![doc = include_str!("../README.md")]

use std::cell::Cell;
use std::collections::BTreeMap;
use std::hash::{BuildHasher, RandomState};
use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tracing::span::{Attributes, Id, Record};
use tracing::{debug, error, Event, Subscriber, Value};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

mod attributes;
mod ids;
mod messaging;

use attributes::OwnedValue;
use ids::VenatorId;
use messaging::{Handshake, Message};

/// This is a builder for configuring a [`Venator`] layer. Use [`.build()`](VenatorBuilder::build)
/// to finalize.
pub struct VenatorBuilder {
    id: u128,
    host: Option<String>,
    emit_enter_events: bool,
    attributes: BTreeMap<String, OwnedValue>,
}

impl VenatorBuilder {
    /// This will set the host address of the `Venator` layer used to connect to
    /// the Venator app.
    ///
    /// Setting the host again will overwrite the previous value. The default is
    /// `"127.0.0.1:8362"`.
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

    /// This configures whether the layer emits `on_enter` and `on_exit` events
    /// to Venator.
    ///
    /// This may not be desired if your case is entirely synchronous (thus
    /// "enter" and "exit" are redundant with "create" and "close") or if  your
    /// case is asynchronous and don't need those events (also used for "busy"
    /// metric) and need to reduce the load.
    ///
    /// Setting this again will overwrite the previous value. The default is
    /// `true`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use venator::Venator;
    /// let venator_layer = Venator::builder()
    ///     .with_enter_events(false)
    ///     .build()
    ///     .install();
    /// ```
    pub fn with_enter_events(mut self, emit: bool) -> VenatorBuilder {
        self.emit_enter_events = emit;
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
            self.attributes.insert(attribute.into(), value);
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
        let connection = Connection::new(self.id, self.host, self.attributes);

        Venator {
            emit_enter_events: self.emit_enter_events,
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
    emit_enter_events: bool,
    connection: Mutex<Connection>,
}

impl Venator {
    /// This creates a builder for a `Venator` layer for configuring the host
    /// and/or attributes.
    pub fn builder() -> VenatorBuilder {
        let s = RandomState::new();
        let a = s.hash_one(0x0f0f0f0f0f0f0f0fu64);
        let b = s.hash_one(0x5555555555555555u64);

        VenatorBuilder {
            id: a as u128 + ((b as u128) << 64),
            host: None,
            emit_enter_events: true,
            attributes: BTreeMap::new(),
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
        thread_local! { static SCRATCH_MESSAGE_BUFFER: Cell<Vec<u8>> = const { Cell::new(Vec::new()) }; }
        thread_local! { static SCRATCH_CHUNK_BUFFER: Cell<Vec<u8>> = const { Cell::new(Vec::new()) }; }

        let mut message_buffer = SCRATCH_MESSAGE_BUFFER.with(|b| b.take());
        if let Err(err) = messaging::encode_message(&mut message_buffer, &message) {
            error!(parent: None, "failed to encode message: {err:?}");
            return;
        };

        let mut chunk_buffer = SCRATCH_CHUNK_BUFFER.with(|b| b.take());
        if let Err(err) = messaging::encode_chunk(&mut chunk_buffer, &message_buffer) {
            error!(parent: None, "failed to encode message chunk: {err:?}");
            return;
        };

        self.connection
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .send(&chunk_buffer);

        SCRATCH_MESSAGE_BUFFER.with(|b| b.set(message_buffer));
        SCRATCH_CHUNK_BUFFER.with(|b| b.set(chunk_buffer));
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
        let Some(span) = ctx.span(id) else {
            // this should not happen
            return;
        };

        span.extensions_mut().insert(vid);

        self.send(&Message::from_new_span(attrs, &vid, &ctx));
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            // this should not happen
            return;
        };

        let Some(&vid) = span.extensions().get::<VenatorId>() else {
            // this should not happen since we insert it on every `on_new_span`
            return;
        };

        self.send(&Message::from_record(&vid, values));
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        if !self.emit_enter_events {
            return;
        }

        let Some(span) = ctx.span(id) else {
            // this should not happen
            return;
        };

        let Some(&vid) = span.extensions().get::<VenatorId>() else {
            // this should not happen since we insert it on every `on_new_span`
            return;
        };

        self.send(&Message::from_enter(&vid));
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        if !self.emit_enter_events {
            return;
        }

        let Some(span) = ctx.span(id) else {
            // this should not happen
            return;
        };

        let Some(&vid) = span.extensions().get::<VenatorId>() else {
            // this should not happen since we insert it on every `on_new_span`
            return;
        };

        self.send(&Message::from_exit(&vid));
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            // this should not happen
            return;
        };

        let Some(&vid) = span.extensions().get::<VenatorId>() else {
            // this should not happen since we insert it on every `on_new_span`
            return;
        };

        self.send(&Message::from_close(&vid));
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if event.metadata().target() == "venator" {
            return;
        }

        self.send(&Message::from_event(event, &ctx));
    }

    fn on_follows_from(&self, span: &Id, follows: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(span) else {
            // this should not happen
            return;
        };

        let Some(&vid) = span.extensions().get::<VenatorId>() else {
            // this should not happen since we insert it on every `on_new_span`
            return;
        };

        let Some(follows_span) = ctx.span(follows) else {
            // this should not happen
            return;
        };

        let Some(&follows_vid) = follows_span.extensions().get::<VenatorId>() else {
            // this should not happen since we insert it on every `on_new_span`
            return;
        };

        self.send(&Message::from_follows(&vid, &follows_vid));
    }

    fn on_id_change(&self, _old: &Id, _new: &Id, _ctx: Context<'_, S>) {
        // we do not handle this because we generate our own ids
    }
}

struct Connection {
    id: u128,
    host: Option<String>,
    attributes: BTreeMap<String, OwnedValue>,
    stream: Option<TcpStream>,
    last_connect_attempt: Instant,
}

impl Connection {
    fn new(id: u128, host: Option<String>, attributes: BTreeMap<String, OwnedValue>) -> Connection {
        Connection {
            id,
            host,
            attributes,
            stream: None,
            last_connect_attempt: Instant::now() - Duration::from_secs(10),
        }
    }

    fn connect(&mut self) {
        self.last_connect_attempt = Instant::now();

        let host = self.host.as_deref().unwrap_or("127.0.0.1:8362");
        let id = self.id;

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

        let header_result = write!(stream, "POST /tracing/v1 HTTP/1.1\r\nHost: {host}\r\nTransfer-Encoding: chunked\r\nInstance-Id: {id:032x}\r\n\r\n");
        if let Err(err) = header_result {
            error!(parent: None, "failed to write header: {err:?}");
            return;
        }

        let handshake = Handshake {
            attributes: self.attributes.clone(),
        };

        let mut message_buffer = vec![];
        if let Err(err) = messaging::encode_message(&mut message_buffer, &handshake) {
            error!(parent: None, "failed to encode handshake message: {err:?}");
            return;
        }

        let mut chunk_buffer = vec![];
        if let Err(err) = messaging::encode_chunk(&mut chunk_buffer, &message_buffer) {
            error!(parent: None, "failed to encode handshake chunk: {err:?}");
            return;
        }

        if let Err(err) = stream.write_all(&chunk_buffer) {
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
