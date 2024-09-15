use std::cell::Cell;
use std::collections::BTreeMap;
use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bincode::{DefaultOptions, Error as BincodeError, Options};
use serde::Serialize;
use tracing::span::{Attributes, Id, Record};
use tracing::{debug, error, Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

pub mod ids;
pub mod messaging;

use ids::VenatorId;
use messaging::{Handshake, Message};

pub struct VenatorBuilder {
    host: Option<String>,
    fields: BTreeMap<String, String>,
}

impl VenatorBuilder {
    pub fn with_host(mut self, host: String) -> VenatorBuilder {
        self.host = Some(host);
        self
    }

    pub fn with_attribute<A: Into<String>, V: Into<String>>(
        mut self,
        attribute: A,
        value: V,
    ) -> VenatorBuilder {
        self.fields.insert(attribute.into(), value.into());
        self
    }

    pub fn build(self) -> Venator {
        let connection = Connection::new(self.host, self.fields);

        Venator {
            connection: Mutex::new(connection),
        }
    }
}

pub struct Venator {
    connection: Mutex<Connection>,
}

impl Venator {
    pub fn builder() -> VenatorBuilder {
        VenatorBuilder {
            host: None,
            fields: BTreeMap::new(),
        }
    }

    fn send(&self, message: &Message) {
        // this persists the space used for encoding the message in a thread
        // local to reduce per-call allocation costs
        thread_local! { static SCRATCH: Cell<Vec<u8>> = const { Cell::new(Vec::new()) }; }

        let mut buffer = SCRATCH.with(|b| b.take());

        if let Err(err) = encode(&mut buffer, &message) {
            error!(parent: None, "failed to encode message: {err:?}");
            return;
        };

        self.connection.lock().unwrap().send(&buffer);

        SCRATCH.with(|b| b.set(buffer));
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
    fields: BTreeMap<String, String>,
    stream: Option<TcpStream>,
    last_connect_attempt: Instant,
}

impl Connection {
    fn new(host: Option<String>, fields: BTreeMap<String, String>) -> Connection {
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

        if let Err(err) = encode(&mut buffer, &handshake) {
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

fn encode<T: Serialize>(buffer: &mut Vec<u8>, payload: &T) -> Result<(), BincodeError> {
    // this uses a two-byte length prefix followed by the bincode-ed payload

    buffer.resize(2, 0);

    DefaultOptions::new()
        .with_varint_encoding()
        .with_big_endian()
        .with_limit(u16::MAX as u64)
        .serialize_into(&mut *buffer, payload)?;

    let payload_size = buffer.len() - 2;
    let payload_size_bytes = (payload_size as u16).to_be_bytes();

    buffer[0..2].copy_from_slice(&payload_size_bytes);

    Ok(())
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
