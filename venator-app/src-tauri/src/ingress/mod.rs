use std::collections::HashSet;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::Instant;

use axum::body::{Bytes, HttpBody};
use axum::extract::{Request, State};
use axum::middleware::{from_fn_with_state, Next};
use axum::response::Response;
use axum::routing::post;
use http_body::Frame;
use tokio::net::TcpListener;
use tonic::service::Routes;

use venator_engine::Engine;

mod otel;
mod tracing;

pub(crate) struct IngressState {
    bind: String,
    error: OnceLock<String>,

    engine: Engine,

    last_check: Mutex<Instant>,
    num_connections: AtomicUsize,
    num_bytes: AtomicUsize,

    // Only one tracing instance for a given ID is allowed at a time, so this
    // keeps track of those that are connected.
    tracing_instances: Mutex<HashSet<u128>>,
}

impl IngressState {
    fn new(engine: Engine, bind: String) -> IngressState {
        IngressState {
            bind,
            error: OnceLock::new(),
            engine,
            last_check: Mutex::new(Instant::now()),
            num_connections: AtomicUsize::new(0),
            num_bytes: AtomicUsize::new(0),
            tracing_instances: Mutex::new(HashSet::new()),
        }
    }

    fn set_error(&self, error: String) {
        let _ = self.error.set(error);
    }

    pub(crate) fn get_status(&self) -> (String, Option<String>) {
        if let Some(err) = self.error.get() {
            let msg = format!("not listening on {}", self.bind);
            let err = err.to_string();

            (msg, Some(err))
        } else {
            let msg = format!("listening on {}", self.bind);

            (msg, None)
        }
    }

    pub(crate) fn get_and_reset_metrics(&self) -> (usize, usize, f64) {
        let now = Instant::now();
        let last = std::mem::replace(&mut *self.last_check.lock().unwrap(), now);
        let elapsed = (now - last).as_secs_f64();

        let num_connections = self.num_connections.load(Ordering::Relaxed);
        let num_bytes = self.num_bytes.swap(0, Ordering::Relaxed);

        (num_connections, num_bytes, elapsed)
    }
}

struct IngressBody<B> {
    state: Arc<IngressState>,
    inner: B,
}

impl<B> HttpBody for IngressBody<B>
where
    B: HttpBody<Data = Bytes> + Unpin,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let res = Pin::new(&mut self.inner).poll_frame(ctx);

        if let Poll::Ready(Some(Ok(ref frame))) = &res {
            if let Some(data) = frame.data_ref() {
                self.state
                    .num_bytes
                    .fetch_add(data.len(), Ordering::Relaxed);
            }
        }

        res
    }
}

async fn ingress_middleware(
    State(state): State<Arc<IngressState>>,
    request: Request,
    next: Next,
) -> Response {
    state.num_connections.fetch_add(1, Ordering::Relaxed);
    let response = next.run(request).await;
    state.num_connections.fetch_sub(1, Ordering::Relaxed);
    response
}

pub fn launch_ingress_thread(engine: Engine, bind: String) -> Arc<IngressState> {
    #[tokio::main(flavor = "current_thread")]
    async fn ingress_task(state: Arc<IngressState>) {
        let listener = match TcpListener::bind(&state.bind).await {
            Ok(listener) => listener,
            Err(err) => {
                state.set_error(format!("failed to listen on bind port: {err}"));
                return;
            }
        };

        let routes = Routes::default()
            .add_service(otel::logs_service(state.engine.clone()))
            .add_service(otel::metrics_service(state.engine.clone()))
            .add_service(otel::trace_service(state.engine.clone()))
            .into_axum_router()
            .with_state(())
            .route("/tracing/v1", post(self::tracing::post_tracing_handler))
            .route("/v1/logs", post(self::otel::post_otel_logs_handler))
            .route("/v1/metrics", post(self::otel::post_otel_metrics_handler))
            .route("/v1/trace", post(self::otel::post_otel_trace_handler))
            .layer(from_fn_with_state(state.clone(), ingress_middleware))
            .with_state(state.clone());

        match axum::serve(listener, routes).await {
            Ok(_) => {
                state.set_error("failed to serve: Exit".to_owned());
                return;
            }
            Err(err) => {
                state.set_error(format!("failed to serve: {err}"));
                return;
            }
        };
    }

    let state = Arc::new(IngressState::new(engine, bind));

    std::thread::spawn({
        let state = state.clone();
        || ingress_task(state)
    });

    state
}
