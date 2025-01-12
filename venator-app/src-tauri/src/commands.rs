use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::sync::Arc;

use tauri::ipc::{Channel, Invoke};
use tauri::{Runtime, State};
use venator_engine::engine::AsyncEngine;
use venator_engine::filter::{
    validate_event_filter, validate_span_filter, FilterPredicate, Order, Query,
};
use venator_engine::{DeleteFilter, SubscriptionId, SubscriptionResponse, Timestamp};

use crate::views::{
    DatasetStatsView, DeleteMetricsView, EventView, FilterPredicateResultView, InputView, Session,
    SpanView, StatusView, SubscriptionResponseView,
};
use crate::{DatasetConfig, IngressState, SessionPersistence};

#[tauri::command]
async fn get_events(
    engine: State<'_, AsyncEngine>,
    filter: Vec<FilterPredicate>,
    order: Order,
    previous: Option<Timestamp>,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
) -> Result<Vec<EventView>, String> {
    let events = engine
        .query_event(Query {
            filter,
            order,
            limit: 50,
            start: start.unwrap_or(Timestamp::MIN),
            end: end.unwrap_or(Timestamp::MAX),
            previous,
        })
        .await
        .map_err(|e| e.to_string())?;

    Ok(events.into_iter().map(EventView::from).collect())
}

#[tauri::command]
async fn get_event_count(
    engine: State<'_, AsyncEngine>,
    filter: Vec<FilterPredicate>,
    start: Timestamp,
    end: Timestamp,
) -> Result<usize, String> {
    let events = engine
        .query_event_count(Query {
            filter,
            order: Order::Asc, // this doesn't matter
            limit: 20,         // this doesn't matter
            start,
            end,
            previous: None,
        })
        .await
        .map_err(|e| e.to_string())?;

    Ok(events)
}

#[tauri::command]
async fn parse_event_filter(
    _engine: State<'_, AsyncEngine>,
    filter: &str,
) -> Result<Vec<InputView>, ()> {
    match FilterPredicate::parse(filter) {
        Ok(predicates) => Ok(predicates
            .into_iter()
            .map(|p| {
                let text = p.to_string();
                InputView::from(validate_event_filter(p).map_err(|e| (e, text)))
            })
            .collect()),
        Err(err) => Ok(vec![InputView {
            result: FilterPredicateResultView::Invalid {
                text: filter.to_owned(),
                error: err.to_string(),
            },
        }]),
    }
}

#[tauri::command]
async fn get_spans(
    engine: State<'_, AsyncEngine>,
    filter: Vec<FilterPredicate>,
    order: Order,
    previous: Option<Timestamp>,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
) -> Result<Vec<SpanView>, String> {
    let spans = engine
        .query_span(Query {
            filter,
            order,
            limit: 50,
            start: start.unwrap_or(Timestamp::MIN),
            end: end.unwrap_or(Timestamp::MAX),
            previous,
        })
        .await
        .map_err(|e| e.to_string())?;

    Ok(spans.into_iter().map(SpanView::from).collect())
}

#[tauri::command]
async fn get_span_count(
    engine: State<'_, AsyncEngine>,
    filter: Vec<FilterPredicate>,
    start: Timestamp,
    end: Timestamp,
) -> Result<usize, String> {
    let spans = engine
        .query_span_count(Query {
            filter,
            order: Order::Asc, // this doesn't matter
            limit: 20,         // this doesn't matter
            start,
            end,
            previous: None,
        })
        .await
        .map_err(|e| e.to_string())?;

    Ok(spans)
}

#[tauri::command]
async fn parse_span_filter(
    _engine: State<'_, AsyncEngine>,
    filter: &str,
) -> Result<Vec<InputView>, ()> {
    match FilterPredicate::parse(filter) {
        Ok(predicates) => Ok(predicates
            .into_iter()
            .map(|p| {
                let text = p.to_string();
                InputView::from(validate_span_filter(p).map_err(|e| (e, text)))
            })
            .collect()),
        Err(err) => Ok(vec![InputView {
            result: FilterPredicateResultView::Invalid {
                text: filter.to_owned(),
                error: err.to_string(),
            },
        }]),
    }
}

#[tauri::command]
async fn delete_entities(
    engine: State<'_, AsyncEngine>,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
    inside: bool,
    dry_run: bool,
) -> Result<DeleteMetricsView, String> {
    let metrics = engine
        .delete(DeleteFilter {
            start: start.unwrap_or(Timestamp::MIN),
            end: end.unwrap_or(Timestamp::MAX),
            inside,
            dry_run,
        })
        .await
        .map_err(|e| e.to_string())?;

    Ok(metrics.into())
}

#[tauri::command]
async fn get_stats(engine: State<'_, AsyncEngine>) -> Result<DatasetStatsView, String> {
    engine
        .query_stats()
        .await
        .map_err(|e| e.to_string())
        .map(|s| s.into())
}

#[tauri::command]
async fn subscribe_to_spans(
    engine: State<'_, AsyncEngine>,
    filter: Vec<FilterPredicate>,
    channel: Channel<SubscriptionResponseView<SpanView>>,
) -> Result<SubscriptionId, String> {
    let (id, mut receiver) = engine
        .subscribe_to_spans(filter)
        .await
        .map_err(|e| e.to_string())?;

    tokio::spawn(async move {
        while let Some(response) = receiver.recv().await {
            let response = match response {
                SubscriptionResponse::Add(span) => {
                    SubscriptionResponseView::Add(SpanView::from(span))
                }
                SubscriptionResponse::Remove(span_key) => {
                    SubscriptionResponseView::Remove(span_key)
                }
            };
            let _ = channel.send(response);
        }
    });

    Ok(id)
}

#[tauri::command]
async fn unsubscribe_from_spans(
    engine: State<'_, AsyncEngine>,
    id: SubscriptionId,
) -> Result<(), String> {
    engine
        .unsubscribe_from_spans(id)
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}

#[tauri::command]
async fn subscribe_to_events(
    engine: State<'_, AsyncEngine>,
    filter: Vec<FilterPredicate>,
    channel: Channel<SubscriptionResponseView<EventView>>,
) -> Result<SubscriptionId, String> {
    let (id, mut receiver) = engine
        .subscribe_to_events(filter)
        .await
        .map_err(|e| e.to_string())?;

    tokio::spawn(async move {
        while let Some(response) = receiver.recv().await {
            let response = match response {
                SubscriptionResponse::Add(event) => {
                    SubscriptionResponseView::Add(EventView::from(event))
                }
                SubscriptionResponse::Remove(event_key) => {
                    SubscriptionResponseView::Remove(event_key)
                }
            };
            let _ = channel.send(response);
        }
    });

    Ok(id)
}

#[tauri::command]
async fn unsubscribe_from_events(
    engine: State<'_, AsyncEngine>,
    id: SubscriptionId,
) -> Result<(), String> {
    engine
        .unsubscribe_from_events(id)
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}

#[tauri::command]
async fn load_session(persist_session: State<'_, SessionPersistence>) -> Result<Session, String> {
    if let SessionPersistence(Some(session_path)) = &*persist_session {
        let session_file = File::open(session_path).map_err(|err| err.to_string())?;
        let session_file = BufReader::new(session_file);
        let session = serde_json::from_reader(session_file).map_err(|err| err.to_string())?;
        Ok(session)
    } else {
        Ok(Session::default())
    }
}

#[tauri::command]
async fn save_session(
    persistence: State<'_, SessionPersistence>,
    session: Session,
) -> Result<(), String> {
    if let SessionPersistence(Some(session_path)) = &*persistence {
        let session_file = File::create(session_path).map_err(|err| err.to_string())?;
        let session_file = BufWriter::new(session_file);
        serde_json::to_writer(session_file, &session).map_err(|err| err.to_string())?;
        Ok(())
    } else {
        Ok(())
    }
}

#[tauri::command]
async fn get_status(
    engine: State<'_, AsyncEngine>,
    dataset: State<'_, DatasetConfig>,
    ingress: State<'_, Option<Arc<IngressState>>>,
) -> Result<StatusView, String> {
    let ((ingress_message, ingress_error), (connections, bytes_per_second)) = match &*ingress {
        Some(ingress) => {
            let status = ingress.get_status();
            let (connections, bytes, seconds) = ingress.get_and_reset_metrics();

            (status, (connections, bytes as f64 / seconds))
        }
        None => (("not listening".into(), None), (0, 0.0)),
    };

    let dataset_name = match &*dataset {
        DatasetConfig::Default(_) => "default dataset".to_owned(),
        DatasetConfig::File(path) => format!("{}", path.display()),
        DatasetConfig::Memory => ":memory:".to_owned(),
    };

    let engine_status = engine.get_status().await.map_err(|e| e.to_string())?;

    Ok(StatusView {
        ingress_message,
        ingress_error,
        dataset_name,
        ingress_connections: connections,
        ingress_bytes_per_second: bytes_per_second,
        engine_load: engine_status.load,
    })
}

pub(crate) fn handler<R: Runtime>() -> impl Fn(Invoke<R>) -> bool + Send + Sync + 'static {
    tauri::generate_handler![
        get_events,
        get_event_count,
        parse_event_filter,
        get_spans,
        get_span_count,
        parse_span_filter,
        delete_entities,
        get_stats,
        subscribe_to_spans,
        unsubscribe_from_spans,
        subscribe_to_events,
        unsubscribe_from_events,
        get_status,
        save_session,
        load_session,
    ]
}
