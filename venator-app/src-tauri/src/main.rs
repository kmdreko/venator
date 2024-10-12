// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Mutex;

use clap::Parser;
use ingress::Ingress;
use serde::{Deserialize, Serialize};
use tauri::ipc::Channel;
use tauri::menu::{MenuBuilder, MenuItem, PredefinedMenuItem, Submenu};
use tauri::{Emitter, State};
use venator_engine::{
    BasicEventFilter, BasicInstanceFilter, BasicSpanFilter, DeleteFilter, DeleteMetrics, Engine,
    EventView, FileStorage, FilterPredicate, FilterPropertyKind, InstanceView, Order, Query,
    SpanView, StatsView, SubscriptionId, Timestamp, TransientStorage, ValuePredicate,
};

mod ingress;

#[tauri::command]
async fn get_instances(
    engine: State<'_, Engine>,
    filter: Vec<FilterPredicate>,
    order: Order,
    previous: Option<Timestamp>,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
) -> Result<Vec<InstanceView>, ()> {
    let events = engine
        .query_instance(Query {
            filter,
            order,
            limit: 50,
            start: start.unwrap_or(Timestamp::MIN),
            end: end.unwrap_or(Timestamp::MAX),
            previous,
        })
        .await;

    Ok(events)
}

#[tauri::command]
async fn get_instance_count(
    engine: State<'_, Engine>,
    filter: Vec<FilterPredicate>,
    start: Timestamp,
    end: Timestamp,
) -> Result<usize, ()> {
    let instances = engine
        .query_instance_count(Query {
            filter,
            order: Order::Asc, // this doesn't matter
            limit: 20,         // this doesn't matter
            start,
            end,
            previous: None,
        })
        .await;

    Ok(instances)
}

#[tauri::command]
async fn parse_instance_filter(
    _engine: State<'_, Engine>,
    filter: &str,
) -> Result<Vec<InputView>, ()> {
    match FilterPredicate::parse(filter) {
        Ok(predicates) => Ok(predicates
            .into_iter()
            .map(|p| {
                let text = p.to_string();
                let result = match BasicInstanceFilter::validate(p) {
                    Ok(predicate) => {
                        FilterPredicateResultView::Valid(FilterPredicateView::from(predicate))
                    }
                    Err(err) => FilterPredicateResultView::Invalid {
                        error: err.to_string(),
                    },
                };

                InputView { text, result }
            })
            .collect()),
        Err(err) => Ok(vec![InputView {
            text: filter.to_owned(),
            result: FilterPredicateResultView::Invalid {
                error: err.to_string(),
            },
        }]),
    }
}

#[tauri::command]
async fn get_events(
    engine: State<'_, Engine>,
    filter: Vec<FilterPredicate>,
    order: Order,
    previous: Option<Timestamp>,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
) -> Result<Vec<EventView>, ()> {
    let events = engine
        .query_event(Query {
            filter,
            order,
            limit: 50,
            start: start.unwrap_or(Timestamp::MIN),
            end: end.unwrap_or(Timestamp::MAX),
            previous,
        })
        .await;

    Ok(events)
}

#[tauri::command]
async fn get_event_count(
    engine: State<'_, Engine>,
    filter: Vec<FilterPredicate>,
    start: Timestamp,
    end: Timestamp,
) -> Result<usize, ()> {
    let events = engine
        .query_event_count(Query {
            filter,
            order: Order::Asc, // this doesn't matter
            limit: 20,         // this doesn't matter
            start,
            end,
            previous: None,
        })
        .await;

    Ok(events)
}

#[tauri::command]
async fn parse_event_filter(
    _engine: State<'_, Engine>,
    filter: &str,
) -> Result<Vec<InputView>, ()> {
    match FilterPredicate::parse(filter) {
        Ok(predicates) => Ok(predicates
            .into_iter()
            .map(|p| {
                let text = p.to_string();
                let result = match BasicEventFilter::validate(p) {
                    Ok(predicate) => {
                        FilterPredicateResultView::Valid(FilterPredicateView::from(predicate))
                    }
                    Err(err) => FilterPredicateResultView::Invalid {
                        error: err.to_string(),
                    },
                };

                InputView { text, result }
            })
            .collect()),
        Err(err) => Ok(vec![InputView {
            text: filter.to_owned(),
            result: FilterPredicateResultView::Invalid {
                error: err.to_string(),
            },
        }]),
    }
}

#[tauri::command]
async fn get_spans(
    engine: State<'_, Engine>,
    filter: Vec<FilterPredicate>,
    order: Order,
    previous: Option<Timestamp>,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
) -> Result<Vec<SpanView>, ()> {
    let spans = engine
        .query_span(Query {
            filter,
            order,
            limit: 50,
            start: start.unwrap_or(Timestamp::MIN),
            end: end.unwrap_or(Timestamp::MAX),
            previous,
        })
        .await;

    Ok(spans)
}

#[tauri::command]
async fn get_span_count(
    engine: State<'_, Engine>,
    filter: Vec<FilterPredicate>,
    start: Timestamp,
    end: Timestamp,
) -> Result<usize, ()> {
    let spans = engine
        .query_span_count(Query {
            filter,
            order: Order::Asc, // this doesn't matter
            limit: 20,         // this doesn't matter
            start,
            end,
            previous: None,
        })
        .await;

    Ok(spans)
}

#[tauri::command]
async fn parse_span_filter(_engine: State<'_, Engine>, filter: &str) -> Result<Vec<InputView>, ()> {
    match FilterPredicate::parse(filter) {
        Ok(predicates) => Ok(predicates
            .into_iter()
            .map(|p| {
                let text = p.to_string();
                let result = match BasicSpanFilter::validate(p) {
                    Ok(predicate) => {
                        FilterPredicateResultView::Valid(FilterPredicateView::from(predicate))
                    }
                    Err(err) => FilterPredicateResultView::Invalid {
                        error: err.to_string(),
                    },
                };

                InputView { text, result }
            })
            .collect()),
        Err(err) => Ok(vec![InputView {
            text: filter.to_owned(),
            result: FilterPredicateResultView::Invalid {
                error: err.to_string(),
            },
        }]),
    }
}

#[tauri::command]
async fn delete_entities(
    engine: State<'_, Engine>,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
    inside: bool,
    dry_run: bool,
) -> Result<DeleteMetricsView, ()> {
    let metrics = engine
        .delete(DeleteFilter {
            start: start.unwrap_or(Timestamp::MIN),
            end: end.unwrap_or(Timestamp::MAX),
            inside,
            dry_run,
        })
        .await;

    Ok(metrics.into())
}

#[tauri::command]
async fn get_stats(engine: State<'_, Engine>) -> Result<StatsView, ()> {
    Ok(engine.query_stats().await)
}

#[tauri::command]
async fn subscribe_to_events(
    engine: State<'_, Engine>,
    filter: Vec<FilterPredicate>,
    channel: Channel<EventView>,
) -> Result<SubscriptionId, String> {
    let (id, mut receiver) = engine.subscribe_to_events(filter).await;

    tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            let _ = channel.send(event);
        }
    });

    Ok(id)
}

#[tauri::command]
async fn unsubscribe_from_events(
    engine: State<'_, Engine>,
    id: SubscriptionId,
) -> Result<(), String> {
    engine.unsubscribe_from_events(id).await;

    Ok(())
}

#[tauri::command]
fn create_attribute_index(engine: State<'_, Engine>, name: String) {
    engine.add_attribute_index(name)
}

#[tauri::command]
async fn get_status(
    engine: State<'_, Engine>,
    dataset: State<'_, DatasetConfig>,
    ingress: State<'_, Mutex<Option<Ingress>>>,
) -> Result<StatusView, String> {
    let ((ingress_message, ingress_error), (connections, bytes_per_second)) =
        match &mut *ingress.lock().unwrap() {
            Some(ingress) => (ingress.status(), ingress.stats()),
            None => (("not listening".into(), None), (0, 0.0)),
        };

    let dataset_message = match &*dataset {
        DatasetConfig::Default(_) => "using default dataset".to_owned(),
        DatasetConfig::File(path) => format!("using {}", path.display()),
        DatasetConfig::Memory => "using :memory:".to_owned(),
    };

    let engine_status = engine.get_status().await;

    Ok(StatusView {
        ingress_message,
        ingress_error,
        dataset_message,
        ingress_connections: connections,
        ingress_bytes_per_second: bytes_per_second,
        engine_load: engine_status.load,
    })
}

enum DatasetConfig {
    Default(PathBuf),
    File(PathBuf),
    Memory,
}

impl DatasetConfig {
    fn prepare(&self) {
        match self {
            DatasetConfig::Memory => { /* nothing to do */ }
            DatasetConfig::Default(path) | DatasetConfig::File(path) => {
                if let Some(dir) = path.parent() {
                    std::fs::create_dir_all(dir).unwrap();
                }
            }
        }
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The file (or :memory:) that holds captured traces
    #[arg(short, long)]
    dataset: Option<String>,

    /// The bind address to accept traces from
    #[arg(short, long)]
    bind: Option<String>,
}

impl Args {
    fn dataset(&self) -> DatasetConfig {
        if let Some(dataset) = &self.dataset {
            if dataset == ":memory:" {
                return DatasetConfig::Memory;
            } else {
                return DatasetConfig::File(PathBuf::from(dataset));
            }
        }

        if cfg!(debug_assertions) {
            DatasetConfig::Default(PathBuf::from("local.db"))
        } else {
            DatasetConfig::Default(
                directories::ProjectDirs::from("", "", "Venator")
                    .map(|dirs| dirs.data_dir().to_path_buf().join("local.db"))
                    .unwrap_or(PathBuf::from("local.db")),
            )
        }
    }

    fn bind(&self) -> Option<&str> {
        // if there is a bind address, use it - otherwise only use the default
        // if also using the default dataset

        if let Some(bind) = &self.bind {
            return Some(bind);
        }

        if self.dataset.is_some() {
            None
        } else {
            Some("0.0.0.0:8362")
        }
    }
}

fn main() {
    let args = Args::parse();
    let dataset = args.dataset();
    let bind = args.bind();

    dataset.prepare();
    let engine = match &dataset {
        DatasetConfig::Default(path) => Engine::new(FileStorage::new(path)),
        DatasetConfig::File(path) => Engine::new(FileStorage::new(path)),
        DatasetConfig::Memory => Engine::new(TransientStorage::new()),
    };

    let ingress = bind.map(|bind| Ingress::start(bind.to_owned(), engine.clone()));

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_clipboard_manager::init())
        .setup(|app| {
            let handle = app.handle();
            let menu = MenuBuilder::new(handle)
                .item(&Submenu::with_items(
                    handle,
                    "File",
                    true,
                    &[
                        &MenuItem::with_id(
                            handle,
                            "open-dataset",
                            "Open dataset",
                            true,
                            None::<&str>,
                        )?,
                        &MenuItem::new(handle, "Open dataset in new window", true, None::<&str>)?,
                        &PredefinedMenuItem::separator(handle)?,
                        &MenuItem::new(handle, "Save", true, None::<&str>)?,
                        &MenuItem::new(handle, "Save as", true, None::<&str>)?,
                        &MenuItem::new(handle, "Export view as CSV", true, None::<&str>)?,
                        &MenuItem::new(handle, "Export view as ...", false, None::<&str>)?,
                        &PredefinedMenuItem::separator(handle)?,
                        &MenuItem::new(handle, "Exit", true, None::<&str>)?,
                    ],
                )?)
                .item(&Submenu::with_items(
                    handle,
                    "Edit",
                    true,
                    &[
                        &MenuItem::new(handle, "Undo", true, None::<&str>)?,
                        &MenuItem::new(handle, "Redo", true, None::<&str>)?,
                        &PredefinedMenuItem::separator(handle)?,
                        &MenuItem::new(handle, "Cut filter", true, None::<&str>)?,
                        &MenuItem::new(handle, "Copy filter", true, None::<&str>)?,
                        &MenuItem::new(handle, "Paste filter", true, None::<&str>)?,
                        &MenuItem::new(handle, "Go to filter", true, None::<&str>)?,
                    ],
                )?)
                .item(&Submenu::with_items(
                    handle,
                    "View",
                    true,
                    &[
                        &MenuItem::new(handle, "New tab", true, None::<&str>)?,
                        &MenuItem::new(handle, "Duplicate tab", true, None::<&str>)?,
                        &MenuItem::new(handle, "Close all tabs", true, None::<&str>)?,
                        &PredefinedMenuItem::separator(handle)?,
                        &MenuItem::new(handle, "Focus", true, None::<&str>)?,
                        &MenuItem::new(handle, "Focus all", true, None::<&str>)?,
                        &PredefinedMenuItem::separator(handle)?,
                        &MenuItem::new(handle, "Go to start", true, None::<&str>)?,
                        &MenuItem::new(handle, "Go to end", true, None::<&str>)?,
                        &PredefinedMenuItem::separator(handle)?,
                        &MenuItem::new(handle, "Zoom in timeframe", true, None::<&str>)?,
                        &MenuItem::new(handle, "Zoom out timeframe", true, None::<&str>)?,
                        &PredefinedMenuItem::separator(handle)?,
                        &MenuItem::new(handle, "Theme", true, None::<&str>)?,
                    ],
                )?)
                .item(&Submenu::with_items(
                    handle,
                    "Data",
                    true,
                    &[
                        &MenuItem::with_id(handle, "delete-all", "Delete all", true, None::<&str>)?,
                        &MenuItem::with_id(
                            handle,
                            "delete-inside",
                            "Delete from timeframe",
                            true,
                            None::<&str>,
                        )?,
                        &MenuItem::with_id(
                            handle,
                            "delete-outside",
                            "Delete outside timeframe",
                            true,
                            None::<&str>,
                        )?,
                        &PredefinedMenuItem::separator(handle)?,
                        &MenuItem::new(handle, "Manage indexes", false, None::<&str>)?,
                    ],
                )?)
                .item(&Submenu::with_items(
                    handle,
                    "Help",
                    true,
                    &[
                        &MenuItem::new(handle, "About Venator", true, None::<&str>)?,
                        &MenuItem::new(handle, "Documentation", true, None::<&str>)?,
                        &MenuItem::new(handle, "Report an issue", true, None::<&str>)?,
                    ],
                )?)
                .build()?;
            app.set_menu(menu)?;
            app.on_menu_event(|app, event| match event.id().as_ref() {
                "open-dataset" => {
                    use tauri_plugin_dialog::DialogExt;

                    let Ok(current_exe) = std::env::current_exe() else {
                        return;
                    };

                    app.dialog().file().pick_file(move |file_path| {
                        let Some(path) = file_path else { return };
                        Command::new(current_exe)
                            .arg("-d")
                            .arg(path.as_path().unwrap())
                            .stdin(Stdio::null())
                            .stdout(Stdio::null())
                            .stderr(Stdio::null())
                            .spawn()
                            .unwrap();
                    });
                }
                "delete-all" => {
                    app.emit("delete-all-clicked", ()).unwrap();
                }
                "delete-inside" => {
                    app.emit("delete-inside-clicked", ()).unwrap();
                }
                "delete-outside" => {
                    app.emit("delete-outside-clicked", ()).unwrap();
                }
                _ => {}
            });
            Ok(())
        })
        .manage(engine)
        .manage(dataset)
        .manage(Mutex::new(ingress))
        .invoke_handler(tauri::generate_handler![
            get_instances,
            get_instance_count,
            parse_instance_filter,
            get_events,
            get_event_count,
            parse_event_filter,
            get_spans,
            get_span_count,
            parse_span_filter,
            delete_entities,
            get_stats,
            subscribe_to_events,
            unsubscribe_from_events,
            create_attribute_index,
            get_status,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[derive(Clone, Serialize, Deserialize)]
struct InputView {
    text: String,
    #[serde(flatten)]
    result: FilterPredicateResultView,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "input", rename_all = "camelCase")]
enum FilterPredicateResultView {
    Valid(FilterPredicateView),
    Invalid { error: String },
}

#[derive(Clone, Serialize, Deserialize)]
struct FilterPredicateView {
    text: String,
    property_kind: Option<FilterPropertyKind>,
    property: String,
    #[serde(flatten)]
    value: ValuePredicate,
}

impl From<FilterPredicate> for FilterPredicateView {
    fn from(inner: FilterPredicate) -> FilterPredicateView {
        FilterPredicateView {
            text: inner.to_string(),
            property_kind: inner.property_kind,
            property: inner.property,
            value: inner.value,
        }
    }
}

#[derive(Serialize)]
struct StatusView {
    ingress_message: String,
    ingress_error: Option<String>,
    ingress_connections: usize,
    ingress_bytes_per_second: f64,
    dataset_message: String,
    engine_load: f64,
}

#[derive(Serialize)]
pub struct DeleteMetricsView {
    instances: usize,
    spans: usize,
    span_events: usize,
    events: usize,
}

impl From<DeleteMetrics> for DeleteMetricsView {
    fn from(metrics: DeleteMetrics) -> Self {
        DeleteMetricsView {
            instances: metrics.instances,
            spans: metrics.spans,
            span_events: metrics.span_events,
            events: metrics.events,
        }
    }
}
