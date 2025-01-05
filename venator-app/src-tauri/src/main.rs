// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;

use clap::{ArgAction, Parser};
use serde::{Deserialize, Serialize};
use tauri::ipc::Channel;
use tauri::menu::{MenuBuilder, MenuItem, PredefinedMenuItem, Submenu};
use tauri::{Emitter, Manager, State, WindowEvent};
use tauri_plugin_dialog::DialogExt;
use venator_engine::{
    BasicEventFilter, BasicSpanFilter, CachedStorage, DeleteFilter, DeleteMetrics, Engine,
    EventView, FallibleFilterPredicate, FileStorage, FilterPredicate, FilterPredicateSingle,
    FilterPropertyKind, InputError, Order, Query, SpanView, StatsView, SubscriptionId,
    SubscriptionResponse, Timestamp, TransientStorage, ValuePredicate,
};

mod ingress;

use ingress::{launch_ingress_thread, IngressState};

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
                InputView::from(BasicEventFilter::validate(p).map_err(|e| (e, text)))
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
                InputView::from(BasicSpanFilter::validate(p).map_err(|e| (e, text)))
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
    channel: Channel<SubscriptionResponseView<EventView>>,
) -> Result<SubscriptionId, String> {
    let (id, mut receiver) = engine.subscribe_to_events(filter).await;

    tokio::spawn(async move {
        while let Some(response) = receiver.recv().await {
            let response = match response {
                SubscriptionResponse::Add(event) => SubscriptionResponseView::Add(event),
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
    engine: State<'_, Engine>,
    id: SubscriptionId,
) -> Result<(), String> {
    engine.unsubscribe_from_events(id).await;

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
    engine: State<'_, Engine>,
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

    let engine_status = engine.get_status().await;

    Ok(StatusView {
        ingress_message,
        ingress_error,
        dataset_name,
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

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// The file (or :memory:) that holds captured telemetry
    #[arg(short, long)]
    dataset: Option<String>,

    /// The bind address to accept traces from
    #[arg(short, long)]
    bind: Option<String>,

    /// Controls whether the user session is saved (use `no-` to negate)
    #[arg(long, action = ArgAction::SetTrue, default_value_t = false)]
    persist_session: bool,
    #[arg(long, action = ArgAction::SetTrue, default_value_t = false, conflicts_with = "persist_session", hide = true)]
    no_persist_session: bool,
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

    fn persist_session(&self) -> Option<PathBuf> {
        if self.persist_session {
            return match self.dataset() {
                DatasetConfig::Default(mut path) | DatasetConfig::File(mut path) => {
                    path.set_extension("user");
                    Some(path)
                }
                DatasetConfig::Memory => {
                    // TODO: warn that session cannot be persisted
                    None
                }
            };
        } else if self.no_persist_session {
            return None;
        }

        match self.dataset() {
            DatasetConfig::Default(mut path) => {
                path.set_extension("user");
                Some(path)
            }
            DatasetConfig::File(mut path) => {
                path.set_extension("user");
                if let Ok(true) = std::fs::exists(&path) {
                    Some(path)
                } else {
                    None
                }
            }
            DatasetConfig::Memory => None,
        }
    }
}

fn main() {
    let args = Args::parse();
    let dataset = args.dataset();
    let bind = args.bind();
    let persist_session = args.persist_session();

    dataset.prepare();
    let engine = match &dataset {
        DatasetConfig::Default(path) => {
            Engine::new(CachedStorage::new(10000, FileStorage::new(path)))
        }
        DatasetConfig::File(path) => Engine::new(CachedStorage::new(10000, FileStorage::new(path))),
        DatasetConfig::Memory => Engine::new(TransientStorage::new()),
    };

    let ingress = bind.map(|bind| launch_ingress_thread(engine.clone(), bind.to_string()));

    tauri::Builder::default()
        .plugin(tauri_plugin_fs::init())
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
                            "Open dataset in new window",
                            true,
                            None::<&str>,
                        )?,
                        &PredefinedMenuItem::separator(handle)?,
                        &MenuItem::with_id(
                            handle,
                            "save-dataset-as",
                            "Save as",
                            true,
                            None::<&str>,
                        )?,
                        &MenuItem::with_id(
                            handle,
                            "save-as-csv",
                            "Export view as CSV",
                            true,
                            None::<&str>,
                        )?,
                        &MenuItem::new(handle, "Export view as ...", false, None::<&str>)?,
                        &PredefinedMenuItem::separator(handle)?,
                        &MenuItem::new(handle, "Exit", true, Some("alt+f4"))?,
                    ],
                )?)
                .item(&Submenu::with_items(
                    handle,
                    "Edit",
                    true,
                    &[
                        &MenuItem::with_id(handle, "undo", "Undo", true, Some("ctrl+z"))?,
                        &MenuItem::with_id(handle, "redo", "Redo", true, Some("ctrl+y"))?,
                        &PredefinedMenuItem::separator(handle)?,
                        &MenuItem::with_id(
                            handle,
                            "focus-filter",
                            "Go to filter",
                            true,
                            Some("ctrl+f"),
                        )?,
                    ],
                )?)
                .item(&Submenu::with_items(
                    handle,
                    "View",
                    true,
                    &[
                        &MenuItem::with_id(
                            handle,
                            "tab-new-events",
                            "New events tab",
                            true,
                            Some("ctrl+t"),
                        )?,
                        &MenuItem::with_id(
                            handle,
                            "tab-new-spans",
                            "New spans tab",
                            true,
                            None::<&str>,
                        )?,
                        &MenuItem::with_id(
                            handle,
                            "tab-duplicate",
                            "Duplicate tab",
                            true,
                            Some("ctrl+d"),
                        )?,
                        &MenuItem::with_id(
                            handle,
                            "tab-close-others",
                            "Close all other tabs",
                            true,
                            None::<&str>,
                        )?,
                        &PredefinedMenuItem::separator(handle)?,
                        &MenuItem::with_id(handle, "focus", "Focus", true, Some("ctrl+g"))?,
                        &MenuItem::with_id(
                            handle,
                            "focus-all",
                            "Focus all",
                            true,
                            Some("ctrl+shift+g"),
                        )?,
                        &PredefinedMenuItem::separator(handle)?,
                        &MenuItem::with_id(
                            handle,
                            "zoom-in",
                            "Zoom in timeframe",
                            true,
                            Some("ctrl+="),
                        )?,
                        &MenuItem::with_id(
                            handle,
                            "zoom-out",
                            "Zoom out timeframe",
                            true,
                            Some("ctrl+-"),
                        )?,
                        &PredefinedMenuItem::separator(handle)?,
                        &Submenu::with_items(
                            handle,
                            "Theme",
                            true,
                            &[
                                &MenuItem::with_id(
                                    handle,
                                    "set-theme-light",
                                    "Light",
                                    true,
                                    None::<&str>,
                                )?,
                                &MenuItem::with_id(
                                    handle,
                                    "set-theme-dark",
                                    "Dark",
                                    true,
                                    None::<&str>,
                                )?,
                            ],
                        )?,
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
                    ],
                )?)
                .item(&Submenu::with_items(
                    handle,
                    "Help",
                    true,
                    &[
                        &MenuItem::with_id(
                            handle,
                            "help-about",
                            "About Venator",
                            true,
                            None::<&str>,
                        )?,
                        &MenuItem::with_id(
                            handle,
                            "help-documentation",
                            "Documentation",
                            true,
                            None::<&str>,
                        )?,
                        &MenuItem::with_id(
                            handle,
                            "help-issue",
                            "Report an issue",
                            true,
                            None::<&str>,
                        )?,
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
                "save-dataset-as" => {
                    let engine = app.state::<Engine>().inner().clone();

                    app.dialog().file().save_file(move |file_path| {
                        let Some(path) = file_path else { return };

                        let new_storage = FileStorage::new(path.as_path().unwrap());

                        // we have no need for the result, and the command is
                        // executed regardless if we poll
                        #[allow(clippy::let_underscore_future)]
                        let _ = engine.copy_dataset(Box::new(new_storage));
                    });
                }
                "save-as-csv" => {
                    app.emit("save-as-csv-clicked", ()).unwrap();
                }
                "undo" => {
                    app.emit("undo-clicked", ()).unwrap();
                }
                "redo" => {
                    app.emit("redo-clicked", ()).unwrap();
                }
                "tab-new-events" => {
                    app.emit("tab-new-events-clicked", ()).unwrap();
                }
                "tab-new-spans" => {
                    app.emit("tab-new-spans-clicked", ()).unwrap();
                }
                "tab-duplicate" => {
                    app.emit("tab-duplicate-clicked", ()).unwrap();
                }
                "tab-close-others" => {
                    app.emit("tab-close-others-clicked", ()).unwrap();
                }
                "focus-filter" => {
                    app.emit("focus-filter-clicked", ()).unwrap();
                }
                "zoom-in" => {
                    app.emit("zoom-in-clicked", ()).unwrap();
                }
                "zoom-out" => {
                    app.emit("zoom-out-clicked", ()).unwrap();
                }
                "focus" => {
                    app.emit("focus-clicked", ()).unwrap();
                }
                "focus-all" => {
                    app.emit("focus-all-clicked", ()).unwrap();
                }
                "set-theme-light" => {
                    app.emit("set-theme-light-clicked", ()).unwrap();
                }
                "set-theme-dark" => {
                    app.emit("set-theme-dark-clicked", ()).unwrap();
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
                "help-about" => {
                    let _ = open::that("https://github.com/kmdreko/venator");
                }
                "help-documentation" => {
                    let _ = open::that("https://github.com/kmdreko/venator/tree/main/docs");
                }
                "help-issue" => {
                    let _ = open::that("https://github.com/kmdreko/venator/issues");
                }
                _ => {}
            });
            Ok(())
        })
        .on_window_event(|window, event| {
            if let WindowEvent::CloseRequested { .. } = event {
                let engine = window.state::<Engine>();
                shutdown(&engine);
            }
        })
        .manage(engine.clone())
        .manage(dataset)
        .manage(ingress)
        .manage(SessionPersistence(persist_session))
        .invoke_handler(tauri::generate_handler![
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
            get_status,
            save_session,
            load_session,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[tokio::main(flavor = "current_thread")]
async fn shutdown(engine: &Engine) {
    engine.save().await;
}

#[derive(Clone, Serialize, Deserialize)]
struct InputView {
    #[serde(flatten)]
    result: FilterPredicateResultView,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "input", rename_all = "camelCase")]
enum FilterPredicateResultView {
    Valid(FilterPredicateView),
    Invalid { text: String, error: String },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(
    tag = "predicate_kind",
    rename_all = "camelCase",
    content = "predicate"
)]
enum FilterPredicateView {
    Single(FilterPredicateSingleView),
    And(Vec<InputView>),
    Or(Vec<InputView>),
}

impl From<Result<FallibleFilterPredicate, (InputError, String)>> for InputView {
    fn from(result: Result<FallibleFilterPredicate, (InputError, String)>) -> Self {
        match result {
            Ok(FallibleFilterPredicate::Single(single)) => InputView {
                result: FilterPredicateResultView::Valid(FilterPredicateView::Single(
                    FilterPredicateSingleView::from(single),
                )),
            },
            Ok(FallibleFilterPredicate::And(predicates)) => InputView {
                result: FilterPredicateResultView::Valid(FilterPredicateView::And(
                    predicates.into_iter().map(InputView::from).collect(),
                )),
            },
            Ok(FallibleFilterPredicate::Or(predicates)) => InputView {
                result: FilterPredicateResultView::Valid(FilterPredicateView::Or(
                    predicates.into_iter().map(InputView::from).collect(),
                )),
            },
            Err((err, text)) => InputView {
                result: FilterPredicateResultView::Invalid {
                    text,
                    error: err.to_string(),
                },
            },
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct FilterPredicateSingleView {
    text: String,
    property_kind: Option<FilterPropertyKind>,
    property: String,
    #[serde(flatten)]
    value: ValuePredicate,
}

impl From<FilterPredicateSingle> for FilterPredicateSingleView {
    fn from(inner: FilterPredicateSingle) -> FilterPredicateSingleView {
        FilterPredicateSingleView {
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
    dataset_name: String,
    engine_load: f64,
}

#[derive(Serialize)]
pub struct DeleteMetricsView {
    spans: usize,
    span_events: usize,
    events: usize,
}

impl From<DeleteMetrics> for DeleteMetricsView {
    fn from(metrics: DeleteMetrics) -> Self {
        DeleteMetricsView {
            spans: metrics.spans,
            span_events: metrics.span_events,
            events: metrics.events,
        }
    }
}

struct SessionPersistence(Option<PathBuf>);

#[derive(Default, Serialize, Deserialize)]
struct Session {
    tabs: Vec<SessionTab>,
}

#[derive(Serialize, Deserialize)]
struct SessionTab {
    kind: String,
    start: Timestamp,
    end: Timestamp,
    filter: String,
    columns: Vec<String>,
}

#[derive(Clone, Serialize)]
#[serde(tag = "kind", content = "entity")]
enum SubscriptionResponseView<T> {
    Add(T),
    Remove(Timestamp),
}
