// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Mutex;

use clap::Parser;
use ingress::Ingress;
use serde::{Deserialize, Serialize};
use tauri::ipc::Channel;
use tauri::menu::{MenuBuilder, MenuItem, Submenu};
use tauri::State;
use venator_engine::{
    BasicEventFilter, BasicInstanceFilter, BasicSpanFilter, Engine, EventView, FileStorage,
    FilterPredicate, FilterPropertyKind, InstanceView, Order, Query, SpanView, StatsView,
    SubscriptionId, Timestamp, TransientStorage, ValuePredicate,
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
                        error: format!("{err:?}"),
                    },
                };

                InputView { text, result }
            })
            .collect()),
        Err(err) => Ok(vec![InputView {
            text: filter.to_owned(),
            result: FilterPredicateResultView::Invalid {
                error: format!("{err:?}"),
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
                        error: format!("{err:?}"),
                    },
                };

                InputView { text, result }
            })
            .collect()),
        Err(err) => Ok(vec![InputView {
            text: filter.to_owned(),
            result: FilterPredicateResultView::Invalid {
                error: format!("{err:?}"),
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
                        error: format!("{err:?}"),
                    },
                };

                InputView { text, result }
            })
            .collect()),
        Err(err) => Ok(vec![InputView {
            text: filter.to_owned(),
            result: FilterPredicateResultView::Invalid {
                error: format!("{err:?}"),
            },
        }]),
    }
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
    _engine: State<'_, Engine>,
    dataset: State<'_, DatasetConfig>,
    ingress: State<'_, Mutex<Option<Ingress>>>,
) -> Result<StatusView, String> {
    let (ingress_message, ingress_error) = match &mut *ingress.lock().unwrap() {
        Some(ingress) => ingress.status(),
        None => ("not listening".into(), None),
    };

    let dataset_message = match &*dataset {
        DatasetConfig::Default(_) => "using default dataset".to_owned(),
        DatasetConfig::File(path) => format!("using {}", path.display()),
        DatasetConfig::Memory => "using :memory:".to_owned(),
    };

    Ok(StatusView {
        ingress_message,
        ingress_error,
        dataset_message,
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
                        &MenuItem::new(handle, "View", true, None::<&str>)?,
                        &MenuItem::new(handle, "Tools", true, None::<&str>)?,
                    ],
                )?)
                .item(&MenuItem::new(handle, "Edit", true, None::<&str>)?)
                .item(&MenuItem::new(handle, "View", true, None::<&str>)?)
                .item(&MenuItem::new(handle, "Tools", true, None::<&str>)?)
                .item(&MenuItem::new(handle, "Help", true, None::<&str>)?)
                .build()?;
            app.set_menu(menu)?;
            app.on_menu_event(|app, event| {
                if event.id().as_ref() == "open-dataset" {
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
    dataset_message: String,
}
