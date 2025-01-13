// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::path::PathBuf;
use std::process::{Command, Stdio};

use anyhow::Error as AnyError;
use clap::{ArgAction, Parser};
use tauri::menu::{Menu, MenuBuilder, MenuEvent, MenuItem, PredefinedMenuItem, Submenu};
use tauri::{AppHandle, Emitter, Manager, WindowEvent, Wry};
use tauri_plugin_dialog::DialogExt;
use venator_engine::engine::AsyncEngine;
use venator_engine::storage::{CachedStorage, FileStorage, TransientStorage};

mod commands;
mod ingress;
mod views;

use ingress::{launch_ingress_thread, IngressState};

enum DatasetConfig {
    Default(PathBuf),
    File(PathBuf),
    Memory,
}

impl DatasetConfig {
    fn prepare(&self) {
        match self {
            DatasetConfig::Memory => { /* nothing to do */ }
            DatasetConfig::File(_) => { /* nothing to do, path should already exist */ }
            DatasetConfig::Default(path) => {
                if let Some(dir) = path.parent() {
                    std::fs::create_dir_all(dir).expect("could not create default directory");
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
            DatasetConfig::Default(PathBuf::from("local.vena.db"))
        } else {
            DatasetConfig::Default(
                directories::ProjectDirs::from("", "", "Venator")
                    .map(|dirs| dirs.data_dir().to_path_buf().join("default.vena.db"))
                    .unwrap_or(PathBuf::from("default.vena.db")),
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

fn main() -> Result<(), AnyError> {
    #[cfg(debug_assertions)]
    tracing_subscriber::fmt()
        .compact()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Args::parse();
    let dataset = args.dataset();
    let bind = args.bind();
    let persist_session = args.persist_session();

    dataset.prepare();
    let engine = match &dataset {
        DatasetConfig::Default(path) => {
            AsyncEngine::new(CachedStorage::new(10000, FileStorage::new(path)))?
        }
        DatasetConfig::File(path) => {
            AsyncEngine::new(CachedStorage::new(10000, FileStorage::new(path)))?
        }
        DatasetConfig::Memory => AsyncEngine::new(TransientStorage::new())?,
    };

    let ingress = bind.map(|bind| launch_ingress_thread(engine.clone(), bind.to_string()));

    tauri::Builder::default()
        .plugin(tauri_plugin_fs::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_clipboard_manager::init())
        .setup(|app| {
            let handle = app.handle();
            let menu = build_menu(handle)?;
            app.set_menu(menu)?;
            app.on_menu_event(handle_menu_event);
            Ok(())
        })
        .on_window_event(|window, event| {
            if let WindowEvent::CloseRequested { .. } = event {
                let engine = window.state::<AsyncEngine>();
                shutdown(&engine);
            }
        })
        .manage(engine.clone())
        .manage(dataset)
        .manage(ingress)
        .manage(SessionPersistence(persist_session))
        .invoke_handler(crate::commands::handler())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");

    Ok(())
}

fn build_menu(handle: &AppHandle) -> Result<Menu<Wry>, Box<dyn std::error::Error>> {
    Ok(MenuBuilder::new(handle)
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
                &MenuItem::with_id(handle, "save-dataset-as", "Save as", true, None::<&str>)?,
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
                &MenuItem::with_id(handle, "focus-filter", "Go to filter", true, Some("ctrl+f"))?,
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
                &MenuItem::with_id(handle, "tab-new-spans", "New spans tab", true, None::<&str>)?,
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
                &MenuItem::with_id(handle, "focus-all", "Focus all", true, Some("ctrl+shift+g"))?,
                &PredefinedMenuItem::separator(handle)?,
                &MenuItem::with_id(handle, "zoom-in", "Zoom in timeframe", true, Some("ctrl+="))?,
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
                        &MenuItem::with_id(handle, "set-theme-light", "Light", true, None::<&str>)?,
                        &MenuItem::with_id(handle, "set-theme-dark", "Dark", true, None::<&str>)?,
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
                &MenuItem::with_id(handle, "help-about", "About Venator", true, None::<&str>)?,
                &MenuItem::with_id(
                    handle,
                    "help-documentation",
                    "Documentation",
                    true,
                    None::<&str>,
                )?,
                &MenuItem::with_id(handle, "help-issue", "Report an issue", true, None::<&str>)?,
            ],
        )?)
        .build()?)
}

fn handle_menu_event(app: &AppHandle<Wry>, event: MenuEvent) {
    match event.id().as_ref() {
        "open-dataset" => {
            use tauri_plugin_dialog::DialogExt;

            let Ok(current_exe) = std::env::current_exe() else {
                return;
            };

            app.dialog().file().pick_file(move |file_path| {
                let Some(path) = file_path else { return };
                let Some(path) = path.as_path() else { return };

                Command::new(current_exe)
                    .arg("-d")
                    .arg(path)
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .expect("could not spawn new process");
            });
        }
        "save-dataset-as" => {
            let engine = app.state::<AsyncEngine>().inner().clone();

            app.dialog().file().save_file(move |file_path| {
                let Some(path) = file_path else { return };
                let Some(path) = path.as_path() else { return };

                let new_storage = FileStorage::new(path);

                // we have no need for the result, and the command is
                // executed regardless if we poll
                #[allow(clippy::let_underscore_future)]
                let _ = engine.copy_dataset(Box::new(new_storage));
            });
        }
        "save-as-csv" => {
            let _ = app.emit("save-as-csv-clicked", ());
        }
        "undo" => {
            let _ = app.emit("undo-clicked", ());
        }
        "redo" => {
            let _ = app.emit("redo-clicked", ());
        }
        "tab-new-events" => {
            let _ = app.emit("tab-new-events-clicked", ());
        }
        "tab-new-spans" => {
            let _ = app.emit("tab-new-spans-clicked", ());
        }
        "tab-duplicate" => {
            let _ = app.emit("tab-duplicate-clicked", ());
        }
        "tab-close-others" => {
            let _ = app.emit("tab-close-others-clicked", ());
        }
        "focus-filter" => {
            let _ = app.emit("focus-filter-clicked", ());
        }
        "zoom-in" => {
            let _ = app.emit("zoom-in-clicked", ());
        }
        "zoom-out" => {
            let _ = app.emit("zoom-out-clicked", ());
        }
        "focus" => {
            let _ = app.emit("focus-clicked", ());
        }
        "focus-all" => {
            let _ = app.emit("focus-all-clicked", ());
        }
        "set-theme-light" => {
            let _ = app.emit("set-theme-light-clicked", ());
        }
        "set-theme-dark" => {
            let _ = app.emit("set-theme-dark-clicked", ());
        }
        "delete-all" => {
            let _ = app.emit("delete-all-clicked", ());
        }
        "delete-inside" => {
            let _ = app.emit("delete-inside-clicked", ());
        }
        "delete-outside" => {
            let _ = app.emit("delete-outside-clicked", ());
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
    }
}

#[tokio::main(flavor = "current_thread")]
async fn shutdown(engine: &AsyncEngine) {
    if let Err(err) = engine.save().await {
        tracing::error!(?err, "failed to save");
    }
}

struct SessionPersistence(Option<PathBuf>);
