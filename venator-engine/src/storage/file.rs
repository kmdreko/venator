use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use bincode::Options;
use rusqlite::{params, Connection as DbConnection, Error as DbError, Params, Row};
use tracing::instrument;

use crate::index::{EventIndexes, SpanEventIndexes, SpanIndexes};
use crate::models::{EventKey, Level, SourceKind, Value};
use crate::{
    Event, FullSpanId, Resource, ResourceKey, Span, SpanEvent, SpanEventKind, SpanKey, Timestamp,
};

use super::{IndexStorage, Storage, StorageError, StorageIter};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum IndexState {
    Stale,
    Fresh,
}

#[allow(unused)]
#[derive(Debug)]
enum FileStorageError {
    Prepare(DbError),
    Query(DbError),
    Row(DbError),
    Insert(DbError),
    Update(DbError),
    Begin(DbError),
    Commit(DbError),
    Delete(DbError),
}

impl From<FileStorageError> for StorageError {
    fn from(value: FileStorageError) -> Self {
        StorageError::Internal(format!("{value:?}"))
    }
}

/// This storage holds all entities in an SQLite database at the provided path.
pub struct FileStorage {
    connection: DbConnection,
    index_state: IndexState,
}

impl FileStorage {
    pub fn new(path: &Path) -> FileStorage {
        let connection = DbConnection::open(path).unwrap();

        connection
            .execute_batch(r#"PRAGMA synchronous = OFF; PRAGMA journal_mode = OFF;"#)
            .unwrap();

        let _ = connection.execute(
            r#"
            CREATE TABLE meta (
                id      INT  NOT NULL,
                version TEXT NOT NULL,
                indexes TEXT NOT NULL,

                CONSTRAINT meta_pk PRIMARY KEY (id)
            );
            "#,
            (),
        );

        let _ = connection.execute(r#"INSERT INTO meta VALUES (1, '0.3', 'STALE');"#, ());

        let (version, index_state): (String, String) = connection
            .query_row(
                "SELECT version, indexes FROM meta WHERE id = 1",
                (),
                |row| row.get(0).and_then(|a| row.get(1).map(|b| (a, b))),
            )
            .unwrap();

        if version != "0.3" {
            panic!("cannot load database with incompatible version");
        }

        let index_state = match &*index_state {
            "STALE" => IndexState::Stale,
            "FRESH" => IndexState::Fresh,
            _ => IndexState::Stale,
        };

        let _ = connection.execute(
            r#"
            CREATE TABLE indexes (
                kind TEXT NOT NULL,
                data BLOB NOT NULL,

                CONSTRAINT indexes_pk PRIMARY KEY (kind)
            );
            "#,
            (),
        );

        let _ = connection.execute(r#"INSERT INTO indexes VALUES ('spans', x'');"#, ());

        let _ = connection.execute(r#"INSERT INTO indexes VALUES ('span_events', x'');"#, ());

        let _ = connection.execute(r#"INSERT INTO indexes VALUES ('events', x'');"#, ());

        let _ = connection.execute(
            r#"
            CREATE TABLE resources (
                key        INT8 NOT NULL,
                attributes TEXT NOT NULL,
                warnings   TEXT NOT NULL,

                CONSTRAINT resources_pk PRIMARY KEY (key)
            );"#,
            (),
        );

        let _ = connection.execute(
            r#"
            CREATE TABLE spans (
                key              INT8 NOT NULL,
                kind             INT NOT NULL,
                resource_key     INT8 NOT NULL,
                id               TEXT NOT NULL,
                closed_at        INT8,
                busy             INT8,
                parent_id        TEXT,
                parent_key       INT8,
                links            TEXT NOT NULL,
                name             TEXT NOT NULL,
                namespace        TEXT,
                function         TEXT,
                level            INT NOT NULL,
                file_name        TEXT,
                file_line        INT,
                file_column      INT,
                instr_attributes TEXT NOT NULL,
                attributes       TEXT NOT NULL,
                warnings         TEXT NOT NULL,

                CONSTRAINT spans_pk PRIMARY KEY (key)
            );"#,
            (),
        );

        let _ = connection.execute(
            r#"
            CREATE TABLE span_events (
                key      INT8 NOT NULL,
                span_key INT8 NOT NULL,
                kind     TEXT NOT NULL,
                data     TEXT,
                warnings TEXT NOT NULL,

                CONSTRAINT span_events_pk PRIMARY KEY (key)
            );"#,
            (),
        );

        let _ = connection.execute(
            r#"
            CREATE TABLE events (
                key          INT8 NOT NULL,
                kind         INT NOT NULL,
                resource_key INT8 NOT NULL,
                parent_id    TEXT,
                parent_key   INT8,
                content      TEXT NOT NULL,
                namespace    TEXT,
                function     TEXT,
                level        INT NOT NULL,
                file_name    TEXT,
                file_line    INT,
                file_column  INT,
                attributes   TEXT NOT NULL,
                warnings     TEXT NOT NULL,

                CONSTRAINT events_pk PRIMARY KEY (key)
            );"#,
            (),
        );

        FileStorage {
            connection,
            index_state,
        }
    }

    fn invalidate_indexes(&mut self) {
        if self.index_state == IndexState::Fresh {
            self.connection
                .execute("UPDATE meta SET indexes = 'STALE' WHERE id = 1", ())
                .unwrap();

            self.index_state = IndexState::Stale;
        }
    }
}

impl Storage for FileStorage {
    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_resource(&self, at: Timestamp) -> Result<Arc<Resource>, StorageError> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM resources WHERE key = ?1")
            .map_err(FileStorageError::Prepare)?;

        let resource = stmt
            .query_row((at,), resource_from_row)
            .map_err(FileStorageError::Row)?;

        Ok(Arc::new(resource))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_span(&self, at: Timestamp) -> Result<Arc<Span>, StorageError> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM spans WHERE key = ?1")
            .map_err(FileStorageError::Prepare)?;

        let span = stmt
            .query_row((at,), span_from_row)
            .map_err(FileStorageError::Row)?;

        Ok(Arc::new(span))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_span_event(&self, at: Timestamp) -> Result<Arc<SpanEvent>, StorageError> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM span_events WHERE key = ?1")
            .map_err(FileStorageError::Prepare)?;

        let span_event = stmt
            .query_row((at,), span_event_from_row)
            .map_err(FileStorageError::Row)?;

        Ok(Arc::new(span_event))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_event(&self, at: Timestamp) -> Result<Arc<Event>, StorageError> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM events WHERE key = ?1")
            .map_err(FileStorageError::Prepare)?;

        let event = stmt
            .query_row((at,), event_from_row)
            .map_err(FileStorageError::Row)?;

        Ok(Arc::new(event))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_all_resources(&self) -> Result<StorageIter<Resource>, StorageError> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM resources ORDER BY key")
            .map_err(FileStorageError::Prepare)?;

        let resources = stmt
            .query_map((), resource_from_row)
            .map_err(FileStorageError::Query)?
            .map(|result| {
                result
                    .map(Arc::new)
                    .map_err(|e| StorageError::from(FileStorageError::Row(e)))
            })
            .collect::<Vec<_>>();

        Ok(Box::new(resources.into_iter()))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_all_spans(&self) -> Result<StorageIter<Span>, StorageError> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM spans ORDER BY key")
            .map_err(FileStorageError::Prepare)?;

        let spans = stmt
            .query_map((), span_from_row)
            .map_err(FileStorageError::Query)?
            .map(|result| {
                result
                    .map(Arc::new)
                    .map_err(|e| StorageError::from(FileStorageError::Row(e)))
            })
            .collect::<Vec<_>>();

        Ok(Box::new(spans.into_iter()))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_all_span_events(&self) -> Result<StorageIter<SpanEvent>, StorageError> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM span_events ORDER BY key")
            .map_err(FileStorageError::Prepare)?;

        let span_events = stmt
            .query_map((), span_event_from_row)
            .map_err(FileStorageError::Query)?
            .map(|result| {
                result
                    .map(Arc::new)
                    .map_err(|e| StorageError::from(FileStorageError::Row(e)))
            })
            .collect::<Vec<_>>();

        Ok(Box::new(span_events.into_iter()))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_all_events(&self) -> Result<StorageIter<Event>, StorageError> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM events ORDER BY key")
            .map_err(FileStorageError::Prepare)?;

        let events = stmt
            .query_map((), event_from_row)
            .map_err(FileStorageError::Query)?
            .map(|result| {
                result
                    .map(Arc::new)
                    .map_err(|e| StorageError::from(FileStorageError::Row(e)))
            })
            .collect::<Vec<_>>();

        Ok(Box::new(events.into_iter()))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn insert_resource(&mut self, resource: Resource) -> Result<(), StorageError> {
        let mut stmt = self
            .connection
            .prepare_cached("INSERT INTO resources VALUES (?1, ?2, ?3)")
            .map_err(FileStorageError::Prepare)?;

        stmt.execute(resource_to_params(resource))
            .map_err(FileStorageError::Insert)?;
        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn insert_span(&mut self, span: Span) -> Result<(), StorageError> {
        self.invalidate_indexes();

        let mut stmt = self
            .connection
            .prepare_cached(
                "INSERT INTO spans VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)",
            )
            .map_err(FileStorageError::Prepare)?;

        // have to inline it since I exceeded 16 elements

        let key = span.created_at;
        let kind = span.kind as i32;
        let resource_key = span.resource_key;
        let id = span.id.to_string();
        let closed_at = span.closed_at;
        let busy = span.busy.map(|b| b as i64);
        let parent_id = span.parent_id.map(|id| id.to_string());
        let parent_key = span.parent_key;
        let links = serde_json::to_string(&span.links).unwrap();
        let name = span.name;
        let namespace = span.namespace;
        let function = span.function;
        let level = span.level.to_db();
        let file_name = span.file_name;
        let file_line = span.file_line;
        let file_column = span.file_column;
        let instrumentation_attributes =
            serde_json::to_string(&span.instrumentation_attributes).unwrap();
        let attributes = serde_json::to_string(&span.attributes).unwrap();
        let warnings = "[]";

        stmt.execute(params![
            key,
            kind,
            resource_key,
            id,
            closed_at,
            busy,
            parent_id,
            parent_key,
            links,
            name,
            namespace,
            function,
            level,
            file_name,
            file_line,
            file_column,
            instrumentation_attributes,
            attributes,
            warnings,
        ])
        .map_err(FileStorageError::Insert)?;
        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn insert_span_event(&mut self, span_event: SpanEvent) -> Result<(), StorageError> {
        self.invalidate_indexes();

        let mut stmt = self
            .connection
            .prepare_cached("INSERT INTO span_events VALUES (?1, ?2, ?3, ?4, ?5)")
            .map_err(FileStorageError::Prepare)?;

        stmt.execute(span_event_to_params(span_event))
            .map_err(FileStorageError::Insert)?;
        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn insert_event(&mut self, event: Event) -> Result<(), StorageError> {
        self.invalidate_indexes();

        let mut stmt = self
            .connection
            .prepare_cached(
                "INSERT INTO events VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            )
            .map_err(FileStorageError::Prepare)?;

        stmt.execute(event_to_params(event))
            .map_err(FileStorageError::Insert)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn update_span_closed(
        &mut self,
        at: Timestamp,
        closed: Timestamp,
        busy: Option<u64>,
    ) -> Result<(), StorageError> {
        self.invalidate_indexes();

        let mut stmt = self
            .connection
            .prepare_cached("UPDATE spans SET closed_at = ?2, busy = ?3 WHERE key = ?1")
            .map_err(FileStorageError::Prepare)?;

        stmt.execute((at, closed, busy.map(|b| b as i64)))
            .map_err(FileStorageError::Update)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn update_span_attributes(
        &mut self,
        at: Timestamp,
        attributes: BTreeMap<String, Value>,
    ) -> Result<(), StorageError> {
        self.invalidate_indexes();

        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM spans WHERE spans.key = ?1")
            .map_err(FileStorageError::Prepare)?;

        let span = stmt
            .query_row((at,), span_from_row)
            .map_err(FileStorageError::Row)?;
        let existing_attributes = span.attributes;

        let attributes = {
            let mut new_attributes = existing_attributes;
            new_attributes.extend(attributes);
            new_attributes
        };
        let attributes = serde_json::to_string(&attributes).unwrap();

        let mut stmt = self
            .connection
            .prepare_cached("UPDATE spans SET attributes = ?2 WHERE key = ?1")
            .map_err(FileStorageError::Prepare)?;

        stmt.execute((at, attributes))
            .map_err(FileStorageError::Update)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn update_span_link(
        &mut self,
        at: Timestamp,
        link: FullSpanId,
        attributes: BTreeMap<String, Value>,
    ) -> Result<(), StorageError> {
        self.invalidate_indexes();

        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM spans WHERE spans.key = ?1")
            .map_err(FileStorageError::Prepare)?;

        let span = stmt
            .query_row((at,), span_from_row)
            .map_err(FileStorageError::Row)?;
        let existing_links = span.links;

        let links = {
            let mut new_linkss = existing_links;
            new_linkss.push((link, attributes));
            new_linkss
        };
        let attributes = serde_json::to_string(&links).unwrap();

        let mut stmt = self
            .connection
            .prepare_cached("UPDATE spans SET follows = ?2 WHERE key = ?1")
            .map_err(FileStorageError::Prepare)?;

        stmt.execute((at, attributes))
            .map_err(FileStorageError::Update)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn update_span_parents(
        &mut self,
        parent_key: SpanKey,
        spans: &[SpanKey],
    ) -> Result<(), StorageError> {
        self.invalidate_indexes();

        let tx = self
            .connection
            .transaction()
            .map_err(FileStorageError::Begin)?;

        let mut stmt = tx
            .prepare_cached("UPDATE spans SET parent_key = ?2 WHERE key = ?1")
            .map_err(FileStorageError::Prepare)?;

        for span_key in spans {
            stmt.execute((span_key, parent_key))
                .map_err(FileStorageError::Update)?;
        }

        drop(stmt);
        tx.commit().map_err(FileStorageError::Commit)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn update_event_parents(
        &mut self,
        parent_key: SpanKey,
        events: &[EventKey],
    ) -> Result<(), StorageError> {
        self.invalidate_indexes();

        let tx = self
            .connection
            .transaction()
            .map_err(FileStorageError::Begin)?;

        let mut stmt = tx
            .prepare_cached("UPDATE events SET parent_key = ?2 WHERE key = ?1")
            .map_err(FileStorageError::Prepare)?;

        for event_key in events {
            stmt.execute((event_key, parent_key))
                .map_err(FileStorageError::Update)?;
        }

        drop(stmt);
        tx.commit().map_err(FileStorageError::Commit)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn drop_resources(&mut self, resources: &[Timestamp]) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction()
            .map_err(FileStorageError::Begin)?;

        let mut stmt = tx
            .prepare_cached("DELETE FROM resources WHERE resources.key = ?1")
            .map_err(FileStorageError::Prepare)?;

        for resource_key in resources {
            stmt.execute((resource_key,))
                .map_err(FileStorageError::Delete)?;
        }

        drop(stmt);
        tx.commit().map_err(FileStorageError::Commit)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn drop_spans(&mut self, spans: &[Timestamp]) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction()
            .map_err(FileStorageError::Begin)?;

        let mut stmt = tx
            .prepare_cached("DELETE FROM spans WHERE spans.key = ?1")
            .map_err(FileStorageError::Prepare)?;

        for span_key in spans {
            stmt.execute((span_key,))
                .map_err(FileStorageError::Delete)?;
        }

        drop(stmt);
        tx.commit().map_err(FileStorageError::Commit)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn drop_span_events(&mut self, span_events: &[Timestamp]) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction()
            .map_err(FileStorageError::Begin)?;

        let mut stmt = tx
            .prepare_cached("DELETE FROM span_events WHERE span_events.key = ?1")
            .map_err(FileStorageError::Prepare)?;

        for span_event_key in span_events {
            stmt.execute((span_event_key,))
                .map_err(FileStorageError::Delete)?;
        }

        drop(stmt);
        tx.commit().map_err(FileStorageError::Commit)?;

        Ok(())
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn drop_events(&mut self, events: &[Timestamp]) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction()
            .map_err(FileStorageError::Begin)?;

        let mut stmt = tx
            .prepare_cached("DELETE FROM events WHERE events.key = ?1")
            .map_err(FileStorageError::Prepare)?;

        for event_key in events {
            stmt.execute((event_key,))
                .map_err(FileStorageError::Delete)?;
        }

        drop(stmt);
        tx.commit().map_err(FileStorageError::Commit)?;

        Ok(())
    }

    #[allow(private_interfaces)]
    fn as_index_storage(&self) -> Option<&dyn IndexStorage> {
        Some(self)
    }

    #[allow(private_interfaces)]
    fn as_index_storage_mut(&mut self) -> Option<&mut dyn IndexStorage> {
        Some(self)
    }
}

impl IndexStorage for FileStorage {
    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn get_indexes(&self) -> Option<(SpanIndexes, SpanEventIndexes, EventIndexes)> {
        use bincode::DefaultOptions;

        if self.index_state == IndexState::Stale {
            return None;
        }

        let span_index_data: Vec<u8> = self
            .connection
            .query_row("SELECT data FROM indexes WHERE kind = 'spans'", (), |row| {
                row.get(0)
            })
            .unwrap();

        let span_event_index_data: Vec<u8> = self
            .connection
            .query_row(
                "SELECT data FROM indexes WHERE kind = 'span_events'",
                (),
                |row| row.get(0),
            )
            .unwrap();

        let event_index_data: Vec<u8> = self
            .connection
            .query_row(
                "SELECT data FROM indexes WHERE kind = 'events'",
                (),
                |row| row.get(0),
            )
            .unwrap();

        let bincode_options = DefaultOptions::new().with_fixint_encoding();

        let span_indexes = bincode_options.deserialize(&span_index_data).unwrap();
        let span_event_indexes = bincode_options.deserialize(&span_event_index_data).unwrap();
        let event_indexes = bincode_options.deserialize(&event_index_data).unwrap();

        Some((span_indexes, span_event_indexes, event_indexes))
    }

    #[instrument(level = tracing::Level::TRACE, skip_all)]
    fn update_indexes(
        &mut self,
        span_indexes: &SpanIndexes,
        span_event_indexes: &SpanEventIndexes,
        event_indexes: &EventIndexes,
    ) {
        use bincode::DefaultOptions;

        let bincode_options = DefaultOptions::new().with_fixint_encoding();

        let span_index_data = bincode_options.serialize(span_indexes).unwrap();
        let span_event_index_data = bincode_options.serialize(span_event_indexes).unwrap();
        let event_index_data = bincode_options.serialize(event_indexes).unwrap();

        let tx = self.connection.transaction().unwrap();

        let mut stmt = tx
            .prepare("UPDATE indexes SET data = ?2 WHERE kind = ?1")
            .unwrap();
        stmt.execute(("spans", span_index_data)).unwrap();
        stmt.execute(("span_events", span_event_index_data))
            .unwrap();
        stmt.execute(("events", event_index_data)).unwrap();
        drop(stmt);

        tx.execute("UPDATE meta SET indexes = 'FRESH' WHERE id = 1", ())
            .unwrap();

        tx.commit().unwrap();
    }
}

fn resource_to_params(resource: Resource) -> impl Params {
    let key = resource.key();
    let attributes = serde_json::to_string(&resource.attributes).unwrap();
    let warnings = "[]";

    (key, attributes, warnings)
}

fn resource_from_row(row: &Row<'_>) -> Result<Resource, DbError> {
    let key: i64 = row.get(0)?;
    let attributes: String = row.get(1)?;
    let attributes = serde_json::from_str(&attributes).unwrap();
    // let warnings = row.get(2)?;

    Ok(Resource {
        created_at: ResourceKey::new(key as u64).unwrap(),
        attributes,
    })
}

fn span_from_row(row: &Row<'_>) -> Result<Span, DbError> {
    let key = row.get(0)?;
    let kind: i32 = row.get(1)?;
    let resource_key = row.get(2)?;
    let id: String = row.get(3)?;
    let closed_at = row.get(4)?;
    let busy: Option<i64> = row.get(5)?;
    let parent_id: Option<String> = row.get(6)?;
    let parent_key = row.get(7)?;
    let links: String = row.get(8)?;
    let links = serde_json::from_str(&links).unwrap();
    let name = row.get(9)?;
    let namespace = row.get(10)?;
    let function = row.get(11)?;
    let level: i32 = row.get(12)?;
    let file_name = row.get(13)?;
    let file_line = row.get(14)?;
    let file_column = row.get(15)?;
    let instrumentation_attributes: String = row.get(16)?;
    let instrumentation_attributes = serde_json::from_str(&instrumentation_attributes).unwrap();
    let attributes: String = row.get(17)?;
    let attributes = serde_json::from_str(&attributes).unwrap();
    // let warnings = row.get(18)?;

    Ok(Span {
        kind: SourceKind::try_from(kind).unwrap(),
        created_at: key,
        resource_key,
        id: id.parse().unwrap(),
        closed_at,
        busy: busy.map(|b| b as u64),
        parent_id: parent_id.map(|id| id.parse().unwrap()),
        parent_key,
        links,
        name,
        namespace,
        function,
        level: Level::from_db(level).unwrap(),
        file_name,
        file_line,
        file_column,
        instrumentation_attributes,
        attributes,
    })
}

fn span_event_to_params(span_event: SpanEvent) -> impl Params {
    match span_event.kind {
        SpanEventKind::Create(create_span_event) => {
            let key = span_event.timestamp;
            let span_key = span_event.span_key;
            let kind = "create";
            let data = serde_json::to_string(&create_span_event).unwrap();
            let warnings = "[]";

            (key, span_key, kind, Some(data), warnings)
        }
        SpanEventKind::Update(update_span_event) => {
            let key = span_event.timestamp;
            let span_key = span_event.span_key;
            let kind = "update";
            let data = serde_json::to_string(&update_span_event).unwrap();
            let warnings = "[]";

            (key, span_key, kind, Some(data), warnings)
        }
        SpanEventKind::Follows(follows_span_event) => {
            let key = span_event.timestamp;
            let span_key = span_event.span_key;
            let kind = "follows";
            let data = serde_json::to_string(&follows_span_event).unwrap();
            let warnings = "[]";

            (key, span_key, kind, Some(data), warnings)
        }
        SpanEventKind::Enter(enter_span_event) => {
            let key = span_event.timestamp;
            let span_key = span_event.span_key;
            let kind = "enter";
            let data = serde_json::to_string(&enter_span_event).unwrap();
            let warnings = "[]";

            (key, span_key, kind, Some(data), warnings)
        }
        SpanEventKind::Exit => {
            let key = span_event.timestamp;
            let span_key = span_event.span_key;
            let kind = "exit";
            let warnings = "[]";

            (key, span_key, kind, None, warnings)
        }
        SpanEventKind::Close(close_span_event) => {
            let key = span_event.timestamp;
            let span_key = span_event.span_key;
            let kind = "close";
            let data = serde_json::to_string(&close_span_event).unwrap();
            let warnings = "[]";

            (key, span_key, kind, Some(data), warnings)
        }
    }
}

fn span_event_from_row(row: &Row<'_>) -> Result<SpanEvent, DbError> {
    let key = row.get(0)?;
    let span_key = row.get(1)?;
    let kind: String = row.get(2)?;
    let data: Option<String> = row.get(3)?;
    // let warnings = row.get(4)?;

    match kind.as_str() {
        "create" => {
            let create_span_event = serde_json::from_str(&data.unwrap()).unwrap();
            Ok(SpanEvent {
                timestamp: key,
                span_key,
                kind: SpanEventKind::Create(create_span_event),
            })
        }
        "update" => {
            let update_span_event = serde_json::from_str(&data.unwrap()).unwrap();
            Ok(SpanEvent {
                timestamp: key,
                span_key,
                kind: SpanEventKind::Update(update_span_event),
            })
        }
        "follows" => {
            let follows_span_event = serde_json::from_str(&data.unwrap()).unwrap();
            Ok(SpanEvent {
                timestamp: key,
                span_key,
                kind: SpanEventKind::Follows(follows_span_event),
            })
        }
        "enter" => {
            let enter_span_event = serde_json::from_str(&data.unwrap()).unwrap();
            Ok(SpanEvent {
                timestamp: key,
                span_key,
                kind: SpanEventKind::Enter(enter_span_event),
            })
        }
        "exit" => Ok(SpanEvent {
            timestamp: key,
            span_key,
            kind: SpanEventKind::Exit,
        }),
        "close" => {
            let close_span_event = serde_json::from_str(&data.unwrap()).unwrap();
            Ok(SpanEvent {
                timestamp: key,
                span_key,
                kind: SpanEventKind::Close(close_span_event),
            })
        }
        _ => panic!("unknown span event kind"),
    }
}

#[rustfmt::skip]
fn event_to_params(event: Event) -> impl Params {
    let key = event.timestamp;
    let kind = event.kind as i32;
    let resource_key = event.resource_key;
    let parent_id = event.parent_id.map(|id| id.to_string());
    let parent_key = event.parent_key;
    let content = serde_json::to_string(&event.content).unwrap();
    let namespace = event.namespace;
    let function = event.function;
    let level = event.level.to_db();
    let file_name = event.file_name;
    let file_line = event.file_line;
    let file_column = event.file_column;
    let attributes = serde_json::to_string(&event.attributes).unwrap();
    let warnings = "[]";

    (key, kind, resource_key, parent_id, parent_key, content, namespace, function, level, file_name, file_line, file_column, attributes, warnings)
}

fn event_from_row(row: &Row<'_>) -> Result<Event, DbError> {
    let key = row.get(0)?;
    let kind: i32 = row.get(1)?;
    let resource_key = row.get(2)?;
    let parent_id: Option<String> = row.get(3)?;
    let parent_key = row.get(4)?;
    let content: String = row.get(5)?;
    let content = serde_json::from_str(&content).unwrap();
    let namespace = row.get(6)?;
    let function = row.get(7)?;
    let level: i32 = row.get(8)?;
    let file_name = row.get(9)?;
    let file_line = row.get(10)?;
    let file_column = row.get(11)?;
    let attributes: String = row.get(12)?;
    let attributes = serde_json::from_str(&attributes).unwrap();
    // let warnings = row.get(13)?;

    Ok(Event {
        kind: SourceKind::try_from(kind).unwrap(),
        timestamp: key,
        resource_key,
        parent_id: parent_id.map(|id| id.parse().unwrap()),
        parent_key,
        content,
        namespace,
        function,
        level: Level::from_db(level).unwrap(),
        file_name,
        file_line,
        file_column,
        attributes,
    })
}
