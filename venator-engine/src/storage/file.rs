use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use rusqlite::{params, Connection as DbConnection, Error as DbError, Params, Row};

use crate::models::{EventKey, Level, Value};
use crate::{Event, Resource, ResourceKey, Span, SpanEvent, SpanEventKind, SpanKey, Timestamp};

use super::Storage;

pub struct FileStorage {
    connection: DbConnection,
}

impl FileStorage {
    pub fn new(path: &Path) -> FileStorage {
        let connection = DbConnection::open(path).unwrap();

        connection
            .execute_batch(r#"PRAGMA synchronous = OFF; PRAGMA journal_mode = OFF;"#)
            .unwrap();

        let _ = connection.execute(
            r#"
            CREATE TABLE resources (
                key             INT8 NOT NULL,
                fields          TEXT,

                CONSTRAINT resources_pk PRIMARY KEY (key)
            );"#,
            (),
        );

        let _ = connection.execute(
            r#"
            CREATE TABLE spans (
                key          INT8 NOT NULL,
                resource_key INT8,
                id           TEXT,
                closed_at    INT8,
                busy         INT8,
                parent_id    TEXT,
                parent_key   INT8,
                follows      TEXT,
                name         TEXT,
                namespace    TEXT,
                function     TEXT,
                level        INT,
                file_name    TEXT,
                file_line    INT,
                file_column  INT,
                instrumentation_fields TEXT,
                fields       TEXT,

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

                CONSTRAINT span_events_pk PRIMARY KEY (key)
            );"#,
            (),
        );

        let _ = connection.execute(
            r#"
            CREATE TABLE events (
                key          INT8 NOT NULL,
                resource_key INT8,
                parent_id    TEXT,
                parent_key   INT8,
                content      TEXT,
                namespace    TEXT,
                function     TEXT,
                level        INT,
                file_name    TEXT,
                file_line    INT,
                file_column  INT,
                fields       TEXT,

                CONSTRAINT events_pk PRIMARY KEY (key)
            );"#,
            (),
        );

        FileStorage { connection }
    }
}

impl Storage for FileStorage {
    fn get_resource(&self, at: Timestamp) -> Option<Arc<Resource>> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM resources WHERE key = ?1")
            .unwrap();

        let result = stmt.query_row((at,), resource_from_row);

        Some(Arc::new(result.unwrap()))
    }

    fn get_span(&self, at: Timestamp) -> Option<Arc<Span>> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM spans WHERE key = ?1")
            .unwrap();

        let result = stmt.query_row((at,), span_from_row);

        Some(Arc::new(result.unwrap()))
    }

    fn get_span_event(&self, at: Timestamp) -> Option<Arc<SpanEvent>> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM span_events WHERE key = ?1")
            .unwrap();

        let result = stmt.query_row((at,), span_event_from_row);

        Some(Arc::new(result.unwrap()))
    }

    fn get_event(&self, at: Timestamp) -> Option<Arc<Event>> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM events WHERE key = ?1")
            .unwrap();

        let result = stmt.query_row((at,), event_from_row);

        Some(Arc::new(result.unwrap()))
    }

    fn get_all_resources(&self) -> Box<dyn Iterator<Item = Arc<Resource>> + '_> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM resources ORDER BY key")
            .unwrap();

        let resources = stmt
            .query_map((), resource_from_row)
            .unwrap()
            .map(|result| result.unwrap())
            .collect::<Vec<_>>();

        Box::new(resources.into_iter().map(Arc::new))
    }

    fn get_all_spans(&self) -> Box<dyn Iterator<Item = Arc<Span>> + '_> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM spans ORDER BY key")
            .unwrap();

        let spans = stmt
            .query_map((), span_from_row)
            .unwrap()
            .map(|result| result.unwrap())
            .collect::<Vec<_>>();

        Box::new(spans.into_iter().map(Arc::new))
    }

    fn get_all_span_events(&self) -> Box<dyn Iterator<Item = Arc<SpanEvent>> + '_> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM span_events ORDER BY key")
            .unwrap();

        let span_events = stmt
            .query_map((), span_event_from_row)
            .unwrap()
            .map(|result| result.unwrap())
            .collect::<Vec<_>>();

        Box::new(span_events.into_iter().map(Arc::new))
    }

    fn get_all_events(&self) -> Box<dyn Iterator<Item = Arc<Event>> + '_> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM events ORDER BY key")
            .unwrap();

        let events = stmt
            .query_map((), event_from_row)
            .unwrap()
            .map(|result| result.unwrap())
            .collect::<Vec<_>>();

        Box::new(events.into_iter().map(Arc::new))
    }

    fn insert_resource(&mut self, resource: Resource) {
        let mut stmt = self
            .connection
            .prepare_cached("INSERT INTO resources VALUES (?1, ?2)")
            .unwrap();

        stmt.execute(resource_to_params(resource)).unwrap();
    }

    fn insert_span(&mut self, span: Span) {
        let mut stmt = self
            .connection
            .prepare_cached(
                "INSERT INTO spans VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            )
            .unwrap();

        // have to inline it since I exceeded 16 elements

        let key = span.created_at;
        let resource_key = span.resource_key;
        let id = span.id.to_string();
        let closed_at = span.closed_at;
        let busy = span.busy.map(|b| b as i64);
        let parent_id = span.parent_id.map(|id| id.to_string());
        let parent_key = span.parent_key;
        let follows = serde_json::to_string(&span.follows).unwrap();
        let name = span.name;
        let namespace = span.namespace;
        let function = span.function;
        let level = span.level.to_db();
        let file_name = span.file_name;
        let file_line = span.file_line;
        let file_column = span.file_column;
        let instrumentation_fields = serde_json::to_string(&span.instrumentation_fields).unwrap();
        let fields = serde_json::to_string(&span.fields).unwrap();

        stmt.execute(params![
            key,
            resource_key,
            id,
            closed_at,
            busy,
            parent_id,
            parent_key,
            follows,
            name,
            namespace,
            function,
            level,
            file_name,
            file_line,
            file_column,
            instrumentation_fields,
            fields
        ])
        .unwrap();
    }

    fn insert_span_event(&mut self, span_event: SpanEvent) {
        let mut stmt = self
            .connection
            .prepare_cached("INSERT INTO span_events VALUES (?1, ?2, ?3, ?4)")
            .unwrap();

        stmt.execute(span_event_to_params(span_event)).unwrap();
    }

    fn insert_event(&mut self, event: Event) {
        let mut stmt = self
            .connection
            .prepare_cached(
                "INSERT INTO events VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            )
            .unwrap();

        stmt.execute(event_to_params(event)).unwrap();
    }

    fn update_span_closed(&mut self, at: Timestamp, closed: Timestamp, busy: Option<u64>) {
        let mut stmt = self
            .connection
            .prepare_cached("UPDATE spans SET closed_at = ?2, busy = ?3 WHERE key = ?1")
            .unwrap();

        stmt.execute((at, closed, busy.map(|b| b as i64))).unwrap();
    }

    fn update_span_fields(&mut self, at: Timestamp, fields: BTreeMap<String, Value>) {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM spans WHERE spans.key = ?1")
            .unwrap();

        let span = stmt.query_row((at,), span_from_row).unwrap();
        let existing_fields = span.fields;

        let fields = {
            let mut new_fields = existing_fields;
            new_fields.extend(fields);
            new_fields
        };
        let fields = serde_json::to_string(&fields).unwrap();

        let mut stmt = self
            .connection
            .prepare_cached("UPDATE spans SET fields = ?2 WHERE key = ?1")
            .unwrap();

        stmt.execute((at, fields)).unwrap();
    }

    fn update_span_follows(&mut self, at: Timestamp, follows: SpanKey) {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT * FROM spans WHERE spans.key = ?1")
            .unwrap();

        let span = stmt.query_row((at,), span_from_row).unwrap();
        let existing_follows = span.follows;

        let follows = {
            let mut new_follows = existing_follows;
            new_follows.push(follows);
            new_follows
        };
        let fields = serde_json::to_string(&follows).unwrap();

        let mut stmt = self
            .connection
            .prepare_cached("UPDATE spans SET follows = ?2 WHERE key = ?1")
            .unwrap();

        stmt.execute((at, fields)).unwrap();
    }

    fn update_span_parents(&mut self, parent_key: SpanKey, spans: &[SpanKey]) {
        let tx = self.connection.transaction().unwrap();

        let mut stmt = tx
            .prepare_cached("UPDATE spans SET parent_key = ?2 WHERE key = ?1")
            .unwrap();

        for span_key in spans {
            stmt.execute((span_key, parent_key)).unwrap();
        }

        drop(stmt);
        tx.commit().unwrap();
    }

    fn update_event_parents(&mut self, parent_key: SpanKey, events: &[EventKey]) {
        let tx = self.connection.transaction().unwrap();

        let mut stmt = tx
            .prepare_cached("UPDATE events SET parent_key = ?2 WHERE key = ?1")
            .unwrap();

        for event_key in events {
            stmt.execute((event_key, parent_key)).unwrap();
        }

        drop(stmt);
        tx.commit().unwrap();
    }

    fn drop_resources(&mut self, resources: &[Timestamp]) {
        let tx = self.connection.transaction().unwrap();

        let mut stmt = tx
            .prepare_cached("DELETE FROM resources WHERE resources.key = ?1")
            .unwrap();

        for resource_key in resources {
            stmt.execute((resource_key,)).unwrap();
        }

        drop(stmt);
        tx.commit().unwrap();
    }

    fn drop_spans(&mut self, spans: &[Timestamp]) {
        let tx = self.connection.transaction().unwrap();

        let mut stmt = tx
            .prepare_cached("DELETE FROM spans WHERE spans.key = ?1")
            .unwrap();

        for span_key in spans {
            stmt.execute((span_key,)).unwrap();
        }

        drop(stmt);
        tx.commit().unwrap();
    }

    fn drop_span_events(&mut self, span_events: &[Timestamp]) {
        let tx = self.connection.transaction().unwrap();

        let mut stmt = tx
            .prepare_cached("DELETE FROM span_events WHERE span_events.key = ?1")
            .unwrap();

        for span_event_key in span_events {
            stmt.execute((span_event_key,)).unwrap();
        }

        drop(stmt);
        tx.commit().unwrap();
    }

    fn drop_events(&mut self, events: &[Timestamp]) {
        let tx = self.connection.transaction().unwrap();

        let mut stmt = tx
            .prepare_cached("DELETE FROM events WHERE events.key = ?1")
            .unwrap();

        for event_key in events {
            stmt.execute((event_key,)).unwrap();
        }

        drop(stmt);
        tx.commit().unwrap();
    }
}

fn resource_to_params(resource: Resource) -> impl Params {
    let key = resource.key();
    let fields = serde_json::to_string(&resource.fields).unwrap();

    (key, fields)
}

fn resource_from_row(row: &Row<'_>) -> Result<Resource, DbError> {
    let key: i64 = row.get(0)?;
    let fields: String = row.get(1)?;
    let fields = serde_json::from_str(&fields).unwrap();

    Ok(Resource {
        created_at: ResourceKey::new(key as u64).unwrap(),
        fields,
    })
}

fn span_from_row(row: &Row<'_>) -> Result<Span, DbError> {
    let key = row.get(0)?;
    let resource_key = row.get(1)?;
    let id: String = row.get(2)?;
    let closed_at = row.get(3)?;
    let busy: Option<i64> = row.get(4)?;
    let parent_id: Option<String> = row.get(5)?;
    let parent_key = row.get(6)?;
    let follows: String = row.get(7)?;
    let follows = serde_json::from_str(&follows).unwrap();
    let name = row.get(8)?;
    let namespace = row.get(9)?;
    let function = row.get(10)?;
    let level: i32 = row.get(11)?;
    let file_name = row.get(12)?;
    let file_line = row.get(13)?;
    let file_column = row.get(14)?;
    let instrumentation_fields: String = row.get(15)?;
    let instrumentation_fields = serde_json::from_str(&instrumentation_fields).unwrap();
    let fields: String = row.get(16)?;
    let fields = serde_json::from_str(&fields).unwrap();

    Ok(Span {
        created_at: key,
        resource_key,
        id: id.parse().unwrap(),
        closed_at,
        busy: busy.map(|b| b as u64),
        parent_id: parent_id.map(|id| id.parse().unwrap()),
        parent_key,
        follows,
        name,
        namespace,
        function,
        level: Level::from_db(level).unwrap(),
        file_name,
        file_line,
        file_column,
        instrumentation_fields,
        fields,
    })
}

fn span_event_to_params(span_event: SpanEvent) -> impl Params {
    match span_event.kind {
        SpanEventKind::Create(create_span_event) => {
            let key = span_event.timestamp;
            let span_key = span_event.span_key;
            let kind = "create";
            let data = serde_json::to_string(&create_span_event).unwrap();

            (key, span_key, kind, Some(data))
        }
        SpanEventKind::Update(update_span_event) => {
            let key = span_event.timestamp;
            let span_key = span_event.span_key;
            let kind = "update";
            let data = serde_json::to_string(&update_span_event).unwrap();

            (key, span_key, kind, Some(data))
        }
        SpanEventKind::Follows(follows_span_event) => {
            let key = span_event.timestamp;
            let span_key = span_event.span_key;
            let kind = "follows";
            let data = serde_json::to_string(&follows_span_event).unwrap();

            (key, span_key, kind, Some(data))
        }
        SpanEventKind::Enter(enter_span_event) => {
            let key = span_event.timestamp;
            let span_key = span_event.span_key;
            let kind = "enter";
            let data = serde_json::to_string(&enter_span_event).unwrap();

            (key, span_key, kind, Some(data))
        }
        SpanEventKind::Exit => {
            let key = span_event.timestamp;
            let span_key = span_event.span_key;
            let kind = "exit";

            (key, span_key, kind, None)
        }
        SpanEventKind::Close(close_span_event) => {
            let key = span_event.timestamp;
            let span_key = span_event.span_key;
            let kind = "close";
            let data = serde_json::to_string(&close_span_event).unwrap();

            (key, span_key, kind, Some(data))
        }
    }
}

fn span_event_from_row(row: &Row<'_>) -> Result<SpanEvent, DbError> {
    let key = row.get(0)?;
    let span_key = row.get(1)?;
    let kind: String = row.get(2)?;
    let data: Option<String> = row.get(3)?;
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
    let fields = serde_json::to_string(&event.fields).unwrap();

    (key, resource_key, parent_id, parent_key, content, namespace, function, level, file_name, file_line, file_column, fields)
}

fn event_from_row(row: &Row<'_>) -> Result<Event, DbError> {
    let key = row.get(0)?;
    let resource_key = row.get(1)?;
    let parent_id: Option<String> = row.get(2)?;
    let parent_key = row.get(3)?;
    let content: String = row.get(4)?;
    let content = serde_json::from_str(&content).unwrap();
    let namespace = row.get(5)?;
    let function = row.get(6)?;
    let level: i32 = row.get(7)?;
    let file_name = row.get(8)?;
    let file_line = row.get(9)?;
    let file_column = row.get(10)?;
    let fields: String = row.get(11)?;
    let fields = serde_json::from_str(&fields).unwrap();

    Ok(Event {
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
        fields,
    })
}
