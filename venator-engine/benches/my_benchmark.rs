use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::path::Path;
use venator_engine::engine::SyncEngine;
use venator_engine::filter::{FilterPredicate, Order, Query};
use venator_engine::storage::{BatchedStorage, FileStorage, TransientStorage};
use venator_engine::{NewEvent, NewResource, SourceKind, Timestamp, Value};

fn count_events_benchmark(c: &mut Criterion) {
    let file_storage = FileStorage::new(Path::new("./benches/test.vena.db"));
    let file_engine = SyncEngine::new(file_storage).unwrap();

    let mut mem_storage = TransientStorage::new();
    file_engine.copy_dataset(&mut mem_storage).unwrap();
    let mem_engine = SyncEngine::new(mem_storage).unwrap();

    let make_query = |q_str| Query {
        filter: FilterPredicate::parse(q_str).unwrap(),
        order: Order::Asc,
        limit: 0,
        start: Timestamp::MIN,
        end: Timestamp::MAX,
        previous: None,
    };

    drop(file_engine);

    let q_str = "";
    let q = make_query(q_str);
    c.bench_function(&format!("count events"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_event_count(q.clone())), 16537))
    });

    let q_str = "#level: >=TRACE";
    let q = make_query(q_str);
    c.bench_function(&format!("count events: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_event_count(q.clone())), 16537))
    });

    let q_str = "#level: >=INFO";
    let q = make_query(q_str);
    c.bench_function(&format!("count events: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_event_count(q.clone())), 1511))
    });

    let q_str = "#level: >=TRACE @http.status_code: 200";
    let q = make_query(q_str);
    c.bench_function(&format!("count events: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_event_count(q.clone())), 14990))
    });

    let q_str = "#level: >=TRACE @http.status_code: !200";
    let q = make_query(q_str);
    c.bench_function(&format!("count events: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_event_count(q.clone())), 1547))
    });

    let q_str = "#level: >=TRACE @http.status_code: >200";
    let q = make_query(q_str);
    c.bench_function(&format!("count events: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_event_count(q.clone())), 1377))
    });

    let q_str = "#level: >=TRACE @http.method: POST";
    let q = make_query(q_str);
    c.bench_function(&format!("count events: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_event_count(q.clone())), 9261))
    });

    let q_str = "#level: >=TRACE @http.method: !POST";
    let q = make_query(q_str);
    c.bench_function(&format!("count events: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_event_count(q.clone())), 7276))
    });

    let q_str = "#level: >=TRACE @http.status_code: 200 @http.method: POST";
    let q = make_query(q_str);
    c.bench_function(&format!("count events: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_event_count(q.clone())), 7884))
    });

    let q_str = "#level: >=INFO #content: /^creating/";
    let q = make_query(q_str);
    c.bench_function(&format!("count events: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_event_count(q.clone())), 23))
    });

    let q_str = "#level: >=TRACE #content: /request$/";
    let q = make_query(q_str);
    c.bench_function(&format!("count events: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_event_count(q.clone())), 15026))
    });

    let q_str = "#level: >=INFO #content: creating*";
    let q = make_query(q_str);
    c.bench_function(&format!("count events: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_event_count(q.clone())), 23))
    });

    let q_str = "#level: >=TRACE #content: *request";
    let q = make_query(q_str);
    c.bench_function(&format!("count events: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_event_count(q.clone())), 15026))
    });
}

fn count_spans_benchmark(c: &mut Criterion) {
    let file_storage = FileStorage::new(Path::new("./benches/test.vena.db"));
    let file_engine = SyncEngine::new(file_storage).unwrap();

    let mut mem_storage = TransientStorage::new();
    file_engine.copy_dataset(&mut mem_storage).unwrap();
    let mem_engine = SyncEngine::new(mem_storage).unwrap();

    let make_query = |q_str| Query {
        filter: FilterPredicate::parse(q_str).unwrap(),
        order: Order::Asc,
        limit: 0,
        start: Timestamp::MIN,
        end: Timestamp::MAX,
        previous: None,
    };

    drop(file_engine);

    let q_str = "";
    let q = make_query(q_str);
    c.bench_function(&format!("count spans"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_span_count(q.clone())), 98197))
    });

    let q_str = "#level: >=TRACE";
    let q = make_query(q_str);
    c.bench_function(&format!("count spans: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_span_count(q.clone())), 98197))
    });

    let q_str = "#level: >=INFO";
    let q = make_query(q_str);
    c.bench_function(&format!("count spans: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_span_count(q.clone())), 98197))
    });

    let q_str = "#level: >=TRACE @http.status_code: 200";
    let q = make_query(q_str);
    c.bench_function(&format!("count spans: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_span_count(q.clone())), 93084))
    });

    let q_str = "#level: >=TRACE @http.status_code: !200";
    let q = make_query(q_str);
    c.bench_function(&format!("count spans: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_span_count(q.clone())), 5113))
    });

    let q_str = "#level: >=TRACE @http.status_code: >200";
    let q = make_query(q_str);
    c.bench_function(&format!("count spans: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_span_count(q.clone())), 4620))
    });

    let q_str = "#level: >=TRACE @http.method: POST";
    let q = make_query(q_str);
    c.bench_function(&format!("count spans: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_span_count(q.clone())), 48133))
    });

    let q_str = "#level: >=TRACE @http.method: !POST";
    let q = make_query(q_str);
    c.bench_function(&format!("count spans: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_span_count(q.clone())), 50064))
    });

    let q_str = "#level: >=TRACE @http.status_code: 200 @http.method: POST";
    let q = make_query(q_str);
    c.bench_function(&format!("count spans: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_span_count(q.clone())), 43513))
    });

    let q_str = "#level: >=TRACE @service.name: /^a/";
    let q = make_query(q_str);
    c.bench_function(&format!("count spans: {q_str}"), |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_span_count(q.clone())), 97704))
    });
}

fn insert_events_benchmark(c: &mut Criterion) {
    fn now() -> Timestamp {
        {
            use std::time::{SystemTime, UNIX_EPOCH};
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("now should not be before the UNIX epoch")
                .as_micros();

            let timestamp = u64::try_from(timestamp)
                .expect("microseconds shouldn't exceed a u64 until the year 586,912 AD");

            Timestamp::new(timestamp).expect("now should not be at the UNIX epoch")
        }
    }

    let create_resource = || NewResource {
        attributes: BTreeMap::from_iter([
            ("attr1".into(), Value::Str("value1".into())),
            ("attr2".into(), Value::Str("value2".into())),
            ("attr3".into(), Value::Str("value3".into())),
        ]),
    };

    let create_event = |resource_key| NewEvent {
        kind: SourceKind::Opentelemetry,
        resource_key,
        timestamp: now(),
        span_id: None,
        content: Value::Str("this is a test message".into()),
        namespace: None,
        function: Some("crate::main".into()),
        level: venator_engine::Level::Debug,
        file_name: Some("src/main.rs".into()),
        file_line: Some(12),
        file_column: Some(4),
        attributes: BTreeMap::from_iter([
            ("attr4".into(), Value::Str("value4".into())),
            ("attr5".into(), Value::Str("value5".into())),
            ("attr6".into(), Value::Str("value6".into())),
        ]),
    };

    c.bench_function("write events to mem", |b| {
        let storage = TransientStorage::new();
        let mut engine = SyncEngine::new(storage).unwrap();

        let resource = engine.insert_resource(create_resource()).unwrap();

        b.iter(|| engine.insert_event(create_event(resource)))
    });

    c.bench_function("write events to sqlite", |b| {
        let storage = FileStorage::new(Path::new("./benches/temp.vena.db"));
        let mut engine = SyncEngine::new(storage).unwrap();

        let resource = engine.insert_resource(create_resource()).unwrap();

        b.iter(|| engine.insert_event(create_event(resource)));

        drop(engine);
        std::fs::remove_file("./benches/temp.vena.db").unwrap();
    });

    c.bench_function("batch write events to sqlite", |b| {
        let storage = FileStorage::new(Path::new("./benches/temp.vena.db"));
        let storage = BatchedStorage::new(storage);
        let mut engine = SyncEngine::new(storage).unwrap();

        let resource = engine.insert_resource(create_resource()).unwrap();

        b.iter(|| {
            for _ in 0..100 {
                let _ = engine.insert_event(create_event(resource));
            }

            let _ = engine.sync();
        });

        drop(engine);
        std::fs::remove_file("./benches/temp.vena.db").unwrap();
    });
}

criterion_group!(
    benches,
    count_events_benchmark,
    count_spans_benchmark,
    insert_events_benchmark
);
criterion_main!(benches);
