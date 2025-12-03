use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use std::path::Path;
use venator_engine::engine::SyncEngine;
use venator_engine::filter::{FilterPredicate, Order, Query};
use venator_engine::storage::{FileStorage, TransientStorage};
use venator_engine::Timestamp;

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

criterion_group!(benches, count_events_benchmark, count_spans_benchmark);
criterion_main!(benches);
