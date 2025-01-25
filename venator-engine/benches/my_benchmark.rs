use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use std::path::Path;
use venator_engine::engine::SyncEngine;
use venator_engine::filter::{FilterPredicate, Order, Query};
use venator_engine::storage::{FileStorage, TransientStorage};
use venator_engine::Timestamp;

fn event_counts_benchmark(c: &mut Criterion) {
    let file_storage = FileStorage::new(Path::new("./benches/test.vena.db"));
    let file_engine = SyncEngine::new(file_storage).unwrap();

    let mut mem_storage = TransientStorage::new();
    file_engine.copy_dataset(&mut mem_storage).unwrap();
    let mem_engine = SyncEngine::new(mem_storage).unwrap();

    drop(file_engine);

    let query = Query {
        filter: FilterPredicate::parse("").unwrap(),
        order: Order::Asc,
        limit: 0,
        start: Timestamp::MIN,
        end: Timestamp::MAX,
        previous: None,
    };

    c.bench_function("count all events", |b| {
        b.iter(|| {
            assert_eq!(
                black_box(mem_engine.query_event_count(query.clone())),
                16537
            )
        })
    });

    let query = Query {
        filter: FilterPredicate::parse("#level: >=INFO").unwrap(),
        order: Order::Asc,
        limit: 0,
        start: Timestamp::MIN,
        end: Timestamp::MAX,
        previous: None,
    };

    c.bench_function("count all events with level", |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_event_count(query.clone())), 1511))
    });

    let query = Query {
        filter: FilterPredicate::parse("#level: >=INFO #content: /^logging in/").unwrap(),
        order: Order::Asc,
        limit: 0,
        start: Timestamp::MIN,
        end: Timestamp::MAX,
        previous: None,
    };

    c.bench_function("count all events with level and regex", |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_event_count(query.clone())), 874))
    });
}

fn span_counts_benchmark(c: &mut Criterion) {
    let file_storage = FileStorage::new(Path::new("./benches/test.vena.db"));
    let file_engine = SyncEngine::new(file_storage).unwrap();

    let mut mem_storage = TransientStorage::new();
    file_engine.copy_dataset(&mut mem_storage).unwrap();
    let mem_engine = SyncEngine::new(mem_storage).unwrap();

    drop(file_engine);

    let query = Query {
        filter: FilterPredicate::parse("").unwrap(),
        order: Order::Asc,
        limit: 0,
        start: Timestamp::MIN,
        end: Timestamp::MAX,
        previous: None,
    };

    c.bench_function("count all spans", |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_span_count(query.clone())), 98197))
    });

    let query = Query {
        filter: FilterPredicate::parse("#level: >=INFO").unwrap(),
        order: Order::Asc,
        limit: 0,
        start: Timestamp::MIN,
        end: Timestamp::MAX,
        previous: None,
    };

    c.bench_function("count all spans with level", |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_span_count(query.clone())), 98197))
    });

    let query = Query {
        filter: FilterPredicate::parse("#level: >=INFO @service.name: /^a/").unwrap(),
        order: Order::Asc,
        limit: 0,
        start: Timestamp::MIN,
        end: Timestamp::MAX,
        previous: None,
    };

    c.bench_function("count all spans with level and regex", |b| {
        b.iter(|| assert_eq!(black_box(mem_engine.query_span_count(query.clone())), 97704))
    });
}

criterion_group!(benches, event_counts_benchmark, span_counts_benchmark);
criterion_main!(benches);
