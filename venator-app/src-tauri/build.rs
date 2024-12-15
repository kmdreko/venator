fn main() {
    tauri_build::build();

    // OpenTelemetry definitions are not expected to change, and thus for the
    // sake of avoiding unnecessary work the Rust files are only generated once.
    // The build configuration below was used if it is ever needed again.

    // tonic_build::configure()
    //     .build_client(false)
    //     .out_dir("src/ingress/otel/")
    //     .compile_protos(
    //         &[
    //             "opentelemetry/proto/collector/logs/v1/logs_service.proto",
    //             "opentelemetry/proto/collector/trace/v1/trace_service.proto",
    //             "opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
    //         ],
    //         &["."],
    //     )
    //     .unwrap();
}
