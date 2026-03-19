# Benchmark Tests

This directory contains benchmark tests comparing razor-rpc performance with gRPC.

## Prerequisites

- protoc (Protocol Buffers compiler)

```
sudo apt install -y protobuf-compiler
```

- Rust toolchain with tokio support

## Running Benchmarks

### Run all benchmarks

```bash
cargo bench -p razor-rpc-test --features tokio
```

### Run specific benchmark

```bash
cargo bench -p razor-rpc-test --features tokio -- echo_1kb
```

## Benchmark Scenarios

### echo_1kb

Compares echo service performance with 1KB payload:
- **gRPC**: Uses tonic with HTTP/2 transport
- **razor-rpc**: Uses msgpack codec with TCP transport

Default configuration:
- Concurrency: 10 clients
- Requests per client: 100
- Total requests: 1000
- Payload size: 1024 bytes

## Test Structure

```
benches/
├── grpc_compare.rs    # Main benchmark file
└── README.md          # This file
```

The benchmark uses Criterion.rs for statistical analysis and generates reports in `target/criterion/`.

## Viewing Results

After running benchmarks, you can view detailed results:

```
target/criterion/
├── echo_1kb/
│   ├── grpc/
│   └── razor_rpc/
```

Open the HTML reports in your browser for visualizations.
