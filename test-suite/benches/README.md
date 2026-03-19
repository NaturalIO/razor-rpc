# Benchmark Tests

This directory contains benchmark tests comparing razor-rpc performance with gRPC (tonic) and volo-grpc.

## Prerequisites

- protoc (Protocol Buffers compiler)

```
sudo apt install -y protobuf-compiler
```

- Rust toolchain with tokio support

## Running Benchmarks

### Run gRPC and razor-rpc comparison

```bash
cargo bench -p razor-rpc-test --features "tokio grpc"
```

### Run all benchmarks (including volo)

```bash
cargo bench -p razor-rpc-test --features "tokio grpc volo"
```

### Run specific benchmark

```bash
cargo bench -p razor-rpc-test --features "tokio grpc" -- echo_1kb
```

## Benchmark Scenarios

### echo_1kb

Compares echo service performance with 1KB payload:
- **gRPC (tonic)**: Uses tonic with HTTP/2 transport
- **volo-grpc**: Uses volo framework (currently stub implementation)
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
│   ├── volo_grpc/
│   └── razor_rpc/
```

Open the HTML reports in your browser for visualizations.

## Known Issues

- **volo-grpc**: Currently has a stub implementation that returns immediately. Full implementation requires more complex setup due to volo's API complexity.
