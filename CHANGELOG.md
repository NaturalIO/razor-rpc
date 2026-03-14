# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

### Removed

### Changed

## [0.7.0] - 2026-03-14

### Changed

- ClientFacts & ServerFacts no lonnger inherits orb::AsyncRuntime.

- All ClientCaller use explicit runtime parameter to spawn.

- Server listen and close need explicit runtime parameter.

## [0.6.0] - 2026-03-13

### Changed

- razor-rpc-macros: Provide `endpoint_client!(ClientName)` for generate client, `#[endpoint_async(ClientName)]` to impl service trait for client.
  Allow impl multiple service for a client.

- razor-rpc: Change AsyncEndpoint & BlockEndpoint from struct to helper trait.

- razor-stream: Simplify FailoverPool to break cycle ref. Add Clone for FailoverPool.

### Removed

- razor-rpc-macros: Drop the support of #[service] on impl block

## [0.5.0] - 2026-03-12

### Changed

- Migrate WaitGroup to crossfire-3.1

- Add ClientFacts::get_timestamp() for user to overwrite

- Improve doc on error module

## [0.4.0]

### Changed

- Upgrade to crossfire-3.0.

- Upgrade orb dep.

## [0.3.0]

### Changed

- Project rename to razor-rpc

- Split runtime traits from tokio and smol plugins to `orb`, `orb-tokio`, and `orb-smol` crates

- Remove core crate (split into codec and stream)

### Fixed

## [0.2.0] - 2025-10-26

### Added

- rpc:
    - Finish api interface client and server macro, and Inline dispatch

- stream:
    - Add ClientPool as connection pool
    - Add FailoverPool for high availability, which wraps user ClientFacts
    - Add ClientCaller and ClientCallerBlocking traits

- core:
    - Add RpcErrCodec trait to support user custom error type
    - Add spawn_detach() to AsyncIO trait
    - Codec: Add encode_into()

- tokio:
    - TokioRT now captures a runtime handle on new

- smol:
    - SmolRT now support new_global() or new() with specified Executor

### Changed

- stream:
    - Rename Factory -> Facts
    - Remove Transport from ClientFacts and ServerFacts (now depend on AsyncIO generic)
    - ClientFacts / ServerFacts now inherits AsyncIO trait
    - ServerFacts Removes RespTask and Codec, moved to Dispatch trait
    - Refactor ClientTask and ServerTask trait to support custom error types, and reduce alloc on encode.

- tcp:
    - Optimise io with buffer

- codec:
    - Adapt to new encode_into() interface
