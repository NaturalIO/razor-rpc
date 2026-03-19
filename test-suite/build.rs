fn main() -> std::io::Result<()> {
    #[cfg(feature = "tokio")]
    {
        tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .compile_protos(&["proto/benchmark.proto"], &["proto"])?;
    }
    Ok(())
}
