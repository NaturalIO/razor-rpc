fn main() -> std::io::Result<()> {
    #[cfg(feature = "grpc")]
    {
        tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .compile_protos(&["proto/benchmark.proto"], &["proto"])?;
    }

    #[cfg(feature = "volo")]
    {
        // Volo-grpc - compile all services in the proto file
        volo_build::Builder::protobuf()
            .add_service("proto/benchmark.proto")
            .include_dirs(vec!["proto".into()])
            .filename("volo_benchmark.rs".into())
            .write()
            .expect("volo build failed");
    }
    Ok(())
}
