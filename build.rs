fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/control.proto"], &["proto/"])
        .unwrap_or_else(|e| panic!("Failed to compile protos: {e}"));
}
