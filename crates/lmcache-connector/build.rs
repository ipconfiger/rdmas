fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "director")]
    {
        tonic_build::compile_protos("/home/alex/Projects/Rectifiers/proto/director.proto")?;
    }
    Ok(())
}
