fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/sync_graph.proto")?;
    Ok(())
}
