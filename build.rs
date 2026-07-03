fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::Config::new()
        .compile_protos(&["proto/msg.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/msg.proto");
    Ok(())
}