fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the bundled protoc — no system protobuf-compiler needed in CI or dev.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", &protoc);

    let include = protoc_bin_vendored::include_path()?;

    let mut config = prost_build::Config::new();
    // Do NOT blanket-derive serde on all types: prost_types::Timestamp doesn't
    // implement serde, so any generated struct that embeds it would fail to compile.
    config.protoc_arg("--experimental_allow_proto3_optional");

    // sapient_message.proto imports everything else transitively.
    // Include both the proto/ dir (for sapient_msg/*) and the vendored
    // protobuf well-known types dir (for google/protobuf/*).
    config.compile_protos(
        &["proto/sapient_msg/bsi_flex_335_v2_0/sapient_message.proto"],
        &["proto/", include.to_str().unwrap()],
    )?;

    println!("cargo:rerun-if-changed=proto/");
    Ok(())
}
