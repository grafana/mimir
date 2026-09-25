fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    unsafe { std::env::set_var("PROTOC", protoc) };
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .codec_path("crate::wire::FastProstCodec")
        .bytes(".cortexpb.LabelPair.name")
        .bytes(".cortexpb.LabelPair.value")
        .bytes(".cortex.Chunk.data")
        .bytes(".cortex.QueryStreamResponse.encoded_response")
        .compile_protos(&["proto/mimir.proto", "proto/ingester.proto"], &["proto"])?;
    Ok(())
}
