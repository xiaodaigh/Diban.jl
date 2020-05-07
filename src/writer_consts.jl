"""To keep the writer constants"""

const COMPRESSION_CODEC_CODE = Dict(
  "snappy" => 1
)


const PAGE_TYPE = Dict(
"DATA_PAGE" => 0,
"INDEX_PAGE" => 1,
"DICTIONARY_PAGE" => 2,
"DATA_PAGE_V2" => 3,
)


const COL_TYPE_CODE = Dict{DataType, Int32}(
    Bool => 0,
    Int32 => 1,
    Int64 => 2,
    #INT96 => 3,  // deprecated, only used by legacy implementations.
    Float32 => 4,
    Float64 => 5#,
    # BYTE_ARRAY => 6,
    # FIXED_LEN_BYTE_ARRAY => 7,
    )
