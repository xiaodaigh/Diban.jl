"""To keep the writer constants"""

const COMPRESSION_CODEC_CODE = Dict(
  "UNCOMPRESSED" => 0,
  "SNAPPY" => 1,
  "GZIP" => 2,
  "LZO" => 3,
  "BROTLI" => 4,
  "LZ4" => 5,
  "ZSTD" => 6
)

const CONVERTED_COL_TYPE_CODE_UTF8 = 0
const PLAIN_ENCODING = 0

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
    Float64 => 5,
    String => 6, # BYTE_ARRAY
    # FIXED_LEN_BYTE_ARRAY => 7,
    )
