using Test
using Diban
using DataFrames

tbl = DataFrame(
    int32 = Int32[-1, 0, 1],
    int64 = Int64[-10, 0, 10],
    float32 = Float32[-0.5, 0, 0.5],
    float64 = Float64[-0.5, 0, 0.5],
    bool = [true, false, true],
    string = ["abc", "def", "ghi"],
    int32m = Union{Missing, Int32}[-1, missing, 1],
    int64m = Union{Missing, Int64}[-10, missing, 10],
    float32m = Union{Missing, Float32}[-0.5, missing, 0.5],
    float64m = Union{Missing, Float64}[-0.5, missing, 0.5],
    boolm = Union{Missing, Bool}[true, missing, false],
    stringm = Union{Missing, String}["abc", missing, "ghi"],
)

path = "tmp.parquet"
#path = "c:/scratch\\tmp.parquet"
# write_parquet(path, tbl; compression_codec = "lz4")
# read_parquet(path)

write_parquet(path, tbl; compression_codec = "snappy")
read_parquet(path)

write_parquet(path, tbl; compression_codec = "uncompressed")
read_parquet(path)

write_parquet(path, tbl; compression_codec = "zstd")
read_parquet(path)

write_parquet(path, tbl; compression_codec = "gzip")
read_parquet(path)

try
    rm(path)
catch
    # do nothing
    sleep(8)
    rm(path)
end
