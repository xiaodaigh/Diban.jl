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

path = "c:/scratch/tmp.parquet"
write_parquet(path, tbl)
a = read_parquet(path)





Diban.metadata(path)

# using Diban: COMPRESSION_CODEC_CODE, write_col_dict, write_col_chunk,
#     create_schema_parent_node, create_col_schema, COL_TYPE_CODE, write_thrift, PAR2

using Thrift
using Tables
using Snappy, CodecZstd
using LittleEndianBase128

using LittleEndianBase128: encode


read_parquet("c:/scratch/abc.parquet")

Diban.metadata("c:/scratch/abc.parquet")

a.refs



a = categorical(["a",missing,"b"])

using DataAPI

DataAPI.refarray(a)
DataAPI.levels(a)
DataAPI.refpool(a)

DataAPI.defaultarray(eltype(a), 1)




io = open("c:/scratch/tmp.parquet")

sz = filesize("c:/scratch/tmp.parquet")

seek(io, sz - 4)

read(io, 4) |> String

seek(io, sz - 8)
metasize = read(io, UInt32)  |> Int

data_size = sz - 12 - metasize

seek(io, 4+data_size)

read_thrift(io, PAR2.FileMetaData)

close(io)




Diban.metadata(path)
ParFile(path)

write_parquet(path, tbl; compression_codec = "lz4")
write_parquet(path, tbl; compression_codec = "zstd")
write_parquet(path, tbl; compression_codec = "gzip")
write_parquet(path, tbl; compression_codec = "uncompressed")

Diban.metadata("c:/scratch/abc.parquet")














io=open("c:/scratch\\abc.parquet")
seek(io, 4)

ph = read_thrift(io, PAR2.PageHeader)







data = read(io, ph.compressed_page_size)
udata = Snappy.uncompress(data)

string(data[end], base=2)


close(io)









a = BitArray(rand(Bool, 65))

io=IOBuffer()
write(io, a)

seek(io, 0)


ok = read(io)
close(io)

string(ok[1], base=2, pad=8)
string(ok[2], base=2, pad=8)
string(ok[3], base=2, pad=8)

colname = "abc"
colvals = tbl[!, colname]

#close(fileio)


Diban.metadata(path)

Diban.metadata("c:/scratch/abc.parquet")












read_parquet(path)




### example
path = "c:/scratch/abc.parquet"
Diban.metadata(path)

ff = open(path)

String(read(ff, 4))
fsz = filesize(path)
seek(ff, fsz-8)

metadata_size = Int(read(ff, UInt32))

data_size = fsz - 12 - metadata_size
seek(ff, 4 + data_size)

example_schema = read_thrift(ff, PAR2.FileMetaData)
close(ff)


close(fileio)

ff = open("tmp.parquet")
String(read(ff, 4))
dh = read_thrift(ff, PAR2.PageHeader)
tmpio = read(ff, dh.compressed_page_size) |> Snappy.uncompress |> IOBuffer
read(tmpio, Int)
read(tmpio, Int)
read(tmpio, Int)
dh = read_thrift(ff, PAR2.PageHeader)

close(ff)

# write encoded data
