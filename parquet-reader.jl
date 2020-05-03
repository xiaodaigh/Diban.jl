using ParquetWriter
using Thrift

using Parquet, ParquetFiles,

load("c:/scratch/python-test.parquet")

p = ParFile("c:/scratch/python-test.parquet")
p

mutable struct Ghi2
    a::Vector{UInt8}
end

rc = RecCursor(p, 1:nrows(p), colnames(p), JuliaBuilder(p, Ghi2))

@which RecCursor(p, 1:nrows(p), colnames(p), JuliaBuilder(p, Ghi2))

using Debugger
Debugger.@enter ColCursor(par, 1:2, "a", 2)
@which ColCursor(par, 1:2, "a", 2)
@which RowCursor(par, 1:3, "a", 1)

rowgroups(par)
rowgroups(par, "a", 1:3)

rc.colcursors


rc.colstates
rc.colcursors[1]



record_state = iterate(rc)

while record_state != nothing
    global record_state
    record = record_state[1]
    state = record_state[2]
    println(record)
    record_state = iterate(rc, state)
end

rci, state = iterate(rc)

rci, state = iterate(rc, state)

rci, state = iterate(rc, state)

state

rci

a =  iterate(rc, state)

a == nothing

iterate(rc, state)


T_row_name = Symbol("RCType$(String(gensym())[3:end])")

@which schema(JuliaConverter(ParquetFiles), p, T_row_name)

conv = JuliaConverter(ParquetFiles)
par = p
schema_name = T_row_name
@which schema(conv, par.schema, schema_name)

sch = par.schema
@which Parquet.schema_to_julia_types(conv.to, sch, schema_name)

io = IOBuffer()
@which Parquet.schema_to_julia_types(io, sch, schema_name)

@which Parquet.schema_to_julia_types(io, sch.schema, schema_name)

sch.schema[1]
sch.schema[2]

typeof(sch.schema[1])

Parquet.isfilled(sch.schema[1], :_type)
Parquet.isfilled(sch.schema[2], :_type)

sch.schema[1].name

fieldnames(typeof(sch.schema[1]))


typestr = "begin\n" * String(take!(io)) * "\nend"
parsedtypes = Meta.parse(typestr)
Core.eval(mod, parsedtypes)







T_row = eval(:haha)

col_names = [:a]
col_types = [i <: Vector{UInt8} ? String : i for i in [String]]

T = NamedTuple{(col_names...,),Tuple{col_types...}}

rc = RecCursor(p, 1:nrows(p), colnames(p), JuliaBuilder(p, T_row))

it = ParquetNamedTupleIterator{T,T_row}(rc, nrows(p))

return it

using Debugger

using ParquetFiles

Debugger.@enter(load("c:/scratch/python-test.parquet"))

io = open("c:/scratch/python-test.parquet", "r")
io = open("C:/data/parquet-test/fannie_mae_perf_small/fannie_mae_perf_small.parquet")
io = open("c:/scratch/test.parquet")
#b = read(io)
#close(io)

using ParquetWriter

metadata("c:/scratch/test.parquet")

par = ParFile("c:/scratch/test.parquet")
Parquet.pages(par, filemetadata.row_groups[1].columns[1])

filemetadata

Parquet.PLAIN_JTYPES

const TYPES = (Bool, Int32, Int64, Int128, Float32, Float64, String, UInt8)

filemetadata.schema[1]._type
filemetadata.schema[2]._type

filemetadata

nrows(par)

pgs = Parquet.pages(par, filemetadata.row_groups[1].columns[1])

vals = values(par, pgs[1])

@which values(par, pgs[1])

@which bytes(pgs[1])

page =  pgs[1]
rawbytes = Parquet.bytes(page)
num_values = Parquet.page_num_values(page)

io = IOBuffer(rawbytes)

ctype = Parquet.coltype(page.colchunk)

ok = Parquet.read_plain_values(io, 131078, ctype)

data = page.data
codec = page.colchunk.meta_data.codec
uncompressed=true

@which Parquet.page_num_values(page)
@which Parquet.page_num_values(page.hdr)

Thrift.isfilled(page.hdr, :data_page_header_v2)
Thrift.isfilled(page.hdr, :data_page_header)
Thrift.isfilled(page.hdr, :dictionary_page_header)

@which Parquet.page_num_values(page.hdr.dictionary_page_header)

page.hdr.dictionary_page_header

codec != Parquet.CompressionCodec.UNCOMPRESSED
using Snappy
data = Snappy.uncompress(data)

SZ_PAR_MAGIC = 4#
SZ_FOOTER = 4

sz = filesize(io)
seek(io, sz -8)
len = read(io, Int32)

data_size = sz -  len -12
SZ_PAR_MAGIC + data_size
seek(io, SZ_PAR_MAGIC + data_size)
filemetadata = read_thrift(io, PAR2.FileMetaData)

sz = filesize(io)

seek(io, sz - SZ_PAR_MAGIC - SZ_FOOTER)

# read footer size as little endian signed Int32


pg = Parquet.pages(par, a.row_groups[1].columns[1])
values(par, pg)
par

load_parquet












a.row_groups[1].file_offset

seek(io, 113)

read_thrift(io, ParquetThrift.ColumnMetaData)


seek(io, 100)
position(io)
read_thrift(io, ParquetThrift.PageHeader )

ParquetThrift.DataPageHeader


a.row_groups[1].columns




a.row_groups

using Parquet

ParFile("c:/scratch/python-test.parquet")

using Test
@test String(b[1:4]) == "PAR1"
@test String(b[end-3:end]) == "PAR1"
footer_length = reinterpret(Int32, b[end-7:end-4])[1]

data = b[5:5+length(b)-4-4-4-footer_length-1]

footer = b[end-8-footer_length+1:end-8]

Int.(footer[1:4])
fileversion = reinterpret(Int32, footer[1:4])[1]

# next is the schema

# schema starts with a type
Int.(footer[5:8])
footer[5:8]
reinterpret(Int32, footer[5:8])[1]
593 613
findall(footer .== UInt('y'))

footer[594]
footer[614]

for i = 1:length(footer)-7
    if reinterpret(Int64, footer[i:i+7])[1] == 1
        println(i)
    end
end
