using Snappy
##### set a path
path = "c:/scratch/test.parquet"

##### load metadata
metadata(path)

#### continues to try

io = open(path)

# the dicvtionary page starts here at 4
seek(io, 4); dict_page_header = read_thrift(io, PAR2.PageHeader)

compressed_length = dict_page_header.compressed_page_size

data_compressed = read(io, compressed_length)
data_uncompressed = Snappy.uncompress(b)

data_actual = reinterpret(Int64, data_uncompressed)

next_page = read_thrift(io, PAR2.PageHeader)

Parquet.coltype(page.colchunk)

io = IOBuffer(data_unc)
using Parquet
@which Parquet.read_plain_values(io, num_values, ctype)

typ = ctype
@which Parquet.read_plain(io, typ)

Parquet.PLAIN_JTYPES[typ+1]

@which Parquet.read_fixed(io, Int64)

ret = convert(UInt64, 0)
N = 8
T = UInt64
for n in 0:(N-1)
    global ret
    byte = convert(T, read(io, UInt8))
    ret |= (byte << *(8,n))
end
reinterpret(Int64, ret)

res = [read(io, Int64) for i in 1:389490]

Int(length(data_unc)//8)



pos= position(io)
read_thrift(io, PAR2.PageHeader)


data = read(io, 34966)
data_unc = Snappy.uncompress(data)

reinterpret(Int32, data_unc)


seek(io, 4)
read_thrift(io, PAR2.PageHeader)


num_values
read_plain_values(io, num_values, 6)

using StatsBase
countmap([pg.hdr._type for pg in pgs])


Debugger.@enter read_column(path, 1)
