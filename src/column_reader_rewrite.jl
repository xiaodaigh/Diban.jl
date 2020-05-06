# meant to be a faster version of column reader
path = "C:/git/parquet-data-collection/dsd50p.parquet"

filemetadata = metadata(path)

io = open(path)

seek(io, 4)

read_thrift(io, PAR2.PageHeader)

using Snappy







compressed_page = read(io, 2461)

uncompressed_page = Snappy.uncompress(compressed_page)

dict = reinterpret(Float64, uncompressed_page)

next_page = read_thrift(io, PAR2.PageHeader)


dict


a = read_parquet("C:/git/parquet-data-collection/dsd50p.parquet")





compressed_page = read(io, 36328)

uncompressed_page = Snappy.uncompress(compressed_page)

Int.(uncompressed_page)

dict[140]

Int(uncompressed_page[5])


# every number here represents a encoding
bit_width = uncompressed_page[1]

iobuffer = IOBuffer(uncompressed_page)

bit_width = Int(read(iobuffer, UInt8))

data_length = Int(ntoh(read(iobuffer, UInt32)))

using LittleEndianBase128

LittleEndianBase128.decode(uncompressed_page[6:9])

using LittleEndianBase128: encode

encode(UInt(1))


uncompressed_page[6:9]


Int(28998//2) # bytes are using to read from dictionary

indices = (uncompressed_page[2:Int(28998//2)+1] .<< 4) .>> 4

indices = uncompressed_page[2:Int(28998//2)+1] .& UInt8(15)

indices2 = uncompressed_page[2:Int(28998//2)+1] .>> 4

dict[indices .+ 1]

dict[indices2 .+ 1]

dict[(uncompressed_page[2:Int(28998//2)+1] .>> 4) .+ 1]
