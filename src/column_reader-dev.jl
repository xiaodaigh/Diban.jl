# R Code to generate data
# ```r
# arrow::write_parquet(
#   data.frame(
#     a = c(1, 3, NA, 4, NA, 1, 1, NA, 1:8),
#     b = c(1:9, 1:7)),
#   "c:/scratch/nihao.parquet")
#   ```

using Diban
using Diban: TYPES, read_column
using Parquet
using Parquet: SZ_PAR_MAGIC, SZ_FOOTER, PAR2, read_thrift

path = "c:/scratch/test.parquet"

#@time read_parquet(path)

path = "c:/scratch/nihao.parquet"

path = "tmp.parquet"


metadata = Diban.metadata(path)












fileio=open(path)

metadata.row_groups[1].columns[1].meta_data.dictionary_page_offset

seek(fileio, metadata.row_groups[1].columns[1].meta_data.dictionary_page_offset)


ph = Parquet.read_thrift(fileio, PAR2.PageHeader)








position(fileio)

compressed_data = read(fileio, ph.compressed_page_size)

using Snappy

uncompressed_data = Snappy.uncompress(compressed_data)

reinterpret(Int64, uncompressed_data)

ph2 = Parquet.read_thrift(fileio, PAR2.PageHeader)










compressed_data = read(fileio, ph2.compressed_page_size)
uncompressed_data = Snappy.uncompress(compressed_data)
uncompressed_data








io = IOBuffer(uncompressed_data)

# the definition data is encoded in the `encoded_data_length` bytes
encoded_data_length = Int(read(io, UInt32))

# the next set of data is a LEB128 unsigned int coded by that
# althought it is possible for the number be larger 2^31-1, in practice it is
# not encouraged, and hence not supported by this algorithm
using Parquet: _read_varint;
header = Parquet._read_varint(io, UInt32)
# if the last binary digit is 1 then the next bits are binary encoded
# otherwise it's rle encoded

bit_pack_encoded = (header & UInt(1)) == 1

if bit_pack_encoded
	bit_packed_run_length = header >> 1
	bit_packed_data = read(io, bit_packed_run_length)
	reduce(vcat, digits.(bit_packed_data, base = 2, pad=8))
else
	rle_run_length = header >> 1
	repeated_value = read(io, UInt8)
end

Int(rle_run_length), Int(repeated_value)

position(io)

encoded_data = read(io)

dataio = IOBuffer(encoded_data)

bitwidth = read(dataio, UInt8)

header = Parquet._read_varint(dataio, UInt32)

string(0x10, base=2, pad=8)
string(0x32, base=2, pad=8)

pos = 0

bytes_to_skip, bits_to_shift = divrem(pos, 8)




bp = BitPackedIterator(read(dataio, 2), 3)

iterate(bp)

for value in bp
	println(value)
end

# TODO what happens if the bitwidth is larger than 8?
