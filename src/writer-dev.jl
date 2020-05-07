using Diban
using Diban: COMPRESSION_CODEC_CODE, write_col_dict, write_col_chunk, create_schema_parent_node, create_col_schema, COL_TYPE_CODE, write_thrift
using Tables
using Parquet: PAR2;
using Thrift, DataFrames
using Snappy, CodecZstd
using LittleEndianBase128
using LittleEndianBase128: encode


tbl = DataFrame(abc = Int32.(1:3))
colname = "abc"
colvals = tbl[!, colname]

close(fileio)
path = "c:/scratch/tmp.parquet"
fileio = open(path, "w")
write(fileio, "PAR1")
col_info = write_col(fileio, colvals, colname)

# write the column metadata


# can probably write the metadata right after the data chunks
col_meta = PAR2.ColumnMetaData()
col_meta



set_field!(col_meta, :_type, COL_TYPE_CODE[eltype(colvals)])
# these are all the fields
# TODO collect all the encodings used
set_field!(col_meta, :encodings, Int32[2, 0, 3])
set_field!(col_meta, :path_in_schema, [colname])
set_field!(col_meta, :codec, COMPRESSION_CODEC_CODE["snappy"])
set_field!(col_meta, :num_values, length(colvals))

set_field!(col_meta, :total_uncompressed_size, col_info.uncompressed_size)
set_field!(col_meta, :total_compressed_size, col_info.compressed_size)

set_field!(col_meta, :data_page_offset, col_info.data_page_offset)
set_field!(col_meta, :dictionary_page_offset, col_info.dictionary_page_offset)

col_meta

col_meta_offset = position(fileio)
write_thrift(fileio, col_meta)





# now all the data is written we write the filemetadata
# finalise it by writing the filemetadata

filemetadata = PAR2.FileMetaData()









set_field!(filemetadata, :version, 1)

## prepare a schema
ncol = 1
schemas = [create_schema_parent_node(ncol), create_col_schema(eltype(colvals), colname)]

set_field!(filemetadata, :version, 1)
set_field!(filemetadata, :schema, schemas)
set_field!(filemetadata, :num_rows, length(colvals))
set_field!(filemetadata, :created_by, "Diban.jl")

# create row_groups
row_group = PAR2.RowGroup()

# the
colchunk = PAR2.ColumnChunk()

set_field!(colchunk, :file_offset, col_meta_offset)
set_field!(colchunk, :meta_data, col_meta)
clear(colchunk, :offset_index_offset)
clear(colchunk, :offset_index_length)
clear(colchunk, :column_index_offset)
clear(colchunk, :column_index_length)
colchunk

set_field!(row_group, :columns, [colchunk])
set_field!(row_group, :total_byte_size, Int64(sum(x->x.meta_data.total_compressed_size, [colchunk])))
set_field!(row_group, :num_rows, length(colvals))
set_field!(row_group, :file_offset, col_info.dictionary_page_offset)
set_field!(row_group, :total_compressed_size, Int64(sum(x->x.meta_data.total_compressed_size, [colchunk])))

set_field!(filemetadata, :row_groups, [row_group])

position_before_filemetadata_write = position(fileio)

write_thrift(fileio, filemetadata)

filemetadata_size = position(fileio) - position_before_filemetadata_write

write(fileio, Int32(filemetadata_size))
write(fileio, "PAR1")
close(fileio)

Diban.metadata(path)












read_parquet(path)




### example
ff = open("c:/scratch/nihao.parquet")

String(read(ff, 4))
fsz = filesize("c:/scratch/nihao.parquet")
seek(ff, fsz-8)

using Parquet: read_thrift
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
