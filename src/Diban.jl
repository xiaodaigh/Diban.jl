module Diban

######################## begin:Taken from Parquet.jl
using Thrift
using ProtoBuf
using Snappy
using CodecZlib, CodecZstd
using MemPool

import Base: show, open, close, values
import Thrift: isfilled

# export is_par_file, ParFile, show, nrows, ncols, rowgroups, columns, pages, bytes, values, colname, colnames
# export SchemaConverter, schema, JuliaConverter, ThriftConverter, ProtoConverter
# export RowCursor, ColCursor, RecCursor
# export AbstractBuilder, JuliaBuilder

# package code goes here
include("PAR2/PAR2.jl")
using .PAR2
include("codec.jl")
include("schema.jl")
include("reader.jl")
include("cursor.jl")
include("show.jl")
######################## end:Taken from Parquet.jl


# package code goes here
include("encoding.jl")
# include("thrift/parquet.jl")
include("metadata.jl")

include("column_reader.jl")
include("read_parquet.jl")

# include("BitPackedIterator.jl")

include("writer_consts.jl")
include("writer.jl")


# from column_reader.jl
# export read_column

# from metadata.jl
# export metadata

# from read_parquet.jl
export read_parquet

# # from BitPackedIterator
# export BitPackedIterator
# export iterate, length

# from writer.jl
export write_parquet


end # module
