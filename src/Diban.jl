module Diban

# package code goes here
include("encoding.jl")
# include("thrift/parquet.jl")
include("metadata.jl")

include("column_reader.jl")
include("read_parquet.jl")

include("BitPackedIterator.jl")

# from column_reader.jl
export read_column

# from#"thrift/parquet.jl"
# using .ParquetThrift
# export ParquetThrift

# from metadata.jl
# export metadata

# from read_parquet.jl
export read_parquet

# from BitPackedIterator
export BitPackedIterator
export iterate, length

end # module
