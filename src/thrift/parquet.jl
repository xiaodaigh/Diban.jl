#
# Autogenerated by Thrift Compiler (0.11.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING


module ParquetThrift

using Thrift
import Thrift.process, Thrift.meta, Thrift.distribute




export meta
export _Type # enum
export ConvertedType # enum
export FieldRepetitionType # enum
export Encoding # enum
export CompressionCodec # enum
export PageType # enum
export BoundaryOrder # enum
export Statistics # struct
export StringType # struct
export UUIDType # struct
export MapType # struct
export ListType # struct
export EnumType # struct
export DateType # struct
export NullType # struct
export DecimalType # struct
export MilliSeconds # struct
export MicroSeconds # struct
export NanoSeconds # struct
export TimeUnit # struct
export TimestampType # struct
export TimeType # struct
export IntType # struct
export JsonType # struct
export BsonType # struct
export LogicalType # struct
export SchemaElement # struct
export DataPageHeader # struct
export IndexPageHeader # struct
export DictionaryPageHeader # struct
export DataPageHeaderV2 # struct
export SplitBlockAlgorithm # struct
export BloomFilterAlgorithm # struct
export XxHash # struct
export BloomFilterHash # struct
export Uncompressed # struct
export BloomFilterCompression # struct
export BloomFilterHeader # struct
export PageHeader # struct
export KeyValue # struct
export SortingColumn # struct
export PageEncodingStats # struct
export ColumnMetaData # struct
export EncryptionWithFooterKey # struct
export EncryptionWithColumnKey # struct
export ColumnCryptoMetaData # struct
export ColumnChunk # struct
export RowGroup # struct
export TypeDefinedOrder # struct
export ColumnOrder # struct
export PageLocation # struct
export OffsetIndex # struct
export ColumnIndex # struct
export AesGcmV1 # struct
export AesGcmCtrV1 # struct
export EncryptionAlgorithm # struct
export FileMetaData # struct
export FileCryptoMetaData # struct

include("parquet_constants.jl")
include("parquet_types.jl")
include("parquet_impl.jl")  # server methods to be hand coded


end # module parquet