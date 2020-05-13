using Tables
using DataAPI
using Thrift
using Snappy
using CodecZstd: ZstdCompressor
using CodecZlib: GzipCompressor
#using CodecLz4: LZ4HCCompressor
using LittleEndianBase128
using LittleEndianBase128: encode
using Base.Iterators: partition
using CategoricalArrays: CategoricalArray, CategoricalValue

# a mapping of Julia types to _Type codes in Parquet format
const COL_TYPE_CODE = Dict{DataType, Int32}(
    Bool => PAR2._Type.BOOLEAN,
    Int32 => PAR2._Type.INT32,
    Int64 => PAR2._Type.INT64,
    #INT96 => 3,  // deprecated, only used by legacy implementations. # not supported
    Float32 => PAR2._Type.FLOAT,
    Float64 => PAR2._Type.DOUBLE,
    String => PAR2._Type.BYTE_ARRAY, # BYTE_ARRAY
    # FIXED_LEN_BYTE_ARRAY => 7,
    )

function write_thrift(fileio, thrift_obj)
    p = TCompactProtocol(TFileTransport(fileio))
    Thrift.write(p, thrift_obj)
end

function compress_using_codec(colvals::AbstractArray, codec::Integer)::Vector{UInt8}
    uncompressed_byte_data = reinterpret(UInt8, colvals) |> collect

    if codec == PAR2.CompressionCodec.UNCOMPRESSED
        return uncompressed_byte_data
    elseif codec == PAR2.CompressionCodec.SNAPPY
        compressed_data = Snappy.compress(uncompressed_byte_data)
    elseif codec == PAR2.CompressionCodec.GZIP
        compressed_data = transcode(GzipCompressor, uncompressed_byte_data)
    elseif codec == PAR2.CompressionCodec.LZ4
        error("lz4 is not supported as data compressed with https://github.com/JuliaIO/CodecLz4.jl can't seem to be read by R or Python. If you know how to fix it please help out.")
        #compressed_data = transcode(LZ4HCCompressor, uncompressed_byte_data)
    elseif codec == PAR2.CompressionCodec.ZSTD
        compressed_data = transcode(ZstdCompressor, uncompressed_byte_data)
    else
        error("not yet implemented")
    end

    return compressed_data
end

function compress_using_codec(colvals::AbstractVector{String}, codec::Int)::Vector{UInt8}
    # the output
    io = IOBuffer()

    # write the values
    for val in colvals
        # for string it needs to be stored as BYTE_ARRAY which needs the length
        # to be the first 4 bytes UInt32
        write(io, val |> sizeof |> UInt32)
        # write each of the strings one after another
        write(io, val)
    end

    uncompressed_bytes = take!(io)
    return compress_using_codec(uncompressed_bytes, codec)
end

function write_col_dict(fileio, colvals::AbstractArray{T}, codec) where T
    """ write the column dictionary chunk """
    uvals = DataAPI.levels(colvals) # this does not include missing

    if length(uvals) > 127
        # do not support dictionary with more than 127 levels
        # TODO relax this 127 restriction
        @warn "More than 127 levels in dictionary. This is not supported at this stage."
        return (offset = -1, uncompressed_size = 0, compressed_size = 0)
    end

    if nonmissingtype(T) == String
        # the raw bytes of made of on UInt32 to indicate string length
        # and the content of the string
        # so the formula for dict size is as below
        uncompressed_dict_size = sizeof(UInt32)*length(uvals) + sum(sizeof, uvals)
    else
        uncompressed_dict_size = length(uvals)*sizeof(eltype(uvals))
    end

    compressed_uvals::Vector{UInt8} = compress_using_codec(uvals, codec)
    compressed_dict_size = length(compressed_uvals)

    # TODO do the CRC properly
    crc = 0

    # construct dictionary metadata
    dict_page_header = PAR2.PageHeader()

    Thrift.set_field!(dict_page_header, :_type, PAGE_TYPE["DICTIONARY_PAGE"])
    Thrift.set_field!(dict_page_header, :uncompressed_page_size , uncompressed_dict_size)
    Thrift.set_field!(dict_page_header, :compressed_page_size , compressed_dict_size)
    Thrift.set_field!(dict_page_header, :crc , crc)

    Thrift.set_field!(dict_page_header, :dictionary_page_header, PAR2.DictionaryPageHeader())
    Thrift.set_field!(dict_page_header.dictionary_page_header, :num_values , Int32(length(uvals)))
    Thrift.set_field!(dict_page_header.dictionary_page_header, :encoding , Int32(2)) # value 2 is plain encoding for dictionary pages
    Thrift.set_field!(dict_page_header.dictionary_page_header, :is_sorted , false)

    before_write_page_header_pos = position(fileio)

    write_thrift(fileio, dict_page_header)

    dict_page_header_size = position(fileio) - before_write_page_header_pos

    # write the dictionary data
    write(fileio, compressed_uvals)

    return (offset = before_write_page_header_pos, uncompressed_size = uncompressed_dict_size + dict_page_header_size, compressed_size = compressed_dict_size + dict_page_header_size)
end

# TODO set the encoding code into a dictionary
function write_col_chunk(fileio, colvals::AbstractArray{T}, codec, encoding) where T
    if encoding == PAR2.Encoding.PLAIN
        # generate the data page header
        data_page_header = PAR2.PageHeader()

        # write repetition level data
        # do nothing
        # this seems to be related to nested columns
        # and hence is not needed here

        # set up a buffer to write to
        data_to_compress_io = IOBuffer()

        if Missing <: T
            # if there is missing
            # use the bit packing algorithm to write the
            # definition_levels

            bytes_needed = ceil(Int, length(colvals) / 8sizeof(UInt8))
            tmp = UInt32((UInt32(bytes_needed) << 1) | 1)
            bitpacking_header = LittleEndianBase128.encode(tmp)

            tmpio = IOBuffer()
            not_missing_bits::BitArray = .!ismissing.(colvals)
            write(tmpio, not_missing_bits)
            seek(tmpio, 0)

            encoded_defn_data = read(tmpio, bytes_needed)

            encoded_defn_data_length = length(bitpacking_header) + bytes_needed
            # write the definition data
            write(data_to_compress_io, UInt32(encoded_defn_data_length))
            write(data_to_compress_io, bitpacking_header)
            write(data_to_compress_io, encoded_defn_data)
        else
            # if there is no missing can just use RLE of one
            # using rle
            rle_header = LittleEndianBase128.encode(UInt32(length(colvals)) << 1)
            repeated_value = UInt8(1)
            encoded_defn_data_length = UInt32(sizeof(rle_header) + sizeof(repeated_value))

            # write the definition data
            write(data_to_compress_io, UInt32(encoded_defn_data_length))
            write(data_to_compress_io, rle_header)
            write(data_to_compress_io, repeated_value)
        end

        if nonmissingtype(T) == String
            # write the values
            for val in skipmissing(colvals)
                # for string it needs to be stored as BYTE_ARRAY which needs the length
                # to be the first 4 bytes UInt32
                write(data_to_compress_io, val |> sizeof |> UInt32)
                # write each of the strings one after another
                write(data_to_compress_io, val)
            end
        elseif nonmissingtype(T) == Bool
            # write the bitacpked bits
            # write a bitarray seems to write 8 bytes at a time
            # so write to a tmpio first
            no_missing_bit_vec =  BitArray(skipmissing(colvals))
            bytes_needed = ceil(Int, length(no_missing_bit_vec) / 8sizeof(UInt8))
            tmpio = IOBuffer()
            write(tmpio, no_missing_bit_vec)
            seek(tmpio, 0)
            packed_bits = read(tmpio, bytes_needed)
            write(data_to_compress_io, packed_bits)
        else
            for val in skipmissing(colvals)
                write(data_to_compress_io, val)
            end
        end

        data_to_compress::Vector{UInt8} = take!(data_to_compress_io)

        compressed_data::Vector{UInt8} = compress_using_codec(data_to_compress, codec)

        uncompressed_page_size = length(data_to_compress)
        compressed_page_size = length(compressed_data)

        Thrift.set_field!(data_page_header, :_type, PAR2.PageType.DATA_PAGE)
        Thrift.set_field!(data_page_header, :uncompressed_page_size, uncompressed_page_size)
        Thrift.set_field!(data_page_header, :compressed_page_size, compressed_page_size)

        # TODO proper CRC
        Thrift.set_field!(data_page_header, :crc , 0)

        Thrift.set_field!(data_page_header, :data_page_header, PAR2.DataPageHeader())
        Thrift.set_field!(data_page_header.data_page_header, :num_values , Int32(length(colvals)))
        Thrift.set_field!(data_page_header.data_page_header, :encoding , encoding) # encoding 0 is plain encoding
        Thrift.set_field!(data_page_header.data_page_header, :definition_level_encoding, 3)
        Thrift.set_field!(data_page_header.data_page_header, :repetition_level_encoding, 3)

        position_before_page_header_write = position(fileio)
        write_thrift(fileio, data_page_header)
        size_of_page_header_defn_repn = position(fileio) - position_before_page_header_write

        # write data
        write(fileio, compressed_data)

        return (
            offset = position_before_page_header_write,
            uncompressed_size = uncompressed_page_size + size_of_page_header_defn_repn,
            compressed_size = compressed_page_size + size_of_page_header_defn_repn,
        )
    elseif encoding == 2
        error("not implemented yet")
        """Dictionary encoding"""
        rle_header = LittleEndianBase128.encode(UInt32(length(colvals)) << 1)
        repeated_value = UInt8(1)

        encoded_defn_data_length = UInt32(sizeof(rle_header) + sizeof(repeated_value))

        ## write the encoded data length
        write(fileio, encoded_defn_data_length)

        write(fileio, rle_header)
        write(fileio, repeated_value)

        position(fileio)

        # write the data

        ## firstly, bit pack it
        colvals

        # the bitwidth to use
        bitwidth = ceil(UInt8, log(2, length(uvals)))
        # the max bitwidth is 32 according to documentation
        @assert bitwidth <= 32
        # to do that I have to figure out the Dictionary index of it
        # build a JuliaDict
        val_index_dict = Dict(zip(uvals, 1:length(uvals)))

        bitwidth_mask = UInt32(2^bitwidth-1)

        bytes_needed = ceil(Int, bitwidth*length(colvals) / 8)

        bit_packed_encoded_data = zeros(UInt8, bytes_needed)
        upto_byte = 1

        bits_written = 0
        bitsz = 8sizeof(UInt8)

        for val in colvals
            bit_packed_val = UInt32(val_index_dict[val]) & bitwidth_mask
            if bitwidth_mask <= bitsz - bits_written
                bit_packed_encoded_data[upto_byte] = (bit_packed_encoded_data[upto_byte] << bitwidth_mask) | bit_packed_val
            else
                # this must mean
                # bitwidth_mask > bitsz - bits_written
                # if the remaining bits is not enough to write a packed number
                42
            end
        end
    else
        error("not implemented yet")
    end
end

write_col(fileio, colvals::CategoricalArray, args...; kwars...) = begin
    throw("Currently CategoricalArrays are not supported.")
end

function write_col(fileio, colvals::AbstractArray{T}, colname, encoding, codec; num_chunks = 1) where T

    # TODO turn writing dictionary back on
    if false
        if nonmissingtype(T) == Bool
            # dictionary type are not supported for
            dict_info = (offset = -1, uncompressed_size = 0, compressed_size = 0)
        else
            dict_info = write_col_dict(fileio, colvals, codec)
        end
    else
        dict_info = (offset = -1, uncompressed_size = 0, compressed_size = 0)
    end

    num_vals_per_chunk = ceil(Int, length(colvals) / num_chunks)

    # TODO choose an encoding
    # TODO put encoding into a dictionary
    chunk_info = [write_col_chunk(fileio, val_chunk, codec, encoding) for val_chunk in partition(colvals, num_vals_per_chunk)]

    sizes = reduce(chunk_info; init = dict_info) do x, y
        (
            uncompressed_size = x.uncompressed_size + y.uncompressed_size,
            compressed_size = x.compressed_size + y.compressed_size
        )
    end

    return (
        dictionary_page_offset = dict_info.offset,
        data_page_offset = chunk_info[1].offset,
        uncompressed_size = sizes.uncompressed_size,
        compressed_size = sizes.compressed_size,
    )

end

function create_schema_parent_node(ncols)
    schmea_parent_node = PAR2.SchemaElement()
    Thrift.set_field!(schmea_parent_node, :name, "schema")
    Thrift.set_field!(schmea_parent_node, :num_children, ncols)
    schmea_parent_node
end

function create_col_schema(type, colname)
    schema_node = PAR2.SchemaElement()
    # look up type code
    Thrift.set_field!(schema_node, :_type, COL_TYPE_CODE[type |> nonmissingtype])
    Thrift.set_field!(schema_node, :repetition_type, 1)
    Thrift.set_field!(schema_node, :name, colname)
    Thrift.set_field!(schema_node, :num_children, 0)

    schema_node
end


function create_col_schema(type::Type{String}, colname)
    """create col schema for string"""
    schema_node = PAR2.SchemaElement()
    # look up type code
    Thrift.set_field!(schema_node, :_type, COL_TYPE_CODE[type])
    Thrift.set_field!(schema_node, :repetition_type, 1)
    Thrift.set_field!(schema_node, :name, colname)
    Thrift.set_field!(schema_node, :num_children, 0)

    # for string set converted type to UTF8
    Thrift.set_field!(schema_node, :converted_type, PAR2.ConvertedType.UTF8)

    logicalType = PAR2.LogicalType()
    Thrift.set_field!(logicalType, :STRING, PAR2.StringType())

    Thrift.set_field!(schema_node, :logicalType, logicalType)

    schema_node
end


function write_parquet(path, tbl; compression_codec = "SNAPPY", encoding = Encoding.PLAIN)
    # tbl needs to be iterable by column as parquet is a columnar format
    @assert Tables.columnaccess(tbl)

    # check that all types are supported
    sch = Tables.schema(tbl)
    err_msgs = String[]
    for type in sch.types
        if type <: CategoricalValue
            push!(err_msgs, "CategoricalArrays are not supported at this stage. \n")
        elseif !(nonmissingtype(type) <: Union{Int32, Int64, Float32, Float64, Bool, String})
            push!(err_msgs, "Column whose `eltype` is $type is not supported at this stage. \n")
        end
    end

    err_msgs = unique(err_msgs)
    if length(err_msgs) > 0
        throw(reduce(*, err_msgs))
    end

    # convert a string or symbol compression codec into the numeric code
    codec = getproperty(PAR2.CompressionCodec, Symbol(uppercase(string(compression_codec))))

    fileio = open(path, "w")
    write(fileio, "PAR1")

    colnames::Vector{Symbol} = Tables.columnnames(tbl)
    ncols = length(colnames)
    nrows = length(Tables.rows(tbl))

    # the + 1 comes from the fact that schema is tree and there is an extra
    # parent node
    schemas = Vector{PAR2.SchemaElement}(undef, ncols + 1)
    schemas[1] = create_schema_parent_node(ncols)
    col_chunk_metas = Vector{PAR2.ColumnChunk}(undef, ncols)
    row_group_file_offset = -1

    # figure out the right number of chunks
    # TODO test that it works for all supported table
    table_size_bytes = Base.summarysize(tbl)

    approx_raw_to_parquet_compression_ratio = 6
    approx_post_compression_size = (table_size_bytes / 2^30) / approx_raw_to_parquet_compression_ratio

    # if size is larger than 64mb and has more than 6 rows
    if (approx_post_compression_size > 0.064) & (nrows > 6)
        recommended_chunks = ceil(Int, approx_post_compression_size / 6) * 6
    else
        recommended_chunks = 1
    end

    @showprogress for (coli, colname_sym) in enumerate(colnames)
        colvals = Tables.getcolumn(tbl, colname_sym)
        colname = String(colname_sym)

        # write the data
        col_info = write_col(fileio, colvals, colname, encoding, codec; num_chunks = recommended_chunks)

        # the `row_group_file_offset` keeps track where the data
        # starts, so keep it at the dictonary of the first data
        if coli == 1
            if col_info.dictionary_page_offset == -1
                row_group_file_offset = col_info.data_page_offset
            else
                row_group_file_offset = col_info.dictionary_page_offset
            end
        end

        # write the column metadata
        # can probably write the metadata right after the data chunks
        col_meta = PAR2.ColumnMetaData()

        Thrift.set_field!(col_meta, :_type, COL_TYPE_CODE[eltype(colvals) |> nonmissingtype])
        # these are all the fields
        # TODO collect all the encodings used
        if eltype(colvals) == Bool
            Thrift.set_field!(col_meta, :encodings, Int32[0, 3])
        else
            Thrift.set_field!(col_meta, :encodings, Int32[2, 0, 3])
        end
        Thrift.set_field!(col_meta, :path_in_schema, [colname])
        Thrift.set_field!(col_meta, :codec, codec)
        Thrift.set_field!(col_meta, :num_values, length(colvals))

        Thrift.set_field!(col_meta, :total_uncompressed_size, col_info.uncompressed_size)
        Thrift.set_field!(col_meta, :total_compressed_size, col_info.compressed_size)

        Thrift.set_field!(col_meta, :data_page_offset, col_info.data_page_offset)
        if col_info.dictionary_page_offset != -1
            Thrift.set_field!(col_meta, :dictionary_page_offset, col_info.dictionary_page_offset)
        end

        # write the column meta data right after the data
        # keep track of the position so it can put into the column chunk
        # metadata
        col_meta_offset = position(fileio)
        write_thrift(fileio, col_meta)

        # Prep metadata for the filemetadata
        ## column chunk metadata
        col_chunk_meta = PAR2.ColumnChunk()

        Thrift.set_field!(col_chunk_meta, :file_offset, col_meta_offset)
        Thrift.set_field!(col_chunk_meta, :meta_data, col_meta)
        Thrift.clear(col_chunk_meta, :offset_index_offset)
        Thrift.clear(col_chunk_meta, :offset_index_length)
        Thrift.clear(col_chunk_meta, :column_index_offset)
        Thrift.clear(col_chunk_meta, :column_index_length)

        col_chunk_metas[coli] = col_chunk_meta

        # add the schema
        schemas[coli + 1] = create_col_schema(eltype(colvals) |> nonmissingtype, colname)
    end

    # now all the data is written we write the filemetadata
    # finalise it by writing the filemetadata
    filemetadata = PAR2.FileMetaData()
    Thrift.set_field!(filemetadata, :version, 1)
    Thrift.set_field!(filemetadata, :schema, schemas)
    Thrift.set_field!(filemetadata, :num_rows, nrows)
    Thrift.set_field!(filemetadata, :created_by, "Diban.jl")

    # create row_groups
    # TODO do multiple row_groups
    row_group = PAR2.RowGroup()

    Thrift.set_field!(row_group, :columns, col_chunk_metas)
    Thrift.set_field!(row_group, :total_byte_size, Int64(sum(x->x.meta_data.total_compressed_size, col_chunk_metas)))
    Thrift.set_field!(row_group, :num_rows, nrows)
    if row_group_file_offset == -1
        error("row_group_file_offset is not set")
    else
        Thrift.set_field!(row_group, :file_offset, row_group_file_offset)
    end
    Thrift.set_field!(row_group, :total_compressed_size, Int64(sum(x->x.meta_data.total_compressed_size, col_chunk_metas)))

    Thrift.set_field!(filemetadata, :row_groups, [row_group])

    position_before_filemetadata_write = position(fileio)

    write_thrift(fileio, filemetadata)

    filemetadata_size = position(fileio) - position_before_filemetadata_write

    write(fileio, Int32(filemetadata_size))
    write(fileio, "PAR1")
    close(fileio)
end
