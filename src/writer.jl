using Tables
using Parquet: PAR2;
using Thrift, DataFrames
using Snappy, CodecZstd
using LittleEndianBase128
using LittleEndianBase128: encode

function write_parquet(path::String, tbl)
    # tbl needs to be iterable by column as parquet is a columnar format
    @assert Tables.columnaccess(tbl)

    fileio = open(path, "w")

    # write the magic number
    write(fileio, "PAR1")

    # start writing the column
    for colname in Tables.columnnames(tbl)
        colvals = Tables.getcolumn(df, :x1)

        # first compute dictionary

        # compute the number missing values


    end
end

function write_thrift(fileio, thrift_obj)
    p = TCompactProtocol(TFileTransport(fileio))
    Thrift.write(p, thrift_obj)
end

function compress_using_codec(colvals, codec = COMPRESSION_CODEC_CODE["snappy"])
    if codec == COMPRESSION_CODEC_CODE["snappy"]
        compressed_data = Snappy.compress(reinterpret(UInt8, colvals) |> collect)
    else
        error("not yet implemented")
    end

    return compressed_data
end

function write_col_dict(fileio, colvals, codec)
    """ write the column dictionary chunk """
    uvals = unique(colvals)

    compressed_uvals::Vector{UInt8} = compress_using_codec(uvals, COMPRESSION_CODEC_CODE["snappy"])

    uncompressed_dict_size = length(uvals)*sizeof(eltype(uvals))
    compressed_dict_size = length(compressed_uvals)

    # TODO do the CRC properly
    crc= 0

    # construct dictionary metadata
    dict_page_header = PAR2.PageHeader()

    set_field!(dict_page_header, :_type, PAGE_TYPE["DICTIONARY_PAGE"])
    set_field!(dict_page_header, :uncompressed_page_size , uncompressed_dict_size)
    set_field!(dict_page_header, :compressed_page_size , compressed_dict_size)
    set_field!(dict_page_header, :crc , 0)

    set_field!(dict_page_header, :dictionary_page_header, PAR2.DictionaryPageHeader())
    set_field!(dict_page_header.dictionary_page_header, :num_values , Int32(length(uvals)))
    set_field!(dict_page_header.dictionary_page_header, :encoding , Int32(2)) # value 2 is plain encoding for dictionary pages
    set_field!(dict_page_header.dictionary_page_header, :is_sorted , false)


    before_write_page_header_pos = position(fileio)

    write_thrift(fileio, dict_page_header)

    dict_page_header_size = position(fileio) - before_write_page_header_pos

    # write the dictionary data
    write(fileio, compressed_uvals)

    return (offset = before_write_page_header_pos, uncompressed_size = uncompressed_dict_size + dict_page_header_size, compressed_size = compressed_dict_size + dict_page_header_size)
end

# TODO set the encoding code into a dictionary
function write_col_chunk(fileio, colvals, encoding = 0, codec = COMPRESSION_CODEC_CODE["snappy"])
    if encoding == 0
        """Plain encoding"""
        # generate the data page header
        data_page_header = PAR2.PageHeader()

        # write repetition level data
        # do nothing

        # write definition_level
        # using rle

        rle_header = LittleEndianBase128.encode(UInt32(length(colvals)) << 1)

        # TODO if there are missing then need to update
        repeated_value = UInt8(1)

        encoded_defn_data_length = UInt32(sizeof(rle_header) + sizeof(repeated_value))

        data_to_compress_io = IOBuffer()
        write(data_to_compress_io, encoded_defn_data_length)
        write(data_to_compress_io, rle_header)
        write(data_to_compress_io, repeated_value)
        write(data_to_compress_io, colvals)

        data_to_compress::Vector{UInt8} = take!(data_to_compress_io)

        compressed_data::Vector{UInt8} = compress_using_codec(data_to_compress, codec)

        uncompressed_page_size = length(data_to_compress)
        compressed_page_size = length(compressed_data)

        set_field!(data_page_header, :_type, PAGE_TYPE["DATA_PAGE"])
        set_field!(data_page_header, :uncompressed_page_size, uncompressed_page_size)
        set_field!(data_page_header, :compressed_page_size, compressed_page_size)
        # TODO proper CRC
        set_field!(data_page_header, :crc , 0)

        set_field!(data_page_header, :data_page_header, PAR2.DataPageHeader())
        set_field!(data_page_header.data_page_header, :num_values , Int32(length(colvals)))
        set_field!(data_page_header.data_page_header, :encoding , encoding) # encoding 0 is plain encoding
        set_field!(data_page_header.data_page_header, :definition_level_encoding, 3)
        set_field!(data_page_header.data_page_header, :repetition_level_encoding, 3)

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

            end
        end
    else
        error("not implemented yet")
    end
end

function write_col(fileio, colvals, colname)
    dict_info = write_col_dict(fileio, colvals, COMPRESSION_CODEC_CODE["snappy"])

    # TODO determine a more appropriate chunk size
    num_chunks =  1

    # TODO choose an encoding
    # TODO put encoding into a dictionary
    encoding = 0
    codec = COMPRESSION_CODEC_CODE["snappy"]
    chunk_info = [write_col_chunk(fileio, colvals, encoding, codec) for i in 1:num_chunks]

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

function create_schema_parent_node(ncol)
    schmea_parent_node = PAR2.SchemaElement()
    # set_field!(schmea_parent_node, :type_length, 0)
    # set_field!(schmea_parent_node, :repetition_type, 0)
    set_field!(schmea_parent_node, :name, "schema")
    set_field!(schmea_parent_node, :num_children, ncol)
    # set_field!(schmea_parent_node, :converted_type, 0)
    # set_field!(schmea_parent_node, :scale, 0)
    # set_field!(schmea_parent_node, :precision, 0)
    # set_field!(schmea_parent_node, :field_id, 0)
    #set_field!(schmea_parent_node, :logicalType, 0) # it's undef
    schmea_parent_node
end

function create_col_schema(type, colname)
    schema_node = PAR2.SchemaElement()
    # look up type code
    set_field!(schema_node, :_type, COL_TYPE_CODE[type])

    # set_field!(schema_node, :type_length, 0)
    set_field!(schema_node, :repetition_type, 1)
    set_field!(schema_node, :name, colname)
    set_field!(schema_node, :num_children, 0)
    # set_field!(schema_node, :converted_type, 17)
    # set_field!(schema_node, :scale, 0)
    # set_field!(schema_node, :precision, 0)
    # set_field!(schema_node, :field_id, 0)
    #set_field!(schema_node, :logicalType, 0) # it's undef
    schema_node
end
