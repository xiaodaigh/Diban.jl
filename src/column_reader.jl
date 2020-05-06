using Parquet: ParFile, PAR2, read_thrift, Encoding, nrows, pages
using Parquet

const TYPES = (Bool, Int32, Int64, Int128, Float32, Float64, String, UInt8)

read_column(path, col_num) = read_column(path, metadata(path), col_num)

function read_column(path, filemetadata, col_num)
    par = ParFile(path)

    T = TYPES[filemetadata.schema[col_num+1]._type+1]
    # TODO detect if missing is necessary
    res = Vector{Union{Missing, T}}(missing, nrows(par))
    write_cursor = 1
    for row_group in filemetadata.row_groups
        pages = Parquet.pages(par, row_group.columns[col_num])

        drop_page_count = 0
        # is the first page a dictionary page
        # this is not the case for boolean values for example
        if isfilled(pages[1].hdr, :dictionary_page_header)
            # the first page is almost always the dictionary page
            dictionary_page = pages[1]
            drop_page_count = 1
            dictionary_of_values = T.(values(par, dictionary_page)[1])
        end

        # TODO deal with other types of pages e.g. dataheaderv2

        # everything after the first data datapages
        for data_page in Base.Iterators.drop(pages, drop_page_count)
            values, repetition, decode = Parquet.values(par, data_page)
            l = sum(repetition)

            # if all repetition values are 1 then it's not used
            repetition_not_used = all(==(1), repetition)

            # data_page can be either
            # * dictionary-encoded in which we should look into the dictionary
            # * plained-encoded in which case just return the values
            page_encoding = Diban.page_encoding(data_page)

            if page_encoding == Parquet.Encoding.PLAIN_DICTIONARY
                if repetition_not_used
                    res[write_cursor:write_cursor+l-1] .= dictionary_of_values[values.+1]
                else
                    for (offset, (repetition, value))  in enumerate(zip(repetition, values))
                        if repetition != 0
                            res[write_cursor+offset-1] = dictionary_of_values[value.+1]
                        end
                    end
                end
            elseif page_encoding == Parquet.Encoding.PLAIN
                if repetition_not_used
                    res[write_cursor:write_cursor+l-1] .= T.(values)
                else
                    for (offset, (repetition, value))  in enumerate(zip(repetition, values))
                        if repetition != 0
                            res[write_cursor+offset-1] = T(value)
                        end
                    end
                end
            else
                error("page encoding not supported yet")
            end

            write_cursor += l
        end
    end
    return res
end
