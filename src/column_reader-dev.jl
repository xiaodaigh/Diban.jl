using ParquetWriter:TYPES
using Parquet
using Parquet: SZ_PAR_MAGIC, SZ_FOOTER, PAR2, read_thrift
path = files[1]
col_num=2

using ParquetWriter
ParquetWriter.read_column(path, 2)


par = ParFile(path)
io = open(path)
sz = filesize(io)
seek(io, sz - 8)
len = read(io, Int32)
seek(io, sz - SZ_PAR_MAGIC - SZ_FOOTER - len)
filemetadata = read_thrift(io, PAR2.FileMetaData)
close(io)

T = TYPES[filemetadata.schema[col_num+1]._type+1]
# TODO detect if missing is necessary
res = Vector{Union{Missing, T}}(missing, nrows(par))
write_cursor = 1
#for row_group in filemetadata.row_groups
row_group =  filemetadata.row_groups[1]
pgs = Parquet.pages(par, row_group.columns[col_num])
# the first page is always the dictionary page
dictionary_page = pgs[1]

# TODO different logic for different stuff
dictionary_of_values = T.(values(par, dictionary_page)[1])

data_page = pgs[2]

vals, repetition, decode = Parquet.values(par, data_page)

@which Parquet.values(par, data_page)

cname = "Cl.thickness"
@which Parquet.max_definition_level(par.schema, cname)
@which Parquet.max_definition_level(par.schema, cname)

parentname("Cl.thickness")

@which Parquet.isrequired(par.schema, "Cl.thickness")

schname = "Cl.thickness"
schelem = Parquet.elem(par.schema, schname)

using Thrift
Thrift.isfilled(schelem, :repetition_type)

@which Parquet.parentname(schname)

join(parentname(), '.')

Parquet.parentname(split(schname, '.'))

(schelem.repetition_type == PAR2.FieldRepetitionType.REQUIRED)

# everything after the first data datapages
for data_page in Base.Iterators.drop(pages, 1)
	values, repetition, decode = Parquet.values(par, data_page)
	l = sum(repetition)

	# if all repetition values are 1 then it's not used
	repetition_not_used = all(==(1), repetition)

	# data_page can be either
	# * dictionary-encoded in which we should look into the dictionary
	# * plained-encoded in which case just return the values
	page_encoding = ParquetWriter.page_encoding(data_page)

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
			res[write_cursor:write_cursor+l-1] .= values
		else
			for (offset, (repetition, value))  in enumerate(zip(repetition, values))
				if repetition != 0
					res[write_cursor+offset-1] = value
				end
			end
		end
	else
		error("page encoding not supported yet")
	end

	write_cursor += l
end
#end
return res
