using Parquet: SZ_PAR_MAGIC, SZ_FOOTER, PAR2, read_thrift
using ParquetWriter: TYPES
path = "c:/scratch/python-test.parquet"
path = "c:/scratch/test.parquet"
col_num = 1
@time col = read_column(path, 3);

first(col, 6)

par = ParFile(path)
io = open(path)
sz = filesize(io)
seek(io, sz - 8)
len = read(io, Int32)
seek(io, sz - SZ_PAR_MAGIC - SZ_FOOTER - len)
filemetadata = read_thrift(io, PAR2.FileMetaData)
close(io)

T = TYPES[filemetadata.schema[col_num+1]._type+1]
res = Vector{T}(undef, nrows(par))
write_cursor = 1
#for row_group in filemetadata.row_groups
row_group = filemetadata.row_groups[1]
pgs = Parquet.pages(par, row_group.columns[col_num])
# the first page is always the dictionary page
dictionary_page = pgs[1]

# TODO different logic for different stuff
dictionary_of_values = T.(values(par, dictionary_page)[1])

# everything after the first data datapages
data_page = pgs[2]

using Snappy
uncompressed_data = Snappy.uncompress(data_page.data)

#############################################
@which Parquet.values(par, data_page)
page = data_page

ParquetWriter.page_encoding(data_page)
Parquet.Encoding.PLAIN_DICTIONARY


using Parquet: coltype, bytes, page_encodings, page_num_values, PageType

# from values
ctype = coltype(page.colchunk)
rawbytes = bytes(page)
io = IOBuffer(rawbytes)
encs = page_encodings(page)
num_values = page_num_values(page)
typ = page.hdr._type
typ == PageType.DATA_PAGE

@which Parquet.read_levels_and_values(io, encs, ctype, num_values, par, page)

# read_levels_and_values
cname = colname(page.colchunk)
enc, defn_enc, rep_enc = encs

#@debug("before reading defn levels bytesavailable in page: $(bytesavailable(io))")
# read definition levels. skipped if column is required
import Parquet: max_definition_level, isrequired, read_levels
defn_levels = isrequired(par.schema, cname) ? Int[] : read_levels(io, max_definition_level(par.schema, cname), defn_enc, num_values)

using StatsBase: countmap
countmap(defn_levels)

#@debug("before reading repn levels bytesavailable in page: $(bytesavailable(io))")
# read repetition levels. skipped if all columns are at 1st level
repn_levels = ('.' in cname) ? read_levels(io, max_repetition_level(par.schema, cname), rep_enc, num_values) : Int[]

#@debug("before reading values bytesavailable in page: $(bytesavailable(io))")
# read values
import Parquet: read_values, read_plain_values

nmissing = sum(==(0), defn_levels)
vals_non_missing = read_values(io, enc, ctype, num_values - nmissing)
vals = Vector{Union{eltype(vals_non_missing), Missing}}(missing, num_values)
# assume
vals[defn_levels .!= 0] .= vals_non_missing

vals


if nmissing > 0
else
	# no missing values
	vals = read_values(io, enc, ctype, num_values)
end


# vals = read_values(io, enc, ctype, sum(defn_levels))
#
# @which read_values(io, enc, ctype, sum(defn_levels))
# typ = ctype
# @which read_plain_values(io, num_values, typ)
#
# vals, defn_levels, repn_levels


##############################################

#for data_page in Base.Iterators.drop(pages, 1)
values, repetition, decode = Parquet.values(par, data_page)
l = sum(repetition)

# if all repetition values are 1 then it's not used
repetition_not_used = all(==(1), repetition)

# data_page can be either
# * dictionary-encoded in which we should look into the dictionary
# * plained-encoded in which case just return the values
page_encoding = ParquetWriter.page_encoding(data_page)

if page_encoding == Encoding.PLAIN_DICTIONARY
	if repetition_not_used
		res[write_cursor:write_cursor+l-1] .= dictionary_of_values[values.+1]
	else
		res[write_cursor:write_cursor+l-1] .= inverse_rle(dictionary_of_values[values.+1], repetition)
	end
elseif page_encoding == Encoding.PLAIN
	if repetition_not_used
		res[write_cursor:write_cursor+l-1] .= values
	else
		res[write_cursor:write_cursor+l-1] .= inverse_rle(values, repetition)
	end
else
	error("page encoding not supported yet")
end

write_cursor += l
#end
#end
return res









using Parquet
ncols(ParFile(path))

colnames(ParFile(path))


@time df = read_parquet("c:/scratch/test.parquet");


head(df)


aa = read_column("c:/scratch/test.parquet", 2)

col

par = ParFile(path)
io = open(path)
sz = filesize(io)
seek(io, sz - 8)
len = reinterpret(Int32, read(io, 4))[1]
seek(io, sz - SZ_PAR_MAGIC - SZ_FOOTER - len)
filemetadata = read_thrift(io, PAR2.FileMetaData)
close(io)

T = TYPES[filemetadata.schema[col_num+1]._type+1]
res = Vector{T}(undef, nrows(par))
write_cursor = 1
row_group = filemetadata.row_groups[1]
pgs = Parquet.pages(par, row_group.columns[col_num])

row_group.columns

page =  pgs[1]
rawbytes = Parquet.bytes(page)
num_values = Parquet.page_num_values(page)
@which Parquet.page_num_values(page)

page

@which Parquet.page_num_values(page.hdr.dictionary_page_header)

page.hdr.dictionary_page_header.num_values
io = IOBuffer(rawbytes)
ctype = Parquet.coltype(page.colchunk)
Parquet.read_plain_values(io, num_values, ctype)


page

filemetadata

using Snappy

io = IOBuffer(Snappy.uncompress(pgs[1].data))
