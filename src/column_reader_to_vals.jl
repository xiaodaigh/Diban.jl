using Diban

path = "tmp.parquet"
#v1 = read_column(path, 1)
col_num=1

par = ParFile(path)
filemetadata = Diban.metadata(path)

T = TYPES[filemetadata.schema[col_num+1]._type+1]
# TODO detect if missing is necessary
res = Vector{Union{Missing, T}}(missing, nrows(par))
write_cursor = 1
#for row_group in filemetadata.row_groups
row_group =  filemetadata.row_groups[1]
pgs = pages(par, row_group.columns[col_num])
# the first page is always the dictionary page
dictionary_page = pgs[1]

# TODO different logic for different stuff
dictionary_of_values = T.(values(par, dictionary_page)[1])

data_page = pgs[2]

@which values(par, data_page)


#@time vals, repetition, decode = values(par, data_page)



################################################################################
# begin: expansion of functions `values`
################################################################################

page = data_page
ctype = coltype(page.colchunk)
rawbytes = bytes(page)
io = IOBuffer(rawbytes)
encs = page_encodings(page)
num_values = page_num_values(page)
typ = page.hdr._type


(typ == PageType.DATA_PAGE) || (typ == PageType.DATA_PAGE_V2)

@which read_levels_and_values(io, encs, ctype, num_values, par, page)
# below line is my code
vals, repetition, dd = read_levels_and_values(io, encs, ctype, num_values, par, page)


	################################################################################
	# begin: expansion of functions `read_levels_and_values`
	################################################################################
cname = colname(page.colchunk)
enc, defn_enc, rep_enc = encs

position(io)
#defn_levels = read(io, 8)

#@debug("before reading repn levels bytesavailable in page: $(bytesavailable(io))")
# read repetition levels. skipped if all columns are at 1st level
# TODO ZJ why is it checking for a "." in the name
repn_levels = ('.' in cname) ? read_levels(io, max_repetition_level(par.schema, cname), rep_enc, num_values) : Int[]


#@debug("before reading defn levels bytesavailable in page: $(bytesavailable(io))")
# read definition levels. skipped if column is required
defn_levels = isrequired(par.schema, cname) ? Int[] : read_levels(io, max_definition_level(par.schema, cname), defn_enc, num_values)


num_values
bw = 1

@which read_hybrid(io, num_values, bw)

read_hybrid(io, num_values, bw)

runhdr = 2
@which read_bitpacked_run(io, runhdr, bits, byt, typ, subarr)

# TODO I don't know how to deal with anything else; so below
@assert all(<=(1), defn_levels)

#@debug("before reading values bytesavailable in page: $(bytesavailable(io))")
# read values
# if there are missing values in the data then
# where defn_levels's elements == 1 are present and only
# sum(defn_levels) values can be read.
# because defn_levels == 0 are where the missing vlaues are
nmissing = sum(==(0), defn_levels)
#vals = read_values(io, enc, ctype, num_values - nmissing)

		################################################################################
		# begin: expansion of functions `read_values`
		################################################################################
@which read_values(io, enc, ctype, num_values - nmissing)

enc == Encoding.PLAIN
enc == Encoding.PLAIN_DICTIONARY || enc == Encoding.RLE_DICTIONARY

#vals = read_rle_dict(io, num_values)
@which read_rle_dict(io, num_values)

# begin: read_rle_dict

bits = read(io, UInt8)
Int(bits)
@debug("reading rle dictionary bits:$bits")
#arr = read_hybrid(io, num_values, bits; read_len=false)
@debug("read $(length(arr)) dictionary values")
#arr
@which read_hybrid(io, num_values, bits; read_len=false)
# end: read_rle_dict

# begin: read_hybrid
read_len = false
position(io)

#abc =  read(io, 2)
#Int.(abc)

using LittleEndianBase128

#LittleEndianBase128.decode(abc)[1] |> Int
#LittleEndianBase128.decodesigned(abc)[1] |> Int

isbitpack

len = read_len ? read_fixed(io, Int32) : Int32(0)
@debug("reading hybrid data", len, num_values, bits)
arrpos = 1
#while arrpos <= num_values

byt = bit2bytewidth(bits)
typ = byt2itype(byt)
@which _read_varint(io, Int)
Int(MSB)

read(io, UInt8)
read(io, UInt8)

using LittleEndianBase128
LittleEndianBase128.decode([0x02, 0x00])

Int(0x7f)
0x02

res = Int(0x02)

0x00 & MSB

runhdr = _read_varint(io, Int)
isbitpack = ((runhdr & 0x1) == 0x1)
runhdr >>= 1
nitems = isbitpack ? min(runhdr*8, num_values - arrpos + 1) : runhdr
#@debug("nitems=$nitems, isbitpack:$isbitpack, runhdr:$runhdr, remaininglen: $(num_values - arrpos + 1)")
#@debug("creating sub array for $nitems items at $arrpos, total length:$(length(arr))")
#subarr = pointer_to_array(pointer(arr, arrpos), nitems)
subarr = unsafe_wrap(Array, pointer(arr, arrpos), nitems, own=false)
isbitpack
position(io)
if isbitpack
	read_bitpacked_run(io, runhdr, bits, byt, typ, subarr)
else # rle
	read_rle_run(io, runhdr, bits, byt, typ, subarr)
end

runhdr
subarr

position(io)
read_rle_run(io, runhdr, bits, byt, typ, subarr)

arrpos += nitems
#end
arr

# end




		################################################################################
		# begin: expansion of functions `_read_varint`
		################################################################################
res = zero(T)
n = 0
byte = UInt8(MSB)
while (byte & MSB) != 0
	global res, n
    byte = read(io, UInt8)
    res |= (convert(T, byte & MASK7) << (7*n))
    n += 1
end
# in case of overflow, consider it as missing field and return default value
if (n-1) > sizeof(T)
    #@debug("overflow reading $T. returning 0")
    return zero(T)
end
res
		################################################################################
		# end: expansion of functions `_read_varint`
		################################################################################

while arrpos <= count
	runhdr = _read_varint(io, Int)
	isbitpack = ((runhdr & 0x1) == 0x1)
	runhdr >>= 1
	nitems = isbitpack ? min(runhdr*8, count - arrpos + 1) : runhdr
	#@debug("nitems=$nitems, isbitpack:$isbitpack, runhdr:$runhdr, remaininglen: $(count - arrpos + 1)")
	#@debug("creating sub array for $nitems items at $arrpos, total length:$(length(arr))")
	#subarr = pointer_to_array(pointer(arr, arrpos), nitems)
	subarr = unsafe_wrap(Array, pointer(arr, arrpos), nitems, own=false)

	if isbitpack
		read_bitpacked_run(io, runhdr, bits, byt, typ, subarr)
	else # rle
		read_rle_run(io, runhdr, bits, byt, typ, subarr)
	end
	arrpos += nitems
end
arr

		################################################################################
		# end: expansion of functions `read_values`
		################################################################################

	################################################################################
	# end: expansion of functions `read_levels_and_values`
	################################################################################if (typ == PageType.DATA_PAGE) || (typ == PageType.DATA_PAGE_V2)

# elseif typ == PageType.DICTIONARY_PAGE
# 	(read_plain_values(io, num_values, ctype),)
# else
# 	()
# end
################################################################################
# begin: expansion of functions `values`
################################################################################


cname = "Cl.thickness"
@which max_definition_level(par.schema, cname)
@which max_definition_level(par.schema, cname)

parentname("Cl.thickness")

@which isrequired(par.schema, "Cl.thickness")

schname = "Cl.thickness"
schelem = elem(par.schema, schname)

using Thrift
Thrift.isfilled(schelem, :repetition_type)

@which parentname(schname)

join(parentname(), '.')

parentname(split(schname, '.'))

(schelem.repetition_type == PAR2.FieldRepetitionType.REQUIRED)

# everything after the first data datapages
for data_page in Base.Iterators.drop(pages, 1)
	values, repetition, decode = values(par, data_page)
	l = sum(repetition)

	# if all repetition values are 1 then it's not used
	repetition_not_used = all(==(1), repetition)

	# data_page can be either
	# * dictionary-encoded in which we should look into the dictionary
	# * plained-encoded in which case just return the values
	page_encoding = Diban.page_encoding(data_page)

	if page_encoding == Encoding.PLAIN_DICTIONARY
		if repetition_not_used
			res[write_cursor:write_cursor+l-1] .= dictionary_of_values[values.+1]
		else
			for (offset, (repetition, value))  in enumerate(zip(repetition, values))
				if repetition != 0
					res[write_cursor+offset-1] = dictionary_of_values[value.+1]
				end
			end
		end
	elseif page_encoding == Encoding.PLAIN
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
