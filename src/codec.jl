# ref: https://github.com/apache/parquet-format/blob/master/Encodings.md

const MSB = 0x80
const MASK7 = 0x7f
const MASK8 = 0xff
const MASK3 = 0x07
function MASKN(nbits)
    T = byt2uitype_small(bit2bytewidth(nbits))
    O = convert(T, 0x1)
    (O << nbits) - O
end

bitwidth(i) = ceil(Int, log(2, i+1))
bytewidth(i) = bit2bytewidth(bitwidth(i))
bit2bytewidth(i) = ceil(Int, i/8)
byt2itype(i) = (i <= 4) ? Int32 : (i <= 8) ? Int64 : Int128
byt2uitype(i) = (i <= 4) ? UInt32 : (i <= 8) ? UInt64 : UInt128
byt2uitype_small(i) = (i <= 1) ? UInt8 : (i <= 2) ? UInt16 : (i <= 4) ? UInt32 : (i <= 8) ? UInt64 : UInt128

read_fixed(io::IO, typ::Type{UInt32}) = _read_fixed(io, convert(UInt32,0), 4)
read_fixed(io::IO, typ::Type{UInt64}) = _read_fixed(io, convert(UInt64,0), 8)
read_fixed(io::IO, typ::Type{Int32}) = reinterpret(Int32, _read_fixed(io, convert(UInt32,0), 4))
read_fixed(io::IO, typ::Type{Int64}) = reinterpret(Int64, _read_fixed(io, convert(UInt64,0), 8))
read_fixed(io::IO, typ::Type{Int128}) = reinterpret(Int128, _read_fixed(io, convert(UInt128, 0), 12))   # INT96: 12 bytes little endian
read_fixed(io::IO, typ::Type{Float32}) = reinterpret(Float32, _read_fixed(io, convert(UInt32,0), 4))
read_fixed(io::IO, typ::Type{Float64}) = reinterpret(Float64, _read_fixed(io, convert(UInt64,0), 8))
function _read_fixed(io::IO, ret::T, N::Int) where {T <: Unsigned}
    for n in 0:(N-1)
        byte = convert(T, read(io, UInt8))
        ret |= (byte << *(8,n))
    end
    ret
end

function _read_varint(io::IO, ::Type{T}) where {T <: Integer}
    res = zero(T)
    n = 0
    byte = UInt8(MSB)
    while (byte & MSB) != 0
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
end

# parquet types:           BOOLEAN,      INT32,    INT64,    INT96,   FLOAT,   DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY
# enum values:                   0,          1,        2,        3,       4,        5,          6,                   7
const PLAIN_PROTOBUF_TYPES = ("bool", "sint32", "sint64", "sint64", "float", "double",    "bytes",              "bytes")

# parquet types:           BOOLEAN, INT32, INT64, INT96,    FLOAT,   DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY
# enum values:                   0,     1,     2,     3,        4,        5,          6,                    7
const PLAIN_THRIFT_TYPES = ("bool", "i32", "i64", "i64", "double", "double",   "binary",            "binary")

# parquet types:   BOOLEAN, INT32, INT64,  INT96,   FLOAT,  DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY
# enum values:           0,     1,     2,      3,       4,       5,          6,                    7
const PLAIN_JTYPES = (Bool, Int32, Int64, Int128, Float32, Float64,      UInt8,               UInt8)

# read plain encoding (PLAIN = 0)
function read_plain(io::IO, typ::Int32, jtype::Type{T}=PLAIN_JTYPES[typ+1]) where {T}
    if typ == _Type.BYTE_ARRAY
        count = read_fixed(io, Int32)
        #@debug("reading bytearray length:$count")
        read!(io, Array{UInt8}(undef, count))
    elseif typ == _Type.BOOLEAN
        error("not implemented") # reading single boolean values is not possible, vectors are read via read_bitpacked_booleans
    elseif typ == _Type.FIXED_LEN_BYTE_ARRAY
        #@debug("reading fixedlenbytearray length:$count")
        #read!(io, Array{UInt8}(count))
        error("not implemented") # this is likely same as BYTE_ARRAY for decoding purpose
    else
        #@debug("reading type:$jtype, typenum:$typ")
        read_fixed(io, jtype)
    end
end

# read plain values or dictionary (PLAIN_DICTIONARY = 2)
function read_plain_values(io::IO, count::Integer, typ::Int32)
    @debug("reading plain values", type=typ, count=count)
    if typ == _Type.BOOLEAN
        arr = read_bitpacked_booleans(io, count)
    else
        arr = [read_plain(io, typ) for i in 1:count]
    end
    @debug("read $(length(arr)) plain values")
    arr
end

function read_bitpacked_booleans(io::IO, count::Integer) #, bits::Integer, byt::Int=bit2bytewidth(bits), typ::Type{T}=byt2itype(byt), arr::Vector{T}=Array{T}(undef, count); read_len::Bool=true) where {T <: Integer}
    @debug("reading bitpacked booleans", count)
    arr = falses(count)
    arrpos = 1
    bits = UInt8(0)
    bitpos = 9
    while arrpos <= count
        if bitpos > 8
            bits = read(io, UInt8)
            @debug("bits", bits, bitstring(bits))
            bitpos = 1
        end
        arr[arrpos] = Bool(bits & 0x1)
        arrpos += 1
        bits >>= 1
        bitpos += 1
    end
    arr
end

# read rle dictionary (RLE_DICTIONARY = 8, or PLAIN_DICTIONARY = 2 in a data page)
function read_rle_dict(io::IO, count::Integer)
    bits = read(io, UInt8)
    @debug("reading rle dictionary bits:$bits")
    arr = read_hybrid(io, count, bits; read_len=false)
    @debug("read $(length(arr)) dictionary values")
    arr
end

# read RLE or bit backed format (RLE = 3)
function read_hybrid(io::IO, count::Integer, bits::Integer, byt::Int=bit2bytewidth(bits), typ::Type{T}=byt2itype(byt), arr::Vector{T}=Array{T}(undef, count); read_len::Bool=true) where {T <: Integer}
    # ZJ: Len is never used if read, so what's the point of reading?
    len = read_len ? read_fixed(io, Int32) : Int32(0)
    @debug("reading hybrid data", len, count, bits)
    arrpos = 1
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
end

function read_rle_run(io::IO, count::Integer, bits::Integer, byt::Int=bit2bytewidth(bits), typ::Type{T}=byt2itype(byt), arr::Vector{T}=Array{T}(count)) where {T <: Integer}
    @debug("read_rle_run. count:$count, typ:$T, nbits:$bits, nbytes:$byt")
    arr[1:count] .= reinterpret(T, _read_fixed(io, zero(byt2uitype(byt)), byt))
    arr
end

function read_bitpacked_run(io::IO, grp_count::Integer, bits::Integer, byt::Int=bit2bytewidth(bits), typ::Type{T}=byt2itype(byt), arr::Vector{T}=Array{T}(undef, grp_count*8)) where {T <: Integer}
    count = min(grp_count * 8, length(arr))
    # multiple of 8 values at a time are bit packed together
    nbytes = bits * grp_count # same as: round(Int, (bits * grp_count * 8) / 8)
    #@debug("read_bitpacked_run. grp_count:$grp_count, count:$count, nbytes:$nbytes, nbits:$bits, available:$(bytesavailable(io))")
    data = Array{UInt8}(undef, min(nbytes, bytesavailable(io)))
    read!(io, data)

    mask = MASKN(bits)
    V = typeof(mask)
    bitbuff = zero(V)
    nbitsbuff = 0
    shift = 0

    arridx = 1
    dataidx = 1
    while arridx <= count
        #@debug("arridx:$arridx nbitsbuff:$nbitsbuff shift:$shift bits:$bits")
        if nbitsbuff > 0
            # we have leftover bits, which must be appended
            if nbitsbuff < bits
                # but only append if we need to read more in this cycle
                arr[arridx] = bitbuff & MASKN(nbitsbuff)
                shift = nbitsbuff
                nbitsbuff = 0
                bitbuff = zero(V)
            end
        end

        # fill buffer
        while (nbitsbuff + shift) < bits
             # shift 8 bits and read directly into bitbuff
            bitbuff |= (V(data[dataidx]) << nbitsbuff)
            dataidx += 1
            nbitsbuff += 8
        end

        # set values
        while ((nbitsbuff + shift) >= bits) && (arridx <= count)
            if shift > 0
                remshift = bits - shift
                #@debug("setting part from bitbuff nbitsbuff:$nbitsbuff, shift:$shift, remshift:$remshift")
                arr[arridx] |= convert(T, (bitbuff << shift) & mask)
                bitbuff >>= remshift
                nbitsbuff -= remshift
                shift = 0
            else
                #@debug("setting all from bitbuff nbitsbuff:$nbitsbuff")
                arr[arridx] = convert(T, bitbuff & mask)
                bitbuff >>= bits
                nbitsbuff -= bits
            end
            arridx += 1
        end
    end
    arr
end

# read bit packed in deprecated format (BIT_PACKED = 4)
function read_bitpacked_run_old(io::IO, count::Integer, bits::Integer, byt::Int=bit2bytewidth(bits), typ::Type{T}=byt2itype(byt), arr::Vector{T}=Array{T}(undef, count)) where {T <: Integer}
    # multiple of 8 values at a time are bit packed together
    nbytes = round(Int, (bits * count) / 8)
    @debug("read_bitpacked_run. count:$count, nbytes:$nbytes, nbits:$bits")
    data = Array{UInt8}(undef, nbytes)
    read!(io, data)

    # the mask is of the smallest bounding type for bits
    # T is one of the types that map on to the appropriate Julia type in Parquet (which may be larger than the mask type)
    mask = MASKN(bits)
    V = typeof(mask)
    bitbuff = zero(V)
    nbitsbuff = 0

    arridx = 1
    dataidx = 1
    while arridx <= count
        diffnbits = bits - nbitsbuff
        while diffnbits > 8
            # shift 8 bits and read directly into bitbuff
            bitbuff <<= 8
            bitbuff |= data[dataidx]
            dataidx += 1
            nbitsbuff += 8
            diffnbits -= 8
        end

        if diffnbits > 0
            # read next byte from input
            nxtdata = data[dataidx]
            dataidx += 1
            # shift bitbuff by diffnbits, add diffnbits and set result
            nbitsbuff = 8 - diffnbits
            arr[arridx] = convert(T, ((bitbuff << diffnbits) | (nxtdata >> nbitsbuff)) & mask)
            arridx += 1
            # keep remaining bits in bitbuff
            bitbuff <<= 8
            bitbuff |= nxtdata
        else
            # set result
            arr[arridx] = convert(T, (bitbuff >> abs(diffnbits)) & mask)
            arridx += 1
            nbitsbuff -= bits
        end
    end
    arr
end
