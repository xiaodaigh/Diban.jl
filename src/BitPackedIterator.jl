import Base: iterate
import Base: length

struct BitPackedIterator
	arr::Vector{UInt8}
	bitwidth::UInt8
end

function iterate(bpiter::BitPackedIterator, pos=0)
	if pos >= sizeof(UInt)*length(bpiter.arr)
		return nothing
	end

	OnesMask = UInt8(2^bpiter.bitwidth - 1)

	bytes_to_skip, bits_to_shift = divrem(pos, sizeof(UInt))
	value = (bpiter.arr[1 + bytes_to_skip] >> bits_to_shift) & OnesMask

	# say pos=6 and bitwidth is 3 then after shifting
	# 6 spots, there are only two bits left and so we need one more bit
	# from the next byte
	bits_left_in_byte_post_shift = sizeof(UInt) - bits_to_shift

	if bits_left_in_byte_post_shift < bpiter.bitwidth
		# in order to look into the next byte you need to have the next byte
		if (1 + bytes_to_skip + 1) <= length(bpiter.arr)
			n_bits_needed_from_next_byte = bpiter.bitwidth - bits_left_in_byte_post_shift

			mask = 2^n_bits_needed_from_next_byte-1
			bits_from_next_byte = bpiter.arr[1 + bytes_to_skip + 1] & mask

			value = value | (bits_from_next_byte << n_bits_needed_from_next_byte)
		end
	end


	new_pos = pos + bpiter.bitwidth
	return value, new_pos
end

IteratorSize(BitPackedIterator) = HasLength()

length(bpiter::BitPackedIterator) = ceil(sizeof(UInt8)*length(bpiter.arr)/ bpiter.bitwidth)
