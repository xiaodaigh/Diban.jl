using Base.Threads: @spawn
using Base.Iterators: drop
using ProgressMeter: Progress, update!
using Parquet: ParFile, ncols
using NamedTupleTools: namedtuple

read_parquet(path, cols::Vector{Symbol}; kwargs...) = read_parquet(path, String.(cols); kwargs...)

read_parquet(path; kwargs...) = read_parquet(path, String[]; kwargs...)

function read_parquet(path, cols::Vector{String}; verbose = false)
	"""function for reading parquet"""

	# df = DataFrame()

	nc = ncols(ParFile(path))

	colnames = [sch.name for sch in  drop(ParFile(path).schema.schema, 1)]

	if length(cols) == 0
		colnums = collect(1:nc)
	else
		colnums = [findfirst(==(c), colnames) for c in cols]
	end

	results = Vector{Any}(undef, length(colnums))
	for (i, j) in enumerate(colnums)
		results[i] = @spawn read_column(path, j)
	end

	symbol_col_names = collect(Symbol(col) for col in colnames[colnums])
	fnl_results = collect(fetch(result) for result in results)

	namedtuple(symbol_col_names, fnl_results)
end
