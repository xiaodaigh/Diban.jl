using Base.Threads: @spawn
using Base.Iterators: drop
#using ProgressMeter: @showprogress
using Parquet: ParFile, ncols
using NamedTupleTools: namedtuple

read_parquet(path, cols::Vector{Symbol}; kwargs...) = read_parquet(path, String.(cols); kwargs...)

read_parquet(path; kwargs...) = read_parquet(path, String[]; kwargs...)

function read_parquet(path, cols::Vector{String}; verbose = false)
	"""function for reading parquet"""

	# use a bounded channel to limit
    c1 = Channel{Bool}(Threads.nthreads())
    atexit(()->close(c1))

	print("ok")

	nc = ncols(ParFile(path))

	colnames = [sch.name for sch in  drop(ParFile(path).schema.schema, 1)]

	if length(cols) == 0
		colnums = collect(1:nc)
	else
		colnums = [findfirst(==(c), colnames) for c in cols]
	end

	results = Vector{Any}(undef, length(colnums))
	for (i, j) in enumerate(colnums)
		put!(c1, true)
		results[i] = @spawn read_column(path, j)
		take!(c1)
	end

	symbol_col_names = collect(Symbol(col) for col in colnames[colnums])
	fnl_results = collect(fetch(result) for result in results)
	#fnl_results = collect(result for result in results)

	namedtuple(symbol_col_names, fnl_results)
end
