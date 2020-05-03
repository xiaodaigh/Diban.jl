

v = reduce(vcat, [readdir(p, join=true) for p in readdir("c:/data/parquet-test\\", join=true)])


files = [v for v in v if splitext(v)[2] == ".parquet"]



using ParquetWriter

@time a = read_parquet.(files)

for f in files
    try
        read_parquet(f)
    catch
        println(f)
    end
end

read_parquet("c:/scratch/python-test.parquet")

read_parquet("c:/scratch/test.parquet", [:V1])
