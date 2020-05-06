using ParquetWriter

v = reduce(vcat, [readdir(p, join=true) for p in readdir("c:/data/parquet-test\\", join=true)])

@time adf = read_parquet("c:/scratch\\test.parquet");

files = [v for v in v if splitext(v)[2] == ".parquet" && isfile(v)]
@time Threads.@threads for f in files
    @time read_parquet(f, multithreaded = false);
end

files = readdir("C:/git/parquet-data-collection", join=true)

ff = [f for f in files if isfile(f) && splitext(f)[2]==".parquet"]

@time read_parquet.(ff)

@time read_parquet(ff[3]);

@time a= read_parquet(ff[3], multithreaded = false);

using BenchmarkTools
@benchmark read_parquet(ff[3])

ncols(ParFile(ff[3]))
filemetadata = metadata(ff[3])

ep = [@elapsed read_column(ff[3], filemetadata, i) for i in 1:169]

using StatsBase, Statistics
mean(ep)
std(ep)


ff[3]

using ParquetFiles

using ParquetFiles

@time read_parquet(ff[2], multithreaded =false);

for i in 1:ncols(ParFile(ff[2]))
    println(i)
    read_column(ff[2], i)
end

read_column(ff[2], 33)

a = metadata("c:/scratch/test.parquet")

a

using Parquet:ParFile
using Parquet
par = ParFile("c:/scratch\\test.parquet")
filemetadata = metadata("c:/scratch\\test.parquet")
filemetadata.row_groups[1].columns[5]
row_group = filemetadata.row_groups[1]
pages = Parquet.pages(par, row_group.columns[5])


@time a = read_parquet.(ff; multithreaded = false);

@time a = read_parquet.(ff; multithreaded = true);

a

for ff in ff
    try
        read_parquet(ff)
    catch
        println(ff)
    end
end


read_parquet("C:/git/parquet-data-collection/noshowappointments.parquet")

read_column("C:/git/parquet-data-collection/noshowappointments.parquet", 1)

using ParquetWriter

metadata.(files)

mf = metadata(files[2])

@time a = read_parquet(files[2])

using DataFrames|
@time a = read_parquet("c:/scratch/test.parquet");
@time ad = DataFrame(a, copycols=false);

read_column(files[1], 9)

read_parquet(files[1])

read_column(files[1], 2)



i = 0
for f in files
    global i
    try
        read_parquet(f)
        println("ok")
    catch
        i += 1
        println(f)
    end
end



metadata("c:/data/parquet-test/deep_solar_dataset/dsd50p.parquet")

m=metadata("c:/scratch/test1.parquet")

read_parquet("c:/scratch/test.parquet", [:V1])
