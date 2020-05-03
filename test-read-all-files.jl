

v = reduce(vcat, [readdir(p, join=true) for p in readdir("c:/data/parquet-test\\", join=true)])


files = [v for v in v if splitext(v)[2] == ".parquet" && isfile(v)]

files = readdir("C:/git/parquet-data-collection", join=true)
ff = [f for f in files if isfile(f) && splitext(f)[2]==".parquet"]
@time a = read_parquet.(ff);

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
