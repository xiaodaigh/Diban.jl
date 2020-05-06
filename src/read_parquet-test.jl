using Diban

@time df = read_parquet("c:/scratch/nihao.parquet", multithreaded = false);

df = nothing

@time df = read_parquet("c:/scratch/test.parquet", [:V1, :V2, :V31]);

@time df = read_parquet("c:/scratch/test.parquet", ["V1", "V2", "V31"]);

using DataFrames


@time df2=DataFrame(df);
