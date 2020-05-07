using Diban

@time df = read_parquet("c:/scratch/nihao.parquet", multithreaded = false);

@time df = read_parquet("c:/scratch/test.parquet", [:V1, :V2, :V31]);

@time df = read_parquet("c:/scratch/test.parquet", ["V1", "V2", "V31"]);

using DataFrames


@time df2=DataFrame(df);

@time df = read_parquet("C:/git/parquet-data-collection/synthetic_data.parquet")

@time df = read_parquet("C:/scratch/nihao_zstd.parquet")

using DataFrames
@time df = DataFrame(df, copycols=false)
