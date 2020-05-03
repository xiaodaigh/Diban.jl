using ParquetWriter

@time df = read_parquet("c:/scratch/test.parquet");

@time df = read_parquet("c:/scratch/test.parquet", [:V1, :V2, :V31]);

@time df = read_parquet("c:/scratch/test.parquet", ["V1", "V2", "V31"]);

using DataFrames


@time df2=DataFrame(df);
