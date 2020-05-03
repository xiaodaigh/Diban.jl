# ParquetWriter.jl

I know it says writer in the title but it's just a temporary title.

There is a `read_parquet` function to read parquet files! It's EXTREMELY slow at the moment

```julia
read_parquet(path)

### reading only these columns
read_parquet(path, ["col1", "col2"])
```
