# Diban.jl

There is a `read_parquet` function to read parquet files! It's EXTREMELY slow at the moment

## Installation & Usage

You need a particular branch of Parquet.jl

```julia
# add a particular version of Parquet.jl with fixes

]add https://github.com/xiaodaigh/Parquet.jl#zj/parquet-writer

# there are some bugs with multithreading so recommend to use without it for now
read_parquet(path, mutlithreaded=false)

### reading only these columns
read_parquet(path, ["col1", "col2"], mutlithreaded=false)
```
