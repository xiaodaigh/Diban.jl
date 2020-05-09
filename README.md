# Diban.jl (Dìbǎn 地板)

There is a `write_paruqet` and `read_parquet` functions to write and read
parquet files! It's EXTREMELY slow at the moment but it works on newer Parquet
files that Parquet.jl can't handle at the moment.

The intention is to contribute these functions back to Parquet.jl so as not to
fragment the community efforts. But the process is likely to be slow. Therefore,
I make Dìbǎn available while Parquet.jl is being worked on.

## Installation

You need a particular branch of Parquet.jl and the master branch of Diban.jl

```julia
# add the latest version of Dìbǎn
]add https://github.com/xiaodaigh/Diban.jl
```

## Usage

### Write
Diban supports `Int32, Int64, Float32, Float64, Bool, String` vectors, `missing`
values are supported.

```julia
using Diban
using DataFrames

tbl = DataFrame(
    int32 = Int32[-1, 0, 1],
    int64 = Int64[-10, 0, 10],
    float32 = Float32[-0.5, 0, 0.5],
    float64 = Float64[-0.5, 0, 0.5],
    bool = [true, false, true],
    string = ["abc", "def", "ghi"],
    int32m = Union{Missing, Int32}[-1, missing, 1],
    int64m = Union{Missing, Int64}[-10, missing, 10],
    float32m = Union{Missing, Float32}[-0.5, missing, 0.5],
    float64m = Union{Missing, Float64}[-0.5, missing, 0.5],
    boolm = Union{Missing, Bool}[true, missing, false],
    stringm = Union{Missing, String}["abc", missing, "ghi"],
)

path = "c:/scratch/tmp.parquet"
write_parquet(path, tbl)
a = read_parquet(path)
```


### Read
```julia
using Diban

# there are some bugs with multithreading so please use
read_parquet(path)

### reading only columns `col1` and `col2`
read_parquet(path, ["col1", "col2"])
```

### Notes & Bugs?

Currently, only UNnested columns are supported.

There are some bugs with multi-threading so you may want to use `mutlithreaded=false`

```
read_parquet(path, mutlithreaded=false)
```



## TODO
* Add support for CategoricalArrays
