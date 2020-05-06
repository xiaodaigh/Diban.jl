# Diban.jl (Dìbǎn 地板)

There is a `read_parquet` function to read parquet files! It's EXTREMELY slow at the moment but it works on newer Parquet files that Parquet.jl can't handle at the moment.

## Installation & Usage

You need a particular branch of Parquet.jl and the master branch of Diban.jl

```julia
]dev Thrift

# add a particular version of Parquet.jl with fixes
]add https://github.com/xiaodaigh/Parquet.jl#zj/fix-reader

# add the latest version of Dìbǎn
]add https://github.com/xiaodaigh/Diban.jl
```

### Usage
```julia
using Diban
# there are some bugs with multithreading so recommend to use without it for now
read_parquet(path, mutlithreaded=false)

### reading only columns `col1` and `col2`
read_parquet(path, ["col1", "col2"], mutlithreaded=false)
```
