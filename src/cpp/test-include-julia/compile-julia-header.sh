
cmake -DCMAKE_BUILD_TYPE=Release \
    -DPARQUET_HOME=/home/xiaodai/git/arrow_install_path/ \
    -DCMAKE_PREFIX_PATH=/home/xiaodai/git/arrow_install_path:/home/xiaodai/.julia/artifacts/b55cd96f4dd60da926644fc18aef04c69f891b7d \
    /home/xiaodai/git/ParquetWriter.jl/test-include-julia

cmake --build . --config Release

-DCMAKE_MODULE_PATH=/home/xiaodai/git/ParquetWriter.jl/test-include-julia/cmake_modules \

