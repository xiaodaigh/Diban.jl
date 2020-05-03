mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release-DCMAKE_PREFIX_PATH=/home/xiaodai/git/arrow_install_path -DThrift_SOURCE=BUNDLED -DARROW_PARQUET=ON /home/xiaodai/git/ParquetWriter.jl/test-cppwrap
cmake --build . --config Release

#/home/xiaodai/.julia/artifacts/b55cd96f4dd60da926644fc18aef04c69f891b7d: