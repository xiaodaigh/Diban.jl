git clone https://github.com/apache/arrow.git
cd arrow/cpp
mkdir release
cd release
cmake .. -DThrift_SOURCE=BUNDLED -DCMAKE_INSTALL_PREFIX=/home/xiaodai/git/arrow_install_path -DARROW_PARQUET=ON
make
make install