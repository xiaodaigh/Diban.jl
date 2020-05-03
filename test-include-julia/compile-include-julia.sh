#export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/xiaodai/git/arrow_install_path/lib:/home/xiaodai/julia-1.4.1/lib

g++ include-julia.cpp \
    -I/home/xiaodai/julia-1.4.1/include/julia \
    #-L/home/xiaodai/julia-1.4.1/lib \
    -o include-julia


g++ include-julia.cpp \
    -I/home/xiaodai/julia-1.4.1/include/julia \
    -o include-julia

gcc -o test -fPIC \
    -I/home/xiaodai/julia-1.4.1/include/julia \
    -L/home/xiaodai/julia-1.4.1/lib \
    test.cpp \
    -ljulia /home/xiaodai/julia-1.4.1/lib/julia/libstdc++.so.6


export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/xiaodai/git/arrow_install_path/lib:/home/xiaodai/julia-1.4.1/lib
gcc include-parquet-too.cpp -o include-parquet -fPIC \
    -I/home/xiaodai/julia-1.4.1/include/julia \
    -L/home/xiaodai/julia-1.4.1/lib \
    -I/home/xiaodai/git/arrow_install_path/include \
    -L/home/xiaodai/git/arrow_install_path/lib \
    -lparquet -larrow \
    -ljulia /home/xiaodai/julia-1.4.1/lib/julia/libstdc++.so.6

################################################################
git clone https://github.com/JuliaInterop/libcxxwrap-julia.git
mkdir libcxxwrap-julia-build
cd libcxxwrap-julia-build
cmake -DJulia_PREFIX=/home/xiaodai/julia-1.4.1 ../libcxxwrap-julia
cmake --build . --config Release

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/xiaodai/git/arrow_install_path/lib:/home/xiaodai/julia-1.4.1/lib:/home/xiaodai/git/libcxxwrap-julia-build/lib

gcc testlib.cpp -o everything -fPIC \
    -I/home/xiaodai/julia-1.4.1/include/julia \
    -L/home/xiaodai/julia-1.4.1/lib \
    -I/home/xiaodai/git/arrow_install_path/include \
    -L/home/xiaodai/git/arrow_install_path/lib \
    -I/home/xiaodai/.julia/artifacts/b55cd96f4dd60da926644fc18aef04c69f891b7d/include \
    -L/home/xiaodai/.julia/artifacts/b55cd96f4dd60da926644fc18aef04c69f891b7d/lib \
    -lparquet -larrow \
    -std=c++1z \
    -ljulia /home/xiaodai/julia-1.4.1/lib/julia/libstdc++.so.6

gcc include-everything.cpp -o include-everything -fPIC \
    -I/home/xiaodai/julia-1.4.1/include/julia \
    -L/home/xiaodai/julia-1.4.1/lib \
    -I/home/xiaodai/git/arrow_install_path/include \
    -L/home/xiaodai/git/arrow_install_path/lib \
    -I/home/xiaodai/.julia/artifacts/b55cd96f4dd60da926644fc18aef04c69f891b7d/include \
    -L/home/xiaodai/git/libcxxwrap-julia-build/lib \
    -lparquet -larrow \
    -std=gnu++1z \
    -ljulia /home/xiaodai/julia-1.4.1/lib/julia/libstdc++.so.6