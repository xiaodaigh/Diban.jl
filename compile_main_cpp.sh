#compile
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/xiaodai/git/arrow_install_path/lib:/home/xiaodai/julia-1.4.1/lib
g++ main.cpp \
    -I/home/xiaodai/git/arrow_install_path/include \
    -L/home/xiaodai/git/arrow_install_path/lib \
    -lparquet -larrow -o main

