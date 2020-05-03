g++ testlib.cpp \
    -I/home/xiaodai/git/arrow_install_path/include \
    -I/home/xiaodai/julia-1.4.1/include/julia \
    -I/home/xiaodai/.julia/artifacts/b55cd96f4dd60da926644fc18aef04c69f891b7d/include \
    -L/home/xiaodai/git/arrow_install_path/lib \
    -L/home/xiaodai/.julia/artifacts/b55cd96f4dd60da926644fc18aef04c69f891b7d/lib \
    -L/home/xiaodai/julia-1.4.1/lib \
    -lparquet -larrow  -ljulia /home/xiaodai/julia-1.4.1/lib/julia/libstdc++.so.6 \
    -o testlib

g++ test.cpp \
    -I/home/xiaodai/git/arrow_install_path/include \
    -I/home/xiaodai/julia-1.4.1/include/julia \
    -I/home/xiaodai/.julia/artifacts/b55cd96f4dd60da926644fc18aef04c69f891b7d/include \
    -L/home/xiaodai/git/arrow_install_path/lib \
    -L/home/xiaodai/.julia/artifacts/b55cd96f4dd60da926644fc18aef04c69f891b7d/lib \
    -L/home/xiaodai/julia-1.4.1/lib \
    -lparquet -larrow  -ljulia /home/xiaodai/julia-1.4.1/lib/julia/libstdc++.so.6 \
    -ljlcxx \
    -o test

