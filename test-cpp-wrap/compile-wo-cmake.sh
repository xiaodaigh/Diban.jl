gcc testlib.cpp -so everything -fPIC \
    -I/home/xiaodai/julia-1.4.1/include/julia \
    -L/home/xiaodai/julia-1.4.1/lib \
    -I/home/xiaodai/git/arrow_install_path/include \
    -L/home/xiaodai/git/arrow_install_path/lib \
    -I/home/xiaodai/.julia/artifacts/b55cd96f4dd60da926644fc18aef04c69f891b7d/include \
    -L/home/xiaodai/.julia/artifacts/b55cd96f4dd60da926644fc18aef04c69f891b7d/lib \
    -lparquet -larrow \
    -std=c++1z \
    -ljulia /home/xiaodai/julia-1.4.1/lib/julia/libstdc++.so.6
