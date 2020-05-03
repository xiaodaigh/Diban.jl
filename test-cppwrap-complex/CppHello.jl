# Load the module and generate the functions
module CppHello
  using CxxWrap
  @wrapmodule(joinpath("/home/xiaodai/git/ParquetWriter.jl/test-cppwrap/build/lib","libtestlib"))

  function __init__()
    @initcxx
  end
end

# Call greet and show the result
@show CppHello.greet()