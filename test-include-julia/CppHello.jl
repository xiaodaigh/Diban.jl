# Load the module and generate the functions
module CppHello
  using CxxWrap
  @wrapmodule(joinpath("/home/xiaodai/git/ParquetWriter.jl/test-include-julia/lib","libtestlib"))

  function __init__()
    @initcxx
  end
end

# Call greet and show the result
@show CppHello.greet("ok")

@show CppHello.greet("hello")