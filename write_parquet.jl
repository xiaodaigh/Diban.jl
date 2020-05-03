a = open("c:/scratch/python-test.parquet", "w")
seek(a, 0)
write(a,"par1")


close(a)

x = Array{Int32, 1}(undef, 100)
