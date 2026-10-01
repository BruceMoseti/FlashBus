# Every benchmark links the allocation counter: "no allocation on the hot path"
# is a measurement, not a comment.
function(flashbus_benchmark name)
  add_executable(${name} ${name}.cpp)
  target_link_libraries(${name} PRIVATE flashbus flashbus_alloc_counter ${ARGN})
endfunction()

flashbus_benchmark(clock_bench)
flashbus_benchmark(queue_bench)
flashbus_benchmark(affinity_bench)
flashbus_benchmark(sustained_bench)
flashbus_benchmark(alloc_bench flashbus_market_data)
