add_executable(flashbus-server flashbus_server.cpp)
target_link_libraries(flashbus-server PRIVATE flashbus)

add_executable(flashbus-pub flashbus_pub.cpp)
target_link_libraries(flashbus-pub PRIVATE flashbus)

add_executable(flashbus-sub flashbus_sub.cpp)
target_link_libraries(flashbus-sub PRIVATE flashbus)

# The benchmark reports allocations during the run, so it needs the counting
# operator new linked in.
add_executable(flashbus-bench flashbus_bench.cpp)
target_link_libraries(flashbus-bench PRIVATE flashbus flashbus_alloc_counter)
