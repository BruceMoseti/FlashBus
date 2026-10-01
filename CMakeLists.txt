cmake_minimum_required(VERSION 3.20)
project(flashbus LANGUAGES CXX VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()

option(FLASHBUS_BUILD_TESTS "Build correctness tests" ON)
option(FLASHBUS_BUILD_BENCHMARKS "Build benchmarks" ON)
option(FLASHBUS_NATIVE_ARCH "Compile with -march=native" OFF)
set(FLASHBUS_SANITIZER "none" CACHE STRING "none|address|thread|undefined")
set_property(CACHE FLASHBUS_SANITIZER PROPERTY STRINGS none address thread undefined)

# Benchmarks need frame pointers to be profileable, and -O2 everywhere so that
# a "measured" number is never an artefact of an unoptimised build.
add_library(flashbus_flags INTERFACE)
target_compile_options(flashbus_flags INTERFACE
  -Wall -Wextra -Wpedantic -Wconversion -Wshadow -fno-omit-frame-pointer)

if(FLASHBUS_NATIVE_ARCH)
  target_compile_options(flashbus_flags INTERFACE -march=native)
endif()

if(NOT FLASHBUS_SANITIZER STREQUAL "none")
  if(FLASHBUS_SANITIZER STREQUAL "undefined")
    set(_fb_san "undefined,float-divide-by-zero")
  else()
    set(_fb_san "${FLASHBUS_SANITIZER}")
  endif()
  target_compile_options(flashbus_flags INTERFACE
    -fsanitize=${_fb_san} -fno-sanitize-recover=all -g)
  target_link_options(flashbus_flags INTERFACE -fsanitize=${_fb_san})
endif()

# Recorded into every benchmark result, so a number can be traced back to the
# build that produced it.
string(TOUPPER "${CMAKE_BUILD_TYPE}" _fb_build_type)
string(STRIP "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${_fb_build_type}}" _fb_flags)
target_compile_definitions(flashbus_flags INTERFACE
  FLASHBUS_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
  FLASHBUS_CXX_FLAGS="${_fb_flags}"
  FLASHBUS_SANITIZER_NAME="${FLASHBUS_SANITIZER}")

find_package(Boost 1.70 REQUIRED COMPONENTS system)
find_package(Threads REQUIRED)

add_library(flashbus
  src/transport.cpp
  src/session.cpp
  src/dispatcher.cpp
  src/publisher.cpp
  src/subscriber.cpp
  src/platform.cpp
  src/metrics.cpp
)
target_include_directories(flashbus PUBLIC include)
target_link_libraries(flashbus
  PUBLIC flashbus_flags Boost::system Threads::Threads)
target_compile_definitions(flashbus PUBLIC BOOST_ASIO_NO_DEPRECATED)

# Counting operator new is how the "no allocation on the hot path" claim is
# checked. An OBJECT library rather than a static one, so the linker cannot
# decide to leave the replacement out.
add_library(flashbus_alloc_counter OBJECT src/alloc_counter.cpp)
target_link_libraries(flashbus_alloc_counter PUBLIC flashbus_flags)
target_include_directories(flashbus_alloc_counter PUBLIC include)

add_subdirectory(apps)
add_subdirectory(examples/market_data)

if(FLASHBUS_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(FLASHBUS_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()

message(STATUS "flashbus: build=${CMAKE_BUILD_TYPE} sanitizer=${FLASHBUS_SANITIZER} "
               "native=${FLASHBUS_NATIVE_ARCH} boost=${Boost_VERSION}")
