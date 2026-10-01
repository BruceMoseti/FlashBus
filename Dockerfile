# Reproducible build and test environment.
#
# Useful for checking the build on a clean machine. Not useful for measuring:
# container CPU limits, a shared host and the absence of a virtual PMU all land
# on the numbers, and docs/BENCHMARKING.md explains why that matters.
#
#   docker build -t flashbus .
#   docker run --rm flashbus                       # runs the correctness suite
#   docker run --rm flashbus make test-all         # and under every sanitizer
#   docker run --rm flashbus ./build/default/apps/flashbus-bench --help

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      g++ \
      cmake \
      make \
      libboost-system-dev \
      libgtest-dev \
      python3 \
    && rm -rf /var/lib/apt/lists/*

# Deliberately no matplotlib: this image builds and tests, and the charts are a
# host-side concern. Keeping the image to apt packages keeps it reproducible.

WORKDIR /flashbus
COPY . .

# /usr/bin/c++ is clang on some images and cannot always find libstdc++.
ENV CXX=g++

RUN cmake --preset default && cmake --build build/default -j"$(nproc)"

CMD ["ctest", "--test-dir", "build/default", "--output-on-failure", "-j1"]
