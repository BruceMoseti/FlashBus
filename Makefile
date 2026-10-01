# Convenience wrapper around CMake and the benchmark scripts.
#
# CMake is the build system; nothing here is required to build FlashBus. This
# exists so that someone who has just cloned the repository can get from zero
# to passing tests in one command.

# Some distributions point /usr/bin/c++ at a clang that cannot find libstdc++,
# so the compiler is named explicitly. Override with `make CXX=clang++`.
CXX ?= g++
JOBS ?= $(shell nproc 2>/dev/null || echo 4)
PRESET ?= default

.DEFAULT_GOAL := help
.PHONY: help deps build test test-all sanitizers bench bench-quick report perf \
        demo lint clean distclean

help: ## Show this help
	@echo "FlashBus — low-latency event streaming engine"
	@echo
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[1m%-14s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "Variables:  CXX=$(CXX)  JOBS=$(JOBS)  PRESET=$(PRESET)"

deps: ## Install build dependencies (Debian/Ubuntu)
	sudo apt-get update
	sudo apt-get install -y g++ cmake libboost-system-dev libgtest-dev python3-pip
	python3 -m pip install --user matplotlib

build: ## Configure and build (PRESET=default|native|asan|ubsan|tsan)
	CXX=$(CXX) cmake --preset $(PRESET)
	cmake --build build/$(PRESET) -j$(JOBS)

test: build ## Build and run the correctness suite
	ctest --test-dir build/$(PRESET) --output-on-failure -j1

test-all: ## Run the correctness suite under every sanitizer
	@for preset in default asan ubsan tsan; do \
	  echo "=== $$preset"; \
	  CXX=$(CXX) cmake --preset $$preset > /dev/null || exit 1; \
	  cmake --build build/$$preset -j$(JOBS) > /dev/null || exit 1; \
	  ctest --test-dir build/$$preset --output-on-failure -j1 || exit 1; \
	done

sanitizers: test-all ## Alias for test-all

bench: build ## Run the full benchmark suite into results/latest (~25 min)
	python3 scripts/run_benchmarks.py
	python3 scripts/plot_latency.py
	python3 scripts/report.py

bench-quick: build ## Short benchmark run, for checking the harness not measuring
	python3 scripts/run_benchmarks.py --quick --out results/quick
	python3 scripts/plot_latency.py --results results/quick

report: ## Regenerate the README tables and charts from results/latest
	python3 scripts/plot_latency.py
	python3 scripts/report.py

perf: build ## Collect Linux perf counters, where the machine exposes them
	bash scripts/run_perf.sh --build build/$(PRESET)

demo: build ## End-to-end demo: broker, publisher, subscriber, order book
	@echo "--- end-to-end through real sockets, 4 publishers and 4 subscribers"
	./build/$(PRESET)/apps/flashbus-bench \
	  --producers 4 --consumers 4 --messages 2000000 --payload 64 --rate 400000
	@echo
	@echo "--- synthetic market data into an order-book consumer, bursty"
	./build/$(PRESET)/examples/market_data/market-data-demo \
	  --events 2000000 --rate 200000 --burst

lint: ## Lint the Python scripts
	python3 -m ruff check scripts/

clean: ## Remove build output
	rm -rf build

distclean: clean ## Also remove generated benchmark output
	rm -rf results/quick results/perf
