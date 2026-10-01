// flashbus-sub: subscribes to a topic and reports end-to-end latency.

#include <csignal>
#include <iomanip>
#include <iostream>

#include "flashbus/cli.hpp"
#include "flashbus/clock.hpp"
#include "flashbus/metrics.hpp"
#include "flashbus/subscriber.hpp"

namespace {
volatile std::sig_atomic_t g_stop = 0;
void handle_signal(int) { g_stop = 1; }
}  // namespace

int main(int argc, char** argv) {
  const flashbus::cli::Args args(argc, argv);
  args.reject_unknown({"host", "port", "topic", "messages", "cpu", "sleep-us", "help"});
  if (args.flag("help")) {
    std::cout << "usage: flashbus-sub [--host H] [--port N] [--topic NAME|ID] [--messages N]\n"
                 "                    [--cpu N] [--sleep-us N]\n"
                 "  --sleep-us makes this a deliberately slow consumer, for testing "
                 "backpressure.\n";
    return 0;
  }

  const std::string host = args.string("host", "127.0.0.1");
  const auto port = args.number<uint16_t>("port", 9000);
  const std::string topic_arg = args.string("topic", "trades");
  const auto topic = flashbus::resolve_topic(topic_arg);
  if (!topic) {
    std::cerr << "flashbus-sub: bad --topic '" << topic_arg << "'\n";
    return 2;
  }
  const auto messages = args.number<uint64_t>("messages", 0);
  const int cpu = args.number<int>("cpu", -1);
  const auto sleep_us = args.number<unsigned>("sleep-us", 0);

  if (cpu >= 0 && !flashbus::pin_to_cpu(static_cast<unsigned>(cpu))) {
    std::cerr << "flashbus-sub: could not pin to CPU " << cpu << " (cpuset restriction?)\n";
  }
  std::signal(SIGINT, handle_signal);
  std::signal(SIGTERM, handle_signal);

  std::unique_ptr<flashbus::Subscriber> subscriber;
  try {
    subscriber = std::make_unique<flashbus::Subscriber>(host, port);
  } catch (const std::exception& error) {
    std::cerr << "flashbus-sub: connect failed: " << error.what() << '\n';
    return 1;
  }
  subscriber->subscribe(*topic);

  flashbus::Histogram latency;
  uint64_t first_ns = 0;
  uint64_t last_ns = 0;
  while (g_stop == 0 && subscriber->connected() &&
         (messages == 0 || subscriber->received() < messages)) {
    subscriber->poll([&](const flashbus::MessageHeader& header, const std::byte*, size_t) {
      const uint64_t received_ns = flashbus::now_ns();
      if (received_ns > header.timestamp_ns) latency.record(received_ns - header.timestamp_ns);
      if (first_ns == 0) first_ns = received_ns;
      last_ns = received_ns;
    });
    if (sleep_us != 0) std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));
  }

  const double elapsed = last_ns > first_ns ? static_cast<double>(last_ns - first_ns) / 1e9 : 0.0;
  const auto us = [](uint64_t ns) { return static_cast<double>(ns) / 1000.0; };
  std::cout << std::fixed << std::setprecision(3) << "received      "
            << subscriber->received() << " events on topic " << flashbus::topic_name(*topic)
            << '\n';
  if (elapsed > 0.0) {
    std::cout << "throughput    " << static_cast<double>(subscriber->received()) / elapsed / 1e6
              << " M msg/s over " << elapsed << " s\n";
  }
  std::cout << "latency us    p50 " << us(latency.percentile(50)) << "  p95 "
            << us(latency.percentile(95)) << "  p99 " << us(latency.percentile(99)) << "  p99.9 "
            << us(latency.percentile(99.9)) << "  max " << us(latency.max()) << '\n'
            << "sequence      " << subscriber->gaps() << " gaps, " << subscriber->missing()
            << " events missing\n";
  if (subscriber->decode_error() != flashbus::DecodeError::kOk) {
    std::cerr << "flashbus-sub: protocol error: "
              << flashbus::to_string(subscriber->decode_error()) << '\n';
    return 1;
  }
  return 0;
}
