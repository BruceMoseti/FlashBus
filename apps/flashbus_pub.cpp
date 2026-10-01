// flashbus-pub: publishes synthetic events at a target rate.

#include <csignal>
#include <iomanip>
#include <iostream>
#include <vector>

#include "flashbus/cli.hpp"
#include "flashbus/clock.hpp"
#include "flashbus/publisher.hpp"

namespace {
volatile std::sig_atomic_t g_stop = 0;
void handle_signal(int) { g_stop = 1; }
}  // namespace

int main(int argc, char** argv) {
  const flashbus::cli::Args args(argc, argv);
  args.reject_unknown(
      {"host", "port", "topic", "rate", "payload", "messages", "batch", "cpu", "help"});
  if (args.flag("help")) {
    std::cout << "usage: flashbus-pub [--host H] [--port N] [--topic NAME|ID] [--rate MSG/S]\n"
                 "                    [--payload BYTES] [--messages N] [--batch FRAMES] "
                 "[--cpu N]\n"
                 "  --rate 0 publishes as fast as possible.\n";
    return 0;
  }

  const std::string host = args.string("host", "127.0.0.1");
  const auto port = args.number<uint16_t>("port", 9000);
  const std::string topic_arg = args.string("topic", "trades");
  const auto topic = flashbus::resolve_topic(topic_arg);
  if (!topic) {
    std::cerr << "flashbus-pub: bad --topic '" << topic_arg << "' (name, or id 0.."
              << flashbus::kMaxTopicId << ")\n";
    return 2;
  }
  const auto rate = args.number<uint64_t>("rate", 100000);
  const auto payload_size = args.number<size_t>("payload", 64);
  const auto messages = args.number<uint64_t>("messages", 0);
  const auto batch = args.number<size_t>("batch", 1);
  const int cpu = args.number<int>("cpu", -1);

  if (payload_size > flashbus::kMaxInlinePayload) {
    std::cerr << "flashbus-pub: --payload above the broker's inline limit of "
              << flashbus::kMaxInlinePayload << " bytes\n";
    return 2;
  }
  if (cpu >= 0 && !flashbus::pin_to_cpu(static_cast<unsigned>(cpu))) {
    std::cerr << "flashbus-pub: could not pin to CPU " << cpu << " (cpuset restriction?)\n";
  }

  std::signal(SIGINT, handle_signal);
  std::signal(SIGTERM, handle_signal);

  flashbus::PublisherConfig config;
  config.batch_frames = batch;
  std::unique_ptr<flashbus::Publisher> publisher;
  try {
    publisher = std::make_unique<flashbus::Publisher>(host, port, config);
  } catch (const std::exception& error) {
    std::cerr << "flashbus-pub: connect failed: " << error.what() << '\n';
    return 1;
  }

  std::vector<std::byte> payload(payload_size);
  for (size_t i = 0; i < payload_size; ++i) payload[i] = static_cast<std::byte>(i & 0xFF);

  const flashbus::Pacer pacer(rate);
  const uint64_t started = flashbus::now_ns();
  uint64_t index = 0;
  while (g_stop == 0 && publisher->connected() && (messages == 0 || index < messages)) {
    pacer.wait_for(index);
    // The publisher's own sequence goes in the first bytes of the payload so a
    // consumer can still see per-publisher ordering after the broker has
    // replaced the header sequence with its own.
    if (payload_size >= sizeof(uint64_t)) {
      const uint64_t stamp = index + 1;
      std::memcpy(payload.data(), &stamp, sizeof(stamp));
    }
    publisher->publish(*topic, payload.data(), payload_size);
    ++index;
  }
  publisher->flush();
  const double elapsed = static_cast<double>(flashbus::now_ns() - started) / 1e9;

  std::cout << std::fixed << std::setprecision(3) << "published " << publisher->sent()
            << " events on topic " << flashbus::topic_name(*topic) << " in " << elapsed << " s ("
            << static_cast<double>(publisher->sent()) / elapsed / 1e6 << " M msg/s, "
            << publisher->bytes_written() << " bytes)\n";
  if (!publisher->connected()) std::cerr << "flashbus-pub: connection dropped\n";
  return 0;
}
