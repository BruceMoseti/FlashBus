// flashbus-server: the broker.

#include <csignal>
#include <iomanip>
#include <iostream>

#include "flashbus/cli.hpp"
#include "flashbus/metrics.hpp"
#include "flashbus/transport.hpp"

namespace {

flashbus::Server* g_server = nullptr;

void handle_signal(int) {
  if (g_server != nullptr) g_server->stop();
}

}  // namespace

int main(int argc, char** argv) {
  const flashbus::cli::Args args(argc, argv);
  args.reject_unknown({"port", "ingress-capacity", "egress-capacity", "egress-batch",
                       "adaptive-batching", "policy", "read-buffer", "network-cpu",
                       "dispatcher-cpu", "idle-spin-us", "idle-sleep-us", "quiet", "help"});
  if (args.flag("help")) {
    std::cout << "usage: flashbus-server [--port N] [--ingress-capacity N] "
                 "[--egress-capacity N]\n"
                 "                       [--egress-batch N] [--adaptive-batching] "
                 "[--policy drop-newest|disconnect|block]\n"
                 "                       [--read-buffer BYTES] [--network-cpu N] "
                 "[--dispatcher-cpu N]\n"
                 "                       [--idle-spin-us N] [--idle-sleep-us N] [--quiet]\n";
    return 0;
  }

  flashbus::ServerConfig config;
  config.port = args.number<uint16_t>("port", 9000);
  config.ingress_capacity = args.number<size_t>("ingress-capacity", config.ingress_capacity);
  config.egress_capacity = args.number<size_t>("egress-capacity", config.egress_capacity);
  config.egress_batch = args.number<size_t>("egress-batch", config.egress_batch);
  config.adaptive_batching = args.flag("adaptive-batching");
  config.read_buffer_bytes = args.number<size_t>("read-buffer", config.read_buffer_bytes);
  config.network_cpu = args.number<int>("network-cpu", -1);
  config.dispatcher_cpu = args.number<int>("dispatcher-cpu", -1);
  config.idle_spin_us = args.number<unsigned>("idle-spin-us", config.idle_spin_us);
  config.idle_sleep_us = args.number<unsigned>("idle-sleep-us", config.idle_sleep_us);

  const std::string policy_name = args.string("policy", "drop-newest");
  const auto policy = flashbus::parse_policy(policy_name);
  if (!policy) {
    std::cerr << "flashbus-server: unknown --policy '" << policy_name
              << "' (drop-newest|disconnect|block)\n";
    return 2;
  }
  config.policy = *policy;

  flashbus::Server server(config);
  g_server = &server;
  std::signal(SIGINT, handle_signal);
  std::signal(SIGTERM, handle_signal);

  try {
    server.start();
  } catch (const std::exception& error) {
    std::cerr << "flashbus-server: " << error.what() << '\n';
    return 1;
  }

  if (!args.flag("quiet")) {
    flashbus::print_env(std::cout);
    std::cout << "listening on port " << server.port() << ", egress batch "
              << config.egress_batch << (config.adaptive_batching ? " (adaptive)" : "")
              << ", overflow policy " << flashbus::to_string(config.policy) << "\n"
              << "ingress capacity " << config.ingress_capacity << ", egress capacity "
              << config.egress_capacity << " frames per connection\n";
  }

  server.run();

  const flashbus::ServerStats stats = server.stats();
  std::cout << "\nconnections   " << stats.connections_accepted << " accepted\n"
            << "frames in     " << stats.frames_in << " (rejected " << stats.frames_rejected
            << ")\n"
            << "frames routed " << stats.frames_routed << " (unroutable "
            << stats.frames_unroutable << ")\n"
            << "frames out    " << stats.frames_delivered << " (dropped " << stats.frames_dropped
            << ")\n"
            << "bytes         " << stats.bytes_in << " in / " << stats.bytes_out << " out\n"
            << "egress depth  " << stats.egress_high_water << " high water\n"
            << "subscriptions " << stats.subscriptions << '\n';
  if (stats.block_spins != 0) {
    std::cout << "block spins   " << stats.block_spins
              << "  <- the dispatcher waited on a slow subscriber this many times\n";
  }
  return 0;
}
