#pragma once

// A deliberately small `--key value` / `--flag` parser.
//
// It rejects unknown options rather than ignoring them. A typo in a benchmark
// flag that silently leaves a default in place is how people publish numbers
// for a configuration they did not run.

#include <charconv>
#include <cstdint>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace flashbus::cli {

class Args {
 public:
  Args(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
      std::string_view token(argv[i]);
      if (!token.starts_with("--")) {
        unexpected_.emplace_back(token);
        continue;
      }
      token.remove_prefix(2);
      if (const auto equals = token.find('='); equals != std::string_view::npos) {
        entries_.emplace_back(std::string(token.substr(0, equals)),
                              std::string(token.substr(equals + 1)));
        continue;
      }
      if (i + 1 < argc && std::string_view(argv[i + 1]).substr(0, 2) != "--") {
        entries_.emplace_back(std::string(token), std::string(argv[++i]));
      } else {
        entries_.emplace_back(std::string(token), std::string{});
      }
    }
  }

  [[nodiscard]] bool has(std::string_view name) const { return find(name) != nullptr; }

  [[nodiscard]] std::string string(std::string_view name, std::string fallback) const {
    const std::string* value = find(name);
    return value && !value->empty() ? *value : fallback;
  }

  template <typename T>
  [[nodiscard]] T number(std::string_view name, T fallback) const {
    const std::string* value = find(name);
    if (value == nullptr || value->empty()) return fallback;
    T parsed{};
    const char* end = value->data() + value->size();
    const auto result = std::from_chars(value->data(), end, parsed);
    if (result.ec != std::errc{} || result.ptr != end) {
      std::cerr << "flashbus: --" << name << " expects a number, got '" << *value << "'\n";
      std::exit(2);
    }
    return parsed;
  }

  /// A bare `--name`, or `--name=true`.
  [[nodiscard]] bool flag(std::string_view name) const {
    const std::string* value = find(name);
    if (value == nullptr) return false;
    return value->empty() || *value == "1" || *value == "true" || *value == "yes";
  }

  /// Prints and exits if anything was passed that the caller does not accept.
  void reject_unknown(const std::vector<std::string_view>& known) const {
    bool bad = false;
    for (const auto& [key, value] : entries_) {
      bool found = false;
      for (const std::string_view candidate : known) {
        if (key == candidate) found = true;
      }
      if (!found) {
        std::cerr << "flashbus: unknown option --" << key << '\n';
        bad = true;
      }
    }
    for (const std::string& token : unexpected_) {
      std::cerr << "flashbus: unexpected argument '" << token << "'\n";
      bad = true;
    }
    if (bad) {
      std::cerr << "flashbus: accepted options:";
      for (const std::string_view candidate : known) std::cerr << " --" << candidate;
      std::cerr << '\n';
      std::exit(2);
    }
  }

 private:
  [[nodiscard]] const std::string* find(std::string_view name) const {
    for (const auto& [key, value] : entries_) {
      if (key == name) return &value;
    }
    return nullptr;
  }

  std::vector<std::pair<std::string, std::string>> entries_;
  std::vector<std::string> unexpected_;
};

}  // namespace flashbus::cli
