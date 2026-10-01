#include "flashbus/protocol.hpp"

#include <gtest/gtest.h>

#include <cstring>
#include <numeric>
#include <random>
#include <vector>

namespace flashbus {
namespace {

struct Decoded {
  MessageHeader header;
  std::vector<std::byte> payload;
};

/// Collects frames so a test can assert on what came out, not just that
/// nothing crashed.
class Sink {
 public:
  auto handler() {
    return [this](const MessageHeader& header, const std::byte* payload, size_t size) {
      frames_.push_back(Decoded{header, std::vector<std::byte>(payload, payload + size)});
    };
  }
  const std::vector<Decoded>& frames() const { return frames_; }
  void clear() { frames_.clear(); }

 private:
  std::vector<Decoded> frames_;
};

std::vector<std::byte> make_frame(uint64_t sequence, uint32_t topic, size_t payload_size,
                                  MessageType type = MessageType::kData) {
  MessageHeader header;
  header.type = type;
  header.topic = topic;
  header.sequence = sequence;
  header.timestamp_ns = 1000 + sequence;
  header.payload_size = static_cast<uint32_t>(payload_size);

  std::vector<std::byte> bytes(kHeaderSize + payload_size);
  encode_header(bytes.data(), header);
  for (size_t i = 0; i < payload_size; ++i) {
    bytes[kHeaderSize + i] = static_cast<std::byte>((sequence + i) & 0xFF);
  }
  return bytes;
}

TEST(Header, RoundTrips) {
  MessageHeader in;
  in.type = MessageType::kSubscribe;
  in.topic = 0xDEADBEEF;
  in.payload_size = 1234;
  in.flags = 0xBEEF;
  in.sequence = 0x0123456789ABCDEF;
  in.timestamp_ns = 0xFEDCBA9876543210;

  std::byte bytes[kHeaderSize];
  encode_header(bytes, in);

  MessageHeader out;
  ASSERT_EQ(decode_header(bytes, out, kMaxPayloadSize), DecodeError::kOk);
  EXPECT_EQ(out.magic, kMagic);
  EXPECT_EQ(out.version, kProtocolVersion);
  EXPECT_EQ(out.type, MessageType::kSubscribe);
  EXPECT_EQ(out.topic, 0xDEADBEEFu);
  EXPECT_EQ(out.payload_size, 1234u);
  EXPECT_EQ(out.flags, 0xBEEFu);
  EXPECT_EQ(out.sequence, 0x0123456789ABCDEFu);
  EXPECT_EQ(out.timestamp_ns, 0xFEDCBA9876543210u);
}

TEST(Header, IsEncodedLittleEndianOnTheWire) {
  MessageHeader in;
  in.topic = 0x04030201;
  in.sequence = 0x0807060504030201;
  std::byte bytes[kHeaderSize];
  encode_header(bytes, in);

  EXPECT_EQ(static_cast<uint8_t>(bytes[0]), 0x55);  // magic 0xFB55, low byte first
  EXPECT_EQ(static_cast<uint8_t>(bytes[1]), 0xFB);
  EXPECT_EQ(static_cast<uint8_t>(bytes[4]), 0x01);
  EXPECT_EQ(static_cast<uint8_t>(bytes[7]), 0x04);
  EXPECT_EQ(static_cast<uint8_t>(bytes[16]), 0x01);
  EXPECT_EQ(static_cast<uint8_t>(bytes[23]), 0x08);
}

TEST(Header, RejectsMalformedFields) {
  const auto corrupt = [](size_t offset, uint8_t value) {
    auto frame = make_frame(1, 1, 0);
    frame[offset] = static_cast<std::byte>(value);
    MessageHeader out;
    return decode_header(frame.data(), out, kMaxPayloadSize);
  };

  EXPECT_EQ(corrupt(0, 0x00), DecodeError::kBadMagic);
  EXPECT_EQ(corrupt(1, 0x00), DecodeError::kBadMagic);
  EXPECT_EQ(corrupt(2, 99), DecodeError::kBadVersion);
  EXPECT_EQ(corrupt(3, 0), DecodeError::kBadType);
  EXPECT_EQ(corrupt(3, 200), DecodeError::kBadType);
  EXPECT_EQ(corrupt(14, 1), DecodeError::kReservedNonZero);

  auto frame = make_frame(1, 1, 0);
  store_le32(frame.data() + 8, kMaxPayloadSize + 1);
  MessageHeader out;
  EXPECT_EQ(decode_header(frame.data(), out, kMaxPayloadSize), DecodeError::kPayloadTooLarge);
}

TEST(StreamDecoder, DecodesOneWholeFrame) {
  StreamDecoder decoder;
  Sink sink;
  const auto frame = make_frame(1, 7, 64);
  ASSERT_EQ(decoder.feed(frame.data(), frame.size(), sink.handler()), DecodeError::kOk);
  ASSERT_EQ(sink.frames().size(), 1u);
  EXPECT_EQ(sink.frames()[0].header.sequence, 1u);
  EXPECT_EQ(sink.frames()[0].header.topic, 7u);
  EXPECT_EQ(sink.frames()[0].payload.size(), 64u);
  EXPECT_EQ(decoder.pending(), 0u);
}

TEST(StreamDecoder, DecodesSeveralFramesFromOneChunk) {
  StreamDecoder decoder;
  Sink sink;
  std::vector<std::byte> stream;
  for (uint64_t i = 1; i <= 5; ++i) {
    const auto frame = make_frame(i, 1, 8 * i);
    stream.insert(stream.end(), frame.begin(), frame.end());
  }
  ASSERT_EQ(decoder.feed(stream.data(), stream.size(), sink.handler()), DecodeError::kOk);
  ASSERT_EQ(sink.frames().size(), 5u);
  for (uint64_t i = 0; i < 5; ++i) {
    EXPECT_EQ(sink.frames()[i].header.sequence, i + 1);
    EXPECT_EQ(sink.frames()[i].payload.size(), 8 * (i + 1));
  }
}

// The case the spec calls out: a header split across recv() boundaries.
TEST(StreamDecoder, HandlesHeaderSplitAcrossChunks) {
  StreamDecoder decoder;
  Sink sink;
  const auto frame = make_frame(42, 3, 16);

  ASSERT_EQ(decoder.feed(frame.data(), 3, sink.handler()), DecodeError::kOk);
  EXPECT_TRUE(sink.frames().empty());
  EXPECT_EQ(decoder.pending(), 3u);

  ASSERT_EQ(decoder.feed(frame.data() + 3, 7, sink.handler()), DecodeError::kOk);
  EXPECT_TRUE(sink.frames().empty());
  EXPECT_EQ(decoder.pending(), 10u);

  ASSERT_EQ(decoder.feed(frame.data() + 10, frame.size() - 10, sink.handler()),
            DecodeError::kOk);
  ASSERT_EQ(sink.frames().size(), 1u);
  EXPECT_EQ(sink.frames()[0].header.sequence, 42u);
  EXPECT_EQ(decoder.pending(), 0u);
}

TEST(StreamDecoder, HandlesPayloadSplitAcrossChunks) {
  StreamDecoder decoder;
  Sink sink;
  const auto frame = make_frame(9, 1, 256);

  ASSERT_EQ(decoder.feed(frame.data(), kHeaderSize + 100, sink.handler()), DecodeError::kOk);
  EXPECT_TRUE(sink.frames().empty());
  ASSERT_EQ(decoder.feed(frame.data() + kHeaderSize + 100, 156, sink.handler()),
            DecodeError::kOk);
  ASSERT_EQ(sink.frames().size(), 1u);
  EXPECT_EQ(sink.frames()[0].payload.size(), 256u);
}

/// The strongest fragmentation test there is: every frame boundary lands in a
/// different place relative to the chunk boundaries.
TEST(StreamDecoder, SurvivesByteAtATimeDelivery) {
  StreamDecoder decoder;
  Sink sink;
  std::vector<std::byte> stream;
  constexpr uint64_t kFrames = 40;
  for (uint64_t i = 1; i <= kFrames; ++i) {
    const auto frame = make_frame(i, static_cast<uint32_t>(i % 4), (i * 37) % 200);
    stream.insert(stream.end(), frame.begin(), frame.end());
  }

  for (const std::byte b : stream) {
    ASSERT_EQ(decoder.feed(&b, 1, sink.handler()), DecodeError::kOk);
  }
  ASSERT_EQ(sink.frames().size(), kFrames);
  for (uint64_t i = 0; i < kFrames; ++i) {
    EXPECT_EQ(sink.frames()[i].header.sequence, i + 1);
    EXPECT_EQ(sink.frames()[i].payload.size(), ((i + 1) * 37) % 200);
  }
  EXPECT_EQ(decoder.frames_decoded(), kFrames);
}

TEST(StreamDecoder, SurvivesEveryChunkSize) {
  std::vector<std::byte> stream;
  constexpr uint64_t kFrames = 25;
  for (uint64_t i = 1; i <= kFrames; ++i) {
    const auto frame = make_frame(i, 1, (i * 53) % 300);
    stream.insert(stream.end(), frame.begin(), frame.end());
  }

  for (size_t chunk = 1; chunk <= 137; ++chunk) {
    StreamDecoder decoder;
    Sink sink;
    for (size_t offset = 0; offset < stream.size(); offset += chunk) {
      const size_t size = std::min(chunk, stream.size() - offset);
      ASSERT_EQ(decoder.feed(stream.data() + offset, size, sink.handler()), DecodeError::kOk)
          << "chunk size " << chunk;
    }
    ASSERT_EQ(sink.frames().size(), kFrames) << "chunk size " << chunk;
    for (uint64_t i = 0; i < kFrames; ++i) {
      ASSERT_EQ(sink.frames()[i].header.sequence, i + 1) << "chunk size " << chunk;
      ASSERT_EQ(sink.frames()[i].payload.size(), ((i + 1) * 53) % 300) << "chunk size " << chunk;
    }
  }
}

TEST(StreamDecoder, HandlesChunksLargerThanItsBuffer) {
  // A chunk bigger than the decoder's internal buffer must still work, with a
  // held-over partial frame at the start so the buffered path is exercised.
  StreamDecoder decoder(256);
  Sink sink;
  std::vector<std::byte> stream;
  for (uint64_t i = 1; i <= 200; ++i) {
    const auto frame = make_frame(i, 1, 200);
    stream.insert(stream.end(), frame.begin(), frame.end());
  }
  ASSERT_GT(stream.size(), kHeaderSize + 256);

  ASSERT_EQ(decoder.feed(stream.data(), 10, sink.handler()), DecodeError::kOk);
  ASSERT_EQ(decoder.feed(stream.data() + 10, stream.size() - 10, sink.handler()),
            DecodeError::kOk);
  ASSERT_EQ(sink.frames().size(), 200u);
  EXPECT_EQ(sink.frames()[199].header.sequence, 200u);
}

TEST(StreamDecoder, StopsAtACorruptHeaderAndReportsWhy) {
  StreamDecoder decoder;
  Sink sink;
  auto good = make_frame(1, 1, 8);
  auto bad = make_frame(2, 1, 8);
  bad[0] = std::byte{0x00};

  std::vector<std::byte> stream(good);
  stream.insert(stream.end(), bad.begin(), bad.end());
  EXPECT_EQ(decoder.feed(stream.data(), stream.size(), sink.handler()), DecodeError::kBadMagic);
  // The frame before the corruption is still delivered; nothing after it is.
  EXPECT_EQ(sink.frames().size(), 1u);
}

TEST(StreamDecoder, EnforcesItsOwnPayloadLimit) {
  StreamDecoder decoder(128);
  Sink sink;
  auto frame = make_frame(1, 1, 0);
  store_le32(frame.data() + 8, 129);
  EXPECT_EQ(decoder.feed(frame.data(), frame.size(), sink.handler()),
            DecodeError::kPayloadTooLarge);
  EXPECT_TRUE(sink.frames().empty());
}

TEST(StreamDecoder, AcceptsZeroLengthPayloads) {
  StreamDecoder decoder;
  Sink sink;
  const auto frame = make_frame(1, 5, 0, MessageType::kSubscribe);
  ASSERT_EQ(decoder.feed(frame.data(), frame.size(), sink.handler()), DecodeError::kOk);
  ASSERT_EQ(sink.frames().size(), 1u);
  EXPECT_EQ(sink.frames()[0].header.type, MessageType::kSubscribe);
  EXPECT_TRUE(sink.frames()[0].payload.empty());
}

// --- fuzzing ---------------------------------------------------------------
// The decoder must reject garbage rather than crash, over-read, or try to
// allocate a payload the length field invented. Run this under ASan and UBSan
// via the sanitizer presets; that is where it earns its keep.

TEST(StreamDecoderFuzz, RejectsRandomBytesWithoutCrashing) {
  std::mt19937 rng(20240601);
  for (int iteration = 0; iteration < 2000; ++iteration) {
    const size_t size = rng() % 512;
    std::vector<std::byte> noise(size);
    for (auto& b : noise) b = static_cast<std::byte>(rng() & 0xFF);

    StreamDecoder decoder;
    size_t delivered = 0;
    uint64_t payload_bytes_seen = 0;
    decoder.feed(noise.data(), noise.size(),
                 [&](const MessageHeader&, const std::byte* payload, size_t payload_size) {
                   ++delivered;
                   // Touch every byte so ASan would catch an over-read.
                   for (size_t i = 0; i < payload_size; ++i) {
                     payload_bytes_seen += static_cast<uint8_t>(payload[i]);
                   }
                 });
    // Random noise should essentially never produce a valid header; the point
    // is that whatever happens is safe and bounded.
    EXPECT_LE(delivered, noise.size() / kHeaderSize);
    (void)payload_bytes_seen;
  }
}

TEST(StreamDecoderFuzz, RejectsMutatedValidFrames) {
  std::mt19937 rng(777);
  const auto base = make_frame(1, 1, 64);
  for (int iteration = 0; iteration < 5000; ++iteration) {
    auto frame = base;
    const size_t flips = 1 + rng() % 4;
    for (size_t i = 0; i < flips; ++i) {
      const size_t offset = rng() % frame.size();
      frame[offset] = static_cast<std::byte>(rng() & 0xFF);
    }
    StreamDecoder decoder;
    uint64_t checksum = 0;
    decoder.feed(frame.data(), frame.size(),
                 [&](const MessageHeader& header, const std::byte* payload, size_t size) {
                   EXPECT_LE(size, kMaxPayloadSize);
                   EXPECT_EQ(header.payload_size, size);
                   for (size_t i = 0; i < size; ++i) checksum += static_cast<uint8_t>(payload[i]);
                 });
    (void)checksum;
  }
}

TEST(StreamDecoderFuzz, RejectsTruncatedFramesWithoutDelivering) {
  const auto frame = make_frame(1, 1, 128);
  for (size_t prefix = 0; prefix < frame.size(); ++prefix) {
    StreamDecoder decoder;
    size_t delivered = 0;
    ASSERT_EQ(decoder.feed(frame.data(), prefix,
                           [&](const MessageHeader&, const std::byte*, size_t) { ++delivered; }),
              DecodeError::kOk);
    EXPECT_EQ(delivered, 0u) << "a frame was delivered from only " << prefix << " bytes";
  }
}

// A huge declared length must be rejected on sight, not used to size anything.
TEST(StreamDecoderFuzz, HugeDeclaredLengthIsRejectedImmediately) {
  StreamDecoder decoder;
  auto frame = make_frame(1, 1, 0);
  store_le32(frame.data() + 8, 0xFFFFFFFF);
  const uint64_t allocations_before = heap_allocation_count();
  EXPECT_EQ(decoder.feed(frame.data(), frame.size(),
                         [](const MessageHeader&, const std::byte*, size_t) { FAIL(); }),
            DecodeError::kPayloadTooLarge);
  EXPECT_EQ(heap_allocation_count(), allocations_before);
}

}  // namespace
}  // namespace flashbus
