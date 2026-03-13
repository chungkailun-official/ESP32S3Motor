#pragma once

#include <cstddef>
#include <cstdint>

namespace AppConfig {

inline constexpr char kDeviceName[] = "ESP32S3-N20";

// Update these pins to match your actual ESP32-S3 Tiny wiring.
inline constexpr int kMotorIn1Pin = 4;
inline constexpr int kMotorIn2Pin = 5;
inline constexpr int kMotorSleepPin = -1;
inline constexpr int kEncoderAPin = 6;
inline constexpr int kEncoderBPin = 7;
inline constexpr bool kInvertEncoderDirection = false;

inline constexpr std::uint32_t kPwmFrequencyHz = 20000;
inline constexpr std::uint32_t kPwmResolutionBits = 8;
inline constexpr std::uint32_t kControlLoopMs = 20;
inline constexpr std::uint32_t kDefaultTelemetryPeriodMs = 100;

// Set this to the actual quadrature counts per revolution that you want
// the reported RPM to represent. For many N20 encoder motors 44 is the
// bare motor-shaft count with x4 decoding, but your gearbox may change
// the output-shaft CPR substantially.
inline constexpr float kDefaultEncoderCountsPerRevolution = 44.0f;

inline constexpr float kDefaultKp = 0.006f;
inline constexpr float kDefaultKi = 0.002f;
inline constexpr float kDefaultKd = 0.0f;
inline constexpr float kIntegralLimit = 200.0f;
inline constexpr float kDefaultMaxCommand = 1.0f;
inline constexpr float kSpeedDeadbandCountsPerSecond = 1.0f;
inline constexpr float kCommandEpsilon = 0.01f;
inline constexpr bool kDefaultBrakeOnStop = true;

inline constexpr std::size_t kMaxCommandLength = 240;
inline constexpr std::size_t kCommandQueueDepth = 8;

}  // namespace AppConfig
