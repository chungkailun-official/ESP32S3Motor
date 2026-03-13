#include "app_config.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>

extern "C" {
#include "cJSON.h"
#include "driver/gpio.h"
#include "driver/ledc.h"
#include "esp_err.h"
#include "esp_log.h"
#include "esp_nimble_hci.h"
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"
#include "freertos/task.h"
#include "host/ble_hs.h"
#include "host/ble_hs_mbuf.h"
#include "nimble/nimble_port.h"
#include "nimble/nimble_port_freertos.h"
#include "nvs_flash.h"
#include "os/os_mbuf.h"
#include "services/gap/ble_svc_gap.h"
#include "services/gatt/ble_svc_gatt.h"
}

namespace {

constexpr char kTag[] = "motor_ble";

constexpr ledc_mode_t kLedcMode = LEDC_LOW_SPEED_MODE;
constexpr ledc_timer_t kLedcTimer = LEDC_TIMER_0;
constexpr ledc_channel_t kLedcChannelIn1 = LEDC_CHANNEL_0;
constexpr ledc_channel_t kLedcChannelIn2 = LEDC_CHANNEL_1;

constexpr int8_t kQuadratureTransitionTable[16] = {
    0, -1, 1, 0,
    1, 0, 0, -1,
    -1, 0, 0, 1,
    0, 1, -1, 0,
};

const ble_uuid128_t kBleUartServiceUuid =
    BLE_UUID128_INIT(0x9e, 0xca, 0xdc, 0x24, 0x0e, 0xe5, 0xa9, 0xe0,
                     0x93, 0xf3, 0xa3, 0xb5, 0x01, 0x00, 0x40, 0x6e);
const ble_uuid128_t kBleUartRxUuid =
    BLE_UUID128_INIT(0x9e, 0xca, 0xdc, 0x24, 0x0e, 0xe5, 0xa9, 0xe0,
                     0x93, 0xf3, 0xa3, 0xb5, 0x02, 0x00, 0x40, 0x6e);
const ble_uuid128_t kBleUartTxUuid =
    BLE_UUID128_INIT(0x9e, 0xca, 0xdc, 0x24, 0x0e, 0xe5, 0xa9, 0xe0,
                     0x93, 0xf3, 0xa3, 0xb5, 0x03, 0x00, 0x40, 0x6e);

enum class ControlMode : std::uint8_t {
  Idle = 0,
  Power = 1,
  Speed = 2,
};

struct ControllerConfig {
  float encoder_counts_per_revolution = AppConfig::kDefaultEncoderCountsPerRevolution;
  float kp = AppConfig::kDefaultKp;
  float ki = AppConfig::kDefaultKi;
  float kd = AppConfig::kDefaultKd;
  float max_command = AppConfig::kDefaultMaxCommand;
  std::uint32_t telemetry_period_ms = AppConfig::kDefaultTelemetryPeriodMs;
  bool brake_on_stop = AppConfig::kDefaultBrakeOnStop;
};

struct ControllerState {
  ControlMode mode = ControlMode::Idle;
  float target_power = 0.0f;
  float target_counts_per_second = 0.0f;
  float applied_command = 0.0f;
  std::uint32_t pwm_duty = 0;
  bool brake_applied = false;

  std::int64_t encoder_count = 0;
  std::int32_t delta_count = 0;
  float measured_counts_per_second = 0.0f;
  float measured_rpm = 0.0f;

  float pid_integral = 0.0f;
  float previous_error = 0.0f;

  std::int64_t previous_encoder_count = 0;
  std::int64_t last_control_time_us = 0;
  std::int64_t last_telemetry_time_us = 0;
};

struct CommandMessage {
  char text[AppConfig::kMaxCommandLength];
};

QueueHandle_t g_command_queue = nullptr;
ControllerConfig g_config;
ControllerState g_state;

portMUX_TYPE g_encoder_mux = portMUX_INITIALIZER_UNLOCKED;
volatile std::int64_t g_encoder_count = 0;
volatile std::uint8_t g_encoder_previous_state = 0;

std::string g_ble_rx_line_buffer;
std::uint16_t g_uart_tx_value_handle = 0;
std::uint8_t g_own_addr_type = 0;
volatile std::uint16_t g_connection_handle = BLE_HS_CONN_HANDLE_NONE;
volatile bool g_ble_connected = false;
volatile bool g_notify_enabled = false;
volatile bool g_send_status_snapshot = false;

struct ble_gatt_chr_def g_uart_characteristics[3];
struct ble_gatt_svc_def g_gatt_services[2];

void check_ble_rc(const char* step, int rc) {
  if (rc == 0) {
    return;
  }

  ESP_LOGE(kTag, "%s failed: rc=%d", step, rc);
  std::abort();
}

const char* control_mode_name(ControlMode mode) {
  switch (mode) {
    case ControlMode::Idle:
      return "idle";
    case ControlMode::Power:
      return "power";
    case ControlMode::Speed:
      return "speed";
    default:
      return "unknown";
  }
}

std::string trim_copy(const std::string& value) {
  const std::size_t first = value.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) {
    return {};
  }

  const std::size_t last = value.find_last_not_of(" \t\r\n");
  return value.substr(first, last - first + 1);
}

std::uint32_t pwm_full_scale() {
  return (1u << AppConfig::kPwmResolutionBits) - 1u;
}

float rpm_to_counts_per_second(float rpm) {
  if (g_config.encoder_counts_per_revolution <= 0.0f) {
    return 0.0f;
  }

  return rpm * g_config.encoder_counts_per_revolution / 60.0f;
}

float counts_per_second_to_rpm(float counts_per_second) {
  if (g_config.encoder_counts_per_revolution <= 0.0f) {
    return 0.0f;
  }

  return counts_per_second * 60.0f / g_config.encoder_counts_per_revolution;
}

void set_ledc_duty(ledc_channel_t channel, std::uint32_t duty) {
  ESP_ERROR_CHECK(ledc_set_duty(kLedcMode, channel, duty));
  ESP_ERROR_CHECK(ledc_update_duty(kLedcMode, channel));
}

void reset_pid_state() {
  g_state.pid_integral = 0.0f;
  g_state.previous_error = 0.0f;
}

void apply_driver_command(float normalized_command) {
  const float limited = std::clamp(normalized_command, -g_config.max_command, g_config.max_command);
  const float magnitude = std::fabs(limited);

  if (magnitude <= AppConfig::kCommandEpsilon) {
    if (g_config.brake_on_stop) {
      const std::uint32_t full_scale = pwm_full_scale();
      set_ledc_duty(kLedcChannelIn1, full_scale);
      set_ledc_duty(kLedcChannelIn2, full_scale);
      g_state.brake_applied = true;
      g_state.pwm_duty = full_scale;
    } else {
      set_ledc_duty(kLedcChannelIn1, 0);
      set_ledc_duty(kLedcChannelIn2, 0);
      g_state.brake_applied = false;
      g_state.pwm_duty = 0;
    }

    g_state.applied_command = 0.0f;
    return;
  }

  const std::uint32_t duty = static_cast<std::uint32_t>(
      std::lround(std::clamp(magnitude, 0.0f, 1.0f) * pwm_full_scale()));

  if (limited > 0.0f) {
    set_ledc_duty(kLedcChannelIn1, duty);
    set_ledc_duty(kLedcChannelIn2, 0);
  } else {
    set_ledc_duty(kLedcChannelIn1, 0);
    set_ledc_duty(kLedcChannelIn2, duty);
  }

  g_state.applied_command = limited;
  g_state.pwm_duty = duty;
  g_state.brake_applied = false;
}

std::uint8_t read_encoder_state() {
  return static_cast<std::uint8_t>(
      (gpio_get_level(static_cast<gpio_num_t>(AppConfig::kEncoderAPin)) << 1) |
      gpio_get_level(static_cast<gpio_num_t>(AppConfig::kEncoderBPin)));
}

void IRAM_ATTR encoder_isr_handler(void* /*arg*/) {
  const std::uint8_t current_state = static_cast<std::uint8_t>(
      (gpio_get_level(static_cast<gpio_num_t>(AppConfig::kEncoderAPin)) << 1) |
      gpio_get_level(static_cast<gpio_num_t>(AppConfig::kEncoderBPin)));

  portENTER_CRITICAL_ISR(&g_encoder_mux);
  const std::uint8_t index = static_cast<std::uint8_t>((g_encoder_previous_state << 2) | current_state);
  int delta = kQuadratureTransitionTable[index];
  if (AppConfig::kInvertEncoderDirection) {
    delta = -delta;
  }

  g_encoder_count += delta;
  g_encoder_previous_state = current_state;
  portEXIT_CRITICAL_ISR(&g_encoder_mux);
}

std::int64_t read_encoder_count() {
  portENTER_CRITICAL(&g_encoder_mux);
  const std::int64_t value = g_encoder_count;
  portEXIT_CRITICAL(&g_encoder_mux);
  return value;
}

void write_encoder_count(std::int64_t value) {
  portENTER_CRITICAL(&g_encoder_mux);
  g_encoder_count = value;
  g_encoder_previous_state = read_encoder_state();
  portEXIT_CRITICAL(&g_encoder_mux);
}

void enqueue_command_line(const std::string& line) {
  if (g_command_queue == nullptr) {
    return;
  }

  const std::string trimmed = trim_copy(line);
  if (trimmed.empty()) {
    return;
  }

  CommandMessage message{};
  const std::size_t bytes_to_copy =
      std::min(trimmed.size(), sizeof(message.text) - 1u);
  std::memcpy(message.text, trimmed.data(), bytes_to_copy);
  message.text[bytes_to_copy] = '\0';

  if (xQueueSend(g_command_queue, &message, 0) != pdTRUE) {
    ESP_LOGW(kTag, "Command queue full, dropping: %s", message.text);
  }
}

void handle_incoming_ble_bytes(const std::uint8_t* data, std::size_t length) {
  for (std::size_t index = 0; index < length; ++index) {
    const char ch = static_cast<char>(data[index]);
    if (ch == '\r') {
      continue;
    }

    if (ch == '\n') {
      enqueue_command_line(g_ble_rx_line_buffer);
      g_ble_rx_line_buffer.clear();
      continue;
    }

    if (g_ble_rx_line_buffer.size() + 1u >= AppConfig::kMaxCommandLength) {
      ESP_LOGW(kTag, "Incoming BLE command too long, buffer cleared");
      g_ble_rx_line_buffer.clear();
      continue;
    }

    g_ble_rx_line_buffer.push_back(ch);
  }
}

void send_ble_text(const std::string& text) {
  ESP_LOGI(kTag, "TX: %s", text.c_str());

  if (!g_ble_connected || !g_notify_enabled ||
      g_connection_handle == BLE_HS_CONN_HANDLE_NONE ||
      g_uart_tx_value_handle == 0) {
    return;
  }

  std::string payload = text;
  payload.push_back('\n');

  os_mbuf* packet = ble_hs_mbuf_from_flat(payload.data(), payload.size());
  if (packet == nullptr) {
    ESP_LOGE(kTag, "Failed to allocate BLE notification payload");
    return;
  }

  const int rc = ble_gatts_notify_custom(g_connection_handle, g_uart_tx_value_handle, packet);
  if (rc != 0) {
    ESP_LOGW(kTag, "BLE notify failed: rc=%d", rc);
    os_mbuf_free_chain(packet);
  }
}

void send_json(cJSON* root) {
  if (root == nullptr) {
    return;
  }

  char* encoded = cJSON_PrintUnformatted(root);
  if (encoded == nullptr) {
    ESP_LOGE(kTag, "Failed to encode JSON");
    cJSON_Delete(root);
    return;
  }

  send_ble_text(encoded);
  cJSON_free(encoded);
  cJSON_Delete(root);
}

void send_error_response(const char* code, const char* detail) {
  cJSON* root = cJSON_CreateObject();
  cJSON_AddStringToObject(root, "type", "error");
  cJSON_AddStringToObject(root, "code", code);
  cJSON_AddStringToObject(root, "detail", detail);
  send_json(root);
}

void add_runtime_fields(cJSON* root) {
  cJSON_AddStringToObject(root, "mode", control_mode_name(g_state.mode));
  cJSON_AddNumberToObject(root, "encoder_count", static_cast<double>(g_state.encoder_count));
  cJSON_AddNumberToObject(root, "delta_count", static_cast<double>(g_state.delta_count));
  cJSON_AddNumberToObject(root, "speed_cps", g_state.measured_counts_per_second);
  cJSON_AddNumberToObject(root, "speed_rpm", g_state.measured_rpm);
  cJSON_AddNumberToObject(root, "target_cps", g_state.target_counts_per_second);
  cJSON_AddNumberToObject(root, "target_power", g_state.target_power);
  cJSON_AddNumberToObject(root, "applied_output", g_state.applied_command);
  cJSON_AddNumberToObject(root, "pwm_duty", static_cast<double>(g_state.pwm_duty));
  cJSON_AddBoolToObject(root, "brake", g_state.brake_applied);
  cJSON_AddBoolToObject(root, "connected", g_ble_connected);
  cJSON_AddBoolToObject(root, "notify_enabled", g_notify_enabled);
  cJSON_AddNumberToObject(root, "uptime_ms", static_cast<double>(esp_timer_get_time() / 1000));
}

void add_config_fields(cJSON* root) {
  cJSON_AddNumberToObject(root, "encoder_cpr", g_config.encoder_counts_per_revolution);
  cJSON_AddNumberToObject(root, "kp", g_config.kp);
  cJSON_AddNumberToObject(root, "ki", g_config.ki);
  cJSON_AddNumberToObject(root, "kd", g_config.kd);
  cJSON_AddNumberToObject(root, "max_output", g_config.max_command);
  cJSON_AddNumberToObject(root, "telemetry_ms", static_cast<double>(g_config.telemetry_period_ms));
  cJSON_AddBoolToObject(root, "brake_on_stop", g_config.brake_on_stop);
}

void send_status_snapshot(const char* type, bool include_config) {
  cJSON* root = cJSON_CreateObject();
  cJSON_AddStringToObject(root, "type", type);
  add_runtime_fields(root);
  if (include_config) {
    add_config_fields(root);
  }
  send_json(root);
}

bool get_number_field(cJSON* root, const char* name, double& out_value) {
  cJSON* item = cJSON_GetObjectItemCaseSensitive(root, name);
  if (!cJSON_IsNumber(item)) {
    return false;
  }

  out_value = item->valuedouble;
  return true;
}

bool get_bool_field(cJSON* root, const char* name, bool& out_value) {
  cJSON* item = cJSON_GetObjectItemCaseSensitive(root, name);
  if (!cJSON_IsBool(item)) {
    return false;
  }

  out_value = cJSON_IsTrue(item);
  return true;
}

const char* get_string_field(cJSON* root, const char* name) {
  cJSON* item = cJSON_GetObjectItemCaseSensitive(root, name);
  if (!cJSON_IsString(item) || item->valuestring == nullptr) {
    return nullptr;
  }

  return item->valuestring;
}

void enter_idle_mode() {
  g_state.mode = ControlMode::Idle;
  g_state.target_power = 0.0f;
  g_state.target_counts_per_second = 0.0f;
  reset_pid_state();
  apply_driver_command(0.0f);
}

void handle_ping_command() {
  cJSON* root = cJSON_CreateObject();
  cJSON_AddStringToObject(root, "type", "ack");
  cJSON_AddStringToObject(root, "cmd", "ping");
  cJSON_AddStringToObject(root, "device", AppConfig::kDeviceName);
  cJSON_AddStringToObject(root, "transport", "ble_uart");
  send_json(root);
}

void handle_help_command() {
  cJSON* root = cJSON_CreateObject();
  cJSON_AddStringToObject(root, "type", "help");

  cJSON* commands = cJSON_AddArrayToObject(root, "commands");
  cJSON_AddItemToArray(commands, cJSON_CreateString("ping"));
  cJSON_AddItemToArray(commands, cJSON_CreateString("set_output"));
  cJSON_AddItemToArray(commands, cJSON_CreateString("stop"));
  cJSON_AddItemToArray(commands, cJSON_CreateString("zero_encoder"));
  cJSON_AddItemToArray(commands, cJSON_CreateString("set_pid"));
  cJSON_AddItemToArray(commands, cJSON_CreateString("set_config"));
  cJSON_AddItemToArray(commands, cJSON_CreateString("get_status"));

  cJSON* modes = cJSON_AddArrayToObject(root, "set_output_modes");
  cJSON_AddItemToArray(modes, cJSON_CreateString("power"));
  cJSON_AddItemToArray(modes, cJSON_CreateString("speed_cps"));
  cJSON_AddItemToArray(modes, cJSON_CreateString("speed_rpm"));

  send_json(root);
}

void handle_stop_command(cJSON* root) {
  bool brake = g_config.brake_on_stop;
  get_bool_field(root, "brake", brake);
  g_config.brake_on_stop = brake;

  enter_idle_mode();

  cJSON* response = cJSON_CreateObject();
  cJSON_AddStringToObject(response, "type", "ack");
  cJSON_AddStringToObject(response, "cmd", "stop");
  cJSON_AddBoolToObject(response, "brake_on_stop", g_config.brake_on_stop);
  add_runtime_fields(response);
  send_json(response);
}

void handle_zero_encoder_command() {
  write_encoder_count(0);
  g_state.encoder_count = 0;
  g_state.previous_encoder_count = 0;
  g_state.delta_count = 0;
  g_state.measured_counts_per_second = 0.0f;
  g_state.measured_rpm = 0.0f;

  cJSON* response = cJSON_CreateObject();
  cJSON_AddStringToObject(response, "type", "ack");
  cJSON_AddStringToObject(response, "cmd", "zero_encoder");
  cJSON_AddNumberToObject(response, "encoder_count", 0);
  send_json(response);
}

void handle_set_pid_command(cJSON* root) {
  double value = 0.0;
  bool changed = false;

  if (get_number_field(root, "kp", value)) {
    g_config.kp = static_cast<float>(std::max(0.0, value));
    changed = true;
  }
  if (get_number_field(root, "ki", value)) {
    g_config.ki = static_cast<float>(std::max(0.0, value));
    changed = true;
  }
  if (get_number_field(root, "kd", value)) {
    g_config.kd = static_cast<float>(std::max(0.0, value));
    changed = true;
  }

  if (!changed) {
    send_error_response("missing_parameter", "set_pid requires kp, ki, or kd");
    return;
  }

  reset_pid_state();

  cJSON* response = cJSON_CreateObject();
  cJSON_AddStringToObject(response, "type", "ack");
  cJSON_AddStringToObject(response, "cmd", "set_pid");
  add_config_fields(response);
  send_json(response);
}

void handle_set_config_command(cJSON* root) {
  double number_value = 0.0;
  bool bool_value = false;
  bool changed = false;

  if (get_number_field(root, "telemetry_ms", number_value)) {
    const double clamped = std::clamp(number_value, 0.0, 5000.0);
    g_config.telemetry_period_ms = static_cast<std::uint32_t>(clamped);
    changed = true;
  }

  if (get_number_field(root, "encoder_cpr", number_value)) {
    if (number_value <= 0.0) {
      send_error_response("bad_value", "encoder_cpr must be greater than zero");
      return;
    }

    g_config.encoder_counts_per_revolution = static_cast<float>(number_value);
    changed = true;
  }

  if (get_number_field(root, "max_output", number_value)) {
    g_config.max_command = static_cast<float>(std::clamp(number_value, 0.0, 1.0));
    changed = true;
  }

  if (get_bool_field(root, "brake_on_stop", bool_value)) {
    g_config.brake_on_stop = bool_value;
    changed = true;
  }

  if (!changed) {
    send_error_response("missing_parameter", "set_config requires telemetry_ms, encoder_cpr, max_output, or brake_on_stop");
    return;
  }

  cJSON* response = cJSON_CreateObject();
  cJSON_AddStringToObject(response, "type", "ack");
  cJSON_AddStringToObject(response, "cmd", "set_config");
  add_config_fields(response);
  send_json(response);
}

void handle_set_output_command(cJSON* root) {
  const char* mode = get_string_field(root, "mode");
  if (mode == nullptr) {
    send_error_response("missing_parameter", "set_output requires mode");
    return;
  }

  bool brake = g_config.brake_on_stop;
  if (get_bool_field(root, "brake", brake)) {
    g_config.brake_on_stop = brake;
  }

  double value = 0.0;
  cJSON* response = cJSON_CreateObject();
  cJSON_AddStringToObject(response, "type", "ack");
  cJSON_AddStringToObject(response, "cmd", "set_output");
  cJSON_AddStringToObject(response, "mode", mode);

  if (std::strcmp(mode, "power") == 0) {
    if (!get_number_field(root, "value", value) && !get_number_field(root, "power", value)) {
      cJSON_Delete(response);
      send_error_response("missing_parameter", "power mode requires value");
      return;
    }

    g_state.mode = ControlMode::Power;
    g_state.target_power = static_cast<float>(std::clamp(value, -1.0, 1.0));
    g_state.target_counts_per_second = 0.0f;
    reset_pid_state();
    apply_driver_command(g_state.target_power);

    cJSON_AddNumberToObject(response, "target_power", g_state.target_power);
    send_json(response);
    return;
  }

  if (std::strcmp(mode, "speed_cps") == 0 || std::strcmp(mode, "speed") == 0) {
    if (!get_number_field(root, "value", value) && !get_number_field(root, "speed_cps", value)) {
      cJSON_Delete(response);
      send_error_response("missing_parameter", "speed_cps mode requires value");
      return;
    }

    g_state.mode = ControlMode::Speed;
    g_state.target_power = 0.0f;
    g_state.target_counts_per_second = static_cast<float>(value);
    reset_pid_state();

    cJSON_AddNumberToObject(response, "target_cps", g_state.target_counts_per_second);
    send_json(response);
    return;
  }

  if (std::strcmp(mode, "speed_rpm") == 0) {
    if (!get_number_field(root, "value", value) && !get_number_field(root, "rpm", value)) {
      cJSON_Delete(response);
      send_error_response("missing_parameter", "speed_rpm mode requires value");
      return;
    }

    g_state.mode = ControlMode::Speed;
    g_state.target_power = 0.0f;
    g_state.target_counts_per_second = rpm_to_counts_per_second(static_cast<float>(value));
    reset_pid_state();

    cJSON_AddNumberToObject(response, "target_rpm", value);
    cJSON_AddNumberToObject(response, "target_cps", g_state.target_counts_per_second);
    send_json(response);
    return;
  }

  cJSON_Delete(response);
  send_error_response("bad_parameter", "Unsupported set_output mode");
}

void process_command_line(const char* command_text) {
  ESP_LOGI(kTag, "RX: %s", command_text);

  cJSON* root = cJSON_Parse(command_text);
  if (root == nullptr) {
    send_error_response("json_parse_error", "Could not parse JSON command");
    return;
  }

  const char* cmd = get_string_field(root, "cmd");
  if (cmd == nullptr) {
    cJSON_Delete(root);
    send_error_response("missing_parameter", "Command must contain a cmd string");
    return;
  }

  if (std::strcmp(cmd, "ping") == 0) {
    handle_ping_command();
  } else if (std::strcmp(cmd, "help") == 0) {
    handle_help_command();
  } else if (std::strcmp(cmd, "set_output") == 0) {
    handle_set_output_command(root);
  } else if (std::strcmp(cmd, "stop") == 0) {
    handle_stop_command(root);
  } else if (std::strcmp(cmd, "zero_encoder") == 0) {
    handle_zero_encoder_command();
  } else if (std::strcmp(cmd, "set_pid") == 0) {
    handle_set_pid_command(root);
  } else if (std::strcmp(cmd, "set_config") == 0) {
    handle_set_config_command(root);
  } else if (std::strcmp(cmd, "get_status") == 0) {
    send_status_snapshot("status", true);
  } else {
    send_error_response("unknown_command", "Unsupported cmd value");
  }

  cJSON_Delete(root);
}

void update_control_loop() {
  const std::int64_t now_us = esp_timer_get_time();
  if (g_state.last_control_time_us == 0) {
    g_state.last_control_time_us = now_us;
    g_state.previous_encoder_count = read_encoder_count();
    return;
  }

  float dt = static_cast<float>(now_us - g_state.last_control_time_us) / 1'000'000.0f;
  if (dt <= 0.0f) {
    dt = AppConfig::kControlLoopMs / 1000.0f;
  }

  g_state.last_control_time_us = now_us;

  const std::int64_t current_encoder_count = read_encoder_count();
  const std::int64_t delta = current_encoder_count - g_state.previous_encoder_count;
  g_state.previous_encoder_count = current_encoder_count;
  g_state.encoder_count = current_encoder_count;
  g_state.delta_count = static_cast<std::int32_t>(delta);
  g_state.measured_counts_per_second = static_cast<float>(delta) / dt;
  g_state.measured_rpm = counts_per_second_to_rpm(g_state.measured_counts_per_second);

  float command = 0.0f;
  switch (g_state.mode) {
    case ControlMode::Idle:
      reset_pid_state();
      command = 0.0f;
      break;

    case ControlMode::Power:
      reset_pid_state();
      command = g_state.target_power;
      break;

    case ControlMode::Speed: {
      if (std::fabs(g_state.target_counts_per_second) <= AppConfig::kSpeedDeadbandCountsPerSecond) {
        reset_pid_state();
        command = 0.0f;
        break;
      }

      const float error = g_state.target_counts_per_second - g_state.measured_counts_per_second;
      g_state.pid_integral = std::clamp(
          g_state.pid_integral + error * dt,
          -AppConfig::kIntegralLimit,
          AppConfig::kIntegralLimit);
      const float derivative = (error - g_state.previous_error) / dt;
      g_state.previous_error = error;

      command = (g_config.kp * error) +
                (g_config.ki * g_state.pid_integral) +
                (g_config.kd * derivative);
      break;
    }
  }

  apply_driver_command(command);
}

void maybe_send_periodic_telemetry() {
  if (g_config.telemetry_period_ms == 0 || !g_ble_connected || !g_notify_enabled) {
    return;
  }

  const std::int64_t now_us = esp_timer_get_time();
  const std::int64_t period_us =
      static_cast<std::int64_t>(g_config.telemetry_period_ms) * 1000;

  if (g_state.last_telemetry_time_us == 0 ||
      now_us - g_state.last_telemetry_time_us >= period_us) {
    g_state.last_telemetry_time_us = now_us;
    send_status_snapshot("telemetry", false);
  }
}

void configure_gatt_database() {
  std::memset(g_uart_characteristics, 0, sizeof(g_uart_characteristics));
  std::memset(g_gatt_services, 0, sizeof(g_gatt_services));

  g_uart_characteristics[0].uuid = &kBleUartRxUuid.u;
  g_uart_characteristics[0].access_cb = [](std::uint16_t /*conn_handle*/,
                                           std::uint16_t /*attr_handle*/,
                                           struct ble_gatt_access_ctxt* ctxt,
                                           void* /*arg*/) -> int {
    if (ctxt->op != BLE_GATT_ACCESS_OP_WRITE_CHR) {
      return BLE_ATT_ERR_UNLIKELY;
    }

    const std::uint16_t packet_length = OS_MBUF_PKTLEN(ctxt->om);
    std::string payload(packet_length, '\0');
    const int rc = ble_hs_mbuf_to_flat(ctxt->om, payload.data(), payload.size(), nullptr);
    if (rc != 0) {
      ESP_LOGW(kTag, "Failed to flatten RX payload: rc=%d", rc);
      return BLE_ATT_ERR_UNLIKELY;
    }

    handle_incoming_ble_bytes(
        reinterpret_cast<const std::uint8_t*>(payload.data()),
        payload.size());
    return 0;
  };
  g_uart_characteristics[0].flags = BLE_GATT_CHR_F_WRITE | BLE_GATT_CHR_F_WRITE_NO_RSP;

  g_uart_characteristics[1].uuid = &kBleUartTxUuid.u;
  g_uart_characteristics[1].val_handle = &g_uart_tx_value_handle;
  g_uart_characteristics[1].flags = BLE_GATT_CHR_F_NOTIFY;

  g_gatt_services[0].type = BLE_GATT_SVC_TYPE_PRIMARY;
  g_gatt_services[0].uuid = &kBleUartServiceUuid.u;
  g_gatt_services[0].characteristics = g_uart_characteristics;
}

void start_advertising();

int gap_event_callback(struct ble_gap_event* event, void* /*arg*/) {
  switch (event->type) {
    case BLE_GAP_EVENT_CONNECT:
      if (event->connect.status == 0) {
        g_ble_connected = true;
        g_notify_enabled = false;
        g_connection_handle = event->connect.conn_handle;
        ESP_LOGI(kTag, "BLE connected, handle=%u", event->connect.conn_handle);
      } else {
        ESP_LOGW(kTag, "BLE connect failed, status=%d", event->connect.status);
        start_advertising();
      }
      return 0;

    case BLE_GAP_EVENT_DISCONNECT:
      ESP_LOGI(kTag, "BLE disconnected, reason=%d", event->disconnect.reason);
      g_ble_connected = false;
      g_notify_enabled = false;
      g_connection_handle = BLE_HS_CONN_HANDLE_NONE;
      start_advertising();
      return 0;

    case BLE_GAP_EVENT_SUBSCRIBE:
      if (event->subscribe.attr_handle == g_uart_tx_value_handle) {
        g_notify_enabled = event->subscribe.cur_notify != 0;
        g_send_status_snapshot = g_notify_enabled;
        ESP_LOGI(kTag, "Notify state changed: %s", g_notify_enabled ? "enabled" : "disabled");
      }
      return 0;

    case BLE_GAP_EVENT_ADV_COMPLETE:
      ESP_LOGI(kTag, "Advertising ended, restarting");
      start_advertising();
      return 0;

    case BLE_GAP_EVENT_MTU:
      ESP_LOGI(kTag, "MTU updated: mtu=%u", event->mtu.value);
      return 0;

    default:
      return 0;
  }
}

void start_advertising() {
  struct ble_hs_adv_fields fields{};
  fields.flags = BLE_HS_ADV_F_DISC_GEN | BLE_HS_ADV_F_BREDR_UNSUP;
  fields.name = reinterpret_cast<std::uint8_t*>(const_cast<char*>(AppConfig::kDeviceName));
  fields.name_len = static_cast<std::uint8_t>(std::strlen(AppConfig::kDeviceName));
  fields.name_is_complete = 1;
  fields.uuids128 = const_cast<ble_uuid128_t*>(&kBleUartServiceUuid);
  fields.num_uuids128 = 1;
  fields.uuids128_is_complete = 1;

  check_ble_rc("ble_gap_adv_set_fields", ble_gap_adv_set_fields(&fields));

  struct ble_gap_adv_params advertising_params{};
  advertising_params.conn_mode = BLE_GAP_CONN_MODE_UND;
  advertising_params.disc_mode = BLE_GAP_DISC_MODE_GEN;

  check_ble_rc(
      "ble_gap_adv_start",
      ble_gap_adv_start(g_own_addr_type, nullptr, BLE_HS_FOREVER,
                        &advertising_params, gap_event_callback, nullptr));
}

void on_ble_stack_reset(int reason) {
  ESP_LOGE(kTag, "BLE host reset, reason=%d", reason);
}

void on_ble_stack_sync() {
  check_ble_rc("ble_hs_id_infer_auto", ble_hs_id_infer_auto(0, &g_own_addr_type));
  start_advertising();
}

void nimble_host_task(void* /*param*/) {
  nimble_port_run();
  nimble_port_freertos_deinit();
}

void initialize_ble() {
  ESP_ERROR_CHECK(esp_nimble_hci_and_controller_init());
  nimble_port_init();

  ble_hs_cfg.reset_cb = on_ble_stack_reset;
  ble_hs_cfg.sync_cb = on_ble_stack_sync;

  ble_svc_gap_init();
  ble_svc_gatt_init();
  check_ble_rc(
      "ble_svc_gap_device_name_set",
      ble_svc_gap_device_name_set(AppConfig::kDeviceName));

  configure_gatt_database();
  check_ble_rc("ble_gatts_count_cfg", ble_gatts_count_cfg(g_gatt_services));
  check_ble_rc("ble_gatts_add_svcs", ble_gatts_add_svcs(g_gatt_services));

  nimble_port_freertos_init(nimble_host_task);
}

void initialize_motor_driver() {
  ledc_timer_config_t timer_config{};
  timer_config.speed_mode = kLedcMode;
  timer_config.duty_resolution = static_cast<ledc_timer_bit_t>(AppConfig::kPwmResolutionBits);
  timer_config.timer_num = kLedcTimer;
  timer_config.freq_hz = AppConfig::kPwmFrequencyHz;
  timer_config.clk_cfg = LEDC_AUTO_CLK;
  ESP_ERROR_CHECK(ledc_timer_config(&timer_config));

  ledc_channel_config_t in1_config{};
  in1_config.gpio_num = AppConfig::kMotorIn1Pin;
  in1_config.speed_mode = kLedcMode;
  in1_config.channel = kLedcChannelIn1;
  in1_config.intr_type = LEDC_INTR_DISABLE;
  in1_config.timer_sel = kLedcTimer;
  in1_config.duty = 0;
  in1_config.hpoint = 0;
  ESP_ERROR_CHECK(ledc_channel_config(&in1_config));

  ledc_channel_config_t in2_config{};
  in2_config.gpio_num = AppConfig::kMotorIn2Pin;
  in2_config.speed_mode = kLedcMode;
  in2_config.channel = kLedcChannelIn2;
  in2_config.intr_type = LEDC_INTR_DISABLE;
  in2_config.timer_sel = kLedcTimer;
  in2_config.duty = 0;
  in2_config.hpoint = 0;
  ESP_ERROR_CHECK(ledc_channel_config(&in2_config));

  if (AppConfig::kMotorSleepPin >= 0) {
    gpio_config_t sleep_config{};
    sleep_config.pin_bit_mask = 1ULL << AppConfig::kMotorSleepPin;
    sleep_config.mode = GPIO_MODE_OUTPUT;
    sleep_config.pull_down_en = GPIO_PULLDOWN_DISABLE;
    sleep_config.pull_up_en = GPIO_PULLUP_DISABLE;
    sleep_config.intr_type = GPIO_INTR_DISABLE;
    ESP_ERROR_CHECK(gpio_config(&sleep_config));
    ESP_ERROR_CHECK(gpio_set_level(static_cast<gpio_num_t>(AppConfig::kMotorSleepPin), 1));
  }

  enter_idle_mode();
}

void initialize_encoder() {
  gpio_config_t encoder_config{};
  encoder_config.pin_bit_mask =
      (1ULL << AppConfig::kEncoderAPin) |
      (1ULL << AppConfig::kEncoderBPin);
  encoder_config.mode = GPIO_MODE_INPUT;
  encoder_config.pull_down_en = GPIO_PULLDOWN_DISABLE;
  encoder_config.pull_up_en = GPIO_PULLUP_ENABLE;
  encoder_config.intr_type = GPIO_INTR_ANYEDGE;
  ESP_ERROR_CHECK(gpio_config(&encoder_config));

  g_encoder_previous_state = read_encoder_state();
  ESP_ERROR_CHECK(gpio_install_isr_service(ESP_INTR_FLAG_IRAM));
  ESP_ERROR_CHECK(gpio_isr_handler_add(
      static_cast<gpio_num_t>(AppConfig::kEncoderAPin),
      encoder_isr_handler,
      nullptr));
  ESP_ERROR_CHECK(gpio_isr_handler_add(
      static_cast<gpio_num_t>(AppConfig::kEncoderBPin),
      encoder_isr_handler,
      nullptr));
}

void control_task(void* /*param*/) {
  g_state.last_control_time_us = esp_timer_get_time();
  g_state.last_telemetry_time_us = g_state.last_control_time_us;
  g_state.previous_encoder_count = read_encoder_count();

  TickType_t last_wake = xTaskGetTickCount();

  while (true) {
    CommandMessage message{};
    while (xQueueReceive(g_command_queue, &message, 0) == pdTRUE) {
      process_command_line(message.text);
    }

    update_control_loop();

    if (g_send_status_snapshot) {
      g_send_status_snapshot = false;
      send_status_snapshot("status", true);
    }

    maybe_send_periodic_telemetry();
    vTaskDelayUntil(&last_wake, pdMS_TO_TICKS(AppConfig::kControlLoopMs));
  }
}

}  // namespace

extern "C" void app_main(void) {
  esp_err_t nvs_result = nvs_flash_init();
  if (nvs_result == ESP_ERR_NVS_NO_FREE_PAGES ||
      nvs_result == ESP_ERR_NVS_NEW_VERSION_FOUND) {
    ESP_ERROR_CHECK(nvs_flash_erase());
    nvs_result = nvs_flash_init();
  }
  ESP_ERROR_CHECK(nvs_result);

  ESP_LOGI(kTag, "Starting %s", AppConfig::kDeviceName);

  g_command_queue = xQueueCreate(
      static_cast<UBaseType_t>(AppConfig::kCommandQueueDepth),
      sizeof(CommandMessage));
  if (g_command_queue == nullptr) {
    ESP_LOGE(kTag, "Failed to create command queue");
    std::abort();
  }

  initialize_motor_driver();
  initialize_encoder();
  initialize_ble();

  BaseType_t task_ok = xTaskCreate(
      control_task,
      "control_task",
      8192,
      nullptr,
      5,
      nullptr);
  if (task_ok != pdPASS) {
    ESP_LOGE(kTag, "Failed to create control task");
    std::abort();
  }
}
