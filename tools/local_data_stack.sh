#!/usr/bin/env bash
# 先根据说明配置好panda quantflow
set -euo pipefail

# 说明：
# 1. 需要提前在本机安装 Kafka、QuestDB、ClickHouse，并设置好 JAVA 环境。
# 2. 如安装路径不同，请通过环境变量覆盖：
#      KAFKA_HOME        Kafka 根目录，内含 bin/zookeeper-server-start.sh
#      QUESTDB_HOME      QuestDB 根目录，内含可执行的 questdb 脚本
#      CLICKHOUSE_BIN    clickhouse 可执行文件（默认为 PATH 中的 clickhouse）
#      CLICKHOUSE_CONFIG ClickHouse config.xml（默认 /usr/local/etc/clickhouse-server/config.xml）
# 3. 所有数据、日志、PID 会写入 user_data/local-stack 下，不会影响系统安装


##例如
# export KAFKA_HOME=/opt/homebrew/opt/kafka
# export QUESTDB_HOME=$HOME/apps/questdb-7.3
# export CLICKHOUSE_BIN=/opt/homebrew/bin/clickhouse
# export CLICKHOUSE_CONFIG=/opt/homebrew/etc/clickhouse-server/config.xml

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STACK_ROOT="${PROJECT_ROOT}/user_data/local-stack"
LOG_DIR="${STACK_ROOT}/logs"
PID_DIR="${STACK_ROOT}/pids"
DATA_DIR="${STACK_ROOT}/data"

mkdir -p "${LOG_DIR}" "${PID_DIR}" "${DATA_DIR}"

KAFKA_HOME="${KAFKA_HOME:-/usr/local/opt/kafka}"
QUESTDB_HOME="${QUESTDB_HOME:-/usr/local/opt/questdb}"
CLICKHOUSE_BIN="${CLICKHOUSE_BIN:-$(command -v clickhouse || true)}"
CLICKHOUSE_CONFIG="${CLICKHOUSE_CONFIG:-/usr/local/etc/clickhouse-server/config.xml}"

KAFKA_BIN="${KAFKA_HOME}/bin"
QUESTDB_BIN="${QUESTDB_HOME}/questdb"

CONFIG_DIR="${STACK_ROOT}/config"
mkdir -p "${CONFIG_DIR}"

ZK_PID_FILE="${PID_DIR}/zookeeper.pid"
KAFKA_PID_FILE="${PID_DIR}/kafka.pid"
QUESTDB_PID_FILE="${PID_DIR}/questdb.pid"
CLICKHOUSE_PID_FILE="${PID_DIR}/clickhouse.pid"

ensure_binary() {
  if [[ ! -x "$1" ]]; then
    echo "未找到可执行文件：$1，请检查对应服务是否已安装，或设置相关环境变量。" >&2
    exit 1
  fi
}

prepare_kafka_configs() {
  cat > "${CONFIG_DIR}/zookeeper.properties" <<EOF
dataDir=${DATA_DIR}/zookeeper
clientPort=2181
maxClientCnxns=0
EOF

  cat > "${CONFIG_DIR}/server.properties" <<EOF
broker.id=1
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://localhost:9092
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=${DATA_DIR}/kafka-logs
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
EOF
}

start_zookeeper() {
  if [[ -f "${ZK_PID_FILE}" ]] && ps -p "$(cat "${ZK_PID_FILE}")" >/dev/null 2>&1; then
    echo "Zookeeper 已在运行 (PID $(cat "${ZK_PID_FILE}")）。"
    return
  fi
  ensure_binary "${KAFKA_BIN}/zookeeper-server-start.sh"
  mkdir -p "${DATA_DIR}/zookeeper"
  nohup "${KAFKA_BIN}/zookeeper-server-start.sh" "${CONFIG_DIR}/zookeeper.properties" \
    > "${LOG_DIR}/zookeeper.log" 2>&1 &
  echo $! > "${ZK_PID_FILE}"
  echo "Zookeeper 已启动，日志：${LOG_DIR}/zookeeper.log"
}

start_kafka() {
  if [[ -f "${KAFKA_PID_FILE}" ]] && ps -p "$(cat "${KAFKA_PID_FILE}")" >/dev/null 2>&1; then
    echo "Kafka 已在运行 (PID $(cat "${KAFKA_PID_FILE}")）。"
    return
  fi
  ensure_binary "${KAFKA_BIN}/kafka-server-start.sh"
  mkdir -p "${DATA_DIR}/kafka-logs"
  nohup "${KAFKA_BIN}/kafka-server-start.sh" "${CONFIG_DIR}/server.properties" \
    > "${LOG_DIR}/kafka.log" 2>&1 &
  echo $! > "${KAFKA_PID_FILE}"
  echo "Kafka 已启动，日志：${LOG_DIR}/kafka.log"
}

start_questdb() {
  if [[ -f "${QUESTDB_PID_FILE}" ]] && ps -p "$(cat "${QUESTDB_PID_FILE}")" >/dev/null 2>&1; then
    echo "QuestDB 已在运行 (PID $(cat "${QUESTDB_PID_FILE}")）。"
    return
  fi
  ensure_binary "${QUESTDB_BIN}"
  mkdir -p "${DATA_DIR}/questdb"
  nohup "${QUESTDB_BIN}" start -d "${DATA_DIR}/questdb" \
    > "${LOG_DIR}/questdb.log" 2>&1 &
  echo $! > "${QUESTDB_PID_FILE}"
  echo "QuestDB 已启动，日志：${LOG_DIR}/questdb.log"
}

start_clickhouse() {
  if [[ -f "${CLICKHOUSE_PID_FILE}" ]] && ps -p "$(cat "${CLICKHOUSE_PID_FILE}")" >/dev/null 2>&1; then
    echo "ClickHouse 已在运行 (PID $(cat "${CLICKHOUSE_PID_FILE}")）。"
    return
  fi
  ensure_binary "${CLICKHOUSE_BIN:-/usr/local/bin/clickhouse}"
  mkdir -p "${DATA_DIR}/clickhouse"
  nohup "${CLICKHOUSE_BIN}" server --config-file "${CLICKHOUSE_CONFIG}" \
    -- --path "${DATA_DIR}/clickhouse" \
    > "${LOG_DIR}/clickhouse.log" 2>&1 &
  echo $! > "${CLICKHOUSE_PID_FILE}"
  echo "ClickHouse 已启动，日志：${LOG_DIR}/clickhouse.log"
}

stop_service() {
  local name="$1"
  local pid_file="$2"
  if [[ -f "${pid_file}" ]]; then
    local pid
    pid="$(cat "${pid_file}")"
    if ps -p "${pid}" >/dev/null 2>&1; then
      echo "停止 ${name} (PID ${pid})…"
      kill "${pid}" || true
      for _ in {1..10}; do
        if ps -p "${pid}" >/dev/null 2>&1; then
          sleep 1
        else
          break
        fi
      done
      if ps -p "${pid}" >/dev/null 2>&1; then
        echo "${name} 仍在运行，发送 SIGKILL"
        kill -9 "${pid}" || true
      fi
    else
      echo "${name} 未运行，但存在旧 PID 文件。"
    fi
    rm -f "${pid_file}"
  else
    echo "${name} 未运行。"
  fi
}

status_service() {
  local name="$1"
  local pid_file="$2"
  if [[ -f "${pid_file}" ]] && ps -p "$(cat "${pid_file}")" >/dev/null 2>&1; then
    echo "${name}: 运行中 (PID $(cat "${pid_file}"))"
  else
    echo "${name}: 未运行"
  fi
}

case "${1:-}" in
  install-configs)
    prepare_kafka_configs
    echo "Kafka/Zookeeper 配置已写入 ${CONFIG_DIR}"
    ;;
  start)
    prepare_kafka_configs
    start_zookeeper
    start_kafka
    start_questdb
    start_clickhouse
    ;;
  stop)
    stop_service "ClickHouse" "${CLICKHOUSE_PID_FILE}"
    stop_service "QuestDB" "${QUESTDB_PID_FILE}"
    stop_service "Kafka" "${KAFKA_PID_FILE}"
    stop_service "Zookeeper" "${ZK_PID_FILE}"
    ;;
  status)
    status_service "Zookeeper" "${ZK_PID_FILE}"
    status_service "Kafka" "${KAFKA_PID_FILE}"
    status_service "QuestDB" "${QUESTDB_PID_FILE}"
    status_service "ClickHouse" "${CLICKHOUSE_PID_FILE}"
    ;;
  logs)
    tail -n 200 -f "${LOG_DIR}"/*.log
    ;;
  clean-data)
    stop_service "ClickHouse" "${CLICKHOUSE_PID_FILE}"
    stop_service "QuestDB" "${QUESTDB_PID_FILE}"
    stop_service "Kafka" "${KAFKA_PID_FILE}"
    stop_service "Zookeeper" "${ZK_PID_FILE}"
    rm -rf "${DATA_DIR}" "${LOG_DIR}"
    mkdir -p "${LOG_DIR}" "${DATA_DIR}"
    echo "数据与日志已清空。"
    ;;
  *)
    cat <<'EOF'
用法: tools/local_data_stack.sh <command>

命令：
  install-configs  生成/覆盖 Kafka、Zookeeper 的本地配置
  start            启动 Zookeeper、Kafka、QuestDB、ClickHouse
  stop             停止所有服务
  status           查看各服务运行状态
  logs             实时查看所有日志
  clean-data       停止服务并清空 user_data/local-stack 下的数据与日志

启动前请确保：
  - 已安装 Kafka / QuestDB / ClickHouse
  - 如安装路径不同，请通过 KAFKA_HOME、QUESTDB_HOME、CLICKHOUSE_BIN、CLICKHOUSE_CONFIG 覆盖
EOF
    exit 1
    ;;
esac
