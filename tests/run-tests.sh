#!/bin/bash
#
# Integration tests for TrigEmit-Kafka.
#
#   tests/run-tests.sh [broker]
#
# Exercises the module against a real MariaDB server and a real Kafka broker.
# There is nothing to mock: the whole point of this module is what happens
# between the two, so a test that stubbed either would be testing nothing.
#
# Expects:
#   - kafka.so already installed in the server's plugin directory
#   - the functions registered (sql/install.sql)
#   - a reachable broker, default localhost:9092
#   - the mariadb client usable without a password (root, socket auth)
#   - kcat, for reading messages back out
#
# Exits non-zero on the first failure, and prints a summary.
#

set -uo pipefail

BROKER="${1:-localhost:9092}"
PASSED=0
FAILED=0

# Unique per run, so a re-run never reads a previous run's messages
RUN_ID="$$_$(date +%s)"

sql() {
    mariadb -N -B -e "$1" 2>&1
}

ok() {
    PASSED=$((PASSED + 1))
    printf '  ok   %s\n' "$1"
}

fail() {
    FAILED=$((FAILED + 1))
    printf '  FAIL %s\n' "$1"
    if [ $# -gt 1 ]; then
        printf '       expected: %s\n' "$2"
        printf '       actual:   %s\n' "$3"
    fi
}

check_eq() {
    local what="$1" expected="$2" actual="$3"

    if [ "$expected" = "$actual" ]; then
        ok "$what"
    else
        fail "$what" "$expected" "$actual"
    fi
}

check_contains() {
    local what="$1" needle="$2" haystack="$3"

    case "$haystack" in
        *"$needle"*) ok "$what" ;;
        *) fail "$what" "something containing '$needle'" "$haystack" ;;
    esac
}

# Reads every message on a topic from the beginning and exits, rather than
# waiting for more. -e is what makes it terminate.
consume() {
    kcat -b "$BROKER" -t "$1" -C -e -o beginning -q 2>/dev/null
}

section() {
    printf '\n%s\n' "$1"
}

trap 'mariadb -e "SELECT kafka_disconnect()" >/dev/null 2>&1' EXIT

# A clean slate: a previous failed run may have left a connection open
mariadb -e "SELECT kafka_disconnect()" >/dev/null 2>&1

# ---------------------------------------------------------------------------
section "Registration"

check_eq "all six functions are registered" \
    "6" \
    "$(sql "SELECT COUNT(*) FROM mysql.func WHERE name LIKE 'kafka\\_%'")"

check_contains "kafka_info() reports the package name" \
    "trigemit-kafka" \
    "$(sql "SELECT kafka_info()")"

# ---------------------------------------------------------------------------
section "Refusals before connecting"

check_contains "kafka_send() is refused while disconnected" \
    "Not connected" \
    "$(sql "SELECT kafka_send('t','x')")"

check_contains "an unknown property is refused" \
    "No such configuration property" \
    "$(sql "SELECT kafka_connection_param('no.such.property','x')")"

check_contains "an invalid value is refused" \
    "Invalid value" \
    "$(sql "SELECT kafka_connection_param('acks','banana')")"

# ---------------------------------------------------------------------------
section "Connecting"

check_eq "a parameter is accepted" \
    "0" \
    "$(sql "SELECT kafka_connection_param('compression.type','lz4')")"

check_eq "kafka_connect() succeeds" \
    "0" \
    "$(sql "SELECT kafka_connect('$BROKER')")"

check_contains "kafka_info() reports the broker" \
    "$BROKER" \
    "$(sql "SELECT kafka_info()")"

check_contains "parameters cannot be changed while connected" \
    "Already connected" \
    "$(sql "SELECT kafka_connection_param('acks','all')")"

check_contains "secrets are redacted by kafka_info()" \
    "********" \
    "$(mariadb -N -B -e "SELECT kafka_disconnect(); SELECT kafka_connection_param('sasl.password','hunter2'); SELECT kafka_connect('$BROKER'); SELECT kafka_info();" 2>&1)"

sql "SELECT kafka_disconnect()" >/dev/null
sql "SELECT kafka_connection_param('sasl.password', NULL)" >/dev/null
sql "SELECT kafka_connect('$BROKER')" >/dev/null

# ---------------------------------------------------------------------------
section "Publishing"

TOPIC="trigemit_test_${RUN_ID}"

check_eq "kafka_send() returns the byte count" \
    "5" \
    "$(sql "SELECT kafka_send('$TOPIC','hello')")"

# The counters only move once the broker has acknowledged, which is what
# kafka_stats() flushes for.
check_eq "the message is delivered, not merely queued" \
    "Transferred: 1 Failed: 0" \
    "$(sql "SELECT kafka_stats('$TOPIC')")"

check_eq "the broker really has the message" \
    "hello" \
    "$(consume "$TOPIC")"

# ---------------------------------------------------------------------------
section "Argument coercion"

COERCE="trigemit_coerce_${RUN_ID}"

sql "SELECT kafka_send('$COERCE', 12345)" >/dev/null
sql "SELECT kafka_send('$COERCE', DATE '2026-01-31')" >/dev/null
sql "SELECT kafka_send('$COERCE', JSON_OBJECT('k','v'))" >/dev/null
sql "SELECT kafka_stats('$COERCE')" >/dev/null

check_eq "an INT, a DATE and a JSON_OBJECT all publish" \
    "12345
2026-01-31
{\"k\": \"v\"}" \
    "$(consume "$COERCE")"

# ---------------------------------------------------------------------------
section "Trigger"

TRIGGER_TOPIC="trigemit_trigger_${RUN_ID}"

mariadb <<EOF >/dev/null 2>&1
DROP DATABASE IF EXISTS trigemit_test;
CREATE DATABASE trigemit_test;
USE trigemit_test;
CREATE TABLE orders (id INT PRIMARY KEY, total DECIMAL(10,2));
CREATE TRIGGER orders_ai AFTER INSERT ON orders FOR EACH ROW
    SELECT kafka_send('$TRIGGER_TOPIC', JSON_OBJECT('id', NEW.id, 'total', NEW.total)) INTO @discard;
EOF

sql "INSERT INTO trigemit_test.orders VALUES (1, 9.99), (2, 20.00)" >/dev/null
sql "SELECT kafka_stats('$TRIGGER_TOPIC')" >/dev/null

check_eq "a trigger publishes one message per row" \
    "{\"id\": 1, \"total\": 9.99}
{\"id\": 2, \"total\": 20.00}" \
    "$(consume "$TRIGGER_TOPIC")"

check_eq "the trigger's messages were all delivered" \
    "Transferred: 2 Failed: 0" \
    "$(sql "SELECT kafka_stats('$TRIGGER_TOPIC')")"

mariadb -e "DROP DATABASE IF EXISTS trigemit_test" >/dev/null 2>&1

# ---------------------------------------------------------------------------
section "Per-topic disconnect"

check_eq "one topic can be dismissed" \
    "0" \
    "$(sql "SELECT kafka_disconnect('$TOPIC')")"

check_contains "stats for a dismissed topic are refused" \
    "unused topic" \
    "$(sql "SELECT kafka_stats('$TOPIC')")"

check_eq "other topics keep working" \
    "1" \
    "$(sql "SELECT kafka_send('$COERCE','z')")"

check_eq "a dismissed topic is recreated on the next send" \
    "1" \
    "$(sql "SELECT kafka_send('$TOPIC','y')")"

# ---------------------------------------------------------------------------
section "Disconnecting"

check_eq "kafka_disconnect() succeeds" \
    "0" \
    "$(sql "SELECT kafka_disconnect()")"

check_contains "kafka_info() reports no connection" \
    "Not connected" \
    "$(sql "SELECT kafka_info()")"

check_contains "sending is refused again" \
    "Not connected" \
    "$(sql "SELECT kafka_send('$TOPIC','x')")"

# ---------------------------------------------------------------------------
printf '\n%d passed, %d failed\n' "$PASSED" "$FAILED"

[ "$FAILED" -eq 0 ]
