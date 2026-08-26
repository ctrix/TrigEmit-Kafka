# TrigEmit-Kafka

## Building

Builds must happen outside the source tree; an in-tree build is refused.

```
cmake -S . -B build
cmake --build build -j$(nproc)
```

### Build type

The default build type is `RelWithDebInfo` (optimised, with debug symbols kept).
Override it at configure time with `-DCMAKE_BUILD_TYPE=`:

```
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
```

Accepted values are `Debug` (`-g -O0`, no optimisation), `Release` (`-O3`) and
`RelWithDebInfo`. The type is cached, so to change it on an existing build
directory either pass it again or delete `build/` and reconfigure.

### Debug output

`debug_print()` diagnostics are compiled out by default and in for a `Debug`
build. Force either way with `-DTRIGEMIT_DEBUG=`:

```
cmake -S . -B build -DTRIGEMIT_DEBUG=ON
```

The output goes to stderr, which for a loaded module means the MariaDB error
log. Like the build type, the value is cached after the first configure.

### Version

There is no version number written down in the source. It comes from the most
recent git tag, so tagging a release is all it takes:

```
git tag v1.2.3
git push --tags
```

`kafka_info()` reports what `git describe` produces — the tag on a tagged
build, otherwise the tag plus commit distance and short hash, with `-dirty`
appended when the working tree was modified:

```
Version: trigemit-kafka-v1.2.3
Version: trigemit-kafka-v1.2.3-4-g1a2b3c4-dirty
Version: trigemit-kafka-1a2b3c4          (no tags yet)
Version: trigemit-kafka-unknown          (no git information at all)
```

A leading `v` is stripped for CMake's own `PROJECT_VERSION`, which falls back
to `0.0.0` when no tag is reachable or the nearest tag is not a version number.
## Installing

```
cmake --install build
```

This copies `kafka.so` into the server's plugin directory — what
`SELECT @@plugin_dir` reports — which is where the server looks and nowhere
else. The directory is detected at configure time by asking the local
`mariadbd` binary for its compiled-in default, and is printed during configure:

```
-- MariaDB server plugin dir: /usr/lib/mysql/plugin
```

Building for a server that is not the local one, or on a machine with no server
binary installed, means saying where it goes:

```
cmake -S . -B build -DMARIADB_PLUGIN_DIR=/usr/lib64/mysql/plugin
```

Note this is **not** `mariadb_config --plugindir`, which reports the *client*
library's plugin directory — a different place entirely.

Staging for a package build works as usual:

```
DESTDIR=/tmp/stage cmake --install build
```

The server must not be running when the file is replaced. Overwriting a
library that a running server has mapped corrupts it in place and takes the
server down:

```
systemctl stop mariadb
cmake --install build
systemctl start mariadb
```

### Registering the functions

The module does nothing until the functions are registered. `sql/install.sql`
does it:

```
mariadb < sql/install.sql
```

`cmake --install` also puts both scripts in the data directory, so from an
installed package it is:

```
mariadb < /usr/share/trigemit-kafka/install.sql
```

Registration is server-wide and survives a restart -- it is a row in
`mysql.func`, not a session setting -- so it is done once, not per connection.
It needs INSERT on `mysql.func`, which in practice means running it as root.

To remove them again:

```
mariadb < sql/uninstall.sql
```

Dropping the last function makes MariaDB `dlclose` the module, which tears the
producer down and flushes what librdkafka still had queued, bounded by
`KAFKA_FLUSH_TIMEOUT_MS`. Against an unreachable broker that takes the full
10 seconds.


## Functions

See **Registering the functions** above for how to install them.

### `kafka_connection_param(property, value)`

Sets any librdkafka configuration property, to be applied when
`kafka_connect()` builds the producer. Returns 0.

Property names are not checked against a list of our own — librdkafka
validates them, so this keeps working as librdkafka gains properties, and a
mistake is refused immediately in librdkafka's own words:

```
MariaDB> SELECT kafka_connection_param('acks', 'banana');
ERROR 1123 (HY000): Can't initialize function 'kafka_connection_param';
Invalid value for configuration property "request.required.acks"
```

Parameters may only be set while disconnected; `kafka_connect()` freezes them,
because librdkafka reads its configuration once and never looks at it again.
Setting the same property twice replaces the earlier value.

### `kafka_connect(brokers)`

Builds the producer from the accumulated parameters, opens the connection and
starts the poll thread. Returns 0. There is one producer for the whole server.

A plaintext connection needs nothing but the broker list:

```sql
SELECT kafka_connect('broker1:9092,broker2:9092');
```

An authenticated, encrypted one is the same call with parameters in front of
it:

```sql
SELECT kafka_connection_param('security.protocol', 'SASL_SSL');
SELECT kafka_connection_param('sasl.mechanism',    'SCRAM-SHA-512');
SELECT kafka_connection_param('sasl.username',     'appuser');
SELECT kafka_connection_param('sasl.password',     's3cret');
SELECT kafka_connection_param('ssl.ca.location',   '/etc/ssl/certs/ca.pem');
SELECT kafka_connect('broker1:9093,broker2:9093');
```

### Delivery guarantees

`enable.idempotence` is **on by default**, because the point of this module is a
stream of row changes, where a reordered or duplicated event is a wrong event.
librdkafka then guarantees ordering and de-duplicates broker-side retries, and
adjusts `acks`, `retries` and the in-flight limit to match.

It requires a broker at 0.11 or newer and forces `acks=all`, so setting `acks`
to anything else is a contradiction and is refused outright:

```
MariaDB> SELECT kafka_connection_param('acks', '0');
MariaDB> SELECT kafka_connect('localhost:9092');
-- NULL, and in the error log:
-- `acks` must be set to `all` when `enable.idempotence` is true
```

For an older broker, or to trade the guarantee for throughput, turn it off
first -- a parameter always overrides the default:

```sql
SELECT kafka_connection_param('enable.idempotence', 'false');
SELECT kafka_connection_param('acks',               '1');
```

Credentials passed this way appear in the statement text, so they can reach
the general query log, the slow query log, `SHOW PROCESSLIST` and the client's
history file. `kafka_info()` redacts any property whose name contains
`password` or `secret`, but the statement itself is not hidden.

### `kafka_send(topic, payload)`

Publishes one message, creating the topic handle on first use. Returns the
number of bytes queued, or NULL on error. Both arguments are coerced to
strings, so an INT column, a DATE or a `JSON_OBJECT(...)` can be passed without
a cast — which is what makes the trigger case work:

```sql
CREATE TRIGGER orders_ai AFTER INSERT ON orders FOR EACH ROW
    SELECT kafka_send('orders', JSON_OBJECT('id', NEW.id, 'total', NEW.total))
    INTO @discard;
```

Publishing is asynchronous: a successful return means the message was queued,
not delivered. `kafka_stats()` reports what happened to it.

### `kafka_disconnect([topic])`

With no argument, tears down every topic and the producer. With a topic name,
tears down that one topic and leaves the producer and the other topics
running. Returns 0.

The parameters are **kept**, so reconnecting is a single call:

```sql
SELECT kafka_disconnect();
SELECT kafka_connect('broker1:9093,broker2:9093');   -- same configuration
```

To take a parameter back out, set it to SQL NULL:

```sql
SELECT kafka_connection_param('acks', NULL);
```

Teardown waits for queued messages, bounded by `KAFKA_FLUSH_TIMEOUT_MS`
(10 seconds), so an unreachable broker cannot hang `systemctl stop mariadb`.
The wait is one flush for the whole producer, regardless of topic count.

### `kafka_stats(topic)` and `kafka_info()`

```
MariaDB> SELECT kafka_stats('orders');
Transferred: 41  Failed: 0

MariaDB> SELECT kafka_info();
Version: trigemit-kafka-v0.1.0
Brokers: broker1:9092
Parameters: 2
  enable.idempotence = true
  sasl.password = ********
```

`kafka_stats()` flushes first, bounded by the same timeout, so the counters
account for everything produced so far.

## Logging

Everything goes to stderr, which for a loaded module is the MariaDB error log,
prefixed `trigemit-kafka:`.

Delivery failures are throttled: the first is logged immediately with the
error, repeats of the same error are counted rather than printed, and a
summary follows at most once every `KAFKA_ERROR_LOG_INTERVAL_S` (60 seconds).
A different error code is reported at once. Recovery is logged too. Without
this a broker outage writes one line per queued message.
