# TrigEmit-Kafka

### Your database already knows what changed. Let it say so.

[![CI](https://github.com/ctrix/TrigEmit-Kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/ctrix/TrigEmit-Kafka/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![MariaDB](https://img.shields.io/badge/MariaDB-11.x-003545?logo=mariadb&logoColor=white)](https://mariadb.org/)
[![librdkafka](https://img.shields.io/badge/librdkafka-2.x-231F20?logo=apachekafka&logoColor=white)](https://github.com/confluentinc/librdkafka)

A MariaDB UDF that publishes to Kafka from inside SQL. Put `kafka_send()` in a
trigger and every row change leaves the database as an event — asynchronously,
without the writing transaction waiting for the broker.

## The problem it solves

Something changed in a table, and something outside the database needs to know:
a search index, a cache, an audit trail, another service.

The usual answers all cost something. Dual writes in the application mean every
writer has to remember to publish, and a crash between the two leaves the
change and the event disagreeing. A polling job trades latency for load and
needs a watermark column on every table it watches. Change data capture is the
principled answer, but it means running and operating a connector, and reading
the replication stream of a database that was not necessarily set up for it.

TrigEmit-Kafka takes the shortest path: the database already fires a trigger on
the row it just changed, so the event is emitted right there, from the one place
that cannot miss a write and cannot disagree with it.

- **Nothing in the application changes.** Any client writing to the table
  produces events, including ones you do not control and ones written before
  Kafka existed in your architecture.
- **The writer does not wait.** `kafka_send()` hands the message to librdkafka's
  queue and returns; a background thread deals with the broker. A slow or absent
  broker does not slow down INSERTs.
- **The payload is yours.** `JSON_OBJECT(...)` and the `NEW` / `OLD` rows mean
  the event carries exactly the fields you want, in the shape the consumer
  wants, rather than a raw row image you have to reinterpret.

The trade-off, stated plainly: this is fire-and-forget, not transactional
outbox. The message is queued when the trigger fires, so a transaction that
rolls back afterwards will still have emitted its event, and messages queued
when the server is killed outright are lost. If you need the event and the row
to commit or fail together, you want an outbox table — with, if you like,
`kafka_send()` on *its* trigger.

## Using it

```sql
-- once per server, at startup
SELECT kafka_connect('broker1:9092,broker2:9092');

-- once per table
CREATE TRIGGER orders_ai AFTER INSERT ON orders FOR EACH ROW
    SELECT kafka_send('orders', JSON_OBJECT(
        'event', 'created',
        'id',    NEW.id,
        'total', NEW.total,
        'at',    NOW()
    )) INTO @discard;
```

That is the whole integration. From here on, every insert into `orders` — from
any client, in any language — puts a message on the `orders` topic:

```json
{"event": "created", "id": 1041, "total": 29.99, "at": "2026-08-26 09:12:44"}
```

An authenticated broker is the same, with parameters set before connecting:

```sql
SELECT kafka_connection_param('security.protocol', 'SASL_SSL');
SELECT kafka_connection_param('sasl.mechanism',    'SCRAM-SHA-512');
SELECT kafka_connection_param('sasl.username',     'appuser');
SELECT kafka_connection_param('sasl.password',     's3cret');
SELECT kafka_connect('broker1:9093,broker2:9093');
```

`kafka_connection_param()` accepts any librdkafka property, so delivery
guarantees, compression, buffering and TLS are all reachable without
rebuilding. See [Functions](#functions) for the full set.

## Performance

Measured with the integration setup: a single-node Kafka broker and the MariaDB
server on the **same 2-core aarch64 host with 2 GB of RAM**, publishing through
a real `AFTER INSERT` trigger. Modest hardware, and the broker is a process on
the same machine, so there is no network in the path — read these as an upper
bound on what the module itself costs, not as a capacity plan.

Inserting 50,000 rows in one statement, with and without the trigger:

| | time | per row |
|---|---|---|
| plain INSERT | 0.033 s | 0.66 µs |
| INSERT + `kafka_send()` trigger | 0.178 s | 3.56 µs |
| **cost of publishing** | **0.145 s** | **2.9 µs** |

Sustained, 200,000 rows through the trigger in a single statement:

```
enqueue:     773 ms   ->  258,000 messages/s
end-to-end:  788 ms   ->  253,000 messages/s   (acks=all, idempotent)
```

`Transferred: 200000  Failed: 0`, and all 200,000 read back off the topic with
`kcat`. End-to-end here means the broker had acknowledged every message, with
the default settings — `enable.idempotence=true`, which forces `acks=all`. The
flush after the insert took 15 ms, because librdkafka had already batched and
shipped nearly everything while the statement was still running.

So the publishing overhead is a few microseconds per row, and throughput is
bounded by the broker and the network long before it is bounded by this module.
Between runs the numbers moved by about 10%, which on a 2-core box is noise.

What actually constrains you in production is the queue, not the CPU:
`queue.buffering.max.messages` (50,000 by default) is how many messages may be
in flight before `kafka_send()` starts waiting for the broker to catch up. If
the broker is unreachable, that is the buffer that absorbs the outage — and
once it is full, INSERTs will block. Size it for how long an outage you want to
survive at your write rate, with `kafka_connection_param()`.

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


## Building a Debian package

```
./build-deb.sh
```

Build dependencies are `debhelper`, `cmake`, `pkg-config`, `libmariadb-dev`
and `librdkafka-dev`; `dpkg-checkbuilddeps` lists whatever is missing.

`debian/` is **generated and not tracked**. The tracked template is
`debian-build/`, a complete Debian directory in its own right, whose changelog
carries the placeholder version `unset`. The script copies it into place,
replaces the changelog with one whose version comes from git, runs
`dpkg-buildpackage`, and collects the results in `dist/` instead of leaving
them in the parent directory. Edit `debian-build/`, never `debian/` -- the
latter is overwritten on every build.

The version is derived the same way the module's own is, so the package and
`kafka_info()` agree. A Debian upstream version has to start with a digit, and
a native package cannot contain a hyphen, so `git describe` is rewritten:

| `git describe`            | package version         |
|---------------------------|-------------------------|
| `v0.1.0`                  | `0.1.0`                 |
| `v0.1.0-3-g1a2b3c4`       | `0.1.0+3.g1a2b3c4`      |
| `1a2b3c4` (no tag)        | `0.0.0+git.1a2b3c4`     |
| any of the above `-dirty` | ...`+dirty`             |

The `+` suffixes sort above the bare tag, so a build made after a release
upgrades over that release. An untagged build sorts below any tagged one,
which is what you want: `apt` will not pull a dev build over a release.

The result installs the module, the registration scripts and the
documentation:

```
/usr/lib/mysql/plugin/kafka.so
/usr/share/trigemit-kafka/install.sql
/usr/share/trigemit-kafka/uninstall.sql
/usr/share/doc/trigemit-kafka/
```

Installing the package does **not** register the functions — that still needs
`mariadb < /usr/share/trigemit-kafka/install.sql`, because it writes to
`mysql.func` on a running server.

Runtime dependencies are worked out by `dpkg-shlibdeps` and come to
`librdkafka1`, `libc6` and `mariadb-server`. Note the absence of a MariaDB
client library: a server-side UDF resolves the UDF ABI from the server's own
symbols and links nothing from MariaDB.

The package is **native** (`debian-build/source/format` is `3.0 (native)`), so
its version comes from the changelog alone and there is no separate upstream
tarball. Tag a release with `git tag v1.2.3` and the next `./build-deb.sh`
picks it up automatically.

## Functions

Six functions, registered by `sql/install.sql`. Connection state is
**server-wide**, not per session: one `kafka_connect()` serves every client and
every trigger.

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

## Tests

`tests/run-tests.sh` is an integration suite. There is nothing to mock — the
whole point of this module is what happens between a MariaDB server and a Kafka
broker, so a test that stubbed either would be testing nothing. It needs both,
plus `kcat` to read messages back out:

```
tests/run-tests.sh [broker]        # default localhost:9092
```

It expects the module installed, the functions registered, and a reachable
broker. It covers registration, the refusals before connecting, property
validation, secret redaction, publishing with delivery confirmed by the
counters *and* by consuming the messages back, argument coercion from INT /
DATE / JSON_OBJECT, a real trigger firing per row, per-topic disconnect and
recreation, and disconnect.

Two behaviours it deliberately does not cover, because both need the broker to
go away mid-run, which a CI service container cannot easily do:

- delivery-failure throttling and the recovery line that follows an outage
- teardown staying bounded when the broker is unreachable

CI checks the second separately by pointing the module at a dead address and
timing `systemctl stop mariadb`.

## Continuous integration

`.github/workflows/ci.yml` runs three jobs on push and pull request:

- **Build** — configures and builds under both `RelWithDebInfo` and `Debug`,
  fails on any compiler warning, and checks all six UDF symbols are actually
  exported. The `Debug` leg matters because it compiles the `debug_print()`
  bodies the release leg optimises away.
- **Debian package** — runs `./build-deb.sh`, prints the package contents and
  metadata, and uploads the `.deb` as an artifact.
- **Integration tests** — brings up a single-node Kafka in KRaft mode as a
  service container and MariaDB on the runner itself, installs the module,
  registers the functions, and runs `tests/run-tests.sh`.

MariaDB runs on the runner rather than as a service container because the
module has to be written into the server's plugin directory and the server
restarted, which is awkward from outside a container.
