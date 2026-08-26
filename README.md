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

Then register the functions:

```
mariadb -e "CREATE FUNCTION kafka_send RETURNS INTEGER SONAME 'kafka.so'"
```

