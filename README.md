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
