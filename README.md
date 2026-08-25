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
