# Fuse logging backend

## Prerequired

- CMAKE Version >= 3.18
- C++20 Compiler
- libfuse3-dev
- ninja, make or another build system

## Compilation

> mkdir build && \
> cd build && \
> cmake .. && \
> cmake --build . --config Release --target logfs

## Running

> sudo ./logfs -f -o default_permissions -o allow_other [mountdir]
