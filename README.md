# Percolator

A small Rust implementation of the Percolator transaction model.

## Build

Make sure you have Rust and `protoc` installed.

On macOS, install `protoc` with Homebrew:

```bash
brew install protobuf
```

Then build the project:

```bash
export PATH="/opt/homebrew/bin:$PATH"
rustup run stable cargo check
```

## Notes

- The project uses `prost` to generate Rust message bindings from [src/msg.proto](src/msg.proto).
- The build script is defined in [build.rs](build.rs).
