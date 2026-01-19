# syntax=docker/dockerfile:1.4
FROM rust:1.91 as builder

# Install build essentials and OpenSSL dev libraries
# cntryl-midge pulls reqwest with native-tls, which requires OpenSSL.
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential ca-certificates libssl-dev pkg-config \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/fitz

# Dependency caching: copy manifests and do a dummy build to cache crates
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo 'fn main() { println!("build-hint"); }' > src/main.rs
RUN cargo build --release

# Copy the full source and perform the real build
COPY . .
RUN cargo build --release --locked \
  && strip target/release/fitz || true

FROM gcr.io/distroless/cc-debian12

WORKDIR /app

# Distroless/cc includes libc6 and C libraries needed for OpenSSL runtime
# No need for additional package installation

# Copy the binary from builder
COPY --from=builder /usr/src/fitz/target/release/fitz /app/fitz

# Run as non-root numeric UID
USER 65532

EXPOSE 4090 4091
CMD ["/app/fitz"]
