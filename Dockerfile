# syntax=docker/dockerfile:1.4

# Stage 1: Build UI (Askr + Vite)
FROM node:slim as ui-builder

WORKDIR /ui

# Copy UI package files
COPY ui/package.json ui/package-lock.json* ./

# Install dependencies
RUN npm install

# Copy UI source
COPY ui/ ./

# Build production UI
RUN npm run build

# Stage 2: Build Rust binary
FROM rust:slim as builder

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

# Stage 3: Runtime
FROM gcr.io/distroless/cc-debian12

WORKDIR /app

# Distroless/cc includes libc6 and C libraries needed for OpenSSL runtime
# No need for additional package installation

# Copy the binary from builder
COPY --from=builder /usr/src/fitz/target/release/fitz /app/fitz

# Copy SPA files for admin UI (built from ui/)
COPY --from=ui-builder /ui/dist /app/public

# Run as non-root numeric UID
USER 65532

EXPOSE 4090 4091
CMD ["/app/fitz"]
