
# Stage 1: Build UI (Askr + Vite+)
FROM node:slim AS frontend

# Install dependencies needed for the curl command
RUN apt-get update \
  && apt-get install -y --no-install-recommends bash curl ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Install Vite+ globally
RUN curl -fsSL https://vite.plus | bash

ENV PATH="/root/.vite-plus/bin:${PATH}"

WORKDIR /ui

# Copy UI package files
COPY ui/package.json ui/package-lock.json* ./

# Install dependencies
RUN npm ci

# Copy UI source
COPY ui/ ./

# Build production UI
RUN npm run build

# Stage 2: Build Rust binary
FROM rust:slim AS backend

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

# Prepare a writable storage directory for the non-root runtime user.
RUN mkdir -p /usr/src/fitz/runtime-data

# Stage 3: Runtime
FROM gcr.io/distroless/cc-debian12

WORKDIR /app

# Distroless/cc includes libc6 and C libraries needed for OpenSSL runtime
# No need for additional package installation

# Copy the binary from backend
COPY --from=backend /usr/src/fitz/target/release/fitz /app/fitz

# Provide a writable /data path for local disk storage.
COPY --from=backend --chown=65532:65532 /usr/src/fitz/runtime-data/ /data/

# Copy SPA files for admin UI (built to ui/dist)
COPY --from=frontend /ui/dist/ /app/public/

# Run as non-root numeric UID
USER 65532

EXPOSE 4090 4091
CMD ["/app/fitz"]
