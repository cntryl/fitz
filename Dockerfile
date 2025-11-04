# syntax=docker/dockerfile:1.4
ARG TARGETPLATFORM
FROM rust:1.91 as builder

# Install musl tools and build essentials. Add pkg-config/libssl-dev
# if your project has native OpenSSL/TLS deps.
RUN apt-get update \
  && apt-get install -y --no-install-recommends musl-tools build-essential ca-certificates pkg-config libssl-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/fitz

# Resolve target triple from TARGETPLATFORM and make it available inside the builder
RUN set -eux; \
  case "${TARGETPLATFORM:-linux/amd64}" in \
    "linux/amd64") TARGET_TRIPLE='x86_64-unknown-linux-musl' ;; \
    "linux/arm64"|"linux/arm64/v8") TARGET_TRIPLE='aarch64-unknown-linux-musl' ;; \
    *) echo "Unsupported platform: $TARGETPLATFORM"; exit 1 ;; \
  esac; \
  rustup target add "$TARGET_TRIPLE"; \
  echo "$TARGET_TRIPLE" > /target_triple

# Dependency caching: copy manifests and do a dummy build to cache crates
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo 'fn main() { println!("build-hint"); }' > src/main.rs
RUN set -eux; \
  TARGET_TRIPLE=$(cat /target_triple); \
  VARNAME=$(echo "$TARGET_TRIPLE" | tr '-' '_' | tr '[:lower:]' '[:upper:]'); \
  export CARGO_TARGET_${VARNAME}_LINKER=musl-gcc; \
  cargo build --release --target "$TARGET_TRIPLE"

# Copy the full source and perform the real build
COPY . .
RUN set -eux; \
  TARGET_TRIPLE=$(cat /target_triple); \
  VARNAME=$(echo "$TARGET_TRIPLE" | tr '-' '_' | tr '[:lower:]' '[:upper:]'); \
  export CARGO_TARGET_${VARNAME}_LINKER=musl-gcc; \
  cargo build --release --target "$TARGET_TRIPLE" --locked; \
  strip target/"$TARGET_TRIPLE"/release/fitz || true; \
  chmod 755 target/"$TARGET_TRIPLE"/release/fitz

FROM gcr.io/distroless/static
WORKDIR /app
# Copy the artifact for the chosen platform (wildcard matches the target dir)
COPY --from=builder /usr/src/fitz/target/*/release/fitz /app/fitz

# Run as non-root numeric UID; distroless has no adduser/passwd files.
USER 65532
EXPOSE 8080
CMD ["/app/fitz"]
