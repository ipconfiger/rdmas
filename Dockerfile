# RDMAS - One-Sided RDMA distributed KV store
#
# Build image for containerized compilation and testing.
#
# Runtime requirements (pass these to docker run / docker compose):
#   --device=/dev/infiniband/*   Expose RDMA HCAs or SoftRoCE devices
#   --network=host               RDMA operates at kernel bypass; host net required
#   --privileged                 Required for HugePages and RDMA resource creation
#   -v /dev/hugepages:/dev/hugepages  HugePages mount for RDMA memory registration
#
# SoftRoCE (software RDMA) setup on the host before running:
#   sudo modprobe rdma_rxe
#   sudo rdma link add rxe0 type rxe netdev eth0

FROM rust:1.85-slim-bookworm

# System dependencies for RDMA and bindgen
RUN apt-get update && apt-get install -y --no-install-recommends \
    rdma-core \
    libibverbs-dev \
    librdmacm-dev \
    ibverbs-utils \
    clang \
    libclang-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Environment for release-optimized builds
ENV RUSTFLAGS="-C target-cpu=native"
ENV CARGO_BUILD_JOBS=$(nproc)

# SoftRoCE environment variable (override at runtime with actual device name)
ENV RDMA_DEVICE_NAME=rxe0

WORKDIR /app

# Pre-cache dependencies (speeds up iterative builds)
COPY Cargo.toml Cargo.lock ./
COPY crates/ibverbs-sys/Cargo.toml crates/ibverbs-sys/build.rs crates/ibverbs-sys/
COPY crates/lmcache-connector/Cargo.toml crates/lmcache-connector/
COPY build.rs ./
COPY proto/ proto/
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs
RUN mkdir -p crates/ibverbs-sys/src && echo '' > crates/ibverbs-sys/src/lib.rs
RUN mkdir -p crates/lmcache-connector/src && echo '' > crates/lmcache-connector/src/lib.rs
RUN cargo fetch && cargo check --workspace || true

# Copy full source and build
COPY . .
RUN cargo build --release

# Note: CMD / ENTRYPOINT are intentionally omitted; override at runtime.
# Typical usage:
#   docker run --rm --privileged --network=host \
#     --device=/dev/infiniband/uverbs0 \
#     -v /dev/hugepages:/dev/hugepages \
#     rdmas-server
