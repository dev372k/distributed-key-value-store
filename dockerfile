FROM rust:1.88 as builder

WORKDIR /app

COPY . .

RUN cargo build --release

# ================= RUNTIME =================

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    openssl \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/kv-node /usr/local/bin/kv-node

EXPOSE 3030

CMD ["kv-node"]