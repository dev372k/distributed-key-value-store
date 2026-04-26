FROM rust:latest AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y musl-tools && rm -rf /var/lib/apt/lists/*
RUN rustup target add x86_64-unknown-linux-musl

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release --target x86_64-unknown-linux-musl
RUN rm -rf src

COPY . .
RUN cargo build --release --target x86_64-unknown-linux-musl

FROM alpine:latest

WORKDIR /output

COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/kv_store ./kv_store

RUN chmod +x kv_store

CMD ["./kv_store"]