# Static arm64 `bootstrap` for provided.al2023. Build with the repository root as the context:
#   docker build -f pdf-extractor/lambda/Dockerfile --target out --output type=local,dest=./out .
FROM clux/muslrust:stable AS build
WORKDIR /volume
COPY pdf_oxide ./pdf_oxide
COPY pdf-extractor/lambda ./pdf-extractor/lambda
WORKDIR /volume/pdf-extractor/lambda
RUN rustup target add aarch64-unknown-linux-musl && \
    cargo build --release --features lambda --bin lambda --target aarch64-unknown-linux-musl

FROM scratch AS out
COPY --from=build /volume/pdf-extractor/lambda/target/aarch64-unknown-linux-musl/release/lambda /bootstrap
