FROM rust:latest
RUN apt-get update && apt-get install -y libudev-dev libclang-dev
RUN rustup component add rustfmt
WORKDIR /srv
COPY . .
RUN cargo build --release
CMD ./docker-heroku-cmd.sh
