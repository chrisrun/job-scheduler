FROM rust:1.61.0-slim-buster as builder

RUN apt-get update && apt-get install -y pkg-config && \
    apt-get install -y libssl-dev && \
    apt-get install -y openssl



WORKDIR /usr/src/scheduler

COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./src ./src
COPY ./.env ./.env

RUN cargo install --path .

FROM rust:1.61.0-slim-buster

RUN apt-get update && apt-get install -y curl && apt-get install -y unzip && apt-get install -y sudo
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && unzip awscliv2.zip && sudo ./aws/install

COPY --from=builder /usr/src/scheduler/.env /etc/scheduler-local.env
COPY --from=builder /usr/local/cargo/bin/scheduler /usr/local/bin/scheduler

ENTRYPOINT ["/usr/local/bin/scheduler"]
CMD ["/etc/job-scheduler.env"]




