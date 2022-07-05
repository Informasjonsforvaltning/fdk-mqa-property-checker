FROM rust:latest AS builder

RUN apt-get update && apt-get -y install \
    build-essential \
    cmake \
    clang 

ADD . ./

RUN cargo build --release

FROM rust:latest

ENV TZ=Europe/Oslo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /usr/local/bin

COPY --from=builder ./target/release/fdk-mqa-property-checker ./fdk-mqa-property-checker

CMD ["./fdk-mqa-property-checker"]
