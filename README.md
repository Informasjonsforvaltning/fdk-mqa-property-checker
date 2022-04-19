# fdk-mqa-property-checker

This service is part of the Metadata Quality Assessment stack. This service listens to dataset harvested events (Kafka) and 
checks if the dataset has defined/set specific properties. Results are stored in a DQV metrics model which is
stored in the MQA event topic (Kafka).

## Install Rust
`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`

## Build dev
`cargo build --verbose`

## Build release
`cargo build --release`

## Run application
`./target/release/fdk-mqa-property-checker`

### Show help
`./target/release/fdk-mqa-property-checker --help`


## Kafka
Use this project to run your local Kafka cluster 

https://github.com/Informasjonsforvaltning/fdk-event-streaming-service
