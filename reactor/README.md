# Reactor

`reactor` is a stream processing tool built using [💥 fusion](https://github.com/spy16/fusion).

## Usage

1. Install `reactor` using `go get -u -v github.com/spy16/fusion/reactor`
2. Create a config file `my_config.json` by referring to config files in [samples](./samples) 
3. Run `reactor -config my_config.json`. When you run this, `reactor` will:

    1. Reactor will parse the proto message and create a message descriptor.
    2. Connect to Kafka cluster and subscribe to given topic name.
    3. Parse every message body as protobuf using the descriptor created in step 1 and log JSON formatted version
       to `stdout`.
