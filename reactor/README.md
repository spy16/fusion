# Reactor

`reactor` is a small stream processing tool built using `fusion`.

## Usage

1. Install `reactor` using `go get -u -v github.com/spy16/fusion/reactor`
2. Create a config file `my_config.json`:

    ```json
    {
        "kafka": {
          "workers": 10,
           "topic": "my-topic",
           "group_id": "fusion-test",
           "brokers": [
               "p-gojek-id-mainstream.golabs.io:6668"
           ]
        },
        "message_type": "com.my.Message",
        "proto_files": [
            "com/my/message.proto"
        ],
        "proto_import_dirs": [
            "/Users/bob/workspace/myproto-project/src/main/proto"
        ]
    } 
    ```
   
   > Note: See [kafka.go](./kafka.go) for config options for Kafka.

3. Run `reactor -config my_config.json`. When you run this, `reactor` will:

    1. Reactor will parse the proto message and create a message descriptor.
    2. Connect to Kafka cluster and subscribe to given topic name.
    3. Parse every message body as protobuf using the descriptor created in step 1 and log JSON formatted version
       to `stdout`.
