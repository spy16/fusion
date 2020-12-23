# Reactor

`reactor` is a small stream processing tool built using `fusion`.

## Usage

1. Install `reactor` using `go get -u -v github.com/spy16/fusion/reactor`
2. Create a config file `my_config.json`: 

    ```json
    {
      "topic": "my-kafka-topic",
      "kafka": {
        "bootstrap.servers": "my-kafka-node:6668",
        "group.id": "reactor-test",
        "auto.offset.reset": "latest",
        "socket.keepalive.enable": true
      },
      "message_type": "com.example.MyMessage",
      "proto_files": [
        "com/example/MyMessage"
      ],
      "proto_import_dirs": [
        "./my-protobuf-project"
      ]
    }
    ```
3. Run `reactor -config my_config.json`. When you run this, `reactor` will:
 
   1. Reactor will parse the proto message and create a message descriptor.
   2. Connect to Kafka cluster and subscribe to given topic name.
   3. Parse every message body as protobuf using the descriptor created in step 1 and log JSON formatted version to `stdout`.
