# Stream

<details>
  <summary>Kafka Stream</summary>
  
  Following snippet shows a Kafka Stream implementation using [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go).
  
  ```go
  
// KafkaConnect connects to the Kafka brokers described by the config and returns
// a fusion stream instance.
func Connect(topic string, cm kafka.ConfigMap) (*Stream, error) {
    c, err := kafka.NewConsumer(&cm)
    if err != nil {
        return nil, err
    }
    _ = c.SubscribeTopics([]string{topic}, nil)

    return &Stream{consumer: c}, nil
}

// Stream implements Kafka based fusion Stream.
type Stream struct {
    ReadTimeout time.Duration
    OnError     func(err error)
    consumer    *kafka.Consumer
    BufferSz    int
}

// Out starts a goroutine to consume from Kafka and writes to the returned
// channel. Returned channel will be closed when the context is cancelled.
func (s *Stream) Out(ctx context.Context) (<-chan fusion.Msg, error) {
    if s.ReadTimeout == 0 {
        s.ReadTimeout = -1
    }

    ch := make(chan fusion.Msg, s.BufferSz)
    go s.runConsumer(ctx, ch)
    return ch, nil
}

func (s *Stream) runConsumer(ctx context.Context, ch chan<- fusion.Msg) {
    defer func() {
        _ = s.consumer.Close()
        close(ch)
    }()

    for {
        msg, err := s.consumer.ReadMessage(s.ReadTimeout)
        if err != nil {
            if s.OnError != nil {
                s.OnError(err)
            }
            continue
        }

        fuMsg := fusion.Msg{
            Key: msg.Key,
            Val: msg.Value,
            Ack: func(err error) {
                if err == nil {
                    // if commit fails, it's okay since consumer will receive the
                    // message again.
                    _, _ = s.consumer.CommitMessage(msg)
                }
            },
        }

        select {
        case <-ctx.Done():
            return
        case ch <- fuMsg:
        }
    }
}
  ```
</details>
