# 💥 Fusion

Fusion is a tiny stream processing library written in `Go`.

## Features

* Simple & lightweight.
* Highly Composable. Compose `Proc` implementations in a way that is similar to 
  middleware pattern to get concurrent processing, automatic retries etc.
* Use for simple single node or more complex distributed setup by using different
  `fusion.Stream` and `fusion.Proc` implementations.

## Usage

A simple line counter implementation:

```go
package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"github.com/spy16/fusion"
)

func main() {
	count := int64(0)
	runner := fusion.Runner{
		Stream: &fusion.LineStream{From: os.Stdin},
		Proc: &fusion.Fn{
			Workers: 5,
			Func: func(ctx context.Context, msg fusion.Msg) error {
				atomic.AddInt64(&count, 1)
				return nil
			},
		},
	}
	_ = runner.Run(context.Background())
	fmt.Printf("Count=%d\n", count)
}
```
