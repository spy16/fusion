package fusion

import (
	"reflect"
	"testing"
	"time"
)

func TestOptions_defaults(t *testing.T) {
	want := Options{
		Workers:      1,
		MaxRetries:   0,
		Backoff:      nil,
		Logger:       goLogger{},
		PollInterval: 300 * time.Millisecond,
	}

	got := Options{}
	got.defaults()

	got.Processor = nil
	got.OnFailure = nil
	got.Queue = nil

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Options.defaults() \nwant=%+v\ngot=%+v", want, got)
	}
}
