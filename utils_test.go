package fusion

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogFrom(t *testing.T) {
	t.Parallel()

	t.Run("NoContext", func(t *testing.T) {
		log := LogFrom(context.Background())
		assert.NotNil(t, log)
		assert.NotPanics(t, func() {
			log(nil)
		})
	})

	t.Run("WithNilValue", func(t *testing.T) {
		ctx := withLog(context.Background(), nil)
		log := LogFrom(ctx)
		assert.NotNil(t, log)
		assert.NotPanics(t, func() {
			log(nil)
		})
	})

	t.Run("WithValue", func(t *testing.T) {
		logged := false
		ctx := withLog(context.Background(), func(_ map[string]interface{}) {
			logged = true
		})

		log := LogFrom(ctx)
		assert.NotNil(t, log)
		assert.NotPanics(t, func() {
			log(nil)
		})
		assert.True(t, logged)
	})
}
