package fusion

import "context"

var logKey = ctxKey("log")

// LogFrom extracts log function set by fusion Runner from the context.
func LogFrom(ctx context.Context) Log {
	log, ok := ctx.Value(logKey).(Log)
	if !ok || log == nil {
		return func(_ map[string]interface{}) {}
	}
	return log
}

func withLog(ctx context.Context, log Log) context.Context {
	return context.WithValue(ctx, logKey, log)
}

type ctxKey string
