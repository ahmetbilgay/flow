package engine

import "context"

type ctxKey string

const (
	ctxNamespace   ctxKey = "flowNamespace"
	ctxExecutionID ctxKey = "flowExecutionID"
	ctxEngine      ctxKey = "flowEngine"
)

// WithExecutionContext attaches namespace/execID for nodes that need durable vars.
func WithExecutionContext(ctx context.Context, namespace, execID string) context.Context {
	ctx = context.WithValue(ctx, ctxNamespace, namespace)
	ctx = context.WithValue(ctx, ctxExecutionID, execID)
	return ctx
}

// WithEngine attaches the engine pointer for setvariable durable writes.
func WithEngine(ctx context.Context, e *Engine) context.Context {
	return context.WithValue(ctx, ctxEngine, e)
}

func NamespaceFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxNamespace).(string)
	return v
}

func ExecutionIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxExecutionID).(string)
	return v
}

func EngineFromContext(ctx context.Context) *Engine {
	v, _ := ctx.Value(ctxEngine).(*Engine)
	return v
}
