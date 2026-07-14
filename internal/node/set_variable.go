package node

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dgmos/flow/internal/engine"
)

// SetVariableNode injects or overwrites keys on the flowing JSON context
// and also persists them to durable execution variables when an engine is
// attached via context.
type SetVariableNode struct{}

func (n *SetVariableNode) Execute(ctx context.Context, config map[string]interface{}, input string) (string, error) {
	out := make(map[string]interface{})
	if input != "" && input != "null" {
		if err := json.Unmarshal([]byte(input), &out); err != nil {
			return "", fmt.Errorf("set_variable: invalid input JSON: %w", err)
		}
	}

	variables, ok := config["variables"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("set_variable: 'variables' must be an object")
	}

	for k, v := range variables {
		out[k] = v
	}

	if eng := engine.EngineFromContext(ctx); eng != nil {
		ns := engine.NamespaceFromContext(ctx)
		execID := engine.ExecutionIDFromContext(ctx)
		if ns != "" && execID != "" {
			_ = eng.MergeVars(ctx, ns, execID, variables)
		}
	}

	b, err := json.Marshal(out)
	if err != nil {
		return "", fmt.Errorf("set_variable: marshal error: %w", err)
	}
	return string(b), nil
}

func (n *SetVariableNode) Validate(config map[string]interface{}) error {
	if _, ok := config["variables"]; !ok {
		return fmt.Errorf("set_variable: missing 'variables' in config")
	}
	if _, ok := config["variables"].(map[string]interface{}); !ok {
		return fmt.Errorf("set_variable: 'variables' must be an object")
	}
	return nil
}
