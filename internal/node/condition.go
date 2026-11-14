package node

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// ConditionNode evaluates a condition and returns a branch ("true" or "false").
//
// Config:
//   - "variable"  (string) : key to read from the input JSON
//   - "operator"  (string) : ==, !=, >, <, >=, <=, contains, not_contains
//   - "value"     (any)    : value to compare against
//
// Output: {"branch": "true"} or {"branch": "false"}
// Original input fields are preserved alongside "branch" so downstream nodes
// can still read them without needing a transform step.
type ConditionNode struct{}

func (n *ConditionNode) Execute(ctx context.Context, config map[string]interface{}, input string) (string, error) {
	var inMap map[string]interface{}
	if err := json.Unmarshal([]byte(input), &inMap); err != nil {
		return "", fmt.Errorf("failed to parse input JSON: %w", err)
	}

	variable, _ := config["variable"].(string)
	operator, _ := config["operator"].(string)
	value := config["value"]

	inputValue, exists := inMap[variable]
	if !exists {
		return buildCondOutput(inMap, "false"), nil
	}

	aStr := fmt.Sprintf("%v", inputValue)
	bStr := fmt.Sprintf("%v", value)

	result, err := compare(operator, aStr, bStr)
	if err != nil {
		return "", err
	}

	branch := "false"
	if result {
		branch = "true"
	}
	return buildCondOutput(inMap, branch), nil
}

// buildCondOutput merges the branch tag back into the input data so
// downstream nodes still have access to original fields.
func buildCondOutput(inMap map[string]interface{}, branch string) string {
	out := make(map[string]interface{})
	for k, v := range inMap {
		out[k] = v
	}
	out["branch"] = branch
	b, _ := json.Marshal(out)
	return string(b)
}

// compare evaluates operator on two values. When both sides are numbers,
// equality and ordering are numeric (so 5 == "5.0"); ordering operators are
// false when either side is not a number.
func compare(operator, aStr, bStr string) (bool, error) {
	a, errA := strconv.ParseFloat(strings.TrimSpace(aStr), 64)
	b, errB := strconv.ParseFloat(strings.TrimSpace(bStr), 64)
	numeric := errA == nil && errB == nil

	switch operator {
	case "==", "equal":
		if numeric {
			return a == b, nil
		}
		return aStr == bStr, nil
	case "!=", "not_equal":
		if numeric {
			return a != b, nil
		}
		return aStr != bStr, nil
	case ">":
		return numeric && a > b, nil
	case "<":
		return numeric && a < b, nil
	case ">=":
		return numeric && a >= b, nil
	case "<=":
		return numeric && a <= b, nil
	case "contains":
		return strings.Contains(aStr, bStr), nil
	case "not_contains":
		return !strings.Contains(aStr, bStr), nil
	default:
		return false, fmt.Errorf("unsupported operator: %s", operator)
	}
}

func (n *ConditionNode) Validate(config map[string]interface{}) error {
	if _, ok := config["variable"].(string); !ok {
		return fmt.Errorf("missing 'variable' in condition config")
	}
	op, ok := config["operator"].(string)
	if !ok {
		return fmt.Errorf("missing 'operator' in condition config")
	}
	if _, err := compare(op, "", ""); err != nil {
		return err
	}
	if _, ok := config["value"]; !ok {
		return fmt.Errorf("missing 'value' in condition config")
	}
	return nil
}
