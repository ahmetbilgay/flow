package node

import (
	"context"
	"fmt"
)

// HumanTaskNode pauses the workflow until a human Claim/Complete (or Advance).
// Config:
//   - assignees []string
//   - candidate_groups []string
//   - claim_mode: "any" | "all" | "first" (default "any")
//   - due_at: RFC3339 string (optional)
//   - priority: number (optional)
//   - execution_mode: typically "interactive" (steps are created WAITING)
//   - timeout: duration string (optional, same as signal)
type HumanTaskNode struct{}

func (h *HumanTaskNode) Execute(ctx context.Context, config map[string]interface{}, input string) (string, error) {
	return "", fmt.Errorf("HUMAN_WAIT")
}

func (h *HumanTaskNode) Validate(config map[string]interface{}) error {
	if config == nil {
		return nil
	}
	if mode, ok := config["claim_mode"]; ok {
		s, ok := mode.(string)
		if !ok {
			return fmt.Errorf("claim_mode must be a string")
		}
		switch s {
		case "", "any", "all", "first":
		default:
			return fmt.Errorf("claim_mode must be any|all|first")
		}
	}
	if t, ok := config["timeout"]; ok {
		if _, ok := t.(string); !ok {
			return fmt.Errorf("timeout must be a duration string, e.g., '1h'")
		}
	}
	return nil
}
