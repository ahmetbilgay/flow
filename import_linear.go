package flow

import (
	"fmt"

	"github.com/dgmos/flow/internal/engine"
	"github.com/dgmos/flow/internal/models"
	"github.com/google/uuid"
)

// LegacyStep describes a linear/legacy workflow step for ImportStepChain.
type LegacyStep = models.LegacyStep

// ImportStepChain converts a linear/legacy step list into a Workflow DAG.
func ImportStepChain(id, name string, steps []LegacyStep) (*Workflow, error) {
	if len(steps) == 0 {
		return nil, fmt.Errorf("ImportStepChain: no steps")
	}
	wf := &Workflow{
		ID:      id,
		Name:    name,
		Version: 1,
		Status:  WorkflowActive,
		Nodes:   make([]Node, 0, len(steps)),
		Edges:   make([]Edge, 0),
	}

	seen := map[string]bool{}
	for _, s := range steps {
		if s.ID == "" {
			return nil, fmt.Errorf("ImportStepChain: step missing id")
		}
		if seen[s.ID] {
			return nil, fmt.Errorf("ImportStepChain: duplicate step id %s", s.ID)
		}
		seen[s.ID] = true
		typ := s.Type
		if typ == "" {
			typ = NodeTypeFunction
		}
		cfg := s.Config
		if cfg == nil {
			cfg = map[string]interface{}{}
		}
		n := Node{
			ID:     s.ID,
			Type:   typ,
			Name:   s.Name,
			Config: cfg,
			Meta:   s.Meta,
		}
		if n.Name == "" {
			n.Name = s.ID
		}
		wf.Nodes = append(wf.Nodes, n)
	}

	for _, s := range steps {
		targets := s.Next
		if len(targets) == 0 && s.Conditions != nil {
			for target := range s.Conditions {
				targets = append(targets, target)
			}
		}
		for _, target := range targets {
			if !seen[target] {
				return nil, fmt.Errorf("ImportStepChain: unknown next target %s from %s", target, s.ID)
			}
			cond := ""
			if s.Conditions != nil {
				cond = s.Conditions[target]
			}
			wf.Edges = append(wf.Edges, Edge{
				ID:        uuid.New().String(),
				SourceID:  s.ID,
				TargetID:  target,
				Condition: cond,
			})
		}
	}

	// Validate DAG
	g, err := engine.NewGraph(wf)
	if err != nil {
		return nil, fmt.Errorf("ImportStepChain: invalid graph: %w", err)
	}
	if err := g.Validate(); err != nil {
		return nil, fmt.Errorf("ImportStepChain: invalid graph: %w", err)
	}
	return wf, nil
}
