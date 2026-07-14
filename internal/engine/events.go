package engine

import (
	"context"
	"sync"

	"github.com/dgmos/flow/internal/models"
)

// EventType represents different workflow events
type EventType string

const (
	EventTypeWorkflowStarted       EventType = "workflow.started"
	EventTypeWorkflowCompleted     EventType = "workflow.completed"
	EventTypeWorkflowFailed        EventType = "workflow.failed"
	EventTypeWorkflowCancelled     EventType = "workflow.cancelled"
	EventTypeStepStarted           EventType = "step.started"
	EventTypeStepCompleted         EventType = "step.completed"
	EventTypeStepFailed            EventType = "step.failed"
	EventTypeStepRetrying          EventType = "step.retrying"
	EventTypeStepWaitingSignal     EventType = "step.waiting_signal"
	EventTypeStepWaitingHuman      EventType = "step.waiting_human"
	EventTypeSignalReceived        EventType = "signal.received"
	EventTypeTaskClaimed           EventType = "task.claimed"
	EventTypeTaskCompleted         EventType = "task.completed"
	EventTypeCompensationTriggered EventType = "compensation.triggered"
	EventTypeRollbackStarted       EventType = "rollback.started"
)

// AllEventTypes lists every known event type (for global handlers).
func AllEventTypes() []EventType {
	return []EventType{
		EventTypeWorkflowStarted,
		EventTypeWorkflowCompleted,
		EventTypeWorkflowFailed,
		EventTypeWorkflowCancelled,
		EventTypeStepStarted,
		EventTypeStepCompleted,
		EventTypeStepFailed,
		EventTypeStepRetrying,
		EventTypeStepWaitingSignal,
		EventTypeStepWaitingHuman,
		EventTypeSignalReceived,
		EventTypeTaskClaimed,
		EventTypeTaskCompleted,
		EventTypeCompensationTriggered,
		EventTypeRollbackStarted,
	}
}

// Event represents a workflow event
type Event struct {
	Type      EventType
	Namespace string
	Execution *models.Execution
	Step      *models.ExecutionStep
	Workflow  *models.Workflow
	Metadata  map[string]interface{}
}

// EventHandler is the interface for handling workflow events
type EventHandler interface {
	HandleEvent(ctx context.Context, event Event) error
}

// HostHooks is an alias for sync host integration (WS, notifications).
type HostHooks = EventHandler

// EventBus manages event handlers
type EventBus struct {
	mu       sync.RWMutex
	handlers map[EventType][]EventHandler
}

func NewEventBus() *EventBus {
	return &EventBus{
		handlers: make(map[EventType][]EventHandler),
	}
}

// RegisterHandler adds an event handler for specific event type
func (eb *EventBus) RegisterHandler(eventType EventType, handler EventHandler) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.handlers[eventType] = append(eb.handlers[eventType], handler)
}

// RegisterGlobalHandler adds a handler for ALL known event types.
func (eb *EventBus) RegisterGlobalHandler(handler EventHandler) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	for _, eventType := range AllEventTypes() {
		eb.handlers[eventType] = append(eb.handlers[eventType], handler)
	}
}

// Emit sends event to all registered handlers (async, non-blocking)
func (eb *EventBus) Emit(ctx context.Context, event Event) {
	eb.mu.RLock()
	handlersList := eb.handlers[event.Type]
	if len(handlersList) == 0 {
		eb.mu.RUnlock()
		return
	}
	handlers := make([]EventHandler, len(handlersList))
	copy(handlers, handlersList)
	eb.mu.RUnlock()

	for _, handler := range handlers {
		go func(h EventHandler) {
			_ = h.HandleEvent(ctx, event)
		}(handler)
	}
}

// EmitSync sends event synchronously (blocking, waits for all handlers)
func (eb *EventBus) EmitSync(ctx context.Context, event Event) []error {
	eb.mu.RLock()
	handlersList := eb.handlers[event.Type]
	if len(handlersList) == 0 {
		eb.mu.RUnlock()
		return nil
	}
	handlers := make([]EventHandler, len(handlersList))
	copy(handlers, handlersList)
	eb.mu.RUnlock()

	var errors []error
	for _, handler := range handlers {
		if err := handler.HandleEvent(ctx, event); err != nil {
			errors = append(errors, err)
		}
	}
	return errors
}
