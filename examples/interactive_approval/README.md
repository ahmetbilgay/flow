# Interactive Approval Example

Demonstrates DGMOS-ready interactive human tasks:

1. Start a workflow whose first node is `humantask` (created as `WAITING` — no worker needed)
2. `ListTasks` / `CompleteTask` with assignee + action (`approve` / `reject`)
3. Advance subsequent interactive nodes via `Advance`
4. `HostHooks` receives `step.waiting_human` and `task.completed` events

```bash
cd examples/interactive_approval
go run .
```
