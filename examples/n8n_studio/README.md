# n8n Studio

A small web UI for trying n8n workflows on the flow engine.

```sh
go run ./examples/n8n_studio            # http://localhost:8080
go run ./examples/n8n_studio -addr localhost:9000
```

- **Load** a workflow: built-in samples, paste or drop an n8n export, or load any
  template from [n8n.io/workflows](https://n8n.io/workflows) by its number.
- **Report**: the workflow is drawn as in n8n; nodes are coloured by support
  (green: supported, amber: supported with notes, red dashed: not supported).
  Blocking issues, notes, triggers and the credentials it needs are listed.
- **Credentials**: enter each credential as JSON with n8n's field names
  (e.g. `openAiApi` → `{"apiKey": "sk-…"}`). They are kept in memory only.
- **Install & Run**: run with the default or a chosen trigger and optional input
  items; every step shows its status, duration and output items, and the graph
  marks what ran.
- **Try it**: chat with Chat Trigger workflows and send requests to webhooks.
- **Executions**: earlier runs of the workflow.

The samples run without credentials: scraping with `require('cheerio')` and an
IF branch, a webhook echo, a chat bot, and an Excel (.xls) round trip.

Everything lives in process memory. The server listens on localhost: it stores
credentials and runs workflows, so do not expose it publicly. Outbound requests
of the workflows follow flow's default SSRF policy (public internet only).
