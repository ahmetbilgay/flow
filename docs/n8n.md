# n8n Workflows

Flow can import and run [n8n](https://n8n.io) workflow exports. Every n8n node becomes a durable engine step, so imported workflows get flow's retries, leases, concurrency and persistence while keeping n8n's data model (items, paired items, expressions).

Measured on the 299 most-viewed public n8n templates, **73% import as fully runnable** (every node and feature supported; credentials still have to be provided). The importer reports exactly what is missing for the rest.

## Quick start

```go
eng := flow.NewEngine(storage, flow.NewRegistry())

creds := flow.NewN8NStaticCredentials() // or your own vault-backed provider
creds.Set("openAiApi", "*", map[string]interface{}{"apiKey": os.Getenv("OPENAI_API_KEY")})

eng.EnableN8N(flow.N8NOptions{
    Credentials:   creds,
    ResumeBaseURL: "https://app.example.com/n8n", // where N8NHandler is mounted
    ResumeSecret:  os.Getenv("N8N_RESUME_SECRET"),
})

data, _ := os.ReadFile("my-workflow.json") // an n8n export or template JSON
res, err := eng.InstallN8NWorkflow(ctx, "default", data, flow.N8NInstallOptions{})
if errors.Is(err, flow.ErrN8NNotRunnable) {
    for _, b := range res.Report.Blocking {
        log.Printf("unsupported: %s (%s): %s", b.Node, b.Type, b.Message)
    }
}

http.Handle("/n8n/", http.StripPrefix("/n8n", eng.N8NHandler(flow.N8NHandlerOptions{Namespace: "default"})))
go eng.CronManager.Start(ctx) // schedule & polling triggers
go eng.StartWorkerWithOptions(ctx, "default", "worker-1", flow.WorkerOptions{Concurrency: 8})
```

`InstallN8NWorkflow` imports, registers and activates the workflow: Schedule and polling triggers become durable schedules; webhook, chat and form triggers are served by `N8NHandler`. Use `ImportN8NWorkflow` to only inspect a workflow without registering it.

## Compatibility report

```go
wf, report, err := flow.ImportN8NWorkflow(data, flow.N8NImportOptions{ID: "lead-intake"})
```

| Field | Meaning |
|---|---|
| `Runnable` | Every node and feature is supported |
| `Blocking` | Unsupported nodes/features (node, type, message) |
| `Warnings` | Supported with caveats (e.g. in-memory stores, disabled nodes) |
| `Credentials` | Credentials the workflow needs (node, type, name) |
| `Triggers` | How it can be started: `manual`, `schedule` (with cron), `poll`, `webhook` (path, method, response mode), `chat`, `form`, `subworkflow` |

A marketplace should only offer templates whose report is runnable, and ask the user for the listed credentials.

## Starting executions

Like n8n, only one trigger runs per execution. Without a selector the default trigger runs (manual → chat → webhook → form → schedule → polling). Select another trigger and pass its items:

```go
eng.Execute(ctx, "default", "lead-intake", []byte(`{"__trigger": "Schedule Trigger"}`))
eng.Execute(ctx, "default", "lead-intake", []byte(`{"__trigger": "Webhook", "items": [{"body": {"email": "a@b.c"}}]}`))
```

Read results with `flow.N8NOutputItems(execution.Output)`.

## HTTP endpoints (`N8NHandler`)

| Endpoint | Purpose |
|---|---|
| `ANY /webhook/{path}` | Webhook trigger. Response modes: `onReceived`, `lastNode`, `responseNode` (Respond to Webhook). Basic and header auth are enforced. |
| `POST /webhook/{webhookId}` | Telegram / WhatsApp triggers (the request body is the item). Register this URL with the service (e.g. Telegram `setWebhook`). |
| `POST /chat/{workflowId}` | Chat trigger: `{"sessionId": "...", "chatInput": "..."}` → `{"output": "..."}` |
| `POST /form/{workflowId}` | Form trigger. Returns `{"page": {...}, "resumeUrl": "..."}` for the next Form page, or `{"completion": {...}}` |
| `GET/POST /resume/{executionId}?sig=…` | Resumes a waiting execution: `$execution.resumeUrl`, Wait (on webhook), send-and-wait approvals, form pages. URLs are HMAC-signed with `ResumeSecret`. |

## Supported nodes

**Triggers:** Manual, Schedule, Cron, Interval, Webhook, Form, Chat, Execute Workflow Trigger, Error Trigger, Telegram, WhatsApp, Gmail (polling), Google Sheets (polling), Google Drive (polling).

**Flow & data:** IF, Switch, Filter, Merge (append, combine by fields / position / all, choose branch, v1 modes), Loop Over Items, Split In Batches v1/v2 (with `$node["X"].context.noItemsLeft`), Set / Edit Fields (v1–v3.4), Code (JavaScript, both modes), Function, Function Item, Split Out, Aggregate, Summarize, Limit, Sort, Remove Duplicates (incl. items seen in previous executions: keys, incremental, date; node/workflow scope), Item Lists, No Op, Wait (time, date, webhook), Stop and Error, Execute Workflow (database / inline JSON, once / each), Respond to Webhook (JSON, text, binary, redirect), Date & Time, XML, HTML (template, extract, table), HTML Extract, Markdown, Extract from File (PDF, CSV, text, JSON, HTML, base64), Convert to File (CSV, JSON, text, HTML, base64), Spreadsheet File (CSV), Compression (zip/gzip, size-limited), Read PDF, Crypto (hash, HMAC, generate), Execution Data, RSS Read, Edit Image (resize, information), Read/Write Files (sandboxed), Form (pages, completion).

**Integrations:** HTTP Request (v1–v4, all auth types except Digest/OAuth1; pagination by parameter or next URL, completion by empty response, status codes or expression), Telegram, Gmail, Google Sheets, Google Drive (v3, and v1/v2 upload/download/copy/delete/share/folder create), Google Calendar, Google Docs, Slack, Discord, WhatsApp, Notion, Airtable, WordPress, X (Twitter), LinkedIn, Microsoft Outlook, Facebook Graph API, Postgres, MySQL (execute query), Redis, Supabase, Send Email (SMTP), OpenAI (legacy and LangChain node).

**AI:** AI Agent (tools, memory, output parsers), Basic LLM Chain, Question and Answer Chain, Summarization Chain, Text Classifier, Information Extractor, Sentiment Analysis, OpenAI. Chat models: OpenAI, Anthropic, Google Gemini, OpenRouter, DeepSeek, Groq, Mistral, xAI, Ollama. Memory: window buffer (Postgres/Redis/Mongo/… chat memories map to the configured chat memory store). Output parsers: structured, auto-fixing, item list. Tools: Call Workflow, HTTP Request, Code, Calculator, Think, Wikipedia, SerpAPI, vector store tools, and **any supported integration as a tool** (`gmailTool`, `googleSheetsTool`, `telegramTool`, … with `$fromAI()`).

**RAG:** Default Data Loader (JSON, PDF, text, CSV), recursive / token / character text splitters, OpenAI / Gemini / Mistral / Ollama embeddings, vector stores In-Memory, Qdrant, Pinecone and Supabase (insert, load, retrieve, retrieve-as-tool). Data is stored in LangChain's layout, so collections created by n8n remain readable.

**Expressions:** `{{ }}` templates evaluated with a sandboxed JavaScript engine (goja): `$json`, `$input`, `$('Node')` with paired-item tracing, `$node`, `$items`, `$now`, `$today`, `DateTime` (Luxon-compatible), `$workflow`, `$execution` (incl. `resumeUrl`), `$prevNode`, `$vars`, `$env` (opt-in), `$if`, `$ifEmpty`, `$fromAI`, n8n's string/array/number/object helpers (`.first()`, `.toJsonString()`, `.extractDomain()`, `.toDateTime()`, …) and `Buffer`.

Loops: Loop Over Items (v3) and any other cycle (e.g. polling: IF → Wait → HTTP → IF) run as one step; nodes inside are not recorded as separate steps.

Waits longer than 65 seconds do not hold a worker: the step is stored as waiting and resumed by the engine (checked every 30 seconds, survives restarts). "Continue on error" settings keep successful items and turn each failing item into an error item, like n8n.

## Not supported

Python Code nodes, `require()` / `this.helpers` in Code, XLS/XLSX/ODS files, Google Drive v1/v2 list operations, MCP nodes, sub-node memory managers and community nodes. The report lists them as blocking issues.

## Security

Imported workflows are untrusted code:

- **Network:** every outbound connection (HTTP, AI providers, integrations, SMTP, databases, Redis) goes through `N8NOptions.Outbound` — public internet addresses only by default (SSRF protection). Allow internal services explicitly with `AllowedHosts`.
- **Credentials are bound to domains.** A template cannot send your credentials to its own server. Well-known credential types (OpenAI, Anthropic, Google, Slack, Notion, …) are only sent to their service's domain or the credential's own `url`/`baseUrl`. Add `allowedDomains` (comma-separated or a list) to any credential to restrict it explicitly; generic header/query/basic credentials without `allowedDomains` are sent wherever the workflow points. `CredentialRef` carries the workflow ID and node name, so your provider can also refuse credentials per workflow.
- **JavaScript:** expressions (5 s) and Code nodes (60 s) run without `require`, network or filesystem, with a recursion limit and regex match timeouts. A run stuck in native code is abandoned so the worker is freed. CPU time is bounded; **memory is not** (a goja limitation), so run untrusted workflows on isolated worker processes with a memory limit.
- **Environment & files:** `$env` is empty unless `N8NOptions.Env` is set; file nodes are disabled unless `N8NOptions.FileSystemRoot` points to a sandbox directory (symlinks are refused, sizes are capped).
- **Resume links** are HMAC-signed with `ResumeSecret` and bound to the waiting node; approval links also sign the answer. Approvals are recorded with POST behind a confirmation page, so mail scanners that open links cannot answer. Execution-level links (`$execution.resumeUrl`) only resume Wait nodes.
- **HTTP endpoints:** Respond to Webhook cannot set cookies or other origin-wide headers (`Set-Cookie`, HSTS, CORS credentials). Workflows still control the response body, so **mount `N8NHandler` on a separate origin** (e.g. `hooks.example.com`) when installing untrusted workflows. A webhook path can only belong to one workflow (`ErrN8NWebhookConflict`).
- **Secrets in errors:** URLs in error messages are redacted (query strings and Telegram bot tokens).

## Credentials

Implement `flow.N8NCredentialProvider` to resolve n8n credentials from your vault:

```go
type Vault struct{ /* ... */ }

func (v *Vault) Credential(ctx context.Context, namespace, credType string, ref flow.N8NCredentialRef) (map[string]interface{}, error) {
    // return n8n's fields for the type, e.g. openAiApi: {"apiKey": "..."}
}
```

OAuth2 credentials (Google, Slack, Microsoft, X, LinkedIn, …) must return a valid `accessToken` — refreshing tokens is the provider's job. Google service accounts (`googleApi`: `email`, `privateKey`) are exchanged for tokens automatically. A credential may also return `__headers` / `__query` maps that are applied verbatim to HTTP requests.

## Production options

| Option | Default | Purpose |
|---|---|---|
| `ChatMemory` | process memory | AI chat history store |
| `PollState` | process memory | Polling-trigger cursors and dedupe history |
| `WorkflowResolver` | IDs as-is | Map n8n workflow IDs (Execute Workflow, tools) to flow IDs |
| `Timezone` | host zone | Default zone for `$now` / `DateTime` when the workflow has none |
| `APIBaseURLs` | official APIs | Route integrations through a proxy |

Provide durable `ChatMemory` and `PollState` implementations when running several instances. If your `PollState` also implements `N8NPollStateSwapper` (`CompareAndSwapPollState`), cursors advance atomically and overlapping polls never emit the same items twice; otherwise polls are only serialized within one process.

## Measuring compatibility

```bash
N8N_CORPUS_DIR=/path/to/templates go test -tags n8ncorpus -run Corpus -v ./internal/n8n
```

The benchmark imports every JSON file in the directory, checks that runnable workflows register and start, and ranks the remaining blockers by how many templates each would unlock.
