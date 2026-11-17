// Command n8n runs n8n workflow exports on the flow engine.
//
//	go run ./examples/n8n path/to/workflow.json [more.json ...]
//
// It prints the compatibility report of each file, installs the runnable
// ones and serves their webhook/chat/form endpoints on :8080/n8n/.
// Credentials come from N8N_CRED_<TYPE> environment variables holding JSON,
// e.g. N8N_CRED_OPENAIAPI='{"apiKey":"sk-..."}'.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/dgmos/flow"
)

// envCredentials resolves credentials from N8N_CRED_<TYPE> variables.
type envCredentials struct{}

func (envCredentials) Credential(_ context.Context, _ string, credType string, ref flow.N8NCredentialRef) (map[string]interface{}, error) {
	raw := os.Getenv("N8N_CRED_" + strings.ToUpper(credType))
	if raw == "" {
		return nil, fmt.Errorf("set N8N_CRED_%s to provide credential %q", strings.ToUpper(credType), ref.Name)
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, fmt.Errorf("N8N_CRED_%s is not valid JSON: %w", strings.ToUpper(credType), err)
	}
	return m, nil
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: n8n workflow.json [more.json ...]")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	eng := flow.NewEngine(flow.NewMemoryStorage(), flow.NewRegistry())
	eng.EnableN8N(flow.N8NOptions{
		Credentials:   envCredentials{},
		ResumeBaseURL: "http://localhost:8080/n8n",
		ResumeSecret:  envOr("N8N_RESUME_SECRET", "change-me"),
	})

	for _, path := range os.Args[1:] {
		data, err := os.ReadFile(path)
		if err != nil {
			log.Fatal(err)
		}
		res, err := eng.InstallN8NWorkflow(ctx, "default", data, flow.N8NInstallOptions{})
		if res != nil && res.Report != nil {
			rep := res.Report
			fmt.Printf("%s: %d/%d nodes supported, runnable=%v\n", path, rep.Supported, rep.Nodes, rep.Runnable)
			for _, b := range rep.Blocking {
				fmt.Printf("  ✗ %s: %s\n", b.Node, b.Message)
			}
			for _, w := range rep.Warnings {
				fmt.Printf("  ! %s: %s\n", w.Node, w.Message)
			}
			for _, c := range rep.Credentials {
				fmt.Printf("  🔑 %s needs %s (%s)\n", c.Node, c.Type, c.Name)
			}
			for _, t := range rep.Triggers {
				fmt.Printf("  ▶ %s trigger %q path=%s cron=%v\n", t.Kind, t.Node, t.Path, t.Cron)
			}
		}
		switch {
		case errors.Is(err, flow.ErrN8NNotRunnable):
			continue
		case err != nil:
			log.Fatal(err)
		}
		fmt.Printf("  installed as workflow %q\n", res.Workflow.ID)
	}

	go func() {
		if err := eng.CronManager.Start(ctx); err != nil {
			log.Println("cron:", err)
		}
	}()
	go eng.StartWorkerWithOptions(ctx, "default", "worker-1", flow.WorkerOptions{Concurrency: 8})

	mux := http.NewServeMux()
	mux.Handle("/n8n/", http.StripPrefix("/n8n", eng.N8NHandler(flow.N8NHandlerOptions{Namespace: "default", BaseURL: "http://localhost:8080/n8n"})))
	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	log.Println("listening on http://localhost:8080/n8n/ (webhook/{path}, chat/{workflowId}, form/{workflowId})")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
