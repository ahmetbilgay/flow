// Command n8n-templates exports public n8n templates as a JSON catalog a
// backend can load into its template table. Each template is checked with
// flow's importer: by default only templates that import and run are
// written, with the credentials and accounts they need, their triggers, the
// parameters a user has to fill in (setup fields) and the n8n workflow
// itself. Paid templates are always skipped.
//
//	go run ./cmd/n8n-templates -ids 2462,1750 -out templates.json
//	go run ./cmd/n8n-templates -all -min-views 5000 -limit 200 -out templates.json
//	go run ./cmd/n8n-templates -all -category "AI Chatbot" -out ai.json
//	go run ./cmd/n8n-templates -dir ./my-workflows -out local.json   // your own exports
//
// Templates are written by their authors on n8n.io; check n8n's terms and
// the authors' rights before offering them in a product.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/dgmos/flow/internal/n8ntemplates"
)

func main() {
	ids := flag.String("ids", "", "comma-separated template ids")
	dir := flag.String("dir", "", "read n8n workflow exports from this folder instead of the gallery")
	all := flag.Bool("all", false, "export the whole gallery (with the filters below)")
	minViews := flag.Int("min-views", 0, "skip templates with fewer total views (-all)")
	category := flag.String("category", "", "only templates in this category (-all)")
	limit := flag.Int("limit", 0, "stop after this many exported templates (0 = no limit)")
	includeBlocked := flag.Bool("include-blocked", false, "also write templates that do not import and run (runnable=false)")
	out := flag.String("out", "-", "output file (- = stdout)")
	parallel := flag.Int("parallel", 4, "concurrent downloads")
	api := flag.String("api", n8ntemplates.DefaultAPI, "template API base URL")
	flag.Parse()

	if *ids == "" && !*all && *dir == "" {
		fmt.Fprintln(os.Stderr, "usage: n8n-templates -ids 1,2,3 | -all [-min-views N] [-category NAME] | -dir FOLDER  [-limit N] [-out file]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	c := &n8ntemplates.Client{API: *api, HTTP: &http.Client{Timeout: 30 * time.Second}}

	if *dir != "" {
		all, err := n8ntemplates.LoadDir(*dir, time.Now().UTC())
		if err != nil {
			log.Fatal(err)
		}
		res := []n8ntemplates.Template{}
		blocked := 0
		for _, t := range all {
			if !t.Runnable {
				blocked++
				if !*includeBlocked {
					continue
				}
			}
			if *limit > 0 && len(res) >= *limit {
				break
			}
			res = append(res, t)
		}
		write(*out, res)
		log.Printf("written %d of %d workflows in %s (blocked %d)", len(res), len(all), *dir, blocked)
		return
	}

	var list []string
	if *ids != "" {
		for _, s := range strings.Split(*ids, ",") {
			if s = strings.TrimSpace(s); s != "" {
				list = append(list, s)
			}
		}
	} else {
		var err error
		if list, err = c.Search(ctx, *minViews, *category); err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("checking %d templates", len(list))

	res, st := n8ntemplates.Export(ctx, c, list, *parallel, *includeBlocked, *limit)
	write(*out, res)
	log.Printf("written %d (runnable %d, blocked %d, paid skipped %d, failed %d)", len(res), st.Runnable, st.Blocked, st.Paid, st.Failed)
}

func write(out string, v interface{}) {
	w := io.Writer(os.Stdout)
	if out != "-" {
		f, err := os.Create(out)
		if err != nil {
			log.Fatal(err)
		}
		defer func() { _ = f.Close() }()
		w = f
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		log.Fatal(err)
	}
}
