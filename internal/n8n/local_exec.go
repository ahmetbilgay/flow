package n8n

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

//go:embed python_runner.py
var pythonRunner string

const maxProcessOutput = 10 << 20

func (ec *execCtx) requireLocalProcesses(what string) error {
	if ec.opts == nil || !ec.opts.AllowLocalProcesses {
		return permanentError{what + " starts local processes, which is disabled (set Options.AllowLocalProcesses for trusted workflows)"}
	}
	return nil
}

// limitedBuffer keeps at most max bytes.
type limitedBuffer struct {
	buf bytes.Buffer
	max int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if room := b.max - b.buf.Len(); room > 0 {
		if len(p) > room {
			b.buf.Write(p[:room])
		} else {
			b.buf.Write(p)
		}
	}
	return len(p), nil
}

func (ec *execCtx) workDir() string {
	if ec.opts != nil && ec.opts.FileSystemRoot != "" {
		if root, err := ec.sandboxRoot(); err == nil {
			return root
		}
	}
	return ""
}

// ---------------------------------------------------------------- Execute Command

func executeCommandNode(ec *execCtx) ([][]Item, error) {
	if err := ec.requireLocalProcesses("Execute Command"); err != nil {
		return nil, err
	}
	items := ec.items()
	n := len(items)
	if once, _ := ec.boolean(0, "executeOnce", true); once || n == 0 {
		n = 1
	}
	var out []Item
	for i := 0; i < n; i++ {
		command, err := ec.str(i, "command", "")
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(ec.ctx, ec.opts.codeTimeout()*5)
		shell, flag := "sh", "-c"
		if runtime.GOOS == "windows" {
			shell, flag = "cmd", "/C"
		}
		cmd := exec.CommandContext(ctx, shell, flag, command)
		cmd.Dir = ec.workDir()
		stdout, stderr := &limitedBuffer{max: maxProcessOutput}, &limitedBuffer{max: maxProcessOutput}
		cmd.Stdout, cmd.Stderr = stdout, stderr
		err = cmd.Run()
		cancel()
		code := 0
		var exitErr *exec.ExitError
		switch {
		case errors.As(err, &exitErr):
			code = exitErr.ExitCode()
		case err != nil:
			return nil, fmt.Errorf("execute command: %w", err)
		}
		if code != 0 {
			return nil, fmt.Errorf("command failed with exit code %d: %s", code, strings.TrimSpace(stderr.buf.String()))
		}
		out = append(out, Item{JSON: map[string]interface{}{"exitCode": float64(code), "stdout": strings.TrimRight(stdout.buf.String(), "\n"), "stderr": strings.TrimRight(stderr.buf.String(), "\n")}, PairedItem: paired(i)})
	}
	return [][]Item{out}, nil
}

// ---------------------------------------------------------------- Python Code

// runPython runs a Python Code node on the local interpreter (Options.
// PythonPath, default python3) with n8n's Pyodide-style globals.
func runPython(ec *execCtx, code string, perItem bool) ([][]Item, error) {
	if err := ec.requireLocalProcesses("Python Code"); err != nil {
		return nil, err
	}
	py := "python3"
	if ec.opts.PythonPath != "" {
		py = ec.opts.PythonPath
	}
	items := ec.items()
	plain := make([]interface{}, len(items))
	for i, it := range items {
		m := map[string]interface{}{"json": it.JSON}
		if it.Binary != nil {
			m["binary"] = it.Binary
		}
		plain[i] = m
	}
	nodes := map[string]interface{}{}
	for name, n := range ec.data.byName {
		if v := ec.data.view(n.ID); v.executed {
			var list []interface{}
			for _, it := range v.outputs[0] {
				list = append(list, map[string]interface{}{"json": it.JSON})
			}
			nodes[name] = list
		}
	}
	tmp, err := os.MkdirTemp("", "flow-py-")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	run := func(index int) (interface{}, error) {
		input, _ := json.Marshal(map[string]interface{}{"code": code, "items": plain, "nodes": nodes, "index": index})
		ctx, cancel := context.WithTimeout(ec.ctx, ec.opts.codeTimeout())
		defer cancel()
		cmd := exec.CommandContext(ctx, py, "-c", pythonRunner)
		cmd.Dir = tmp
		cmd.Stdin = bytes.NewReader(input)
		stdout, stderr := &limitedBuffer{max: maxProcessOutput}, &limitedBuffer{max: 64 << 10}
		cmd.Stdout, cmd.Stderr = stdout, stderr
		if err := cmd.Run(); err != nil && stdout.buf.Len() == 0 {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("python code timed out")
			}
			return nil, fmt.Errorf("python: %v: %s", err, strings.TrimSpace(stderr.buf.String()))
		}
		var res struct {
			Result interface{} `json:"result"`
			Error  string      `json:"error"`
		}
		if err := json.Unmarshal(stdout.buf.Bytes(), &res); err != nil {
			return nil, fmt.Errorf("python: invalid result: %v", err)
		}
		if res.Error != "" {
			return nil, permanentError{"python: " + res.Error}
		}
		return res.Result, nil
	}
	if !perItem {
		v, err := run(0)
		if err != nil {
			return nil, err
		}
		out, err := toItemsResult(v, len(items))
		if err != nil {
			return nil, err
		}
		return [][]Item{out}, nil
	}
	var out []Item
	for i := range items {
		v, err := run(i)
		if err != nil {
			return nil, fmt.Errorf("item %d: %w", i, err)
		}
		if v == nil {
			continue
		}
		res, err := toItemsResult(v, 1)
		if err != nil {
			return nil, err
		}
		for _, it := range res {
			it.PairedItem = paired(i)
			out = append(out, it)
		}
	}
	return [][]Item{out}, nil
}

// ---------------------------------------------------------------- Local File Trigger

// localFileTriggerPollCron is how often watched paths are checked (n8n
// watches them continuously).
const localFileTriggerPollCron = "*/10 * * * * *"

const maxWatchedFiles = 10000

type fileState struct {
	Size  int64 `json:"s"`
	Mtime int64 `json:"m"`
	Dir   bool  `json:"d,omitempty"`
}

// localFileTriggerNode reports added, changed and removed files under a
// path inside Options.FileSystemRoot by comparing snapshots between polls.
func localFileTriggerNode(ec *execCtx) ([][]Item, error) {
	p, _ := ec.str(0, "path", "")
	if p == "" {
		return nil, permanentError{"Local File Trigger: path is empty"}
	}
	root, err := ec.sandboxPath(p)
	if err != nil {
		return nil, err
	}
	if err := ec.insideSandbox(root); err != nil {
		return nil, err
	}
	on, _ := ec.str(0, "triggerOn", "file")
	depth := int(toFloatDefault(mustParam(ec, "options.depth", -1.0), -1))
	ignored := map[string]bool{}
	if s, _ := ec.str(0, "options.ignored", ""); s != "" {
		for _, g := range splitList(s) {
			ignored[g] = true
		}
	}
	snap := map[string]fileState{}
	if on == "file" {
		if fi, err := os.Stat(root); err == nil {
			snap[root] = fileState{Size: fi.Size(), Mtime: fi.ModTime().UnixNano()}
		}
	} else {
		base := strings.Count(root, string(os.PathSeparator))
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if path == root {
				return nil
			}
			if d.Type()&fs.ModeSymlink != 0 {
				return nil // never follow links out of the sandbox
			}
			for g := range ignored {
				if ok, _ := filepath.Match(g, d.Name()); ok {
					if d.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
			}
			if depth >= 0 && strings.Count(path, string(os.PathSeparator))-base > depth+1 {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if len(snap) >= maxWatchedFiles {
				return filepath.SkipAll
			}
			fi, err := d.Info()
			if err != nil {
				return nil
			}
			snap[path] = fileState{Size: fi.Size(), Mtime: fi.ModTime().UnixNano(), Dir: d.IsDir()}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	pc, err := ec.openPoll()
	if err != nil {
		return nil, err
	}
	defer pc.close()
	state, _ := json.Marshal(snap)
	if !pc.seen {
		return pc.commit(string(state), nil)
	}
	var prev map[string]fileState
	_ = json.Unmarshal([]byte(pc.prev), &prev)
	events := map[string]bool{}
	if on == "file" {
		events["change"] = true
	} else {
		for _, e := range asList(mustParam(ec, "events", []interface{}{})) {
			events[toString(e)] = true
		}
	}
	sandbox, _ := ec.sandboxRoot()
	rel := func(path string) string {
		return "/" + strings.TrimPrefix(filepath.ToSlash(strings.TrimPrefix(path, sandbox)), "/")
	}
	var out []Item
	emit := func(path, event string) {
		if events[event] {
			out = append(out, Item{JSON: map[string]interface{}{"path": rel(path), "event": event}})
		}
	}
	for path, s := range snap {
		old, ok := prev[path]
		switch {
		case !ok && s.Dir:
			emit(path, "addDir")
		case !ok:
			emit(path, "add")
		case !s.Dir && (old.Size != s.Size || old.Mtime != s.Mtime):
			emit(path, "change")
		}
	}
	for path, s := range prev {
		if _, ok := snap[path]; !ok {
			if s.Dir {
				emit(path, "unlinkDir")
			} else {
				emit(path, "unlink")
			}
		}
	}
	sort.Slice(out, func(a, b int) bool { return toString(out[a].JSON["path"]) < toString(out[b].JSON["path"]) })
	return pc.commit(string(state), out)
}
