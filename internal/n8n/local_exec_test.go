package n8n

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLocalProcessesDisabledByDefault(t *testing.T) {
	ec := &execCtx{opts: &Options{}}
	for _, what := range []string{"Execute Command", "Python Code"} {
		if err := ec.requireLocalProcesses(what); err == nil || !strings.Contains(err.Error(), "AllowLocalProcesses") {
			t.Fatalf("%s must be disabled by default: %v", what, err)
		}
	}
	if _, err := runPython(&execCtx{opts: &Options{}}, "return []", false); err == nil {
		t.Fatal("python must not run without AllowLocalProcesses")
	}
}

func TestExecuteCommandAndPython(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	h := newHarness(t, &Options{AllowLocalProcesses: true})
	py := `
import micropip
await micropip.install("nothing")
seed = _('Seed').first().json
print("debug output goes to stderr")
total = sum(p.n for p in _input.first().json.points)
return [{"json": {"total": total, "name": seed.name, "plain": _input.first().json.points.to_py()[0]}}]`
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		nd("Seed", "set", 3.4, map[string]interface{}{"mode": "raw", "jsonOutput": `{"name":"Ada","points":[{"n":1},{"n":2},{"n":3}]}`}),
		nd("Cmd", "executeCommand", 1, map[string]interface{}{"command": "=echo {{ $json.name }} && echo oops 1>&2"}),
		nd("Py", "code", 2, map[string]interface{}{"language": "python", "pythonCode": py}),
		nd("PyEach", "code", 2, map[string]interface{}{"language": "python", "mode": "runOnceForEachItem", "pythonCode": `return {"json": {"double": _json["n"] * 2}}`}),
		nd("Split", "splitOut", 1, map[string]interface{}{"fieldToSplitOut": "points"}),
	}, "Start->Seed", "Seed->Cmd", "Seed->Py", "Seed->Split", "Split->PyEach")
	_, steps := h.run(t, data, "")
	if got := itemsOf(t, steps["Cmd"], 0); len(got) != 1 || got[0]["stdout"] != "Ada" || got[0]["stderr"] != "oops" || got[0]["exitCode"] != 0.0 {
		t.Fatalf("execute command: %v (%s)", got, steps["Cmd"].Error)
	}
	if got := itemsOf(t, steps["Py"], 0); len(got) != 1 || got[0]["total"] != 6.0 || got[0]["name"] != "Ada" || asMap(got[0]["plain"])["n"] != 1.0 {
		t.Fatalf("python: %v (%s)", got, steps["Py"].Error)
	}
	if got := itemsOf(t, steps["PyEach"], 0); len(got) != 3 || got[2]["double"] != 6.0 {
		t.Fatalf("python per item: %v (%s)", got, steps["PyEach"].Error)
	}
}

func TestLocalFileTrigger(t *testing.T) {
	root := t.TempDir()
	watch := filepath.Join(root, "inbox")
	if err := os.MkdirAll(watch, 0o755); err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, &Options{FileSystemRoot: root, PollState: &memPollState{m: map[string]string{}}})
	h.fixedID = "files-wf"
	data := wfJSON(t, []map[string]interface{}{
		{"name": "Watch", "type": "n8n-nodes-base.localFileTrigger", "typeVersion": 1, "parameters": map[string]interface{}{"triggerOn": "folder", "path": "/inbox", "events": []interface{}{"add", "change", "unlink"}}},
		nd("Out", "noOp", 1, nil),
	}, "Watch->Out")
	_, rep, _ := Import(data, ImportOptions{})
	if rep.Triggers[0].Kind != "poll" || rep.Triggers[0].Cron[0] != localFileTriggerPollCron {
		t.Fatalf("trigger spec: %+v", rep.Triggers)
	}
	poll := func() []map[string]interface{} {
		_, steps := h.run(t, data, `{"__trigger":"Watch"}`)
		if steps["Out"].Status == "SKIPPED" {
			return nil
		}
		return itemsOf(t, steps["Out"], 0)
	}
	if got := poll(); got != nil {
		t.Fatalf("first poll records the snapshot only: %v", got)
	}
	f := filepath.Join(watch, "a.txt")
	_ = os.WriteFile(f, []byte("1"), 0o644)
	if got := poll(); len(got) != 1 || got[0]["event"] != "add" || got[0]["path"] != "/inbox/a.txt" {
		t.Fatalf("add: %v", got)
	}
	_ = os.WriteFile(f, []byte("12"), 0o644)
	_ = os.Chtimes(f, time.Now().Add(time.Minute), time.Now().Add(time.Minute))
	if got := poll(); len(got) != 1 || got[0]["event"] != "change" {
		t.Fatalf("change: %v", got)
	}
	_ = os.Remove(f)
	if got := poll(); len(got) != 1 || got[0]["event"] != "unlink" {
		t.Fatalf("unlink: %v", got)
	}
}

// A minimal STDIO MCP server in Python, driven by the community node.
const stdioMCPServer = `
import json, sys
for line in sys.stdin:
    m = json.loads(line)
    if "id" not in m:
        continue
    method = m["method"]
    if method == "initialize":
        r = {"protocolVersion": "2025-03-26", "capabilities": {"tools": {}}, "serverInfo": {"name": "py", "version": "1"}}
    elif method == "tools/list":
        r = {"tools": [{"name": "shout", "description": "upper-cases text", "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}}}}]}
    elif method == "tools/call":
        r = {"content": [{"type": "text", "text": m["params"]["arguments"]["text"].upper()}]}
    else:
        r = {}
    sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": m["id"], "result": r}) + "\n")
    sys.stdout.flush()
`

func TestStdioMCPWhenAllowed(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	script := filepath.Join(t.TempDir(), "server.py")
	if err := os.WriteFile(script, []byte(stdioMCPServer), 0o600); err != nil {
		t.Fatal(err)
	}
	creds := NewStaticCredentials()
	creds.Set("mcpClientApi", "*", map[string]interface{}{"command": "python3", "args": script})
	h := newHarness(t, &Options{Credentials: creds, AllowLocalProcesses: true})
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		credNode(map[string]interface{}{"name": "List", "type": "n8n-nodes-mcp.mcpClient", "typeVersion": 1, "parameters": map[string]interface{}{}}, "mcpClientApi"),
		credNode(map[string]interface{}{"name": "Call", "type": "n8n-nodes-mcp.mcpClient", "typeVersion": 1, "parameters": map[string]interface{}{"operation": "executeTool", "toolName": "shout", "toolParameters": `{"text":"hi"}`}}, "mcpClientApi"),
	}, "Start->List", "Start->Call")
	_, steps := h.run(t, data, "")
	if got := itemsOf(t, steps["List"], 0); len(got) != 1 || !strings.Contains(mustJSON(got[0]["tools"]), `"shout"`) {
		t.Fatalf("stdio list: %v (%s)", got, steps["List"].Error)
	}
	if got := itemsOf(t, steps["Call"], 0); len(got) != 1 || !strings.Contains(mustJSON(got[0]["result"]), `"HI"`) {
		t.Fatalf("stdio call: %v (%s)", got, steps["Call"].Error)
	}
}
