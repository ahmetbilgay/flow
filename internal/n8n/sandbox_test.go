package n8n

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestJSCannotHangWorker(t *testing.T) {
	cases := []string{
		"={{ ({toString(){ for(;;); }}) + '' }}",
		"={{ /^(a+)+(?=b)$/.test('a'.repeat(40)+'!') }}",
	}
	for _, c := range cases {
		rt := testRuntime(t)
		rt.timeout = 300 * time.Millisecond
		start := time.Now()
		_, err := rt.resolve(c)
		if err == nil || time.Since(start) > 4*time.Second {
			t.Fatalf("%s: err=%v after %s", c, err, time.Since(start))
		}
	}
	rt := testRuntime(t)
	rt.timeout = 300 * time.Millisecond
	start := time.Now()
	_, err := rt.runJS("return [{json:{toJSON(){ for(;;); }}}];", 300*time.Millisecond)
	if err == nil || time.Since(start) > 4*time.Second {
		t.Fatalf("toJSON loop: err=%v after %s", err, time.Since(start))
	}
}

func TestFileSandboxRejectsSymlinks(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	_ = os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("top secret"), 0o644)
	_ = os.WriteFile(filepath.Join(root, "ok.txt"), []byte("fine"), 0o644)
	if err := os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(root, "link.txt")); err != nil {
		t.Skip("symlinks unsupported")
	}
	h := newHarness(t, &Options{FileSystemRoot: root})
	data := wfJSON(t, []map[string]interface{}{nd("Start", "manualTrigger", 1, nil), nd("Read", "readWriteFile", 1, map[string]interface{}{"fileSelector": "*.txt"})}, "Start->Read")
	_, steps := h.run(t, data, "")
	got := itemsOf(t, steps["Read"], 0)
	if len(got) != 1 || got[0]["fileName"] != "ok.txt" {
		t.Fatalf("symlink to a file outside the sandbox must be skipped: %v", got)
	}
}
