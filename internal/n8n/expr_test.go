package n8n

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dgmos/flow/internal/models"
)

// fixture: Trigger -> Set -> (current node "Cur")
func testRuntime(t *testing.T) *Runtime {
	t.Helper()
	wf := &models.Workflow{ID: "wf1", Name: "Test WF", Nodes: []models.Node{
		{ID: "t", Name: "Trigger"}, {ID: "s", Name: "Set"}, {ID: "c", Name: "Cur"},
	}}
	trigOut, _ := encodeOutput([][]Item{{{JSON: map[string]interface{}{"x": 1.0}}, {JSON: map[string]interface{}{"x": 2.0}}}}, nil)
	setIn := `{"__n8n":1,"inputs":[[{"json":{"x":1}},{"json":{"x":2}}]],"sources":[[{"node":"t","port":"0","count":2}]]}`
	// Set reverses the order: item 0 derives from trigger item 1.
	setOut, _ := encodeOutput([][]Item{{
		{JSON: map[string]interface{}{"y": "b"}, PairedItem: paired(1)},
		{JSON: map[string]interface{}{"y": "a"}, PairedItem: paired(0)},
	}}, nil)
	steps := []*models.ExecutionStep{
		{NodeID: "t", Status: models.TaskCompleted, Output: trigOut},
		{NodeID: "s", Status: models.TaskCompleted, Input: setIn, Output: setOut},
	}
	data := newExecData(&models.Execution{ID: "e1"}, wf, steps, &Options{Timezone: "UTC"})
	in := parseNodeInput(`{"__n8n":1,"inputs":[[{"json":{"y":"b","name":"Ada","n":5}},{"json":{"y":"a"}}]],"sources":[[{"node":"s","port":"0","count":2}]]}`)
	rt, err := newRuntime(data, &wf.Nodes[2], in, in.Main())
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func TestExpressions(t *testing.T) {
	rt := testRuntime(t)
	cases := []struct {
		expr string
		want interface{}
	}{
		{"={{ $json.n }}", 5.0},
		{"=Hello {{ $json.name }}!", "Hello Ada!"},
		{"={{ $json.missing }}", nil},
		{"=a{{ $json.missing }}b", "ab"},
		{"=x {{ {a: 1} }}", "x [object Object]"},
		{"={{ {a:{b:1}}.a.b }}", 1.0},
		{"={{ (() => { const x = 1; return x + 1 })() }}", 2.0},
		{"={{ $input.all().length }}", 2.0},
		{"={{ $('Trigger').item.json.x }}", 2.0}, // paired through Set's reordering
		{"={{ $('Trigger').first().json.x }}", 1.0},
		{"={{ $('Set').all().map(i => i.json.y).join(',') }}", "b,a"},
		{"={{ $node['Trigger'].json.x }}", 2.0},
		{"={{ $workflow.name }}", "Test WF"},
		{"={{ $execution.id }}", "e1"},
		{"={{ $prevNode.name }}", "Set"},
		{"={{ 'Hello World'.toSnakeCase() }}", "hello_world"},
		{"={{ 'me@mail.example.com'.extractDomain() }}", "mail.example.com"},
		{"={{ 'https://www.n8n.io/x'.extractDomain() }}", "n8n.io"},
		{"={{ [1,2,2,3].unique().sum() }}", 6.0},
		{"={{ [].isEmpty() && ({}).isEmpty() && 'x'.isNotEmpty() }}", true},
		{"={{ {a:1}.toJsonString() }}", `{"a":1}`},
		{"={{ '{\"a\":2}'.parseJson().a }}", 2.0},
		{"={{ (3.14159).round(2) }}", 3.14},
		{"={{ Buffer.from('hi').toString('base64') }}", "aGk="},
		{"={{ Buffer.from('aGk=', 'base64').toString() }}", "hi"},
		{"={{ 'abc'.hash('sha256').length }}", 64.0},
		{"={{ $ifEmpty('', 'd') + $if(true, 'y', 'n') }}", "dy"},
		{"={{ DateTime.fromISO('2024-01-31T10:00:00Z', {zone: 'UTC'}).plus({months: 1}).toISODate() }}", "2024-02-29"},
		{"={{ DateTime.fromISO('2024-03-05T10:20:30Z', {zone: 'UTC'}).startOf('month').toISO() }}", "2024-03-01T00:00:00.000Z"},
		{"={{ DateTime.fromISO('2024-03-05T10:20:30Z', {zone: 'UTC'}).toFormat('dd LLL yyyy HH:mm EEEE') }}", "05 Mar 2024 10:20 Tuesday"},
		{"={{ DateTime.fromISO('2024-03-05T10:00:00Z').setZone('Europe/Istanbul').hour }}", 13.0},
		{"={{ DateTime.fromFormat('05/03/2024 14:30', 'dd/MM/yyyy HH:mm', {zone: 'UTC'}).toISO() }}", "2024-03-05T14:30:00.000Z"},
		{"={{ DateTime.fromISO('2024-03-05', {zone:'UTC'}).diff(DateTime.fromISO('2024-03-01', {zone:'UTC'}), 'days').days }}", 4.0},
		{"={{ DateTime.fromISO('2024-03-09T12:00:00Z').isWeekend }}", true},
		{"={{ '2024-03-05T00:00:00Z'.toDateTime().year }}", 2024.0},
		{"={{ typeof $now.toISO() }}", "string"},
		{"={{ $now.minus({days: 1}) < $now }}", true},
		{"={{ $today.hour }}", 0.0},
		{"={{ JSON.stringify(DateTime.fromMillis(0, {zone:'UTC'})) }}", `"1970-01-01T00:00:00.000Z"`},
		// Calendar month/year diffs (Luxon semantics, end-of-month clamping).
		{"={{ DateTime.fromISO('2024-02-29', {zone:'UTC'}).diff(DateTime.fromISO('2024-01-31', {zone:'UTC'}), 'months').months }}", 1.0},
		{"={{ DateTime.fromISO('2024-03-15', {zone:'UTC'}).diff(DateTime.fromISO('2024-01-15', {zone:'UTC'}), 'months').months }}", 2.0},
		{"={{ DateTime.fromISO('2024-01-16T12:00:00', {zone:'UTC'}).diff(DateTime.fromISO('2024-01-01', {zone:'UTC'}), 'months').months }}", 0.5},
		{"={{ DateTime.fromISO('2024-03-15', {zone:'UTC'}).diff(DateTime.fromISO('2024-05-15', {zone:'UTC'}), 'months').months }}", -2.0},
		{"={{ DateTime.fromISO('2026-06-01', {zone:'UTC'}).diff(DateTime.fromISO('2024-06-01', {zone:'UTC'}), 'years').years }}", 2.0},
		// Quotes and braces inside regex literals.
		{`={{ "a'b".replace(/'/g, '') }}`, "ab"},
		{`=x{{ "a}b'".replace(/[}']/g, '') }}y{{ 1 + 1 }}`, "xaby2"},
		{`={{ 10 / 2 / 5 }}`, 1.0},
		{"={{ $jmespath($input.all(), '[].json.y').join(',') }}", "b,a"},
		{"={{ $jmespath({a: {b: [1, 2]}}, 'a.b[1]') }}", 2.0},
		{"={{ $jmespath($json, 'missing') }}", nil},
	}
	for _, c := range cases {
		rt.setItem(0)
		got, err := rt.resolve(c.expr)
		if err != nil {
			t.Errorf("%s: %v", c.expr, err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s = %#v, want %#v", c.expr, got, c.want)
		}
	}
}

func TestExpressionErrors(t *testing.T) {
	rt := testRuntime(t)
	for _, e := range []string{"={{ $('Nope').first() }}", "={{ $json.a.b.c }}", "={{ 1 + }}", "={{ require('fs') }}"} {
		if _, err := rt.resolve(e); err == nil {
			t.Errorf("%s: expected error", e)
		}
	}
}

func TestExpressionTimeout(t *testing.T) {
	rt := testRuntime(t)
	rt.timeout = 50 * time.Millisecond
	start := time.Now()
	_, err := rt.resolve("={{ (() => { while(true){} })() }}")
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("want timeout, got %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("timeout not enforced")
	}
	// Runtime stays usable afterwards.
	if v, err := rt.resolve("={{ 1 + 1 }}"); err != nil || v != 2.0 {
		t.Fatalf("runtime unusable after timeout: %v %v", v, err)
	}
}

func TestSplitTemplate(t *testing.T) {
	segs, err := splitTemplate(`a {{ "}}" + '{{' }} b {{ {x:{y:1}} }}`)
	if err != nil {
		t.Fatal(err)
	}
	var exprs []string
	for _, s := range segs {
		if s.expr {
			exprs = append(exprs, strings.TrimSpace(s.text))
		}
	}
	if len(exprs) != 2 || exprs[0] != `"}}" + '{{'` || exprs[1] != "{x:{y:1}}" {
		t.Fatalf("bad split: %#v", exprs)
	}
}

func TestCodeNodeSloppyMode(t *testing.T) {
	rt := testRuntime(t)
	v, err := rt.runJS("var out = []; for (item of $input.all()) { out.push({json: {y: item.json.y}}); } return out;", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	items, err := toItemsResult(v, 2)
	if err != nil || len(items) != 2 || items[0].JSON["y"] != "b" {
		t.Fatalf("got %v %v", items, err)
	}
}

func TestRecursionFailsFast(t *testing.T) {
	rt := testRuntime(t)
	start := time.Now()
	_, err := rt.resolve("={{ (function f(n){ return f(n+1) })(0) }}")
	if err == nil || time.Since(start) > time.Second {
		t.Fatalf("runaway recursion should fail fast: %v after %s", err, time.Since(start))
	}
	if v, err := rt.resolve("={{ (function f(n){ return n <= 1 ? 1 : n * f(n-1) })(20) }}"); err != nil || v != 2432902008176640000.0 {
		t.Fatalf("normal recursion broken: %v %v", v, err)
	}
}
