package n8n

import (
	"testing"
)

func dedupeWF(t *testing.T, seed string, params map[string]interface{}) []byte {
	params["operation"] = "removeItemsSeenInPreviousExecutions"
	return wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		nd("Seed", "set", 3.4, map[string]interface{}{"mode": "raw", "jsonOutput": `{"rows":` + seed + `}`}),
		nd("Split", "splitOut", 1, map[string]interface{}{"fieldToSplitOut": "rows"}),
		nd("Dedupe", "removeDuplicates", 2, params),
	}, "Start->Seed", "Seed->Split", "Split->Dedupe")
}

func dedupeIDs(t *testing.T, h *harness, data []byte) []interface{} {
	t.Helper()
	_, steps := h.run(t, data, "")
	var ids []interface{}
	for _, it := range itemsOf(t, steps["Dedupe"], 0) {
		ids = append(ids, it["id"])
	}
	return ids
}

func TestDedupeAcrossExecutions(t *testing.T) {
	cases := []struct {
		name   string
		params func() map[string]interface{}
		first  string
		second string
		want1  int
		want2  []interface{}
	}{
		{"seen keys", func() map[string]interface{} {
			return map[string]interface{}{"dedupeValue": "={{ $json.id }}", "options": map[string]interface{}{"scope": "node", "historySize": 10000}}
		}, `[{"id":1},{"id":2},{"id":2}]`, `[{"id":2},{"id":3}]`, 2, []interface{}{3.0}},
		{"history size evicts oldest", func() map[string]interface{} {
			return map[string]interface{}{"dedupeValue": "={{ $json.id }}", "options": map[string]interface{}{"historySize": 1}}
		}, `[{"id":1},{"id":2}]`, `[{"id":1},{"id":2}]`, 2, []interface{}{1.0}},
		{"incremental", func() map[string]interface{} {
			return map[string]interface{}{"logic": "removeItemsUpToStoredIncrementalKey", "incrementalDedupeValue": "={{ $json.id }}"}
		}, `[{"id":5},{"id":3}]`, `[{"id":4},{"id":6}]`, 2, []interface{}{6.0}},
		{"date", func() map[string]interface{} {
			return map[string]interface{}{"logic": "removeItemsUpToStoredDate", "dateDedupeValue": "={{ $json.at }}"}
		}, `[{"id":1,"at":"2024-01-02T00:00:00Z"}]`, `[{"id":2,"at":"2024-01-01T00:00:00Z"},{"id":3,"at":"2024-01-03T00:00:00Z"}]`, 1, []interface{}{3.0}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t, &Options{PollState: &memPollState{m: map[string]string{}}})
			h.fixedID = "wf-dedupe"
			if got := dedupeIDs(t, h, dedupeWF(t, c.first, c.params())); len(got) != c.want1 {
				t.Fatalf("first run: %v, want %d items", got, c.want1)
			}
			got := dedupeIDs(t, h, dedupeWF(t, c.second, c.params()))
			if len(got) != len(c.want2) || (len(got) > 0 && got[0] != c.want2[0]) {
				t.Fatalf("second run: %v, want %v", got, c.want2)
			}
		})
	}
}

func TestDedupeLegacyHistoryFormat(t *testing.T) {
	store := &memPollState{m: map[string]string{"ns/wf-legacy/id-3#dedupe": `["1"]`}}
	h := newHarness(t, &Options{PollState: store})
	h.fixedID = "wf-legacy"
	got := dedupeIDs(t, h, dedupeWF(t, `[{"id":1},{"id":2}]`, map[string]interface{}{"dedupeValue": "={{ $json.id }}"}))
	if len(got) != 1 || got[0] != 2.0 {
		t.Fatalf("legacy history not honored: %v", got)
	}
}
