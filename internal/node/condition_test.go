package node

import "testing"

func TestConditionCompare(t *testing.T) {
	cases := []struct {
		op, a, b string
		want     bool
	}{
		{"==", "5", "5.0", true},
		{"!=", "5", "5.0", false},
		{"==", "abc", "abc", true},
		{">", "10 apples", "5", false},
		{">", "10", "5", true},
		{"<=", "1e3", "1000", true},
		{"contains", "hello world", "world", true},
	}
	for _, c := range cases {
		got, err := compare(c.op, c.a, c.b)
		if err != nil || got != c.want {
			t.Errorf("compare(%q,%q,%q) = %v,%v want %v", c.op, c.a, c.b, got, err, c.want)
		}
	}
	if err := (&ConditionNode{}).Validate(map[string]interface{}{"variable": "x", "operator": "~=", "value": 1}); err == nil {
		t.Error("unknown operator should fail validation")
	}
}
