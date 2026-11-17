package n8n

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/dlclark/regexp2"
)

// ---------------------------------------------------------------- paths

// splitPath splits "a.b[0].c" / "a['x y']" into keys.
func splitPath(p string) []string {
	var out []string
	var cur strings.Builder
	for i := 0; i < len(p); i++ {
		c := p[i]
		switch c {
		case '.':
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
		case '[':
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
			j := strings.IndexByte(p[i:], ']')
			if j < 0 {
				cur.WriteString(p[i:])
				i = len(p)
				break
			}
			key := strings.Trim(p[i+1:i+j], `'"`)
			out = append(out, key)
			i += j
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

// getPath reads a (possibly dotted) field from an item.
func getPath(m map[string]interface{}, path string, dot bool) (interface{}, bool) {
	if !dot {
		v, ok := m[path]
		return v, ok
	}
	if v, ok := m[path]; ok {
		return v, true
	}
	var cur interface{} = m
	for _, k := range splitPath(path) {
		switch t := cur.(type) {
		case map[string]interface{}:
			v, ok := t[k]
			if !ok {
				return nil, false
			}
			cur = v
		case []interface{}:
			i, err := strconv.Atoi(k)
			if err != nil || i < 0 || i >= len(t) {
				return nil, false
			}
			cur = t[i]
		default:
			return nil, false
		}
	}
	return cur, true
}

// setPath writes a field, creating nested objects for dotted names.
func setPath(m map[string]interface{}, path string, v interface{}, dot bool) {
	if !dot || !strings.ContainsAny(path, ".[") {
		m[path] = v
		return
	}
	keys := splitPath(path)
	cur := m
	for i, k := range keys {
		if i == len(keys)-1 {
			cur[k] = v
			return
		}
		next, ok := cur[k].(map[string]interface{})
		if !ok {
			next = map[string]interface{}{}
			cur[k] = next
		}
		cur = next
	}
}

func deletePath(m map[string]interface{}, path string, dot bool) {
	if _, ok := m[path]; ok || !dot {
		delete(m, path)
		return
	}
	keys := splitPath(path)
	cur := m
	for i, k := range keys {
		if i == len(keys)-1 {
			delete(cur, k)
			return
		}
		next, ok := cur[k].(map[string]interface{})
		if !ok {
			return
		}
		cur = next
	}
}

// ---------------------------------------------------------------- conditions

// condV2 evaluates n8n's filter-parameter conditions (IF v2, Filter v2,
// Switch v3): {combinator, conditions:[{leftValue, rightValue, operator}], options}.
func condV2(c map[string]interface{}, loose bool) (bool, error) {
	conds, _ := c["conditions"].([]interface{})
	comb, _ := c["combinator"].(string)
	opts, _ := c["options"].(map[string]interface{})
	caseSensitive := true
	if v, ok := opts["caseSensitive"].(bool); ok {
		caseSensitive = v
	}
	if tv, _ := opts["typeValidation"].(string); tv == "loose" {
		loose = true
	}
	if len(conds) == 0 {
		return true, nil
	}
	for _, raw := range conds {
		cond, _ := raw.(map[string]interface{})
		ok, err := evalCond(cond, caseSensitive, loose)
		if err != nil {
			return false, err
		}
		if comb == "or" && ok {
			return true, nil
		}
		if comb != "or" && !ok {
			return false, nil
		}
	}
	return comb != "or", nil
}

func evalCond(cond map[string]interface{}, caseSensitive, loose bool) (bool, error) {
	op, _ := cond["operator"].(map[string]interface{})
	typ, _ := op["type"].(string)
	operation, _ := op["operation"].(string)
	left, right := cond["leftValue"], cond["rightValue"]

	switch operation {
	case "exists":
		return left != nil, nil
	case "notExists":
		return left == nil, nil
	case "empty", "isEmpty":
		return isEmptyValue(left), nil
	case "notEmpty", "isNotEmpty":
		return !isEmptyValue(left), nil
	}
	// n8n accepts a missing (null/undefined) left value in every type
	// validation mode; comparisons with it are simply false.
	if left == nil && typ != "string" {
		switch operation {
		case "notEquals", "notContains":
			return true, nil
		}
		return false, nil
	}

	switch typ {
	case "boolean":
		lb, ok := boolOf(left, loose)
		if !ok {
			return false, typeErr(left, "boolean")
		}
		switch operation {
		case "true":
			return lb, nil
		case "false":
			return !lb, nil
		case "equals":
			rb, _ := boolOf(right, true)
			return lb == rb, nil
		case "notEquals":
			rb, _ := boolOf(right, true)
			return lb != rb, nil
		}
	case "number":
		lf, ok := numOf(left, loose)
		if !ok {
			return false, typeErr(left, "number")
		}
		rf, _ := numOf(right, true)
		switch operation {
		case "equals":
			return lf == rf, nil
		case "notEquals":
			return lf != rf, nil
		case "gt":
			return lf > rf, nil
		case "lt":
			return lf < rf, nil
		case "gte":
			return lf >= rf, nil
		case "lte":
			return lf <= rf, nil
		}
	case "dateTime":
		lt, ok1 := timeOf(left)
		rt, ok2 := timeOf(right)
		if !ok1 || !ok2 {
			return false, typeErr(left, "dateTime")
		}
		switch operation {
		case "equals":
			return lt.Equal(rt), nil
		case "notEquals":
			return !lt.Equal(rt), nil
		case "after":
			return lt.After(rt), nil
		case "before":
			return lt.Before(rt), nil
		case "afterOrEquals":
			return !lt.Before(rt), nil
		case "beforeOrEquals":
			return !lt.After(rt), nil
		}
	case "array":
		arr, ok := left.([]interface{})
		if !ok {
			if left == nil && loose {
				arr = nil
			} else if !ok {
				return false, typeErr(left, "array")
			}
		}
		n, _ := numOf(right, true)
		switch operation {
		case "contains":
			return containsValue(arr, right), nil
		case "notContains":
			return !containsValue(arr, right), nil
		case "lengthEquals":
			return float64(len(arr)) == n, nil
		case "lengthNotEquals":
			return float64(len(arr)) != n, nil
		case "lengthGt":
			return float64(len(arr)) > n, nil
		case "lengthLt":
			return float64(len(arr)) < n, nil
		case "lengthGte":
			return float64(len(arr)) >= n, nil
		case "lengthLte":
			return float64(len(arr)) <= n, nil
		}
	case "object":
		// only exists/empty operations, handled above
	default: // string
		if !loose {
			if _, isStr := left.(string); !isStr && left != nil {
				if _, isNum := left.(float64); !isNum {
					return false, typeErr(left, "string")
				}
			}
		}
		ls, rs := toString(left), toString(right)
		if !caseSensitive {
			ls, rs = strings.ToLower(ls), strings.ToLower(rs)
		}
		switch operation {
		case "equals":
			return ls == rs, nil
		case "notEquals":
			return ls != rs, nil
		case "contains":
			return strings.Contains(ls, rs), nil
		case "notContains":
			return !strings.Contains(ls, rs), nil
		case "startsWith":
			return strings.HasPrefix(ls, rs), nil
		case "notStartsWith":
			return !strings.HasPrefix(ls, rs), nil
		case "endsWith":
			return strings.HasSuffix(ls, rs), nil
		case "notEndsWith":
			return !strings.HasSuffix(ls, rs), nil
		case "regex", "notRegex":
			re, err := compileJSRegex(toString(right), !caseSensitive)
			if err != nil {
				return false, err
			}
			m := re.MatchString(toString(left))
			return m == (operation == "regex"), nil
		}
	}
	return false, fmt.Errorf("unsupported condition %s.%s", typ, operation)
}

func typeErr(v interface{}, want string) error {
	return fmt.Errorf("wrong type: '%v' is not a %s (enable 'Convert types where required' / loose type validation)", toString(v), want)
}

// jsRegex is a JavaScript-compatible regular expression.
type jsRegex struct{ re *regexp2.Regexp }

func (r jsRegex) MatchString(s string) bool {
	ok, _ := r.re.MatchString(s)
	return ok
}

// compileJSRegex accepts "/pattern/flags" or a bare pattern, with
// ECMAScript semantics (lookarounds, backreferences, …).
func compileJSRegex(s string, ci bool) (jsRegex, error) {
	opts := regexp2.RegexOptions(regexp2.ECMAScript)
	if len(s) > 1 && s[0] == '/' {
		if j := strings.LastIndex(s, "/"); j > 0 {
			flags := s[j+1:]
			s = s[1:j]
			if strings.Contains(flags, "i") {
				ci = true
			}
			if strings.Contains(flags, "m") {
				opts |= regexp2.Multiline
			}
			if strings.Contains(flags, "s") {
				opts |= regexp2.Singleline
			}
		}
	}
	if ci {
		opts |= regexp2.IgnoreCase
	}
	re, err := regexp2.Compile(s, opts)
	if err != nil {
		return jsRegex{}, fmt.Errorf("invalid regex %q: %w", s, err)
	}
	re.MatchTimeout = time.Second
	return jsRegex{re: re}, nil
}

func isEmptyValue(v interface{}) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		return t == ""
	case []interface{}:
		return len(t) == 0
	case map[string]interface{}:
		return len(t) == 0
	}
	return false
}

func boolOf(v interface{}, loose bool) (bool, bool) {
	switch t := v.(type) {
	case bool:
		return t, true
	case nil:
		return false, loose
	}
	if !loose {
		return false, false
	}
	switch t := v.(type) {
	case string:
		s := strings.ToLower(strings.TrimSpace(t))
		if s == "true" || s == "1" {
			return true, true
		}
		if s == "false" || s == "0" || s == "" {
			return false, true
		}
	case float64:
		return t != 0, true
	}
	return false, false
}

func numOf(v interface{}, loose bool) (float64, bool) {
	if f, ok := v.(float64); ok {
		return f, true
	}
	if !loose {
		return 0, false
	}
	return toFloat(v)
}

func timeOf(v interface{}) (time.Time, bool) {
	switch t := v.(type) {
	case string:
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.000-07:00", "2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006-01-02"} {
			if tm, err := time.Parse(layout, t); err == nil {
				return tm, true
			}
		}
	case float64:
		return time.UnixMilli(int64(t)), true
	}
	return time.Time{}, false
}

func containsValue(arr []interface{}, v interface{}) bool {
	for _, x := range arr {
		if reflect.DeepEqual(x, v) || toString(x) == toString(v) {
			return true
		}
	}
	return false
}

// condV1 evaluates the legacy IF/Filter v1 format:
// {boolean:[{value1,value2,operation}], number:[…], string:[…], dateTime:[…]}.
func condV1(c map[string]interface{}, combine string) (bool, error) {
	var results []bool
	for typ, raw := range c {
		list, _ := raw.([]interface{})
		for _, r := range list {
			cond, _ := r.(map[string]interface{})
			ok, err := evalCondV1(typ, cond)
			if err != nil {
				return false, err
			}
			results = append(results, ok)
		}
	}
	if len(results) == 0 {
		return true, nil
	}
	for _, r := range results {
		if combine == "any" && r {
			return true, nil
		}
		if combine != "any" && !r {
			return false, nil
		}
	}
	return combine != "any", nil
}

func evalCondV1(typ string, c map[string]interface{}) (bool, error) {
	op, _ := c["operation"].(string)
	v1, v2 := c["value1"], c["value2"]
	switch op {
	case "isEmpty":
		return isEmptyValue(v1), nil
	case "isNotEmpty":
		return !isEmptyValue(v1), nil
	}
	switch typ {
	case "number":
		a, _ := toFloat(v1)
		b, _ := toFloat(v2)
		switch op {
		case "smaller":
			return a < b, nil
		case "smallerEqual":
			return a <= b, nil
		case "equal", "":
			return a == b, nil
		case "notEqual":
			return a != b, nil
		case "larger":
			return a > b, nil
		case "largerEqual":
			return a >= b, nil
		}
	case "boolean":
		eq := toBool(v1) == toBool(v2)
		if op == "notEqual" {
			return !eq, nil
		}
		return eq, nil
	case "dateTime":
		a, _ := timeOf(v1)
		b, _ := timeOf(v2)
		switch op {
		case "after":
			return a.After(b), nil
		case "before":
			return a.Before(b), nil
		}
	default:
		a, b := toString(v1), toString(v2)
		switch op {
		case "equal", "":
			return a == b, nil
		case "notEqual":
			return a != b, nil
		case "contains":
			return strings.Contains(a, b), nil
		case "notContains":
			return !strings.Contains(a, b), nil
		case "startsWith":
			return strings.HasPrefix(a, b), nil
		case "notStartsWith":
			return !strings.HasPrefix(a, b), nil
		case "endsWith":
			return strings.HasSuffix(a, b), nil
		case "notEndsWith":
			return !strings.HasSuffix(a, b), nil
		case "regex", "notRegex":
			re, err := compileJSRegex(b, false)
			if err != nil {
				return false, err
			}
			return re.MatchString(a) == (op == "regex"), nil
		}
	}
	return false, fmt.Errorf("unsupported v1 condition %s.%s", typ, op)
}
