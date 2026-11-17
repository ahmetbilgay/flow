package n8n

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"
)

// ---------------------------------------------------------------- Move Binary Data

func moveBinaryDataNode(ec *execCtx) ([][]Item, error) {
	mode, _ := ec.str(0, "mode", "binaryToJson")
	var out []Item
	for i, it := range ec.items() {
		res := Item{JSON: map[string]interface{}{}, PairedItem: paired(i)}
		keep, _ := ec.boolean(i, "options.keepSource", false)
		if mode == "jsonToBinary" {
			all, _ := ec.boolean(i, "convertAllData", true)
			var v interface{} = it.JSON
			src, _ := ec.str(i, "sourceKey", "data")
			if !all {
				v, _ = getPath(it.JSON, src, true)
			}
			var data []byte
			if s, ok := v.(string); ok {
				if raw, _ := ec.boolean(i, "options.useRawData", false); raw {
					data = []byte(s)
				}
			}
			if data == nil {
				data, _ = json.Marshal(v)
			}
			if bom, _ := ec.boolean(i, "options.addBOM", false); bom {
				data = append([]byte("\ufeff"), data...)
			}
			mt, _ := ec.str(i, "options.mimeType", "application/json")
			name, _ := ec.str(i, "options.fileName", "")
			if keep {
				res.JSON = cloneJSON(it.JSON)
			} else if !all {
				res.JSON = cloneJSON(it.JSON)
				deletePath(res.JSON, src, true)
			}
			res.Binary = map[string]interface{}{}
			for k, b := range it.Binary {
				res.Binary[k] = b
			}
			dest, _ := ec.str(i, "destinationKey", "data")
			res.Binary[dest] = binaryData(data, mt, name)
		} else {
			src, _ := ec.str(i, "sourceKey", "data")
			data, _, _, err := binaryOf(it, src)
			if err != nil {
				return nil, err
			}
			if strip, _ := ec.boolean(i, "options.stripBOM", true); strip {
				data = []byte(strings.TrimPrefix(string(data), "\ufeff"))
			}
			if !utf8.Valid(data) {
				return nil, permanentError{"Move Binary Data: binary data is not valid UTF-8 text"}
			}
			if all, _ := ec.boolean(i, "setAllData", true); all {
				var m map[string]interface{}
				if err := json.Unmarshal(data, &m); err != nil {
					return nil, permanentError{"Move Binary Data: binary data is not a JSON object"}
				}
				res.JSON = m
			} else {
				res.JSON = cloneJSON(it.JSON)
				var v interface{} = string(data)
				if p, _ := ec.boolean(i, "options.jsonParse", false); p {
					if err := json.Unmarshal(data, &v); err != nil {
						return nil, permanentError{"Move Binary Data: binary data is not valid JSON"}
					}
				}
				dest, _ := ec.str(i, "destinationKey", "data")
				setPath(res.JSON, dest, v, true)
			}
			if keep {
				res.Binary = it.Binary
			} else if len(it.Binary) > 1 {
				res.Binary = map[string]interface{}{}
				for k, b := range it.Binary {
					if k != src {
						res.Binary[k] = b
					}
				}
			}
		}
		out = append(out, res)
	}
	return [][]Item{out}, nil
}

// ---------------------------------------------------------------- Customer Datastore (n8n training)

var trainingPeople = []map[string]interface{}{
	{"id": "23423532", "name": "Jay Gatsby", "email": "gatsby@west-egg.com", "notes": "Keeps asking about a green light??", "country": "US", "created": "1925-04-10"},
	{"id": "23423533", "name": "José Arcadio Buendía", "email": "jab@macondo.co", "notes": "Lots of people named after him. Very confusing", "country": "CO", "created": "1967-05-05"},
	{"id": "23423534", "name": "Max Sendak", "email": "info@in-and-out-of-weeks.org", "notes": "Keeps rolling his terrible eyes", "country": "US", "created": "1963-04-09"},
	{"id": "23423535", "name": "Zaphod Beeblebrox", "email": "captain@heartofgold.com", "notes": "Felt like I was talking to more than one person", "country": nil, "created": "1979-10-12"},
	{"id": "23423536", "name": "Edmund Pevensie", "email": "edmund@narnia.gov", "notes": "Passionate sailor", "country": "UK", "created": "1950-10-16"},
}

func trainingCustomerDatastoreNode(ec *execCtx) ([][]Item, error) {
	op, _ := ec.str(0, "operation", "getOnePerson")
	n := 1
	if op == "getAllPeople" {
		n = len(trainingPeople)
		if all, _ := ec.boolean(0, "returnAll", false); !all {
			l, _ := ec.num(0, "limit", 5)
			if int(l) < n {
				n = int(l)
			}
		}
	}
	var out []Item
	for k := 0; k < n; k++ {
		out = append(out, Item{JSON: cloneJSON(trainingPeople[k])})
	}
	return [][]Item{out}, nil
}

// ---------------------------------------------------------------- Compare Datasets

// compareDatasetsNode matches input A against input B by key fields and
// emits to "In A only", "Same", "Different" and "In B only".
func compareDatasetsNode(ec *execCtx) ([][]Item, error) {
	var a, b []Item
	if len(ec.input.Inputs) > 0 {
		a = ec.input.Inputs[0]
	}
	if len(ec.input.Inputs) > 1 {
		b = ec.input.Inputs[1]
	}
	fv, _ := ec.param(0, "mergeByFields.values", nil)
	var f1, f2 []string
	for _, e := range asList(fv) {
		m := asMap(e)
		f1, f2 = append(f1, toString(m["field1"])), append(f2, toString(m["field2"]))
	}
	if len(f1) == 0 {
		return nil, permanentError{"Compare Datasets: no fields to match on"}
	}
	fuzzy, _ := ec.boolean(0, "fuzzyCompare", false)
	if !fuzzy {
		fuzzy, _ = ec.boolean(0, "options.fuzzyCompare", false)
	}
	norm := func(v interface{}) string {
		if fuzzy {
			return strings.ToLower(strings.TrimSpace(toString(v)))
		}
		return mustJSON(v)
	}
	key := func(it Item, fields []string) string {
		parts := make([]string, len(fields))
		for k, f := range fields {
			v, _ := getPath(it.JSON, f, true)
			parts[k] = norm(v)
		}
		return strings.Join(parts, "\x00")
	}
	skip := map[string]bool{}
	if s, _ := ec.str(0, "options.skipFields", ""); s != "" {
		for _, f := range splitList(s) {
			skip[f] = true
		}
	}
	multiple, _ := ec.str(0, "options.multipleMatches", "first")
	resolve, _ := ec.str(0, "resolve", "includeBoth")
	byKey := map[string][]int{}
	for j, it := range b {
		k := key(it, f2)
		byKey[k] = append(byKey[k], j)
	}
	usedB := map[int]bool{}
	outs := make([][]Item, 4)
	for i, it := range a {
		matches := byKey[key(it, f1)]
		if len(matches) == 0 {
			it.PairedItem = paired(i)
			outs[0] = append(outs[0], it)
			continue
		}
		if multiple != "all" {
			matches = matches[:1]
		}
		for _, j := range matches {
			usedB[j] = true
			bi := b[j]
			same, diff := map[string]interface{}{}, map[string]interface{}{}
			fields := map[string]bool{}
			for k := range it.JSON {
				fields[k] = true
			}
			for k := range bi.JSON {
				fields[k] = true
			}
			for k := range fields {
				if skip[k] {
					continue
				}
				va, vb := it.JSON[k], bi.JSON[k]
				eq := reflect.DeepEqual(va, vb) || (fuzzy && norm(va) == norm(vb))
				if eq {
					same[k] = va
				} else {
					diff[k] = map[string]interface{}{"inputA": va, "inputB": vb}
				}
			}
			if len(diff) == 0 {
				it.PairedItem = paired(i)
				outs[1] = append(outs[1], it)
				continue
			}
			var res Item
			switch resolve {
			case "preferInput1":
				res = it
			case "preferInput2":
				res = bi
			default:
				keys := map[string]interface{}{}
				for _, f := range f1 {
					v, _ := getPath(it.JSON, f, true)
					keys[f] = v
				}
				res = Item{JSON: map[string]interface{}{"keys": keys, "same": same, "different": diff}}
			}
			res.PairedItem = paired(i)
			outs[2] = append(outs[2], res)
		}
	}
	for j, it := range b {
		if !usedB[j] {
			it.PairedItem = map[string]interface{}{"item": j, "input": 1}
			outs[3] = append(outs[3], it)
		}
	}
	return outs, nil
}

// ---------------------------------------------------------------- Debug Helper

func debugHelperNode(ec *execCtx) ([][]Item, error) {
	switch cat, _ := ec.str(0, "category", "doNothing"); cat {
	case "doNothing":
		return passthrough(ec)
	case "throwError":
		msg, _ := ec.str(0, "throwErrorMessage", "Node has thrown an error")
		return nil, permanentError{msg}
	case "oom":
		return nil, permanentError{"Debug Helper: out-of-memory simulation is disabled"}
	case "randomData":
		n, _ := ec.num(0, "randomDataCount", 10)
		if n < 0 || n > 10000 {
			return nil, permanentError{"Debug Helper: random data count must be between 0 and 10000"}
		}
		seed := time.Now().UnixNano()
		if s, _ := ec.str(0, "randomDataSeed", ""); s != "" {
			seed = int64(len(s))
			for _, c := range s {
				seed = seed*31 + int64(c)
			}
		}
		r := rand.New(rand.NewSource(seed)) //nolint:gosec // test data
		typ, _ := ec.str(0, "randomDataType", "user")
		first := []string{"Ada", "Alan", "Grace", "Linus", "Margaret", "Ken", "Barbara", "Dennis", "Radia", "Tim"}
		last := []string{"Lovelace", "Turing", "Hopper", "Torvalds", "Hamilton", "Thompson", "Liskov", "Ritchie", "Perlman", "Berners-Lee"}
		out := make([]Item, int(n))
		for k := range out {
			fn, ln := first[r.Intn(len(first))], last[r.Intn(len(last))]
			email := strings.ToLower(fn+"."+strings.ReplaceAll(ln, "-", "")) + fmt.Sprintf("%d@example.com", r.Intn(1000))
			j := map[string]interface{}{"uid": fmt.Sprintf("%08x-%04x-4%03x-a%03x-%012x", r.Uint32(), r.Intn(65536), r.Intn(4096), r.Intn(4096), r.Int63n(1<<48))}
			switch typ {
			case "email":
				j["email"] = email
			case "uuid":
			default:
				j["email"], j["firstname"], j["lastname"], j["password"] = email, fn, ln, fmt.Sprintf("%x", r.Int63())
			}
			out[k] = Item{JSON: j}
		}
		return [][]Item{out}, nil
	}
	return passthrough(ec)
}

// ---------------------------------------------------------------- AI Transform

// aiTransformNode runs the JavaScript n8n generated from the instructions
// (stored in the node as jsCode) like a Code node in all-items mode.
func aiTransformNode(ec *execCtx) ([][]Item, error) {
	code, _ := ec.str(0, "jsCode", "")
	if strings.TrimSpace(code) == "" {
		return nil, permanentError{"AI Transform: no generated code; generate it in n8n before exporting"}
	}
	return runCode(ec, code, false, "")
}

// ---------------------------------------------------------------- Chat Memory Manager

// ChatMemoryReplacer is an optional extension of ChatMemoryStore needed by
// the Chat Memory Manager's override-insert and delete modes.
type ChatMemoryReplacer interface {
	Replace(ctx context.Context, sessionKey string, msgs []ChatMessage) error
}

func (m *memStore) Replace(_ context.Context, key string, msgs []ChatMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = append([]ChatMessage(nil), msgs...)
	return nil
}

func memoryManagerNode(ec *execCtx) ([][]Item, error) {
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		mem, err := ec.buildMemory(i)
		if err != nil {
			return nil, err
		}
		if mem == nil {
			return nil, permanentError{"Chat Memory Manager: connect a memory sub-node"}
		}
		replace := func(msgs []ChatMessage) error {
			r, ok := mem.store.(ChatMemoryReplacer)
			if !ok {
				return permanentError{"Chat Memory Manager: the configured ChatMemory store does not support replacing messages (implement N8NChatMemoryReplacer)"}
			}
			return r.Replace(ec.ctx, mem.key, msgs)
		}
		switch mode, _ := ec.str(i, "mode", "load"); mode {
		case "load":
			msgs, err := mem.store.Load(ec.ctx, mem.key)
			if err != nil {
				return nil, err
			}
			typ := map[string]string{"user": "human", "assistant": "ai", "system": "system"}
			if simple, _ := ec.boolean(i, "simplifyOutput", true); !simple {
				list := make([]interface{}, len(msgs))
				for k, m := range msgs {
					list[k] = map[string]interface{}{"type": typ[m.Role], "data": map[string]interface{}{"content": m.Content}}
				}
				return one(map[string]interface{}{"messages": list}), nil
			}
			var list []interface{}
			for k := 0; k < len(msgs); k += 2 {
				ex := map[string]interface{}{typ[msgs[k].Role]: msgs[k].Content}
				if k+1 < len(msgs) {
					ex[typ[msgs[k+1].Role]] = msgs[k+1].Content
				}
				list = append(list, ex)
			}
			return one(map[string]interface{}{"messages": list}), nil
		case "insert":
			mv, err := ec.param(i, "messages.messageValues", nil)
			if err != nil {
				return nil, err
			}
			role := map[string]string{"user": "user", "ai": "assistant", "system": "system"}
			var add []ChatMessage
			for _, e := range asList(mv) {
				m := asMap(e)
				r := role[toString(m["type"])]
				if r == "" {
					r = "user"
				}
				add = append(add, ChatMessage{Role: r, Content: toString(m["message"])})
			}
			if im, _ := ec.str(i, "insertMode", "insert"); im == "override" {
				if err := replace(add); err != nil {
					return nil, err
				}
			} else if err := mem.store.Append(ec.ctx, mem.key, add...); err != nil {
				return nil, err
			}
			return one(map[string]interface{}{"success": true}), nil
		case "delete":
			keep := []ChatMessage(nil)
			if dm, _ := ec.str(i, "deleteMode", "lastN"); dm == "lastN" {
				msgs, err := mem.store.Load(ec.ctx, mem.key)
				if err != nil {
					return nil, err
				}
				n, _ := ec.num(i, "lastMessagesCount", 2)
				if int(n) < len(msgs) {
					keep = msgs[:len(msgs)-int(n)]
				}
			}
			if err := replace(keep); err != nil {
				return nil, err
			}
			return one(map[string]interface{}{"success": true}), nil
		default:
			return nil, permanentError{fmt.Sprintf("Chat Memory Manager: mode %q is not supported", mode)}
		}
	})
}

// ---------------------------------------------------------------- Respond to Chat

// chatNode sends a message to the chat session (returned by the chat
// endpoint) and passes its input through.
func chatNode(ec *execCtx) ([][]Item, error) {
	var msgs []interface{}
	for i := range ec.items() {
		m, err := ec.str(i, "message", "")
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	if len(ec.items()) == 0 {
		m, _ := ec.str(0, "message", "")
		msgs = append(msgs, m)
	}
	ec.extra = map[string]interface{}{chatMessagesKey: msgs}
	return passthrough(ec)
}

const chatMessagesKey = "__chatMessages"
