package n8n

import (
	_ "embed"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
	"github.com/andybalholm/cascadia"
	"github.com/dop251/goja"
	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

// Host side of the cheerio subset available to Code nodes
// (require('cheerio')): documents are parsed with x/net/html, queried with
// goquery, and nodes are passed to JavaScript as integer handles. The JS
// wrapper is cheerio.js.

//go:embed cheerio.js
var cheerioSrc string

var (
	cheerioOnce sync.Once
	cheerioProg *goja.Program
	cheerioErr  error
)

// cheerioModule evaluates the JS wrapper on first require('cheerio') and
// returns the same module object afterwards.
func (r *Runtime) cheerioModule() goja.Value {
	if r.chModule != nil {
		return r.chModule
	}
	cheerioOnce.Do(func() { cheerioProg, cheerioErr = goja.Compile("cheerio.js", cheerioSrc, true) })
	if cheerioErr != nil {
		r.throw("cheerio: %v", cheerioErr)
	}
	v, err := r.vm.RunProgram(cheerioProg)
	if err != nil {
		panic(err)
	}
	r.chModule = v
	return v
}

const (
	maxCheerioNodes = 500000
	maxCheerioDoc   = 20 << 20
)

// chHandle returns the handle of n, assigning one on first sight.
func (r *Runtime) chHandle(n *html.Node) int {
	if id, ok := r.chIndex[n]; ok {
		return id
	}
	if len(r.chNodes) >= maxCheerioNodes {
		r.throw("cheerio: too many nodes")
	}
	if r.chIndex == nil {
		r.chIndex = map[*html.Node]int{}
	}
	r.chNodes = append(r.chNodes, n)
	r.chIndex[n] = len(r.chNodes) - 1
	return len(r.chNodes) - 1
}

func (r *Runtime) chNode(id int) *html.Node {
	if id >= 0 && id < len(r.chNodes) {
		return r.chNodes[id]
	}
	return nil
}

func (r *Runtime) chSel(ids []interface{}) *goquery.Selection {
	nodes := make([]*html.Node, 0, len(ids))
	for _, id := range ids {
		f, _ := toFloat(id)
		if n := r.chNode(int(f)); n != nil {
			nodes = append(nodes, n)
		}
	}
	return &goquery.Selection{Nodes: nodes}
}

func (r *Runtime) chIDs(s *goquery.Selection) []interface{} {
	out := make([]interface{}, 0, len(s.Nodes))
	for _, n := range s.Nodes {
		out = append(out, r.chHandle(n))
	}
	return out
}

// chCheck throws a JS error for selectors goquery would silently treat as
// matching nothing; cheerio rejects them.
func (r *Runtime) chCheck(sel string) {
	if _, err := cascadia.ParseGroupWithPseudoElements(sel); err != nil {
		r.throw("cheerio: invalid selector %q: %v", sel, err)
	}
}

func (r *Runtime) chParse(src string, fragment bool) *html.Node {
	if len(src) > maxCheerioDoc {
		r.throw("cheerio: document too large")
	}
	if !fragment {
		doc, err := html.Parse(strings.NewReader(src))
		if err != nil {
			r.throw("cheerio: %v", err)
		}
		return doc
	}
	nodes, err := html.ParseFragment(strings.NewReader(src), &html.Node{Type: html.ElementNode, Data: "body", DataAtom: atom.Body})
	if err != nil {
		r.throw("cheerio: %v", err)
	}
	root := &html.Node{Type: html.DocumentNode}
	for _, n := range nodes {
		root.AppendChild(n)
	}
	return root
}

func chRender(nodes []*html.Node, inner bool) string {
	var b strings.Builder
	for _, n := range nodes {
		if inner || n.Type == html.DocumentNode {
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				_ = html.Render(&b, c)
			}
			continue
		}
		_ = html.Render(&b, n)
	}
	return b.String()
}

func chNodeType(n *html.Node) string {
	switch n.Type {
	case html.ElementNode:
		if n.Data == "script" || n.Data == "style" {
			return n.Data
		}
		return "tag"
	case html.TextNode:
		return "text"
	case html.CommentNode:
		return "comment"
	case html.DocumentNode:
		return "root"
	case html.DoctypeNode:
		return "directive"
	}
	return "unknown"
}

// chDetach unlinks n from its parent (no-op for roots).
func chDetach(n *html.Node) {
	if n.Parent != nil {
		n.Parent.RemoveChild(n)
	}
}

func (r *Runtime) cheerioHost() map[string]interface{} {
	filtered := func(s *goquery.Selection, sel string) *goquery.Selection {
		if sel == "" {
			return s
		}
		r.chCheck(sel)
		return s.Filter(sel)
	}
	return map[string]interface{}{
		"__chModule": r.cheerioModule,
		"__chLoad": func(src string, fragment bool) int {
			return r.chHandle(r.chParse(src, fragment))
		},
		// __chParse parses an HTML fragment and returns its top-level nodes,
		// detached, for $('<li>…</li>') and the insertion methods.
		"__chParse": func(src string) []interface{} {
			root := r.chParse(src, true)
			var out []interface{}
			for c := root.FirstChild; c != nil; {
				next := c.NextSibling
				root.RemoveChild(c)
				out = append(out, r.chHandle(c))
				c = next
			}
			return out
		},
		"__chFind": func(ids []interface{}, sel string) []interface{} {
			r.chCheck(sel)
			return r.chIDs(r.chSel(ids).Find(sel))
		},
		"__chFilter": func(ids []interface{}, sel string) []interface{} {
			r.chCheck(sel)
			return r.chIDs(r.chSel(ids).Filter(sel))
		},
		"__chNot": func(ids []interface{}, sel string) []interface{} {
			r.chCheck(sel)
			return r.chIDs(r.chSel(ids).Not(sel))
		},
		"__chIs": func(ids []interface{}, sel string) bool {
			r.chCheck(sel)
			return r.chSel(ids).Is(sel)
		},
		"__chRel": func(ids []interface{}, rel, sel string) []interface{} {
			s := r.chSel(ids)
			var res *goquery.Selection
			switch rel {
			case "children":
				res = filtered(s.Children(), sel)
			case "contents":
				res = s.Contents()
			case "parent":
				res = filtered(s.Parent(), sel)
			case "parents":
				res = filtered(s.Parents(), sel)
			case "parentsUntil":
				r.chCheck(sel)
				res = s.ParentsUntil(sel)
			case "closest":
				r.chCheck(sel)
				res = s.Closest(sel)
			case "next":
				res = filtered(s.Next(), sel)
			case "prev":
				res = filtered(s.Prev(), sel)
			case "nextAll":
				res = filtered(s.NextAll(), sel)
			case "prevAll":
				res = filtered(s.PrevAll(), sel)
			case "nextUntil":
				r.chCheck(sel)
				res = s.NextUntil(sel)
			case "prevUntil":
				r.chCheck(sel)
				res = s.PrevUntil(sel)
			case "siblings":
				res = filtered(s.Siblings(), sel)
			case "has":
				// Selection.Has needs a goquery Document, which these
				// handle-built selections lack.
				r.chCheck(sel)
				res = &goquery.Selection{}
				for _, n := range s.Nodes {
					if (&goquery.Selection{Nodes: []*html.Node{n}}).Find(sel).Length() > 0 {
						res.Nodes = append(res.Nodes, n)
					}
				}
			default:
				r.throw("cheerio: unknown traversal %q", rel)
			}
			return r.chIDs(res)
		},
		"__chText": func(ids []interface{}) string { return r.chSel(ids).Text() },
		// __chHTML renders the inner HTML of the first node, or the outer
		// HTML of all nodes; null for an empty selection.
		"__chHTML": func(ids []interface{}, outer bool) interface{} {
			s := r.chSel(ids)
			if len(s.Nodes) == 0 {
				return nil
			}
			if outer {
				return chRender(s.Nodes, false)
			}
			return chRender(s.Nodes[:1], true)
		},
		"__chAttrs": func(id int) map[string]interface{} {
			out := map[string]interface{}{}
			if n := r.chNode(id); n != nil {
				for _, a := range n.Attr {
					out[a.Key] = a.Val
				}
			}
			return out
		},
		"__chSetAttr": func(ids []interface{}, name string, val interface{}) {
			name = strings.ToLower(name)
			for _, n := range r.chSel(ids).Nodes {
				if n.Type != html.ElementNode {
					continue
				}
				kept := n.Attr[:0]
				for _, a := range n.Attr {
					if a.Key != name {
						kept = append(kept, a)
					}
				}
				n.Attr = kept
				if val != nil {
					n.Attr = append(n.Attr, html.Attribute{Key: name, Val: toString(val)})
				}
			}
		},
		"__chInfo": func(id int) map[string]interface{} {
			n := r.chNode(id)
			if n == nil {
				return nil
			}
			info := map[string]interface{}{"type": chNodeType(n)}
			switch n.Type {
			case html.ElementNode:
				info["name"] = n.Data
			case html.TextNode, html.CommentNode, html.DoctypeNode:
				info["data"] = n.Data
			}
			return info
		},
		// __chLink returns [parent, prev, next, first, last] handles (-1 for none).
		"__chLink": func(id int) []interface{} {
			n := r.chNode(id)
			if n == nil {
				return []interface{}{-1, -1, -1, -1, -1}
			}
			h := func(m *html.Node) interface{} {
				if m == nil {
					return -1
				}
				return r.chHandle(m)
			}
			return []interface{}{h(n.Parent), h(n.PrevSibling), h(n.NextSibling), h(n.FirstChild), h(n.LastChild)}
		},
		"__chRemove": func(ids []interface{}) {
			for _, n := range r.chSel(ids).Nodes {
				chDetach(n)
			}
		},
		"__chEmpty": func(ids []interface{}) {
			for _, n := range r.chSel(ids).Nodes {
				for n.FirstChild != nil {
					n.RemoveChild(n.FirstChild)
				}
			}
		},
		"__chSetText": func(ids []interface{}, text string) {
			for _, n := range r.chSel(ids).Nodes {
				for n.FirstChild != nil {
					n.RemoveChild(n.FirstChild)
				}
				n.AppendChild(&html.Node{Type: html.TextNode, Data: text})
			}
		},
		// __chInsert moves the given nodes relative to each target: where is
		// append, prepend, before, after or replace. Targets after the first
		// receive deep clones.
		"__chInsert": func(targets []interface{}, where string, content []interface{}) {
			ts := r.chSel(targets).Nodes
			cs := r.chSel(content).Nodes
			for i, t := range ts {
				nodes := cs
				if i < len(ts)-1 {
					nodes = make([]*html.Node, len(cs))
					for k, c := range cs {
						nodes[k] = chClone(c)
					}
				}
				switch where {
				case "append":
					for _, c := range nodes {
						if c == t || chContains(c, t) {
							continue
						}
						chDetach(c)
						t.AppendChild(c)
					}
				case "prepend":
					first := t.FirstChild
					for _, c := range nodes {
						if c == t || chContains(c, t) {
							continue
						}
						chDetach(c)
						if first == nil {
							t.AppendChild(c)
						} else {
							t.InsertBefore(c, first)
						}
					}
				case "before", "after", "replace":
					if t.Parent == nil {
						continue
					}
					anchor := t
					if where == "after" {
						anchor = t.NextSibling
					}
					p := t.Parent
					for _, c := range nodes {
						if c == t || chContains(c, t) {
							continue
						}
						chDetach(c)
						if anchor == nil {
							p.AppendChild(c)
						} else {
							p.InsertBefore(c, anchor)
						}
					}
					if where == "replace" && t.Parent == p {
						p.RemoveChild(t)
					}
				default:
					r.throw("cheerio: unknown insertion %q", where)
				}
			}
		},
		"__chClone": func(ids []interface{}) []interface{} {
			var out []interface{}
			for _, n := range r.chSel(ids).Nodes {
				out = append(out, r.chHandle(chClone(n)))
			}
			return out
		},
	}
}

// chContains reports whether n is an ancestor of m.
func chContains(n, m *html.Node) bool {
	for p := m.Parent; p != nil; p = p.Parent {
		if p == n {
			return true
		}
	}
	return false
}

func chClone(n *html.Node) *html.Node {
	c := &html.Node{Type: n.Type, DataAtom: n.DataAtom, Data: n.Data, Namespace: n.Namespace}
	c.Attr = append([]html.Attribute(nil), n.Attr...)
	for k := n.FirstChild; k != nil; k = k.NextSibling {
		c.AppendChild(chClone(k))
	}
	return c
}
