package n8n

import (
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"html"
	"io"
	"sort"
	"strings"
	"unicode/utf8"

	md "github.com/JohannesKaufmann/html-to-markdown"
	"github.com/PuerkitoBio/goquery"
	"github.com/ledongthuc/pdf"
	"github.com/yuin/goldmark"
)

// ---------------------------------------------------------------- Extract from File

func extractFromFileNode(ec *execCtx) ([][]Item, error) {
	op, _ := ec.str(0, "operation", "csv")
	switch op {
	case "xls", "xlsx", "ods", "rtf":
		return nil, permanentError{fmt.Sprintf("Extract from File: %s is not supported", op)}
	}
	var out []Item
	for i, it := range ec.items() {
		prop, _ := ec.str(i, "binaryPropertyName", "data")
		data, mt, _, err := binaryOf(it, prop)
		if err != nil {
			return nil, err
		}
		dest, _ := ec.str(i, "options.destinationKey", "data")
		res, err := extractOne(ec, i, op, data, mt, dest)
		if err != nil {
			return nil, fmt.Errorf("item %d: %w", i, err)
		}
		for _, r := range res {
			r.PairedItem = paired(i)
			if keep, _ := ec.str(i, "options.keepSource", "json"); keep == "binary" || keep == "both" {
				r.Binary = it.Binary
			}
			out = append(out, r)
		}
	}
	return [][]Item{out}, nil
}

func extractOne(ec *execCtx, i int, op string, data []byte, mt, dest string) ([]Item, error) {
	text := func() string { return strings.TrimPrefix(string(data), "\ufeff") }
	switch op {
	case "csv":
		delim, _ := ec.str(i, "options.delimiter", ",")
		header, _ := ec.boolean(i, "options.headerRow", true)
		fromLine, _ := ec.num(i, "options.fromLine", 0)
		maxRows, _ := ec.num(i, "options.maxRowCount", -1)
		r := csv.NewReader(strings.NewReader(text()))
		if delim != "" {
			r.Comma, _ = utf8.DecodeRuneInString(delim)
		}
		r.FieldsPerRecord = -1
		r.LazyQuotes, _ = ec.boolean(i, "options.relaxQuotes", false)
		rows, err := r.ReadAll()
		if err != nil {
			return nil, err
		}
		if int(fromLine) > 0 && int(fromLine) <= len(rows) {
			rows = rows[int(fromLine):]
		}
		var out []Item
		var cols []string
		for ri, row := range rows {
			if header && ri == 0 {
				cols = row
				continue
			}
			j := map[string]interface{}{}
			for c, v := range row {
				name := fmt.Sprintf("field_%d", c+1)
				if header && c < len(cols) {
					name = cols[c]
				}
				j[name] = v
			}
			out = append(out, Item{JSON: j})
			if maxRows >= 0 && len(out) >= int(maxRows) {
				break
			}
		}
		return out, nil
	case "pdf":
		join, _ := ec.boolean(i, "options.joinPages", true)
		maxPages, _ := ec.num(i, "options.maxPages", 0)
		pages, info, err := pdfText(data, int(maxPages))
		if err != nil {
			return nil, err
		}
		if !join {
			out := make([]Item, len(pages))
			for p, t := range pages {
				out[p] = Item{JSON: map[string]interface{}{"text": t, "page": float64(p + 1)}}
			}
			return out, nil
		}
		return []Item{{JSON: map[string]interface{}{"numpages": float64(info), "numrender": float64(len(pages)), "info": map[string]interface{}{}, "text": strings.Join(pages, "\n\n")}}}, nil
	case "text", "html", "ics":
		return []Item{{JSON: map[string]interface{}{dest: text()}}}, nil
	case "fromJson":
		var v interface{}
		if err := json.Unmarshal([]byte(text()), &v); err != nil {
			return nil, permanentError{"file is not valid JSON"}
		}
		return []Item{{JSON: map[string]interface{}{dest: v}}}, nil
	case "binaryToPropery", "binaryToProperty":
		enc, _ := ec.str(i, "options.encoding", "")
		if enc != "" && enc != "base64" {
			return []Item{{JSON: map[string]interface{}{dest: text()}}}, nil
		}
		return []Item{{JSON: map[string]interface{}{dest: base64.StdEncoding.EncodeToString(data)}}}, nil
	}
	return nil, permanentError{fmt.Sprintf("Extract from File: %s is not supported", op)}
}

// pdfText extracts plain text per page.
func pdfText(data []byte, maxPages int) (pages []string, total int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("cannot read PDF: %v", r)
		}
	}()
	r, err := pdf.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, 0, permanentError{"cannot read PDF: " + err.Error()}
	}
	total = r.NumPage()
	for p := 1; p <= total; p++ {
		if maxPages > 0 && p > maxPages {
			break
		}
		page := r.Page(p)
		if page.V.IsNull() {
			pages = append(pages, "")
			continue
		}
		rows, err := page.GetTextByRow()
		if err != nil {
			t, _ := page.GetPlainText(nil)
			pages = append(pages, t)
			continue
		}
		var lines []string
		for _, row := range rows {
			var b strings.Builder
			for _, w := range row.Content {
				b.WriteString(w.S)
			}
			lines = append(lines, b.String())
		}
		pages = append(pages, strings.Join(lines, "\n"))
	}
	return pages, total, nil
}

// ---------------------------------------------------------------- Convert to File

func convertToFileNode(ec *execCtx) ([][]Item, error) {
	op, _ := ec.str(0, "operation", "csv")
	items := ec.items()
	prop, _ := ec.str(0, "options.binaryPropertyName", "data")
	allPaired := make([]interface{}, len(items))
	for i := range items {
		allPaired[i] = paired(i)
	}
	single := func(data []byte, mt, def string) ([][]Item, error) {
		name, _ := ec.str(0, "options.fileName", def)
		return [][]Item{{{JSON: map[string]interface{}{}, Binary: map[string]interface{}{prop: binaryData(data, mt, name)}, PairedItem: allPaired}}}, nil
	}
	switch op {
	case "toBinary", "toText", "toJson":
		if op == "toJson" {
			if mode, _ := ec.str(0, "mode", "once"); mode == "once" {
				list := make([]interface{}, len(items))
				for i, it := range items {
					list[i] = it.JSON
				}
				return single(jsonBytes(ec, list), "application/json", "file.json")
			}
		}
		var out []Item
		for i, it := range items {
			var data []byte
			var mt, def string
			switch op {
			case "toBinary":
				src, _ := ec.str(i, "sourceProperty", "data")
				v, _ := getPath(it.JSON, src, true)
				isB64, _ := ec.boolean(i, "options.dataIsBase64", true)
				if isB64 {
					b, err := decodeB64(toString(v))
					if err != nil {
						return nil, fmt.Errorf("item %d: property %q is not valid base64", i, src)
					}
					data = b
				} else {
					data = []byte(toString(v))
				}
				mt, _ = ec.str(i, "options.mimeType", "")
				def = "file"
			case "toText":
				src, _ := ec.str(i, "sourceProperty", "data")
				v, _ := getPath(it.JSON, src, true)
				if s, ok := v.(string); ok {
					data = []byte(s)
				} else {
					data = []byte(mustJSON(v))
				}
				mt, def = "text/plain", "file.txt"
			case "toJson":
				data, mt, def = jsonBytes(ec, it.JSON), "application/json", "file.json"
			}
			name, _ := ec.str(i, "options.fileName", def)
			if mt == "" {
				mt = mimeByExt(name)
				if mt == "application/octet-stream" {
					mt = sniffMime(data)
				}
			}
			out = append(out, Item{JSON: it.JSON, Binary: map[string]interface{}{prop: binaryData(data, mt, name)}, PairedItem: paired(i)})
		}
		return [][]Item{out}, nil
	case "csv":
		delim, _ := ec.str(0, "options.delimiter", ",")
		header, _ := ec.boolean(0, "options.headerRow", true)
		cols := columnsOf(items)
		var buf bytes.Buffer
		w := csv.NewWriter(&buf)
		if delim != "" {
			w.Comma, _ = utf8.DecodeRuneInString(delim)
		}
		if header {
			_ = w.Write(cols)
		}
		for _, it := range items {
			row := make([]string, len(cols))
			for c, k := range cols {
				row[c] = cellString(it.JSON[k])
			}
			_ = w.Write(row)
		}
		w.Flush()
		return single(buf.Bytes(), "text/csv", "data.csv")
	case "html":
		cols := columnsOf(items)
		var b strings.Builder
		b.WriteString("<table>\n<thead><tr>")
		for _, c := range cols {
			b.WriteString("<th>" + html.EscapeString(c) + "</th>")
		}
		b.WriteString("</tr></thead>\n<tbody>\n")
		for _, it := range items {
			b.WriteString("<tr>")
			for _, c := range cols {
				b.WriteString("<td>" + html.EscapeString(cellString(it.JSON[c])) + "</td>")
			}
			b.WriteString("</tr>\n")
		}
		b.WriteString("</tbody>\n</table>")
		return single([]byte(b.String()), "text/html", "data.html")
	}
	return nil, permanentError{fmt.Sprintf("Convert to File: %s is not supported", op)}
}

func jsonBytes(ec *execCtx, v interface{}) []byte {
	if pretty, _ := ec.boolean(0, "options.format", false); pretty {
		b, _ := json.MarshalIndent(v, "", "  ")
		return b
	}
	b, _ := json.Marshal(v)
	return b
}

func columnsOf(items []Item) []string {
	seen := map[string]bool{}
	var cols []string
	for _, it := range items {
		keys := make([]string, 0, len(it.JSON))
		for k := range it.JSON {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if !seen[k] {
				seen[k] = true
				cols = append(cols, k)
			}
		}
	}
	return cols
}

func cellString(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case map[string]interface{}, []interface{}:
		return mustJSON(t)
	}
	return toString(v)
}

func sniffMime(data []byte) string {
	switch {
	case bytes.HasPrefix(data, []byte("%PDF")):
		return "application/pdf"
	case bytes.HasPrefix(data, []byte("\x89PNG")):
		return "image/png"
	case bytes.HasPrefix(data, []byte("\xff\xd8\xff")):
		return "image/jpeg"
	case bytes.HasPrefix(data, []byte("GIF8")):
		return "image/gif"
	case utf8.Valid(data):
		return "text/plain"
	}
	return "application/octet-stream"
}

// ---------------------------------------------------------------- Markdown

func markdownNode(ec *execCtx) ([][]Item, error) {
	var out []Item
	for i, it := range ec.items() {
		mode, _ := ec.str(i, "mode", "htmlToMarkdown")
		dest, _ := ec.str(i, "destinationKey", "data")
		j := cloneJSON(it.JSON)
		if mode == "markdownToHtml" {
			src, err := ec.str(i, "markdown", "")
			if err != nil {
				return nil, err
			}
			var buf bytes.Buffer
			if err := goldmark.Convert([]byte(src), &buf); err != nil {
				return nil, err
			}
			setPath(j, dest, buf.String(), true)
		} else {
			src, err := ec.str(i, "html", "")
			if err != nil {
				return nil, err
			}
			conv := md.NewConverter("", true, &md.Options{HeadingStyle: "atx", CodeBlockStyle: "fenced", BulletListMarker: "*"})
			res, err := conv.ConvertString(src)
			if err != nil {
				return nil, err
			}
			setPath(j, dest, res, true)
		}
		out = append(out, Item{JSON: j, Binary: it.Binary, PairedItem: paired(i)})
	}
	return [][]Item{out}, nil
}

// ---------------------------------------------------------------- HTML

func htmlNode(ec *execCtx) ([][]Item, error) {
	op, _ := ec.str(0, "operation", "generateHtmlTemplate")
	var out []Item
	for i, it := range ec.items() {
		switch op {
		case "generateHtmlTemplate":
			s, err := ec.str(i, "html", "")
			if err != nil {
				return nil, err
			}
			out = append(out, Item{JSON: map[string]interface{}{"html": s}, PairedItem: paired(i)})
		case "extractHtmlContent":
			j, err := extractHTML(ec, i, it)
			if err != nil {
				return nil, fmt.Errorf("item %d: %w", i, err)
			}
			out = append(out, Item{JSON: j, PairedItem: paired(i)})
		case "convertToHtmlTable":
			cols := columnsOf(ec.items())
			var b strings.Builder
			b.WriteString("<table><tr>")
			for _, c := range cols {
				b.WriteString("<th>" + html.EscapeString(c) + "</th>")
			}
			b.WriteString("</tr>")
			for _, row := range ec.items() {
				b.WriteString("<tr>")
				for _, c := range cols {
					b.WriteString("<td>" + html.EscapeString(cellString(row.JSON[c])) + "</td>")
				}
				b.WriteString("</tr>")
			}
			b.WriteString("</table>")
			return [][]Item{{{JSON: map[string]interface{}{"table": b.String()}, PairedItem: paired(0)}}}, nil
		default:
			return nil, permanentError{fmt.Sprintf("HTML: operation %q is not supported", op)}
		}
	}
	return [][]Item{out}, nil
}

func extractHTML(ec *execCtx, i int, it Item) (map[string]interface{}, error) {
	src, _ := ec.str(i, "sourceData", "json")
	prop, _ := ec.str(i, "dataPropertyName", "data")
	var doc string
	if src == "binary" {
		data, _, _, err := binaryOf(it, prop)
		if err != nil {
			return nil, err
		}
		doc = string(data)
	} else {
		v, _ := getPath(it.JSON, prop, true)
		doc = toString(v)
	}
	d, err := goquery.NewDocumentFromReader(strings.NewReader(doc))
	if err != nil {
		return nil, err
	}
	trim, _ := ec.boolean(i, "options.trimValues", true)
	clean, _ := ec.boolean(i, "options.cleanUpText", true)
	vals, err := ec.param(i, "extractionValues.values", nil)
	if err != nil {
		return nil, err
	}
	out := map[string]interface{}{}
	for _, raw := range asList(vals) {
		v, _ := raw.(map[string]interface{})
		sel := d.Find(toString(v["cssSelector"]))
		if skip := toString(v["skipSelectors"]); skip != "" {
			sel.Find(skip).Remove()
		}
		get := func(s *goquery.Selection) interface{} {
			var r string
			switch toString(v["returnValue"]) {
			case "html":
				r, _ = s.Html()
			case "attribute":
				r, _ = s.Attr(toString(v["attribute"]))
			case "value":
				r, _ = s.Attr("value")
			default:
				r = s.Text()
				if clean {
					r = strings.Join(strings.Fields(r), " ")
				}
			}
			if trim {
				r = strings.TrimSpace(r)
			}
			return r
		}
		key := toString(v["key"])
		if toBool(v["returnArray"]) {
			list := []interface{}{}
			sel.Each(func(_ int, s *goquery.Selection) { list = append(list, get(s)) })
			out[key] = list
		} else if sel.Length() > 0 {
			out[key] = get(sel.First())
		} else {
			out[key] = nil
		}
	}
	return out, nil
}

// ---------------------------------------------------------------- XML

func xmlNode(ec *execCtx) ([][]Item, error) {
	var out []Item
	for i, it := range ec.items() {
		mode, _ := ec.str(i, "mode", "xmlToJson")
		prop, _ := ec.str(i, "dataPropertyName", "data")
		if mode == "jsonToxml" {
			root, _ := ec.str(i, "options.rootName", "root")
			headless, _ := ec.boolean(i, "options.headless", false)
			var b strings.Builder
			if !headless {
				b.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` + "\n")
			}
			src := it.JSON
			if len(src) == 1 {
				for k, v := range src {
					root = k
					if m, ok := v.(map[string]interface{}); ok {
						src = m
					}
				}
			}
			writeXML(&b, root, src)
			j := map[string]interface{}{prop: b.String()}
			out = append(out, Item{JSON: j, PairedItem: paired(i)})
			continue
		}
		v, _ := getPath(it.JSON, prop, true)
		parsed, err := xmlToJSON(toString(v), ec, i)
		if err != nil {
			return nil, fmt.Errorf("item %d: %w", i, err)
		}
		out = append(out, Item{JSON: parsed, PairedItem: paired(i)})
	}
	return [][]Item{out}, nil
}

type xmlNodeT struct {
	name     string
	attrs    map[string]interface{}
	text     strings.Builder
	children []*xmlNodeT
}

// xmlToJSON converts XML the way xml2js does with n8n's defaults
// (explicitArray=false, explicitRoot=true, attrkey="$", charkey="_").
func xmlToJSON(s string, ec *execCtx, i int) (map[string]interface{}, error) {
	attrKey, _ := ec.str(i, "options.attrkey", "$")
	charKey, _ := ec.str(i, "options.charkey", "_")
	explicitArray, _ := ec.boolean(i, "options.explicitArray", false)
	explicitRoot, _ := ec.boolean(i, "options.explicitRoot", true)
	ignoreAttrs, _ := ec.boolean(i, "options.ignoreAttrs", false)
	mergeAttrs, _ := ec.boolean(i, "options.mergeAttrs", false)
	trim, _ := ec.boolean(i, "options.trim", false)
	dec := xml.NewDecoder(strings.NewReader(s))
	dec.Strict = false
	root := &xmlNodeT{}
	stack := []*xmlNodeT{root}
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, permanentError{"invalid XML: " + err.Error()}
		}
		cur := stack[len(stack)-1]
		switch t := tok.(type) {
		case xml.StartElement:
			n := &xmlNodeT{name: t.Name.Local, attrs: map[string]interface{}{}}
			if t.Name.Space != "" && !strings.HasPrefix(t.Name.Space, "http") {
				n.name = t.Name.Space + ":" + t.Name.Local
			}
			for _, a := range t.Attr {
				n.attrs[a.Name.Local] = a.Value
			}
			cur.children = append(cur.children, n)
			stack = append(stack, n)
		case xml.EndElement:
			stack = stack[:len(stack)-1]
		case xml.CharData:
			cur.text.Write(t)
		}
	}
	var conv func(n *xmlNodeT) interface{}
	conv = func(n *xmlNodeT) interface{} {
		text := n.text.String()
		if trim || strings.TrimSpace(text) == "" {
			text = strings.TrimSpace(text)
		}
		hasAttrs := len(n.attrs) > 0 && !ignoreAttrs
		if len(n.children) == 0 && !hasAttrs {
			return text
		}
		obj := map[string]interface{}{}
		if hasAttrs {
			if mergeAttrs {
				for k, v := range n.attrs {
					obj[k] = v
				}
			} else {
				obj[attrKey] = n.attrs
			}
		}
		if text != "" {
			obj[charKey] = text
		}
		for _, c := range n.children {
			v := conv(c)
			if prev, ok := obj[c.name]; ok {
				if arr, ok := prev.([]interface{}); ok {
					obj[c.name] = append(arr, v)
				} else {
					obj[c.name] = []interface{}{prev, v}
				}
			} else if explicitArray {
				obj[c.name] = []interface{}{v}
			} else {
				obj[c.name] = v
			}
		}
		return obj
	}
	if len(root.children) == 0 {
		return map[string]interface{}{}, nil
	}
	top := root.children[0]
	if explicitRoot {
		return map[string]interface{}{top.name: conv(top)}, nil
	}
	if m, ok := conv(top).(map[string]interface{}); ok {
		return m, nil
	}
	return map[string]interface{}{top.name: conv(top)}, nil
}

func writeXML(b *strings.Builder, name string, v interface{}) {
	switch t := v.(type) {
	case []interface{}:
		for _, x := range t {
			writeXML(b, name, x)
		}
		return
	case map[string]interface{}:
		b.WriteString("<" + name)
		if attrs, ok := t["$"].(map[string]interface{}); ok {
			keys := make([]string, 0, len(attrs))
			for k := range attrs {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Fprintf(b, ` %s="%s"`, k, html.EscapeString(toString(attrs[k])))
			}
		}
		b.WriteString(">")
		keys := make([]string, 0, len(t))
		for k := range t {
			if k != "$" && k != "_" {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		if s, ok := t["_"]; ok {
			_ = xml.EscapeText(b2w{b}, []byte(toString(s)))
		}
		for _, k := range keys {
			writeXML(b, k, t[k])
		}
		b.WriteString("</" + name + ">")
	default:
		b.WriteString("<" + name + ">")
		_ = xml.EscapeText(b2w{b}, []byte(toString(t)))
		b.WriteString("</" + name + ">")
	}
}

type b2w struct{ b *strings.Builder }

func (w b2w) Write(p []byte) (int, error) { return w.b.Write(p) }
