package n8n

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

type sheetsAPI struct {
	ec      *execCtx
	base    string
	headers map[string]string
	titles  map[string]map[string]string // doc -> gid -> title
}

func googleSheetsNode(ec *execCtx) ([][]Item, error) {
	if ec.version < 4 {
		return nil, permanentError{"Google Sheets node v1-v3 is not supported; re-import with v4"}
	}
	h, err := ec.googleAuth("sheets", "googleSheetsOAuth2Api")
	if err != nil {
		return nil, err
	}
	api := &sheetsAPI{ec: ec, base: ec.apiBase("sheets", "https://sheets.googleapis.com"), headers: h, titles: map[string]map[string]string{}}
	resource, _ := ec.str(0, "resource", "sheet")
	op, _ := ec.str(0, "operation", map[string]string{"sheet": "read", "spreadsheet": "create"}[resource])
	switch resource + "." + op {
	case "spreadsheet.create":
		return api.createSpreadsheet()
	case "sheet.read":
		return api.read()
	case "sheet.append", "sheet.update", "sheet.appendOrUpdate", "sheet.upsert":
		return api.write(op)
	case "sheet.delete":
		return api.deleteRows()
	case "sheet.clear":
		return api.clear()
	}
	return nil, permanentError{fmt.Sprintf("Google Sheets %s/%s is not supported", resource, op)}
}

func (s *sheetsAPI) call(method, path string, body interface{}) (map[string]interface{}, error) {
	out, _, err := s.ec.apiCall(method, s.base+path, s.headers, body, "")
	return out, err
}

// target resolves the document ID and the A1-quoted sheet title for item i.
func (s *sheetsAPI) target(i int) (string, string, string, error) {
	doc, err := s.ec.rlString(i, "documentId")
	if err != nil || doc == "" {
		return "", "", "", permanentError{"Google Sheets: document is not set"}
	}
	v, err := s.ec.param(i, "sheetName", "")
	if err != nil {
		return "", "", "", err
	}
	mode, val := "", rlValue(v)
	if m, ok := v.(map[string]interface{}); ok {
		mode = toString(m["mode"])
		if mode == "url" {
			if j := strings.Index(val, "gid="); j >= 0 {
				val = strings.TrimFunc(val[j+4:], func(r rune) bool { return r < '0' || r > '9' })
			}
		}
	}
	val = strings.TrimPrefix(val, "gid=")
	if mode == "name" {
		return doc, val, "", nil
	}
	if _, err := strconv.Atoi(val); err != nil {
		return doc, val, "", nil // a title
	}
	titles, err := s.sheetTitles(doc)
	if err != nil {
		return "", "", "", err
	}
	title, ok := titles[val]
	if !ok {
		return "", "", "", permanentError{fmt.Sprintf("sheet with ID %s not found", val)}
	}
	return doc, title, val, nil
}

func (s *sheetsAPI) sheetTitles(doc string) (map[string]string, error) {
	if t, ok := s.titles[doc]; ok {
		return t, nil
	}
	out, err := s.call("GET", "/v4/spreadsheets/"+url.PathEscape(doc)+"?fields=sheets.properties(sheetId,title)", nil)
	if err != nil {
		return nil, err
	}
	t := map[string]string{}
	for _, raw := range asList(out["sheets"]) {
		sh, _ := raw.(map[string]interface{})
		p, _ := sh["properties"].(map[string]interface{})
		t[toString(p["sheetId"])] = toString(p["title"])
	}
	s.titles[doc] = t
	return t, nil
}

func quoteSheet(title string) string {
	return "'" + strings.ReplaceAll(title, "'", "''") + "'"
}

// values reads all cells of a sheet.
func (s *sheetsAPI) values(doc, title string, i int) ([][]interface{}, error) {
	render, _ := s.ec.str(i, "options.outputFormatting.values.general", "UNFORMATTED_VALUE")
	date, _ := s.ec.str(i, "options.outputFormatting.values.date", "FORMATTED_STRING")
	rng := quoteSheet(title)
	if r, _ := s.ec.str(i, "options.dataLocationOnSheet.values.range", ""); r != "" {
		rng += "!" + r
	}
	q := url.Values{"valueRenderOption": {render}, "dateTimeRenderOption": {date}}
	out, err := s.call("GET", "/v4/spreadsheets/"+url.PathEscape(doc)+"/values/"+url.PathEscape(rng)+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	var rows [][]interface{}
	for _, r := range asList(out["values"]) {
		rows = append(rows, asList(r))
	}
	return rows, nil
}

func rowObject(header []string, row []interface{}, rowNumber int) map[string]interface{} {
	m := map[string]interface{}{"row_number": float64(rowNumber)}
	for c, h := range header {
		if h == "" {
			continue
		}
		if c < len(row) {
			m[h] = row[c]
		} else {
			m[h] = ""
		}
	}
	return m
}

func headerOf(rows [][]interface{}) []string {
	if len(rows) == 0 {
		return nil
	}
	h := make([]string, len(rows[0]))
	for i, v := range rows[0] {
		h[i] = toString(v)
	}
	return h
}

func (s *sheetsAPI) read() ([][]Item, error) {
	var out []Item
	for i := range s.ec.items() {
		doc, title, _, err := s.target(i)
		if err != nil {
			return nil, err
		}
		rows, err := s.values(doc, title, i)
		if err != nil {
			return nil, err
		}
		header := headerOf(rows)
		filters, _ := s.ec.param(i, "filtersUI.values", nil)
		combine, _ := s.ec.str(i, "combineFilters", "AND")
		firstOnly, _ := s.ec.boolean(i, "options.returnFirstMatch", false)
		for r := 1; r < len(rows); r++ {
			obj := rowObject(header, rows[r], r+1)
			if !matchFilters(obj, asList(filters), combine) {
				continue
			}
			out = append(out, Item{JSON: obj, PairedItem: paired(i)})
			if firstOnly {
				break
			}
		}
	}
	return [][]Item{out}, nil
}

func matchFilters(obj map[string]interface{}, filters []interface{}, combine string) bool {
	if len(filters) == 0 {
		return true
	}
	any := false
	for _, raw := range filters {
		f, _ := raw.(map[string]interface{})
		ok := toString(obj[toString(f["lookupColumn"])]) == toString(f["lookupValue"])
		if combine == "OR" && ok {
			return true
		}
		if combine != "OR" && !ok {
			return false
		}
		any = any || ok
	}
	return combine != "OR" || any
}

// write implements append, update and appendOrUpdate.
func (s *sheetsAPI) write(op string) ([][]Item, error) {
	items := s.ec.items()
	if len(items) == 0 {
		return [][]Item{nil}, nil
	}
	doc, title, _, err := s.target(0)
	if err != nil {
		return nil, err
	}
	rows, err := s.values(doc, title, 0)
	if err != nil {
		return nil, err
	}
	header := headerOf(rows)
	mode, _ := s.ec.str(0, "columns.mappingMode", "defineBelow")
	extra, _ := s.ec.str(0, "options.handlingExtraData", "insertInNewColumn")
	matchCol := ""
	if mc, err := s.ec.param(0, "columns.matchingColumns", nil); err == nil {
		if l := asList(mc); len(l) > 0 {
			matchCol = toString(l[0])
		}
	}
	if matchCol == "" {
		matchCol, _ = s.ec.str(0, "columnToMatchOn", "")
	}
	if op != "append" && matchCol == "" {
		return nil, permanentError{"Google Sheets: 'Column to match on' is required"}
	}
	headerChanged := false
	var appendRows [][]interface{}
	type upd struct {
		row  int
		vals []interface{}
	}
	var updates []upd
	var out []Item
	for i, it := range items {
		var data map[string]interface{}
		if mode == "autoMapInputData" {
			data = it.JSON
		} else {
			v, err := s.ec.param(i, "columns.value", map[string]interface{}{})
			if err != nil {
				return nil, err
			}
			data, _ = v.(map[string]interface{})
			if data == nil {
				data = map[string]interface{}{}
			}
		}
		for k := range data {
			if k == "row_number" || indexOf(header, k) >= 0 {
				continue
			}
			switch {
			case len(header) == 0 || extra == "insertInNewColumn":
				header = append(header, k)
				headerChanged = true
			case extra == "error":
				return nil, permanentError{fmt.Sprintf("column %q does not exist in the sheet", k)}
			}
		}
		target := -1
		if op != "append" {
			key := toString(data[matchCol])
			col := indexOf(header, matchCol)
			if col < 0 {
				return nil, permanentError{fmt.Sprintf("column to match on %q not found", matchCol)}
			}
			for r := 1; r < len(rows); r++ {
				if col < len(rows[r]) && toString(rows[r][col]) == key {
					target = r
					break
				}
			}
		}
		if target >= 0 {
			vals := make([]interface{}, len(header))
			for c, h := range header {
				if c < len(rows[target]) {
					vals[c] = rows[target][c]
				}
				if v, ok := data[h]; ok {
					vals[c] = cellValue(v)
				}
			}
			rows[target] = vals
			updates = append(updates, upd{target + 1, vals})
		} else if op != "update" {
			vals := make([]interface{}, len(header))
			for c, h := range header {
				vals[c] = cellValue(data[h])
			}
			appendRows = append(appendRows, vals)
			rows = append(rows, vals)
		} else {
			continue
		}
		out = append(out, Item{JSON: data, PairedItem: paired(i)})
	}
	input, _ := s.ec.str(0, "options.cellFormat", "USER_ENTERED")
	docPath := "/v4/spreadsheets/" + url.PathEscape(doc)
	if headerChanged {
		h := make([]interface{}, len(header))
		for i, v := range header {
			h[i] = v
		}
		if _, err := s.call("PUT", docPath+"/values/"+url.PathEscape(quoteSheet(title)+"!A1")+"?valueInputOption=RAW", map[string]interface{}{"values": [][]interface{}{h}}); err != nil {
			return nil, err
		}
	}
	if len(updates) > 0 {
		var data []interface{}
		for _, u := range updates {
			data = append(data, map[string]interface{}{"range": fmt.Sprintf("%s!A%d", quoteSheet(title), u.row), "values": [][]interface{}{u.vals}})
		}
		if _, err := s.call("POST", docPath+"/values:batchUpdate", map[string]interface{}{"valueInputOption": input, "data": data}); err != nil {
			return nil, err
		}
	}
	if len(appendRows) > 0 {
		q := url.Values{"valueInputOption": {input}, "insertDataOption": {"INSERT_ROWS"}}
		if _, err := s.call("POST", docPath+"/values/"+url.PathEscape(quoteSheet(title))+":append?"+q.Encode(), map[string]interface{}{"values": appendRows}); err != nil {
			return nil, err
		}
	}
	return [][]Item{out}, nil
}

func cellValue(v interface{}) interface{} {
	switch t := v.(type) {
	case nil:
		return ""
	case map[string]interface{}, []interface{}:
		return mustJSON(t)
	}
	return v
}

func indexOf(list []string, s string) int {
	for i, x := range list {
		if x == s {
			return i
		}
	}
	return -1
}

func (s *sheetsAPI) deleteRows() ([][]Item, error) {
	doc, _, gid, err := s.target(0)
	if err != nil {
		return nil, err
	}
	if gid == "" {
		return nil, permanentError{"Google Sheets delete needs the sheet selected by ID"}
	}
	what, _ := s.ec.str(0, "toDelete", "rows")
	start, _ := s.ec.num(0, "startIndex", 2)
	count, _ := s.ec.num(0, "numberToDelete", 1)
	dim := "ROWS"
	if what == "columns" {
		dim = "COLUMNS"
	}
	sid, _ := strconv.Atoi(gid)
	req := map[string]interface{}{"requests": []interface{}{map[string]interface{}{"deleteDimension": map[string]interface{}{"range": map[string]interface{}{
		"sheetId": sid, "dimension": dim, "startIndex": int(start) - 1, "endIndex": int(start) - 1 + int(count)}}}}}
	if _, err := s.call("POST", "/v4/spreadsheets/"+url.PathEscape(doc)+":batchUpdate", req); err != nil {
		return nil, err
	}
	return passthrough(s.ec)
}

func (s *sheetsAPI) clear() ([][]Item, error) {
	doc, title, _, err := s.target(0)
	if err != nil {
		return nil, err
	}
	rng := quoteSheet(title)
	if c, _ := s.ec.str(0, "clear", "wholeSheet"); c == "specificRange" {
		r, _ := s.ec.str(0, "range", "")
		rng += "!" + r
	} else if keep, _ := s.ec.boolean(0, "keepFirstRow", false); keep {
		rng += "!A2:ZZZ"
	}
	if _, err := s.call("POST", "/v4/spreadsheets/"+url.PathEscape(doc)+"/values/"+url.PathEscape(rng)+":clear", map[string]interface{}{}); err != nil {
		return nil, err
	}
	return passthrough(s.ec)
}

func (s *sheetsAPI) createSpreadsheet() ([][]Item, error) {
	var out []Item
	for i := range s.ec.items() {
		title, _ := s.ec.str(i, "title", "Untitled")
		body := map[string]interface{}{"properties": map[string]interface{}{"title": title}}
		if v, _ := s.ec.param(i, "sheetsUi.sheetValues", nil); len(asList(v)) > 0 {
			var sheets []interface{}
			for _, raw := range asList(v) {
				m, _ := raw.(map[string]interface{})
				sheets = append(sheets, map[string]interface{}{"properties": map[string]interface{}{"title": m["title"]}})
			}
			body["sheets"] = sheets
		}
		res, err := s.call("POST", "/v4/spreadsheets", body)
		if err != nil {
			return nil, err
		}
		out = append(out, Item{JSON: res, PairedItem: paired(i)})
	}
	return [][]Item{out}, nil
}
