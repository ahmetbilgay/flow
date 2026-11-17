package n8n

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// maxPaginationRequests bounds pagination without "Limit Pages Fetched", so
// a completion condition that never becomes true cannot loop forever.
var maxPaginationRequests = 1000

const paginationPath = "options.pagination.pagination"

// paginationConfig returns the HTTP Request node's pagination settings, or
// nil when pagination is off.
func paginationConfig(ec *execCtx) map[string]interface{} {
	raw, ok := ec.raw(paginationPath)
	if !ok {
		return nil
	}
	cfg := asMap(raw)
	if cfg == nil || toString(cfg["paginationMode"]) == "off" {
		return nil
	}
	return cfg
}

// paginateHTTP follows n8n's HTTP Request pagination: the first request is
// sent as configured; after each response the completion condition is
// evaluated with $response, $request (the request just sent) and
// $pageCount (pages fetched so far), and, if not complete, the next request
// is derived from the previous one by setting the configured parameters or
// following the next URL. Items of all pages are concatenated.
func paginateHTTP(ec *execCtx, i int, client *http.Client, limit int64, s *httpRequestSpec, cfg map[string]interface{}) ([]Item, error) {
	mode := toString(cfg["paginationMode"])
	if mode == "" {
		mode = "updateAParameterInEachRequest"
	}
	complete := toString(cfg["paginationCompleteWhen"])
	if complete == "" {
		complete = "responseIsEmpty"
	}
	doneCodes := map[int]bool{}
	if complete == "receiveSpecificStatusCodes" {
		for _, c := range strings.Split(toString(cfg["statusCodesWhenComplete"]), ",") {
			if n, err := strconv.Atoi(strings.TrimSpace(c)); err == nil {
				doneCodes[n] = true
			}
		}
	}
	maxPages := maxPaginationRequests
	if toBool(cfg["limitPagesFetched"]) {
		if n := int(toFloatDefault(cfg["maxRequests"], 100)); n > 0 && n < maxPages {
			maxPages = n
		}
	}
	interval := time.Duration(toFloatDefault(cfg["requestInterval"], 0)) * time.Millisecond

	rt, err := ec.runtime()
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, k := range []string{"$response", "$request", "$pageCount"} {
			_ = rt.setGlobalJSON(k, nil)
		}
	}()

	if s.body != nil && s.bodyBytes == nil {
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(s.body); err != nil {
			return nil, err
		}
		s.bodyBytes = buf.Bytes()
	}
	var out []Item
	for page := 1; ; page++ {
		req := *s
		if len(doneCodes) > 0 {
			req.neverError = true
		}
		resp, body, err := sendHTTP(ec, client, limit, &req)
		if err != nil {
			return nil, fmt.Errorf("page %d: %w", page, err)
		}
		finished := doneCodes[resp.StatusCode]
		if finished && resp.StatusCode >= 300 {
			break // the terminating status is not data
		}
		if resp.StatusCode >= 400 && !s.neverError {
			return nil, fmt.Errorf("page %d: HTTP Request failed with status code %d", page, resp.StatusCode)
		}
		items, err := httpResponseItems(s, resp, body)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
		if finished || page >= maxPages {
			break
		}

		parsed := parsedBody(resp, body)
		headers := map[string]interface{}{}
		for k := range resp.Header {
			headers[strings.ToLower(k)] = resp.Header.Get(k)
		}
		vars := map[string]interface{}{
			"$response":  map[string]interface{}{"body": parsed, "headers": headers, "statusCode": float64(resp.StatusCode), "statusMessage": http.StatusText(resp.StatusCode)},
			"$request":   requestView(s),
			"$pageCount": float64(page),
		}
		for k, v := range vars {
			if err := rt.setGlobalJSON(k, v); err != nil {
				return nil, err
			}
		}
		more := false
		switch complete {
		case "responseIsEmpty":
			switch b := parsed.(type) {
			case []interface{}:
				more = len(b) > 0
			case string:
				more = b != ""
			default:
				more = b != nil
			}
		case "receiveSpecificStatusCodes":
			more = true
		default: // other: completeExpression
			v, err := ec.param(i, paginationPath+".completeExpression", false)
			if err != nil {
				return nil, fmt.Errorf("pagination: %w", err)
			}
			more = !toBool(v)
		}
		if !more {
			break
		}
		if err := nextPageRequest(ec, i, s, mode); err != nil {
			return nil, err
		}
		if interval > 0 {
			t := time.NewTimer(interval)
			select {
			case <-ec.ctx.Done():
				t.Stop()
				return nil, ec.ctx.Err()
			case <-t.C:
			}
		}
	}
	return out, nil
}

// nextPageRequest updates s for the next page (with $response etc. set).
func nextPageRequest(ec *execCtx, i int, s *httpRequestSpec, mode string) error {
	if mode == "responseContainsNextURL" {
		next, err := ec.str(i, paginationPath+".nextURL", "")
		if err != nil {
			return fmt.Errorf("pagination: %w", err)
		}
		if next == "" {
			return permanentError{"pagination: next URL is empty"}
		}
		if u, err := url.Parse(next); err == nil {
			base, _ := url.Parse(s.url)
			if base != nil {
				u = base.ResolveReference(u)
			}
			// Query parameters already in the next URL replace configured ones.
			inURL := u.Query()
			var kept [][2]string
			for _, q := range s.query {
				if _, dup := inURL[q[0]]; !dup {
					kept = append(kept, q)
				}
			}
			s.query, next = kept, u.String()
		}
		s.url = next
		return nil
	}
	v, err := ec.param(i, paginationPath+".parameters.parameters", nil)
	if err != nil {
		return fmt.Errorf("pagination: %w", err)
	}
	for _, e := range asList(v) {
		p := asMap(e)
		name := toString(p["name"])
		if name == "" {
			return permanentError{"pagination: parameter name is empty"}
		}
		val := p["value"]
		switch typ := toString(p["type"]); typ {
		case "", "qs":
			s.query = setKV(s.query, name, toString(val))
		case "headers":
			s.headers = setKV(s.headers, name, toString(val))
		case "body":
			if err := setBodyParam(s, name, val); err != nil {
				return err
			}
		default:
			return permanentError{fmt.Sprintf("pagination: parameter type %q is not supported", typ)}
		}
	}
	return nil
}

func setKV(kv [][2]string, name, value string) [][2]string {
	var out [][2]string
	for _, p := range kv {
		if p[0] != name {
			out = append(out, p)
		}
	}
	return append(out, [2]string{name, value})
}

// setBodyParam sets a field of a JSON or form-urlencoded request body.
func setBodyParam(s *httpRequestSpec, name string, val interface{}) error {
	switch {
	case strings.HasPrefix(s.contentType, "application/x-www-form-urlencoded"):
		form, _ := url.ParseQuery(string(s.bodyBytes))
		form.Set(name, toString(val))
		s.bodyBytes = []byte(form.Encode())
	case s.contentType == "" || strings.Contains(s.contentType, "json"):
		m := map[string]interface{}{}
		if len(bytes.TrimSpace(s.bodyBytes)) > 0 {
			if err := json.Unmarshal(s.bodyBytes, &m); err != nil {
				return permanentError{"pagination: body parameters need a JSON object body"}
			}
		}
		setPath(m, name, val, true)
		s.bodyBytes, _ = json.Marshal(m)
		if s.contentType == "" {
			s.contentType = "application/json"
		}
	default:
		return permanentError{fmt.Sprintf("pagination: body parameters are not supported for %s bodies", s.contentType)}
	}
	return nil
}

// parsedBody decodes a response body for $response.body (JSON when possible).
func parsedBody(resp *http.Response, body []byte) interface{} {
	var v interface{}
	if len(bytes.TrimSpace(body)) > 0 && json.Unmarshal(body, &v) == nil {
		return v
	}
	if strings.Contains(resp.Header.Get("Content-Type"), "json") && len(bytes.TrimSpace(body)) == 0 {
		return map[string]interface{}{}
	}
	return string(body)
}

// requestView is $request: the request just sent. Numeric query values are
// exposed as numbers so "{{ $request.qs.page + 1 }}" increments.
func requestView(s *httpRequestSpec) map[string]interface{} {
	qs := map[string]interface{}{}
	for _, q := range s.query {
		if f, err := strconv.ParseFloat(q[1], 64); err == nil {
			qs[q[0]] = f
		} else {
			qs[q[0]] = q[1]
		}
	}
	headers := map[string]interface{}{}
	for _, h := range s.headers {
		headers[h[0]] = h[1]
	}
	var body interface{}
	if len(s.bodyBytes) > 0 && json.Unmarshal(s.bodyBytes, &body) != nil {
		body = string(s.bodyBytes)
	}
	return map[string]interface{}{"method": strings.ToUpper(s.method), "url": s.url, "uri": s.url, "qs": qs, "headers": headers, "body": body}
}
