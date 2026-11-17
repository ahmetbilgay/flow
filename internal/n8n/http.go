package n8n

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/dgmos/flow/internal/node"
)

type httpRequestSpec struct {
	method      string
	url         string
	query       [][2]string
	headers     [][2]string
	body        io.Reader
	bodyBytes   []byte // body, buffered so the request can be re-sent (pagination)
	contentType string
	timeout     time.Duration
	format      string // autodetect | json | text | file
	full        bool
	neverError  bool
	follow      bool
	outProp     string
}

func httpRequestNode(ec *execCtx) ([][]Item, error) {
	client := outboundClient(ec.opts.Outbound)
	limit := ec.opts.Outbound.MaxResponseBytesOrDefault()
	var out []Item
	for i := range ec.items() {
		spec, err := buildHTTPSpec(ec, i)
		if err != nil {
			return nil, fmt.Errorf("item %d: %w", i, err)
		}
		var items []Item
		if pg := paginationConfig(ec); pg != nil {
			items, err = paginateHTTP(ec, i, client, limit, spec, pg)
		} else {
			items, err = doHTTP(ec, client, limit, spec)
		}
		if err != nil {
			return nil, err
		}
		for _, it := range items {
			it.PairedItem = paired(i)
			out = append(out, it)
		}
	}
	return [][]Item{out}, nil
}

func kvList(v interface{}) [][2]string {
	var out [][2]string
	for _, e := range asList(v) {
		m, _ := e.(map[string]interface{})
		if m == nil {
			continue
		}
		name := toString(m["name"])
		if name == "" {
			continue
		}
		out = append(out, [2]string{name, toString(m["value"])})
	}
	return out
}

func jsonKV(v interface{}) ([][2]string, error) {
	var m map[string]interface{}
	switch t := v.(type) {
	case map[string]interface{}:
		m = t
	case string:
		if strings.TrimSpace(t) == "" {
			return nil, nil
		}
		if err := json.Unmarshal([]byte(t), &m); err != nil {
			return nil, fmt.Errorf("invalid JSON: %v", err)
		}
	}
	var out [][2]string
	for k, v := range m {
		out = append(out, [2]string{k, toString(v)})
	}
	return out, nil
}

func buildHTTPSpec(ec *execCtx, i int) (*httpRequestSpec, error) {
	s := &httpRequestSpec{follow: true, format: "autodetect", outProp: "data"}
	var err error
	if ec.version < 2 {
		return buildHTTPSpecV1(ec, i)
	}
	if s.method, err = ec.str(i, "method", "GET"); err != nil {
		return nil, err
	}
	if s.url, err = ec.str(i, "url", ""); err != nil {
		return nil, err
	}
	if q, _ := ec.boolean(i, "sendQuery", false); q {
		if mode, _ := ec.str(i, "specifyQuery", "keypair"); mode == "json" {
			v, err := ec.param(i, "jsonQuery", "{}")
			if err != nil {
				return nil, err
			}
			if s.query, err = jsonKV(v); err != nil {
				return nil, fmt.Errorf("query: %w", err)
			}
		} else {
			v, err := ec.param(i, "queryParameters.parameters", nil)
			if err != nil {
				return nil, err
			}
			s.query = kvList(v)
		}
	}
	if h, _ := ec.boolean(i, "sendHeaders", false); h {
		if mode, _ := ec.str(i, "specifyHeaders", "keypair"); mode == "json" {
			v, err := ec.param(i, "jsonHeaders", "{}")
			if err != nil {
				return nil, err
			}
			if s.headers, err = jsonKV(v); err != nil {
				return nil, fmt.Errorf("headers: %w", err)
			}
		} else {
			v, err := ec.param(i, "headerParameters.parameters", nil)
			if err != nil {
				return nil, err
			}
			s.headers = kvList(v)
		}
	}
	if b, _ := ec.boolean(i, "sendBody", false); b {
		if err := buildHTTPBody(ec, i, s); err != nil {
			return nil, err
		}
	}
	if t, _ := ec.num(i, "options.timeout", 0); t > 0 {
		s.timeout = time.Duration(t) * time.Millisecond
	}
	if f, _ := ec.str(i, "options.response.response.responseFormat", "autodetect"); f != "" {
		s.format = f
	}
	s.full, _ = ec.boolean(i, "options.response.response.fullResponse", false)
	s.neverError, _ = ec.boolean(i, "options.response.response.neverError", false)
	if p, _ := ec.str(i, "options.response.response.outputPropertyName", "data"); p != "" {
		s.outProp = p
	}
	if v, ok := ec.raw("options.redirect.redirect.followRedirects"); ok {
		s.follow = toBool(v)
	}
	return s, nil
}

func buildHTTPBody(ec *execCtx, i int, s *httpRequestSpec) error {
	ct, _ := ec.str(i, "contentType", "json")
	specify, _ := ec.str(i, "specifyBody", "keypair")
	switch ct {
	case "json":
		var payload interface{}
		if specify == "json" {
			v, err := ec.param(i, "jsonBody", "")
			if err != nil {
				return err
			}
			if str, ok := v.(string); ok {
				str = strings.TrimSpace(str)
				if str != "" && json.Unmarshal([]byte(str), &payload) != nil {
					return fmt.Errorf("JSON parameter needs to be valid JSON")
				}
			} else {
				payload = v
			}
		} else {
			v, err := ec.param(i, "bodyParameters.parameters", nil)
			if err != nil {
				return err
			}
			m := map[string]interface{}{}
			for _, e := range asList(v) {
				p, _ := e.(map[string]interface{})
				if name := toString(p["name"]); name != "" {
					m[name] = p["value"]
				}
			}
			payload = m
		}
		b, _ := json.Marshal(payload)
		s.body, s.contentType = bytes.NewReader(b), "application/json"
	case "form-urlencoded":
		var kv [][2]string
		if specify == "string" {
			v, _ := ec.str(i, "body", "")
			s.body, s.contentType = strings.NewReader(v), "application/x-www-form-urlencoded"
			return nil
		}
		v, err := ec.param(i, "bodyParameters.parameters", nil)
		if err != nil {
			return err
		}
		kv = kvList(v)
		form := url.Values{}
		for _, p := range kv {
			form.Add(p[0], p[1])
		}
		s.body, s.contentType = strings.NewReader(form.Encode()), "application/x-www-form-urlencoded"
	case "multipart-form-data":
		v, err := ec.param(i, "bodyParameters.parameters", nil)
		if err != nil {
			return err
		}
		var buf bytes.Buffer
		w := multipart.NewWriter(&buf)
		item := ec.items()[i]
		for _, e := range asList(v) {
			p, _ := e.(map[string]interface{})
			name := toString(p["name"])
			if toString(p["parameterType"]) == "formBinaryData" {
				field := toString(p["inputDataFieldName"])
				data, mimeType, fileName, err := binaryOf(item, field)
				if err != nil {
					return err
				}
				h := make(textproto.MIMEHeader)
				h.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q; filename=%q`, name, fileName))
				h.Set("Content-Type", mimeType)
				part, _ := w.CreatePart(h)
				_, _ = part.Write(data)
				continue
			}
			_ = w.WriteField(name, toString(p["value"]))
		}
		_ = w.Close()
		s.body, s.contentType = &buf, w.FormDataContentType()
	case "raw":
		v, _ := ec.str(i, "body", "")
		rct, _ := ec.str(i, "rawContentType", "text/plain")
		s.body, s.contentType = strings.NewReader(v), rct
	case "binaryData":
		field, _ := ec.str(i, "inputDataFieldName", "data")
		data, mimeType, _, err := binaryOf(ec.items()[i], field)
		if err != nil {
			return err
		}
		s.body, s.contentType = bytes.NewReader(data), mimeType
	default:
		return fmt.Errorf("unsupported body content type %q", ct)
	}
	return nil
}

func buildHTTPSpecV1(ec *execCtx, i int) (*httpRequestSpec, error) {
	s := &httpRequestSpec{follow: true, format: "autodetect", outProp: "data"}
	s.method, _ = ec.str(i, "requestMethod", "GET")
	var err error
	if s.url, err = ec.str(i, "url", ""); err != nil {
		return nil, err
	}
	rf, _ := ec.str(i, "responseFormat", "json")
	s.format = map[string]string{"json": "json", "string": "text", "file": "file"}[rf]
	if p, _ := ec.str(i, "dataPropertyName", "data"); p != "" {
		s.outProp = p
	}
	s.full, _ = ec.boolean(i, "options.fullResponse", false)
	if t, _ := ec.num(i, "options.timeout", 0); t > 0 {
		s.timeout = time.Duration(t) * time.Millisecond
	}
	if jp, _ := ec.boolean(i, "jsonParameters", false); jp {
		for _, f := range []struct {
			key string
			dst *[][2]string
		}{{"queryParametersJson", &s.query}, {"headerParametersJson", &s.headers}} {
			v, err := ec.param(i, f.key, "")
			if err != nil {
				return nil, err
			}
			if *f.dst, err = jsonKV(v); err != nil {
				return nil, err
			}
		}
		if v, err := ec.param(i, "bodyParametersJson", ""); err == nil && toString(v) != "" {
			b := []byte(toString(v))
			if m, ok := v.(map[string]interface{}); ok {
				b, _ = json.Marshal(m)
			}
			s.body, s.contentType = bytes.NewReader(b), "application/json"
		}
	} else {
		q, _ := ec.param(i, "queryParametersUi.parameter", nil)
		s.query = kvList(q)
		h, _ := ec.param(i, "headerParametersUi.parameter", nil)
		s.headers = kvList(h)
		if b, _ := ec.param(i, "bodyParametersUi.parameter", nil); len(asList(b)) > 0 {
			m := map[string]interface{}{}
			for _, p := range kvList(b) {
				m[p[0]] = p[1]
			}
			raw, _ := json.Marshal(m)
			s.body, s.contentType = bytes.NewReader(raw), "application/json"
		}
	}
	return s, nil
}

func doHTTP(ec *execCtx, client *http.Client, limit int64, s *httpRequestSpec) ([]Item, error) {
	resp, body, err := sendHTTP(ec, client, limit, s)
	if err != nil {
		return nil, err
	}
	return httpResponseItems(s, resp, body)
}

// sendHTTP performs the request described by s and returns the response
// with its (size-limited) body. Statuses >= 400 are errors unless s.neverError.
func sendHTTP(ec *execCtx, client *http.Client, limit int64, s *httpRequestSpec) (*http.Response, []byte, error) {
	if s.url == "" {
		return nil, nil, permanentError{"HTTP Request: URL is empty"}
	}
	u, err := url.Parse(s.url)
	if err != nil {
		return nil, nil, permanentError{fmt.Sprintf("HTTP Request: invalid URL %q", s.url)}
	}
	if u.Scheme == "" {
		u, _ = url.Parse("https://" + s.url)
	}
	if err := node.CheckScheme(u); err != nil {
		return nil, nil, err
	}
	if len(s.query) > 0 {
		extra := url.Values{}
		for _, p := range s.query {
			extra.Add(p[0], p[1])
		}
		if u.RawQuery != "" {
			u.RawQuery += "&" + extra.Encode()
		} else {
			u.RawQuery = extra.Encode()
		}
	}
	ctx := ec.ctx
	if s.timeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
	if s.body != nil && s.bodyBytes == nil {
		if s.bodyBytes, err = io.ReadAll(s.body); err != nil {
			return nil, nil, err
		}
	}
	var reqBody io.Reader
	if s.bodyBytes != nil {
		reqBody = bytes.NewReader(s.bodyBytes)
	}
	req, err := http.NewRequestWithContext(ctx, strings.ToUpper(s.method), u.String(), reqBody)
	if err != nil {
		return nil, nil, err
	}
	if s.contentType != "" {
		req.Header.Set("Content-Type", s.contentType)
	}
	for _, h := range s.headers {
		req.Header.Set(h[0], h[1])
	}
	if err := authenticateHTTP(ec, req); err != nil {
		return nil, nil, err
	}
	c := client
	if !s.follow {
		cc := *client
		cc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		c = &cc
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, nil, sanitizeErr(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := node.ReadLimited(resp.Body, limit)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode >= 400 && !s.neverError {
		snippet := string(body)
		if len(snippet) > 500 {
			snippet = snippet[:500] + "…"
		}
		return nil, nil, fmt.Errorf("HTTP Request failed with status code %d: %s", resp.StatusCode, snippet)
	}
	return resp, body, nil
}

func authenticateHTTP(ec *execCtx, req *http.Request) error {
	auth, _ := ec.str(0, "authentication", "none")
	var credType string
	switch auth {
	case "none", "":
		return nil
	case "genericCredentialType":
		credType, _ = ec.str(0, "genericAuthType", "")
	case "predefinedCredentialType":
		credType, _ = ec.str(0, "nodeCredentialType", "")
	case "basicAuth":
		credType = "httpBasicAuth"
	case "headerAuth":
		credType = "httpHeaderAuth"
	case "queryAuth":
		credType = "httpQueryAuth"
	case "oAuth2":
		credType = "oAuth2Api"
	default:
		credType = auth
	}
	if credType == "" {
		return permanentError{"HTTP Request: authentication type is not set"}
	}
	c, err := ec.credential(credType)
	if err != nil {
		return err
	}
	if !credentialHostAllowed(credType, c, req.URL.Hostname()) {
		return permanentError{fmt.Sprintf("credential %s may not be sent to %s (set allowedDomains on the credential to allow it)", credType, req.URL.Hostname())}
	}
	return applyCredential(req, credType, c)
}

func httpResponseItems(s *httpRequestSpec, resp *http.Response, body []byte) ([]Item, error) {
	ct := resp.Header.Get("Content-Type")
	mediaType, _, _ := mime.ParseMediaType(ct)
	format := s.format
	if format == "autodetect" || format == "" {
		switch {
		case strings.Contains(mediaType, "json"):
			format = "json"
		case strings.HasPrefix(mediaType, "text/") || strings.Contains(mediaType, "xml") || strings.Contains(mediaType, "javascript") || mediaType == "":
			format = "text"
		default:
			format = "file"
		}
	}
	var payload interface{}
	var bin map[string]interface{}
	switch format {
	case "json":
		if len(bytes.TrimSpace(body)) == 0 {
			payload = map[string]interface{}{}
		} else if err := json.Unmarshal(body, &payload); err != nil {
			if s.format == "json" {
				return nil, permanentError{"HTTP Request: response is not valid JSON"}
			}
			payload = string(body)
		}
	case "file":
		name := path.Base(resp.Request.URL.Path)
		bin = map[string]interface{}{s.outProp: binaryData(body, mediaType, name)}
	default:
		payload = string(body)
	}
	if s.full {
		headers := map[string]interface{}{}
		for k := range resp.Header {
			headers[strings.ToLower(k)] = resp.Header.Get(k)
		}
		j := map[string]interface{}{"body": payload, "headers": headers, "statusCode": float64(resp.StatusCode), "statusMessage": http.StatusText(resp.StatusCode)}
		return []Item{{JSON: j, Binary: bin}}, nil
	}
	if bin != nil {
		return []Item{{JSON: map[string]interface{}{}, Binary: bin}}, nil
	}
	switch t := payload.(type) {
	case []interface{}:
		out := make([]Item, 0, len(t))
		for _, el := range t {
			if m, ok := el.(map[string]interface{}); ok {
				out = append(out, Item{JSON: m})
			} else {
				out = append(out, Item{JSON: map[string]interface{}{s.outProp: el}})
			}
		}
		return out, nil
	case map[string]interface{}:
		return []Item{{JSON: t}}, nil
	default:
		return []Item{{JSON: map[string]interface{}{s.outProp: t}}}, nil
	}
}

// binaryData builds n8n's binary property format.
func binaryData(data []byte, mimeType, fileName string) map[string]interface{} {
	ext := strings.TrimPrefix(path.Ext(fileName), ".")
	if ext == "" {
		if exts, _ := mime.ExtensionsByType(mimeType); len(exts) > 0 {
			ext = strings.TrimPrefix(exts[0], ".")
		}
	}
	return map[string]interface{}{
		"data":          base64.StdEncoding.EncodeToString(data),
		"mimeType":      mimeType,
		"fileName":      fileName,
		"fileExtension": ext,
		"fileSize":      fmt.Sprintf("%d B", len(data)),
	}
}

// binaryOf reads a binary property from an item.
func binaryOf(it Item, field string) ([]byte, string, string, error) {
	if field == "" {
		field = "data"
	}
	b, ok := it.Binary[field].(map[string]interface{})
	if !ok {
		return nil, "", "", permanentError{fmt.Sprintf("item has no binary property %q", field)}
	}
	data, err := base64.StdEncoding.DecodeString(toString(b["data"]))
	if err != nil {
		return nil, "", "", errors.New("binary property is not valid base64")
	}
	mt := toString(b["mimeType"])
	if mt == "" {
		mt = "application/octet-stream"
	}
	name := toString(b["fileName"])
	if name == "" {
		name = "file"
	}
	return data, mt, name, nil
}
