package n8n

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dgmos/flow/internal/node"
)

// codeHTTPRequest implements this.helpers.httpRequest / request for Code
// nodes. Requests go through the outbound policy and size limit; no
// credentials are attached.
func (r *Runtime) codeHTTPRequest(optsJSON string) (string, error) {
	var o map[string]interface{}
	if err := json.Unmarshal([]byte(optsJSON), &o); err != nil {
		return "", err
	}
	opts := &Options{}
	if r.data != nil && r.data.opts != nil {
		opts = r.data.opts
	}
	raw := toString(o["url"])
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("httpRequest: invalid URL %q", raw)
	}
	if err := node.CheckScheme(u); err != nil {
		return "", err
	}
	if qs := asMap(o["qs"]); len(qs) > 0 {
		q := u.Query()
		for k, v := range qs {
			for _, x := range asListOrOne(v) {
				q.Add(k, toString(x))
			}
		}
		u.RawQuery = q.Encode()
	}
	method := strings.ToUpper(toString(o["method"]))
	if method == "" {
		method = "GET"
	}
	headers := map[string]string{}
	for k, v := range asMap(o["headers"]) {
		headers[k] = toString(v)
	}
	var body []byte
	switch {
	case o["__bodyB64"] != nil:
		body, _ = base64.StdEncoding.DecodeString(toString(o["__bodyB64"]))
	case o["form"] != nil:
		f := url.Values{}
		for k, v := range asMap(o["form"]) {
			f.Set(k, toString(v))
		}
		body = []byte(f.Encode())
		setDefaultHeader(headers, "Content-Type", "application/x-www-form-urlencoded")
	case o["body"] != nil:
		if s, ok := o["body"].(string); ok {
			body = []byte(s)
		} else {
			body, _ = json.Marshal(o["body"])
			setDefaultHeader(headers, "Content-Type", "application/json")
		}
	}
	timeout := 30 * time.Second
	if t := toFloatDefault(o["timeout"], 0); t > 0 && time.Duration(t)*time.Millisecond < timeout {
		timeout = time.Duration(t) * time.Millisecond
	}
	parent := r.ctx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	if body == nil {
		req.Body = http.NoBody
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := outboundClient(opts.Outbound).Do(req)
	if err != nil {
		return "", sanitizeErr(err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := node.ReadLimited(resp.Body, opts.Outbound.MaxResponseBytesOrDefault())
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 400 && !toBool(o["ignoreHttpStatusErrors"]) && o["simple"] != false {
		snippet := string(data)
		if len(snippet) > 300 {
			snippet = snippet[:300] + "…"
		}
		return "", fmt.Errorf("request failed with status code %d: %s", resp.StatusCode, snippet)
	}
	hs := map[string]interface{}{}
	for k := range resp.Header {
		hs[strings.ToLower(k)] = resp.Header.Get(k)
	}
	res := map[string]interface{}{"headers": hs, "statusCode": float64(resp.StatusCode), "statusMessage": http.StatusText(resp.StatusCode)}
	switch enc := toString(o["encoding"]); {
	case enc == "arraybuffer" || enc == "binary" || enc == "null" || (o["encoding"] == nil && o["json"] == nil && !textual(resp.Header.Get("Content-Type"))):
		res["binary"] = base64.StdEncoding.EncodeToString(data)
	default:
		var v interface{}
		wantJSON := toBool(o["json"]) || (o["json"] == nil && strings.Contains(resp.Header.Get("Content-Type"), "json"))
		if wantJSON && len(bytes.TrimSpace(data)) > 0 && json.Unmarshal(data, &v) == nil {
			res["body"] = v
		} else {
			res["body"] = string(data)
		}
	}
	return mustJSON(res), nil
}

func textual(ct string) bool {
	ct = strings.ToLower(ct)
	return ct == "" || strings.HasPrefix(ct, "text/") || strings.Contains(ct, "json") || strings.Contains(ct, "xml") || strings.Contains(ct, "javascript") || strings.Contains(ct, "x-www-form-urlencoded")
}

func setDefaultHeader(h map[string]string, k, v string) {
	for hk := range h {
		if strings.EqualFold(hk, k) {
			return
		}
	}
	h[k] = v
}

func asListOrOne(v interface{}) []interface{} {
	if l, ok := v.([]interface{}); ok {
		return l
	}
	return []interface{}{v}
}

// codeDigest implements require('crypto').createHash/createHmac digests over
// base64 chunks; it returns the digest in base64.
func codeDigest(alg, keyB64, chunksJSON string, isHMAC bool) (string, error) {
	var newHash func() hash.Hash
	switch strings.ToLower(strings.ReplaceAll(alg, "-", "")) {
	case "md5":
		newHash = md5.New
	case "sha1":
		newHash = sha1.New
	case "sha256":
		newHash = sha256.New
	case "sha384":
		newHash = sha512.New384
	case "sha512":
		newHash = sha512.New
	default:
		return "", fmt.Errorf("digest method %q is not supported", alg)
	}
	h := newHash()
	if isHMAC {
		key, _ := base64.StdEncoding.DecodeString(keyB64)
		h = hmac.New(newHash, key)
	}
	var chunks []string
	_ = json.Unmarshal([]byte(chunksJSON), &chunks)
	for _, c := range chunks {
		b, _ := base64.StdEncoding.DecodeString(c)
		h.Write(b)
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}

func randomB64(n int) (string, error) {
	if n < 0 || n > 1<<20 {
		return "", fmt.Errorf("randomBytes: size %d out of range", n)
	}
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func randomFloat() float64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return float64(binary.BigEndian.Uint64(b[:])>>11) / float64(1<<53)
}
