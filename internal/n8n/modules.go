package n8n

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/url"
	"strings"
	"sync"

	"github.com/dop251/goja"
)

// Node.js modules for Code nodes (modules.js), loaded on first require.

//go:embed modules.js
var modulesSrc string

// nodeModuleNames are the modules modules.js provides.
var nodeModuleNames = []string{"luxon", "path", "url", "querystring", "buffer", "axios", "node-fetch", "zlib", "jsonwebtoken"}

var (
	modulesOnce sync.Once
	modulesProg *goja.Program
	modulesErr  error
)

func (r *Runtime) nodeModules() goja.Value {
	if r.modules != nil {
		return r.modules
	}
	modulesOnce.Do(func() { modulesProg, modulesErr = goja.Compile("modules.js", modulesSrc, true) })
	if modulesErr != nil {
		r.throw("modules: %v", modulesErr)
	}
	v, err := r.vm.RunProgram(modulesProg)
	if err != nil {
		panic(err)
	}
	r.modules = v
	return v
}

func (r *Runtime) moduleHost() map[string]interface{} {
	b64 := func(s string) []byte {
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			r.throw("invalid base64 data")
		}
		return b
	}
	return map[string]interface{}{
		"__nodeModules": r.nodeModules,
		"__latin1b64": func(s string) string {
			b := make([]byte, 0, len(s))
			for _, c := range s {
				b = append(b, byte(c))
			}
			return base64.StdEncoding.EncodeToString(b)
		},
		"__b64latin1": func(s string) string {
			b := b64(s)
			rs := make([]rune, len(b))
			for i, c := range b {
				rs[i] = rune(c)
			}
			return string(rs)
		},
		"__urlParse": func(raw, base string) string {
			u, err := url.Parse(strings.TrimSpace(raw))
			if err == nil && base != "" {
				var bu *url.URL
				if bu, err = url.Parse(strings.TrimSpace(base)); err == nil && bu.Scheme != "" {
					u = bu.ResolveReference(u)
				} else if err == nil {
					err = url.InvalidHostError(base)
				}
			}
			if err != nil || u.Scheme == "" || (u.Host == "" && u.Opaque == "" && u.Scheme != "file") {
				return `{"error":true}`
			}
			path := u.EscapedPath()
			if u.Opaque != "" {
				path = u.Opaque
			} else if path == "" && u.Host != "" {
				path = "/"
			}
			pw, _ := u.User.Password()
			out := map[string]string{"protocol": strings.ToLower(u.Scheme) + ":", "username": u.User.Username(), "password": pw,
				"hostname": strings.ToLower(u.Hostname()), "port": u.Port(), "pathname": path}
			if u.RawQuery != "" || u.ForceQuery {
				out["search"] = "?" + u.RawQuery
			}
			if f := u.EscapedFragment(); f != "" {
				out["hash"] = "#" + f
			}
			if (out["protocol"] == "http:" && out["port"] == "80") || (out["protocol"] == "https:" && out["port"] == "443") {
				out["port"] = ""
			}
			return mustJSON(out)
		},
		"__zlib": func(op, data string) string {
			in := b64(data)
			var out []byte
			var err error
			compress := func(w io.WriteCloser, buf *bytes.Buffer) {
				if _, err = w.Write(in); err == nil {
					err = w.Close()
				}
				out = buf.Bytes()
			}
			limit := func(rd io.Reader) {
				out, err = io.ReadAll(io.LimitReader(rd, maxDecompressedBytes+1))
				if err == nil && int64(len(out)) > int64(maxDecompressedBytes) {
					r.throw("zlib: decompressed data is too large")
				}
			}
			switch op {
			case "gzip":
				var buf bytes.Buffer
				compress(gzip.NewWriter(&buf), &buf)
			case "deflate":
				var buf bytes.Buffer
				compress(zlib.NewWriter(&buf), &buf)
			case "deflateRaw":
				var buf bytes.Buffer
				w, _ := flate.NewWriter(&buf, flate.DefaultCompression)
				compress(w, &buf)
			case "gunzip":
				var zr *gzip.Reader
				if zr, err = gzip.NewReader(bytes.NewReader(in)); err == nil {
					limit(zr)
				}
			case "inflate":
				var zr io.ReadCloser
				if zr, err = zlib.NewReader(bytes.NewReader(in)); err == nil {
					limit(zr)
				}
			case "inflateRaw":
				limit(flate.NewReader(bytes.NewReader(in)))
			case "unzip": // gzip or zlib, by header
				if len(in) > 1 && in[0] == 0x1f && in[1] == 0x8b {
					var zr *gzip.Reader
					if zr, err = gzip.NewReader(bytes.NewReader(in)); err == nil {
						limit(zr)
					}
				} else {
					var zr io.ReadCloser
					if zr, err = zlib.NewReader(bytes.NewReader(in)); err == nil {
						limit(zr)
					}
				}
			default:
				r.throw("zlib: unknown operation %s", op)
			}
			if err != nil {
				r.throw("zlib: %v", err)
			}
			return base64.StdEncoding.EncodeToString(out)
		},
		"__jwtSign": func(headerJSON, payloadJSON, alg, key string) string {
			var header, payload map[string]interface{}
			if json.Unmarshal([]byte(headerJSON), &header) != nil {
				r.throw("jsonwebtoken: invalid header")
			}
			if json.Unmarshal([]byte(payloadJSON), &payload) != nil {
				r.throw("jsonwebtoken: payload must be a plain object")
			}
			if alg == "none" {
				r.throw("jsonwebtoken: unsigned tokens are not supported")
			}
			tok, err := jwtSign(header, payload, alg, key, strings.Contains(key, "-----BEGIN"))
			if err != nil {
				r.throw("jsonwebtoken: %v", err)
			}
			return tok
		},
		"__jwtDecode": func(tok string) string {
			header, payload, sig, _, err := jwtParse(tok)
			if err != nil {
				return `{"error":true}`
			}
			return mustJSON(map[string]interface{}{"header": header, "payload": payload, "signature": base64.RawURLEncoding.EncodeToString(sig)})
		},
		// __jwtVerify returns an error message, or "" when the signature is valid.
		"__jwtVerify": func(tok, alg, key string) string {
			_, _, sig, signed, err := jwtParse(tok)
			if err != nil {
				return "jwt malformed"
			}
			if key == "" {
				return "secret or public key must be provided"
			}
			if err := jwtVerify(signed, sig, alg, key, strings.Contains(key, "-----BEGIN")); err != nil {
				return "invalid signature"
			}
			return ""
		},
	}
}
