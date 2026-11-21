package n8n

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dgmos/flow/internal/node"
)

func TestCodeNodeModules(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/fail":
			w.WriteHeader(422)
			_, _ = w.Write([]byte(`{"error":"bad"}`))
		case "/text":
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("plain ✓"))
		default:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"method": r.Method, "q": r.URL.Query().Get("q"), "body": string(body),
				"ct": r.Header.Get("Content-Type"), "auth": r.Header.Get("Authorization"), "x": r.Header.Get("X-Test")})
		}
	}))
	defer srv.Close()
	h := newHarness(t, &Options{Outbound: node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1"}}})
	code := `
const base = '` + srv.URL + `';
const axios = require('axios');
const fetch = require('node-fetch');
const path = require('node:path');
const qs = require('querystring');
const { URL: U, URLSearchParams } = require('url');
const zlib = require('zlib');
const jwt = require('jsonwebtoken');
const { DateTime } = require('luxon');

const g = await axios.get(base + '/echo', { params: { q: 'go' }, headers: { 'X-Test': 'yes' } });
const api = axios.create({ baseURL: base, auth: { username: 'u', password: 'p' } });
const p = await api.post('/echo', { a: 1 });
let failStatus = 0, failData = null;
try { await axios.get(base + '/fail'); } catch (e) { failStatus = e.response.status; failData = e.response.data; }
const okFail = await axios.get(base + '/fail', { validateStatus: () => true });
const f = await fetch(base + '/echo', { method: 'PUT', body: new URLSearchParams({ k: 'v w' }) });
const fj = await f.json();
const ft = await (await fetch(base + '/text')).text();

const u = new U('https://user:pw@Example.com:8443/a/b?x=1&y=2#frag');
u.searchParams.set('x', '9');
u.searchParams.append('z', 'a b');
const rel = new URL('../c?d=1', 'https://h.io/a/b/');

const gz = zlib.gzipSync(Buffer.from('hello hello hello'));
const token = jwt.sign({ sub: 'ada', role: 'admin' }, 'secret', { expiresIn: '1h', issuer: 'flow' });
const verified = jwt.verify(token, 'secret', { issuer: 'flow' });
let badSig = '', expired = '';
try { jwt.verify(token, 'wrong'); } catch (e) { badSig = e.name + ': ' + e.message; }
try { jwt.verify(jwt.sign({ a: 1, iat: 1000 }, 'secret', { expiresIn: 60 }), 'secret'); } catch (e) { expired = e.name; }

return [{ json: {
  get: g.data, getStatus: g.status, post: p.data, failStatus, failData, okFailStatus: okFail.status,
  fetchOk: f.ok, fetchStatus: f.status, fetchBody: fj.body, fetchCt: fj.ct, fetchText: ft, fetchHeader: f.headers.get('content-type'),
  href: u.href, host: u.host, port: u.port, origin: u.origin, x: u.searchParams.get('x'), rel: rel.href,
  path: [path.join('/a/b', '../c', 'd.txt'), path.basename('/x/y.tar.gz', '.gz'), path.extname('f.json'), path.dirname('/a/b/c'), path.resolve('/a', 'b', '../c')],
  qs: qs.stringify({ a: [1, 2], b: 'x y' }), qsParsed: qs.parse('a=1&a=2&b=x%20y'),
  gunzip: zlib.gunzipSync(gz).toString(), inflate: zlib.inflateSync(zlib.deflateSync('abc')).toString(),
  jwtParts: token.split('.').length, verified: { sub: verified.sub, iss: verified.iss, exp: verified.exp - verified.iat },
  decoded: jwt.decode(token, { complete: true }).header, badSig, expired,
  luxon: DateTime.fromISO('2024-03-15T10:00:00Z').toUTC().plus({ days: 1 }).toISODate(),
}}];`
	data := wfJSON(t, []map[string]interface{}{nd("Start", "manualTrigger", 1, nil), nd("Code", "code", 2, map[string]interface{}{"jsCode": code})}, "Start->Code")
	_, steps := h.run(t, data, "")
	j := decodeOutput(steps["Code"].Output)[0][0].JSON
	var buf strings.Builder
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(j)
	got := buf.String()
	for _, want := range []string{
		`"get":{"auth":"","body":"","ct":"","method":"GET","q":"go","x":"yes"}`, `"getStatus":200`,
		`"post":{"auth":"Basic dTpw","body":"{\"a\":1}","ct":"application/json","method":"POST","q":"","x":""}`,
		`"failStatus":422`, `"failData":{"error":"bad"}`, `"okFailStatus":422`,
		`"fetchOk":true`, `"fetchStatus":200`, `"fetchBody":"k=v+w"`, `"fetchCt":"application/x-www-form-urlencoded;charset=UTF-8"`, `"fetchText":"plain ✓"`, `"fetchHeader":"application/json"`,
		`"href":"https://user:pw@example.com:8443/a/b?x=9&y=2&z=a+b#frag"`, `"host":"example.com:8443"`, `"port":"8443"`, `"origin":"https://example.com:8443"`, `"x":"9"`, `"rel":"https://h.io/a/c?d=1"`,
		`"path":["/a/c/d.txt","y.tar",".json","/a/b","/a/c"]`, `"qs":"a=1&a=2&b=x%20y"`, `"qsParsed":{"a":["1","2"],"b":"x y"}`,
		`"gunzip":"hello hello hello"`, `"inflate":"abc"`, `"jwtParts":3`, `"verified":{"exp":3600,"iss":"flow","sub":"ada"}`,
		`"decoded":{"alg":"HS256","typ":"JWT"}`, `"badSig":"JsonWebTokenError: invalid signature"`, `"expired":"TokenExpiredError"`, `"luxon":"2024-03-16"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s\nin %s", want, got)
		}
	}

	// The outbound policy applies: without the allowance, loopback is refused.
	h2 := newHarness(t, nil)
	blocked := wfJSON(t, []map[string]interface{}{nd("Start", "manualTrigger", 1, nil), nd("Code", "code", 2, map[string]interface{}{"jsCode": `
let err = '';
try { await require('axios').get('` + srv.URL + `'); } catch (e) { err = e.code + ' ' + e.message; }
let ferr = '';
try { await require('node-fetch')('` + srv.URL + `'); } catch (e) { ferr = e.message; }
return [{json: {err, ferr, url: new URL('https://a.b/c').hostname}}];`})}, "Start->Code")
	_, steps = h2.run(t, blocked, "")
	j = decodeOutput(steps["Code"].Output)[0][0].JSON
	if !strings.Contains(toString(j["err"]), "ERR_NETWORK") || !strings.Contains(toString(j["ferr"]), "fetch failed") || j["url"] != "a.b" {
		t.Fatalf("outbound policy: %v", j)
	}
}

func TestCodeRequireScanIgnoresComments(t *testing.T) {
	cases := map[string]int{
		"// no require() here\nreturn 1;":                         0,
		"/* require('fs') */ const a = 1;":                        0,
		"const s = 'http://x // require(\"fs\")'; return s;":      0,
		"const r = /\\/\\/ require\\(/; return r.test('a');":      0,
		"const x = 1 / 2; // require()\nconst fs = require('fs')": 1,
		"const m = name; require(m);":                             1,
		"const t = `a ${1} // b`; require('child_process')":       1,
		"const path = require('path'); /* ok */":                  0,
	}
	for code, want := range cases {
		if got := codeFeatureIssues(code, "n8n-nodes-base.code"); len(got) != want {
			t.Errorf("%q: %v", code, got)
		}
	}
}

func TestCodeWebCryptoGlobals(t *testing.T) {
	j, errMsg := runCheerioCode(t, `
const enc = new TextEncoder();
const key = await crypto.subtle.importKey('raw', enc.encode('key'), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
const sig = await crypto.subtle.sign('HMAC', key, enc.encode('abc'));
const hex = Array.from(new Uint8Array(sig)).map(b => b.toString(16).padStart(2, '0')).join('');
const dig = await crypto.subtle.digest('SHA-256', enc.encode('abc'));
const rnd = crypto.getRandomValues(new Uint8Array(16));
const nodeRnd = require('crypto').getRandomValues(new Uint32Array(4));
const h = require('crypto').createHash('sha256').update(new Uint8Array([97, 98, 99])).digest('hex');
return [{ json: { hex, dig: new Uint8Array(dig)[0], rnd: rnd.length, nodeRnd: nodeRnd.length, uuid: crypto.randomUUID().length,
  dec: new TextDecoder().decode(enc.encode('çay ☕')), h } }];`)
	if errMsg != "" {
		t.Fatal(errMsg)
	}
	if j["hex"] != "9c196e32dc0175f86f4b1cb89289d6619de6bee699e4c378e68309ed97a1a6ab" || j["dig"] != 186.0 || j["rnd"] != 16.0 || j["nodeRnd"] != 4.0 ||
		j["uuid"] != 36.0 || j["dec"] != "çay ☕" || j["h"] != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatalf("web crypto: %v", j)
	}
}
