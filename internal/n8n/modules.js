// Node.js modules for Code nodes beyond crypto/util: luxon, path, url,
// querystring, buffer, axios, node-fetch, zlib and jsonwebtoken. Evaluated
// on first use (see nodeModules in modules.go). Network access goes through
// the same outbound policy as this.helpers.httpRequest.
(function (g) {
  'use strict';

  function str(v) { return v === undefined || v === null ? '' : String(v); }
  function isObj(v) { return v !== null && typeof v === 'object' && !Array.isArray(v); }
  function b64(v) {
    if (g.Buffer.isBuffer(v)) return v.toString('base64');
    if (v instanceof ArrayBuffer || ArrayBuffer.isView(v)) {
      var u = new Uint8Array(v.buffer || v, v.byteOffset || 0, v.byteLength), s = '';
      for (var i = 0; i < u.length; i++) s += String.fromCharCode(u[i]);
      return __latin1b64(s);
    }
    return g.Buffer.from(str(v)).toString('base64');
  }
  function bufFromB64(s) { return g.Buffer.from(s, 'base64'); }
  // callbackify adds Node's (err, value) callback form to sync functions.
  function withCallback(fn) {
    return function () {
      var args = Array.prototype.slice.call(arguments);
      var cb = typeof args[args.length - 1] === 'function' ? args.pop() : null;
      if (!cb) return fn.apply(this, args);
      var res, err = null;
      try { res = fn.apply(this, args); } catch (e) { err = e; }
      cb(err, res);
    };
  }

  // ---------------------------------------------------------------- path (POSIX)
  function normalizeArray(parts, absolute) {
    var out = [];
    parts.forEach(function (p) {
      if (!p || p === '.') return;
      if (p === '..') {
        if (out.length && out[out.length - 1] !== '..') out.pop();
        else if (!absolute) out.push('..');
      } else out.push(p);
    });
    return out;
  }
  var path = {
    sep: '/',
    delimiter: ':',
    normalize: function (p) {
      p = str(p);
      if (!p) return '.';
      var abs = p.charAt(0) === '/', trail = p.charAt(p.length - 1) === '/';
      var out = normalizeArray(p.split('/'), abs).join('/');
      if (!out && !abs) out = '.';
      if (out && trail) out += '/';
      return (abs ? '/' : '') + out;
    },
    join: function () {
      var parts = Array.prototype.slice.call(arguments).map(str).filter(Boolean);
      return parts.length ? path.normalize(parts.join('/')) : '.';
    },
    resolve: function () {
      var r = '';
      for (var i = arguments.length - 1; i >= 0 && r.charAt(0) !== '/'; i--) {
        var p = str(arguments[i]);
        if (p) r = p + (r ? '/' + r : '');
      }
      if (r.charAt(0) !== '/') r = '/' + r;
      var out = '/' + normalizeArray(r.split('/'), true).join('/');
      return out;
    },
    isAbsolute: function (p) { return str(p).charAt(0) === '/'; },
    dirname: function (p) {
      p = str(p).replace(/\/+$/, '');
      if (!p) return str(arguments[0]).charAt(0) === '/' ? '/' : '.';
      var i = p.lastIndexOf('/');
      if (i < 0) return '.';
      return i === 0 ? '/' : p.slice(0, i);
    },
    basename: function (p, ext) {
      p = str(p).replace(/\/+$/, '');
      var b = p.slice(p.lastIndexOf('/') + 1);
      if (ext && b !== ext && b.slice(-ext.length) === ext) b = b.slice(0, -ext.length);
      return b;
    },
    extname: function (p) {
      var b = path.basename(p), i = b.lastIndexOf('.');
      return i <= 0 ? '' : b.slice(i);
    },
    relative: function (from, to) {
      var f = path.resolve(from).split('/').filter(Boolean), t = path.resolve(to).split('/').filter(Boolean);
      var i = 0;
      while (i < f.length && i < t.length && f[i] === t[i]) i++;
      return f.slice(i).map(function () { return '..'; }).concat(t.slice(i)).join('/');
    },
    parse: function (p) {
      p = str(p);
      var base = path.basename(p), ext = path.extname(p), dir = p.indexOf('/') >= 0 ? path.dirname(p) : '';
      return { root: p.charAt(0) === '/' ? '/' : '', dir: dir, base: base, ext: ext, name: ext ? base.slice(0, -ext.length) : base };
    },
    format: function (o) {
      o = o || {};
      var base = o.base || (str(o.name) + str(o.ext));
      var dir = o.dir || o.root || '';
      return dir ? (dir === o.root ? dir + base : dir + '/' + base) : base;
    },
    toNamespacedPath: function (p) { return p; }
  };
  path.posix = path;
  path.win32 = path;

  // ---------------------------------------------------------------- querystring & URL
  function encodeQS(s) { return encodeURIComponent(s).replace(/[!'()*]/g, function (c) { return '%' + c.charCodeAt(0).toString(16).toUpperCase(); }); }
  function decodeQS(s) {
    s = str(s).replace(/\+/g, ' ');
    try { return decodeURIComponent(s); } catch (e) { return s; }
  }
  var querystring = {
    escape: encodeQS,
    unescape: decodeQS,
    parse: function (s, sep, eq) {
      sep = sep || '&'; eq = eq || '=';
      var out = Object.create(null);
      str(s).split(sep).forEach(function (pair) {
        if (!pair) return;
        var i = pair.indexOf(eq);
        var k = decodeQS(i < 0 ? pair : pair.slice(0, i)), v = i < 0 ? '' : decodeQS(pair.slice(i + eq.length));
        if (k in out) out[k] = [].concat(out[k], v); else out[k] = v;
      });
      return out;
    },
    stringify: function (o, sep, eq) {
      sep = sep || '&'; eq = eq || '=';
      return Object.keys(o || {}).map(function (k) {
        var v = o[k];
        return [].concat(v === undefined ? [] : v).map(function (x) {
          return encodeQS(k) + eq + encodeQS(typeof x === 'object' && x !== null ? '' : str(x));
        }).join(sep);
      }).filter(Boolean).join(sep);
    }
  };
  querystring.decode = querystring.parse;
  querystring.encode = querystring.stringify;

  function URLSearchParams(init) {
    this._list = [];
    if (init instanceof URLSearchParams) init = init.toString();
    if (typeof init === 'string') {
      init.replace(/^\?/, '').split('&').forEach(function (pair) {
        if (!pair) return;
        var i = pair.indexOf('=');
        this._list.push([decodeQS(i < 0 ? pair : pair.slice(0, i)), decodeQS(i < 0 ? '' : pair.slice(i + 1))]);
      }, this);
    } else if (Array.isArray(init)) {
      init.forEach(function (p) { this._list.push([str(p[0]), str(p[1])]); }, this);
    } else if (isObj(init)) {
      Object.keys(init).forEach(function (k) { this._list.push([k, str(init[k])]); }, this);
    }
    this._url = null;
  }
  var usp = URLSearchParams.prototype;
  usp._changed = function () { if (this._url) this._url._search = this._list.length ? '?' + this.toString() : ''; };
  usp.append = function (k, v) { this._list.push([str(k), str(v)]); this._changed(); };
  usp['delete'] = function (k) { this._list = this._list.filter(function (p) { return p[0] !== str(k); }); this._changed(); };
  usp.get = function (k) { for (var i = 0; i < this._list.length; i++) if (this._list[i][0] === str(k)) return this._list[i][1]; return null; };
  usp.getAll = function (k) { return this._list.filter(function (p) { return p[0] === str(k); }).map(function (p) { return p[1]; }); };
  usp.has = function (k) { return this.get(k) !== null; };
  usp.set = function (k, v) {
    k = str(k);
    var done = false;
    this._list = this._list.filter(function (p) {
      if (p[0] !== k) return true;
      if (done) return false;
      p[1] = str(v); done = true; return true;
    });
    if (!done) this._list.push([k, str(v)]);
    this._changed();
  };
  usp.sort = function () { this._list.sort(function (a, b) { return a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0; }); this._changed(); };
  usp.forEach = function (cb, self) { this._list.forEach(function (p) { cb.call(self, p[1], p[0], this); }, this); };
  usp.keys = function () { return this._list.map(function (p) { return p[0]; })[Symbol.iterator](); };
  usp.values = function () { return this._list.map(function (p) { return p[1]; })[Symbol.iterator](); };
  usp.entries = function () { return this._list.map(function (p) { return [p[0], p[1]]; })[Symbol.iterator](); };
  usp[Symbol.iterator] = usp.entries;
  usp.toString = function () {
    return this._list.map(function (p) { return encodeQS(p[0]).replace(/%20/g, '+') + '=' + encodeQS(p[1]).replace(/%20/g, '+'); }).join('&');
  };
  Object.defineProperty(usp, 'size', { get: function () { return this._list.length; } });

  function URL(input, base) {
    var p = JSON.parse(__urlParse(str(input), base === undefined ? '' : str(base)));
    if (p.error) { var e = new TypeError('Invalid URL: ' + str(input)); e.code = 'ERR_INVALID_URL'; throw e; }
    this._protocol = str(p.protocol); this._username = str(p.username); this._password = str(p.password);
    this._hostname = str(p.hostname); this._port = str(p.port); this._pathname = str(p.pathname);
    this._search = str(p.search); this._hash = str(p.hash);
    this._params = null;
  }
  var up = URL.prototype;
  function prop(name, get, set) { Object.defineProperty(up, name, { get: get, set: set, enumerable: true }); }
  prop('protocol', function () { return this._protocol; }, function (v) { this._protocol = str(v).replace(/:?$/, ':'); });
  prop('username', function () { return this._username; }, function (v) { this._username = encodeURIComponent(str(v)); });
  prop('password', function () { return this._password; }, function (v) { this._password = encodeURIComponent(str(v)); });
  prop('hostname', function () { return this._hostname; }, function (v) { this._hostname = str(v); });
  prop('port', function () { return this._port; }, function (v) { this._port = str(v); });
  prop('host', function () { return this._hostname + (this._port ? ':' + this._port : ''); }, function (v) {
    var m = /^(\[[^\]]*\]|[^:]*)(?::(\d*))?$/.exec(str(v));
    if (m) { this._hostname = m[1]; this._port = m[2] || ''; }
  });
  prop('origin', function () { return /^(https?|ftp|wss?):$/.test(this._protocol) ? this._protocol + '//' + this.host : 'null'; });
  prop('pathname', function () { return this._pathname; }, function (v) { v = str(v); this._pathname = v.charAt(0) === '/' ? v : '/' + v; });
  prop('search', function () { return this._search; }, function (v) {
    v = str(v); this._search = v && v.charAt(0) !== '?' ? '?' + v : (v === '?' ? '' : v);
    if (this._params) { this._params._list = new URLSearchParams(this._search)._list; }
  });
  prop('hash', function () { return this._hash; }, function (v) { v = str(v); this._hash = v && v.charAt(0) !== '#' ? '#' + v : (v === '#' ? '' : v); });
  prop('searchParams', function () {
    if (!this._params) { this._params = new URLSearchParams(this._search); this._params._url = this; }
    return this._params;
  });
  prop('href', function () {
    var auth = this._username ? this._username + (this._password ? ':' + this._password : '') + '@' : '';
    var slashes = /^(https?|ftp|wss?|file):$/.test(this._protocol) || this.host ? '//' : '';
    return this._protocol + slashes + auth + this.host + this._pathname + this._search + this._hash;
  }, function (v) {
    var u = new URL(v);
    for (var k in u) if (k.charAt(0) === '_') this[k] = u[k];
    this._params = null;
  });
  up.toString = function () { return this.href; };
  up.toJSON = function () { return this.href; };
  URL.canParse = function (u, b) { try { new URL(u, b); return true; } catch (e) { return false; } };
  var url = {
    URL: URL,
    URLSearchParams: URLSearchParams,
    parse: function (s, parseQuery) {
      var u;
      try { u = new URL(s); } catch (e) {
        return { href: str(s), path: str(s), pathname: str(s).split(/[?#]/)[0], query: parseQuery ? {} : null, search: null };
      }
      var q = u.search ? u.search.slice(1) : null;
      return {
        protocol: u.protocol, slashes: true, auth: u.username ? decodeURIComponent(u.username) + (u.password ? ':' + decodeURIComponent(u.password) : '') : null,
        host: u.host, port: u.port || null, hostname: u.hostname, hash: u.hash || null, search: u.search || null,
        query: parseQuery ? querystring.parse(q || '') : q, pathname: u.pathname, path: u.pathname + u.search, href: u.href
      };
    },
    format: function (o) {
      if (o instanceof URL) return o.href;
      o = o || {};
      var proto = o.protocol ? str(o.protocol).replace(/:?$/, ':') : '';
      var host = o.host || ((o.hostname || '') + (o.port ? ':' + o.port : ''));
      var search = o.search || (o.query && typeof o.query === 'object' ? '?' + querystring.stringify(o.query) : '');
      if (search === '?') search = '';
      return proto + (host ? '//' + (o.auth ? o.auth + '@' : '') + host : '') + str(o.pathname) + search + str(o.hash);
    },
    resolve: function (from, to) { return new URL(to, new URL(from, 'resolve://')).href.replace(/^resolve:\/\//, ''); },
    fileURLToPath: function (u) { return decodeURIComponent(new URL(u).pathname); },
    pathToFileURL: function (p) { return new URL('file://' + encodeURI(path.resolve(p))); }
  };

  // ---------------------------------------------------------------- HTTP: axios & fetch
  function headerBag(h) {
    var lower = {};
    Object.keys(h || {}).forEach(function (k) { lower[k.toLowerCase()] = h[k]; });
    return lower;
  }
  function mergeConfig(a, b) {
    var out = Object.assign({}, a || {}, b || {});
    out.headers = Object.assign({}, (a && a.headers) || {}, (b && b.headers) || {});
    return out;
  }
  function axiosError(message, code, config, response) {
    var e = new Error(message);
    e.name = 'AxiosError'; e.isAxiosError = true; e.code = code; e.config = config;
    if (response) { e.response = response; e.status = response.status; }
    e.toJSON = function () { return { message: e.message, name: e.name, code: e.code, status: e.status }; };
    return e;
  }
  function axiosRequest(config) {
    config = Object.assign({ method: 'get', responseType: 'json' }, config);
    var u = str(config.url);
    if (config.baseURL && !/^[a-z][a-z0-9+.-]*:/i.test(u)) u = str(config.baseURL).replace(/\/+$/, '') + '/' + u.replace(/^\/+/, '');
    var headers = Object.assign({}, config.headers || {});
    ['common', 'get', 'post', 'put', 'patch', 'delete', 'head'].forEach(function (k) { delete headers[k]; });
    if (config.auth) headers.Authorization = 'Basic ' + g.Buffer.from(str(config.auth.username) + ':' + str(config.auth.password)).toString('base64');
    var opts = { url: u, method: str(config.method).toUpperCase(), headers: headers, returnFullResponse: true, ignoreHttpStatusErrors: true };
    if (config.params) opts.qs = config.params;
    if (config.timeout) opts.timeout = config.timeout;
    var data = config.data;
    if (data instanceof URLSearchParams) { data = data.toString(); if (!headerBag(headers)['content-type']) headers['Content-Type'] = 'application/x-www-form-urlencoded'; }
    if (data !== undefined && data !== null) opts.body = g.Buffer.isBuffer(data) || typeof data === 'string' ? data : data;
    if (config.responseType === 'arraybuffer' || config.responseType === 'stream' || config.responseType === 'blob') opts.encoding = 'arraybuffer';
    else if (config.responseType === 'text') { opts.json = false; opts.encoding = 'utf8'; }
    else opts.json = true;
    var r;
    try { r = g.__httpSync(opts); } catch (e) {
      throw axiosError(e.message, /timed? ?out|deadline/i.test(e.message) ? 'ECONNABORTED' : 'ERR_NETWORK', config);
    }
    var res = { data: r.body, status: r.statusCode, statusText: r.statusMessage, headers: r.headers || {}, config: config, request: {} };
    var valid = config.validateStatus === undefined ? function (s) { return s >= 200 && s < 300; } : config.validateStatus;
    if (valid && !valid(res.status)) {
      throw axiosError('Request failed with status code ' + res.status, res.status >= 500 ? 'ERR_BAD_RESPONSE' : 'ERR_BAD_REQUEST', config, res);
    }
    return res;
  }
  function makeAxios(defaults) {
    function call(config) { return Promise.resolve().then(function () { return axiosRequest(mergeConfig(defaults, config)); }); }
    var ax = function (a, b) { return typeof a === 'string' ? call(Object.assign({}, b, { url: a })) : call(a); };
    ax.request = call;
    ['get', 'delete', 'head', 'options'].forEach(function (m) { ax[m] = function (u, c) { return call(Object.assign({}, c, { url: u, method: m })); }; });
    ['post', 'put', 'patch'].forEach(function (m) { ax[m] = function (u, d, c) { return call(Object.assign({}, c, { url: u, method: m, data: d })); }; });
    ax.defaults = Object.assign({ headers: {} }, defaults);
    ax.create = function (c) { return makeAxios(mergeConfig(defaults, c)); };
    ax.isAxiosError = function (e) { return !!(e && e.isAxiosError); };
    ax.all = function (p) { return Promise.all(p); };
    ax.spread = function (fn) { return function (arr) { return fn.apply(null, arr); }; };
    ax.interceptors = { request: { use: function () { return 0; }, eject: function () {} }, response: { use: function () { return 0; }, eject: function () {} } };
    ax.AxiosError = axiosError;
    ax['default'] = ax;
    return ax;
  }
  var axios = makeAxios({});

  function Headers(init) {
    this._h = {};
    if (init instanceof Headers) init = init._h;
    if (Array.isArray(init)) init.forEach(function (p) { this.append(p[0], p[1]); }, this);
    else if (init) Object.keys(init).forEach(function (k) { this.append(k, init[k]); }, this);
  }
  Headers.prototype.append = function (k, v) { k = str(k).toLowerCase(); this._h[k] = this._h[k] !== undefined ? this._h[k] + ', ' + str(v) : str(v); };
  Headers.prototype.set = function (k, v) { this._h[str(k).toLowerCase()] = str(v); };
  Headers.prototype.get = function (k) { var v = this._h[str(k).toLowerCase()]; return v === undefined ? null : v; };
  Headers.prototype.has = function (k) { return str(k).toLowerCase() in this._h; };
  Headers.prototype['delete'] = function (k) { delete this._h[str(k).toLowerCase()]; };
  Headers.prototype.forEach = function (cb, self) { Object.keys(this._h).forEach(function (k) { cb.call(self, this._h[k], k, this); }, this); };
  Headers.prototype.entries = function () { var h = this._h; return Object.keys(h).map(function (k) { return [k, h[k]]; })[Symbol.iterator](); };
  Headers.prototype[Symbol.iterator] = Headers.prototype.entries;
  Headers.prototype.raw = function () { var o = {}; for (var k in this._h) o[k] = [this._h[k]]; return o; };

  function Response(buf, status, statusText, headers, u) {
    this._buf = buf; this.status = status; this.statusText = statusText; this.ok = status >= 200 && status < 300;
    this.headers = new Headers(headers); this.url = u; this.redirected = false; this.type = 'basic'; this.bodyUsed = false;
  }
  Response.prototype.text = function () { this.bodyUsed = true; return Promise.resolve(this._buf.toString('utf8')); };
  Response.prototype.json = function () { var self = this; return this.text().then(function (t) { return JSON.parse(t); }); };
  Response.prototype.buffer = function () { this.bodyUsed = true; return Promise.resolve(this._buf); };
  Response.prototype.arrayBuffer = function () {
    this.bodyUsed = true;
    var s = __b64latin1(this._buf.toString('base64')), ab = new ArrayBuffer(s.length), u = new Uint8Array(ab);
    for (var i = 0; i < s.length; i++) u[i] = s.charCodeAt(i);
    return Promise.resolve(ab);
  };
  Response.prototype.clone = function () { return new Response(this._buf, this.status, this.statusText, this.headers._h, this.url); };
  function fetch(input, init) {
    return Promise.resolve().then(function () {
      init = init || {};
      var u = input instanceof URL ? input.href : (input && input.url) || str(input);
      var headers = {};
      new Headers(init.headers).forEach(function (v, k) { headers[k] = v; });
      var opts = { url: u, method: str(init.method || 'GET').toUpperCase(), headers: headers, encoding: 'arraybuffer', returnFullResponse: true, ignoreHttpStatusErrors: true };
      var body = init.body;
      if (body instanceof URLSearchParams) { body = body.toString(); if (!headers['content-type']) headers['content-type'] = 'application/x-www-form-urlencoded;charset=UTF-8'; }
      if (body !== undefined && body !== null) opts.body = g.Buffer.isBuffer(body) ? body : str(body);
      var r;
      try { r = g.__httpSync(opts); } catch (e) { throw new TypeError('fetch failed: ' + e.message); }
      return new Response(r.body, r.statusCode, r.statusMessage, r.headers, u);
    });
  }
  fetch.Headers = Headers; fetch.Response = Response; fetch['default'] = fetch;

  // ---------------------------------------------------------------- zlib
  function zop(op) {
    return withCallback(function (data) { return bufFromB64(__zlib(op, b64(data))); });
  }
  var zlib = {
    gzipSync: zop('gzip'), gunzipSync: zop('gunzip'), deflateSync: zop('deflate'), inflateSync: zop('inflate'),
    deflateRawSync: zop('deflateRaw'), inflateRawSync: zop('inflateRaw'), unzipSync: zop('unzip'),
    brotliCompressSync: function () { throw new Error('zlib: brotli is not available'); },
    brotliDecompressSync: function () { throw new Error('zlib: brotli is not available'); },
    constants: {}
  };
  ['gzip', 'gunzip', 'deflate', 'inflate', 'deflateRaw', 'inflateRaw', 'unzip'].forEach(function (op) { zlib[op] = zop(op); });

  // ---------------------------------------------------------------- jsonwebtoken
  function jwtError(name, message, extra) {
    var e = new Error(message);
    e.name = name;
    Object.assign(e, extra || {});
    return e;
  }
  // seconds converts jsonwebtoken timespans: numbers are seconds, strings
  // use ms-style units ("2h", "7 days"; a bare numeric string is ms).
  var spans = { '': 0.001, ms: 0.001, msec: 0.001, msecs: 0.001, millisecond: 0.001, milliseconds: 0.001,
    s: 1, sec: 1, secs: 1, second: 1, seconds: 1, m: 60, min: 60, mins: 60, minute: 60, minutes: 60,
    h: 3600, hr: 3600, hrs: 3600, hour: 3600, hours: 3600, d: 86400, day: 86400, days: 86400,
    w: 604800, week: 604800, weeks: 604800, y: 31557600, yr: 31557600, yrs: 31557600, year: 31557600, years: 31557600 };
  function seconds(v) {
    if (typeof v === 'number') return v;
    var m = /^\s*(-?\d+(?:\.\d+)?)\s*([a-z]*)\s*$/i.exec(str(v));
    if (!m || !(m[2].toLowerCase() in spans)) throw new Error('"expiresIn" should be a number of seconds or string representing a timespan');
    return Math.floor(parseFloat(m[1]) * spans[m[2].toLowerCase()]);
  }
  function keyString(k) {
    if (k === undefined || k === null) return '';
    if (g.Buffer.isBuffer(k)) return k.toString('utf8');
    if (isObj(k) && k.key !== undefined) return keyString(k.key);
    return str(k);
  }
  var jwt = {
    sign: withCallback(function (payload, secret, options) {
      options = options || {};
      var p = typeof payload === 'string' ? payload : Object.assign({}, payload);
      var alg = options.algorithm || 'HS256';
      if (typeof p === 'object') {
        var now = Math.floor((p.iat !== undefined ? p.iat : Date.now() / 1000));
        if (!options.noTimestamp && p.iat === undefined) p.iat = now;
        if (options.expiresIn !== undefined) p.exp = now + seconds(options.expiresIn);
        if (options.notBefore !== undefined) p.nbf = now + seconds(options.notBefore);
        if (options.audience !== undefined) p.aud = options.audience;
        if (options.issuer !== undefined) p.iss = options.issuer;
        if (options.subject !== undefined) p.sub = options.subject;
        if (options.jwtid !== undefined) p.jti = options.jwtid;
      }
      var header = Object.assign({ alg: alg, typ: 'JWT' }, options.header || {});
      if (options.keyid !== undefined) header.kid = options.keyid;
      return __jwtSign(JSON.stringify(header), typeof p === 'string' ? JSON.stringify(p) : JSON.stringify(p), alg, keyString(secret));
    }),
    decode: function (token, options) {
      var r = JSON.parse(__jwtDecode(str(token)));
      if (r.error) return null;
      return options && options.complete ? { header: r.header, payload: r.payload, signature: r.signature } : r.payload;
    },
    verify: withCallback(function (token, secret, options) {
      options = options || {};
      var d = JSON.parse(__jwtDecode(str(token)));
      if (d.error) throw jwtError('JsonWebTokenError', 'jwt malformed');
      if (typeof secret === 'function') throw jwtError('JsonWebTokenError', 'a key callback is not supported; pass the key');
      var alg = d.header.alg;
      if (options.algorithms && options.algorithms.indexOf(alg) < 0) throw jwtError('JsonWebTokenError', 'invalid algorithm');
      var err = __jwtVerify(str(token), alg, keyString(secret));
      if (err) throw jwtError('JsonWebTokenError', err);
      var p = d.payload, now = Math.floor(Date.now() / 1000) - (options.clockTolerance || 0);
      if (!options.ignoreNotBefore && typeof p.nbf === 'number' && p.nbf > now + 2 * (options.clockTolerance || 0)) {
        throw jwtError('NotBeforeError', 'jwt not active', { date: new Date(p.nbf * 1000) });
      }
      if (!options.ignoreExpiration && typeof p.exp === 'number' && now >= p.exp) {
        throw jwtError('TokenExpiredError', 'jwt expired', { expiredAt: new Date(p.exp * 1000) });
      }
      if (options.audience !== undefined) {
        var want = [].concat(options.audience), have = [].concat(p.aud === undefined ? [] : p.aud);
        if (!want.some(function (a) { return have.some(function (h) { return a instanceof RegExp ? a.test(h) : a === h; }); })) {
          throw jwtError('JsonWebTokenError', 'jwt audience invalid. expected: ' + want.join(' or '));
        }
      }
      if (options.issuer !== undefined && [].concat(options.issuer).indexOf(p.iss) < 0) throw jwtError('JsonWebTokenError', 'jwt issuer invalid. expected: ' + [].concat(options.issuer).join(','));
      if (options.subject !== undefined && p.sub !== options.subject) throw jwtError('JsonWebTokenError', 'jwt subject invalid. expected: ' + options.subject);
      if (options.maxAge !== undefined && typeof p.iat === 'number' && now - p.iat > seconds(options.maxAge)) throw jwtError('TokenExpiredError', 'maxAge exceeded');
      return options.complete ? { header: d.header, payload: p, signature: d.signature } : p;
    }),
    JsonWebTokenError: function (m) { return jwtError('JsonWebTokenError', m); },
    TokenExpiredError: function (m, at) { return jwtError('TokenExpiredError', m, { expiredAt: at }); },
    NotBeforeError: function (m, at) { return jwtError('NotBeforeError', m, { date: at }); }
  };

  // ---------------------------------------------------------------- luxon & buffer
  var luxon = { DateTime: g.DateTime, Duration: g.Duration, Interval: g.Interval, Settings: { defaultZone: __defaultZone(), throwOnInvalid: false }, Info: {} };

  var mods = {
    luxon: luxon, path: path, url: url, querystring: querystring, buffer: { Buffer: g.Buffer },
    axios: axios, 'node-fetch': fetch, zlib: zlib, jsonwebtoken: jwt
  };
  Object.keys(mods).forEach(function (k) { if (!mods[k]['default']) mods[k]['default'] = mods[k]; });
  return mods;
})(this)
