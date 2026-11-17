// Runtime prelude for n8n expressions and Code nodes, evaluated in goja.
// Host functions (prefixed with __) are provided by the Go side.
(function (g) {
  'use strict';

  function def(proto, name, fn) {
    Object.defineProperty(proto, name, { value: fn, writable: true, configurable: true, enumerable: false });
  }
  function isObj(v) { return v !== null && typeof v === 'object' && !Array.isArray(v); }
  function keyOf(v) { return JSON.stringify(v); }

  // ---------------------------------------------------------------- Array
  def(Array.prototype, 'first', function () { return this[0]; });
  def(Array.prototype, 'last', function () { return this[this.length - 1]; });
  def(Array.prototype, 'isEmpty', function () { return this.length === 0; });
  def(Array.prototype, 'isNotEmpty', function () { return this.length > 0; });
  def(Array.prototype, 'sum', function () { return this.reduce(function (a, b) { return a + (Number(b) || 0); }, 0); });
  def(Array.prototype, 'average', function () { return this.length ? this.sum() / this.length : 0; });
  def(Array.prototype, 'max', function () { return this.length ? Math.max.apply(null, this.map(Number)) : undefined; });
  def(Array.prototype, 'min', function () { return this.length ? Math.min.apply(null, this.map(Number)) : undefined; });
  def(Array.prototype, 'pluck', function () {
    var keys = Array.prototype.slice.call(arguments);
    return this.map(function (o) {
      if (o == null) return undefined;
      if (keys.length === 0) return o;
      if (keys.length === 1) return o[keys[0]];
      var r = {}; keys.forEach(function (k) { r[k] = o[k]; }); return r;
    }).filter(function (v) { return v !== undefined; });
  });
  function unique() {
    var keys = Array.prototype.slice.call(arguments), seen = {};
    return this.filter(function (v) {
      var k = keys.length && isObj(v) ? keyOf(keys.map(function (f) { return v[f]; })) : keyOf(v);
      if (seen[k]) return false; seen[k] = true; return true;
    });
  }
  def(Array.prototype, 'unique', unique);
  def(Array.prototype, 'removeDuplicates', unique);
  def(Array.prototype, 'compact', function () {
    return this.filter(function (v) { return v !== null && v !== undefined && v !== '' && !(typeof v === 'number' && isNaN(v)); });
  });
  def(Array.prototype, 'chunk', function (size) {
    size = Math.max(1, size | 0); var out = [];
    for (var i = 0; i < this.length; i += size) out.push(this.slice(i, i + size));
    return out;
  });
  def(Array.prototype, 'randomItem', function () { return this[Math.floor(Math.random() * this.length)]; });
  def(Array.prototype, 'difference', function (other) {
    var s = {}; (other || []).forEach(function (v) { s[keyOf(v)] = 1; });
    return this.filter(function (v) { return !s[keyOf(v)]; });
  });
  def(Array.prototype, 'intersection', function (other) {
    var s = {}; (other || []).forEach(function (v) { s[keyOf(v)] = 1; });
    return unique.call(this.filter(function (v) { return s[keyOf(v)]; }));
  });
  def(Array.prototype, 'union', function (other) { return unique.call(this.concat(other || [])); });
  def(Array.prototype, 'merge', function () {
    var out = {}; this.forEach(function (o) { if (isObj(o)) Object.keys(o).forEach(function (k) { if (!(k in out)) out[k] = o[k]; }); });
    return out;
  });
  def(Array.prototype, 'smartJoin', function (keyField, valueField) {
    var out = {}; this.forEach(function (o) { if (isObj(o)) out[o[keyField]] = o[valueField]; }); return out;
  });
  def(Array.prototype, 'append', function () { return this.concat(Array.prototype.slice.call(arguments)); });
  def(Array.prototype, 'toJsonString', function () { return JSON.stringify(this); });
  def(Array.prototype, 'renameKeys', function () { return this; });

  // ---------------------------------------------------------------- String
  var S = String.prototype;
  def(S, 'isEmpty', function () { return this.length === 0; });
  def(S, 'isNotEmpty', function () { return this.length > 0; });
  def(S, 'toNumber', function () { return Number(this); });
  def(S, 'toInt', function () { return parseInt(this, 10); });
  def(S, 'toFloat', function () { return parseFloat(this); });
  def(S, 'toBoolean', function () {
    var v = String(this).trim().toLowerCase();
    return !(v === '' || v === 'false' || v === '0' || v === 'no' || v === 'n' || v === 'off');
  });
  def(S, 'toDateTime', function (fmt) { return fmt ? DateTime.fromFormat(String(this), fmt) : DateTime.fromAny(String(this)); });
  def(S, 'toJsonString', function () { return JSON.stringify(String(this)); });
  def(S, 'parseJson', function () { return JSON.parse(this); });
  def(S, 'extractEmail', function () { var m = String(this).match(/[\w.+-]+@[\w-]+(\.[\w-]+)+/); return m ? m[0] : undefined; });
  def(S, 'extractUrl', function () { var m = String(this).match(/https?:\/\/[^\s"'<>]+/); return m ? m[0] : undefined; });
  def(S, 'extractDomain', function () {
    var s = String(this), m = s.match(/@([\w-]+(\.[\w-]+)+)/);
    if (m && s.indexOf('://') < 0) return m[1];
    m = s.match(/^(?:[a-z]+:\/\/)?(?:[^@\/]+@)?([^\/:?#]+)/i);
    return m ? m[1].replace(/^www\./, '') : undefined;
  });
  def(S, 'extractUrlPath', function () { var m = String(this).match(/^[a-z]+:\/\/[^\/]+(\/[^?#]*)/i); return m ? m[1] : undefined; });
  def(S, 'urlEncode', function (all) { return all ? encodeURIComponent(this) : encodeURI(this); });
  def(S, 'urlDecode', function (all) { return all ? decodeURIComponent(this) : decodeURI(this); });
  def(S, 'toTitleCase', function () { return String(this).toLowerCase().replace(/(^|\s)\S/g, function (c) { return c.toUpperCase(); }); });
  def(S, 'toSentenceCase', function () { var s = String(this).toLowerCase(); return s.charAt(0).toUpperCase() + s.slice(1); });
  def(S, 'toSnakeCase', function () {
    return String(this).replace(/([a-z0-9])([A-Z])/g, '$1_$2').replace(/[\s\-]+/g, '_').replace(/[^\w]/g, '').toLowerCase();
  });
  def(S, 'toCamelCase', function () {
    return String(this).toLowerCase().replace(/[^a-z0-9]+(.)/g, function (_, c) { return c.toUpperCase(); });
  });
  def(S, 'removeTags', function () { return String(this).replace(/<[^>]*>?/g, ''); });
  def(S, 'removeMarkdown', function () {
    return String(this).replace(/```[\s\S]*?```/g, '').replace(/`([^`]*)`/g, '$1').replace(/!\[[^\]]*\]\([^)]*\)/g, '')
      .replace(/\[([^\]]*)\]\([^)]*\)/g, '$1').replace(/^#{1,6}\s*/gm, '').replace(/(\*\*|__)(.*?)\1/g, '$2')
      .replace(/(\*|_)(.*?)\1/g, '$2').replace(/^\s*[-*+]\s+/gm, '').replace(/^\s*>\s?/gm, '');
  });
  def(S, 'hash', function (algo) { return __hash(algo || 'md5', String(this)); });
  def(S, 'base64Encode', function () { return __b64enc(String(this)); });
  def(S, 'base64Decode', function () { return __b64dec(String(this)); });
  def(S, 'isUrl', function () { return /^https?:\/\/[^\s.]+\.[^\s]+$/i.test(this); });
  def(S, 'isEmail', function () { return /^[\w.+-]+@[\w-]+(\.[\w-]+)+$/.test(this); });
  def(S, 'isDomain', function () { return /^([a-z0-9-]+\.)+[a-z]{2,}$/i.test(this); });
  def(S, 'isNumeric', function () { return String(this).trim() !== '' && !isNaN(Number(this)); });
  def(S, 'quote', function (mark) { mark = mark || '"'; return mark + String(this).split(mark).join('\\' + mark) + mark; });
  def(S, 'replaceSpecialChars', function () {
    var s = String(this);
    try { s = s.normalize('NFD'); } catch (e) { }
    return s.replace(/[̀-ͯ]/g, '');
  });

  // ---------------------------------------------------------------- Number
  var N = Number.prototype;
  function rounder(fn) { return function (d) { var p = Math.pow(10, d || 0); return fn(this * p) / p; }; }
  def(N, 'round', rounder(Math.round));
  def(N, 'floor', rounder(Math.floor));
  def(N, 'ceil', rounder(Math.ceil));
  def(N, 'isEven', function () { return this % 2 === 0; });
  def(N, 'isOdd', function () { return Math.abs(this % 2) === 1; });
  def(N, 'toBoolean', function () { return this.valueOf() !== 0; });
  def(N, 'toDateTime', function (unit) {
    return unit === 's' || unit === 'seconds' ? DateTime.fromSeconds(this.valueOf()) : DateTime.fromMillis(this.valueOf());
  });
  def(N, 'format', function () { return String(this); });

  // ---------------------------------------------------------------- Object
  var O = Object.prototype;
  def(O, 'isEmpty', function () { return Object.keys(this).length === 0; });
  def(O, 'isNotEmpty', function () { return Object.keys(this).length > 0; });
  def(O, 'hasField', function (k) { return Object.prototype.hasOwnProperty.call(this, k); });
  def(O, 'removeField', function (k) { var o = Object.assign({}, this); delete o[k]; return o; });
  def(O, 'removeFieldsContaining', function (v) {
    var o = {}, self = this; Object.keys(self).forEach(function (k) { if (String(self[k]).indexOf(v) < 0) o[k] = self[k]; }); return o;
  });
  def(O, 'keepFieldsContaining', function (v) {
    var o = {}, self = this; Object.keys(self).forEach(function (k) { if (String(self[k]).indexOf(v) >= 0) o[k] = self[k]; }); return o;
  });
  def(O, 'toJsonString', function () { return JSON.stringify(this); });

  // ---------------------------------------------------------------- DateTime
  var UNITS = ['year', 'quarter', 'month', 'week', 'day', 'hour', 'minute', 'second', 'millisecond'];
  var MS = { millisecond: 1, second: 1e3, minute: 6e4, hour: 36e5, day: 864e5, week: 6048e5 };
  var MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
  var DAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
  function normUnit(u) {
    u = String(u || '').replace(/s$/, '');
    return u === 'ms' ? 'millisecond' : u;
  }
  function pad(n, w) { n = String(Math.abs(n)); while (n.length < w) n = '0' + n; return n; }
  function daysInMonth(y, m) { return new Date(Date.UTC(y, m, 0)).getUTCDate(); }

  function DT(ts, zone) {
    this.ts = ts;
    this.zoneName = zone || __defaultZone();
    this.isValid = !isNaN(ts) && ts !== null;
    if (!this.isValid) { this.invalidReason = 'unparsable'; return; }
    var off = __tzOffset(this.zoneName, ts);
    if (off === null) { this.isValid = false; this.invalidReason = 'unsupported zone'; return; }
    this.offset = off;
    var d = new Date(ts + off * 6e4);
    this.year = d.getUTCFullYear(); this.month = d.getUTCMonth() + 1; this.day = d.getUTCDate();
    this.hour = d.getUTCHours(); this.minute = d.getUTCMinutes(); this.second = d.getUTCSeconds();
    this.millisecond = d.getUTCMilliseconds();
    this.weekday = d.getUTCDay() === 0 ? 7 : d.getUTCDay();
    this.quarter = Math.ceil(this.month / 3);
    var start = Date.UTC(this.year, 0, 1);
    this.ordinal = Math.floor((Date.UTC(this.year, this.month - 1, this.day) - start) / 864e5) + 1;
    var th = new Date(Date.UTC(this.year, this.month - 1, this.day + 4 - this.weekday));
    this.weekYear = th.getUTCFullYear();
    this.weekNumber = Math.ceil(((th - Date.UTC(this.weekYear, 0, 1)) / 864e5 + 1) / 7);
    this.monthLong = MONTHS[this.month - 1]; this.monthShort = this.monthLong.slice(0, 3);
    this.weekdayLong = DAYS[this.weekday - 1]; this.weekdayShort = this.weekdayLong.slice(0, 3);
    this.daysInMonth = daysInMonth(this.year, this.month);
    this.isWeekend = this.weekday >= 6;
    this.isInLeapYear = daysInMonth(this.year, 2) === 29;
  }
  function fromParts(o, zone) {
    zone = zone || __defaultZone();
    var ts = __tzLocal(zone, o.year, o.month || 1, o.day || 1, o.hour || 0, o.minute || 0, o.second || 0, o.millisecond || 0);
    return new DT(ts, zone);
  }
  var P = DT.prototype;
  P.toMillis = P.valueOf = function () { return this.ts; };
  P.toSeconds = function () { return this.ts / 1e3; };
  P.toUnixInteger = function () { return Math.floor(this.ts / 1e3); };
  P.toJSDate = function () { return new Date(this.ts); };
  function offStr(o, colon) {
    var s = o < 0 ? '-' : '+'; o = Math.abs(o);
    return s + pad(Math.floor(o / 60), 2) + (colon ? ':' : '') + pad(o % 60, 2);
  }
  P.toISODate = function () { return this.isValid ? pad(this.year, 4) + '-' + pad(this.month, 2) + '-' + pad(this.day, 2) : null; };
  P.toISOTime = function () {
    if (!this.isValid) return null;
    return pad(this.hour, 2) + ':' + pad(this.minute, 2) + ':' + pad(this.second, 2) + '.' + pad(this.millisecond, 3) +
      (this.offset === 0 && this.zoneName === 'UTC' ? 'Z' : offStr(this.offset, true));
  };
  P.toISO = function () { return this.isValid ? this.toISODate() + 'T' + this.toISOTime() : null; };
  P.toString = P.toJSON = function () { return this.isValid ? this.toISO() : 'Invalid DateTime'; };
  P.toSQLDate = P.toISODate;
  P.toSQL = function () { return this.toISODate() + ' ' + this.toFormat('HH:mm:ss.SSS ZZ'); };
  P.toRFC2822 = function () { return this.toFormat('EEE, dd MMM yyyy HH:mm:ss ZZZ'); };
  P.toHTTP = function () { return this.toUTC().toFormat("EEE, dd MMM yyyy HH:mm:ss 'GMT'"); };
  var TOKENS = /'[^']*'|yyyy|yy|y|kkkk|kk|MMMM|MMM|MM|M|LLLL|LLL|LL|L|dd|d|EEEE|EEE|E|cccc|ccc|c|HH|H|hh|h|mm|m|ss|s|SSS|S|a|ZZZ|ZZ|Z|z|WW|W|ooo|o|q|X|x|DDDD|DDD|DD|D|ttt|tt|t|TT|T/g;
  P.toFormat = function (fmt) {
    if (!this.isValid) return 'Invalid DateTime';
    var d = this;
    return String(fmt).replace(TOKENS, function (t) {
      switch (t) {
        case 'yyyy': return pad(d.year, 4); case 'yy': return pad(d.year % 100, 2); case 'y': return String(d.year);
        case 'kkkk': return pad(d.weekYear, 4); case 'kk': return pad(d.weekYear % 100, 2);
        case 'MMMM': case 'LLLL': return d.monthLong; case 'MMM': case 'LLL': return d.monthShort;
        case 'MM': case 'LL': return pad(d.month, 2); case 'M': case 'L': return String(d.month);
        case 'dd': return pad(d.day, 2); case 'd': return String(d.day);
        case 'EEEE': case 'cccc': return d.weekdayLong; case 'EEE': case 'ccc': return d.weekdayShort;
        case 'E': case 'c': return String(d.weekday);
        case 'HH': return pad(d.hour, 2); case 'H': return String(d.hour);
        case 'hh': return pad(d.hour % 12 || 12, 2); case 'h': return String(d.hour % 12 || 12);
        case 'mm': return pad(d.minute, 2); case 'm': return String(d.minute);
        case 'ss': return pad(d.second, 2); case 's': return String(d.second);
        case 'SSS': return pad(d.millisecond, 3); case 'S': return String(d.millisecond);
        case 'a': return d.hour < 12 ? 'AM' : 'PM';
        case 'ZZZ': return offStr(d.offset, false); case 'ZZ': return offStr(d.offset, true);
        case 'Z': return (d.offset < 0 ? '-' : '+') + Math.floor(Math.abs(d.offset) / 60) + (d.offset % 60 ? ':' + pad(Math.abs(d.offset) % 60, 2) : '');
        case 'z': return d.zoneName;
        case 'WW': return pad(d.weekNumber, 2); case 'W': return String(d.weekNumber);
        case 'ooo': return pad(d.ordinal, 3); case 'o': return String(d.ordinal);
        case 'q': return String(d.quarter);
        case 'X': return String(d.toUnixInteger()); case 'x': return String(d.ts);
        case 'D': return d.toLocaleString(DateTime.DATE_SHORT); case 'DD': return d.toLocaleString(DateTime.DATE_MED);
        case 'DDD': return d.toLocaleString(DateTime.DATE_FULL); case 'DDDD': return d.toLocaleString(DateTime.DATE_HUGE);
        case 't': return d.toLocaleString(DateTime.TIME_SIMPLE); case 'tt': return d.toLocaleString(DateTime.TIME_WITH_SECONDS);
        case 'T': return d.toLocaleString(DateTime.TIME_24_SIMPLE); case 'TT': return d.toLocaleString(DateTime.TIME_24_WITH_SECONDS);
        case 'ttt': return d.toLocaleString(DateTime.TIME_WITH_SHORT_OFFSET);
        default: return t.charAt(0) === "'" ? t.slice(1, -1) : t;
      }
    });
  };
  P.format = P.toFormat;
  P.toLocaleString = function (preset) {
    if (!this.isValid) return 'Invalid DateTime';
    var p = preset || DateTime.DATETIME_SHORT, parts = [];
    var date = '';
    if (p.weekday === 'long') date += this.weekdayLong + ', ';
    if (p.weekday === 'short') date += this.weekdayShort + ', ';
    if (p.month === 'numeric') date += this.month + '/' + this.day + '/' + this.year;
    else if (p.month === 'short') date += this.monthShort + ' ' + this.day + ', ' + this.year;
    else if (p.month === 'long') date += this.monthLong + ' ' + this.day + ', ' + this.year;
    if (p.month) parts.push(date);
    if (p.hour) {
      var t = p.hourCycle === 'h23' ? pad(this.hour, 2) + ':' + pad(this.minute, 2) : (this.hour % 12 || 12) + ':' + pad(this.minute, 2);
      if (p.second) t += ':' + pad(this.second, 2);
      if (p.hourCycle !== 'h23') t += ' ' + (this.hour < 12 ? 'AM' : 'PM');
      if (p.timeZoneName) t += ' ' + (this.zoneName === 'UTC' ? 'UTC' : 'GMT' + offStr(this.offset, true));
      parts.push(t);
    }
    return parts.length ? parts.join(', ') : this.toISO();
  };
  function addDur(dt, dur, sign) {
    if (dur && dur.__dur) dur = dur.__dur;
    if (typeof dur === 'number') { var tmp = {}; tmp[normUnit(arguments[3] || 'millisecond')] = dur; dur = tmp; }
    var o = {};
    Object.keys(dur || {}).forEach(function (k) { o[normUnit(k)] = Number(dur[k]) * sign; });
    var y = dt.year + (o.year || 0), m = dt.month + (o.month || 0) + 3 * (o.quarter || 0);
    y += Math.floor((m - 1) / 12); m = ((m - 1) % 12 + 12) % 12 + 1;
    var day = Math.min(dt.day, daysInMonth(y, m));
    var base = fromParts({ year: y, month: m, day: day, hour: dt.hour, minute: dt.minute, second: dt.second, millisecond: dt.millisecond }, dt.zoneName);
    var days = (o.day || 0) + 7 * (o.week || 0);
    if (days) base = fromParts({ year: base.year, month: base.month, day: base.day + days, hour: base.hour, minute: base.minute, second: base.second, millisecond: base.millisecond }, dt.zoneName);
    var ms = (o.hour || 0) * MS.hour + (o.minute || 0) * MS.minute + (o.second || 0) * MS.second + (o.millisecond || 0);
    return new DT(base.ts + ms, dt.zoneName);
  }
  // calDiff returns the calendar difference a-b in units of `step` months,
  // like Luxon: whole units by calendar arithmetic (with end-of-month
  // clamping), plus the elapsed fraction of the following unit.
  function calDiff(a, b, step) {
    if (a.ts < b.ts) return -calDiff(b, a, step);
    b = new DT(b.ts, a.zoneName);
    var whole = Math.floor(((a.year - b.year) * 12 + (a.month - b.month)) / step);
    var cur = addDur(b, { month: whole * step }, 1);
    while (whole > 0 && cur.ts > a.ts) { whole--; cur = addDur(b, { month: whole * step }, 1); }
    var next = addDur(b, { month: (whole + 1) * step }, 1);
    return whole + (a.ts - cur.ts) / (next.ts - cur.ts);
  }
  P.plus = function (dur, unit) { return addDur(this, dur, 1, unit); };
  P.minus = function (dur, unit) { return addDur(this, dur, -1, unit); };
  P.startOf = function (unit) {
    unit = normUnit(unit);
    if (unit === 'week') return fromParts({ year: this.year, month: this.month, day: this.day - (this.weekday - 1) }, this.zoneName);
    var o = { year: this.year, month: this.month, day: this.day, hour: this.hour, minute: this.minute, second: this.second, millisecond: this.millisecond };
    switch (unit) {
      case 'year': o.month = 1; o.day = 1; o.hour = 0; o.minute = 0; o.second = 0; o.millisecond = 0; break;
      case 'quarter': o.month = (this.quarter - 1) * 3 + 1; o.day = 1; o.hour = 0; o.minute = 0; o.second = 0; o.millisecond = 0; break;
      case 'month': o.day = 1; o.hour = 0; o.minute = 0; o.second = 0; o.millisecond = 0; break;
      case 'day': o.hour = 0; o.minute = 0; o.second = 0; o.millisecond = 0; break;
      case 'hour': o.minute = 0; o.second = 0; o.millisecond = 0; break;
      case 'minute': o.second = 0; o.millisecond = 0; break;
      case 'second': o.millisecond = 0; break;
    }
    return fromParts(o, this.zoneName);
  };
  P.endOf = function (unit) {
    var tmp = {}; tmp[normUnit(unit)] = 1;
    return new DT(this.startOf(unit).plus(tmp).ts - 1, this.zoneName);
  };
  P.set = function (vals) {
    var o = { year: this.year, month: this.month, day: this.day, hour: this.hour, minute: this.minute, second: this.second, millisecond: this.millisecond };
    Object.keys(vals || {}).forEach(function (k) { o[normUnit(k)] = vals[k]; });
    return fromParts(o, this.zoneName);
  };
  P.setZone = function (zone, opts) {
    if (zone === 'local' || zone === undefined) zone = __localZone();
    if (opts && opts.keepLocalTime) return fromParts(this, zone);
    return new DT(this.ts, zone);
  };
  P.toUTC = function () { return new DT(this.ts, 'UTC'); };
  P.toLocal = function () { return new DT(this.ts, __localZone()); };
  P.reconfigure = function () { return this; };
  P.setLocale = function () { return this; };
  P.diff = function (other, unit) {
    var a = this, b = DateTime.fromAny(other), ms = a.ts - b.ts;
    var units = Array.isArray(unit) ? unit : [unit || 'milliseconds'];
    var res = { values: {} };
    units.forEach(function (u) {
      var n = normUnit(u), v;
      if (n === 'month' || n === 'year' || n === 'quarter') {
        v = calDiff(a, b, n === 'year' ? 12 : n === 'quarter' ? 3 : 1);
      } else v = ms / MS[n];
      res.values[n + 's'] = v; res[n + 's'] = v;
    });
    res.as = function (u) { var n = normUnit(u); return n in MS ? ms / MS[n] : res.values[n + 's']; };
    res.toObject = function () { return res.values; };
    res.toMillis = function () { return ms; };
    return res;
  };
  P.diffTo = function (other, unit) { return DateTime.fromAny(other).diff(this, unit || 'days').as(unit || 'days'); };
  P.diffToNow = function (unit) { return DateTime.now().diff(this, unit || 'days').as(unit || 'days'); };
  P.equals = function (o) { return o instanceof DT && o.ts === this.ts && o.zoneName === this.zoneName; };
  P.hasSame = function (o, unit) { return this.startOf(unit).ts === DateTime.fromAny(o).setZone(this.zoneName).startOf(unit).ts; };
  P.isBetween = function (a, b) { var x = DateTime.fromAny(a).ts, y = DateTime.fromAny(b).ts; return this.ts > Math.min(x, y) && this.ts < Math.max(x, y); };
  P.extract = function (unit) { return this[normUnit(unit || 'week') === 'week' ? 'weekNumber' : normUnit(unit)]; };
  P.toRelative = function () {
    var s = Math.round((this.ts - Date.now()) / 1e3), a = Math.abs(s), u, n;
    if (a < 60) { n = a; u = 'second'; } else if (a < 3600) { n = Math.round(a / 60); u = 'minute'; }
    else if (a < 86400) { n = Math.round(a / 3600); u = 'hour'; } else { n = Math.round(a / 86400); u = 'day'; }
    var w = n + ' ' + u + (n === 1 ? '' : 's');
    return s < 0 ? w + ' ago' : 'in ' + w;
  };

  var MONTH_IDX = {};
  MONTHS.forEach(function (m, i) { MONTH_IDX[m.toLowerCase()] = i + 1; MONTH_IDX[m.slice(0, 3).toLowerCase()] = i + 1; });
  var PARSE = {
    yyyy: ['(\\d{4})', 'year'], yy: ['(\\d{2})', 'yy'], y: ['(\\d{1,4})', 'year'],
    MMMM: ['([A-Za-z]+)', 'mname'], MMM: ['([A-Za-z]{3})', 'mname'], MM: ['(\\d{2})', 'month'], M: ['(\\d{1,2})', 'month'],
    LLLL: ['([A-Za-z]+)', 'mname'], LLL: ['([A-Za-z]{3})', 'mname'], LL: ['(\\d{2})', 'month'], L: ['(\\d{1,2})', 'month'],
    dd: ['(\\d{2})', 'day'], d: ['(\\d{1,2})', 'day'], EEEE: ['[A-Za-z]+', null], EEE: ['[A-Za-z]{3}', null], cccc: ['[A-Za-z]+', null], ccc: ['[A-Za-z]{3}', null],
    HH: ['(\\d{2})', 'hour'], H: ['(\\d{1,2})', 'hour'], hh: ['(\\d{2})', 'hour12'], h: ['(\\d{1,2})', 'hour12'],
    mm: ['(\\d{2})', 'minute'], m: ['(\\d{1,2})', 'minute'], ss: ['(\\d{2})', 'second'], s: ['(\\d{1,2})', 'second'],
    SSS: ['(\\d{3})', 'millisecond'], S: ['(\\d{1,3})', 'millisecond'], a: ['(AM|PM|am|pm)', 'ampm'],
    ZZ: ['([+-]\\d{2}:\\d{2}|Z)', 'off'], ZZZ: ['([+-]\\d{4}|Z)', 'off'], Z: ['([+-]\\d{1,2}(?::\\d{2})?|Z)', 'off'],
    X: ['(\\d+)', 'unix'], x: ['(\\d+)', 'unixms']
  };
  function parseFormat(str, fmt, opts) {
    var fields = [], re = '^' + String(fmt).replace(/'[^']*'|[A-Za-z]+|[^A-Za-z']+/g, function (t) {
      if (t.charAt(0) === "'") return t.slice(1, -1).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      if (/^[A-Za-z]+$/.test(t)) {
        var out = '';
        while (t.length) {
          var hit = null;
          for (var len = Math.min(4, t.length); len > 0; len--) if (PARSE[t.slice(0, len)]) { hit = t.slice(0, len); break; }
          if (!hit) { out += t.charAt(0).replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); t = t.slice(1); continue; }
          out += PARSE[hit][0]; if (PARSE[hit][1]) fields.push(PARSE[hit][1]); t = t.slice(hit.length);
        }
        return out;
      }
      return t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }) + '$';
    var m = new RegExp(re).exec(String(str).trim());
    if (!m) return new DT(NaN, (opts && opts.zone) || __defaultZone());
    var o = {}, ampm, off;
    fields.forEach(function (f, i) {
      var v = m[i + 1];
      if (f === 'mname') o.month = MONTH_IDX[v.toLowerCase()];
      else if (f === 'yy') o.year = 2000 + Number(v);
      else if (f === 'hour12') o.hour = Number(v) % 12;
      else if (f === 'ampm') ampm = v.toLowerCase();
      else if (f === 'off') off = v;
      else if (f === 'unix') o.unix = Number(v) * 1e3;
      else if (f === 'unixms') o.unix = Number(v);
      else o[f] = Number(v);
    });
    if (o.unix !== undefined) return new DT(o.unix, (opts && opts.zone) || __defaultZone());
    if (ampm === 'pm' && o.hour < 12) o.hour += 12;
    if (off) {
      var mins = off === 'Z' ? 0 : (off.charAt(0) === '-' ? -1 : 1) * (Number(off.slice(1, 3)) * 60 + Number(off.replace(':', '').slice(3, 5) || 0));
      var ts = Date.UTC(o.year || 1970, (o.month || 1) - 1, o.day || 1, o.hour || 0, o.minute || 0, o.second || 0, o.millisecond || 0) - mins * 6e4;
      return new DT(ts, (opts && opts.zone) || __defaultZone());
    }
    return fromParts(o, (opts && opts.zone) || __defaultZone());
  }

  var DateTime = {
    now: function () { return new DT(Date.now()); },
    local: function (y, mo, d, h, mi, s, ms) {
      if (y === undefined) return DateTime.now();
      return fromParts({ year: y, month: mo, day: d, hour: h, minute: mi, second: s, millisecond: ms });
    },
    utc: function (y, mo, d, h, mi, s, ms) {
      if (y === undefined) return new DT(Date.now(), 'UTC');
      return fromParts({ year: y, month: mo, day: d, hour: h, minute: mi, second: s, millisecond: ms }, 'UTC');
    },
    fromMillis: function (ms, o) { return new DT(Number(ms), o && o.zone); },
    fromSeconds: function (s, o) { return new DT(Number(s) * 1e3, o && o.zone); },
    fromJSDate: function (d, o) { return new DT(d.getTime(), o && o.zone); },
    fromObject: function (obj, o) { return fromParts(obj || {}, o && o.zone); },
    fromISO: function (s, o) {
      s = String(s).trim();
      var m = /^(\d{4})(?:-?(\d{2})(?:-?(\d{2}))?)?(?:[T ](\d{2})(?::?(\d{2})(?::?(\d{2})(?:[.,](\d{1,9}))?)?)?)?\s*(Z|[+-]\d{2}(?::?\d{2})?)?$/i.exec(s);
      var zone = (o && o.zone) || __defaultZone();
      if (!m) return new DT(NaN, zone);
      var parts = { year: +m[1], month: +(m[2] || 1), day: +(m[3] || 1), hour: +(m[4] || 0), minute: +(m[5] || 0), second: +(m[6] || 0), millisecond: m[7] ? +(m[7] + '00').slice(0, 3) : 0 };
      if (m[8]) {
        var z = m[8].toUpperCase(), mins = z === 'Z' ? 0 : (z.charAt(0) === '-' ? -1 : 1) * (+z.slice(1, 3) * 60 + +(z.replace(':', '').slice(3, 5) || 0));
        var ts = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second, parts.millisecond) - mins * 6e4;
        return new DT(ts, (o && o.setZone) ? (z === 'Z' ? 'UTC' : zone) : zone);
      }
      return fromParts(parts, zone);
    },
    fromSQL: function (s, o) { return DateTime.fromISO(String(s).replace(' ', 'T'), o); },
    fromRFC2822: function (s, o) { return new DT(Date.parse(s), o && o.zone); },
    fromHTTP: function (s, o) { return new DT(Date.parse(s), o && o.zone); },
    fromFormat: function (s, fmt, o) { return parseFormat(s, fmt, o); },
    fromAny: function (v) {
      if (v instanceof DT) return v;
      if (v instanceof Date) return new DT(v.getTime());
      if (typeof v === 'number') return new DT(v);
      var d = DateTime.fromISO(v);
      return d.isValid ? d : new DT(Date.parse(v));
    },
    isDateTime: function (v) { return v instanceof DT; },
    max: function () { return Array.prototype.slice.call(arguments).reduce(function (a, b) { return a.ts >= b.ts ? a : b; }); },
    min: function () { return Array.prototype.slice.call(arguments).reduce(function (a, b) { return a.ts <= b.ts ? a : b; }); },
    invalid: function (r) { var d = new DT(NaN); d.invalidReason = r; return d; },
    DATE_SHORT: { month: 'numeric' }, DATE_MED: { month: 'short' }, DATE_MED_WITH_WEEKDAY: { month: 'short', weekday: 'short' },
    DATE_FULL: { month: 'long' }, DATE_HUGE: { month: 'long', weekday: 'long' },
    TIME_SIMPLE: { hour: 1 }, TIME_WITH_SECONDS: { hour: 1, second: 1 }, TIME_WITH_SHORT_OFFSET: { hour: 1, second: 1, timeZoneName: 1 },
    TIME_24_SIMPLE: { hour: 1, hourCycle: 'h23' }, TIME_24_WITH_SECONDS: { hour: 1, second: 1, hourCycle: 'h23' },
    TIME_24_WITH_SHORT_OFFSET: { hour: 1, second: 1, hourCycle: 'h23', timeZoneName: 1 },
    DATETIME_SHORT: { month: 'numeric', hour: 1 }, DATETIME_MED: { month: 'short', hour: 1 },
    DATETIME_FULL: { month: 'long', hour: 1, timeZoneName: 1 }, DATETIME_HUGE: { month: 'long', weekday: 'long', hour: 1, timeZoneName: 1 },
    DATETIME_SHORT_WITH_SECONDS: { month: 'numeric', hour: 1, second: 1 }, DATETIME_MED_WITH_SECONDS: { month: 'short', hour: 1, second: 1 }
  };
  var Duration = {
    fromObject: function (o) {
      var ms = 0; Object.keys(o || {}).forEach(function (k) { ms += (MS[normUnit(k)] || 0) * o[k]; });
      return { __dur: o, toMillis: function () { return ms; }, as: function (u) { return ms / MS[normUnit(u)]; }, toObject: function () { return o; } };
    },
    fromMillis: function (ms) { return Duration.fromObject({ milliseconds: ms }); }
  };
  g.DateTime = DateTime; g.Duration = Duration; g.Interval = {};
  g.__isDT = function (v) { return v instanceof DT; };

  // ---------------------------------------------------------------- Buffer
  function Buf(b64) { this.__b64 = b64; this.length = __b64len(b64); }
  Buf.prototype.toString = function (enc) {
    enc = (enc || 'utf8').toLowerCase();
    if (enc === 'base64') return this.__b64;
    if (enc === 'hex') return __b64hex(this.__b64);
    return __b64dec(this.__b64);
  };
  Buf.prototype.toJSON = function () { return { type: 'Buffer', base64: this.__b64 }; };
  g.Buffer = {
    from: function (data, enc) {
      if (data instanceof Buf) return new Buf(data.__b64);
      enc = (enc || 'utf8').toLowerCase();
      if (enc === 'base64') return new Buf(__b64norm(String(data)));
      if (enc === 'hex') return new Buf(__hexb64(String(data)));
      return new Buf(__b64enc(String(data)));
    },
    isBuffer: function (v) { return v instanceof Buf; },
    byteLength: function (s) { return __b64len(__b64enc(String(s))); }
  };
  g.btoa = function (s) { return __b64enc(String(s)); };
  g.atob = function (s) { return __b64dec(String(s)); };

  // ---------------------------------------------------------------- Code node helpers & modules
  function bufB64(v, enc) {
    if (v instanceof Buf) return v.__b64;
    enc = (enc || 'utf8').toLowerCase();
    if (enc === 'base64') return __b64norm(String(v));
    if (enc === 'hex') return __hexb64(String(v));
    return __b64enc(String(v));
  }
  function promised(fn) {
    return function () {
      var self = this, args = arguments;
      return new Promise(function (res, rej) { try { res(fn.apply(self, args)); } catch (e) { rej(e); } });
    };
  }
  function httpRequest(opts) {
    if (typeof opts === 'string') opts = { url: opts };
    opts = Object.assign({}, opts || {});
    if (opts.uri && !opts.url) opts.url = opts.uri;
    if (opts.body instanceof Buf) { opts.__bodyB64 = opts.body.__b64; delete opts.body; }
    var r = JSON.parse(__httpRequest(JSON.stringify(opts)));
    if (r.binary !== undefined) r.body = new Buf(r.binary);
    return opts.returnFullResponse || opts.resolveWithFullResponse ? { body: r.body, headers: r.headers, statusCode: r.statusCode, statusMessage: r.statusMessage } : r.body;
  }
  g.__httpSync = httpRequest;
  g.helpers = {
    httpRequest: promised(httpRequest),
    request: promised(function (opts) { if (opts && typeof opts === 'object' && opts.json === undefined) opts = Object.assign({ json: false }, opts); return httpRequest(opts); }),
    prepareBinaryData: promised(function (data, fileName, mimeType) { return JSON.parse(__binaryData(bufB64(data), fileName || '', mimeType || '')); }),
    getBinaryDataBuffer: promised(function (itemIndex, prop) {
      var it = g.$input.all()[itemIndex];
      var b = it && it.binary && it.binary[prop];
      if (!b) throw new Error('item ' + itemIndex + ' has no binary property "' + prop + '"');
      return new Buf(b.data);
    }),
    binaryToBuffer: promised(function (b) { return b instanceof Buf ? b : new Buf(String(b)); }),
    returnJsonArray: function (d) { return (Array.isArray(d) ? d : [d]).map(function (j) { return { json: j }; }); }
  };
  function hasher(alg, key) {
    var chunks = [], h = {
      update: function (d, enc) { chunks.push(bufB64(d, enc)); return h; },
      digest: function (enc) {
        var out = __digest(String(alg), key === undefined ? '' : bufB64(key), JSON.stringify(chunks), key !== undefined);
        enc = enc && String(enc).toLowerCase();
        if (enc === 'hex') return __b64hex(out);
        if (enc === 'base64') return out;
        if (enc === 'base64url') return out.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
        return new Buf(out);
      }
    };
    return h;
  }
  var cryptoModule = {
    createHash: function (alg) { return hasher(alg); },
    createHmac: function (alg, key) { return hasher(alg, key); },
    randomUUID: function () { return __uuid(); },
    randomBytes: function (n) { return new Buf(__randomB64(Number(n))); },
    randomInt: function (min, max) { if (max === undefined) { max = min; min = 0; } return min + Math.floor(__randomFloat() * (max - min)); },
    timingSafeEqual: function (a, b) { return bufB64(a) === bufB64(b); }
  };
  function inspect(v) { if (typeof v === 'string') return "'" + v + "'"; try { return JSON.stringify(v, null, 2); } catch (e) { return String(v); } }
  var utilModule = {
    format: function (f) {
      var args = Array.prototype.slice.call(arguments, 1), i = 0;
      var s = String(f).replace(/%[sdifjoO%]/g, function (m) {
        if (m === '%%') return '%';
        if (i >= args.length) return m;
        var a = args[i++];
        switch (m) {
          case '%d': case '%i': return String(m === '%i' ? parseInt(a, 10) : Number(a));
          case '%f': return String(parseFloat(a));
          case '%j': return JSON.stringify(a);
          case '%o': case '%O': return inspect(a);
          default: return typeof a === 'object' ? inspect(a) : String(a);
        }
      });
      while (i < args.length) { var a = args[i++]; s += ' ' + (typeof a === 'object' ? inspect(a) : String(a)); }
      return s;
    },
    inspect: inspect,
    isDeepStrictEqual: function (a, b) { return JSON.stringify(a) === JSON.stringify(b); },
    promisify: function (fn) { return function () { var args = Array.prototype.slice.call(arguments); return new Promise(function (res, rej) { args.push(function (e, v) { if (e) rej(e); else res(v); }); fn.apply(null, args); }); }; },
    types: { isDate: function (v) { return v instanceof Date; }, isRegExp: function (v) { return v instanceof RegExp; } }
  };
  g.require = function (name) {
    name = String(name).replace(/^node:/, '');
    if (name === 'crypto') return cryptoModule;
    if (name === 'util') return utilModule;
    throw new Error("Cannot find module '" + name + "' (only 'crypto' and 'util' are available)");
  };

  // ---------------------------------------------------------------- helpers
  g.console = { log: __log, info: __log, warn: __log, error: __log, debug: __log };
  g.$if = function (c, a, b) { return c ? a : b; };
  g.$ifEmpty = function (v, d) {
    if (v === undefined || v === null || v === '' || (Array.isArray(v) && !v.length) || (isObj(v) && !Object.keys(v).length)) return d;
    return v;
  };
  g.$max = function () { return Math.max.apply(null, arguments); };
  g.$min = function () { return Math.min.apply(null, arguments); };

  // __export converts a JS value to a JSON string for the Go side.
  g.__export = function (v) {
    if (v === undefined) return '{"u":1}';
    return JSON.stringify({ v: v }, function (k, x) {
      if (typeof x === 'function') return undefined;
      if (typeof x === 'number' && !isFinite(x)) return null;
      return x;
    });
  };
  g.__str = function (v) {
    if (v === undefined || v === null) return '';
    if (v instanceof DT) return v.toISO();
    return String(v);
  };
})(this);
