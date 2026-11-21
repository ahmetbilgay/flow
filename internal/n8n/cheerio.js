// cheerio subset for Code nodes (require('cheerio')). Documents live on the
// Go side (cheerio.go); a selection holds integer node handles and the
// element objects handed to callbacks mirror cheerio's domhandler nodes
// (type, name, attribs, data, parent, children, prev, next).
(function () {
  'use strict';

  var nodes = {};

  function list(x) {
    var out = [];
    if (x) for (var i = 0; i < x.length; i++) out.push(x[i]);
    return out;
  }
  function isNode(v) { return v instanceof Node; }
  function isSel(v) { return v instanceof Cheerio; }
  function handle(id) { return id < 0 ? null : node(id); }

  // ---------------------------------------------------------------- nodes
  function Node(id) {
    var info = __chInfo(id) || {};
    Object.defineProperty(this, '__id', { value: id });
    this.type = info.type;
    if (info.name !== undefined) { this.name = info.name; this.tagName = info.name; }
    if (info.data !== undefined) this.data = info.data;
  }
  function node(id) {
    return nodes[id] || (nodes[id] = new Node(id));
  }
  function link(n, k) { return handle(__chLink(n.__id)[k]); }
  Object.defineProperties(Node.prototype, {
    attribs: { get: function () { var a = __chAttrs(this.__id), o = {}; for (var k in a) o[k] = a[k]; return o; } },
    parent: { get: function () { return link(this, 0); } },
    parentNode: { get: function () { return link(this, 0); } },
    prev: { get: function () { return link(this, 1); } },
    previousSibling: { get: function () { return link(this, 1); } },
    next: { get: function () { return link(this, 2); } },
    nextSibling: { get: function () { return link(this, 2); } },
    firstChild: { get: function () { return link(this, 3); } },
    lastChild: { get: function () { return link(this, 4); } },
    children: { get: function () { return list(__chRel([this.__id], 'contents', '')).map(node); } },
    childNodes: { get: function () { return this.children; } },
    nodeType: { get: function () { return { tag: 1, script: 1, style: 1, text: 3, comment: 8, root: 9, directive: 10 }[this.type] || 1; } },
    nodeValue: { get: function () { return this.data === undefined ? null : this.data; } }
  });

  // ---------------------------------------------------------------- helpers
  function idsOf(items) {
    var out = [], seen = {};
    for (var i = 0; i < items.length; i++) {
      var it = items[i];
      if (isNode(it) && !seen[it.__id]) { seen[it.__id] = 1; out.push(it.__id); }
    }
    return out;
  }
  function str(v) { return v === undefined || v === null ? '' : String(v); }
  // content converts insertion arguments (HTML, nodes, selections, arrays)
  // to handles.
  function content(args, $, el, i) {
    var out = [];
    for (var k = 0; k < args.length; k++) {
      var a = args[k];
      if (typeof a === 'function') a = a.call(el, i, __chHTML([el.__id], false) || '');
      if (a === undefined || a === null) continue;
      if (isSel(a)) out = out.concat(idsOf(a.toArray()));
      else if (isNode(a)) out.push(a.__id);
      else if (Array.isArray(a)) out = out.concat(content(a, $, el, i));
      else out = out.concat(list(__chParse(String(a))));
    }
    return out;
  }
  function camel(s) { return s.replace(/-([a-z])/g, function (_, c) { return c.toUpperCase(); }); }
  function dashed(s) { return s.replace(/[A-Z]/g, function (c) { return '-' + c.toLowerCase(); }); }
  function dataValue(v) {
    if (v === 'true') return true;
    if (v === 'false') return false;
    if (v === 'null') return null;
    if (v !== '' && String(Number(v)) === v) return Number(v);
    if (/^[\[{]/.test(v)) { try { return JSON.parse(v); } catch (e) { /* keep string */ } }
    return v;
  }
  function classes(el) { return str(el.attribs['class']).split(/\s+/).filter(Boolean); }
  function parseStyle(s) {
    var out = {};
    str(s).split(';').forEach(function (d) {
      var i = d.indexOf(':');
      if (i > 0) out[d.slice(0, i).trim()] = d.slice(i + 1).trim();
    });
    return out;
  }
  function matcher(sel, $) {
    if (typeof sel === 'function') return sel;
    if (typeof sel === 'string') return function (i, el) { return __chIs([el.__id], sel); };
    var set = {};
    idsOf(isSel(sel) ? sel.toArray() : isNode(sel) ? [sel] : list(sel)).forEach(function (id) { set[id] = 1; });
    return function (i, el) { return !!set[el.__id]; };
  }

  // ---------------------------------------------------------------- selection
  function Cheerio(items, $, prevObject) {
    for (var i = 0; i < items.length; i++) this[i] = items[i];
    Object.defineProperty(this, 'length', { value: items.length, writable: true });
    Object.defineProperty(this, '_root', { value: $ });
    Object.defineProperty(this, 'prevObject', { value: prevObject });
  }
  var fn = Cheerio.prototype;
  fn.cheerio = '[cheerio object]';
  function wrapIDs(ids, $, prev) { return new Cheerio(list(ids).map(node), $, prev); }
  fn._ids = function () { return idsOf(this.toArray()); };
  fn._make = function (ids) { return wrapIDs(ids, this._root, this); };
  fn._nodes = function () { return this.toArray().filter(isNode); };

  // traversal
  fn.find = function (sel) {
    if (typeof sel === 'string') return this._make(__chFind(this._ids(), sel));
    var self = this._ids(), within = {};
    self.forEach(function (id) { within[id] = 1; });
    var out = idsOf(isSel(sel) ? sel.toArray() : [sel]).filter(function (id) {
      for (var p = node(id).parent; p; p = p.parent) if (within[p.__id]) return true;
      return false;
    });
    return this._make(out);
  };
  ['children', 'parent', 'parents', 'next', 'prev', 'nextAll', 'prevAll', 'siblings'].forEach(function (rel) {
    fn[rel] = function (sel) { return this._make(__chRel(this._ids(), rel, sel === undefined ? '' : String(sel))); };
  });
  ['closest', 'parentsUntil', 'nextUntil', 'prevUntil'].forEach(function (rel) {
    fn[rel] = function (sel) {
      if (sel === undefined || sel === null) {
        if (rel === 'closest') return this._make([]);
        return this[{ parentsUntil: 'parents', nextUntil: 'nextAll', prevUntil: 'prevAll' }[rel]]();
      }
      return this._make(__chRel(this._ids(), rel, String(sel)));
    };
  });
  fn.contents = function () { return this._make(__chRel(this._ids(), 'contents', '')); };
  fn.has = function (sel) {
    if (typeof sel === 'string') return this._make(__chRel(this._ids(), 'has', sel));
    var inner = this._root(sel);
    return this.filter(function (i, el) { return this._root(el).find(inner).length > 0; }.bind(this));
  };
  fn.filter = function (sel) {
    if (typeof sel === 'string') return this._make(__chFilter(this._ids(), sel));
    var m = matcher(sel, this._root);
    return new Cheerio(this.toArray().filter(function (el, i) { return isNode(el) && m.call(el, i, el); }), this._root, this);
  };
  fn.not = function (sel) {
    if (typeof sel === 'string') return this._make(__chNot(this._ids(), sel));
    var m = matcher(sel, this._root);
    return new Cheerio(this.toArray().filter(function (el, i) { return isNode(el) && !m.call(el, i, el); }), this._root, this);
  };
  fn.is = function (sel) {
    if (!sel) return false;
    if (typeof sel === 'string') return __chIs(this._ids(), sel);
    var m = matcher(sel, this._root);
    return this.toArray().some(function (el, i) { return isNode(el) && !!m.call(el, i, el); });
  };
  fn.first = function () { return this.eq(0); };
  fn.last = function () { return this.eq(-1); };
  fn.eq = function (i) {
    i = +i;
    if (i < 0) i += this.length;
    return new Cheerio(i >= 0 && i < this.length ? [this[i]] : [], this._root, this);
  };
  fn.get = function (i) {
    if (i === undefined) return this.toArray();
    i = +i;
    return this[i < 0 ? i + this.length : i];
  };
  fn.toArray = function () { var out = []; for (var i = 0; i < this.length; i++) out.push(this[i]); return out; };
  fn.slice = function (a, b) { return new Cheerio(this.toArray().slice(a, b), this._root, this); };
  fn.index = function (sel) {
    if (sel === undefined) {
      var el = this[0];
      if (!el) return -1;
      return this._root(el).prevAll().length;
    }
    var target = typeof sel === 'string' ? this._root(sel)[0] : isSel(sel) ? sel[0] : sel;
    for (var i = 0; i < this.length; i++) if (this[i] === target) return i;
    return -1;
  };
  fn.each = function (cb) {
    for (var i = 0; i < this.length; i++) if (cb.call(this[i], i, this[i]) === false) break;
    return this;
  };
  fn.map = function (cb) {
    var out = [];
    for (var i = 0; i < this.length; i++) {
      var v = cb.call(this[i], i, this[i]);
      if (v === undefined || v === null) continue;
      out = out.concat(Array.isArray(v) ? v : [v]);
    }
    return new Cheerio(out, this._root, this);
  };
  fn.add = function (sel, ctx) {
    var more = this._root(sel, ctx);
    return this._make(idsOf(this.toArray().concat(more.toArray())));
  };
  fn.addBack = function (sel) {
    var prev = this.prevObject ? this.prevObject : this._make([]);
    if (sel) prev = prev.filter(sel);
    return this._make(idsOf(this.toArray().concat(prev.toArray())));
  };
  fn.end = function () { return this.prevObject || this._make([]); };

  // content
  fn.text = function (t) {
    if (t === undefined) return __chText(this._ids());
    var self = this;
    return this.each(function (i, el) { __chSetText([el.__id], typeof t === 'function' ? str(t.call(el, i, self._root(el).text())) : str(t)); });
  };
  fn.html = function (h) {
    if (h === undefined) return __chHTML(this._ids(), false);
    return this.each(function (i, el) {
      __chEmpty([el.__id]);
      var c = content([h], null, el, i);
      if (c.length) __chInsert([el.__id], 'append', c);
    });
  };
  fn.toString = function () { return __chHTML(this._ids(), true) || ''; };

  // attributes
  fn.attr = function (name, value) {
    if (name !== null && typeof name === 'object') {
      for (var k in name) this.attr(k, name[k]);
      return this;
    }
    if (value === undefined) {
      var el = this._nodes()[0];
      if (!el) return undefined;
      if (name === undefined) return el.attribs;
      var a = el.attribs;
      return Object.prototype.hasOwnProperty.call(a, String(name).toLowerCase()) ? a[String(name).toLowerCase()] : undefined;
    }
    return this.each(function (i, el) {
      if (!isNode(el)) return;
      var v = typeof value === 'function' ? value.call(el, i, el.attribs[name]) : value;
      __chSetAttr([el.__id], String(name), v === null ? null : String(v));
    });
  };
  fn.removeAttr = function (name) {
    var names = String(name).split(/\s+/).filter(Boolean), ids = this._ids();
    names.forEach(function (n) { __chSetAttr(ids, n, null); });
    return this;
  };
  fn.prop = function (name, value) {
    if (value !== undefined) {
      if (typeof value === 'boolean') return this.each(function (i, el) { __chSetAttr([el.__id], String(name), value ? '' : null); });
      return this.attr(name, value);
    }
    var el = this._nodes()[0];
    if (!el) return undefined;
    switch (name) {
      case 'tagName': case 'nodeName': return el.name ? el.name.toUpperCase() : undefined;
      case 'innerHTML': return __chHTML([el.__id], false);
      case 'outerHTML': return __chHTML([el.__id], true);
      case 'textContent': case 'innerText': return __chText([el.__id]);
      case 'checked': case 'selected': case 'disabled': case 'readonly': case 'required': case 'multiple':
        return Object.prototype.hasOwnProperty.call(el.attribs, name);
      case 'nodeType': return el.nodeType;
      case 'attributes': return el.attribs;
    }
    return el.attribs[name];
  };
  fn.data = function (key, value) {
    if (key !== null && typeof key === 'object') {
      for (var k in key) this.data(k, key[k]);
      return this;
    }
    if (value !== undefined) return this.attr('data-' + dashed(key), typeof value === 'object' ? JSON.stringify(value) : value);
    var el = this._nodes()[0];
    if (!el) return undefined;
    var a = el.attribs;
    if (key === undefined) {
      var out = {};
      for (var n in a) if (n.indexOf('data-') === 0) out[camel(n.slice(5))] = dataValue(a[n]);
      return out;
    }
    var attr = 'data-' + dashed(key);
    return Object.prototype.hasOwnProperty.call(a, attr) ? dataValue(a[attr]) : undefined;
  };
  fn.val = function (v) {
    var el = this._nodes()[0];
    if (v !== undefined) {
      return this.each(function (i, e) {
        if (e.name === 'textarea') __chSetText([e.__id], str(v));
        else if (e.name === 'select') {
          var want = [].concat(v).map(String);
          wrapIDs(__chFind([e.__id], 'option'), null).each(function (k, o) {
            var ov = o.attribs.value !== undefined ? o.attribs.value : __chText([o.__id]);
            __chSetAttr([o.__id], 'selected', want.indexOf(ov) >= 0 ? '' : null);
          });
        } else __chSetAttr([e.__id], 'value', str(v));
      });
    }
    if (!el) return undefined;
    if (el.name === 'textarea') return __chText([el.__id]);
    if (el.name === 'select') {
      var optVal = function (o) { return o.attribs.value !== undefined ? o.attribs.value : __chText([o.__id]); };
      var sel = list(__chFind([el.__id], 'option[selected]')).map(node);
      if ('multiple' in el.attribs) return sel.map(optVal);
      if (sel.length) return optVal(sel[0]);
      var first = list(__chFind([el.__id], 'option')).map(node)[0];
      return first ? optVal(first) : undefined;
    }
    if (el.name === 'option') return el.attribs.value !== undefined ? el.attribs.value : __chText([el.__id]);
    return el.attribs.value;
  };
  fn.hasClass = function (c) { return this._nodes().some(function (el) { return classes(el).indexOf(c) >= 0; }); };
  function editClass(self, names, op) {
    return self.each(function (i, el) {
      if (!isNode(el) || el.type !== 'tag') return;
      var list = typeof names === 'function' ? names.call(el, i, str(el.attribs['class'])) : names;
      var cur = classes(el);
      str(list).split(/\s+/).filter(Boolean).forEach(function (c) {
        var at = cur.indexOf(c);
        if (op === 'add' || (op === 'toggle' && at < 0)) { if (at < 0) cur.push(c); } else if (at >= 0) cur.splice(at, 1);
      });
      __chSetAttr([el.__id], 'class', cur.join(' '));
    });
  }
  fn.addClass = function (c) { return editClass(this, c, 'add'); };
  fn.removeClass = function (c) {
    if (c === undefined) return this.removeAttr('class');
    return editClass(this, c, 'remove');
  };
  fn.toggleClass = function (c, state) { return editClass(this, c, state === undefined ? 'toggle' : state ? 'add' : 'remove'); };
  fn.css = function (prop, value) {
    if (prop !== null && typeof prop === 'object') {
      for (var k in prop) this.css(k, prop[k]);
      return this;
    }
    if (value === undefined) {
      var el = this._nodes()[0];
      if (!el) return undefined;
      var st = parseStyle(el.attribs.style);
      return prop === undefined ? st : st[prop];
    }
    return this.each(function (i, el) {
      var st = parseStyle(el.attribs.style);
      if (value === '' || value === null) delete st[prop]; else st[prop] = String(value);
      var s = Object.keys(st).map(function (k) { return k + ': ' + st[k] + ';'; }).join(' ');
      __chSetAttr([el.__id], 'style', s || null);
    });
  };

  // manipulation
  function inserter(where) {
    return function () {
      var args = arguments, self = this, ids = this._ids();
      ids.forEach(function (id, i) {
        var c = content(args, self._root, node(id), i);
        // The last target receives the original nodes, earlier ones clones.
        if (c.length) __chInsert([id], where, i === ids.length - 1 ? c : list(__chClone(c)));
      });
      return this;
    };
  }
  fn.append = inserter('append');
  fn.prepend = inserter('prepend');
  fn.before = inserter('before');
  fn.after = inserter('after');
  fn.replaceWith = function (c) {
    inserter('replace').call(this, c);
    return this;
  };
  function inserterTo(where) {
    return function (target) {
      var t = typeof target === 'string' ? this._root(target) : isSel(target) ? target : this._root(target);
      t[where](this);
      return this;
    };
  }
  fn.appendTo = inserterTo('append');
  fn.prependTo = inserterTo('prepend');
  fn.insertBefore = inserterTo('before');
  fn.insertAfter = inserterTo('after');
  fn.wrap = function (w) {
    var self = this;
    return this.each(function (i, el) {
      var wrapper = list(__chClone(content([w], self._root, el, i)))[0];
      if (wrapper === undefined) return;
      __chInsert([el.__id], 'before', [wrapper]);
      var inner = wrapper;
      for (var c = list(__chRel([inner], 'children', '')); c.length; c = list(__chRel([inner], 'children', ''))) inner = c[0];
      __chInsert([inner], 'append', [el.__id]);
    });
  };
  fn.unwrap = function (sel) {
    this.parent(sel).not('body').each(function (i, p) {
      var kids = list(__chRel([p.__id], 'contents', ''));
      if (kids.length) __chInsert([p.__id], 'replace', kids); else __chRemove([p.__id]);
    });
    return this;
  };
  fn.remove = function (sel) {
    __chRemove((sel ? this.filter(sel) : this)._ids());
    return this;
  };
  fn.empty = function () { __chEmpty(this._ids()); return this; };
  fn.clone = function () { return this._make(__chClone(this._ids())); };
  fn.serializeArray = function () {
    var out = [];
    var fields = this._make(__chFind(this._ids(), 'input,select,textarea')).add(this.filter('input,select,textarea'));
    fields.filter('[name]').not(':disabled,[type=submit],[type=button],[type=reset],[type=file],[type=image]').each(function (i, el) {
      var $el = wrapIDs([el.__id], null);
      var type = str(el.attribs.type).toLowerCase();
      if ((type === 'checkbox' || type === 'radio') && !('checked' in el.attribs)) return;
      var v = el.name === 'input' && (type === 'checkbox' || type === 'radio') && el.attribs.value === undefined ? 'on' : $el.val();
      [].concat(v === undefined ? '' : v).forEach(function (x) { out.push({ name: el.attribs.name, value: str(x).replace(/\r?\n/g, '\r\n') }); });
    });
    return out;
  };
  fn.serialize = function () {
    return this.serializeArray().map(function (p) {
      return encodeURIComponent(p.name) + '=' + encodeURIComponent(p.value).replace(/%20/g, '+');
    }).join('&');
  };
  fn.extract = function (map) {
    var out = {}, self = this;
    Object.keys(map).forEach(function (key) {
      var spec = map[key], many = Array.isArray(spec);
      if (many) spec = spec[0];
      if (typeof spec === 'string') spec = { selector: spec, value: 'textContent' };
      var found = self.find(spec.selector);
      var pick = function (el) {
        var v = spec.value;
        if (typeof v === 'function') return v(el, key);
        if (v !== null && typeof v === 'object') return wrapIDs([el.__id], self._root).extract(v);
        return wrapIDs([el.__id], self._root).prop(v || 'textContent');
      };
      out[key] = many ? found.toArray().map(pick) : found.length ? pick(found[0]) : undefined;
    });
    return out;
  };
  if (typeof Symbol === 'function' && Symbol.iterator) {
    fn[Symbol.iterator] = function () { return this.toArray()[Symbol.iterator](); };
  }

  // ---------------------------------------------------------------- load
  function load(src, options, isDocument) {
    var rootID;
    if (isNode(src)) rootID = src.__id;
    else {
      if (Array.isArray(src)) src = src.map(function (n) { return isNode(n) ? __chHTML([n.__id], true) : str(n); }).join('');
      rootID = __chLoad(str(src), isDocument === false);
    }
    var root;
    function $(sel, ctx) {
      if (sel === undefined || sel === null || sel === '') return new Cheerio([], $);
      if (isSel(sel)) return new Cheerio(sel.toArray(), $);
      if (isNode(sel)) return new Cheerio([sel], $);
      if (Array.isArray(sel)) return new Cheerio(sel.filter(isNode), $);
      if (typeof sel === 'function') return new Cheerio([], $);
      sel = String(sel);
      if (/^\s*</.test(sel)) return wrapIDs(__chParse(sel.trim()), $);
      if (ctx !== undefined && ctx !== null) return (isSel(ctx) ? ctx : $(ctx)).find(sel);
      return root.find(sel);
    }
    root = wrapIDs([rootID], $);
    $.root = function () { return wrapIDs([rootID], $); };
    $.html = function (sel) {
      if (sel === undefined) return __chHTML([rootID], true) || '';
      return $(sel).toString();
    };
    $.text = function (sel) { return sel === undefined ? __chText([rootID]) : $(sel).text(); };
    $.xml = $.html;
    $.contains = contains;
    $.parseHTML = parseHTML;
    $.merge = merge;
    $.load = load;
    $.extract = function (map) { return root.extract(map); };
    $.fn = fn;
    $._root = root;
    return $;
  }
  function contains(a, b) {
    if (!isNode(a) || !isNode(b) || a === b) return false;
    for (var p = b.parent; p; p = p.parent) if (p === a) return true;
    return false;
  }
  function parseHTML(s) {
    if (typeof s !== 'string' || !s) return null;
    return list(__chParse(s)).map(node);
  }
  function merge(a, b) {
    var n = a.length;
    for (var i = 0; i < b.length; i++) a[n++] = b[i];
    a.length = n;
    return a;
  }
  function staticHTML(x) {
    if (x === undefined) return '';
    if (isSel(x)) return x.toString();
    if (isNode(x)) return __chHTML([x.__id], true) || '';
    if (Array.isArray(x)) return x.map(staticHTML).join('');
    return '';
  }
  function staticText(x) {
    if (isSel(x)) return x.text();
    if (isNode(x)) return __chText([x.__id]);
    if (Array.isArray(x)) return x.map(staticText).join('');
    return '';
  }

  var mod = {
    load: load,
    html: staticHTML,
    text: staticText,
    contains: contains,
    parseHTML: parseHTML,
    merge: merge,
    Cheerio: Cheerio
  };
  mod['default'] = mod;
  return mod;
})()
