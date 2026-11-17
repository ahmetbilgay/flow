// LangChain Code node shim: the subset of LangChain JS used by templates,
// backed by the connected model sub-nodes and the outbound HTTP helper.
(function (g) {
  function text(v) {
    if (v === undefined || v === null) return '';
    if (typeof v === 'string') return v;
    if (typeof v.content === 'string') return v.content;
    if (typeof v.value === 'string') return v.value;
    return JSON.stringify(v);
  }
  function role(m) {
    var t = m._getType ? m._getType() : (m.role || m.type || 'user');
    return { human: 'user', ai: 'assistant', system: 'system', user: 'user', assistant: 'assistant', placeholder: 'user' }[t] || 'user';
  }
  function toMessages(input) {
    if (input && input.messages) input = input.messages;
    if (Array.isArray(input)) {
      return input.map(function (m) {
        if (Array.isArray(m)) return { role: role({ role: m[0] }), content: text(m[1]) };
        return { role: role(m), content: text(m) };
      });
    }
    return [{ role: 'user', content: text(input) }];
  }
  function Message(type, content) { this.content = content; this.text = content; this._type = type; }
  Message.prototype._getType = function () { return this._type; };
  Message.prototype.toString = function () { return this.content; };
  Message.prototype.toJSON = function () { return { type: this._type, content: this.content }; };
  function runnable(fn) {
    var r = { invoke: function (x) { return Promise.resolve(x).then(fn); } };
    r.pipe = function (next) {
      return runnable(function (x) { return r.invoke(x).then(function (v) { return typeof next === 'function' ? next(v) : next.invoke(v); }); });
    };
    r.batch = function (xs) { return Promise.all(xs.map(function (x) { return r.invoke(x); })); };
    r.stream = function (x) { return r.invoke(x).then(function (v) { return [v]; }); };
    return r;
  }
  function fill(tpl, vars) {
    vars = vars || {};
    return String(tpl).replace(/\{([A-Za-z_][\w]*)\}/g, function (m, k) { return k in vars ? text(vars[k]) : m; });
  }
  function model(idx) {
    var m = runnable(function (input) { return new Message('ai', __lcChat(idx, JSON.stringify(toMessages(input)))); });
    m.call = function (input) { return m.invoke(input); };
    m._llmType = function () { return 'n8n-model'; };
    return m;
  }
  var prompts = {
    PromptTemplate: {
      fromTemplate: function (tpl) {
        var p = runnable(function (vars) { return fill(tpl, vars); });
        p.format = function (vars) { return Promise.resolve(fill(tpl, vars)); };
        p.template = tpl;
        return p;
      }
    },
    ChatPromptTemplate: {
      fromMessages: function (msgs) {
        var p = runnable(function (vars) {
          return msgs.map(function (m) {
            if (Array.isArray(m)) return new Message({ human: 'human', user: 'human', ai: 'ai', assistant: 'ai', system: 'system' }[m[0]] || 'human', fill(m[1], vars));
            return new Message(m._getType ? m._getType() : 'human', fill(text(m), vars));
          });
        });
        p.formatMessages = function (vars) { return p.invoke(vars); };
        return p;
      },
      fromTemplate: function (tpl) { return prompts.ChatPromptTemplate.fromMessages([['human', tpl]]); }
    }
  };
  var messages = {
    HumanMessage: function (c) { Message.call(this, 'human', typeof c === 'string' ? c : c.content); },
    SystemMessage: function (c) { Message.call(this, 'system', typeof c === 'string' ? c : c.content); },
    AIMessage: function (c) { Message.call(this, 'ai', typeof c === 'string' ? c : c.content); }
  };
  ['HumanMessage', 'SystemMessage', 'AIMessage'].forEach(function (k) { messages[k].prototype = Message.prototype; });
  var parsers = {
    StringOutputParser: function () { var r = runnable(function (v) { return text(v); }); this.invoke = r.invoke; this.pipe = r.pipe; },
    JsonOutputParser: function () { var r = runnable(function (v) { var s = text(v).replace(/^```(json)?|```$/g, '').trim(); return JSON.parse(s); }); this.invoke = r.invoke; this.pipe = r.pipe; }
  };
  function http(opts) { return g.__httpSync(Object.assign({ json: true }, opts)); }
  function OpenAIEmbeddings(cfg) {
    cfg = cfg || {};
    var key = cfg.apiKey || cfg.openAIApiKey, base = (cfg.configuration && cfg.configuration.baseURL) || 'https://api.openai.com/v1';
    var mdl = cfg.model || cfg.modelName || 'text-embedding-3-small';
    function embed(input) {
      var r = http({ method: 'POST', url: base + '/embeddings', headers: { Authorization: 'Bearer ' + key }, body: { model: mdl, input: input } });
      return r.data.map(function (d) { return d.embedding; });
    }
    this.embedDocuments = function (ts) { return Promise.resolve().then(function () { return embed(ts); }); };
    this.embedQuery = function (t) { return Promise.resolve().then(function () { return embed([t])[0]; }); };
  }
  function ChatOpenAI(cfg) {
    cfg = cfg || {};
    var key = cfg.apiKey || cfg.openAIApiKey, base = (cfg.configuration && cfg.configuration.baseURL) || 'https://api.openai.com/v1';
    var body = { model: cfg.model || cfg.modelName || 'gpt-4o-mini' };
    if (cfg.temperature !== undefined) body.temperature = cfg.temperature;
    var r = runnable(function (input) {
      var res = http({ method: 'POST', url: base + '/chat/completions', headers: { Authorization: 'Bearer ' + key }, body: Object.assign({ messages: toMessages(input) }, body) });
      return new Message('ai', res.choices[0].message.content || '');
    });
    this.invoke = r.invoke; this.pipe = r.pipe; this.batch = r.batch;
  }
  var qdrant = {
    QdrantVectorStore: {
      fromExistingCollection: function (embeddings, cfg) {
        var base = String(cfg.url || '').replace(/\/+$/, ''), headers = cfg.apiKey ? { 'api-key': cfg.apiKey } : {};
        var coll = cfg.collectionName;
        function call(method, path, body) { return http({ method: method, url: base + path, headers: headers, body: body }); }
        var client = {
          delete: function (c, body) { return Promise.resolve().then(function () { return call('POST', '/collections/' + encodeURIComponent(c) + '/points/delete?wait=true', body); }); },
          search: function (c, body) { return Promise.resolve().then(function () { return call('POST', '/collections/' + encodeURIComponent(c) + '/points/search', body).result; }); },
          upsert: function (c, body) { return Promise.resolve().then(function () { return call('PUT', '/collections/' + encodeURIComponent(c) + '/points?wait=true', body); }); },
          scroll: function (c, body) { return Promise.resolve().then(function () { return call('POST', '/collections/' + encodeURIComponent(c) + '/points/scroll', body).result; }); }
        };
        var store = {
          client: client,
          similaritySearchWithScore: function (q, k, filter) {
            return embeddings.embedQuery(q).then(function (v) {
              return client.search(coll, { vector: v, limit: k || 4, with_payload: true, filter: filter }).then(function (res) {
                return res.map(function (p) { return [{ pageContent: (p.payload || {}).content, metadata: (p.payload || {}).metadata || {} }, p.score]; });
              });
            });
          },
          similaritySearch: function (q, k, filter) { return store.similaritySearchWithScore(q, k, filter).then(function (r) { return r.map(function (x) { return x[0]; }); }); },
          addDocuments: function (docs) {
            return embeddings.embedDocuments(docs.map(function (d) { return d.pageContent; })).then(function (vecs) {
              return client.upsert(coll, { points: docs.map(function (d, i) { return { id: __uuidV4(), vector: vecs[i], payload: { content: d.pageContent, metadata: d.metadata || {} } }; }) });
            });
          },
          delete: function (opts) { return client.delete(coll, opts && opts.filter ? { filter: opts.filter } : { points: (opts && opts.ids) || [] }); }
        };
        return Promise.resolve(store);
      }
    }
  };
  var modules = {
    '@langchain/core/prompts': prompts, 'langchain/prompts': prompts,
    '@langchain/core/messages': messages, 'langchain/schema': messages,
    '@langchain/core/output_parsers': parsers, 'langchain/schema/output_parser': parsers,
    '@langchain/openai': { OpenAIEmbeddings: OpenAIEmbeddings, ChatOpenAI: ChatOpenAI },
    '@langchain/qdrant': qdrant,
    '@langchain/core/documents': { Document: function (d) { this.pageContent = d.pageContent; this.metadata = d.metadata || {}; } }
  };
  var base = g.require;
  g.require = function (name) { return Object.prototype.hasOwnProperty.call(modules, name) ? modules[name] : base(name); };
  g.getInputConnectionData = function (type, index) {
    if (type === 'ai_languageModel') return Promise.resolve(model(index || 0));
    return Promise.reject(new Error('getInputConnectionData("' + type + '") is not supported in LangChain Code nodes (ai_languageModel is)'));
  };
  g.getInputData = function () { return g.$input.all(); };
  g.getNodeParameter = function (name, i, def) { var p = g.$parameter; return p && name in p ? p[name] : def; };
})(this);
