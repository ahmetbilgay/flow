# Runs an n8n Python Code node (Pyodide-style) on a local interpreter.
# Input (stdin): {"code", "mode", "items", "nodes", "index"}; the result is
# written to stdout as JSON; print() goes to stderr.
import asyncio, json, sys, types, textwrap

_out = sys.stdout
sys.stdout = sys.stderr


class _Obj(dict):
    """A JSON object with attribute access and Pyodide's to_py()."""
    def __getattr__(self, k):
        try:
            return _wrap(self[k])
        except KeyError:
            raise AttributeError(k)
    def __getitem__(self, k):
        return _wrap(dict.__getitem__(self, k))
    def get(self, k, d=None):
        return _wrap(dict.get(self, k, d))
    def to_py(self):
        return _plain(self)


class _List(list):
    def __getitem__(self, i):
        v = list.__getitem__(self, i)
        return _List(v) if isinstance(i, slice) else _wrap(v)
    def __iter__(self):
        return (_wrap(v) for v in list.__iter__(self))
    def to_py(self):
        return _plain(self)


def _wrap(v):
    if isinstance(v, dict) and not isinstance(v, _Obj):
        return _Obj(v)
    if isinstance(v, list) and not isinstance(v, _List):
        return _List(v)
    return v


def _plain(v):
    if isinstance(v, dict):
        return {k: _plain(x) for k, x in dict.items(v)}
    if isinstance(v, (list, tuple)):
        return [_plain(x) for x in list.__iter__(v)] if isinstance(v, list) else [_plain(x) for x in v]
    if hasattr(v, "to_py") and not isinstance(v, (_Obj, _List)):
        return _plain(v.to_py())
    if hasattr(v, "tolist"):
        return v.tolist()
    if hasattr(v, "item") and callable(v.item):
        try:
            return v.item()
        except Exception:
            pass
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    return str(v)


class _Items:
    def __init__(self, items, index=0):
        self._items = [_Obj(i) for i in items]
        self._index = index
    def all(self, *a):
        return _List(self._items)
    def first(self, *a):
        return self._items[0] if self._items else None
    def last(self, *a):
        return self._items[-1] if self._items else None
    @property
    def item(self):
        return self._items[self._index] if self._index < len(self._items) else None
    def itemMatching(self, i):
        return self._items[i] if i < len(self._items) else None


def main():
    req = json.load(sys.stdin)
    items, nodes, index = req.get("items", []), req.get("nodes", {}), req.get("index", 0)
    micropip = types.ModuleType("micropip")

    async def _install(*a, **k):  # packages must be installed on the worker
        return None
    micropip.install = _install
    sys.modules["micropip"] = micropip

    def node(name):
        if name not in nodes:
            raise KeyError("node %r has not been executed" % name)
        return _Items(nodes[name], index)

    inp = _Items(items, index)
    env = {"_input": inp, "items": _List(items), "_items": _List(items), "_": node, "_node": node,
           "_json": _Obj(items[index]["json"]) if index < len(items) else _Obj({}), "__name__": "__n8n__"}
    src = "async def __n8n_main():\n" + textwrap.indent(req["code"], "    ") + "\n"
    exec(compile(src, "<code>", "exec"), env)
    result = asyncio.run(env["__n8n_main"]())
    _out.write(json.dumps({"result": _plain(result)}))


try:
    main()
except SystemExit:
    raise
except BaseException as e:  # report to the Go side
    _out.write(json.dumps({"error": "%s: %s" % (type(e).__name__, e)}))
