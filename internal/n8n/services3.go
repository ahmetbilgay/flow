package n8n

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"

	"github.com/dgmos/flow/internal/node"
)

// ---------------------------------------------------------------- Summarize

func summarizeNode(ec *execCtx) ([][]Item, error) {
	items := ec.items()
	fv, err := ec.param(0, "fieldsToSummarize.values", nil)
	if err != nil {
		return nil, err
	}
	splitStr, _ := ec.str(0, "fieldsToSplitBy", "")
	split := splitList(splitStr)
	dot := !mustBool(ec, 0, "options.disableDotNotation")
	type group struct {
		key   []interface{}
		items []int
	}
	var groups []*group
	byKey := map[string]*group{}
	for i, it := range items {
		var key []interface{}
		for _, f := range split {
			v, _ := getPath(it.JSON, f, dot)
			key = append(key, v)
		}
		k := mustJSON(key)
		g, ok := byKey[k]
		if !ok {
			g = &group{key: key}
			byKey[k] = g
			groups = append(groups, g)
		}
		g.items = append(g.items, i)
	}
	names := map[string]string{"count": "count", "countUnique": "unique_count", "sum": "sum", "average": "average", "min": "min", "max": "max", "concatenate": "concatenated", "append": "appended"}
	var out []Item
	for _, g := range groups {
		res := map[string]interface{}{}
		for n, f := range split {
			res[f] = g.key[n]
		}
		var pis []interface{}
		for _, idx := range g.items {
			pis = append(pis, paired(idx))
		}
		for _, raw := range asList(fv) {
			a, _ := raw.(map[string]interface{})
			agg, field := toString(a["aggregation"]), toString(a["field"])
			if agg == "" {
				agg = "count"
			}
			var vals []interface{}
			for _, idx := range g.items {
				v, ok := getPath(items[idx].JSON, field, dot)
				if !ok || (v == nil || v == "") && !toBool(a["includeEmpty"]) && agg != "count" {
					continue
				}
				if !ok && agg == "count" {
					continue
				}
				vals = append(vals, v)
			}
			var r interface{}
			switch agg {
			case "count":
				r = float64(len(vals))
			case "countUnique":
				seen := map[string]bool{}
				for _, v := range vals {
					seen[mustJSON(v)] = true
				}
				r = float64(len(seen))
			case "sum", "average":
				sum := 0.0
				for _, v := range vals {
					f, _ := toFloat(v)
					sum += f
				}
				r = sum
				if agg == "average" {
					if len(vals) == 0 {
						r = nil
					} else {
						r = sum / float64(len(vals))
					}
				}
			case "min", "max":
				var best *float64
				for _, v := range vals {
					f, ok := toFloat(v)
					if !ok {
						continue
					}
					if best == nil || agg == "min" && f < *best || agg == "max" && f > *best {
						ff := f
						best = &ff
					}
				}
				if best != nil {
					r = *best
				}
			case "concatenate":
				sep := map[string]string{"comma": ",", "commaAndSpace": ", ", "newLine": "\n", "space": " ", "none": "", "other": toString(a["customSeparator"])}[toString(a["separateBy"])]
				if toString(a["separateBy"]) == "" {
					sep = ","
				}
				parts := make([]string, len(vals))
				for n, v := range vals {
					parts[n] = cellString(v)
				}
				r = strings.Join(parts, sep)
			case "append":
				if vals == nil {
					vals = []interface{}{}
				}
				r = vals
			default:
				return nil, permanentError{fmt.Sprintf("Summarize: aggregation %q is not supported", agg)}
			}
			res[names[agg]+"_"+field] = r
		}
		out = append(out, Item{JSON: res, PairedItem: pis})
	}
	if f, _ := ec.str(0, "options.outputFormat", "separateItems"); f == "singleItem" && len(out) > 0 {
		list := make([]interface{}, len(out))
		for n, it := range out {
			list[n] = it.JSON
		}
		return [][]Item{{{JSON: map[string]interface{}{"data": list}}}}, nil
	}
	return [][]Item{out}, nil
}

// ---------------------------------------------------------------- Date & Time

// momentToLuxon converts moment.js format tokens (Date & Time v1).
func momentToLuxon(f string) string {
	r := strings.NewReplacer("YYYY", "yyyy", "YY", "yy", "Do", "d", "DD", "dd", "D", "d", "dddd", "EEEE", "ddd", "EEE", "A", "a", "ZZ", "ZZZ", "Z", "ZZ")
	return r.Replace(f)
}

func dateTimeNode(ec *execCtx) ([][]Item, error) {
	rt, err := ec.runtime()
	if err != nil {
		return nil, err
	}
	call := func(js string, args ...interface{}) (interface{}, error) {
		_ = rt.vm.Set("__a", args)
		return rt.evalExpr(js)
	}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		j := cloneJSON(it.JSON)
		if ec.version < 2 {
			action, _ := ec.str(i, "action", "format")
			prop, _ := ec.str(i, "dataPropertyName", "data")
			val, _ := ec.param(i, "value", "")
			var v interface{}
			var err error
			if action == "calculate" {
				op, _ := ec.str(i, "operation", "add")
				dur, _ := ec.num(i, "duration", 0)
				unit, _ := ec.str(i, "timeUnit", "days")
				if op == "subtract" {
					dur = -dur
				}
				v, err = call(`(function(){ var o = {}; o[__a[2]] = __a[1]; return DateTime.fromAny(__a[0]).plus(o).toISO(); })()`, val, dur, unit)
			} else {
				f, _ := ec.str(i, "toFormat", "")
				if c, _ := ec.boolean(i, "custom", false); !c && f == "" {
					f = "yyyy-MM-dd"
				}
				v, err = call(`DateTime.fromAny(__a[0]).toFormat(__a[1])`, val, momentToLuxon(f))
			}
			if err != nil {
				return nil, err
			}
			setPath(j, prop, v, true)
			return []Item{{JSON: j, Binary: it.Binary}}, nil
		}
		op, _ := ec.str(i, "operation", "getCurrentDate")
		var v interface{}
		var outKey string
		switch op {
		case "getCurrentDate":
			outKey = mustStr(ec, i, "outputFieldName", "currentDate")
			inc, _ := ec.boolean(i, "includeTime", true)
			if inc {
				v, err = call(`DateTime.now().toISO()`)
			} else {
				v, err = call(`DateTime.now().startOf('day').toISO()`)
			}
		case "formatDate":
			outKey = mustStr(ec, i, "outputFieldName", "formattedDate")
			date, _ := ec.param(i, "date", "")
			f, _ := ec.str(i, "format", "MM/dd/yyyy")
			if f == "custom" {
				f, _ = ec.str(i, "customFormat", "")
			}
			v, err = call(`DateTime.fromAny(__a[0]).toFormat(__a[1])`, date, f)
		case "addToDate", "subtractFromDate":
			outKey = mustStr(ec, i, "outputFieldName", "newDate")
			date, _ := ec.param(i, "magnitude", "")
			dur, _ := ec.num(i, "duration", 0)
			unit, _ := ec.str(i, "timeUnit", "days")
			if op == "subtractFromDate" {
				dur = -dur
			}
			v, err = call(`(function(){ var o = {}; o[__a[2]] = __a[1]; return DateTime.fromAny(__a[0]).plus(o).toISO(); })()`, date, dur, unit)
		case "roundDate":
			outKey = mustStr(ec, i, "outputFieldName", "roundedDate")
			date, _ := ec.param(i, "date", "")
			mode, _ := ec.str(i, "mode", "roundDown")
			unit, _ := ec.str(i, "toNearest", "day")
			if mode == "roundUp" {
				v, err = call(`DateTime.fromAny(__a[0]).endOf(__a[1]).plus({milliseconds:1}).toISO()`, date, unit)
			} else {
				v, err = call(`DateTime.fromAny(__a[0]).startOf(__a[1]).toISO()`, date, unit)
			}
		case "getTimeBetweenDates":
			outKey = mustStr(ec, i, "outputFieldName", "timeDifference")
			s, _ := ec.param(i, "startDate", "")
			e, _ := ec.param(i, "endDate", "")
			units, _ := ec.param(i, "units", []interface{}{"day"})
			v, err = call(`(function(){ var r = {}; (Array.isArray(__a[2]) ? __a[2] : [__a[2]]).forEach(function(u){ r[u + 's'] = DateTime.fromAny(__a[1]).diff(DateTime.fromAny(__a[0]), u).as(u); }); return r; })()`, s, e, units)
		case "extractDate":
			outKey = mustStr(ec, i, "outputFieldName", "datePart")
			date, _ := ec.param(i, "date", "")
			part, _ := ec.str(i, "part", "month")
			v, err = call(`DateTime.fromAny(__a[0])[__a[1] === 'week' ? 'weekNumber' : __a[1]]`, date, part)
		default:
			return nil, permanentError{fmt.Sprintf("Date & Time: operation %q is not supported", op)}
		}
		if err != nil {
			return nil, err
		}
		setPath(j, outKey, v, true)
		return []Item{{JSON: j, Binary: it.Binary}}, nil
	})
}

// ---------------------------------------------------------------- Supabase (rows)

func supabaseNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("supabaseApi")
	if err != nil {
		return nil, err
	}
	key := credString(c, "serviceRole", "apiKey")
	base := credBaseURL(c, "") + "/rest/v1/"
	h := map[string]string{"apikey": key, "Authorization": "Bearer " + key, "Prefer": "return=representation"}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		_, o := ec.resourceOp(i, "row", "create")
		table, _ := ec.rlString(i, "tableId")
		u := base + url.PathEscape(table)
		filters := func() string {
			if ft, _ := ec.str(i, "filterType", "manual"); ft == "string" {
				s, _ := ec.str(i, "filterString", "")
				return s
			}
			fv, _ := ec.param(i, "filters.conditions", nil)
			q := url.Values{}
			for _, raw := range asList(fv) {
				m, _ := raw.(map[string]interface{})
				cond := firstNonEmpty(toString(m["condition"]), "eq")
				q.Add(toString(m["keyName"]), cond+"."+toString(m["keyValue"]))
			}
			return q.Encode()
		}
		row := func() map[string]interface{} {
			if d, _ := ec.str(i, "dataToSend", "defineBelow"); d == "autoMapInputData" {
				return it.JSON
			}
			fv, _ := ec.param(i, "fieldsUi.fieldValues", nil)
			m := map[string]interface{}{}
			for _, raw := range asList(fv) {
				f, _ := raw.(map[string]interface{})
				m[toString(f["fieldId"])] = f["fieldValue"]
			}
			return m
		}
		decode := func(raw []byte) []Item {
			var list []interface{}
			_ = json.Unmarshal(raw, &list)
			return listItems(list)
		}
		switch o {
		case "create":
			_, raw, err := ec.apiCall("POST", u, h, row(), "")
			return decode(raw), err
		case "getAll", "get":
			q := filters()
			if o == "getAll" {
				if all, _ := ec.boolean(i, "returnAll", false); !all {
					limit, _ := ec.num(i, "limit", 50)
					q += "&limit=" + strconv.Itoa(int(limit))
				}
			}
			_, raw, err := ec.apiCall("GET", u+"?"+strings.TrimPrefix(q, "&"), h, nil, "")
			return decode(raw), err
		case "update":
			_, raw, err := ec.apiCall("PATCH", u+"?"+filters(), h, row(), "")
			return decode(raw), err
		case "delete":
			_, raw, err := ec.apiCall("DELETE", u+"?"+filters(), h, nil, "")
			return decode(raw), err
		}
		return nil, unsupportedOp("Supabase", "row", o)
	})
}

// ---------------------------------------------------------------- Redis

var redisClients = newPoolCache(32)

func redisNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("redis")
	if err != nil {
		return nil, err
	}
	addr := net.JoinHostPort(firstNonEmpty(credString(c, "host"), "localhost"), strconv.Itoa(int(toFloatDefault(c["port"], 6379))))
	cacheKey := secretKey(addr, credString(c, "user"), credString(c, "password"), credString(c, "database"), mustJSON(ec.opts.Outbound))
	pool, err := redisClients.get(cacheKey, func() (io.Closer, error) {
		dial := node.NewOutboundDialer(ec.opts.Outbound)
		return redis.NewClient(&redis.Options{Addr: addr, Username: credString(c, "user"), Password: credString(c, "password"), DB: int(toFloatDefault(c["database"], 0)),
			Dialer: func(ctx context.Context, network, a string) (net.Conn, error) { return dial(ctx, network, a) }}), nil
	})
	if err != nil {
		return nil, err
	}
	cl := pool.(*redis.Client)
	ctx := ec.ctx
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		op, _ := ec.str(i, "operation", "info")
		key, _ := ec.str(i, "key", "")
		j := cloneJSON(it.JSON)
		switch op {
		case "set":
			kt, _ := ec.str(i, "keyType", "automatic")
			val, _ := ec.param(i, "value", "")
			var err error
			switch {
			case kt == "hash" || kt == "automatic" && isObject(val):
				m, _ := val.(map[string]interface{})
				if s, ok := val.(string); ok {
					_ = json.Unmarshal([]byte(s), &m)
				}
				err = cl.HSet(ctx, key, flatStrings(m)...).Err()
			case kt == "list" || kt == "automatic" && isArray(val):
				var vals []interface{}
				for _, x := range asList(val) {
					vals = append(vals, toString(x))
				}
				err = cl.RPush(ctx, key, vals...).Err()
			case kt == "sets":
				err = cl.SAdd(ctx, key, toString(val)).Err()
			default:
				err = cl.Set(ctx, key, toString(val), 0).Err()
			}
			if err != nil {
				return nil, err
			}
			if exp, _ := ec.boolean(i, "expire", false); exp {
				ttl, _ := ec.num(i, "ttl", 60)
				cl.Expire(ctx, key, time.Duration(ttl)*time.Second)
			}
			return []Item{{JSON: it.JSON}}, nil
		case "get":
			prop, _ := ec.str(i, "propertyName", "propertyName")
			kt, _ := ec.str(i, "keyType", "automatic")
			if kt == "automatic" {
				kt, _ = cl.Type(ctx, key).Result()
			}
			var v interface{}
			switch kt {
			case "hash":
				m, _ := cl.HGetAll(ctx, key).Result()
				mm := map[string]interface{}{}
				for k, x := range m {
					mm[k] = x
				}
				v = mm
			case "list":
				l, _ := cl.LRange(ctx, key, 0, -1).Result()
				v = l
			case "set", "sets":
				l, _ := cl.SMembers(ctx, key).Result()
				v = l
			default:
				s, err := cl.Get(ctx, key).Result()
				if err == redis.Nil {
					v = nil
				} else if err != nil {
					return nil, err
				} else {
					v = s
				}
			}
			setPath(j, prop, v, true)
			return []Item{{JSON: j}}, nil
		case "delete":
			if err := cl.Del(ctx, key).Err(); err != nil {
				return nil, err
			}
			return []Item{{JSON: it.JSON}}, nil
		case "incr":
			n, err := cl.Incr(ctx, key).Result()
			if err != nil {
				return nil, err
			}
			if exp, _ := ec.boolean(i, "expire", false); exp {
				ttl, _ := ec.num(i, "ttl", 60)
				cl.Expire(ctx, key, time.Duration(ttl)*time.Second)
			}
			return []Item{{JSON: map[string]interface{}{key: float64(n)}}}, nil
		case "keys":
			pat, _ := ec.str(i, "keyPattern", "*")
			keys, err := cl.Keys(ctx, pat).Result()
			if err != nil {
				return nil, err
			}
			out := map[string]interface{}{}
			getVals, _ := ec.boolean(i, "getValues", true)
			for _, k := range keys {
				if getVals {
					v, _ := cl.Get(ctx, k).Result()
					out[k] = v
				} else {
					out[k] = nil
				}
			}
			return []Item{{JSON: out}}, nil
		case "push":
			list, _ := ec.str(i, "list", "")
			msg, _ := ec.str(i, "messageData", "")
			var err error
			if tail, _ := ec.boolean(i, "tail", false); tail {
				err = cl.RPush(ctx, list, msg).Err()
			} else {
				err = cl.LPush(ctx, list, msg).Err()
			}
			if err != nil {
				return nil, err
			}
			return []Item{{JSON: it.JSON}}, nil
		case "pop":
			list, _ := ec.str(i, "list", "")
			prop, _ := ec.str(i, "propertyName", "propertyName")
			var s string
			var err error
			if tail, _ := ec.boolean(i, "tail", false); tail {
				s, err = cl.RPop(ctx, list).Result()
			} else {
				s, err = cl.LPop(ctx, list).Result()
			}
			if err != nil && err != redis.Nil {
				return nil, err
			}
			var v interface{} = s
			var parsed interface{}
			if json.Unmarshal([]byte(s), &parsed) == nil {
				v = parsed
			}
			setPath(j, prop, v, true)
			return []Item{{JSON: j}}, nil
		case "publish":
			ch, _ := ec.str(i, "channel", "")
			msg, _ := ec.str(i, "messageData", "")
			if err := cl.Publish(ctx, ch, msg).Err(); err != nil {
				return nil, err
			}
			return []Item{{JSON: it.JSON}}, nil
		}
		return nil, unsupportedOp("Redis", "", op)
	})
}

func isObject(v interface{}) bool {
	if _, ok := v.(map[string]interface{}); ok {
		return true
	}
	s, ok := v.(string)
	return ok && strings.HasPrefix(strings.TrimSpace(s), "{")
}

func isArray(v interface{}) bool { _, ok := v.([]interface{}); return ok }

func flatStrings(m map[string]interface{}) []interface{} {
	var out []interface{}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out = append(out, k, cellString(m[k]))
	}
	return out
}

// ---------------------------------------------------------------- MySQL

var (
	mysqlPools  = newPoolCache(32)
	mysqlDialMu sync.Mutex
	mysqlDials  = map[string]string{}
)

func mysqlNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("mySql")
	if err != nil {
		return nil, err
	}
	policyKey := mustJSON(ec.opts.Outbound)
	mysqlDialMu.Lock()
	netName, ok := mysqlDials[policyKey]
	if !ok {
		netName = fmt.Sprintf("flowguard%d", len(mysqlDials))
		dial := node.NewOutboundDialer(ec.opts.Outbound)
		mysql.RegisterDialContext(netName, func(ctx context.Context, addr string) (net.Conn, error) { return dial(ctx, "tcp", addr) })
		mysqlDials[policyKey] = netName
	}
	mysqlDialMu.Unlock()
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.DBName = credString(c, "user"), credString(c, "password"), credString(c, "database")
	cfg.Net, cfg.Addr = netName, net.JoinHostPort(credString(c, "host"), strconv.Itoa(int(toFloatDefault(c["port"], 3306))))
	cfg.ParseTime = true
	dsn := cfg.FormatDSN()
	pool, err := mysqlPools.get(secretKey(dsn), func() (io.Closer, error) {
		conn, err := mysql.NewConnector(cfg)
		if err != nil {
			return nil, permanentError{"mysql: " + redactSecrets(err.Error())}
		}
		db := sql.OpenDB(conn)
		db.SetMaxOpenConns(5)
		return db, nil
	})
	if err != nil {
		return nil, err
	}
	db := pool.(*sql.DB)
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		op, _ := ec.str(i, "operation", "insert")
		if op != "executeQuery" {
			return nil, permanentError{fmt.Sprintf("MySQL: operation %q is not supported (executeQuery is)", op)}
		}
		q, err := ec.str(i, "query", "")
		if err != nil {
			return nil, err
		}
		var args []interface{}
		if rep, _ := ec.str(i, "options.queryReplacement", ""); rep != "" {
			for _, a := range splitList(rep) {
				args = append(args, a)
			}
		}
		return pgQuery(ec, db, q, args...)
	})
}

// ---------------------------------------------------------------- Spreadsheet File (CSV)

func spreadsheetFileNode(ec *execCtx) ([][]Item, error) {
	op, _ := ec.str(0, "operation", "fromFile")
	format, _ := ec.str(0, "fileFormat", "csv")
	if op == "toFile" {
		if format != "csv" {
			return nil, permanentError{fmt.Sprintf("Spreadsheet File: %s is not supported (csv is)", format)}
		}
		ec.params["operation"] = "csv"
		return convertToFileNode(ec)
	}
	ec.params["operation"] = "csv"
	if d, _ := ec.str(0, "options.delimiter", ""); d == "" {
		if ec.params["options"] == nil {
			ec.params["options"] = map[string]interface{}{}
		}
	}
	return extractFromFileNode(ec)
}

// ---------------------------------------------------------------- RSS Read

type rssDoc struct {
	Channel struct {
		Items []rssItem `xml:"item"`
	} `xml:"channel"`
	Entries []atomEntry `xml:"entry"`
}

type rssItem struct {
	Title       string   `xml:"title"`
	Link        string   `xml:"link"`
	PubDate     string   `xml:"pubDate"`
	Description string   `xml:"description"`
	Content     string   `xml:"http://purl.org/rss/1.0/modules/content/ encoded"`
	Creator     string   `xml:"http://purl.org/dc/elements/1.1/ creator"`
	GUID        string   `xml:"guid"`
	Categories  []string `xml:"category"`
}

type atomEntry struct {
	Title   string `xml:"title"`
	ID      string `xml:"id"`
	Updated string `xml:"updated"`
	Summary string `xml:"summary"`
	Content string `xml:"content"`
	Links   []struct {
		Href string `xml:"href,attr"`
		Rel  string `xml:"rel,attr"`
	} `xml:"link"`
	Author struct {
		Name string `xml:"name"`
	} `xml:"author"`
}

func rssFeedReadNode(ec *execCtx) ([][]Item, error) {
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		u, err := ec.str(i, "url", "")
		if err != nil {
			return nil, err
		}
		data, _, err := ec.download(u)
		if err != nil {
			return nil, err
		}
		var doc rssDoc
		dec := xml.NewDecoder(bytes.NewReader(data))
		dec.Strict = false
		if err := dec.Decode(&doc); err != nil {
			return nil, permanentError{"RSS: invalid feed: " + err.Error()}
		}
		var out []Item
		for _, x := range doc.Channel.Items {
			j := map[string]interface{}{"title": x.Title, "link": x.Link, "pubDate": x.PubDate, "content": firstNonEmpty(x.Content, x.Description),
				"contentSnippet": strings.TrimSpace(stripTags(firstNonEmpty(x.Description, x.Content))), "guid": x.GUID, "creator": x.Creator}
			if t, err := time.Parse(time.RFC1123Z, x.PubDate); err == nil {
				j["isoDate"] = t.UTC().Format("2006-01-02T15:04:05.000Z")
			} else if t, err := time.Parse(time.RFC1123, x.PubDate); err == nil {
				j["isoDate"] = t.UTC().Format("2006-01-02T15:04:05.000Z")
			}
			if len(x.Categories) > 0 {
				j["categories"] = x.Categories
			}
			out = append(out, Item{JSON: j})
		}
		for _, e := range doc.Entries {
			link := ""
			for _, l := range e.Links {
				if l.Rel == "" || l.Rel == "alternate" {
					link = l.Href
				}
			}
			j := map[string]interface{}{"title": e.Title, "link": link, "id": e.ID, "pubDate": e.Updated, "isoDate": e.Updated,
				"content": firstNonEmpty(e.Content, e.Summary), "contentSnippet": strings.TrimSpace(stripTags(firstNonEmpty(e.Summary, e.Content))), "author": e.Author.Name}
			out = append(out, Item{JSON: j})
		}
		return out, nil
	})
}

func stripTags(s string) string {
	var b strings.Builder
	in := false
	for _, r := range s {
		switch {
		case r == '<':
			in = true
		case r == '>':
			in = false
		case !in:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// ---------------------------------------------------------------- HTML Extract (legacy)

func htmlExtractNode(ec *execCtx) ([][]Item, error) {
	var out []Item
	for i, it := range ec.items() {
		j, err := extractHTML(ec, i, it)
		if err != nil {
			return nil, err
		}
		out = append(out, Item{JSON: j, PairedItem: paired(i)})
	}
	return [][]Item{out}, nil
}

// ---------------------------------------------------------------- files (sandboxed)

// sandboxRoot returns the absolute, symlink-resolved sandbox directory.
func (ec *execCtx) sandboxRoot() (string, error) {
	root := ""
	if ec.opts != nil {
		root = ec.opts.FileSystemRoot
	}
	if root == "" {
		return "", permanentError{"file access is disabled (set Options.FileSystemRoot to allow a sandbox directory)"}
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if real, err := filepath.EvalSymlinks(abs); err == nil {
		abs = real
	}
	return abs, nil
}

// sandboxPath maps a workflow path into the sandbox (it may not exist yet).
func (ec *execCtx) sandboxPath(p string) (string, error) {
	root, err := ec.sandboxRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, filepath.Clean("/"+p)), nil
}

// insideSandbox verifies that an existing path, with every symlink
// resolved, is a regular file or directory within the sandbox, and that the
// final component is not itself a symlink.
func (ec *execCtx) insideSandbox(path string) error {
	root, err := ec.sandboxRoot()
	if err != nil {
		return err
	}
	if fi, err := os.Lstat(path); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		return permanentError{"symbolic links are not allowed in the sandbox"}
	}
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if real != root && !strings.HasPrefix(real, root+string(os.PathSeparator)) {
		return permanentError{"path escapes the sandbox directory"}
	}
	return nil
}

func fileItem(path string, data []byte, prop string) Item {
	name := filepath.Base(path)
	mt := mimeByExt(name)
	if mt == "application/octet-stream" {
		mt = sniffMime(data)
	}
	ext := strings.TrimPrefix(filepath.Ext(name), ".")
	return Item{JSON: map[string]interface{}{"mimeType": mt, "fileType": strings.Split(mt, "/")[0], "fileName": name, "fileExtension": ext,
		"fileSize": fmt.Sprintf("%d B", len(data)), "directory": filepath.Dir(path)}, Binary: map[string]interface{}{prop: binaryData(data, mt, name)}}
}

func readWriteFileNode(ec *execCtx) ([][]Item, error) {
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		op, _ := ec.str(i, "operation", "read")
		if short(ec.node.Type) == "readBinaryFile" {
			op = "read"
		}
		if short(ec.node.Type) == "writeBinaryFile" {
			op = "write"
		}
		prop := mustStr(ec, i, "options.dataPropertyName", mustStr(ec, i, "dataPropertyName", "data"))
		if op == "write" {
			name, err := ec.str(i, "fileName", "")
			if err != nil {
				return nil, err
			}
			path, err := ec.sandboxPath(name)
			if err != nil {
				return nil, err
			}
			data, _, _, err := binaryOf(it, prop)
			if err != nil {
				return nil, err
			}
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				return nil, err
			}
			if err := ec.insideSandbox(filepath.Dir(path)); err != nil {
				return nil, err
			}
			if _, err := os.Lstat(path); err == nil {
				if err := ec.insideSandbox(path); err != nil {
					return nil, err
				}
			}
			if limit := ec.opts.Outbound.MaxResponseBytesOrDefault(); limit > 0 && int64(len(data)) > limit {
				return nil, permanentError{"file is larger than the configured size limit"}
			}
			flag := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
			if app, _ := ec.boolean(i, "options.append", false); app {
				flag = os.O_CREATE | os.O_WRONLY | os.O_APPEND
			}
			f, err := os.OpenFile(path, flag, 0o644)
			if err != nil {
				return nil, err
			}
			defer f.Close()
			if _, err := f.Write(data); err != nil {
				return nil, err
			}
			j := cloneJSON(it.JSON)
			j["fileName"] = name
			return []Item{{JSON: j, Binary: it.Binary}}, nil
		}
		pattern, _ := ec.str(i, "fileSelector", "")
		if pattern == "" {
			pattern, _ = ec.str(i, "filePath", "")
		}
		full, err := ec.sandboxPath(pattern)
		if err != nil {
			return nil, err
		}
		matches, err := filepath.Glob(full)
		if err != nil {
			return nil, permanentError{"invalid file selector: " + err.Error()}
		}
		var out []Item
		limit := ec.opts.Outbound.MaxResponseBytesOrDefault()
		for _, m := range matches {
			if err := ec.insideSandbox(m); err != nil {
				continue // symlinks or paths outside the sandbox are skipped
			}
			fi, err := os.Stat(m)
			if err != nil || fi.IsDir() {
				continue
			}
			if limit > 0 && fi.Size() > limit {
				return nil, permanentError{fmt.Sprintf("file %s is larger than the configured size limit", filepath.Base(m))}
			}
			data, err := os.ReadFile(m)
			if err != nil {
				return nil, err
			}
			out = append(out, fileItem(m, data, prop))
		}
		return out, nil
	})
}
