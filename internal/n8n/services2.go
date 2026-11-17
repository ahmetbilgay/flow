package n8n

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/dgmos/flow/internal/node"
)

// ---------------------------------------------------------------- OpenAI (legacy node)

func openAiLegacyNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("openAiApi")
	if err != nil {
		return nil, err
	}
	base := credBaseURL(c, ec.apiBase("openai", "https://api.openai.com/v1"))
	h := map[string]string{"Authorization": "Bearer " + credString(c, "apiKey")}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "text", "")
		opts, _ := ec.param(i, "options", map[string]interface{}{})
		om, _ := opts.(map[string]interface{})
		body := map[string]interface{}{}
		for k, v := range om {
			switch k {
			case "maxTokens":
				body["max_tokens"] = toFloatDefault(v, 16)
			case "temperature", "n", "top_p", "frequency_penalty", "presence_penalty", "size":
				body[k] = v
			case "topP":
				body["top_p"] = v
			case "frequencyPenalty":
				body["frequency_penalty"] = v
			case "presencePenalty":
				body["presence_penalty"] = v
			}
		}
		switch r {
		case "chat":
			model, _ := ec.str(i, "model", "gpt-3.5-turbo")
			mv, err := ec.param(i, "prompt.messages", nil)
			if err != nil {
				return nil, err
			}
			var msgs []interface{}
			for _, raw := range asList(mv) {
				m, _ := raw.(map[string]interface{})
				msgs = append(msgs, map[string]interface{}{"role": firstNonEmpty(toString(m["role"]), "user"), "content": toString(m["content"])})
			}
			body["model"], body["messages"] = model, msgs
			res, _, err := ec.apiCall("POST", base+"/chat/completions", h, body, "")
			if err != nil {
				return nil, err
			}
			if simple, _ := ec.boolean(i, "simplifyOutput", true); !simple {
				return one(res), nil
			}
			return listItems(res["choices"]), nil
		case "image":
			prompt, _ := ec.str(i, "prompt", "")
			body["prompt"] = prompt
			if m, _ := ec.str(i, "model", ""); m != "" {
				body["model"] = m
			}
			format, _ := ec.str(i, "responseFormat", "binaryData")
			body["response_format"] = "b64_json"
			if format == "imageUrl" {
				body["response_format"] = "url"
			}
			res, _, err := ec.apiCall("POST", base+"/images/generations", h, body, "")
			if err != nil {
				return nil, err
			}
			var out []Item
			for k, d := range asList(res["data"]) {
				dm, _ := d.(map[string]interface{})
				if format == "imageUrl" {
					out = append(out, Item{JSON: map[string]interface{}{"url": dm["url"]}})
					continue
				}
				data, _ := base64.StdEncoding.DecodeString(toString(dm["b64_json"]))
				out = append(out, Item{JSON: map[string]interface{}{}, Binary: map[string]interface{}{"data": binaryData(data, "image/png", fmt.Sprintf("image_%d.png", k))}})
			}
			return out, nil
		default: // text: complete / edit / moderate
			switch o {
			case "moderate":
				input, _ := ec.str(i, "input", "")
				res, _, err := ec.apiCall("POST", base+"/moderations", h, map[string]interface{}{"input": input}, "")
				if err != nil {
					return nil, err
				}
				return listItems(res["results"]), nil
			case "edit":
				input, _ := ec.str(i, "input", "")
				instr, _ := ec.str(i, "instruction", "")
				body["model"] = "gpt-4o-mini"
				body["messages"] = []interface{}{map[string]interface{}{"role": "system", "content": instr}, map[string]interface{}{"role": "user", "content": input}}
				res, _, err := ec.apiCall("POST", base+"/chat/completions", h, body, "")
				if err != nil {
					return nil, err
				}
				var out []Item
				for _, ch := range asList(res["choices"]) {
					cm, _ := ch.(map[string]interface{})
					msg, _ := cm["message"].(map[string]interface{})
					out = append(out, Item{JSON: map[string]interface{}{"index": cm["index"], "text": msg["content"]}})
				}
				return out, nil
			default: // complete
				prompt, _ := ec.str(i, "prompt", "")
				model, _ := ec.str(i, "model", "gpt-3.5-turbo-instruct")
				if model == "text-davinci-003" || model == "text-davinci-002" {
					model = "gpt-3.5-turbo-instruct" // retired models
				}
				body["model"], body["prompt"] = model, prompt
				res, _, err := ec.apiCall("POST", base+"/completions", h, body, "")
				if err != nil {
					return nil, err
				}
				if simple, _ := ec.boolean(i, "simplifyOutput", true); !simple {
					return one(res), nil
				}
				return listItems(res["choices"]), nil
			}
		}
	})
}

// ---------------------------------------------------------------- X (Twitter)

func twitterNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("twitterOAuth2Api")
	if err != nil {
		return nil, err
	}
	h := map[string]string{"Authorization": "Bearer " + accessToken(c)}
	base := ec.apiBase("twitter", "https://api.x.com/2")
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "tweet", "create")
		switch r + "." + o {
		case "tweet.create":
			text, _ := ec.str(i, "text", "")
			body := map[string]interface{}{"text": text}
			if v, _ := ec.rlString(i, "additionalFields.inReplyToStatusId"); v != "" {
				body["reply"] = map[string]interface{}{"in_reply_to_tweet_id": tweetID(v)}
			}
			if v, _ := ec.str(i, "additionalFields.attachments", ""); v != "" {
				body["media"] = map[string]interface{}{"media_ids": splitAny(v)}
			}
			res, _, err := ec.apiCall("POST", base+"/tweets", h, body, "")
			if err != nil {
				return nil, err
			}
			data, _ := res["data"].(map[string]interface{})
			return one(data), nil
		case "tweet.delete":
			id, _ := ec.rlString(i, "tweetDeleteId")
			res, _, err := ec.apiCall("DELETE", base+"/tweets/"+url.PathEscape(tweetID(id)), h, nil, "")
			return one(res), err
		case "tweet.search":
			q, _ := ec.str(i, "searchText", "")
			limit, _ := ec.num(i, "limit", 50)
			res, _, err := ec.apiCall("GET", base+"/tweets/search/recent?"+url.Values{"query": {q}, "max_results": {strconv.Itoa(int(clampF(limit, 10, 100)))}}.Encode(), h, nil, "")
			if err != nil {
				return nil, err
			}
			return listItems(res["data"]), nil
		}
		return nil, unsupportedOp("X", r, o)
	})
}

func tweetID(s string) string {
	if i := strings.LastIndex(s, "/status/"); i >= 0 {
		s = s[i+8:]
	}
	return strings.TrimFunc(s, func(r rune) bool { return r < '0' || r > '9' })
}

func clampF(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// ---------------------------------------------------------------- LinkedIn

func linkedInNode(ec *execCtx) ([][]Item, error) {
	credType := "linkedInOAuth2Api"
	if a, _ := ec.str(0, "authentication", "standard"); a == "communityManagement" {
		credType = "linkedInCommunityManagementOAuth2Api"
	}
	c, err := ec.credential(credType)
	if err != nil {
		return nil, err
	}
	base := ec.apiBase("linkedin", "https://api.linkedin.com")
	h := map[string]string{"Authorization": "Bearer " + accessToken(c), "LinkedIn-Version": "202404", "X-Restli-Protocol-Version": "2.0.0"}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "post", "create")
		if r+"."+o != "post.create" {
			return nil, unsupportedOp("LinkedIn", r, o)
		}
		author := ""
		if as, _ := ec.str(i, "postAs", "person"); as == "organization" {
			org, _ := ec.str(i, "organization", "")
			author = "urn:li:organization:" + strings.TrimPrefix(org, "urn:li:organization:")
		} else {
			p, _ := ec.str(i, "person", "")
			author = "urn:li:person:" + strings.TrimPrefix(p, "urn:li:person:")
		}
		text, _ := ec.str(i, "text", "")
		vis, _ := ec.str(i, "additionalFields.visibility", "PUBLIC")
		post := map[string]interface{}{"author": author, "commentary": text, "visibility": vis, "lifecycleState": "PUBLISHED",
			"distribution": map[string]interface{}{"feedDistribution": "MAIN_FEED", "targetEntities": []interface{}{}, "thirdPartyDistributionChannels": []interface{}{}}}
		switch cat, _ := ec.str(i, "shareMediaCategory", "NONE"); cat {
		case "ARTICLE":
			u, _ := ec.str(i, "additionalFields.originalUrl", "")
			title, _ := ec.str(i, "additionalFields.title", "")
			desc, _ := ec.str(i, "additionalFields.description", "")
			post["content"] = map[string]interface{}{"article": map[string]interface{}{"source": u, "title": title, "description": desc}}
		case "IMAGE":
			prop, _ := ec.str(i, "binaryPropertyName", "data")
			data, _, _, err := binaryOf(it, prop)
			if err != nil {
				return nil, err
			}
			init, _, err := ec.apiCall("POST", base+"/rest/images?action=initializeUpload", h, map[string]interface{}{"initializeUploadRequest": map[string]interface{}{"owner": author}}, "")
			if err != nil {
				return nil, err
			}
			val, _ := init["value"].(map[string]interface{})
			if _, _, err := ec.apiCall("PUT", toString(val["uploadUrl"]), map[string]string{"Authorization": h["Authorization"]}, data, "application/octet-stream"); err != nil {
				return nil, err
			}
			title, _ := ec.str(i, "additionalFields.title", "")
			post["content"] = map[string]interface{}{"media": map[string]interface{}{"id": val["image"], "title": title}}
		}
		if _, err := ec.doRequest("POST", base+"/rest/posts", h, mustBytes(post), "application/json"); err != nil {
			return nil, err
		}
		return one(map[string]interface{}{"success": true}), nil
	})
}

func mustBytes(v interface{}) []byte { return []byte(mustJSON(v)) }

// ---------------------------------------------------------------- Postgres

var pgPools = newPoolCache(32)

type pgDialer struct {
	dial func(ctx context.Context, network, addr string) (net.Conn, error)
}

func (d pgDialer) Dial(network, addr string) (net.Conn, error) {
	return d.dial(context.Background(), network, addr)
}
func (d pgDialer) DialTimeout(network, addr string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.dial(ctx, network, addr)
}

func (ec *execCtx) postgresDB() (*sql.DB, error) {
	c, err := ec.credential("postgres")
	if err != nil {
		return nil, err
	}
	ssl := credString(c, "ssl")
	if ssl == "" || ssl == "allow" {
		ssl = "prefer"
	}
	if ssl == "prefer" {
		ssl = "disable"
		if toBool(c["allowUnauthorizedCerts"]) {
			ssl = "require"
		}
	}
	u := url.URL{Scheme: "postgres", User: url.UserPassword(credString(c, "user"), credString(c, "password")),
		Host: net.JoinHostPort(credString(c, "host"), strconv.Itoa(int(toFloatDefault(c["port"], 5432)))), Path: "/" + credString(c, "database")}
	q := url.Values{"sslmode": {ssl}, "connect_timeout": {"15"}}
	u.RawQuery = q.Encode()
	dsn := u.String()
	pool, err := pgPools.get(secretKey(dsn, mustJSON(ec.opts.Outbound)), func() (io.Closer, error) {
		connector, err := pq.NewConnector(dsn)
		if err != nil {
			return nil, permanentError{"postgres: " + redactSecrets(err.Error())}
		}
		connector.Dialer(pgDialer{dial: node.NewOutboundDialer(ec.opts.Outbound)})
		db := sql.OpenDB(connector)
		db.SetMaxOpenConns(5)
		db.SetConnMaxIdleTime(5 * time.Minute)
		return db, nil
	})
	if err != nil {
		return nil, err
	}
	return pool.(*sql.DB), nil
}

func pgIdent(s string) string { return pq.QuoteIdentifier(s) }

func postgresNode(ec *execCtx) ([][]Item, error) {
	db, err := ec.postgresDB()
	if err != nil {
		return nil, err
	}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		op, _ := ec.str(i, "operation", "insert")
		schema, _ := ec.rlString(i, "schema")
		if schema == "" {
			schema = "public"
		}
		table, _ := ec.rlString(i, "table")
		tbl := pgIdent(schema) + "." + pgIdent(table)
		switch op {
		case "executeQuery":
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
		case "select":
			q := "SELECT * FROM " + tbl
			where, args := pgWhere(ec, i)
			q += where
			if sv, _ := ec.param(i, "sort.values", nil); len(asList(sv)) > 0 {
				var parts []string
				for _, raw := range asList(sv) {
					m, _ := raw.(map[string]interface{})
					dir := "ASC"
					if toString(m["direction"]) == "DESC" {
						dir = "DESC"
					}
					parts = append(parts, pgIdent(toString(m["column"]))+" "+dir)
				}
				q += " ORDER BY " + strings.Join(parts, ", ")
			}
			if all, _ := ec.boolean(i, "returnAll", false); !all {
				limit, _ := ec.num(i, "limit", 50)
				q += fmt.Sprintf(" LIMIT %d", int(limit))
			}
			return pgQuery(ec, db, q, args...)
		case "insert", "upsert", "update":
			data := pgColumns(ec, i, it)
			if len(data) == 0 {
				return nil, permanentError{"postgres: no columns to write"}
			}
			cols := make([]string, 0, len(data))
			for k := range data {
				cols = append(cols, k)
			}
			sortStrings(cols)
			var args []interface{}
			var ph, quoted []string
			for n, k := range cols {
				args = append(args, pgValue(data[k]))
				ph = append(ph, "$"+strconv.Itoa(n+1))
				quoted = append(quoted, pgIdent(k))
			}
			matchRaw, _ := ec.param(i, "columns.matchingColumns", nil)
			var match []string
			for _, m := range asList(matchRaw) {
				match = append(match, toString(m))
			}
			switch op {
			case "insert":
				return pgQuery(ec, db, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING *", tbl, strings.Join(quoted, ","), strings.Join(ph, ",")), args...)
			case "upsert":
				if len(match) == 0 {
					return nil, permanentError{"postgres upsert: no column to match on"}
				}
				var sets []string
				for _, k := range cols {
					sets = append(sets, pgIdent(k)+"=EXCLUDED."+pgIdent(k))
				}
				var mq []string
				for _, m := range match {
					mq = append(mq, pgIdent(m))
				}
				return pgQuery(ec, db, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s RETURNING *", tbl, strings.Join(quoted, ","), strings.Join(ph, ","), strings.Join(mq, ","), strings.Join(sets, ",")), args...)
			default:
				if len(match) == 0 {
					return nil, permanentError{"postgres update: no column to match on"}
				}
				var sets, conds []string
				for n, k := range cols {
					isMatch := false
					for _, m := range match {
						if m == k {
							isMatch = true
						}
					}
					if isMatch {
						conds = append(conds, pgIdent(k)+"=$"+strconv.Itoa(n+1))
					} else {
						sets = append(sets, pgIdent(k)+"=$"+strconv.Itoa(n+1))
					}
				}
				return pgQuery(ec, db, fmt.Sprintf("UPDATE %s SET %s WHERE %s RETURNING *", tbl, strings.Join(sets, ","), strings.Join(conds, " AND ")), args...)
			}
		case "deleteTable":
			switch cmd, _ := ec.str(i, "deleteCommand", "truncate"); cmd {
			case "delete":
				where, args := pgWhere(ec, i)
				if _, err := db.ExecContext(ec.ctx, "DELETE FROM "+tbl+where, args...); err != nil {
					return nil, err
				}
			case "drop":
				if _, err := db.ExecContext(ec.ctx, "DROP TABLE IF EXISTS "+tbl); err != nil {
					return nil, err
				}
			default:
				if _, err := db.ExecContext(ec.ctx, "TRUNCATE TABLE "+tbl); err != nil {
					return nil, err
				}
			}
			return one(map[string]interface{}{"success": true}), nil
		}
		return nil, unsupportedOp("Postgres", "database", op)
	})
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func pgValue(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}, []interface{}:
		return mustJSON(t)
	}
	return v
}

func pgColumns(ec *execCtx, i int, it Item) map[string]interface{} {
	if mode, _ := ec.str(i, "columns.mappingMode", "autoMapInputData"); mode == "autoMapInputData" {
		return it.JSON
	}
	v, _ := ec.param(i, "columns.value", map[string]interface{}{})
	m, _ := v.(map[string]interface{})
	return m
}

func pgWhere(ec *execCtx, i int) (string, []interface{}) {
	return sqlWhere(ec, i, pgIdent, func(n int) string { return "$" + strconv.Itoa(n) })
}

// sqlWhere builds the WHERE clause of the SQL nodes' select/delete
// operations with the dialect's identifier quoting and placeholders.
func sqlWhere(ec *execCtx, i int, quote func(string) string, ph func(int) string) (string, []interface{}) {
	wv, _ := ec.param(i, "where.values", nil)
	var conds []string
	var args []interface{}
	ops := map[string]string{"equal": "=", "notEqual": "<>", "LIKE": "LIKE", "gt": ">", "lt": "<", "gte": ">=", "lte": "<="}
	for _, raw := range asList(wv) {
		m, _ := raw.(map[string]interface{})
		col := quote(toString(m["column"]))
		switch cond := toString(m["condition"]); cond {
		case "IS NULL", "IS NOT NULL":
			conds = append(conds, col+" "+cond)
		default:
			op := ops[cond]
			if op == "" {
				op = "="
			}
			args = append(args, m["value"])
			conds = append(conds, fmt.Sprintf("%s %s %s", col, op, ph(len(args))))
		}
	}
	if len(conds) == 0 {
		return "", nil
	}
	comb := " AND "
	if c, _ := ec.str(i, "combineConditions", "AND"); c == "OR" {
		comb = " OR "
	}
	return " WHERE " + strings.Join(conds, comb), args
}

func pgQuery(ec *execCtx, db *sql.DB, q string, args ...interface{}) ([]Item, error) {
	rows, err := db.QueryContext(ec.ctx, q, args...)
	if err != nil {
		return nil, permanentError{"postgres: " + err.Error()}
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []Item
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for k := range vals {
			ptrs[k] = &vals[k]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		j := map[string]interface{}{}
		for k, c := range cols {
			switch v := vals[k].(type) {
			case []byte:
				j[c] = string(v)
			case time.Time:
				j[c] = v.Format(time.RFC3339Nano)
			case int64:
				j[c] = float64(v)
			default:
				j[c] = v
			}
		}
		out = append(out, Item{JSON: j})
	}
	if len(out) == 0 && len(cols) == 0 {
		out = []Item{{JSON: map[string]interface{}{"success": true}}}
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------- Item Lists (legacy)

func itemListsNode(ec *execCtx) ([][]Item, error) {
	op, _ := ec.str(0, "operation", "splitOutItems")
	switch op {
	case "splitOutItems":
		return splitOutNode(ec)
	case "aggregateItems", "concatenateItems":
		if ec.version < 2 {
			// v1 used "aggregate" values without the newer default.
			if _, ok := ec.raw("aggregate"); !ok {
				ec.params["aggregate"] = "aggregateIndividualFields"
			}
		}
		return aggregateNode(ec)
	case "removeDuplicates":
		return removeDuplicatesNode(ec)
	case "sort":
		return sortNode(ec)
	case "limit":
		return limitNode(ec)
	}
	return nil, permanentError{fmt.Sprintf("Item Lists: operation %q is not supported", op)}
}

// ---------------------------------------------------------------- Airtable

func airtableNode(ec *execCtx) ([][]Item, error) {
	credType := "airtableTokenApi"
	if a, _ := ec.str(0, "authentication", "airtableTokenApi"); a == "airtableOAuth2Api" {
		credType = a
	}
	c, err := ec.credential(credType)
	if err != nil {
		return nil, err
	}
	h := map[string]string{"Authorization": "Bearer " + firstNonEmpty(accessToken(c), credString(c, "apiKey"))}
	base := ec.apiBase("airtable", "https://api.airtable.com/v0")
	flatten := func(rec map[string]interface{}) map[string]interface{} {
		out := map[string]interface{}{"id": rec["id"], "createdTime": rec["createdTime"]}
		if f, ok := rec["fields"].(map[string]interface{}); ok {
			for k, v := range f {
				out[k] = v
			}
		}
		return out
	}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "record", "search")
		baseID, _ := ec.rlString(i, "base")
		table, _ := ec.rlString(i, "table")
		tpath := base + "/" + url.PathEscape(baseID) + "/" + url.PathEscape(table)
		fields := func() map[string]interface{} {
			if mode, _ := ec.str(i, "columns.mappingMode", "defineBelow"); mode == "autoMapInputData" {
				return cloneJSON(it.JSON)
			}
			v, _ := ec.param(i, "columns.value", map[string]interface{}{})
			m, _ := v.(map[string]interface{})
			return m
		}
		switch r + "." + o {
		case "record.search", "record.list":
			q := url.Values{}
			if f, _ := ec.str(i, "filterByFormula", ""); f != "" {
				q.Set("filterByFormula", f)
			}
			if fv, _ := ec.param(i, "options.fields", nil); fv != nil {
				for _, x := range asList(fv) {
					q.Add("fields[]", toString(x))
				}
			}
			if v, _ := ec.rlString(i, "options.view"); v != "" {
				q.Set("view", v)
			}
			returnAll, _ := ec.boolean(i, "returnAll", true)
			limit, _ := ec.num(i, "limit", 100)
			var out []Item
			for offset := ""; ; {
				if offset != "" {
					q.Set("offset", offset)
				}
				res, _, err := ec.apiCall("GET", tpath+"?"+q.Encode(), h, nil, "")
				if err != nil {
					return nil, err
				}
				for _, rec := range asList(res["records"]) {
					rm, _ := rec.(map[string]interface{})
					out = append(out, Item{JSON: flatten(rm)})
				}
				offset = toString(res["offset"])
				if offset == "" || !returnAll && len(out) >= int(limit) {
					break
				}
			}
			if !returnAll && len(out) > int(limit) {
				out = out[:int(limit)]
			}
			return out, nil
		case "record.get":
			id, _ := ec.str(i, "id", "")
			res, _, err := ec.apiCall("GET", tpath+"/"+url.PathEscape(id), h, nil, "")
			return []Item{{JSON: flatten(res)}}, err
		case "record.create":
			res, _, err := ec.apiCall("POST", tpath, h, map[string]interface{}{"fields": fields(), "typecast": mustBool(ec, i, "options.typecast")}, "")
			return []Item{{JSON: flatten(res)}}, err
		case "record.update", "record.upsert":
			f := fields()
			matchRaw, _ := ec.param(i, "columns.matchingColumns", nil)
			match := "id"
			if l := asList(matchRaw); len(l) > 0 {
				match = toString(l[0])
			}
			if o == "upsert" || match != "id" {
				delete(f, "id")
				body := map[string]interface{}{"records": []interface{}{map[string]interface{}{"fields": f}}, "performUpsert": map[string]interface{}{"fieldsToMergeOn": []string{match}}, "typecast": mustBool(ec, i, "options.typecast")}
				res, _, err := ec.apiCall("PATCH", tpath, h, body, "")
				if err != nil {
					return nil, err
				}
				var out []Item
				for _, rec := range asList(res["records"]) {
					rm, _ := rec.(map[string]interface{})
					out = append(out, Item{JSON: flatten(rm)})
				}
				return out, nil
			}
			id := toString(f["id"])
			delete(f, "id")
			res, _, err := ec.apiCall("PATCH", tpath+"/"+url.PathEscape(id), h, map[string]interface{}{"fields": f, "typecast": mustBool(ec, i, "options.typecast")}, "")
			return []Item{{JSON: flatten(res)}}, err
		case "record.deleteRecord":
			id, _ := ec.str(i, "id", "")
			res, _, err := ec.apiCall("DELETE", tpath+"/"+url.PathEscape(id), h, nil, "")
			return one(res), err
		case "base.getSchema":
			res, _, err := ec.apiCall("GET", base+"/meta/bases/"+url.PathEscape(baseID)+"/tables", h, nil, "")
			if err != nil {
				return nil, err
			}
			return listItems(res["tables"]), nil
		case "base.getMany":
			res, _, err := ec.apiCall("GET", base+"/meta/bases", h, nil, "")
			if err != nil {
				return nil, err
			}
			return listItems(res["bases"]), nil
		}
		return nil, unsupportedOp("Airtable", r, o)
	})
}

func mustBool(ec *execCtx, i int, path string) bool {
	b, _ := ec.boolean(i, path, false)
	return b
}

// ---------------------------------------------------------------- Summarization Chain

func chainSummarizationNode(ec *execCtx) ([][]Item, error) {
	mode, _ := ec.str(0, "operationMode", "nodeInputJson")
	if mode == "documentLoader" {
		docs, err := ec.loadDocuments(ec.node.Config)
		if err != nil {
			return nil, fmt.Errorf("summarization chain: %w", err)
		}
		texts := make([]string, len(docs))
		for k, d := range docs {
			texts[k] = d.PageContent
		}
		return summarizeTexts(ec, texts)
	}
	var texts []string
	for _, it := range ec.items() {
		if mode == "nodeInputBinary" {
			for _, b := range it.Binary {
				bm, _ := b.(map[string]interface{})
				raw, _ := base64.StdEncoding.DecodeString(toString(bm["data"]))
				if strings.Contains(toString(bm["mimeType"]), "pdf") {
					if pages, _, err := pdfText(raw, 0); err == nil {
						raw = []byte(strings.Join(pages, "\n"))
					}
				}
				texts = append(texts, string(raw))
			}
			continue
		}
		texts = append(texts, mustJSON(it.JSON))
	}
	return summarizeTexts(ec, texts)
}

// summarizeTexts summarizes texts in one call, or map-reduce style when
// they exceed the chunk size.
func summarizeTexts(ec *execCtx, texts []string) ([][]Item, error) {
	chunk, _ := ec.num(0, "options.chunkSize", 0)
	if chunk <= 0 {
		chunk, _ = ec.num(0, "chunkSize", 1000)
	}
	model, base, err := ec.buildModel(0)
	if err != nil {
		return nil, err
	}
	prompt := "Write a concise summary of the following:\n\n\"{text}\"\n\nCONCISE SUMMARY:"
	if p, _ := ec.str(0, "options.summarizationMethodAndPrompts.values.prompt", ""); p != "" {
		prompt = p
	}
	combine := prompt
	if p, _ := ec.str(0, "options.summarizationMethodAndPrompts.values.combineMapPrompt", ""); p != "" {
		combine = p
	}
	summarize := func(tpl, text string) (string, error) {
		req := base
		req.Messages = []chatMsg{{Role: "user", Content: strings.ReplaceAll(tpl, "{text}", text)}}
		res, err := model.Chat(ec.ctx, req)
		if err != nil {
			return "", err
		}
		return res.Content, nil
	}
	all := strings.Join(texts, "\n\n")
	limit := int(chunk) * 4 // chunkSize is in tokens; ~4 chars per token
	if limit <= 0 || len(all) <= limit*4 {
		s, err := summarize(prompt, all)
		if err != nil {
			return nil, err
		}
		return [][]Item{{{JSON: map[string]interface{}{"response": map[string]interface{}{"text": s}}, PairedItem: paired(0)}}}, nil
	}
	var parts []string
	for start := 0; start < len(all); start += limit {
		end := start + limit
		if end > len(all) {
			end = len(all)
		}
		s, err := summarize(prompt, all[start:end])
		if err != nil {
			return nil, err
		}
		parts = append(parts, s)
	}
	s, err := summarize(combine, strings.Join(parts, "\n\n"))
	if err != nil {
		return nil, err
	}
	return [][]Item{{{JSON: map[string]interface{}{"response": map[string]interface{}{"text": s}}, PairedItem: paired(0)}}}, nil
}

// ---------------------------------------------------------------- Microsoft Outlook

func outlookNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("microsoftOutlookOAuth2Api")
	if err != nil {
		return nil, err
	}
	h := map[string]string{"Authorization": "Bearer " + accessToken(c)}
	base := ec.apiBase("graph", "https://graph.microsoft.com/v1.0") + "/me"
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "message", "send")
		id, _ := ec.rlString(i, "messageId")
		switch r + "." + o {
		case "message.send":
			subject, _ := ec.str(i, "subject", "")
			content, _ := ec.str(i, "bodyContent", "")
			ctype, _ := ec.str(i, "additionalFields.bodyContentType", "html")
			to, _ := ec.str(i, "toRecipients", "")
			msg := map[string]interface{}{"subject": subject, "body": map[string]interface{}{"contentType": ctype, "content": content}, "toRecipients": graphRecipients(to)}
			if cc, _ := ec.str(i, "additionalFields.ccRecipients", ""); cc != "" {
				msg["ccRecipients"] = graphRecipients(cc)
			}
			if _, err := ec.doRequest("POST", base+"/sendMail", h, mustBytes(map[string]interface{}{"message": msg}), "application/json"); err != nil {
				return nil, err
			}
			return one(map[string]interface{}{"success": true}), nil
		case "message.update":
			uf, _ := ec.param(i, "updateFields", map[string]interface{}{})
			res, _, err := ec.apiCall("PATCH", base+"/messages/"+url.PathEscape(id), h, uf, "")
			return one(res), err
		case "message.move":
			folder, _ := ec.rlString(i, "folderId")
			res, _, err := ec.apiCall("POST", base+"/messages/"+url.PathEscape(id)+"/move", h, map[string]interface{}{"destinationId": folder}, "")
			return one(res), err
		case "message.get":
			res, _, err := ec.apiCall("GET", base+"/messages/"+url.PathEscape(id), h, nil, "")
			return one(res), err
		case "message.delete":
			if _, err := ec.doRequest("DELETE", base+"/messages/"+url.PathEscape(id), h, nil, ""); err != nil {
				return nil, err
			}
			return one(map[string]interface{}{"success": true}), nil
		case "message.reply":
			msg, _ := ec.str(i, "message", "")
			if _, err := ec.doRequest("POST", base+"/messages/"+url.PathEscape(id)+"/reply", h, mustBytes(map[string]interface{}{"comment": msg}), "application/json"); err != nil {
				return nil, err
			}
			return one(map[string]interface{}{"success": true}), nil
		case "message.getAll":
			limit, _ := ec.num(i, "limit", 100)
			q := url.Values{"$top": {strconv.Itoa(int(clampF(limit, 1, 1000)))}}
			if f, _ := ec.str(i, "filtersUI.values.filterBy", ""); f == "search" {
				s, _ := ec.str(i, "filtersUI.values.search", "")
				q.Set("$search", `"`+s+`"`)
			}
			path := "/messages"
			if f, _ := ec.rlString(i, "filtersUI.values.foldersToInclude"); f != "" {
				path = "/mailFolders/" + url.PathEscape(f) + "/messages"
			}
			res, _, err := ec.apiCall("GET", base+path+"?"+q.Encode(), h, nil, "")
			if err != nil {
				return nil, err
			}
			return listItems(res["value"]), nil
		}
		return nil, unsupportedOp("Outlook", r, o)
	})
}

func graphRecipients(s string) []interface{} {
	var out []interface{}
	for _, a := range splitList(s) {
		out = append(out, map[string]interface{}{"emailAddress": map[string]interface{}{"address": a}})
	}
	return out
}

// ---------------------------------------------------------------- Edit Image (resize / information)

func editImageNode(ec *execCtx) ([][]Item, error) {
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		op, _ := ec.str(i, "operation", "information")
		prop, _ := ec.str(i, "dataPropertyName", "data")
		data, mt, name, err := binaryOf(it, prop)
		if err != nil {
			return nil, err
		}
		img, format, err := image.Decode(bytes.NewReader(data))
		if err != nil {
			return nil, permanentError{"Edit Image: unsupported image format (jpeg, png and gif are supported)"}
		}
		switch op {
		case "information":
			b := img.Bounds()
			j := cloneJSON(it.JSON)
			j["size"] = map[string]interface{}{"width": float64(b.Dx()), "height": float64(b.Dy())}
			j["format"] = strings.ToUpper(format)
			j["filesize"] = fmt.Sprintf("%dB", len(data))
			return []Item{{JSON: j, Binary: it.Binary}}, nil
		case "resize":
			w, _ := ec.num(i, "width", 500)
			h, _ := ec.num(i, "height", 500)
			mode, _ := ec.str(i, "resizeOption", "maximumArea")
			nw, nh := resizeDims(img.Bounds().Dx(), img.Bounds().Dy(), w, h, mode)
			out := scaleBilinear(img, nw, nh)
			var buf bytes.Buffer
			switch format {
			case "jpeg":
				err = jpeg.Encode(&buf, out, &jpeg.Options{Quality: int(mustNum(ec, i, "options.quality", 90))})
			case "gif":
				err = gif.Encode(&buf, out, nil)
			default:
				err = png.Encode(&buf, out)
			}
			if err != nil {
				return nil, err
			}
			bin := map[string]interface{}{}
			for k, v := range it.Binary {
				bin[k] = v
			}
			bin[prop] = binaryData(buf.Bytes(), mt, name)
			return []Item{{JSON: it.JSON, Binary: bin}}, nil
		}
		return nil, permanentError{fmt.Sprintf("Edit Image: operation %q is not supported", op)}
	})
}

func mustNum(ec *execCtx, i int, path string, def float64) float64 {
	v, err := ec.num(i, path, def)
	if err != nil {
		return def
	}
	return v
}

func resizeDims(ow, oh int, w, h float64, mode string) (int, int) {
	fw, fh := float64(ow), float64(oh)
	switch mode {
	case "ignoreAspectRatio":
		return int(w), int(h)
	case "percent":
		return int(fw * w / 100), int(fh * h / 100)
	case "onlyIfLarger":
		if fw <= w && fh <= h {
			return ow, oh
		}
	case "onlyIfSmaller":
		if fw >= w || fh >= h {
			return ow, oh
		}
	case "minimumArea":
		s := maxF(w/fw, h/fh)
		return int(fw * s), int(fh * s)
	}
	s := minF(w/fw, h/fh)
	return int(fw * s), int(fh * s)
}

func minF(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func maxF(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// scaleBilinear resizes an image with bilinear interpolation.
func scaleBilinear(src image.Image, w, h int) *image.RGBA {
	if w < 1 {
		w = 1
	}
	if h < 1 {
		h = 1
	}
	dst := image.NewRGBA(image.Rect(0, 0, w, h))
	b := src.Bounds()
	sx := float64(b.Dx()) / float64(w)
	sy := float64(b.Dy()) / float64(h)
	for y := 0; y < h; y++ {
		fy := (float64(y)+0.5)*sy - 0.5
		y0 := int(fy)
		if fy < 0 {
			y0, fy = 0, 0
		}
		y1 := minI(y0+1, b.Dy()-1)
		dy := fy - float64(y0)
		for x := 0; x < w; x++ {
			fx := (float64(x)+0.5)*sx - 0.5
			x0 := int(fx)
			if fx < 0 {
				x0, fx = 0, 0
			}
			x1 := minI(x0+1, b.Dx()-1)
			dx := fx - float64(x0)
			c00 := color.RGBAModel.Convert(src.At(b.Min.X+x0, b.Min.Y+y0)).(color.RGBA)
			c10 := color.RGBAModel.Convert(src.At(b.Min.X+x1, b.Min.Y+y0)).(color.RGBA)
			c01 := color.RGBAModel.Convert(src.At(b.Min.X+x0, b.Min.Y+y1)).(color.RGBA)
			c11 := color.RGBAModel.Convert(src.At(b.Min.X+x1, b.Min.Y+y1)).(color.RGBA)
			mix := func(a, b, c, d uint8) uint8 {
				top := float64(a)*(1-dx) + float64(b)*dx
				bot := float64(c)*(1-dx) + float64(d)*dx
				return uint8(top*(1-dy) + bot*dy + 0.5)
			}
			dst.SetRGBA(x, y, color.RGBA{mix(c00.R, c10.R, c01.R, c11.R), mix(c00.G, c10.G, c01.G, c11.G), mix(c00.B, c10.B, c01.B, c11.B), mix(c00.A, c10.A, c01.A, c11.A)})
		}
	}
	return dst
}

func minI(a, b int) int {
	if a < b {
		return a
	}
	return b
}
