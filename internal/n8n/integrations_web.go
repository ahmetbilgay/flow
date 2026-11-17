package n8n

import (
	"encoding/base64"
	"net/url"
	"strconv"
	"strings"
)

// pageCount returns how many items a getAll operation should fetch.
func pageCount(ec *execCtx, i int, def float64) (all bool, limit int) {
	all, _ = ec.boolean(i, "returnAll", false)
	l, _ := ec.num(i, "limit", def)
	return all, int(l)
}

func capItems(items []Item, all bool, limit int) []Item {
	if !all && limit >= 0 && len(items) > limit {
		return items[:limit]
	}
	return items
}

func basicAuth(user, pass string) map[string]string {
	return map[string]string{"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))}
}

// ---------------------------------------------------------------- GitHub

func githubNode(ec *execCtx) ([][]Item, error) {
	credType := "githubApi"
	if a, _ := ec.str(0, "authentication", "accessToken"); a == "oAuth2" {
		credType = "githubOAuth2Api"
	}
	c, err := ec.credential(credType)
	if err != nil {
		return nil, err
	}
	tok := credString(c, "accessToken")
	if tok == "" {
		tok = accessToken(c)
	}
	base := strings.TrimRight(credString(c, "server"), "/")
	if base == "" {
		base = ec.apiBase("github", "https://api.github.com")
	}
	h := map[string]string{"Authorization": "token " + tok, "Accept": "application/vnd.github+json", "User-Agent": "flow-n8n"}
	call := func(method, path string, body interface{}) (map[string]interface{}, []byte, error) {
		return ec.apiCall(method, base+path, h, body, "")
	}
	list := func(i int, path string, q url.Values) ([]Item, error) {
		all, limit := pageCount(ec, i, 50)
		var out []Item
		for page := 1; page <= 100; page++ {
			q.Set("per_page", "100")
			q.Set("page", strconv.Itoa(page))
			_, raw, err := call("GET", path+"?"+q.Encode(), nil)
			if err != nil {
				return nil, err
			}
			var arr []interface{}
			_ = jsonUnmarshal(raw, &arr)
			out = append(out, listItems(arr)...)
			if len(arr) < 100 || (!all && len(out) >= limit) {
				break
			}
		}
		return capItems(out, all, limit), nil
	}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOpBy(i, "issue", map[string]string{"issue": "create", "file": "create", "repository": "getIssues", "user": "getRepositories", "release": "create"})
		owner, _ := ec.rlString(i, "owner")
		repo, _ := ec.rlString(i, "repository")
		rp := "/repos/" + url.PathEscape(owner) + "/" + url.PathEscape(repo)
		switch r + "." + o {
		case "file.create", "file.edit":
			path, _ := ec.str(i, "filePath", "")
			msg, _ := ec.str(i, "commitMessage", "")
			var content []byte
			if bin, _ := ec.boolean(i, "binaryData", false); bin {
				prop, _ := ec.str(i, "binaryPropertyName", "data")
				data, _, _, err := binaryOf(it, prop)
				if err != nil {
					return nil, err
				}
				content = data
			} else {
				s, _ := ec.str(i, "fileContent", "")
				content = []byte(s)
			}
			body := map[string]interface{}{"message": msg, "content": base64.StdEncoding.EncodeToString(content)}
			ap := asMap(mustParam(ec, "additionalParameters", map[string]interface{}{}))
			if b := asMap(ap["branch"]); toString(b["branch"]) != "" {
				body["branch"] = b["branch"]
			}
			for _, k := range []string{"author", "committer"} {
				if m := asMap(ap[k]); len(m) > 0 {
					body[k] = m
				}
			}
			if o == "edit" {
				q := ""
				if b := toString(body["branch"]); b != "" {
					q = "?ref=" + url.QueryEscape(b)
				}
				cur, _, err := call("GET", rp+"/contents/"+escapePath(path)+q, nil)
				if err != nil {
					return nil, err
				}
				body["sha"] = cur["sha"]
			}
			res, _, err := call("PUT", rp+"/contents/"+escapePath(path), body)
			return one(res), err
		case "file.get":
			path, _ := ec.str(i, "filePath", "")
			q := ""
			if ref, _ := ec.str(i, "additionalParameters.reference", ""); ref != "" {
				q = "?ref=" + url.QueryEscape(ref)
			}
			res, _, err := call("GET", rp+"/contents/"+escapePath(path)+q, nil)
			if err != nil {
				return nil, err
			}
			if asBin, _ := ec.boolean(i, "asBinaryProperty", true); asBin {
				data, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(toString(res["content"]), "\n", ""))
				if err != nil {
					return nil, permanentError{"github: file content is not base64"}
				}
				prop, _ := ec.str(i, "binaryPropertyName", "data")
				name := toString(res["name"])
				return []Item{{JSON: it.JSON, Binary: map[string]interface{}{prop: binaryData(data, mimeByExt(name), name)}}}, nil
			}
			return one(res), nil
		case "file.delete":
			path, _ := ec.str(i, "filePath", "")
			msg, _ := ec.str(i, "commitMessage", "")
			cur, _, err := call("GET", rp+"/contents/"+escapePath(path), nil)
			if err != nil {
				return nil, err
			}
			res, _, err := call("DELETE", rp+"/contents/"+escapePath(path), map[string]interface{}{"message": msg, "sha": cur["sha"]})
			return one(res), err
		case "file.list":
			path, _ := ec.str(i, "filePath", "")
			_, raw, err := call("GET", rp+"/contents/"+escapePath(path), nil)
			if err != nil {
				return nil, err
			}
			var arr []interface{}
			_ = jsonUnmarshal(raw, &arr)
			return listItems(arr), nil
		case "issue.create":
			title, _ := ec.str(i, "title", "")
			body := map[string]interface{}{"title": title}
			if b, _ := ec.str(i, "body", ""); b != "" {
				body["body"] = b
			}
			for _, k := range []string{"labels", "assignees"} {
				if v, _ := ec.param(i, k, nil); len(asList(v)) > 0 {
					var l []string
					for _, x := range asList(v) {
						if s := toString(asMap(x)[strings.TrimSuffix(k, "s")]); s != "" {
							l = append(l, s)
						} else if s := toString(x); s != "" && asMap(x) == nil {
							l = append(l, s)
						}
					}
					body[k] = l
				}
			}
			res, _, err := call("POST", rp+"/issues", body)
			return one(res), err
		case "issue.get", "issue.edit", "issue.createComment", "issue.lock":
			num, _ := ec.str(i, "issueNumber", "")
			p := rp + "/issues/" + url.PathEscape(num)
			switch o {
			case "get":
				res, _, err := call("GET", p, nil)
				return one(res), err
			case "edit":
				fields := map[string]interface{}{}
				for k := range asMap(mustParam(ec, "editFields", map[string]interface{}{})) {
					v, _ := ec.param(i, "editFields."+k, nil)
					fields[k] = v
				}
				res, _, err := call("PATCH", p, fields)
				return one(res), err
			case "createComment":
				b, _ := ec.str(i, "body", "")
				res, _, err := call("POST", p+"/comments", map[string]interface{}{"body": b})
				return one(res), err
			default:
				reason, _ := ec.str(i, "lockReason", "resolved")
				if _, _, err := call("PUT", p+"/lock", map[string]interface{}{"lock_reason": reason}); err != nil {
					return nil, err
				}
				return one(map[string]interface{}{"success": true}), nil
			}
		case "repository.get":
			res, _, err := call("GET", rp, nil)
			return one(res), err
		case "repository.getIssues", "repository.getPullRequests":
			q := url.Values{}
			key := "getRepositoryIssuesFilters"
			path := rp + "/issues"
			if o == "getPullRequests" {
				key, path = "getRepositoryPullRequestsFilters", rp+"/pulls"
			}
			for k := range asMap(mustParam(ec, key, map[string]interface{}{})) {
				v, _ := ec.param(i, key+"."+k, nil)
				q.Set(k, toString(v))
			}
			return list(i, path, q)
		case "user.getRepositories":
			return list(i, "/users/"+url.PathEscape(owner)+"/repos", url.Values{})
		case "release.create":
			tag, _ := ec.str(i, "releaseTag", "")
			body := map[string]interface{}{"tag_name": tag}
			for k := range asMap(mustParam(ec, "additionalFields", map[string]interface{}{})) {
				v, _ := ec.param(i, "additionalFields."+k, nil)
				body[k] = v
			}
			res, _, err := call("POST", rp+"/releases", body)
			return one(res), err
		}
		return nil, unsupportedOp("GitHub", r, o)
	})
}

func escapePath(p string) string {
	parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
	for k, s := range parts {
		parts[k] = url.PathEscape(s)
	}
	return strings.Join(parts, "/")
}

// ---------------------------------------------------------------- Hacker News

func hackerNewsNode(ec *execCtx) ([][]Item, error) {
	base := ec.apiBase("hackernews", "https://hn.algolia.com") + "/api/v1"
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "article", "get")
		switch {
		case r == "all":
			all, limit := pageCount(ec, i, 100)
			q := url.Values{}
			if kw, _ := ec.str(i, "additionalFields.keyword", ""); kw != "" {
				q.Set("query", kw)
			}
			var tags []string
			if v, _ := ec.param(i, "additionalFields.tags", nil); v != nil {
				for _, t := range asList(v) {
					tags = append(tags, toString(t))
				}
			}
			if len(tags) > 0 {
				q.Set("tags", strings.Join(tags, ","))
			}
			var out []Item
			for page := 0; page < 50; page++ {
				q.Set("hitsPerPage", "100")
				q.Set("page", strconv.Itoa(page))
				res, _, err := ec.apiCall("GET", base+"/search?"+q.Encode(), nil, nil, "")
				if err != nil {
					return nil, err
				}
				hits := listItems(res["hits"])
				out = append(out, hits...)
				if len(hits) < 100 || (!all && len(out) >= limit) || page+1 >= int(toFloatDefault(res["nbPages"], 1)) {
					break
				}
			}
			return capItems(out, all, limit), nil
		case r == "article" && o == "get":
			id, _ := ec.str(i, "articleId", "")
			res, _, err := ec.apiCall("GET", base+"/items/"+url.PathEscape(id), nil, nil, "")
			if err != nil {
				return nil, err
			}
			if inc, _ := ec.boolean(i, "additionalFields.includeComments", false); !inc {
				delete(res, "children")
			}
			return one(res), nil
		case r == "user" && o == "get":
			u, _ := ec.str(i, "username", "")
			res, _, err := ec.apiCall("GET", base+"/users/"+url.PathEscape(u), nil, nil, "")
			return one(res), err
		}
		return nil, unsupportedOp("Hacker News", r, o)
	})
}

// ---------------------------------------------------------------- Reddit

func redditNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("redditOAuth2Api")
	if err != nil {
		return nil, err
	}
	h := map[string]string{"Authorization": "Bearer " + accessToken(c), "User-Agent": "flow-n8n"}
	base := ec.apiBase("reddit", "https://oauth.reddit.com")
	get := func(path string, q url.Values) (interface{}, error) {
		if q == nil {
			q = url.Values{}
		}
		q.Set("raw_json", "1")
		_, raw, err := ec.apiCall("GET", base+path+"?"+q.Encode(), h, nil, "")
		if err != nil {
			return nil, err
		}
		var v interface{}
		_ = jsonUnmarshal(raw, &v)
		return v, nil
	}
	children := func(v interface{}) []Item {
		var out []Item
		for _, ch := range asList(asMap(asMap(v)["data"])["children"]) {
			if d := asMap(asMap(ch)["data"]); d != nil {
				out = append(out, Item{JSON: d})
			}
		}
		return out
	}
	form := func(path string, f url.Values) (map[string]interface{}, error) {
		f.Set("api_type", "json")
		res, _, err := ec.apiCall("POST", base+path, h, []byte(f.Encode()), "application/x-www-form-urlencoded")
		return res, err
	}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOpBy(i, "post", map[string]string{"post": "create", "postComment": "create", "profile": "get"})
		sub, _ := ec.str(i, "subreddit", "")
		all, limit := pageCount(ec, i, 100)
		switch r + "." + o {
		case "post.search":
			kw, _ := ec.str(i, "keyword", "")
			q := url.Values{"q": {kw}, "limit": {strconv.Itoa(minPositive(limit, 100, all))}}
			path := "/search.json"
			if loc, _ := ec.str(i, "location", "subreddit"); loc == "subreddit" && sub != "" {
				path = "/r/" + url.PathEscape(sub) + "/search.json"
				q.Set("restrict_sr", "on")
			}
			if s, _ := ec.str(i, "additionalFields.sort", ""); s != "" {
				q.Set("sort", s)
			}
			v, err := get(path, q)
			if err != nil {
				return nil, err
			}
			return capItems(children(v), all, limit), nil
		case "post.getAll":
			cat, _ := ec.str(i, "filters.category", "hot")
			v, err := get("/r/"+url.PathEscape(sub)+"/"+url.PathEscape(cat)+".json", url.Values{"limit": {strconv.Itoa(minPositive(limit, 100, all))}})
			if err != nil {
				return nil, err
			}
			return capItems(children(v), all, limit), nil
		case "post.get", "postComment.getAll":
			id, _ := ec.str(i, "postId", "")
			v, err := get("/r/"+url.PathEscape(sub)+"/comments/"+url.PathEscape(id)+".json", nil)
			if err != nil {
				return nil, err
			}
			l := asList(v)
			if o == "get" {
				if len(l) > 0 {
					if p := children(l[0]); len(p) > 0 {
						return p[:1], nil
					}
				}
				return nil, permanentError{"reddit: post not found"}
			}
			if len(l) < 2 {
				return nil, nil
			}
			return capItems(children(l[1]), all, limit), nil
		case "post.create":
			kind, _ := ec.str(i, "kind", "self")
			title, _ := ec.str(i, "title", "")
			f := url.Values{"sr": {sub}, "kind": {kind}, "title": {title}}
			if kind == "self" {
				t, _ := ec.str(i, "text", "")
				f.Set("text", t)
			} else {
				u, _ := ec.str(i, "url", "")
				f.Set("url", u)
			}
			if rs, _ := ec.boolean(i, "resubmit", false); rs {
				f.Set("resubmit", "true")
			}
			res, err := form("/api/submit", f)
			if err != nil {
				return nil, err
			}
			return one(asMap(asMap(res["json"])["data"])), nil
		case "postComment.create":
			id, _ := ec.str(i, "postId", "")
			text, _ := ec.str(i, "commentText", "")
			res, err := form("/api/comment", url.Values{"thing_id": {"t3_" + strings.TrimPrefix(id, "t3_")}, "text": {text}})
			if err != nil {
				return nil, err
			}
			things := asList(asMap(asMap(asMap(res["json"])["data"]))["things"])
			if len(things) > 0 {
				return one(asMap(asMap(things[0])["data"])), nil
			}
			return one(res), nil
		case "profile.get":
			v, err := get("/api/v1/me", nil)
			if err != nil {
				return nil, err
			}
			return one(asMap(v)), nil
		}
		return nil, unsupportedOp("Reddit", r, o)
	})
}

func minPositive(limit, max int, all bool) int {
	if all || limit <= 0 || limit > max {
		return max
	}
	return limit
}

// ---------------------------------------------------------------- Twilio

func twilioNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("twilioApi")
	if err != nil {
		return nil, err
	}
	sid := credString(c, "accountSid")
	h := basicAuth(sid, credString(c, "authToken"))
	if credString(c, "authType") == "apiKey" {
		h = basicAuth(credString(c, "apiKeySid"), credString(c, "apiKeySecret"))
	}
	base := ec.apiBase("twilio", "https://api.twilio.com") + "/2010-04-01/Accounts/" + url.PathEscape(sid)
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "sms", "send")
		from, _ := ec.str(i, "from", "")
		to, _ := ec.str(i, "to", "")
		f := url.Values{"From": {from}, "To": {to}}
		path := "/Messages.json"
		switch r + "." + o {
		case "sms.send":
			if wa, _ := ec.boolean(i, "toWhatsapp", false); wa {
				f.Set("From", "whatsapp:"+from)
				f.Set("To", "whatsapp:"+to)
			}
			msg, _ := ec.str(i, "message", "")
			f.Set("Body", msg)
		case "call.make":
			msg, _ := ec.str(i, "message", "")
			if tw, _ := ec.boolean(i, "twiml", false); !tw {
				msg = "<Response><Say>" + xmlEscape(msg) + "</Say></Response>"
			}
			f.Set("Twiml", msg)
			path = "/Calls.json"
		default:
			return nil, unsupportedOp("Twilio", r, o)
		}
		if cb, _ := ec.str(i, "options.statusCallback", ""); cb != "" {
			f.Set("StatusCallback", cb)
		}
		res, _, err := ec.apiCall("POST", base+path, h, []byte(f.Encode()), "application/x-www-form-urlencoded")
		return one(res), err
	})
}

func xmlEscape(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&apos;").Replace(s)
}

// ---------------------------------------------------------------- OpenWeatherMap

func openWeatherMapNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("openWeatherMapApi")
	if err != nil {
		return nil, err
	}
	base := ec.apiBase("openweathermap", "https://api.openweathermap.org") + "/data/2.5"
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		op, _ := ec.str(i, "operation", "currentWeather")
		units, _ := ec.str(i, "format", "metric")
		q := url.Values{"appid": {credString(c, "accessToken")}, "units": {units}}
		switch loc, _ := ec.str(i, "locationSelection", "cityName"); loc {
		case "cityId":
			id, _ := ec.str(i, "cityId", "")
			q.Set("id", id)
		case "coordinates":
			lat, _ := ec.str(i, "latitude", "")
			lon, _ := ec.str(i, "longitude", "")
			q.Set("lat", lat)
			q.Set("lon", lon)
		case "zipCode":
			z, _ := ec.str(i, "zipCode", "")
			q.Set("zip", z)
		default:
			city, _ := ec.str(i, "cityName", "")
			q.Set("q", city)
		}
		if l, _ := ec.str(i, "language", ""); l != "" {
			q.Set("lang", l)
		}
		path := "/weather"
		if op == "5DayForecast" {
			path = "/forecast"
		}
		res, _, err := ec.apiCall("GET", base+path+"?"+q.Encode(), nil, nil, "")
		return one(res), err
	})
}

// ---------------------------------------------------------------- urlscan.io

func urlScanIoNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("urlScanIoApi")
	if err != nil {
		return nil, err
	}
	h := map[string]string{"API-Key": credString(c, "apiKey")}
	base := ec.apiBase("urlscan", "https://urlscan.io") + "/api/v1"
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "scan", "perform")
		switch r + "." + o {
		case "scan.perform":
			u, _ := ec.str(i, "url", "")
			body := map[string]interface{}{"url": u}
			for k := range asMap(mustParam(ec, "additionalFields", map[string]interface{}{})) {
				v, _ := ec.param(i, "additionalFields."+k, nil)
				if k == "tags" {
					v = splitList(toString(v))
				}
				if k == "customAgent" {
					k = "customagent"
				}
				body[k] = v
			}
			res, _, err := ec.apiCall("POST", base+"/scan/", h, body, "")
			if err != nil {
				return nil, err
			}
			res["scanId"] = res["uuid"]
			return one(res), nil
		case "scan.get":
			id, _ := ec.str(i, "scanId", "")
			res, _, err := ec.apiCall("GET", base+"/result/"+url.PathEscape(id)+"/", h, nil, "")
			return one(res), err
		case "scan.getAll":
			q, _ := ec.str(i, "filters.query", "*")
			all, limit := pageCount(ec, i, 50)
			res, _, err := ec.apiCall("GET", base+"/search/?"+url.Values{"q": {q}, "size": {strconv.Itoa(minPositive(limit, 10000, all))}}.Encode(), h, nil, "")
			if err != nil {
				return nil, err
			}
			return capItems(listItems(res["results"]), all, limit), nil
		}
		return nil, unsupportedOp("urlscan.io", r, o)
	})
}

// ---------------------------------------------------------------- APITemplate.io

func apiTemplateIoNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("apiTemplateIoApi")
	if err != nil {
		return nil, err
	}
	h := map[string]string{"X-API-KEY": credString(c, "apiKey")}
	base := ec.apiBase("apitemplate", "https://api.apitemplate.io") + "/v1"
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "image", "create")
		switch r + "." + o {
		case "account.get":
			res, _, err := ec.apiCall("GET", base+"/account-information", h, nil, "")
			return one(res), err
		case "image.create", "pdf.create":
			tplKey := "imageTemplateId"
			if r == "pdf" {
				tplKey = "pdfTemplateId"
			}
			tpl, _ := ec.str(i, tplKey, "")
			if tpl == "" {
				return nil, permanentError{"APITemplate.io: template ID is empty"}
			}
			var body interface{}
			jsonParams, _ := ec.boolean(i, "jsonParameters", false)
			switch {
			case r == "image" && jsonParams:
				v, _ := ec.param(i, "overridesJson", "[]")
				body = map[string]interface{}{"overrides": parseJSONParam(v)}
			case r == "image":
				var ov []interface{}
				v, _ := ec.param(i, "overridesUi.overrideValues", nil)
				for _, e := range asList(v) {
					m := map[string]interface{}{}
					for _, p := range asList(asMap(asMap(e)["propertiesUi"])["propertyValues"]) {
						pm := asMap(p)
						m[toString(pm["key"])] = pm["value"]
					}
					ov = append(ov, m)
				}
				body = map[string]interface{}{"overrides": ov}
			case jsonParams:
				v, _ := ec.param(i, "propertiesJson", "{}")
				body = parseJSONParam(v)
			default:
				m := map[string]interface{}{}
				v, _ := ec.param(i, "propertiesUi.propertyValues", nil)
				for _, p := range asList(v) {
					pm := asMap(p)
					m[toString(pm["key"])] = pm["value"]
				}
				body = m
			}
			res, _, err := ec.apiCall("POST", base+"/create?template_id="+url.QueryEscape(tpl), h, body, "")
			if err != nil {
				return nil, err
			}
			if dl, _ := ec.boolean(i, "download", false); dl {
				data, mt, err := ec.downloadWith(toString(res["download_url"]), nil)
				if err != nil {
					return nil, err
				}
				name, _ := ec.str(i, "options.fileName", "")
				if name == "" {
					name = toString(res["transaction_ref"])
				}
				prop, _ := ec.str(i, "binaryProperty", "data")
				return []Item{{JSON: res, Binary: map[string]interface{}{prop: binaryData(data, mt, name)}}}, nil
			}
			return one(res), nil
		}
		return nil, unsupportedOp("APITemplate.io", r, o)
	})
}

func parseJSONParam(v interface{}) interface{} {
	if s, ok := v.(string); ok {
		var out interface{}
		if jsonUnmarshal([]byte(s), &out) == nil {
			return out
		}
		return s
	}
	return v
}

// ---------------------------------------------------------------- uProc

func uprocNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("uprocApi")
	if err != nil {
		return nil, err
	}
	h := basicAuth(credString(c, "email"), credString(c, "apiKey"))
	base := ec.apiBase("uproc", "https://api.uproc.com") + "/api/v2/process"
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		tool, _ := ec.str(i, "tool", "")
		if tool == "" {
			return nil, permanentError{"uProc: tool is empty"}
		}
		params := map[string]interface{}{}
		for k := range ec.params {
			switch k {
			case "group", "tool", "additionalOptions":
				continue
			}
			v, err := ec.param(i, k, nil)
			if err != nil {
				return nil, err
			}
			params[k] = v
		}
		body := map[string]interface{}{"processor": tool, "params": params}
		if hook, _ := ec.str(i, "additionalOptions.dataWebhook", ""); hook != "" {
			body["callback"] = map[string]interface{}{"data": hook}
		}
		res, _, err := ec.apiCall("POST", base, h, body, "")
		return one(res), err
	})
}

// ---------------------------------------------------------------- WooCommerce

func wooCommerceNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("wooCommerceApi")
	if err != nil {
		return nil, err
	}
	base := credBaseURL(c, "") + "/wp-json/wc/v3"
	h := basicAuth(credString(c, "consumerKey"), credString(c, "consumerSecret"))
	paths := map[string]string{"customer": "/customers", "order": "/orders", "product": "/products"}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "product", "create")
		p, ok := paths[r]
		if !ok {
			return nil, unsupportedOp("WooCommerce", r, o)
		}
		idKey := r + "Id"
		fields := func(key string) map[string]interface{} {
			m := map[string]interface{}{}
			for k := range asMap(mustParam(ec, key, map[string]interface{}{})) {
				v, _ := ec.param(i, key+"."+k, nil)
				m[k] = v
			}
			return m
		}
		switch o {
		case "getAll":
			all, limit := pageCount(ec, i, 50)
			q := url.Values{}
			for k, v := range fields("filters") {
				q.Set(k, toString(v))
			}
			if r != "customer" {
				for k, v := range fields("options") {
					q.Set(k, toString(v))
				}
			}
			var out []Item
			for page := 1; page <= 100; page++ {
				q.Set("per_page", "100")
				q.Set("page", strconv.Itoa(page))
				_, raw, err := ec.apiCall("GET", base+p+"?"+q.Encode(), h, nil, "")
				if err != nil {
					return nil, err
				}
				var arr []interface{}
				_ = jsonUnmarshal(raw, &arr)
				out = append(out, listItems(arr)...)
				if len(arr) < 100 || (!all && len(out) >= limit) {
					break
				}
			}
			return capItems(out, all, limit), nil
		case "get", "delete":
			id, _ := ec.str(i, idKey, "")
			method, q := "GET", ""
			if o == "delete" {
				method, q = "DELETE", "?force=true"
			}
			res, _, err := ec.apiCall(method, base+p+"/"+url.PathEscape(id)+q, h, nil, "")
			return one(res), err
		case "create":
			body := fields("additionalFields")
			switch r {
			case "customer":
				e, _ := ec.str(i, "email", "")
				body["email"] = e
			case "product":
				n, _ := ec.str(i, "name", "")
				body["name"] = n
			}
			res, _, err := ec.apiCall("POST", base+p, h, body, "")
			return one(res), err
		case "update":
			id, _ := ec.str(i, idKey, "")
			res, _, err := ec.apiCall("PUT", base+p+"/"+url.PathEscape(id), h, fields("updateFields"), "")
			return one(res), err
		}
		return nil, unsupportedOp("WooCommerce", r, o)
	})
}
