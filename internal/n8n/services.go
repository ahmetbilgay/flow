package n8n

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// eachItem runs fn per item and collects result items.
func eachItem(ec *execCtx, fn func(i int, it Item) ([]Item, error)) ([][]Item, error) {
	var out []Item
	for i, it := range ec.items() {
		res, err := fn(i, it)
		if err != nil {
			return nil, fmt.Errorf("item %d: %w", i, err)
		}
		for _, r := range res {
			r.PairedItem = paired(i)
			out = append(out, r)
		}
	}
	return [][]Item{out}, nil
}

func listItems(v interface{}) []Item {
	var out []Item
	for _, x := range asList(v) {
		if m, ok := x.(map[string]interface{}); ok {
			out = append(out, Item{JSON: m})
		}
	}
	return out
}

func (ec *execCtx) resourceOp(i int, defResource, defOp string) (string, string) {
	r, _ := ec.str(i, "resource", defResource)
	o, _ := ec.str(i, "operation", defOp)
	return r, o
}

func unsupportedOp(node, r, o string) error {
	return permanentError{fmt.Sprintf("%s: %s/%s is not supported", node, r, o)}
}

// ---------------------------------------------------------------- WhatsApp

func whatsAppNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("whatsAppApi")
	if err != nil {
		return nil, err
	}
	h := map[string]string{"Authorization": "Bearer " + credString(c, "accessToken")}
	base := ec.apiBase("whatsapp", "https://graph.facebook.com/v21.0")
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "message", "send")
		switch r + "." + o {
		case "message.send", "message.sendTemplate":
			phoneID, _ := ec.str(i, "phoneNumberId", "")
			to, _ := ec.str(i, "recipientPhoneNumber", "")
			body := map[string]interface{}{"messaging_product": "whatsapp", "recipient_type": "individual", "to": strings.TrimPrefix(strings.ReplaceAll(to, " ", ""), "+")}
			typ, _ := ec.str(i, "messageType", "text")
			if o == "sendTemplate" {
				typ = "template"
			}
			switch typ {
			case "text":
				text, _ := ec.str(i, "textBody", "")
				prev, _ := ec.boolean(i, "additionalFields.previewUrl", false)
				body["type"], body["text"] = "text", map[string]interface{}{"body": text, "preview_url": prev}
			case "template":
				tpl, _ := ec.str(i, "template", "")
				name, lang, _ := strings.Cut(tpl, "|")
				t := map[string]interface{}{"name": name, "language": map[string]interface{}{"code": firstNonEmpty(lang, "en_US")}}
				if comps, _ := ec.param(i, "components.component", nil); len(asList(comps)) > 0 {
					t["components"] = whatsAppComponents(asList(comps))
				}
				body["type"], body["template"] = "template", t
			case "image", "audio", "document", "video", "sticker":
				media := map[string]interface{}{}
				switch p, _ := ec.str(i, "mediaPath", "useMediaLink"); p {
				case "useMediaLink":
					media["link"], _ = ec.str(i, "mediaLink", "")
				case "useMedianId", "useMediaId":
					media["id"], _ = ec.str(i, "mediaId", "")
				case "useMediaBinaryData":
					id, err := whatsAppUpload(ec, i, it, base, h, phoneID)
					if err != nil {
						return nil, err
					}
					media["id"] = id
				}
				if cap, _ := ec.str(i, "additionalFields.mediaCaption", ""); cap != "" && typ != "audio" && typ != "sticker" {
					media["caption"] = cap
				}
				if fn, _ := ec.str(i, "additionalFields.mediaFilename", ""); fn != "" && typ == "document" {
					media["filename"] = fn
				}
				body["type"], body[typ] = typ, media
			case "location":
				lat, _ := ec.num(i, "latitude", 0)
				lon, _ := ec.num(i, "longitude", 0)
				body["type"], body["location"] = "location", map[string]interface{}{"latitude": lat, "longitude": lon}
			default:
				return nil, permanentError{fmt.Sprintf("WhatsApp: message type %q is not supported", typ)}
			}
			res, _, err := ec.apiCall("POST", base+"/"+url.PathEscape(phoneID)+"/messages", h, body, "")
			return one(res), err
		case "media.mediaUrlGet":
			id, _ := ec.str(i, "mediaGetId", "")
			res, _, err := ec.apiCall("GET", base+"/"+url.PathEscape(id), h, nil, "")
			return one(res), err
		case "media.mediaDelete":
			id, _ := ec.str(i, "mediaDeleteId", "")
			res, _, err := ec.apiCall("DELETE", base+"/"+url.PathEscape(id), h, nil, "")
			return one(res), err
		case "media.mediaUpload":
			phoneID, _ := ec.str(i, "phoneNumberId", "")
			id, err := whatsAppUpload(ec, i, it, base, h, phoneID)
			return one(map[string]interface{}{"id": id}), err
		}
		return nil, unsupportedOp("WhatsApp", r, o)
	})
}

func whatsAppComponents(list []interface{}) []interface{} {
	var out []interface{}
	for _, raw := range list {
		c, _ := raw.(map[string]interface{})
		comp := map[string]interface{}{"type": c["type"]}
		var params []interface{}
		bp, _ := c["bodyParameters"].(map[string]interface{})
		for _, p := range asList(bp["parameter"]) {
			pm, _ := p.(map[string]interface{})
			params = append(params, map[string]interface{}{"type": firstNonEmpty(toString(pm["type"]), "text"), "text": pm["text"]})
		}
		if len(params) > 0 {
			comp["parameters"] = params
		}
		out = append(out, comp)
	}
	return out
}

func whatsAppUpload(ec *execCtx, i int, it Item, base string, h map[string]string, phoneID string) (string, error) {
	prop, _ := ec.str(i, "mediaPropertyName", "data")
	data, mt, name, err := binaryOf(it, prop)
	if err != nil {
		return "", err
	}
	body, ctype := multipartBody(map[string]string{"messaging_product": "whatsapp", "type": mt}, "file", name, mt, data)
	res, _, err := ec.apiCall("POST", base+"/"+url.PathEscape(phoneID)+"/media", h, body, ctype)
	if err != nil {
		return "", err
	}
	return toString(res["id"]), nil
}

// ---------------------------------------------------------------- WordPress

func wordpressNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("wordpressApi")
	if err != nil {
		return nil, err
	}
	base := strings.TrimRight(credString(c, "url"), "/") + "/wp-json/wp/v2"
	h := map[string]string{"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte(credString(c, "username")+":"+credString(c, "password")))}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "post", "create")
		kind := map[string]string{"post": "posts", "page": "pages", "user": "users"}[r]
		if kind == "" {
			return nil, unsupportedOp("WordPress", r, o)
		}
		switch o {
		case "create":
			body := map[string]interface{}{}
			if t, _ := ec.str(i, "title", ""); t != "" {
				body["title"] = t
			}
			if af, _ := ec.param(i, "additionalFields", nil); af != nil {
				for k, v := range asMap(af) {
					body[wpField(k)] = v
				}
			}
			res, _, err := ec.apiCall("POST", base+"/"+kind, h, body, "")
			return one(res), err
		case "update":
			id, _ := ec.str(i, r+"Id", "")
			body := map[string]interface{}{}
			if uf, _ := ec.param(i, "updateFields", nil); uf != nil {
				for k, v := range asMap(uf) {
					body[wpField(k)] = v
				}
			}
			res, _, err := ec.apiCall("POST", base+"/"+kind+"/"+url.PathEscape(id), h, body, "")
			return one(res), err
		case "get":
			id, _ := ec.str(i, r+"Id", "")
			res, _, err := ec.apiCall("GET", base+"/"+kind+"/"+url.PathEscape(id), h, nil, "")
			return one(res), err
		case "getAll":
			limit, _ := ec.num(i, "limit", 10)
			q := url.Values{"per_page": {fmt.Sprint(int(limit))}}
			if opts, _ := ec.param(i, "options", nil); opts != nil {
				for k, v := range asMap(opts) {
					q.Set(k, toString(v))
				}
			}
			_, raw, err := ec.apiCall("GET", base+"/"+kind+"?"+q.Encode(), h, nil, "")
			if err != nil {
				return nil, err
			}
			var list []interface{}
			_ = jsonUnmarshal(raw, &list)
			return listItems(list), nil
		case "delete":
			id, _ := ec.str(i, r+"Id", "")
			res, _, err := ec.apiCall("DELETE", base+"/"+kind+"/"+url.PathEscape(id)+"?force=true", h, nil, "")
			return one(res), err
		}
		return nil, unsupportedOp("WordPress", r, o)
	})
}

func wpField(k string) string {
	switch k {
	case "authorId":
		return "author"
	case "commentStatus":
		return "comment_status"
	case "pingStatus":
		return "ping_status"
	case "featuredMediaId":
		return "featured_media"
	}
	return k
}

// ---------------------------------------------------------------- Notion

func notionNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("notionApi")
	if err != nil {
		return nil, err
	}
	h := map[string]string{"Authorization": "Bearer " + credString(c, "apiKey"), "Notion-Version": "2022-06-28"}
	base := ec.apiBase("notion", "https://api.notion.com") + "/v1"
	call := func(method, path string, body interface{}) (map[string]interface{}, error) {
		res, _, err := ec.apiCall(method, base+path, h, body, "")
		return res, err
	}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "page", "")
		if o == "" {
			o = map[string]string{"page": "create", "database": "get", "databasePage": "create", "block": "append", "user": "get"}[r]
		}
		simple, _ := ec.boolean(i, "simple", true)
		page := func(p map[string]interface{}) Item {
			if simple {
				return Item{JSON: simplifyNotionPage(p)}
			}
			return Item{JSON: p}
		}
		switch r + "." + o {
		case "databasePage.getAll":
			db, _ := ec.rlString(i, "databaseId")
			body := map[string]interface{}{}
			if f, err := notionFilter(ec, i); err != nil {
				return nil, err
			} else if f != nil {
				body["filter"] = f
			}
			returnAll, _ := ec.boolean(i, "returnAll", false)
			limit, _ := ec.num(i, "limit", 50)
			var out []Item
			for cursor := ""; ; {
				if cursor != "" {
					body["start_cursor"] = cursor
				}
				res, err := call("POST", "/databases/"+notionID(db)+"/query", body)
				if err != nil {
					return nil, err
				}
				for _, p := range asList(res["results"]) {
					pm, _ := p.(map[string]interface{})
					out = append(out, page(pm))
				}
				cursor = toString(res["next_cursor"])
				if !toBool(res["has_more"]) || cursor == "" || !returnAll && len(out) >= int(limit) {
					break
				}
			}
			if !returnAll && len(out) > int(limit) {
				out = out[:int(limit)]
			}
			return out, nil
		case "databasePage.get", "page.get":
			id, _ := ec.rlString(i, "pageId")
			res, err := call("GET", "/pages/"+notionID(id), nil)
			return []Item{page(res)}, err
		case "databasePage.update":
			id, _ := ec.rlString(i, "pageId")
			props, err := notionProperties(ec, i)
			if err != nil {
				return nil, err
			}
			res, err := call("PATCH", "/pages/"+notionID(id), map[string]interface{}{"properties": props})
			return []Item{page(res)}, err
		case "databasePage.create":
			db, _ := ec.rlString(i, "databaseId")
			props, err := notionProperties(ec, i)
			if err != nil {
				return nil, err
			}
			if t, _ := ec.str(i, "title", ""); t != "" {
				dbInfo, err := call("GET", "/databases/"+notionID(db), nil)
				if err != nil {
					return nil, err
				}
				for name, p := range asMap(dbInfo["properties"]) {
					if pm, _ := p.(map[string]interface{}); pm["type"] == "title" {
						props[name] = map[string]interface{}{"title": richText(t)}
					}
				}
			}
			body := map[string]interface{}{"parent": map[string]interface{}{"database_id": notionID(db)}, "properties": props}
			if blocks := notionBlocks(ec, i); len(blocks) > 0 {
				body["children"] = blocks
			}
			res, err := call("POST", "/pages", body)
			return []Item{page(res)}, err
		case "page.create":
			parent, _ := ec.rlString(i, "pageId")
			t, _ := ec.str(i, "title", "")
			body := map[string]interface{}{"parent": map[string]interface{}{"page_id": notionID(parent)}, "properties": map[string]interface{}{"title": map[string]interface{}{"title": richText(t)}}}
			if blocks := notionBlocks(ec, i); len(blocks) > 0 {
				body["children"] = blocks
			}
			res, err := call("POST", "/pages", body)
			return []Item{page(res)}, err
		case "page.archive":
			id, _ := ec.rlString(i, "pageId")
			res, err := call("PATCH", "/pages/"+notionID(id), map[string]interface{}{"archived": true})
			return []Item{page(res)}, err
		case "database.get":
			db, _ := ec.rlString(i, "databaseId")
			res, err := call("GET", "/databases/"+notionID(db), nil)
			return one(res), err
		case "database.getAll", "database.search", "page.search":
			q, _ := ec.str(i, "text", "")
			kind := "database"
			if r == "page" {
				kind = "page"
			}
			res, err := call("POST", "/search", map[string]interface{}{"query": q, "filter": map[string]interface{}{"property": "object", "value": kind}})
			if err != nil {
				return nil, err
			}
			return listItems(res["results"]), nil
		case "block.getAll":
			id, _ := ec.rlString(i, "blockId")
			res, err := call("GET", "/blocks/"+notionID(id)+"/children?page_size=100", nil)
			if err != nil {
				return nil, err
			}
			items := listItems(res["results"])
			for k := range items {
				if t := blockText(items[k].JSON); t != "" {
					items[k].JSON["content"] = t
				}
			}
			return items, nil
		case "block.append":
			id, _ := ec.rlString(i, "blockId")
			res, err := call("PATCH", "/blocks/"+notionID(id)+"/children", map[string]interface{}{"children": notionBlocks(ec, i)})
			if err != nil {
				return nil, err
			}
			return listItems(res["results"]), nil
		case "user.get":
			id, _ := ec.str(i, "userId", "")
			res, err := call("GET", "/users/"+url.PathEscape(id), nil)
			return one(res), err
		}
		return nil, unsupportedOp("Notion", r, o)
	})
}

var notionIDRe = regexp.MustCompile(`[0-9a-fA-F]{32}`)

func notionID(s string) string {
	if m := notionIDRe.FindString(strings.ReplaceAll(s, "-", "")); m != "" {
		return m
	}
	return s
}

func richText(s string) []interface{} {
	return []interface{}{map[string]interface{}{"type": "text", "text": map[string]interface{}{"content": s}}}
}

func plainText(v interface{}) string {
	var b strings.Builder
	for _, rt := range asList(v) {
		m, _ := rt.(map[string]interface{})
		b.WriteString(toString(m["plain_text"]))
	}
	return b.String()
}

func blockText(b map[string]interface{}) string {
	t, _ := b["type"].(string)
	inner, _ := b[t].(map[string]interface{})
	return plainText(inner["rich_text"])
}

var nonWord = regexp.MustCompile(`[^a-zA-Z0-9]+`)

// simplifyNotionPage mirrors n8n's simplified page output.
func simplifyNotionPage(p map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{"id": p["id"], "url": p["url"]}
	props, _ := p["properties"].(map[string]interface{})
	for name, raw := range props {
		pm, _ := raw.(map[string]interface{})
		typ := toString(pm["type"])
		v := pm[typ]
		var val interface{}
		switch typ {
		case "title", "rich_text":
			val = plainText(v)
			if typ == "title" {
				out["name"] = val
			}
		case "select", "status":
			if m, ok := v.(map[string]interface{}); ok {
				val = m["name"]
			}
		case "multi_select":
			var names []interface{}
			for _, o := range asList(v) {
				om, _ := o.(map[string]interface{})
				names = append(names, om["name"])
			}
			val = names
		case "date":
			if m, ok := v.(map[string]interface{}); ok {
				if m["end"] != nil {
					val = map[string]interface{}{"start": m["start"], "end": m["end"]}
				} else {
					val = m["start"]
				}
			}
		case "people", "relation":
			var ids []interface{}
			for _, o := range asList(v) {
				om, _ := o.(map[string]interface{})
				ids = append(ids, om["id"])
			}
			val = ids
		case "formula":
			if m, ok := v.(map[string]interface{}); ok {
				val = m[toString(m["type"])]
			}
		case "files":
			var urls []interface{}
			for _, o := range asList(v) {
				om, _ := o.(map[string]interface{})
				ft := toString(om["type"])
				if inner, ok := om[ft].(map[string]interface{}); ok {
					urls = append(urls, inner["url"])
				}
			}
			val = urls
		default:
			val = v
		}
		key := "property_" + strings.Trim(strings.ToLower(nonWord.ReplaceAllString(name, "_")), "_")
		out[key] = val
	}
	return out
}

// notionProperties converts propertiesUi.propertyValues into Notion values.
func notionProperties(ec *execCtx, i int) (map[string]interface{}, error) {
	v, err := ec.param(i, "propertiesUi.propertyValues", nil)
	if err != nil {
		return nil, err
	}
	props := map[string]interface{}{}
	for _, raw := range asList(v) {
		p, _ := raw.(map[string]interface{})
		name, typ, _ := strings.Cut(toString(p["key"]), "|")
		var val interface{}
		switch typ {
		case "title":
			val = map[string]interface{}{"title": richText(toString(p["title"]))}
		case "rich_text":
			val = map[string]interface{}{"rich_text": richText(firstNonEmpty(toString(p["textContent"]), toString(p["richText"])))}
		case "number":
			val = map[string]interface{}{"number": p["numberValue"]}
		case "checkbox":
			val = map[string]interface{}{"checkbox": toBool(p["checkboxValue"])}
		case "select":
			val = map[string]interface{}{"select": map[string]interface{}{"name": p["selectValue"]}}
		case "status":
			val = map[string]interface{}{"status": map[string]interface{}{"name": p["statusValue"]}}
		case "multi_select":
			var opts []interface{}
			ms := p["multiSelectValue"]
			if s, ok := ms.(string); ok {
				ms = splitAny(s)
			}
			for _, o := range asList(ms) {
				opts = append(opts, map[string]interface{}{"name": o})
			}
			val = map[string]interface{}{"multi_select": opts}
		case "url":
			val = map[string]interface{}{"url": p["urlValue"]}
		case "email":
			val = map[string]interface{}{"email": p["emailValue"]}
		case "phone_number":
			val = map[string]interface{}{"phone_number": p["phoneValue"]}
		case "date":
			d := map[string]interface{}{"start": p["date"]}
			if toBool(p["range"]) {
				d = map[string]interface{}{"start": p["dateStart"], "end": p["dateEnd"]}
			}
			val = map[string]interface{}{"date": d}
		case "relation":
			var rels []interface{}
			for _, id := range splitAny(toString(p["relationValue"])) {
				rels = append(rels, map[string]interface{}{"id": id})
			}
			val = map[string]interface{}{"relation": rels}
		case "people":
			var ppl []interface{}
			for _, id := range splitAny(toString(p["peopleValue"])) {
				ppl = append(ppl, map[string]interface{}{"id": id})
			}
			val = map[string]interface{}{"people": ppl}
		default:
			return nil, permanentError{fmt.Sprintf("Notion property type %q is not supported", typ)}
		}
		props[name] = val
	}
	return props, nil
}

func splitAny(s string) []interface{} {
	var out []interface{}
	for _, p := range splitList(s) {
		out = append(out, p)
	}
	return out
}

func notionBlocks(ec *execCtx, i int) []interface{} {
	v, _ := ec.param(i, "blockUi.blockValues", nil)
	var out []interface{}
	for _, raw := range asList(v) {
		b, _ := raw.(map[string]interface{})
		typ := firstNonEmpty(toString(b["type"]), "paragraph")
		text := toString(b["textContent"])
		if text == "" {
			if rt, ok := b["text"].(map[string]interface{}); ok {
				var parts []string
				for _, t := range asList(rt["text"]) {
					tm, _ := t.(map[string]interface{})
					parts = append(parts, toString(tm["text"]))
				}
				text = strings.Join(parts, "")
			}
		}
		out = append(out, map[string]interface{}{"object": "block", "type": typ, typ: map[string]interface{}{"rich_text": richText(text)}})
	}
	return out
}

func notionFilter(ec *execCtx, i int) (interface{}, error) {
	ft, _ := ec.str(i, "filterType", "none")
	switch ft {
	case "json":
		v, err := ec.param(i, "filterJson", "")
		if err != nil {
			return nil, err
		}
		var f interface{}
		if s, ok := v.(string); ok {
			if err := jsonUnmarshal([]byte(s), &f); err != nil {
				return nil, permanentError{"Notion filter JSON is invalid"}
			}
			return f, nil
		}
		return v, nil
	case "manual":
		v, _ := ec.param(i, "filters.conditions", nil)
		var conds []interface{}
		for _, raw := range asList(v) {
			c, _ := raw.(map[string]interface{})
			name, typ, _ := strings.Cut(toString(c["key"]), "|")
			cond := toString(c["condition"])
			valKey := map[string]string{"title": "titleValue", "rich_text": "richTextValue", "number": "numberValue", "checkbox": "checkboxValue",
				"select": "selectValue", "multi_select": "multiSelectValue", "status": "statusValue", "date": "date", "email": "emailValue", "url": "urlValue", "phone_number": "phoneNumberValue"}[typ]
			val := c[valKey]
			if strings.HasPrefix(cond, "is_") && strings.HasSuffix(cond, "empty") {
				val = true
			}
			conds = append(conds, map[string]interface{}{"property": name, typ: map[string]interface{}{cond: val}})
		}
		if len(conds) == 0 {
			return nil, nil
		}
		if len(conds) == 1 {
			return conds[0], nil
		}
		if mt, _ := ec.str(i, "matchType", "anyFilter"); mt == "allFilters" {
			return map[string]interface{}{"and": conds}, nil
		}
		return map[string]interface{}{"or": conds}, nil
	}
	return nil, nil
}

// ---------------------------------------------------------------- Slack

func slackNode(ec *execCtx) ([][]Item, error) {
	credType := "slackApi"
	if a, _ := ec.str(0, "authentication", "accessToken"); a == "oAuth2" {
		credType = "slackOAuth2Api"
	}
	c, err := ec.credential(credType)
	if err != nil {
		return nil, err
	}
	h := map[string]string{"Authorization": "Bearer " + accessToken(c)}
	base := ec.apiBase("slack", "https://slack.com/api")
	call := func(method string, body map[string]interface{}) (map[string]interface{}, error) {
		res, _, err := ec.apiCall("POST", base+"/"+method, h, body, "application/json; charset=utf-8")
		if err != nil {
			return nil, err
		}
		if ok, _ := res["ok"].(bool); !ok {
			return nil, permanentError{"Slack error: " + toString(res["error"])}
		}
		return res, nil
	}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "message", "post")
		target := func() (string, error) {
			if sel, _ := ec.str(i, "select", "channel"); sel == "user" {
				return ec.rlString(i, "user")
			}
			return ec.rlString(i, "channelId")
		}
		switch r + "." + o {
		case "message.post", "message.update":
			ch, err := target()
			if err != nil {
				return nil, err
			}
			body := map[string]interface{}{"channel": ch}
			mt, _ := ec.str(i, "messageType", "text")
			switch mt {
			case "block":
				bv, _ := ec.param(i, "blocksUi", "")
				var blocks interface{}
				if s, ok := bv.(string); ok {
					_ = jsonUnmarshal([]byte(s), &blocks)
				} else {
					blocks = bv
				}
				if m, ok := blocks.(map[string]interface{}); ok && m["blocks"] != nil {
					blocks = m["blocks"]
				}
				body["blocks"] = blocks
				if t, _ := ec.str(i, "text", ""); t != "" {
					body["text"] = t
				}
			default:
				body["text"], _ = ec.str(i, "text", "")
			}
			if oo, _ := ec.param(i, "otherOptions", nil); oo != nil {
				for k, v := range asMap(oo) {
					switch k {
					case "thread_ts":
						if m, ok := v.(map[string]interface{}); ok {
							if rv, ok := m["replyValues"].(map[string]interface{}); ok {
								body["thread_ts"] = rv["thread_ts"]
								if toBool(rv["reply_broadcast"]) {
									body["reply_broadcast"] = true
								}
							}
						}
					case "mrkdwn", "unfurl_links", "unfurl_media", "link_names":
						body[k] = v
					case "botProfile":
						if m, ok := v.(map[string]interface{}); ok {
							if iv, ok := m["imageValues"].(map[string]interface{}); ok {
								body["icon_emoji"], body["icon_url"] = iv["icon_emoji"], iv["icon_url"]
							}
						}
					}
				}
			}
			method := "chat.postMessage"
			if o == "update" {
				method = "chat.update"
				body["ts"], _ = ec.str(i, "ts", "")
			}
			res, err := call(method, body)
			return one(res), err
		case "message.sendAndWait":
			ch, err := target()
			if err != nil {
				return nil, err
			}
			a, err := ec.sendAndWaitApproval(i)
			if err != nil {
				return nil, err
			}
			msg, _ := ec.str(i, "message", "")
			btns := []interface{}{map[string]interface{}{"type": "button", "style": "primary", "text": map[string]interface{}{"type": "plain_text", "text": a.approveLabel}, "url": a.approveURL}}
			if a.double {
				btns = append(btns, map[string]interface{}{"type": "button", "style": "danger", "text": map[string]interface{}{"type": "plain_text", "text": a.declineLabel}, "url": a.declineURL})
			}
			blocks := []interface{}{
				map[string]interface{}{"type": "section", "text": map[string]interface{}{"type": "mrkdwn", "text": msg}},
				map[string]interface{}{"type": "actions", "elements": btns},
			}
			if _, err := call("chat.postMessage", map[string]interface{}{"channel": ch, "text": msg, "blocks": blocks}); err != nil {
				return nil, err
			}
			return nil, errWaitSignal
		case "message.delete":
			ch, err := target()
			if err != nil {
				return nil, err
			}
			ts, _ := ec.str(i, "timestamp", "")
			res, err := call("chat.delete", map[string]interface{}{"channel": ch, "ts": ts})
			return one(res), err
		case "message.search":
			q, _ := ec.str(i, "query", "")
			res, _, err := ec.apiCall("GET", base+"/search.messages?"+url.Values{"query": {q}}.Encode(), h, nil, "")
			if err != nil {
				return nil, err
			}
			msgs, _ := res["messages"].(map[string]interface{})
			return listItems(msgs["matches"]), nil
		case "user.info":
			u, _ := ec.rlString(i, "user")
			res, _, err := ec.apiCall("GET", base+"/users.info?"+url.Values{"user": {u}}.Encode(), h, nil, "")
			if err != nil {
				return nil, err
			}
			user, _ := res["user"].(map[string]interface{})
			return one(user), nil
		}
		return nil, unsupportedOp("Slack", r, o)
	})
}

// ---------------------------------------------------------------- Discord

func discordNode(ec *execCtx) ([][]Item, error) {
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		content, _ := ec.str(i, "content", "")
		if content == "" {
			content, _ = ec.str(i, "text", "")
		}
		body := map[string]interface{}{"content": content}
		if opts, _ := ec.param(i, "options", nil); opts != nil {
			for k, v := range asMap(opts) {
				switch k {
				case "username", "avatar_url", "tts":
					body[k] = v
				case "avatarUrl":
					body["avatar_url"] = v
				}
			}
		}
		auth, _ := ec.str(i, "authentication", "webhook")
		if ec.version < 2 {
			u, _ := ec.str(i, "webhookUri", "")
			res, _, err := ec.apiCall("POST", u+"?wait=true", nil, body, "")
			return one(res), err
		}
		switch auth {
		case "webhook":
			c, err := ec.credential("discordWebhookApi")
			if err != nil {
				return nil, err
			}
			res, _, err := ec.apiCall("POST", credString(c, "webhookUri")+"?wait=true", nil, body, "")
			return one(res), err
		default:
			c, err := ec.credential("discordBotApi")
			if err != nil {
				return nil, err
			}
			ch, _ := ec.rlString(i, "channelId")
			res, _, err := ec.apiCall("POST", ec.apiBase("discord", "https://discord.com/api/v10")+"/channels/"+url.PathEscape(ch)+"/messages",
				map[string]string{"Authorization": "Bot " + credString(c, "botToken")}, body, "")
			return one(res), err
		}
	})
}

// ---------------------------------------------------------------- Facebook Graph API

func facebookGraphNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("facebookGraphApi")
	if err != nil {
		return nil, err
	}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		host, _ := ec.str(i, "hostUrl", "graph.facebook.com")
		switch host {
		case "graph", "":
			host = "graph.facebook.com"
		case "graph-video":
			host = "graph-video.facebook.com"
		}
		ver, _ := ec.str(i, "graphApiVersion", "")
		nodeID, _ := ec.str(i, "node", "")
		edge, _ := ec.str(i, "edge", "")
		method, _ := ec.str(i, "httpRequestMethod", "GET")
		u := ec.apiBase("facebook", "https://"+host)
		if ver != "" {
			u += "/" + ver
		}
		u += "/" + nodeID
		if edge != "" {
			u += "/" + edge
		}
		q := url.Values{"access_token": {credString(c, "accessToken")}}
		if f, _ := ec.param(i, "options.fields.field", nil); f != nil {
			var names []string
			for _, x := range asList(f) {
				m, _ := x.(map[string]interface{})
				names = append(names, toString(m["name"]))
			}
			q.Set("fields", strings.Join(names, ","))
		}
		if qp, _ := ec.param(i, "options.queryParameters.parameter", nil); qp != nil {
			for _, p := range kvList(qp) {
				q.Set(p[0], p[1])
			}
		}
		if qj, _ := ec.param(i, "options.queryParametersJson", ""); toString(qj) != "" {
			kv, err := jsonKV(qj)
			if err != nil {
				return nil, err
			}
			for _, p := range kv {
				q.Set(p[0], p[1])
			}
		}
		var body interface{}
		ctype := ""
		if sb, _ := ec.boolean(i, "sendBinaryData", false); sb {
			prop, _ := ec.str(i, "binaryPropertyName", "data")
			field, name := prop, prop
			if k, v, ok := strings.Cut(prop, ":"); ok {
				field, name = k, v
			}
			data, mt, fname, err := binaryOf(it, name)
			if err != nil {
				return nil, err
			}
			body, ctype = multipartBody(nil, field, fname, mt, data)
		}
		res, _, err := ec.apiCall(method, u+"?"+q.Encode(), nil, body, ctype)
		return one(res), err
	})
}
