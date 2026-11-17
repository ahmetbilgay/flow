package n8n

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// graphClient calls Microsoft Graph with an OAuth2 credential.
type graphClient struct {
	ec   *execCtx
	base string
	h    map[string]string
}

func (ec *execCtx) graph(credType string) (*graphClient, error) {
	c, err := ec.credential(credType)
	if err != nil {
		return nil, err
	}
	tok := accessToken(c)
	if tok == "" {
		return nil, permanentError{credType + " credential has no access token"}
	}
	return &graphClient{ec: ec, base: ec.apiBase("graph", "https://graph.microsoft.com/v1.0"), h: map[string]string{"Authorization": "Bearer " + tok}}, nil
}

func (g *graphClient) call(method, path string, body interface{}) (map[string]interface{}, error) {
	res, _, err := g.ec.apiCall(method, g.base+path, g.h, body, "")
	return res, err
}

// list follows @odata.nextLink pages.
func (g *graphClient) list(path string, all bool, limit int) ([]Item, error) {
	var out []Item
	next := g.base + path
	for page := 0; next != "" && page < 100; page++ {
		res, _, err := g.ec.apiCall("GET", next, g.h, nil, "")
		if err != nil {
			return nil, err
		}
		out = append(out, listItems(res["value"])...)
		if !all && len(out) >= limit {
			break
		}
		next = toString(res["@odata.nextLink"])
	}
	return capItems(out, all, limit), nil
}

// ---------------------------------------------------------------- OneDrive

func microsoftOneDriveNode(ec *execCtx) ([][]Item, error) {
	g, err := ec.graph("microsoftOneDriveOAuth2Api")
	if err != nil {
		return nil, err
	}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOpBy(i, "file", map[string]string{"file": "upload", "folder": "create"})
		item := func(key string) string { id, _ := ec.str(i, key, ""); return "/me/drive/items/" + url.PathEscape(id) }
		switch r + "." + o {
		case "file.upload":
			name, _ := ec.str(i, "fileName", "")
			parent, _ := ec.str(i, "parentId", "")
			var data []byte
			mt := "text/plain"
			if bin, _ := ec.boolean(i, "binaryData", false); bin {
				prop, _ := ec.str(i, "binaryPropertyName", "data")
				d, m, fname, err := binaryOf(it, prop)
				if err != nil {
					return nil, err
				}
				data, mt = d, m
				if name == "" {
					name = fname
				}
			} else {
				s, _ := ec.str(i, "fileContent", "")
				data = []byte(s)
			}
			if name == "" {
				return nil, permanentError{"OneDrive: file name is empty"}
			}
			p := "/me/drive/root:/" + escapePath(name) + ":/content"
			if parent != "" {
				p = "/me/drive/items/" + url.PathEscape(parent) + ":/" + escapePath(name) + ":/content"
			}
			res, _, err := ec.apiCall("PUT", g.base+p, g.h, data, mt)
			return one(res), err
		case "file.download":
			meta, err := g.call("GET", item("fileId"), nil)
			if err != nil {
				return nil, err
			}
			data, mt, err := ec.downloadWith(g.base+item("fileId")+"/content", g.h)
			if err != nil {
				return nil, err
			}
			prop, _ := ec.str(i, "binaryPropertyName", "data")
			name := toString(meta["name"])
			if fm := asMap(meta["file"]); toString(fm["mimeType"]) != "" {
				mt = toString(fm["mimeType"])
			}
			return []Item{{JSON: it.JSON, Binary: map[string]interface{}{prop: binaryData(data, mt, name)}}}, nil
		case "file.get", "folder.get":
			res, err := g.call("GET", item(map[string]string{"file": "fileId", "folder": "folderId"}[r]), nil)
			return one(res), err
		case "file.delete", "folder.delete":
			if _, err := g.call("DELETE", item(map[string]string{"file": "fileId", "folder": "folderId"}[r]), nil); err != nil {
				return nil, err
			}
			return one(map[string]interface{}{"success": true}), nil
		case "file.rename", "folder.rename":
			name, _ := ec.str(i, "newName", "")
			res, err := g.call("PATCH", item(map[string]string{"file": "fileId", "folder": "folderId"}[r]), map[string]interface{}{"name": name})
			return one(res), err
		case "file.copy":
			parent, _ := ec.str(i, "parentReference.id", "")
			body := map[string]interface{}{}
			if parent != "" {
				body["parentReference"] = map[string]interface{}{"id": parent}
			}
			if n, _ := ec.str(i, "additionalFields.name", ""); n != "" {
				body["name"] = n
			}
			if _, err := g.call("POST", item("fileId")+"/copy", body); err != nil {
				return nil, err
			}
			return one(map[string]interface{}{"success": true}), nil
		case "file.search", "folder.search":
			q, _ := ec.str(i, "query", "")
			return g.list("/me/drive/root/search(q='"+url.PathEscape(strings.ReplaceAll(q, "'", "''"))+"')", true, -1)
		case "folder.create":
			name, _ := ec.str(i, "name", "")
			parent, _ := ec.str(i, "options.parentFolderId", "")
			p := "/me/drive/root/children"
			if parent != "" {
				p = "/me/drive/items/" + url.PathEscape(parent) + "/children"
			}
			res, err := g.call("POST", p, map[string]interface{}{"name": name, "folder": map[string]interface{}{}, "@microsoft.graph.conflictBehavior": "rename"})
			return one(res), err
		case "folder.getChildren":
			return g.list(item("folderId")+"/children", true, -1)
		}
		return nil, unsupportedOp("OneDrive", r, o)
	})
}

// ---------------------------------------------------------------- Excel 365

func microsoftExcelNode(ec *execCtx) ([][]Item, error) {
	g, err := ec.graph("microsoftExcelOAuth2Api")
	if err != nil {
		return nil, err
	}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOpBy(i, "table", map[string]string{"table": "append", "worksheet": "readRows", "workbook": "getAll"})
		wb, _ := ec.rlString(i, "workbook")
		wbPath := "/me/drive/items/" + url.PathEscape(wb) + "/workbook"
		ws, _ := ec.rlString(i, "worksheet")
		wsPath := wbPath + "/worksheets/" + url.PathEscape(ws)
		switch r + "." + o {
		case "worksheet.append", "worksheet.readRows", "worksheet.getContent":
			used, err := g.call("GET", wsPath+"/usedRange", nil)
			if err != nil {
				return nil, err
			}
			rows := asList(used["values"])
			var header []string
			if len(rows) > 0 {
				for _, h := range asList(rows[0]) {
					header = append(header, toString(h))
				}
			}
			if o != "append" {
				var out []Item
				for _, row := range rows[min(1, len(rows)):] {
					j := map[string]interface{}{}
					for c, v := range asList(row) {
						if c < len(header) && header[c] != "" {
							j[header[c]] = v
						}
					}
					out = append(out, Item{JSON: j})
				}
				return out, nil
			}
			data := it.JSON
			if mode, _ := ec.str(i, "dataMode", "autoMap"); mode == "define" {
				data = map[string]interface{}{}
				v, _ := ec.param(i, "fieldsUi.values", nil)
				for _, f := range asList(v) {
					m := asMap(f)
					data[toString(m["column"])] = m["fieldValue"]
				}
			}
			if len(header) == 0 || (len(rows) == 1 && strings.Join(header, "") == "") {
				for k := range data {
					header = append(header, k)
				}
				sortStrings(header)
				if _, err := g.call("PATCH", wsPath+"/range(address='"+excelRange(1, 1, len(header))+"')", map[string]interface{}{"values": [][]interface{}{toIfaces(header)}}); err != nil {
					return nil, err
				}
				rows = []interface{}{toIfaces(header)}
			}
			vals := make([]interface{}, len(header))
			for c, h := range header {
				if v, ok := data[h]; ok {
					vals[c] = v
				} else {
					vals[c] = ""
				}
			}
			start := excelFirstRow(toString(used["address"])) + len(rows)
			res, err := g.call("PATCH", wsPath+"/range(address='"+excelRange(start, 1, len(header))+"')", map[string]interface{}{"values": []interface{}{vals}})
			return one(res), err
		case "table.append":
			tbl, _ := ec.rlString(i, "table")
			vals := []interface{}{}
			for _, v := range it.JSON {
				vals = append(vals, v)
			}
			res, err := g.call("POST", wsPath+"/tables/"+url.PathEscape(tbl)+"/rows", map[string]interface{}{"values": []interface{}{vals}})
			return one(res), err
		case "table.getRows":
			tbl, _ := ec.rlString(i, "table")
			all, limit := pageCount(ec, i, 100)
			return g.list(wsPath+"/tables/"+url.PathEscape(tbl)+"/rows", all, limit)
		case "workbook.getAll":
			all, limit := pageCount(ec, i, 100)
			return g.list("/me/drive/root/search(q='.xlsx')", all, limit)
		case "worksheet.getAll":
			all, limit := pageCount(ec, i, 100)
			return g.list(wbPath+"/worksheets", all, limit)
		}
		return nil, unsupportedOp("Microsoft Excel", r, o)
	})
}

func toIfaces(s []string) []interface{} {
	out := make([]interface{}, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}

func excelColumn(n int) string {
	s := ""
	for n > 0 {
		n--
		s = string(rune('A'+n%26)) + s
		n /= 26
	}
	return s
}

func excelRange(row, col, width int) string {
	return fmt.Sprintf("%s%d:%s%d", excelColumn(col), row, excelColumn(col+max(width, 1)-1), row)
}

// excelFirstRow returns the first row of an address like "Sheet1!A1:C9".
func excelFirstRow(addr string) int {
	if i := strings.LastIndex(addr, "!"); i >= 0 {
		addr = addr[i+1:]
	}
	n := 0
	for _, c := range addr {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		} else if n > 0 {
			break
		}
	}
	if n == 0 {
		n = 1
	}
	return n
}

// ---------------------------------------------------------------- Teams

func microsoftTeamsNode(ec *execCtx) ([][]Item, error) {
	g, err := ec.graph("microsoftTeamsOAuth2Api")
	if err != nil {
		return nil, err
	}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "channel", "create")
		team, _ := ec.rlString(i, "teamId")
		channel, _ := ec.rlString(i, "channelId")
		chPath := "/teams/" + url.PathEscape(team) + "/channels"
		message := func() map[string]interface{} {
			typ, _ := ec.str(i, "messageType", "text")
			msg, _ := ec.str(i, "message", "")
			return map[string]interface{}{"body": map[string]interface{}{"contentType": typ, "content": msg}}
		}
		switch r + "." + o {
		case "channel.create":
			name, _ := ec.str(i, "name", "")
			body := map[string]interface{}{"displayName": name}
			if d, _ := ec.str(i, "options.description", ""); d != "" {
				body["description"] = d
			}
			if t, _ := ec.str(i, "options.type", ""); t != "" {
				body["membershipType"] = t
			}
			res, err := g.call("POST", chPath, body)
			return one(res), err
		case "channel.get":
			res, err := g.call("GET", chPath+"/"+url.PathEscape(channel), nil)
			return one(res), err
		case "channel.getAll":
			all, limit := pageCount(ec, i, 100)
			return g.list(chPath, all, limit)
		case "channel.update":
			body := map[string]interface{}{}
			if n, _ := ec.str(i, "updateFields.name", ""); n != "" {
				body["displayName"] = n
			}
			if d, _ := ec.str(i, "updateFields.description", ""); d != "" {
				body["description"] = d
			}
			if _, err := g.call("PATCH", chPath+"/"+url.PathEscape(channel), body); err != nil {
				return nil, err
			}
			return one(map[string]interface{}{"success": true}), nil
		case "channel.delete":
			if _, err := g.call("DELETE", chPath+"/"+url.PathEscape(channel), nil); err != nil {
				return nil, err
			}
			return one(map[string]interface{}{"success": true}), nil
		case "channelMessage.create":
			p := chPath + "/" + url.PathEscape(channel) + "/messages"
			if reply, _ := ec.str(i, "options.makeReply", ""); reply != "" {
				p += "/" + url.PathEscape(reply) + "/replies"
			}
			res, err := g.call("POST", p, message())
			return one(res), err
		case "channelMessage.getAll":
			all, limit := pageCount(ec, i, 100)
			return g.list(chPath+"/"+url.PathEscape(channel)+"/messages", all, limit)
		case "chatMessage.create":
			chat, _ := ec.rlString(i, "chatId")
			res, err := g.call("POST", "/chats/"+url.PathEscape(chat)+"/messages", message())
			return one(res), err
		case "chatMessage.getAll":
			chat, _ := ec.rlString(i, "chatId")
			all, limit := pageCount(ec, i, 100)
			return g.list("/chats/"+url.PathEscape(chat)+"/messages", all, limit)
		}
		return nil, unsupportedOp("Microsoft Teams", r, o)
	})
}

// ---------------------------------------------------------------- Outlook trigger

// microsoftOutlookTriggerNode polls for messages received since the
// previous poll (the first poll only records the time).
func microsoftOutlookTriggerNode(ec *execCtx) ([][]Item, error) {
	g, err := ec.graph("microsoftOutlookOAuth2Api")
	if err != nil {
		return nil, err
	}
	pc, err := ec.openPoll()
	if err != nil {
		return nil, err
	}
	defer pc.close()
	now := time.Now().UTC().Format(time.RFC3339)
	if !pc.seen {
		return pc.commit(now, nil)
	}
	filter := []string{"receivedDateTime gt " + pc.prev}
	if rs, _ := ec.str(0, "filters.readStatus", ""); rs == "unread" {
		filter = append(filter, "isRead eq false")
	} else if rs == "read" {
		filter = append(filter, "isRead eq true")
	}
	if ha, _ := ec.boolean(0, "filters.hasAttachments", false); ha {
		filter = append(filter, "hasAttachments eq true")
	}
	if s, _ := ec.str(0, "filters.sender", ""); s != "" {
		filter = append(filter, "from/emailAddress/address eq '"+strings.ReplaceAll(s, "'", "''")+"'")
	}
	if cf, _ := ec.str(0, "filters.custom", ""); cf != "" {
		filter = append(filter, "("+cf+")")
	}
	q := url.Values{"$filter": {strings.Join(filter, " and ")}, "$orderby": {"receivedDateTime asc"}, "$top": {"100"}}
	output, _ := ec.str(0, "output", "simple")
	switch output {
	case "simple":
		q.Set("$select", "id,conversationId,subject,bodyPreview,from,toRecipients,categories,hasAttachments,receivedDateTime,isRead,webLink")
	case "fields":
		if f, _ := ec.param(0, "fields", nil); len(asList(f)) > 0 {
			var fs []string
			for _, x := range asList(f) {
				fs = append(fs, toString(x))
			}
			q.Set("$select", strings.Join(fs, ","))
		}
	}
	items, err := g.list("/me/messages?"+q.Encode(), true, -1)
	if err != nil {
		return nil, err
	}
	if dl, _ := ec.boolean(0, "options.downloadAttachments", false); dl {
		prefix, _ := ec.str(0, "options.attachmentsPrefix", "attachment_")
		for k, it := range items {
			if !toBool(it.JSON["hasAttachments"]) {
				continue
			}
			atts, err := g.list("/me/messages/"+url.PathEscape(toString(it.JSON["id"]))+"/attachments", true, -1)
			if err != nil {
				return nil, err
			}
			bin := map[string]interface{}{}
			for n, a := range atts {
				data, err := decodeB64(toString(a.JSON["contentBytes"]))
				if err != nil {
					continue
				}
				bin[fmt.Sprintf("%s%d", prefix, n)] = binaryData(data, toString(a.JSON["contentType"]), toString(a.JSON["name"]))
			}
			items[k].Binary = bin
		}
	}
	return pc.commit(now, items)
}
