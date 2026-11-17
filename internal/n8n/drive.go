package n8n

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/textproto"
	"net/url"
	"strings"
)

type driveAPI struct {
	ec      *execCtx
	base    string
	upload  string
	headers map[string]string
}

var googleExportDefaults = map[string]struct{ key, def string }{
	"application/vnd.google-apps.document":     {"docsToFormat", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
	"application/vnd.google-apps.spreadsheet":  {"sheetsToFormat", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
	"application/vnd.google-apps.presentation": {"slidesToFormat", "application/vnd.openxmlformats-officedocument.presentationml.presentation"},
	"application/vnd.google-apps.drawing":      {"drawingsToFormat", "image/jpeg"},
}

func googleDriveNode(ec *execCtx) ([][]Item, error) {
	if ec.version < 3 {
		params, err := driveV2Params(ec.params)
		if err != nil {
			return nil, err
		}
		v3 := *ec
		v3.params, v3.version = params, 3
		ec = &v3
	}
	h, err := ec.googleAuth("drive", "googleDriveOAuth2Api")
	if err != nil {
		return nil, err
	}
	base := ec.apiBase("drive", "https://www.googleapis.com")
	d := &driveAPI{ec: ec, base: base + "/drive/v3", upload: base + "/upload/drive/v3", headers: h}
	var out []Item
	for i, it := range ec.items() {
		resource, _ := ec.str(i, "resource", "file")
		op, _ := ec.str(i, "operation", map[string]string{"file": "upload", "fileFolder": "search", "folder": "create"}[resource])
		res, err := d.do(i, it, resource, op)
		if err != nil {
			return nil, fmt.Errorf("google drive %s/%s: %w", resource, op, err)
		}
		for _, r := range res {
			r.PairedItem = paired(i)
			out = append(out, r)
		}
	}
	return [][]Item{out}, nil
}

// driveV2Ops maps Google Drive v1/v2 operations to their v3 names.
var driveV2Ops = map[string]string{
	"file.upload": "upload", "file.download": "download", "file.delete": "deleteFile", "file.copy": "copy",
	"file.share": "share", "folder.create": "create", "folder.delete": "deleteFolder", "folder.share": "share",
}

// driveV2Params rewrites Google Drive v1/v2 parameters into the v3 layout.
func driveV2Params(p map[string]interface{}) (map[string]interface{}, error) {
	resource := toString(p["resource"])
	if resource == "" {
		resource = "file"
	}
	op := toString(p["operation"])
	if op == "" {
		op = map[string]string{"file": "upload", "folder": "create"}[resource]
	}
	v3op, ok := driveV2Ops[resource+"."+op]
	if !ok {
		return nil, permanentError{fmt.Sprintf("Google Drive v1/v2 operation %s/%s is not supported (v3 is)", resource, op)}
	}
	out := map[string]interface{}{}
	for k, v := range p {
		out[k] = v
	}
	out["resource"], out["operation"] = resource, v3op
	opts := asMap(p["options"])
	firstParent := func(v interface{}) {
		if l := asList(v); len(l) > 0 && toString(l[0]) != "" {
			out["folderId"] = l[0]
		}
	}
	switch resource + "." + v3op {
	case "file.upload":
		firstParent(p["parents"])
		if toBool(p["binaryData"]) {
			prop := toString(p["binaryPropertyName"])
			if prop == "" {
				prop = "data"
			}
			out["inputDataFieldName"] = prop
		} else {
			out["operation"], out["content"] = "createFromText", p["fileContent"]
		}
	case "file.copy":
		if n, ok := opts["name"]; ok {
			out["name"] = n
		}
		firstParent(opts["parents"])
	case "folder.create":
		firstParent(opts["parents"])
	case "folder.deleteFolder", "folder.share":
		out["folderNoRootId"] = p["fileId"]
	}
	return out, nil
}

func (d *driveAPI) call(method, u string, body interface{}) (map[string]interface{}, error) {
	out, _, err := d.ec.apiCall(method, u, d.headers, body, "")
	return out, err
}

func (d *driveAPI) parent(i int) (string, error) {
	f, err := d.ec.rlString(i, "folderId")
	if err != nil {
		return "", err
	}
	if f == "" {
		f = "root"
	}
	return f, nil
}

func (d *driveAPI) multipartUpload(meta map[string]interface{}, data []byte, mt string) (map[string]interface{}, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	mh := textproto.MIMEHeader{"Content-Type": {"application/json; charset=UTF-8"}}
	p, _ := w.CreatePart(mh)
	mb, _ := json.Marshal(meta)
	_, _ = p.Write(mb)
	p, _ = w.CreatePart(textproto.MIMEHeader{"Content-Type": {mt}})
	_, _ = p.Write(data)
	_ = w.Close()
	u := d.upload + "/files?uploadType=multipart&supportsAllDrives=true&fields=id,name,mimeType,webViewLink,parents"
	out, _, err := d.ec.apiCall("POST", u, d.headers, buf.Bytes(), "multipart/related; boundary="+w.Boundary())
	return out, err
}

func (d *driveAPI) do(i int, it Item, resource, op string) ([]Item, error) {
	ec := d.ec
	fileID := func() string { id, _ := ec.rlString(i, "fileId"); return id }
	switch resource + "." + op {
	case "file.upload":
		prop, _ := ec.str(i, "inputDataFieldName", "data")
		data, mt, fname, err := binaryOf(it, prop)
		if err != nil {
			return nil, err
		}
		name, _ := ec.str(i, "name", fname)
		if name == "" {
			name = fname
		}
		parent, err := d.parent(i)
		if err != nil {
			return nil, err
		}
		res, err := d.multipartUpload(map[string]interface{}{"name": name, "parents": []string{parent}}, data, mt)
		return one(res), err
	case "file.createFromText":
		content, _ := ec.str(i, "content", "")
		name, _ := ec.str(i, "name", "Untitled")
		parent, err := d.parent(i)
		if err != nil {
			return nil, err
		}
		meta := map[string]interface{}{"name": name, "parents": []string{parent}}
		if conv, _ := ec.boolean(i, "options.convertToGoogleDocument", false); conv {
			meta["mimeType"] = "application/vnd.google-apps.document"
		}
		res, err := d.multipartUpload(meta, []byte(content), "text/plain")
		return one(res), err
	case "file.download":
		id := fileID()
		meta, err := d.call("GET", d.base+"/files/"+url.PathEscape(id)+"?fields=id,name,mimeType&supportsAllDrives=true", nil)
		if err != nil {
			return nil, err
		}
		mt := toString(meta["mimeType"])
		u := d.base + "/files/" + url.PathEscape(id) + "?alt=media&supportsAllDrives=true"
		name := toString(meta["name"])
		if conv, ok := googleExportDefaults[mt]; ok {
			target, _ := ec.str(i, "options.googleFileConversion.conversion."+conv.key, conv.def)
			u = d.base + "/files/" + url.PathEscape(id) + "/export?mimeType=" + url.QueryEscape(target)
			mt = target
		}
		data, rmt, err := d.ec.downloadWith(u, d.headers)
		if err != nil {
			return nil, err
		}
		if rmt != "" && !strings.HasPrefix(rmt, "application/json") {
			mt = strings.Split(rmt, ";")[0]
		}
		if n, _ := ec.str(i, "options.fileName", ""); n != "" {
			name = n
		}
		prop, _ := ec.str(i, "options.binaryPropertyName", "data")
		return []Item{{JSON: it.JSON, Binary: map[string]interface{}{prop: binaryData(data, mt, name)}}}, nil
	case "file.copy":
		body := map[string]interface{}{}
		if n, _ := ec.str(i, "name", ""); n != "" {
			body["name"] = n
		}
		if ec.hasParam("folderId") {
			parent, _ := d.parent(i)
			body["parents"] = []string{parent}
		}
		res, err := d.call("POST", d.base+"/files/"+url.PathEscape(fileID())+"/copy?supportsAllDrives=true", body)
		return one(res), err
	case "file.move":
		id := fileID()
		meta, err := d.call("GET", d.base+"/files/"+url.PathEscape(id)+"?fields=parents&supportsAllDrives=true", nil)
		if err != nil {
			return nil, err
		}
		var old []string
		for _, p := range asList(meta["parents"]) {
			old = append(old, toString(p))
		}
		parent, _ := d.parent(i)
		q := url.Values{"addParents": {parent}, "removeParents": {strings.Join(old, ",")}, "supportsAllDrives": {"true"}}
		res, err := d.call("PATCH", d.base+"/files/"+url.PathEscape(id)+"?"+q.Encode(), map[string]interface{}{})
		return one(res), err
	case "file.update":
		body := map[string]interface{}{}
		if n, _ := ec.str(i, "newUpdatedFileName", ""); n != "" {
			body["name"] = n
		}
		res, err := d.call("PATCH", d.base+"/files/"+url.PathEscape(fileID())+"?supportsAllDrives=true", body)
		return one(res), err
	case "file.deleteFile", "folder.deleteFolder":
		key := "fileId"
		if resource == "folder" {
			key = "folderNoRootId"
		}
		id, _ := ec.rlString(i, key)
		if _, err := d.call("DELETE", d.base+"/files/"+url.PathEscape(id)+"?supportsAllDrives=true", nil); err != nil {
			return nil, err
		}
		return one(map[string]interface{}{"fileId": id, "success": true}), nil
	case "file.share", "folder.share":
		key := "fileId"
		if resource == "folder" {
			key = "folderNoRootId"
		}
		id, _ := ec.rlString(i, key)
		pv, _ := ec.param(i, "permissionsUi.permissionsValues", map[string]interface{}{})
		perm, _ := pv.(map[string]interface{})
		q := url.Values{"supportsAllDrives": {"true"}}
		if v, _ := ec.boolean(i, "options.sendNotificationEmail", true); !v {
			q.Set("sendNotificationEmail", "false")
		}
		if m, _ := ec.str(i, "options.emailMessage", ""); m != "" {
			q.Set("emailMessage", m)
		}
		res, err := d.call("POST", d.base+"/files/"+url.PathEscape(id)+"/permissions?"+q.Encode(), perm)
		return one(res), err
	case "folder.create":
		name, _ := ec.str(i, "name", "Untitled")
		parent, _ := d.parent(i)
		res, err := d.call("POST", d.base+"/files?supportsAllDrives=true&fields=id,name,mimeType,webViewLink", map[string]interface{}{"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": []string{parent}})
		return one(res), err
	case "fileFolder.search":
		return d.search(i)
	}
	return nil, permanentError{fmt.Sprintf("operation %s/%s is not supported", resource, op)}
}

func (d *driveAPI) search(i int) ([]Item, error) {
	ec := d.ec
	var q []string
	method, _ := ec.str(i, "searchMethod", "name")
	text, _ := ec.str(i, "queryString", "")
	if method == "query" {
		if text != "" {
			q = append(q, text)
		}
	} else if text != "" {
		q = append(q, fmt.Sprintf("name contains '%s'", strings.ReplaceAll(text, "'", `\'`)))
	}
	if f, _ := ec.rlString(i, "filter.folderId"); f != "" {
		q = append(q, fmt.Sprintf("'%s' in parents", f))
	}
	switch w, _ := ec.str(i, "filter.whatToSearch", "all"); w {
	case "files":
		q = append(q, "mimeType != 'application/vnd.google-apps.folder'")
	case "folders":
		q = append(q, "mimeType = 'application/vnd.google-apps.folder'")
	}
	if tr, _ := ec.boolean(i, "filter.includeTrashed", false); !tr {
		q = append(q, "trashed = false")
	}
	fields := "files(id,name,mimeType,webViewLink,parents,createdTime,modifiedTime,size),nextPageToken"
	returnAll, _ := ec.boolean(i, "returnAll", false)
	limit, _ := ec.num(i, "limit", 50)
	var out []Item
	token := ""
	for {
		v := url.Values{"q": {strings.Join(q, " and ")}, "fields": {fields}, "pageSize": {"100"}, "supportsAllDrives": {"true"}, "includeItemsFromAllDrives": {"true"}}
		if token != "" {
			v.Set("pageToken", token)
		}
		res, err := d.call("GET", d.base+"/files?"+v.Encode(), nil)
		if err != nil {
			return nil, err
		}
		for _, f := range asList(res["files"]) {
			m, _ := f.(map[string]interface{})
			out = append(out, Item{JSON: m})
		}
		token = toString(res["nextPageToken"])
		if token == "" || !returnAll && len(out) >= int(limit) {
			break
		}
	}
	if !returnAll && len(out) > int(limit) {
		out = out[:int(limit)]
	}
	return out, nil
}

// downloadWith fetches a URL with extra headers through the outbound policy.
func (ec *execCtx) downloadWith(u string, headers map[string]string) ([]byte, string, error) {
	_, raw, err := ec.apiCallRaw("GET", u, headers)
	if err != nil {
		return nil, "", err
	}
	return raw.body, raw.contentType, nil
}
